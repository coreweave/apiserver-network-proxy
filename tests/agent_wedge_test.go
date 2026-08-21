/*
Copyright 2026 The Kubernetes Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package tests

import (
	"bufio"
	"context"
	"fmt"
	"net"
	"net/http"
	"sync"
	"syscall"
	"testing"
	"time"

	"sigs.k8s.io/apiserver-network-proxy/tests/framework"
)

// Two tests, identical except for one thing: whether the stuck destination
// endpoint ever goes away.
//
//   Destination eventually dies -> --backend-dial-timeout works as intended:
//       the wedged agent is retired, and once the stuck destination is gone
//       the agent unblocks, notices its stream is dead, and reconnects.
//
//   Destination stays stuck     -> the agent is retired and NEVER comes back,
//       while reporting itself connected and ready the whole time. Zombie.
//
// How one stuck destination wedges the whole agent (pkg/agent/client.go):
//
//   destination stops reading
//     -> agent's proxyToRemote parks in conn.Write        (client.go:609)
//     -> that connection's dataCh (150 slots) fills
//     -> agent's ONE stream-reading goroutine, Serve(), parks at
//        `e.dataCh <- msg`                                (client.go:74)
//     -> agent stops reading its gRPC stream entirely
//     -> server's backend.Send blocks on HTTP/2 flow control
//     -> next dial trips --backend-dial-timeout -> 504 -> backend retired
//
// Retirement ends the Connect RPC, but the agent only notices when a Recv()
// on the stream returns an error. Normally that works: the agent's
// receive loop ((*Client).Serve, pkg/agent/client.go:315) is waiting in
// Recv(), gets the error, runs its defers (close endpoint conns,
// RemoveClient), and reconnects. But IF the dataCh is full, the agent is
// stuck on sending to the dataCh ((*endpointConn).send,
// pkg/agent/client.go:62, called from Serve's DATA case) and never makes it
// back to Recv() -- so it never sees the error, and never runs the defers
// that would close out the agent side of the connection. The ONLY thing that
// frees it is the destination-side write unblocking ((*Client).proxyToRemote,
// pkg/agent/client.go:609) or manually restarting the container -- which is
// exactly where the two tests vary.
//
// The worrying thing about this situation is that the server would
// essentially be running with fewer agents than expected. The server
// recognizes the wedged agent and rightfully stops sending new dials to it,
// but the agent stays stuck: it never processes the signal to restart its
// connection. Additionally, the Watchdog wouldn't be able to reach it. Since
// the server no longer registers it as a valid backend, a Watchdog probe to
// its kubelet just hits the healthy agent: the destHost proxy strategy is
// what lets the Watchdog target a specific agent, but the wedged backend no
// longer exists in the server, so the probe falls back to the default
// strategy. So this agent doesn't heal until it's unblocked or manually
// rolled.
//
// Ultimately, I think we should still roll this out, but pretty
// slowly, with alerting to catch the case where manual intervention is
// required, just to make sure this isn't a common case.

// Happy path: the destination is stuck long enough to trip the backend dial
// timeout, but then dies (process killed, conn reset). The agent unwedges,
// notices the retirement, and reconnects. This is retirement working as
// designed.
func TestBackendDialTimeout_StuckDestinationDies_AgentRecovers(t *testing.T) {
	dest := newStuckDestination(t)
	ps, a := startProxyAndAgent(t)

	wedgeAgentUntilRetired(t, ps, dest)

	// The stuck destination dies. The agent's blocked Write errors out,
	// Serve() unparks, reads the pending stream-dead error, runs its cleanup
	// (RemoveClient), and the sync loop redials within ~100ms.
	dest.die()

	waitForConnectedAgentCount(t, 1, ps)
	conn, status := httpConnect(t, ps.FrontAddr(), dest.addr)
	conn.Close()
	if status != http.StatusOK {
		t.Fatalf("expected 200 after agent recovery, got %d", status)
	}
	if !a.Ready() {
		t.Fatal("expected recovered agent to be ready")
	}
	t.Log("RECOVERED: destination died -> agent unwedged, reconnected, and is serving dials again")
}

// Same test, but the stuck destination never goes away
// (slow reader that never dies). The agent is retired and never comes
// back, while reporting itself connected and ready.
func TestBackendDialTimeout_StuckDestinationPersists_AgentNeverReconnects(t *testing.T) {
	dest := newStuckDestination(t)
	ps, a := startProxyAndAgent(t)

	wedgeAgentUntilRetired(t, ps, dest)

	// The agent never comes back. The test framework runs the agent's sync
	// loop every 100ms, so 5 seconds is ~50 missed chances to reconnect.
	time.Sleep(5 * time.Second)

	if count, _ := ps.ConnectedBackends(); count != 0 {
		t.Fatalf("agent reconnected (%d backends) -- zombie bug appears fixed!", count)
	}
	csc, _ := a.GetConnectedServerCount()
	t.Logf("ZOMBIE: server sees 0 backends; agent believes it has %d healthy server connections and reports ready=%v",
		csc, a.Ready())
	if csc != 1 {
		t.Errorf("expected wedged agent to still believe it is connected, got %d", csc)
	}
	if !a.Ready() {
		t.Error("expected wedged agent to still report ready")
	}
}

// wedgeAgentUntilRetired floods a tunnel to the stuck destination until the
// agent wedges and a dial trips the backend dial timeout (504), then waits
// for the server to retire the backend.
func wedgeAgentUntilRetired(t *testing.T, ps framework.ProxyServer, dest *stuckDestination) {
	t.Helper()

	// ONE connection to one stuck destination is all it takes.
	tunnel, status := httpConnect(t, ps.FrontAddr(), dest.addr)
	if status != http.StatusOK {
		t.Fatalf("CONNECT: expected 200, got %d", status)
	}
	t.Cleanup(func() { tunnel.Close() })
	go func() { // flood until the tunnel is torn down
		buf := make([]byte, 64*1024)
		for {
			if _, err := tunnel.Write(buf); err != nil {
				return
			}
		}
	}()

	// Keep dialing until the wedge bites: a DIAL_REQ send that can't complete
	// within backend-dial-timeout -> 504 + backend retirement.
	for i := 0; i < 100; i++ {
		conn, status := httpConnect(t, ps.FrontAddr(), dest.addr)
		conn.Close()
		if status == http.StatusGatewayTimeout {
			t.Log("dial failed with 504 -> server retired the backend")
			waitForConnectedAgentCount(t, 0, ps)
			return
		}
		if status != http.StatusOK {
			t.Fatalf("dial %d: expected 200 or 504, got %d", i, status)
		}
		time.Sleep(200 * time.Millisecond)
	}
	t.Fatal("agent never wedged")
}

// startProxyAndAgent starts an http-connect proxy server with a 500ms backend
// dial timeout, plus one agent, and waits for the agent to register.
func startProxyAndAgent(t *testing.T) (framework.ProxyServer, framework.Agent) {
	ps, err := Framework.ProxyServerRunner.Start(t, framework.ProxyServerOpts{
		Mode:               "http-connect",
		ServerCount:        1,
		BackendDialTimeout: 500 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("failed to start proxy server: %v", err)
	}
	a := runAgent(t, ps.AgentAddr())
	waitForConnectedAgentCount(t, 1, ps)
	return ps, a
}

// stuckDestination is a TCP endpoint that accepts connections and never reads
// from them -- a hung-but-alive process. die() closes its connections,
// simulating the hung process finally being killed.
type stuckDestination struct {
	addr string

	mu    sync.Mutex
	conns []net.Conn
}

// The 4KB SO_RCVBUF matters: explicitly setting it disables kernel buffer
// autotuning. With default buffers, loopback TCP can absorb several MB of
// backlog, letting the agent's blocked Write complete and the wedge self-heal
// -- a real hung endpoint has a full receive buffer: a zero TCP window.
func newStuckDestination(t *testing.T) *stuckDestination {
	lc := net.ListenConfig{
		Control: func(_, _ string, c syscall.RawConn) error {
			return c.Control(func(fd uintptr) {
				syscall.SetsockoptInt(int(fd), syscall.SOL_SOCKET, syscall.SO_RCVBUF, 4096)
			})
		},
	}
	lis, err := lc.Listen(context.Background(), "tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	d := &stuckDestination{addr: lis.Addr().String()}
	go func() {
		for {
			conn, err := lis.Accept()
			if err != nil {
				return
			}
			// Hold the connection referenced and never read from it. (If the
			// conn became unreachable, a GC finalizer would close it and
			// quietly release the wedge.)
			d.mu.Lock()
			d.conns = append(d.conns, conn)
			d.mu.Unlock()
		}
	}()
	t.Cleanup(func() {
		lis.Close()
		d.die()
	})
	return d
}

func (d *stuckDestination) die() {
	d.mu.Lock()
	defer d.mu.Unlock()
	for _, c := range d.conns {
		c.Close()
	}
	d.conns = nil
}

// httpConnect sends a CONNECT for target through the proxy's UDS frontend and
// returns the connection and the response status code.
func httpConnect(t *testing.T, proxyUDS, target string) (net.Conn, int) {
	t.Helper()
	conn, err := net.Dial("unix", proxyUDS)
	if err != nil {
		t.Fatal(err)
	}
	fmt.Fprintf(conn, "CONNECT %s HTTP/1.1\r\nHost: %s\r\n\r\n", target, "127.0.0.1")
	res, err := http.ReadResponse(bufio.NewReader(conn), nil)
	if err != nil {
		t.Fatalf("reading CONNECT response: %v", err)
	}
	res.Body.Close()
	return conn, res.StatusCode
}
