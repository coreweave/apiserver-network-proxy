/*
Copyright 2023 The Kubernetes Authors.

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

package framework

import (
	"fmt"
	"math/rand"
	"net"
	"net/http"
	"strconv"
	"time"
)

var ForeverTestTimeout = time.Minute

func checkReadiness(addr string) bool {
	resp, err := http.Get(fmt.Sprintf("http://%s/readyz", addr))
	if err != nil {
		return false
	}
	resp.Body.Close()
	return resp.StatusCode == http.StatusOK
}

func checkLiveness(addr string) bool {
	resp, err := http.Get(fmt.Sprintf("http://%s/healthz", addr))
	if err != nil {
		return false
	}
	resp.Body.Close()
	return resp.StatusCode == http.StatusOK
}

// FreePorts finds [count] available ports.
// Server option validation rejects ports above 49151, but on some platforms
// (e.g. darwin) the OS ephemeral range starts at 49152, so a port from
// listening on ":0" may never validate. Fall back to probing random
// non-ephemeral ports in that case.
func FreePorts(count int) ([]int, error) {
	ports := make([]int, count)
	for i := 0; i < count; i++ {
		p, err := freePort()
		if err != nil {
			return nil, err
		}
		ports[i] = p
	}
	return ports, nil
}

func freePort() (int, error) {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return 0, fmt.Errorf("failed to reserve ports: %w", err)
	}
	defer l.Close()
	_, p, err := net.SplitHostPort(l.Addr().String())
	if err != nil {
		return 0, fmt.Errorf("failed to reserve ports: %w", err)
	}
	port, err := strconv.Atoi(p)
	if err != nil {
		return 0, fmt.Errorf("failed to reserve ports: %w", err)
	}
	if port <= 49151 {
		return port, nil
	}
	// OS handed us an ephemeral-range port the server would reject; probe
	// random non-ephemeral ports instead.
	for attempt := 0; attempt < 100; attempt++ {
		candidate := 20000 + rand.Intn(49151-20000)
		cl, err := net.Listen("tcp", fmt.Sprintf("127.0.0.1:%d", candidate))
		if err != nil {
			continue // port in use, try another
		}
		cl.Close()
		return candidate, nil
	}
	return 0, fmt.Errorf("failed to find a free non-ephemeral port")
}
