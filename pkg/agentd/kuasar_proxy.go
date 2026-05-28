/*
Copyright The Volcano Authors.

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

package agentd

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"strings"

	"k8s.io/klog/v2"
)

const (
	// KuasarAdminSocketPath is the default Unix socket path for the Kuasar Admin API.
	KuasarAdminSocketPath = "/run/vmm-sandboxer-admin.sock"
)

// allowedActions is the whitelist of Kuasar Admin API operations the proxy will forward.
var allowedActions = map[string]bool{
	"create-template": true,
	"delete-template": true,
	"list-templates":  true,
}

// KuasarAdminProxy is a plain-HTTP server that proxies allowed Kuasar Admin API
// calls from Workload Manager to the local Kuasar Admin Unix socket.
// It listens on Pod IP:9090 and authenticates callers via a shared Bearer token.
// NetworkPolicy additionally restricts access to Workload Manager pods.
type KuasarAdminProxy struct {
	listenAddr string
	socketPath string
	token      string
	server     *http.Server
}

// NewKuasarAdminProxy creates a KuasarAdminProxy.
// listenAddr is the host:port to listen on (e.g. "10.0.0.1:9090").
// socketPath is the Kuasar Admin Unix socket; "" uses KuasarAdminSocketPath.
// token is the shared Bearer token callers must present; Start() fails if empty.
func NewKuasarAdminProxy(listenAddr, socketPath, token string) *KuasarAdminProxy {
	if socketPath == "" {
		socketPath = KuasarAdminSocketPath
	}
	p := &KuasarAdminProxy{
		listenAddr: listenAddr,
		socketPath: socketPath,
		token:      token,
	}
	mux := http.NewServeMux()
	mux.HandleFunc("/kuasar-admin", p.handleRequest)
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	})
	p.server = &http.Server{
		Addr:    listenAddr,
		Handler: mux,
	}
	return p
}

// Start starts the proxy server. It blocks until the server stops.
// Returns an error immediately if the token is empty.
func (p *KuasarAdminProxy) Start() error {
	if p.token == "" {
		return fmt.Errorf("KuasarAdminProxy: token is empty; refusing to start without authentication")
	}
	klog.Infof("KuasarAdminProxy: listening on %s", p.listenAddr)
	return p.server.ListenAndServe()
}

// DefaultToken reads the proxy Bearer token from KUASAR_PROXY_TOKEN env var.
func DefaultToken() string {
	return os.Getenv("KUASAR_PROXY_TOKEN")
}

// DefaultListenAddr returns the default listen address using Pod IP:9090.
// Returns an error if POD_IP is not set; the proxy must not bind to 0.0.0.0.
func DefaultListenAddr() (string, error) {
	podIP := os.Getenv("POD_IP")
	if podIP == "" {
		return "", fmt.Errorf("POD_IP environment variable is not set; KuasarAdminProxy cannot start (must not bind to 0.0.0.0)")
	}
	return net.JoinHostPort(podIP, "9090"), nil
}

// handleRequest authenticates the caller, validates the action, and forwards to Kuasar.
func (p *KuasarAdminProxy) handleRequest(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	if r.Header.Get("Authorization") != "Bearer "+p.token {
		http.Error(w, "unauthorized", http.StatusUnauthorized)
		return
	}

	body, err := io.ReadAll(io.LimitReader(r.Body, 4<<20))
	if err != nil {
		http.Error(w, "read body: "+err.Error(), http.StatusBadRequest)
		return
	}

	var payload map[string]interface{}
	if err := json.Unmarshal(body, &payload); err != nil {
		http.Error(w, "invalid JSON: "+err.Error(), http.StatusBadRequest)
		return
	}
	action, _ := payload["action"].(string)
	if !allowedActions[action] {
		http.Error(w, fmt.Sprintf("action %q not allowed", action), http.StatusBadRequest)
		return
	}

	kuasarResp, err := p.forwardToKuasar(body)
	if err != nil {
		klog.Errorf("KuasarAdminProxy: forward action %q to kuasar: %v", action, err)
		http.Error(w, "kuasar error: "+err.Error(), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(kuasarResp)
}

// forwardToKuasar sends a JSON request to the local Kuasar Admin socket and returns the response body.
func (p *KuasarAdminProxy) forwardToKuasar(body []byte) ([]byte, error) {
	transport := &http.Transport{
		DialContext: func(ctx context.Context, network, addr string) (net.Conn, error) {
			return net.Dial("unix", p.socketPath)
		},
	}
	client := &http.Client{Transport: transport}

	req, err := http.NewRequest(http.MethodPost, "http://kuasar-admin/admin", bytes.NewReader(body))
	if err != nil {
		return nil, fmt.Errorf("create request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("unix socket POST: %w", err)
	}
	defer resp.Body.Close()

	data, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("read kuasar response: %w", err)
	}

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("kuasar status %d: %s", resp.StatusCode, strings.TrimSpace(string(data)))
	}
	return data, nil
}
