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

package picod

import (
	"bytes"
	"encoding/json"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// -- handleInjectConn tests (use net.Pipe for in-process connection) --

func TestHandleInjectConn_Probe(t *testing.T) {
	client, server := net.Pipe()
	defer client.Close()

	done := make(chan struct{})
	go func() {
		defer close(done)
		params, err := handleInjectConn(server)
		assert.NoError(t, err)
		assert.Nil(t, params, "probe must return nil params")
	}()

	// Sandboxer side: read CAPABILITIES, then close (EOF).
	msg := recvFrame(t, client)
	assert.Equal(t, "CAPABILITIES", msg.Type)
	assert.Equal(t, warmforkProtocolVersion, msg.ProtocolVersion)
	client.Close()

	<-done
}

func TestHandleInjectConn_AutonomousMode(t *testing.T) {
	client, server := net.Pipe()
	defer client.Close()

	done := make(chan struct{})
	go func() {
		defer close(done)
		params, err := handleInjectConn(server)
		require.NoError(t, err)
		require.NotNil(t, params)
		assert.Empty(t, params.TaskID)
		assert.Empty(t, params.EnvOverrides)
	}()

	// Sandboxer: read CAPABILITIES, send COMMIT directly (no PREPARE).
	recvFrame(t, client)
	sendFrame(t, client, injectMsg{Type: "COMMIT"})

	// Workload sends STARTED.
	msg := recvFrame(t, client)
	assert.Equal(t, "STARTED", msg.Type)
	client.Close()

	<-done
}

func TestHandleInjectConn_InjectionMode(t *testing.T) {
	client, server := net.Pipe()
	defer client.Close()

	done := make(chan struct{})
	go func() {
		defer close(done)
		params, err := handleInjectConn(server)
		require.NoError(t, err)
		require.NotNil(t, params)
		assert.Equal(t, "task-42", params.TaskID)
		assert.Equal(t, "myctx", params.Context)
		assert.Equal(t, "bar", params.EnvOverrides["FOO"])
	}()

	// Sandboxer: CAPABILITIES → PREPARE → (wait for READY) → COMMIT → (wait for STARTED).
	recvFrame(t, client)
	sendFrame(t, client, injectMsg{
		Type:         "PREPARE",
		TaskID:       "task-42",
		Context:      "myctx",
		EnvOverrides: map[string]string{"FOO": "bar"},
	})
	msg := recvFrame(t, client)
	assert.Equal(t, "READY", msg.Type)
	sendFrame(t, client, injectMsg{Type: "COMMIT"})
	msg = recvFrame(t, client)
	assert.Equal(t, "STARTED", msg.Type)
	client.Close()

	<-done
}

func TestHandleInjectConn_CancelBeforePrepare(t *testing.T) {
	client, server := net.Pipe()
	defer client.Close()

	done := make(chan struct{})
	go func() {
		defer close(done)
		params, err := handleInjectConn(server)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "CANCEL")
		assert.Nil(t, params)
	}()

	recvFrame(t, client)
	sendFrame(t, client, injectMsg{Type: "CANCEL", Reason: "rollback"})
	client.Close()

	<-done
}

func TestHandleInjectConn_CancelAfterReady(t *testing.T) {
	client, server := net.Pipe()
	defer client.Close()

	done := make(chan struct{})
	go func() {
		defer close(done)
		params, err := handleInjectConn(server)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "CANCEL")
		assert.Nil(t, params)
	}()

	recvFrame(t, client)
	sendFrame(t, client, injectMsg{Type: "PREPARE", TaskID: "t1"})
	recvFrame(t, client) // READY
	sendFrame(t, client, injectMsg{Type: "CANCEL", Reason: "rollback"})
	client.Close()

	<-done
}

// -- gate middleware tests --

func TestGateMiddleware_BlocksBeforeOpen(t *testing.T) {
	server, ts := setupSessionGateServer(t)
	defer ts.Close()

	resp, err := ts.Client().Post(ts.URL+"/api/execute", "application/json", bytes.NewBufferString(`{}`))
	require.NoError(t, err)
	assert.Equal(t, http.StatusServiceUnavailable, resp.StatusCode)

	_ = server // referenced for clarity
}

func TestGateMiddleware_AllowsAfterOpen(t *testing.T) {
	server, ts := setupSessionGateServer(t)
	defer ts.Close()

	// Before OpenSessionGate: blocked.
	resp, err := ts.Client().Post(ts.URL+"/api/execute", "application/json", bytes.NewBufferString(`{}`))
	require.NoError(t, err)
	assert.Equal(t, http.StatusServiceUnavailable, resp.StatusCode)

	// After OpenSessionGate: auth middleware takes over (401, not 503).
	server.OpenSessionGate()
	resp, err = ts.Client().Post(ts.URL+"/api/execute", "application/json", bytes.NewBufferString(`{}`))
	require.NoError(t, err)
	assert.Equal(t, http.StatusUnauthorized, resp.StatusCode)
}

func TestGateMiddleware_OpenIsIdempotent(t *testing.T) {
	server, ts := setupSessionGateServer(t)
	defer ts.Close()

	require.NotPanics(t, func() {
		server.OpenSessionGate()
		server.OpenSessionGate()
		server.OpenSessionGate()
	})
}

func TestGateMiddleware_HealthNotGated(t *testing.T) {
	_, ts := setupSessionGateServer(t)
	defer ts.Close()

	resp, err := ts.Client().Get(ts.URL + "/health")
	require.NoError(t, err)
	assert.Equal(t, http.StatusOK, resp.StatusCode)
}

func TestSessionGate_NotSetByDefault(t *testing.T) {
	os.Unsetenv("AGENTCUBE_SESSION_GATE")
	_, pubStr := generateRSAKeys(t)
	os.Setenv(PublicKeyEnvVar, pubStr)
	defer os.Unsetenv(PublicKeyEnvVar)

	tmpDir := t.TempDir()
	s := NewServer(Config{Workspace: tmpDir})
	assert.False(t, s.SessionGateEnabled())
}

// -- helpers --

// setupSessionGateServer creates a Server with the session gate enabled.
func setupSessionGateServer(t *testing.T) (*Server, *httptest.Server) {
	t.Helper()
	t.Setenv("AGENTCUBE_SESSION_GATE", "true")

	_, pubStr := generateRSAKeys(t)
	t.Setenv(PublicKeyEnvVar, pubStr)

	tmpDir := t.TempDir()
	s := NewServer(Config{Workspace: tmpDir})
	ts := httptest.NewServer(s.engine)
	return s, ts
}

func sendFrame(t *testing.T, conn net.Conn, msg injectMsg) {
	t.Helper()
	require.NoError(t, writeFrame(conn, msg))
}

func recvFrame(t *testing.T, conn net.Conn) injectMsg {
	t.Helper()
	msg, err := readFrame(conn)
	require.NoError(t, err)
	return msg
}

// jsonRoundtrip is a helper for verifying JSON serialisation in tests.
func jsonRoundtrip(t *testing.T, v any) string {
	t.Helper()
	b, err := json.Marshal(v)
	require.NoError(t, err)
	return string(b)
}

// resolveSocketPath returns a unique temp socket path for test isolation.
func resolveSocketPath(t *testing.T) string {
	t.Helper()
	return filepath.Join(t.TempDir(), "inject.sock")
}
