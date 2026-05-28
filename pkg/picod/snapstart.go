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
	cryptorand "crypto/rand"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"math/rand"
	"net"
	"os"
	"sync/atomic"
	"time"

	"k8s.io/klog/v2"
)

const (
	// warmForkSocketEnv is the env var that overrides the default inject socket path.
	warmForkSocketEnv = "WARMFORK_READINESS_SOCKET"
	// defaultWarmForkSocket is the default path for the Kuasar inject socket.
	defaultWarmForkSocket = "/run/warmfork-readiness.sock"
	// maxMessageSize is the maximum size of a single inject-protocol message (4 MiB).
	maxMessageSize = 4 << 20
)

// injectPrepare is the PREPARE message from Kuasar sandboxer.
type injectPrepare struct {
	Type         string            `json:"type"`
	TaskID       string            `json:"task_id"`
	Context      string            `json:"context"`
	EnvOverrides map[string]string `json:"env_overrides,omitempty"`
}

// socketPath returns the inject socket path, overridable by env var.
func socketPath() string {
	if p := os.Getenv(warmForkSocketEnv); p != "" {
		return p
	}
	return defaultWarmForkSocket
}

// writeMsg writes a length-prefixed JSON message to the connection.
// Wire format: 4-byte big-endian uint32 length + JSON body.
func writeMsg(conn net.Conn, v interface{}) error {
	body, err := json.Marshal(v)
	if err != nil {
		return fmt.Errorf("marshal: %w", err)
	}
	hdr := make([]byte, 4)
	binary.BigEndian.PutUint32(hdr, uint32(len(body)))
	if _, err := conn.Write(hdr); err != nil {
		return fmt.Errorf("write header: %w", err)
	}
	if _, err := conn.Write(body); err != nil {
		return fmt.Errorf("write body: %w", err)
	}
	return nil
}

// readRawMsg reads a length-prefixed message and returns the raw JSON bytes.
func readRawMsg(conn net.Conn) ([]byte, error) {
	hdr := make([]byte, 4)
	if _, err := io.ReadFull(conn, hdr); err != nil {
		return nil, fmt.Errorf("read header: %w", err)
	}
	size := binary.BigEndian.Uint32(hdr)
	if size > maxMessageSize {
		return nil, fmt.Errorf("message size %d exceeds maximum %d", size, maxMessageSize)
	}
	body := make([]byte, size)
	if _, err := io.ReadFull(conn, body); err != nil {
		return nil, fmt.Errorf("read body: %w", err)
	}
	return body, nil
}

// msgType extracts the "type" field from a raw JSON message.
func msgType(raw []byte) string {
	var v struct {
		Type string `json:"type"`
	}
	_ = json.Unmarshal(raw, &v)
	return v.Type
}

// runInjectSocket binds the WarmFork inject socket, enters accept() in a loop,
// and signals safeToSnapshot once the listener is ready.
// This function blocks until the process exits (via STARTED/CANCEL).
func (s *Server) runInjectSocket() {
	path := socketPath()
	_ = os.Remove(path) // clean up any leftover socket from a previous run

	ln, err := net.Listen("unix", path)
	if err != nil {
		klog.Errorf("snapstart: bind inject socket %s: %v", path, err)
		return
	}
	defer ln.Close()
	klog.Infof("snapstart: inject socket listening on %s", path)

	// Signal HTTP goroutine: accept() loop is ready → safeToSnapshot can become true.
	atomic.StoreInt32(&safeToSnapshot, 1)
	close(s.snapReady)

	for {
		conn, err := ln.Accept()
		if err != nil {
			klog.V(4).Infof("snapstart: accept: %v", err)
			return
		}
		if done := s.handleInjectConn(conn); done {
			return
		}
	}
}

// handleInjectConn handles a single connection from the Kuasar sandboxer.
// Returns true if picod should stop accepting (STARTED or unrecoverable error).
func (s *Server) handleInjectConn(conn net.Conn) (done bool) {
	defer conn.Close()

	// Send CAPABILITIES
	caps := map[string]interface{}{
		"type":               "CAPABILITIES",
		"protocol_version":   "1",
		"supported_features": []string{"prepare", "commit", "cancel"},
	}
	if err := writeMsg(conn, caps); err != nil {
		klog.Warningf("snapstart: send CAPABILITIES: %v", err)
		return false
	}

	// Read next message — may be PREPARE, CANCEL, or EOF (probe connection).
	raw, err := readRawMsg(conn)
	if err != nil {
		// EOF: probe connection from Kuasar (read CAPABILITIES then close). Loop back.
		klog.V(4).Infof("snapstart: read after CAPABILITIES: %v (likely probe)", err)
		return false
	}

	switch msgType(raw) {
	case "PREPARE":
		return s.handlePrepare(conn, raw)
	case "CANCEL":
		klog.Infof("snapstart: received CANCEL before PREPARE")
		os.Exit(0)
		return true
	default:
		klog.Warningf("snapstart: unexpected message type %q after CAPABILITIES", msgType(raw))
		return false
	}
}

// handlePrepare processes PREPARE → READY → COMMIT → STARTED.
func (s *Server) handlePrepare(conn net.Conn, raw []byte) bool {
	var prepare injectPrepare
	if err := json.Unmarshal(raw, &prepare); err != nil {
		klog.Warningf("snapstart: unmarshal PREPARE: %v", err)
		return false
	}
	klog.Infof("snapstart: PREPARE taskID=%s context=%s", prepare.TaskID, prepare.Context)

	// Send READY
	if err := writeMsg(conn, map[string]string{"type": "READY"}); err != nil {
		klog.Warningf("snapstart: send READY: %v", err)
		return false
	}

	// Read COMMIT or CANCEL
	nextRaw, err := readRawMsg(conn)
	if err != nil {
		klog.Warningf("snapstart: read COMMIT/CANCEL: %v", err)
		return false
	}

	switch msgType(nextRaw) {
	case "CANCEL":
		klog.Infof("snapstart: received CANCEL after READY")
		os.Exit(0)
		return true
	case "COMMIT":
		return s.handleCommit(conn, &prepare)
	default:
		klog.Warningf("snapstart: unexpected message %q after READY", msgType(nextRaw))
		return false
	}
}

// handleCommit applies session identity and transitions to Running.
func (s *Server) handleCommit(conn net.Conn, prepare *injectPrepare) bool {
	// 1. Apply env overrides
	for k, v := range prepare.EnvOverrides {
		if err := os.Setenv(k, v); err != nil {
			klog.Warningf("snapstart: setenv %s: %v", k, err)
		}
	}

	// 2. Reseed PRNG — CoW clone means all restore instances share the same internal PRNG
	// state at snapshot time. Use kernel-backed entropy (crypto/rand) as the seed source;
	// time.Now() ^ os.Getpid() is predictable and insufficient for CoW-clone isolation.
	var seedBytes [8]byte
	if _, err := cryptorand.Read(seedBytes[:]); err == nil {
		//nolint:gosec // intentional global-rand reseed post-fork; not used for crypto
		rand.Seed(int64(binary.BigEndian.Uint64(seedBytes[:])))
	} else {
		// crypto/rand unavailable (extremely unlikely); log a warning and fall back.
		klog.Warningf("snapstart: crypto/rand unavailable for PRNG reseed: %v; using time-based fallback", err)
		//nolint:gosec // fallback only when crypto/rand is unavailable
		rand.Seed(time.Now().UnixNano() ^ int64(os.Getpid()))
	}

	// 3. Clear safeToSnapshot — this instance is now Running, not a build sandbox.
	atomic.StoreInt32(&safeToSnapshot, 0)

	// 4. Send STARTED
	if err := writeMsg(conn, map[string]string{"type": "STARTED"}); err != nil {
		klog.Warningf("snapstart: send STARTED: %v", err)
	}

	klog.Infof("snapstart: session injected — taskID=%s, picod now Running", prepare.TaskID)
	return true
}
