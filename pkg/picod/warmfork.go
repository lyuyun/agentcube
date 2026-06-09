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
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"os"

	"k8s.io/klog/v2"
)

const (
	// DefaultInjectSocketPath is the Kuasar WarmFork inject socket default path.
	// Override via AGENTCUBE_INJECT_SOCKET_PATH.
	DefaultInjectSocketPath = "/run/warmfork-readiness.sock"

	// injectSocketEnvVar overrides DefaultInjectSocketPath when set.
	injectSocketEnvVar = "AGENTCUBE_INJECT_SOCKET_PATH"

	// maxFrameBodySize is the Kuasar WarmFork protocol maximum frame body size.
	maxFrameBodySize = 4 << 20 // 4 MiB

	warmforkProtocolVersion = "1"
)

// HandshakeResult carries the session state delivered by the Kuasar sandboxer
// during the WarmFork handshake. All fields are empty in autonomous mode.
type HandshakeResult struct {
	TaskID       string
	EnvOverrides map[string]string
	Context      string
}

// WaitForHandshake implements the Kuasar WarmFork workload side of the
// readiness and handshake protocol (v1).
//
// It opens the inject socket, then loops on Accept() to handle the
// pre-snapshot probe and (after VM restore) the handshake:
//
//   - Pre-snapshot probe: accept → CAPABILITIES → EOF → loop
//   - Injection mode:     accept → CAPABILITIES → PREPARE → READY → COMMIT → STARTED
//   - Autonomous mode:    accept → CAPABILITIES → COMMIT → STARTED
//
// WaitForHandshake blocks until STARTED is sent to the sandboxer (the formal
// restore commit point). The caller must call Server.OpenSessionGate() afterwards to
// open the HTTP API for user requests.
//
// socketPath may be empty; the function then reads AGENTCUBE_INJECT_SOCKET_PATH
// or falls back to DefaultInjectSocketPath.
func WaitForHandshake(ctx context.Context, socketPath string) (*HandshakeResult, error) {
	if socketPath == "" {
		if v := os.Getenv(injectSocketEnvVar); v != "" {
			socketPath = v
		} else {
			socketPath = DefaultInjectSocketPath
		}
	}

	ln, err := net.Listen("unix", socketPath)
	if err != nil {
		return nil, fmt.Errorf("warmfork: listen on %s: %w", socketPath, err)
	}
	defer ln.Close()

	// Close the listener when ctx is cancelled to unblock Accept.
	// The goroutine exits when either ctx fires (closes ln) or WaitForHandshake
	// returns (closes stopCh), whichever comes first.
	stopCh := make(chan struct{})
	defer close(stopCh)
	go func() {
		select {
		case <-ctx.Done():
			ln.Close()
		case <-stopCh:
		}
	}()

	klog.V(2).InfoS("warmfork: inject socket open, blocking on accept", "path", socketPath)

	for {
		conn, err := ln.Accept()
		if err != nil {
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			default:
				return nil, fmt.Errorf("warmfork: accept: %w", err)
			}
		}

		params, err := handleInjectConn(conn)
		if err != nil {
			// Non-probe errors (CANCEL, protocol errors) are fatal: the sandboxer
			// is aborting the restore. Return immediately rather than looping.
			return nil, err
		}
		if params != nil {
			return params, nil
		}
		// params == nil, err == nil: pre-snapshot probe, loop back to accept().
	}
}

// handleInjectConn handles one accepted connection.
//
// Returns (*HandshakeResult, nil) when the handshake is complete (STARTED sent).
// Returns (nil, nil) for a probe connection (EOF after CAPABILITIES): caller loops back.
// Returns (nil, err) on protocol errors or CANCEL: caller returns the error.
func handleInjectConn(conn net.Conn) (*HandshakeResult, error) {
	defer conn.Close()

	if err := writeFrame(conn, injectMsg{
		Type:              "CAPABILITIES",
		ProtocolVersion:   warmforkProtocolVersion,
		SupportedFeatures: []string{"prepare", "commit", "cancel"},
	}); err != nil {
		return nil, fmt.Errorf("warmfork: write CAPABILITIES: %w", err)
	}

	msg, err := readFrame(conn)
	if err == io.EOF || err == io.ErrUnexpectedEOF {
		// Sandboxer closed the connection after reading CAPABILITIES: pre-snapshot probe.
		klog.V(4).InfoS("warmfork: pre-snapshot probe complete")
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("warmfork: read after CAPABILITIES: %w", err)
	}

	switch msg.Type {
	case "PREPARE":
		return handlePrepare(conn, msg)
	case "COMMIT":
		// Autonomous mode: sandboxer skips PREPARE/READY, sends COMMIT directly.
		return completeHandshake(conn, &HandshakeResult{})
	case "CANCEL":
		return nil, fmt.Errorf("warmfork: CANCEL before PREPARE: %s", msg.Reason)
	default:
		return nil, fmt.Errorf("warmfork: unexpected message type %q", msg.Type)
	}
}

func handlePrepare(conn net.Conn, msg injectMsg) (*HandshakeResult, error) {
	// Validate the PREPARE payload. Per protocol: no externally-visible side
	// effects before READY is sent.
	params := &HandshakeResult{
		TaskID:       msg.TaskID,
		EnvOverrides: msg.EnvOverrides,
		Context:      msg.Context,
	}

	if err := writeFrame(conn, injectMsg{Type: "READY"}); err != nil {
		return nil, fmt.Errorf("warmfork: write READY: %w", err)
	}

	next, err := readFrame(conn)
	if err != nil {
		return nil, fmt.Errorf("warmfork: read after READY: %w", err)
	}
	switch next.Type {
	case "COMMIT":
		return completeHandshake(conn, params)
	case "CANCEL":
		return nil, fmt.Errorf("warmfork: CANCEL after READY: %s", next.Reason)
	default:
		return nil, fmt.Errorf("warmfork: unexpected message %q after READY", next.Type)
	}
}

// completeHandshake sends STARTED (the formal restore commit point).
// The connection is closed by the deferred close in handleInjectConn.
func completeHandshake(conn net.Conn, params *HandshakeResult) (*HandshakeResult, error) {
	if err := writeFrame(conn, injectMsg{Type: "STARTED"}); err != nil {
		return nil, fmt.Errorf("warmfork: write STARTED: %w", err)
	}
	klog.V(2).InfoS("warmfork: STARTED sent, restore committed", "taskID", params.TaskID)
	return params, nil
}

// -- Wire protocol: 4-byte big-endian length prefix + UTF-8 JSON body --

type injectMsg struct {
	Type              string            `json:"type"`
	ProtocolVersion   string            `json:"protocol_version,omitempty"`
	SupportedFeatures []string          `json:"supported_features,omitempty"`
	TaskID            string            `json:"task_id,omitempty"`
	EnvOverrides      map[string]string `json:"env_overrides,omitempty"`
	Context           string            `json:"context,omitempty"`
	Reason            string            `json:"reason,omitempty"`
	Message           string            `json:"message,omitempty"`
}

func writeFrame(conn net.Conn, msg injectMsg) error {
	body, err := json.Marshal(msg)
	if err != nil {
		return err
	}
	if len(body) > maxFrameBodySize {
		return fmt.Errorf("frame body too large: %d bytes", len(body))
	}
	var hdr [4]byte
	binary.BigEndian.PutUint32(hdr[:], uint32(len(body)))
	if _, err := conn.Write(hdr[:]); err != nil {
		return err
	}
	_, err = conn.Write(body)
	return err
}

func readFrame(conn net.Conn) (injectMsg, error) {
	var hdr [4]byte
	if _, err := io.ReadFull(conn, hdr[:]); err != nil {
		return injectMsg{}, err
	}
	size := binary.BigEndian.Uint32(hdr[:])
	if size > maxFrameBodySize {
		return injectMsg{}, fmt.Errorf("frame body too large: %d bytes", size)
	}
	body := make([]byte, size)
	if _, err := io.ReadFull(conn, body); err != nil {
		return injectMsg{}, err
	}
	var msg injectMsg
	if err := json.Unmarshal(body, &msg); err != nil {
		return injectMsg{}, fmt.Errorf("unmarshal inject message: %w", err)
	}
	return msg, nil
}
