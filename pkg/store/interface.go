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

package store

import (
	"context"
	"time"

	"github.com/volcano-sh/agentcube/pkg/common/types"
)

// SnapshotStore is the narrow interface for SnapStart snapshot metadata.
// SnapshotController depends on SnapshotStore, not the full Store, so that
// unrelated components (Router, GC) are not forced to implement these methods.
type SnapshotStore interface {
	// StoreSnapshot writes or updates the snapshot record for a specific (runtime, node) pair.
	// Redis key: snapshot:{namespace}:{name}, field: {nodeName}, value: JSON-encoded SnapshotInfo.
	StoreSnapshot(ctx context.Context, namespace, name string, info *types.SnapshotInfo) error
	// GetSnapshotNodes returns all per-node snapshot records for the given runtime.
	// Returns an empty slice (not error) if no snapshots are available.
	GetSnapshotNodes(ctx context.Context, namespace, name string) ([]*types.SnapshotInfo, error)
	// DeleteSnapshot removes the snapshot record for a specific (runtime, node) pair.
	DeleteSnapshot(ctx context.Context, namespace, name, nodeName string) error
	// DeleteAllSnapshots removes all per-node snapshot records for a runtime (used on SnapStart deletion).
	DeleteAllSnapshots(ctx context.Context, namespace, name string) error
	// ListSnapshotTemplateIDs returns all tracked template IDs across all runtimes and nodes.
	// Used by orphan GC to diff against Kuasar-reported templates.
	ListSnapshotTemplateIDs(ctx context.Context) ([]string, error)
	// ListAllSnapshotKeys returns all [namespace, name] pairs that have snapshot data in the store.
	// Used by stale-build GC to scan for Building entries stuck past buildingTimeout.
	ListAllSnapshotKeys(ctx context.Context) ([][2]string, error)
}

// Store is the full session + snapshot store interface used by Workload Manager.
type Store interface {
	// Ping check store provider available or not
	Ping(ctx context.Context) error
	// GetSandboxBySessionID get the sandbox by session ID
	GetSandboxBySessionID(ctx context.Context, sessionID string) (*types.SandboxInfo, error)
	// StoreSandbox store sandbox into storage
	StoreSandbox(ctx context.Context, sandboxStore *types.SandboxInfo) error
	// UpdateSandbox update sandbox of storage
	UpdateSandbox(ctx context.Context, sandboxStore *types.SandboxInfo) error
	// DeleteSandboxBySessionID delete sandbox by session ID
	DeleteSandboxBySessionID(ctx context.Context, sessionID string) error
	// ListExpiredSandboxes returns up to limit sandboxes with ExpiresAt before the given time
	ListExpiredSandboxes(ctx context.Context, before time.Time, limit int64) ([]*types.SandboxInfo, error)
	// ListInactiveSandboxes returns up to limit sandboxes with last-activity time before the given time
	ListInactiveSandboxes(ctx context.Context, before time.Time, limit int64) ([]*types.SandboxInfo, error)
	// UpdateSessionLastActivity updates the last-activity index for the given session
	UpdateSessionLastActivity(ctx context.Context, sessionID string, at time.Time) error
	// Close releases all resources held by the store (e.g. connection pools)
	Close() error
	// SnapshotStore embeds the snapshot-specific operations.
	SnapshotStore
}
