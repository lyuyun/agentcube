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

package workloadmanager

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/util/workqueue"

	runtimev1alpha1 "github.com/volcano-sh/agentcube/pkg/apis/runtime/v1alpha1"
	"github.com/volcano-sh/agentcube/pkg/common/types"
	pkgstore "github.com/volcano-sh/agentcube/pkg/store"
)

// ---------------------------------------------------------------------------
// Shared test helpers
// ---------------------------------------------------------------------------

// snapFakeStore is a controllable fake for snapshot-specific store methods.
type snapFakeStore struct {
	pkgstore.Store // embed nil; only snapshot methods are implemented below

	snapshots     map[string]map[string]*types.SnapshotInfo // key: "ns/name", field: nodeName
	storeErr      error
	allKeysResult [][2]string
	allKeysErr    error
}

func newSnapFakeStore() *snapFakeStore {
	return &snapFakeStore{snapshots: make(map[string]map[string]*types.SnapshotInfo)}
}

func (s *snapFakeStore) runtimeKey(ns, name string) string { return ns + "/" + name }

func (s *snapFakeStore) StoreSnapshot(_ context.Context, ns, name string, info *types.SnapshotInfo) error {
	if s.storeErr != nil {
		return s.storeErr
	}
	k := s.runtimeKey(ns, name)
	if s.snapshots[k] == nil {
		s.snapshots[k] = make(map[string]*types.SnapshotInfo)
	}
	cp := *info
	s.snapshots[k][info.NodeName] = &cp
	return nil
}

func (s *snapFakeStore) GetSnapshotNodes(_ context.Context, ns, name string) ([]*types.SnapshotInfo, error) {
	k := s.runtimeKey(ns, name)
	var result []*types.SnapshotInfo
	for _, v := range s.snapshots[k] {
		cp := *v
		result = append(result, &cp)
	}
	return result, nil
}

func (s *snapFakeStore) ListAllSnapshotKeys(_ context.Context) ([][2]string, error) {
	if s.allKeysErr != nil {
		return nil, s.allKeysErr
	}
	return s.allKeysResult, nil
}

// fakeSharedIndexInformer wraps a cache.Store so it can satisfy cache.SharedIndexInformer
// for unit tests that only need GetStore().
type fakeSharedIndexInformer struct {
	cache.SharedIndexInformer // nil embed — other methods must not be called in tests
	testStore                 cache.Store
}

func (f *fakeSharedIndexInformer) GetStore() cache.Store { return f.testStore }

// ciToUnstructured marshals a CodeInterpreter into an *unstructured.Unstructured so it
// can be inserted into a fake informer cache (which stores unstructured objects).
func ciToUnstructured(t *testing.T, ci *runtimev1alpha1.CodeInterpreter) *unstructured.Unstructured {
	t.Helper()
	b, err := json.Marshal(ci)
	require.NoError(t, err)
	var obj map[string]interface{}
	require.NoError(t, json.Unmarshal(b, &obj))
	u := &unstructured.Unstructured{Object: obj}
	u.SetAPIVersion("runtime.agentcube.volcano.sh/v1alpha1")
	u.SetKind("CodeInterpreter")
	return u
}

// makeTestController builds a minimal SnapshotController with a fake store and a
// fake CI informer pre-populated with the given CodeInterpreter.
func makeTestController(t *testing.T, ci *runtimev1alpha1.CodeInterpreter, store *snapFakeStore) *SnapshotController {
	t.Helper()
	ciStore := cache.NewIndexer(cache.MetaNamespaceKeyFunc, cache.Indexers{})
	if ci != nil {
		require.NoError(t, ciStore.Add(ciToUnstructured(t, ci)))
	}
	return &SnapshotController{
		storeClient: store,
		informers: &Informers{
			CodeInterpreterInformer: &fakeSharedIndexInformer{testStore: ciStore},
		},
		indexer:       newSnapStartIndexer(),
		queue:         workqueue.NewNamedRateLimitingQueue(workqueue.DefaultControllerRateLimiter(), "snapstart-test"),
		probeInterval: 1 * time.Millisecond, // fast polling in tests
	}
}

// ---------------------------------------------------------------------------
// shouldInvalidate
// ---------------------------------------------------------------------------
// filterSnapshotsByVersion
// ---------------------------------------------------------------------------

func makeFullInfo(nodeName, specHash, imageRef, checkpoint, protoVer string) *types.SnapshotInfo {
	return &types.SnapshotInfo{
		NodeName:        nodeName,
		SpecHash:        specHash,
		ImageRef:        imageRef,
		Checkpoint:      checkpoint,
		ProtocolVersion: protoVer,
	}
}

func TestFilterSnapshotsByVersion(t *testing.T) {
	gate := snapshotVersionGate{
		SpecHash:        "hash1",
		ImageRef:        "img:v1",
		Checkpoint:      "InterpreterReady",
		ProtocolVersion: "1",
	}
	cases := []struct {
		name          string
		infos         []*types.SnapshotInfo
		gate          snapshotVersionGate
		wantNodeNames []string
	}{
		{
			name: "all fields match — both nodes pass",
			infos: []*types.SnapshotInfo{
				makeFullInfo("node-a", "hash1", "img:v1", "InterpreterReady", "1"),
				makeFullInfo("node-b", "hash1", "img:v1", "InterpreterReady", "1"),
			},
			gate:          gate,
			wantNodeNames: []string{"node-a", "node-b"},
		},
		{
			name: "specHash mismatch drops node",
			infos: []*types.SnapshotInfo{
				makeFullInfo("node-a", "old-hash", "img:v1", "InterpreterReady", "1"),
				makeFullInfo("node-b", "hash1", "img:v1", "InterpreterReady", "1"),
			},
			gate:          gate,
			wantNodeNames: []string{"node-b"},
		},
		{
			name: "imageRef mismatch drops node",
			infos: []*types.SnapshotInfo{
				makeFullInfo("node-a", "hash1", "img:v1", "InterpreterReady", "1"),
				makeFullInfo("node-b", "hash1", "img:v2", "InterpreterReady", "1"),
			},
			gate:          gate,
			wantNodeNames: []string{"node-a"},
		},
		{
			name: "checkpoint mismatch drops node",
			infos: []*types.SnapshotInfo{
				makeFullInfo("node-a", "hash1", "img:v1", "BrowserReady", "1"),
				makeFullInfo("node-b", "hash1", "img:v1", "InterpreterReady", "1"),
			},
			gate:          gate,
			wantNodeNames: []string{"node-b"},
		},
		{
			name: "protocolVersion mismatch drops node",
			infos: []*types.SnapshotInfo{
				makeFullInfo("node-a", "hash1", "img:v1", "InterpreterReady", "0"),
				makeFullInfo("node-b", "hash1", "img:v1", "InterpreterReady", "1"),
			},
			gate:          gate,
			wantNodeNames: []string{"node-b"},
		},
		{
			name: "empty mandatory fields — stale pre-upgrade entry rejected",
			infos: []*types.SnapshotInfo{
				{NodeName: "node-a", SpecHash: "", ImageRef: ""},
			},
			gate:          gate,
			wantNodeNames: nil,
		},
		{
			name: "all mismatch returns nil",
			infos: []*types.SnapshotInfo{
				makeFullInfo("node-a", "wrong", "img:v1", "InterpreterReady", "1"),
			},
			gate:          gate,
			wantNodeNames: nil,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := filterSnapshotsByVersion(tc.infos, tc.gate)
			var gotNames []string
			for _, g := range got {
				gotNames = append(gotNames, g.NodeName)
			}
			assert.Equal(t, tc.wantNodeNames, gotNames)
		})
	}
}

// ---------------------------------------------------------------------------

func makeSS(phase runtimev1alpha1.SnapshotPhase,
	restoreFailures int32, readyAt *metav1.Time, inv *runtimev1alpha1.SnapStartInvalidation,
) *runtimev1alpha1.SnapStart {
	return &runtimev1alpha1.SnapStart{
		ObjectMeta: metav1.ObjectMeta{Namespace: "default", Name: "ss-test"},
		Spec: runtimev1alpha1.SnapStartSpec{
			RuntimeRef:   runtimev1alpha1.RuntimeReference{Kind: "CodeInterpreter", Name: "ci-test"},
			Checkpoint:   "InterpreterReady",
			Invalidation: inv,
		},
		Status: runtimev1alpha1.SnapStartStatus{
			Snapshot: &runtimev1alpha1.SnapshotStatus{
				Phase:               phase,
				RestoreFailureCount: restoreFailures,
				ReadyAt:             readyAt,
			},
		},
	}
}

func makeCI(image, runtimeClass string, args []string) *runtimev1alpha1.CodeInterpreter {
	tmpl := &runtimev1alpha1.CodeInterpreterSandboxTemplate{
		Image: image,
		Args:  args,
	}
	if runtimeClass != "" {
		tmpl.RuntimeClassName = &runtimeClass
	}
	return &runtimev1alpha1.CodeInterpreter{
		ObjectMeta: metav1.ObjectMeta{Namespace: "default", Name: "ci-test"},
		Spec:       runtimev1alpha1.CodeInterpreterSpec{Template: tmpl},
	}
}

func boolPtr(b bool) *bool { return &b }

func TestShouldInvalidate_RestoreFailureCount(t *testing.T) {
	sc := makeTestController(t, makeCI("img:v1", "", nil), newSnapFakeStore())
	ss := makeSS(runtimev1alpha1.SnapshotPhaseReady, 3, nil, nil)
	reason := sc.shouldInvalidate(context.Background(), ss)
	assert.Contains(t, reason, "3 consecutive restore failures")
}

func TestShouldInvalidate_MaxAge(t *testing.T) {
	sc := makeTestController(t, makeCI("img:v1", "", nil), newSnapFakeStore())
	past := metav1.NewTime(time.Now().Add(-25 * time.Hour))
	maxAge := &metav1.Duration{Duration: 24 * time.Hour}
	ss := makeSS(runtimev1alpha1.SnapshotPhaseReady, 0, &past,
		&runtimev1alpha1.SnapStartInvalidation{MaxAge: maxAge})
	reason := sc.shouldInvalidate(context.Background(), ss)
	assert.Contains(t, reason, "maxAge")
}

func TestShouldInvalidate_MaxAge_NotExpired(t *testing.T) {
	sc := makeTestController(t, makeCI("img:v1", "", nil), newSnapFakeStore())
	recent := metav1.NewTime(time.Now().Add(-1 * time.Hour))
	maxAge := &metav1.Duration{Duration: 24 * time.Hour}
	ss := makeSS(runtimev1alpha1.SnapshotPhaseReady, 0, &recent,
		&runtimev1alpha1.SnapStartInvalidation{MaxAge: maxAge})
	assert.Empty(t, sc.shouldInvalidate(context.Background(), ss))
}

// shouldInvalidate spec/image drift tests pre-populate the Redis store (snapFakeStore)
// since per-node SpecHash/ImageRef is now stored in Redis only, not in CRD status.
func TestShouldInvalidate_SpecHashDrift(t *testing.T) {
	ci := makeCI("img:v1", "", []string{"--old-flag"})
	store := newSnapFakeStore()
	require.NoError(t, store.StoreSnapshot(context.Background(), "default", "ss-test", &types.SnapshotInfo{
		NodeName: "node-a", SpecHash: "stale-hash", ImageRef: "img:v1",
		Checkpoint: "InterpreterReady", ProtocolVersion: "1",
		CacheState: string(runtimev1alpha1.CacheStateLocalReady),
	}))
	sc := makeTestController(t, ci, store)
	ss := makeSS(runtimev1alpha1.SnapshotPhaseReady, 0, nil, nil)
	reason := sc.shouldInvalidate(context.Background(), ss)
	assert.Contains(t, reason, "spec changed")
}

func TestShouldInvalidate_SpecHashMatch(t *testing.T) {
	ci := makeCI("img:v1", "", []string{"--flag"})
	store := newSnapFakeStore()
	require.NoError(t, store.StoreSnapshot(context.Background(), "default", "ss-test", &types.SnapshotInfo{
		NodeName: "node-a", SpecHash: computeSpecHashNoImage(ci), ImageRef: "img:v1",
		Checkpoint: "InterpreterReady", ProtocolVersion: "1",
		CacheState: string(runtimev1alpha1.CacheStateLocalReady),
	}))
	sc := makeTestController(t, ci, store)
	ss := makeSS(runtimev1alpha1.SnapshotPhaseReady, 0, nil, nil)
	assert.Empty(t, sc.shouldInvalidate(context.Background(), ss))
}

func TestShouldInvalidate_ImageRefChanged(t *testing.T) {
	ci := makeCI("img:v2", "", nil)
	store := newSnapFakeStore()
	require.NoError(t, store.StoreSnapshot(context.Background(), "default", "ss-test", &types.SnapshotInfo{
		NodeName: "node-a", SpecHash: computeSpecHashNoImage(ci), ImageRef: "img:v1",
		Checkpoint: "InterpreterReady", ProtocolVersion: "1",
		CacheState: string(runtimev1alpha1.CacheStateLocalReady),
	}))
	sc := makeTestController(t, ci, store)
	ss := makeSS(runtimev1alpha1.SnapshotPhaseReady, 0, nil, nil)
	reason := sc.shouldInvalidate(context.Background(), ss)
	assert.Contains(t, reason, "image changed")
	assert.Contains(t, reason, "img:v1")
	assert.Contains(t, reason, "img:v2")
}

func TestShouldInvalidate_OnImageDigestChange_False(t *testing.T) {
	ci := makeCI("img:v2", "", nil)
	store := newSnapFakeStore()
	require.NoError(t, store.StoreSnapshot(context.Background(), "default", "ss-test", &types.SnapshotInfo{
		NodeName: "node-a", SpecHash: computeSpecHashNoImage(ci), ImageRef: "img:v1",
		Checkpoint: "InterpreterReady", ProtocolVersion: "1",
		CacheState: string(runtimev1alpha1.CacheStateLocalReady),
	}))
	sc := makeTestController(t, ci, store)
	ss := makeSS(runtimev1alpha1.SnapshotPhaseReady, 0, nil,
		&runtimev1alpha1.SnapStartInvalidation{OnImageDigestChange: boolPtr(false)})
	assert.Empty(t, sc.shouldInvalidate(context.Background(), ss))
}

func TestShouldInvalidate_EmptyStore(t *testing.T) {
	ci := makeCI("img:v1", "", nil)
	sc := makeTestController(t, ci, newSnapFakeStore())
	ss := makeSS(runtimev1alpha1.SnapshotPhaseReady, 0, nil, nil)
	// No Redis entries → nothing to compare; should not invalidate.
	assert.Empty(t, sc.shouldInvalidate(context.Background(), ss))
}

// ---------------------------------------------------------------------------
// sweepStaleBuildingEntries
// ---------------------------------------------------------------------------

func TestSweepStaleBuildingEntries_ResetsStuckEntry(t *testing.T) {
	store := newSnapFakeStore()
	store.allKeysResult = [][2]string{{"default", "ci-test"}}
	// Inject a Building entry started 20 minutes ago (> buildingTimeout of 10m).
	staleStart := time.Now().Add(-20 * time.Minute)
	require.NoError(t, store.StoreSnapshot(context.Background(), "default", "ci-test", &types.SnapshotInfo{
		CacheState: string(runtimev1alpha1.CacheStateBuilding),
		NodeName:   "node-a",
		NodeIP:     "1.2.3.4",
		StartedAt:  staleStart,
	}))

	sc := makeTestController(t, nil, store)
	sc.sweepStaleBuildingEntries(context.Background())

	infos, err := store.GetSnapshotNodes(context.Background(), "default", "ci-test")
	require.NoError(t, err)
	require.Len(t, infos, 1)
	assert.Equal(t, string(runtimev1alpha1.CacheStateFailed), infos[0].CacheState,
		"stale Building entry should be reset to Failed")
}

func TestSweepStaleBuildingEntries_IgnoresFreshEntry(t *testing.T) {
	store := newSnapFakeStore()
	store.allKeysResult = [][2]string{{"default", "ci-test"}}
	// Fresh Building entry (only 2 minutes old).
	require.NoError(t, store.StoreSnapshot(context.Background(), "default", "ci-test", &types.SnapshotInfo{
		CacheState: string(runtimev1alpha1.CacheStateBuilding),
		NodeName:   "node-a",
		StartedAt:  time.Now().Add(-2 * time.Minute),
	}))

	sc := makeTestController(t, nil, store)
	sc.sweepStaleBuildingEntries(context.Background())

	infos, _ := store.GetSnapshotNodes(context.Background(), "default", "ci-test")
	require.Len(t, infos, 1)
	assert.Equal(t, string(runtimev1alpha1.CacheStateBuilding), infos[0].CacheState,
		"fresh Building entry must not be reset")
}

func TestSweepStaleBuildingEntries_IgnoresReadyEntry(t *testing.T) {
	store := newSnapFakeStore()
	store.allKeysResult = [][2]string{{"default", "ci-test"}}
	require.NoError(t, store.StoreSnapshot(context.Background(), "default", "ci-test", &types.SnapshotInfo{
		CacheState: string(runtimev1alpha1.CacheStateLocalReady),
		NodeName:   "node-a",
		StartedAt:  time.Now().Add(-60 * time.Minute),
	}))

	sc := makeTestController(t, nil, store)
	sc.sweepStaleBuildingEntries(context.Background())

	infos, _ := store.GetSnapshotNodes(context.Background(), "default", "ci-test")
	require.Len(t, infos, 1)
	assert.Equal(t, string(runtimev1alpha1.CacheStateLocalReady), infos[0].CacheState,
		"LocalReady entry must never be reset by stale-build GC")
}

func TestSweepStaleBuildingEntries_ListKeysError(t *testing.T) {
	store := newSnapFakeStore()
	store.allKeysErr = fmt.Errorf("redis unavailable")
	sc := makeTestController(t, nil, store)
	// Must not panic; errors are only logged.
	assert.NotPanics(t, func() { sc.sweepStaleBuildingEntries(context.Background()) })
}

// ---------------------------------------------------------------------------
// waitSafeToSnapshot — contamination guard
// ---------------------------------------------------------------------------

func TestWaitSafeToSnapshot_RejectsUserStateLoaded(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewEncoder(w).Encode(runtimeStatusResponse{
			Checkpoint:      "InterpreterReady",
			SafeToSnapshot:  true,
			UserStateLoaded: true, // contaminated
		})
	}))
	defer srv.Close()

	sc := &SnapshotController{probeInterval: 1 * time.Millisecond}
	err := sc.waitSafeToSnapshot(context.Background(), srv.URL, "InterpreterReady")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "userStateLoaded")
}

func TestWaitSafeToSnapshot_RejectsActiveTasks(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewEncoder(w).Encode(runtimeStatusResponse{
			Checkpoint:     "InterpreterReady",
			SafeToSnapshot: true,
			ActiveTasks:    2,
		})
	}))
	defer srv.Close()

	sc := &SnapshotController{probeInterval: 1 * time.Millisecond}
	err := sc.waitSafeToSnapshot(context.Background(), srv.URL, "InterpreterReady")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "activeTasks=2")
}

func TestWaitSafeToSnapshot_CleanSnapshot(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewEncoder(w).Encode(runtimeStatusResponse{
			Checkpoint:      "InterpreterReady",
			SafeToSnapshot:  true,
			UserStateLoaded: false,
			ActiveTasks:     0,
		})
	}))
	defer srv.Close()

	sc := &SnapshotController{probeInterval: 1 * time.Millisecond}
	err := sc.waitSafeToSnapshot(context.Background(), srv.URL, "InterpreterReady")
	assert.NoError(t, err)
}

func TestWaitSafeToSnapshot_PersistentNon200_ProtocolNotSupported(t *testing.T) {
	calls := 0
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer srv.Close()

	sc := &SnapshotController{probeInterval: 1 * time.Millisecond}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	err := sc.waitSafeToSnapshot(ctx, srv.URL, "InterpreterReady")
	require.Error(t, err)
	assert.ErrorIs(t, err, errProtocolNotSupported,
		"persistent 500 must be classified as ProtocolNotSupported")
	assert.GreaterOrEqual(t, calls, protoErrorThreshold,
		"must probe at least protoErrorThreshold times before giving up")
}

func TestWaitSafeToSnapshot_PersistentBadJSON_ProtocolNotSupported(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte("not json"))
	}))
	defer srv.Close()

	sc := &SnapshotController{probeInterval: 1 * time.Millisecond}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	err := sc.waitSafeToSnapshot(ctx, srv.URL, "InterpreterReady")
	require.Error(t, err)
	assert.ErrorIs(t, err, errProtocolNotSupported,
		"persistent JSON parse failure must be classified as ProtocolNotSupported")
}

func TestWaitSafeToSnapshot_PersistentNotFound_ProtocolNotSupported(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNotFound)
	}))
	defer srv.Close()

	sc := &SnapshotController{probeInterval: 1 * time.Millisecond}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	err := sc.waitSafeToSnapshot(ctx, srv.URL, "InterpreterReady")
	require.Error(t, err)
	assert.ErrorIs(t, err, errProtocolNotSupported,
		"persistent 404 must be classified as ProtocolNotSupported")
}

// ---------------------------------------------------------------------------
// resolveImageDigest — parsing logic tests (no Kubernetes clientset needed)
// ---------------------------------------------------------------------------

// parseImageDigestFromStatus mirrors the parsing logic inside resolveImageDigest so we can
// unit-test it without a live Kubernetes clientset.
func parseImageDigestFromStatus(imageID string) (string, error) {
	parts := strings.SplitN(imageID, "@sha256:", 2)
	if len(parts) == 2 {
		digest := parts[1]
		if len(digest) >= 16 {
			return "sha256-" + digest[:16], nil
		}
		return "sha256-" + digest, nil
	}
	return "", fmt.Errorf("imageID %q does not match expected '<image>@sha256:<hash>' format; "+
		"cannot derive a unique template key", imageID)
}

func TestResolveImageDigest_ValidDigest(t *testing.T) {
	digest, err := parseImageDigestFromStatus("registry.io/img@sha256:abcdef1234567890extra")
	require.NoError(t, err)
	assert.Equal(t, "sha256-abcdef1234567890", digest)
}

func TestResolveImageDigest_ShortDigest(t *testing.T) {
	digest, err := parseImageDigestFromStatus("img@sha256:abc")
	require.NoError(t, err)
	assert.Equal(t, "sha256-abc", digest)
}

func TestResolveImageDigest_NonStandardImageID_ReturnsError(t *testing.T) {
	_, err := parseImageDigestFromStatus("just-a-name-without-digest")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "does not match expected",
		"non-standard imageID must return an error, not silently truncate")
}

// ---------------------------------------------------------------------------
// buildPlacementFromInfos conversion completeness
// ---------------------------------------------------------------------------

// TestBuildPlacementFromInfos_Completeness verifies that buildPlacementFromInfos
// TestCountSnapshotNodes verifies that aggregate counts are correctly derived from Redis
// SnapshotInfo records. Per-node details never appear in CRD status.
func TestCountSnapshotNodes(t *testing.T) {
	infos := []*types.SnapshotInfo{
		{NodeName: "node-a", CacheState: string(runtimev1alpha1.CacheStateLocalReady)},
		{NodeName: "node-b", CacheState: string(runtimev1alpha1.CacheStateLocalReady)},
		{NodeName: "node-c", CacheState: string(runtimev1alpha1.CacheStateFailed)},
		{NodeName: "node-d", CacheState: string(runtimev1alpha1.CacheStateUnavailable)},
		{NodeName: "node-e", CacheState: string(runtimev1alpha1.CacheStateBuilding)},
	}
	eligible, ready, failed, unavailable := countSnapshotNodes(infos)
	assert.Equal(t, int32(4), eligible)
	assert.Equal(t, int32(2), ready)
	assert.Equal(t, int32(1), failed)
	assert.Equal(t, int32(1), unavailable)
}

func TestSnapshotPlacementNeedsBuild(t *testing.T) {
	now := time.Now()
	tests := []struct {
		name string
		info *types.SnapshotInfo
		want bool
	}{
		{name: "missing entry", want: true},
		{name: "ready entry", info: &types.SnapshotInfo{CacheState: string(runtimev1alpha1.CacheStateLocalReady)}, want: false},
		{name: "recent failure waits", info: &types.SnapshotInfo{
			CacheState: string(runtimev1alpha1.CacheStateFailed),
			StartedAt:  now.Add(-incrementalRetryDelay / 2),
		}, want: false},
		{name: "old failure retries", info: &types.SnapshotInfo{
			CacheState: string(runtimev1alpha1.CacheStateFailed),
			StartedAt:  now.Add(-incrementalRetryDelay),
		}, want: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, snapshotPlacementNeedsBuild(tt.info, now))
		})
	}
}
