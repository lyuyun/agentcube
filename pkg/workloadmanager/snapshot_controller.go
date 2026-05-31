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
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"sort"
	"strings"
	"sync"
	"time"

	runtimev1alpha1 "github.com/volcano-sh/agentcube/pkg/apis/runtime/v1alpha1"
	"github.com/volcano-sh/agentcube/pkg/common/types"
	"github.com/volcano-sh/agentcube/pkg/store"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	k8stypes "k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/scheme"
	typedcorev1 "k8s.io/client-go/kubernetes/typed/core/v1"
	"k8s.io/client-go/tools/record"
	"k8s.io/client-go/util/workqueue"
	"k8s.io/klog/v2"
	sandboxv1alpha1 "sigs.k8s.io/agent-sandbox/api/v1alpha1"
)

// eventScheme combines core Kubernetes types with AgentCube CRD types for event recording.
var eventScheme = func() *runtime.Scheme {
	s := runtime.NewScheme()
	_ = scheme.AddToScheme(s)
	_ = runtimev1alpha1.AddToScheme(s)
	return s
}()

const (
	// buildingTimeout is the maximum time a per-node snapshot can remain in Building phase
	// before it is considered failed and reset by the stale-build GC.
	buildingTimeout = 10 * time.Minute

	// notFoundThreshold is how many consecutive 404s trigger ProtocolNotSupported.
	notFoundThreshold = 3
	// protoErrorThreshold is how many consecutive non-200 or bad-JSON responses trigger ProtocolNotSupported.
	protoErrorThreshold = 5

	// orphanGCInterval is how often the orphan GC sweep runs.
	orphanGCInterval = 1 * time.Hour

	// orphanGracePeriod is the minimum age before an untracked template is considered an orphan.
	orphanGracePeriod = 15 * time.Minute

	// maxSessionTTL is the maximum wait time for lease_count to reach zero during cleanup.
	maxSessionTTL = 30 * time.Minute

	// snapshotReadyProbeInterval is how often to poll /runtime/status.
	snapshotReadyProbeInterval = 5 * time.Second

	// snapshotReadyProbeTimeout is the maximum time to wait for safeToSnapshot=true.
	snapshotReadyProbeTimeout = 15 * time.Minute

	// incrementalRetryDelay prevents a failed per-node placement from being rebuilt in a
	// tight informer loop. Periodic reconcile will retry it after this delay.
	incrementalRetryDelay = 1 * time.Minute

	// agentdKuasarProxyTokenSecret is the K8s Secret holding the agentd proxy Bearer token.
	agentdKuasarProxyTokenSecret = "agentd-kuasar-proxy-token" //nolint:gosec
	// agentdKuasarProxyTokenKey is the key within the Secret for the token value.
	agentdKuasarProxyTokenKey = "token"
)

// SnapStartGVR is the GroupVersionResource for the SnapStart CRD.
var SnapStartGVR = schema.GroupVersionResource{
	Group:    "runtime.agentcube.volcano.sh",
	Version:  "v1alpha1",
	Resource: "snapstarts",
}

// runtimeStatusResponse is the JSON response from GET /runtime/status on picod.
type runtimeStatusResponse struct {
	Checkpoint      string                 `json:"checkpoint"`
	SafeToSnapshot  bool                   `json:"safeToSnapshot"`
	UserStateLoaded bool                   `json:"userStateLoaded"`
	ActiveTasks     int                    `json:"activeTasks"`
	Details         map[string]interface{} `json:"details,omitempty"`
}

// snapStartIndexer maintains an in-memory index: runtimeRef namespace/name → SnapStart list.
type snapStartIndexer struct {
	mu    sync.RWMutex
	index map[string][]*runtimev1alpha1.SnapStart // key: "namespace/runtimeName"
}

func newSnapStartIndexer() *snapStartIndexer {
	return &snapStartIndexer{index: make(map[string][]*runtimev1alpha1.SnapStart)}
}

func (si *snapStartIndexer) runtimeKey(namespace, runtimeName string) string {
	return namespace + "/" + runtimeName
}

func (si *snapStartIndexer) upsert(ss *runtimev1alpha1.SnapStart) {
	k := si.runtimeKey(ss.Namespace, ss.Spec.RuntimeRef.Name)
	si.mu.Lock()
	defer si.mu.Unlock()
	for i, existing := range si.index[k] {
		if existing.Name == ss.Name {
			si.index[k][i] = ss
			return
		}
	}
	si.index[k] = append(si.index[k], ss)
}

func (si *snapStartIndexer) remove(ss *runtimev1alpha1.SnapStart) {
	k := si.runtimeKey(ss.Namespace, ss.Spec.RuntimeRef.Name)
	si.mu.Lock()
	defer si.mu.Unlock()
	list := si.index[k]
	for i, existing := range list {
		if existing.Name == ss.Name {
			si.index[k] = append(list[:i], list[i+1:]...)
			return
		}
	}
}

func (si *snapStartIndexer) getByRuntime(namespace, runtimeName string) []*runtimev1alpha1.SnapStart {
	k := si.runtimeKey(namespace, runtimeName)
	si.mu.RLock()
	defer si.mu.RUnlock()
	result := make([]*runtimev1alpha1.SnapStart, len(si.index[k]))
	copy(result, si.index[k])
	return result
}

// SnapshotController watches SnapStart CRD objects and manages the complete
// WarmForkSnapshot lifecycle: build → ready → invalidate/cleanup.
type SnapshotController struct {
	k8sClient     *K8sClient
	clientset     kubernetes.Interface
	dynamicClient dynamic.Interface
	storeClient   store.SnapshotStore
	informers     *Informers
	indexer       *snapStartIndexer
	queue         workqueue.RateLimitingInterface
	probeInterval time.Duration // interval between /runtime/status polls; defaults to snapshotReadyProbeInterval
	recorder      record.EventRecorder
}

// newSnapshotController creates a new SnapshotController.
func newSnapshotController(k8sClient *K8sClient, storeClient store.SnapshotStore, informers *Informers) *SnapshotController {
	broadcaster := record.NewBroadcaster()
	broadcaster.StartRecordingToSink(&typedcorev1.EventSinkImpl{Interface: k8sClient.clientset.CoreV1().Events("")})
	recorder := broadcaster.NewRecorder(eventScheme, corev1.EventSource{Component: "snapshot-controller"})
	return &SnapshotController{
		k8sClient:     k8sClient,
		clientset:     k8sClient.clientset,
		dynamicClient: k8sClient.dynamicClient,
		storeClient:   storeClient,
		informers:     informers,
		indexer:       newSnapStartIndexer(),
		queue:         workqueue.NewNamedRateLimitingQueue(workqueue.DefaultControllerRateLimiter(), "snapstart"),
		probeInterval: snapshotReadyProbeInterval,
		recorder:      recorder,
	}
}

// newKuasarClient returns a KuasarAdminClient for the given node IP.
// The Bearer token is fetched from the agentd proxy token Secret.
// If the token cannot be fetched, the client is still returned with an empty token
// (the proxy will reject it, surfacing the misconfiguration at call time).
func (sc *SnapshotController) newKuasarClient(ctx context.Context, nodeIP string) *KuasarAdminClient {
	token, err := sc.getAgentdProxyToken(ctx)
	if err != nil {
		klog.Warningf("SnapshotController: get agentd proxy token: %v (calls will be rejected by proxy)", err)
	}
	return NewKuasarAdminClient(nodeIP, token)
}

// getAgentdProxyToken reads the Bearer token from the agentd proxy Secret.
func (sc *SnapshotController) getAgentdProxyToken(ctx context.Context) (string, error) {
	secret, err := sc.clientset.CoreV1().Secrets(IdentitySecretNamespace).Get(
		ctx, agentdKuasarProxyTokenSecret, metav1.GetOptions{})
	if err != nil {
		return "", fmt.Errorf("get secret %s/%s: %w", IdentitySecretNamespace, agentdKuasarProxyTokenSecret, err)
	}
	token, ok := secret.Data[agentdKuasarProxyTokenKey]
	if !ok {
		return "", fmt.Errorf("key %q not found in secret %s/%s",
			agentdKuasarProxyTokenKey, IdentitySecretNamespace, agentdKuasarProxyTokenSecret)
	}
	return strings.TrimSpace(string(token)), nil
}

// EnqueueAllReady re-enqueues every known SnapStart so that periodic checks (maxAge,
// node availability) are applied even without an incoming event.
func (sc *SnapshotController) EnqueueAllReady() {
	sc.indexer.mu.RLock()
	defer sc.indexer.mu.RUnlock()
	for _, list := range sc.indexer.index {
		for _, ss := range list {
			sc.Enqueue(ss.Namespace, ss.Name)
		}
	}
}

// Start implements manager.Runnable. controller-runtime calls Start only on the elected
// leader, so SnapshotController lifecycle mutations are naturally single-writer without a
// separate Lease. Session restore reads remain available on every replica because the
// informer event handlers (which maintain the in-memory indexer) run independently of Start.
func (sc *SnapshotController) Start(ctx context.Context) error {
	// Informers are started by server.Start() concurrently; wait until they are synced
	// before processing work items to avoid acting on a stale cache.
	if err := sc.informers.waitForCacheSync(ctx); err != nil {
		return fmt.Errorf("SnapshotController: wait for informer cache sync: %w", err)
	}
	sc.runAsLeader(ctx)
	return nil
}

// NeedLeaderElection implements manager.LeaderElectionRunnable.
// Returning true tells controller-runtime to call Start only on the elected leader.
func (sc *SnapshotController) NeedLeaderElection() bool { return true }

func (sc *SnapshotController) runAsLeader(ctx context.Context) {
	klog.Info("SnapshotController: starting")
	go sc.runOrphanGC(ctx)
	defer sc.queue.ShutDown()

	// Periodic enqueue to catch maxAge expiry and node-availability changes that
	// don't generate direct SnapStart events.
	maxAgeTicker := time.NewTicker(5 * time.Minute)
	defer maxAgeTicker.Stop()
	for i := 0; i < 4; i++ {
		go func() {
			for sc.processNextWorkItem(ctx) {
			}
		}()
	}

	for {
		select {
		case <-ctx.Done():
			klog.Info("SnapshotController: stopping")
			return
		case <-maxAgeTicker.C:
			sc.EnqueueAllReady()
		}
	}
}

func (sc *SnapshotController) processNextWorkItem(ctx context.Context) bool {
	item, shutdown := sc.queue.Get()
	if shutdown {
		return false
	}
	defer sc.queue.Done(item)

	key, ok := item.(string)
	if !ok {
		sc.queue.Forget(item)
		return true
	}
	parts := strings.SplitN(key, "/", 2)
	if len(parts) != 2 {
		klog.Warningf("SnapshotController: invalid queue key %q", key)
		sc.queue.Forget(item)
		return true
	}
	if err := sc.reconcileByKey(ctx, parts[0], parts[1]); err != nil {
		klog.Errorf("SnapshotController: reconcile %s failed: %v", key, err)
		sc.queue.AddRateLimited(key)
		return true
	}
	sc.queue.Forget(item)
	return true
}

// emitEvent emits a Kubernetes Event on the SnapStart object. No-op if recorder is nil (test environments).
func (sc *SnapshotController) emitEvent(ss *runtimev1alpha1.SnapStart, eventType, reason, msgFmt string, args ...interface{}) {
	if sc.recorder == nil {
		return
	}
	sc.recorder.Eventf(ss, eventType, reason, msgFmt, args...)
}

// Enqueue adds a SnapStart key to the reconcile queue (exported for informer use).
func (sc *SnapshotController) Enqueue(namespace, name string) {
	sc.queue.Add(namespace + "/" + name)
}

// reconcileByKey fetches a SnapStart by key and calls reconcile.
func (sc *SnapshotController) reconcileByKey(ctx context.Context, namespace, name string) error {
	obj, err := sc.dynamicClient.Resource(SnapStartGVR).Namespace(namespace).Get(ctx, name, metav1.GetOptions{})
	if err != nil {
		if apierrors.IsNotFound(err) {
			return nil
		}
		return fmt.Errorf("get SnapStart %s/%s: %w", namespace, name, err)
	}

	var ss runtimev1alpha1.SnapStart
	if err := unstructuredToSnapStart(obj, &ss); err != nil {
		return fmt.Errorf("convert SnapStart: %w", err)
	}
	return sc.reconcile(ctx, &ss)
}

// reconcile is the main reconcile loop for a single SnapStart.
func (sc *SnapshotController) reconcile(ctx context.Context, ss *runtimev1alpha1.SnapStart) error {
	// Handle deletion
	if !ss.DeletionTimestamp.IsZero() {
		return sc.handleDeletion(ctx, ss)
	}

	// Inject finalizer if not present
	if !hasFinalizer(ss.Finalizers, types.SnapshotFinalizer) {
		return sc.addFinalizer(ctx, ss)
	}

	// Phase 1 constraint validation (defense-in-depth: webhook may be disabled).
	// Guard: if already Failed with the same reason, skip the PATCH to avoid an
	// infinite reconcile loop (PATCH → informer notification → reconcile → PATCH…).
	alreadyFailedWith := func(msg string) bool {
		return ss.Status.Snapshot != nil &&
			ss.Status.Snapshot.Phase == runtimev1alpha1.SnapshotPhaseFailed &&
			ss.Status.Snapshot.FailedReason == msg
	}
	if ss.Spec.RuntimeRef.Kind == "AgentRuntime" {
		msg := "AgentRuntime SnapStart is Phase 2 only; not yet implemented"
		if alreadyFailedWith(msg) {
			return nil
		}
		return sc.patchStatusForSnapStart(ctx, ss, runtimev1alpha1.SessionStartupModeCold,
			runtimev1alpha1.SnapshotPhaseFailed, msg, nil)
	}
	if ss.Spec.Artifact != nil {
		switch ss.Spec.Artifact.Distribution {
		case "", runtimev1alpha1.DistributionModeNodeLocal:
			// ok
		default:
			msg := fmt.Sprintf("artifact.distribution=%s is not supported in Phase 1; only NodeLocal is available",
				ss.Spec.Artifact.Distribution)
			if alreadyFailedWith(msg) {
				return nil
			}
			return sc.patchStatusForSnapStart(ctx, ss, runtimev1alpha1.SessionStartupModeCold,
				runtimev1alpha1.SnapshotPhaseFailed, msg, nil)
		}
	}
	// Register in indexer
	sc.indexer.upsert(ss)

	// Sync node availability: mark Redis entries Unavailable for nodes that are no longer
	// eligible or Ready. Done here (inside reconcile) so errors get the standard backoff
	// and there is no concurrent Redis access from event handlers.
	if availabilityChanged := sc.syncNodeAvailability(ctx, ss); availabilityChanged &&
		ss.Status.Snapshot != nil && ss.Status.Snapshot.Phase == runtimev1alpha1.SnapshotPhaseReady {
		return sc.patchReadyStatusForSnapStart(ctx, ss, false, runtimev1alpha1.SnapshotPhaseReady,
			ss.Status.Message, ss.Status.Conditions)
	}

	// Check for duplicate SnapStart (same runtimeRef)
	conflicts := sc.indexer.getByRuntime(ss.Namespace, ss.Spec.RuntimeRef.Name)
	for _, c := range conflicts {
		if c.Name != ss.Name && c.DeletionTimestamp == nil {
			msg := fmt.Sprintf("conflict: SnapStart %s already references %s %s; delete it before creating a new one",
				c.Name, ss.Spec.RuntimeRef.Kind, ss.Spec.RuntimeRef.Name)
			if alreadyFailedWith(msg) {
				return nil
			}
			return sc.patchStatusForSnapStart(ctx, ss, runtimev1alpha1.SessionStartupModeCold,
				runtimev1alpha1.SnapshotPhaseFailed, msg, nil)
		}
	}

	// Retry transient build failures at the persisted retry time. Keeping retry state in the
	// CRD avoids hot loops from informer updates and preserves the retry budget across restarts.
	if annotationValue, exists := ss.Annotations[types.AnnotationForceRebuild]; exists && annotationValue != "true" &&
		ss.Status.Snapshot != nil && ss.Status.Snapshot.BuildFailureCount > 0 {
		return sc.resetBuildRetryState(ctx, ss)
	}

	retryableBuildDue := false
	if ss.Status.Snapshot != nil && ss.Status.Snapshot.Phase == runtimev1alpha1.SnapshotPhaseFailed &&
		ss.Status.Snapshot.BuildFailureCount > 0 {
		if ss.Status.Snapshot.BuildFailureCount >= 3 {
			return nil
		}
		if retryAt := ss.Status.Snapshot.NextBuildRetryAt; retryAt != nil && time.Now().Before(retryAt.Time) {
			sc.queue.AddAfter(ss.Namespace+"/"+ss.Name, time.Until(retryAt.Time))
			return nil
		}
		retryableBuildDue = true
	}

	// Check for force-rebuild annotation
	if ss.Annotations[types.AnnotationForceRebuild] == "true" {
		klog.Infof("SnapshotController: force rebuild triggered for %s/%s", ss.Namespace, ss.Name)
		if err := sc.cleanupSnapshot(ctx, ss.Namespace, ss.Name); err != nil {
			return fmt.Errorf("force rebuild cleanup: %w", err)
		}
		if err := sc.patchStatusForSnapStart(ctx, ss, runtimev1alpha1.SessionStartupModeCold,
			runtimev1alpha1.SnapshotPhaseInvalidated, "manual force-rebuild", nil); err != nil {
			return err
		}
		if err := sc.buildSnapshot(ctx, ss); err != nil {
			return fmt.Errorf("force rebuild build: %w", err)
		}
		// Remove annotation after success (best-effort)
		_ = sc.removeAnnotation(ctx, ss.Namespace, ss.Name, types.AnnotationForceRebuild, ss.Annotations)
		return nil
	}

	// When phase=Ready, check for invalidation and new/recovered eligible nodes.
	if ss.Status.Snapshot != nil && ss.Status.Snapshot.Phase == runtimev1alpha1.SnapshotPhaseReady {
		if reason := sc.shouldInvalidate(ctx, ss); reason != "" {
			klog.Infof("SnapshotController: invalidating %s/%s: %s", ss.Namespace, ss.Name, reason)
			if err := sc.cleanupSnapshot(ctx, ss.Namespace, ss.Name); err != nil {
				return fmt.Errorf("invalidate snapshot: %w", err)
			}
			if err := sc.patchStatusForSnapStart(ctx, ss, runtimev1alpha1.SessionStartupModeCold,
				runtimev1alpha1.SnapshotPhaseInvalidated, reason, nil); err != nil {
				return err
			}
			return sc.buildSnapshot(ctx, ss)
		}
		// Build snapshots on any newly eligible nodes without changing the overall Ready phase.
		if sc.hasNewEligibleNodes(ctx, ss) {
			klog.Infof("SnapshotController: %s/%s is Ready but new eligible nodes found; running incremental build",
				ss.Namespace, ss.Name)
			return sc.buildSnapshotIncremental(ctx, ss)
		}
		return nil
	}

	// Do not retry if the runtime permanently does not implement /runtime/status.
	for _, c := range ss.Status.Conditions {
		if c.Type == "ProtocolNotSupported" && c.Status == metav1.ConditionTrue {
			klog.V(4).Infof("SnapshotController: skipping build for %s/%s: ProtocolNotSupported is permanent", ss.Namespace, ss.Name)
			return nil
		}
	}

	// Phase=Failed: prevent tight retry loops.
	// A rebuild is only attempted when new eligible nodes appear (nodes that were not
	// targeted in the previous build attempt). Triggering events:
	//   - A new node gets the kuasar-snapstart label → NodeInformer UpdateFunc → EnqueueAllReady
	//   - An existing node recovers → NodeInformer UpdateFunc → EnqueueAllReady
	//   - Force-rebuild annotation → handled above
	// Without a triggering event, the informer notification from the Failed PATCH would
	// otherwise cause an immediate retry → another Failed PATCH → infinite storm.
	if ss.Status.Snapshot != nil && ss.Status.Snapshot.Phase == runtimev1alpha1.SnapshotPhaseFailed {
		if !retryableBuildDue && !sc.hasNewEligibleNodes(ctx, ss) {
			klog.V(4).Infof("SnapshotController: %s/%s is Failed and no new eligible nodes; skipping auto-retry", ss.Namespace, ss.Name)
			return nil
		}
		klog.Infof("SnapshotController: %s/%s is Failed but new eligible nodes found; retrying build", ss.Namespace, ss.Name)
	}

	// Build snapshot
	if err := sc.patchStatusForSnapStart(ctx, ss, runtimev1alpha1.SessionStartupModeCold,
		runtimev1alpha1.SnapshotPhasePending,
		"Snapshot template is being created. Sessions are cold starting.", nil); err != nil {
		return err
	}
	err := sc.buildSnapshot(ctx, ss)
	if err == nil && ss.Annotations[types.AnnotationForceRebuild] == "true" {
		_ = sc.removeAnnotation(ctx, ss.Namespace, ss.Name, types.AnnotationForceRebuild, ss.Annotations)
	}
	return err
}

// hasNewEligibleNodes returns true when at least one currently eligible node has no Redis
// entry or has a Failed entry whose incremental retry delay has elapsed.
func (sc *SnapshotController) hasNewEligibleNodes(ctx context.Context, ss *runtimev1alpha1.SnapStart) bool {
	ci, err := sc.getCodeInterpreter(ss.Namespace, ss.Spec.RuntimeRef.Name)
	if err != nil {
		return false
	}
	eligible, err := sc.getEligibleNodesForCI(ctx, ci)
	if err != nil || len(eligible) == 0 {
		return false
	}
	infos, _ := sc.storeClient.GetSnapshotNodes(ctx, ss.Namespace, ss.Name)
	tracked := make(map[string]*types.SnapshotInfo, len(infos))
	for _, info := range infos {
		tracked[info.NodeName] = info
	}
	for _, n := range eligible {
		if snapshotPlacementNeedsBuild(tracked[n.Name], time.Now()) {
			return true
		}
	}
	return false
}

// shouldInvalidate checks whether a Ready snapshot should be invalidated due to spec drift,
// maxAge expiry, or 3 consecutive restore failures. Returns a non-empty reason string if
// invalidation is needed, empty string if the snapshot is still valid.
func (sc *SnapshotController) shouldInvalidate(ctx context.Context, ss *runtimev1alpha1.SnapStart) string {
	snap := ss.Status.Snapshot

	// restoreFailureCount >= 3 → Invalidated (restore path is broken, rebuild)
	if snap.RestoreFailureCount >= 3 {
		return fmt.Sprintf("invalidated after %d consecutive restore failures", snap.RestoreFailureCount)
	}

	// maxAge expired
	if snap.ReadyAt != nil && ss.Spec.Invalidation != nil && ss.Spec.Invalidation.MaxAge != nil {
		if time.Since(snap.ReadyAt.Time) > ss.Spec.Invalidation.MaxAge.Duration {
			return fmt.Sprintf("maxAge %s exceeded (snapshot age: %s)",
				ss.Spec.Invalidation.MaxAge.Duration, time.Since(snap.ReadyAt.Time).Truncate(time.Second))
		}
	}

	// Spec and image drift: compare stored state against the current CI spec.
	// OnArgsChange (default true) triggers on non-image field changes (args, env, resources, runtimeClass).
	// OnImageDigestChange (default true) triggers when the image reference changes (tag bump or
	// digest-pinned image update). Same-tag digest changes require maxAge or force-rebuild.
	ci, err := sc.getCodeInterpreter(ss.Namespace, ss.Spec.RuntimeRef.Name)
	if err == nil {
		onArgsChange := true
		if ss.Spec.Invalidation != nil && ss.Spec.Invalidation.OnArgsChange != nil {
			onArgsChange = *ss.Spec.Invalidation.OnArgsChange
		}
		onImageDigestChange := true
		if ss.Spec.Invalidation != nil && ss.Spec.Invalidation.OnImageDigestChange != nil {
			onImageDigestChange = *ss.Spec.Invalidation.OnImageDigestChange
		}
		// Per-node SpecHash and ImageRef are stored in Redis only (not in CRD status).
		// Read them to detect spec drift.
		currentSpecHash := computeSpecHashNoImage(ci)
		infos, redisErr := sc.storeClient.GetSnapshotNodes(ctx, ss.Namespace, ss.Name)
		if redisErr != nil {
			klog.Warningf("SnapshotController: shouldInvalidate %s/%s: cannot read Redis nodes: %v; skipping drift check",
				ss.Namespace, ss.Name, redisErr)
		}
		for _, info := range infos {
			if onArgsChange && info.SpecHash != "" && info.SpecHash != currentSpecHash {
				return fmt.Sprintf("spec changed (args/env/resources/runtimeClass): stored=%s current=%s",
					info.SpecHash, currentSpecHash)
			}
			if onImageDigestChange && info.ImageRef != "" && info.ImageRef != ci.Spec.Template.Image {
				return fmt.Sprintf("image changed: stored=%q current=%q", info.ImageRef, ci.Spec.Template.Image)
			}
			// RuntimeGeneration covers fields outside SpecHash (labels, annotations,
			// imagePullSecrets, imagePullPolicy). The restore path rejects entries with
			// stale generation; detect this here so the controller proactively rebuilds
			// instead of leaving ActiveMode=Snapshot while sessions silently cold-start.
			if info.RuntimeGeneration != 0 && info.RuntimeGeneration != ci.Generation {
				return fmt.Sprintf("CI generation changed: stored=%d current=%d (labels/annotations/imagePullSecrets may have changed)",
					info.RuntimeGeneration, ci.Generation)
			}
		}
	}

	return ""
}

// buildSnapshot enumerates eligible nodes and launches one build job per node concurrently.
func (sc *SnapshotController) buildSnapshot(ctx context.Context, ss *runtimev1alpha1.SnapStart) error {
	if err := sc.patchStatusForSnapStart(ctx, ss, runtimev1alpha1.SessionStartupModeCold,
		runtimev1alpha1.SnapshotPhaseCreating,
		"Snapshot template is being created. Sessions are cold starting.", nil); err != nil {
		return err
	}

	ci, err := sc.getCodeInterpreter(ss.Namespace, ss.Spec.RuntimeRef.Name)
	if err != nil {
		return fmt.Errorf("get CodeInterpreter %s/%s: %w", ss.Namespace, ss.Spec.RuntimeRef.Name, err)
	}

	// Issue 3: filter eligible nodes by runtime scheduling constraints (runtimeClass.scheduling.nodeSelector).
	nodes, err := sc.getEligibleNodesForCI(ctx, ci)
	if err != nil {
		return fmt.Errorf("list eligible nodes: %w", err)
	}
	if len(nodes) == 0 {
		return sc.patchStatusForSnapStart(ctx, ss, runtimev1alpha1.SessionStartupModeCold,
			runtimev1alpha1.SnapshotPhaseFailed,
			fmt.Sprintf("no eligible nodes found (label %s=true + runtime constraints required)", types.LabelKuasarSnapstart),
			nil)
	}

	var mu sync.Mutex
	var nodeErrs []error
	var wg sync.WaitGroup

	// Cap concurrent per-node builds to avoid overwhelming node-local resources on large clusters.
	const maxConcurrentBuilds = 10
	sem := make(chan struct{}, maxConcurrentBuilds)

	for _, node := range nodes {
		wg.Add(1)
		go func(n *corev1.Node) {
			defer wg.Done()
			sem <- struct{}{}
			defer func() { <-sem }()
			if buildErr := sc.buildSnapshotOnNode(ctx, ss, ci, n); buildErr != nil {
				mu.Lock()
				nodeErrs = append(nodeErrs, fmt.Errorf("node %s: %w", n.Name, buildErr))
				mu.Unlock()
			}
		}(node)
	}
	wg.Wait()

	// Check if any node reported ProtocolNotSupported — this is permanent, stop retrying.
	for _, e := range nodeErrs {
		if errors.Is(e, errProtocolNotSupported) {
			protocolCond := []metav1.Condition{{
				Type:               "ProtocolNotSupported",
				Status:             metav1.ConditionTrue,
				Reason:             "RuntimeStatusEndpointMissing",
				Message:            "runtime does not implement GET /runtime/status; snapshot build will not be retried",
				LastTransitionTime: metav1.Now(),
			}}
			return sc.patchStatusForSnapStart(ctx, ss, runtimev1alpha1.SessionStartupModeCold,
				runtimev1alpha1.SnapshotPhaseFailed,
				"ProtocolNotSupported: runtime does not implement /runtime/status",
				protocolCond)
		}
	}

	successCount := len(nodes) - len(nodeErrs)

	if successCount == 0 {
		msg := errors.Join(nodeErrs...).Error()
		sc.emitEvent(ss, corev1.EventTypeWarning, "SnapshotFailed",
			"Snapshot build failed on all %d nodes: %s", len(nodes), msg)
		if patchErr := sc.recordRetryableBuildFailure(ctx, ss, msg); patchErr != nil {
			return patchErr
		}
		// Return an error so the workqueue also schedules a rate-limited retry. The persisted
		// NextBuildRetryAt gate above prevents informer updates from causing a hot loop.
		return fmt.Errorf("snapshot build failed on all %d eligible nodes: %s", len(nodes), msg)
	}

	var conditions []metav1.Condition
	if len(nodeErrs) > 0 {
		klog.Warningf("SnapshotController: %d/%d nodes failed for %s/%s: %v",
			len(nodeErrs), len(nodes), ss.Namespace, ss.Name, errors.Join(nodeErrs...))
		conditions = append(conditions, metav1.Condition{
			Type:               "Degraded",
			Status:             metav1.ConditionTrue,
			Reason:             "PartialNodeFailure",
			Message:            fmt.Sprintf("readyNodes=%d eligibleNodes=%d", successCount, len(nodes)),
			LastTransitionTime: metav1.Now(),
		})
	}

	sc.emitEvent(ss, corev1.EventTypeNormal, "SnapshotReady",
		"WarmForkSnapshot ready on %d/%d nodes; new sessions will use fast startup (~0.5-2s)",
		successCount, len(nodes))
	return sc.patchReadyStatusForSnapStart(ctx, ss, true,
		runtimev1alpha1.SnapshotPhaseReady,
		"Snapshot ready. New sessions will use fast startup (~0.5-2s).",
		conditions)
}

// buildSnapshotIncremental builds snapshots only on eligible nodes that have no Redis entry
// or whose previous build failed and is due for a retry. The overall phase stays Ready so
// existing sessions are not disrupted.
func (sc *SnapshotController) buildSnapshotIncremental(ctx context.Context, ss *runtimev1alpha1.SnapStart) error {
	ci, err := sc.getCodeInterpreter(ss.Namespace, ss.Spec.RuntimeRef.Name)
	if err != nil {
		return fmt.Errorf("get CodeInterpreter: %w", err)
	}

	eligible, err := sc.getEligibleNodesForCI(ctx, ci)
	if err != nil {
		return fmt.Errorf("list eligible nodes: %w", err)
	}

	infos, _ := sc.storeClient.GetSnapshotNodes(ctx, ss.Namespace, ss.Name)
	tracked := make(map[string]*types.SnapshotInfo, len(infos))
	for _, info := range infos {
		tracked[info.NodeName] = info
	}

	var newNodes []*corev1.Node
	for _, n := range eligible {
		if snapshotPlacementNeedsBuild(tracked[n.Name], time.Now()) {
			newNodes = append(newNodes, n)
		}
	}
	if len(newNodes) == 0 {
		return nil
	}

	klog.Infof("SnapshotController: incremental build for %s/%s: %d new node(s)", ss.Namespace, ss.Name, len(newNodes))

	var mu sync.Mutex
	var nodeErrs []error
	var wg sync.WaitGroup
	const maxConcurrentBuilds = 10
	sem := make(chan struct{}, maxConcurrentBuilds)

	for _, node := range newNodes {
		wg.Add(1)
		go func(n *corev1.Node) {
			defer wg.Done()
			sem <- struct{}{}
			defer func() { <-sem }()
			if buildErr := sc.buildSnapshotOnNode(ctx, ss, ci, n); buildErr != nil {
				mu.Lock()
				nodeErrs = append(nodeErrs, fmt.Errorf("node %s: %w", n.Name, buildErr))
				mu.Unlock()
			}
		}(node)
	}
	wg.Wait()

	if len(nodeErrs) > 0 {
		klog.Warningf("SnapshotController: incremental build %s/%s: %d/%d new nodes failed: %v",
			ss.Namespace, ss.Name, len(nodeErrs), len(newNodes), errors.Join(nodeErrs...))
	}

	var conditions []metav1.Condition
	if len(nodeErrs) > 0 {
		conditions = append(conditions, metav1.Condition{
			Type:               "Degraded",
			Status:             metav1.ConditionTrue,
			Reason:             "PartialNodeFailure",
			Message:            fmt.Sprintf("%d/%d incremental node builds failed", len(nodeErrs), len(newNodes)),
			LastTransitionTime: metav1.Now(),
		})
	}
	// Refresh aggregate counts; keep phase=Ready to avoid disrupting existing sessions.
	return sc.patchReadyStatusForSnapStart(ctx, ss, false,
		runtimev1alpha1.SnapshotPhaseReady,
		"Snapshot ready. New sessions will use fast startup (~0.5-2s).",
		conditions)
}

func snapshotPlacementNeedsBuild(info *types.SnapshotInfo, now time.Time) bool {
	return info == nil || (info.CacheState == string(runtimev1alpha1.CacheStateFailed) &&
		(info.FailedAt.IsZero() || now.Sub(info.FailedAt) >= incrementalRetryDelay))
}

// buildSnapshotOnNode performs the full per-node snapshot build workflow.
func (sc *SnapshotController) buildSnapshotOnNode(
	ctx context.Context,
	ss *runtimev1alpha1.SnapStart,
	ci *runtimev1alpha1.CodeInterpreter,
	node *corev1.Node,
) (retErr error) {
	start := time.Now()
	defer func() {
		outcome := "success"
		if retErr != nil {
			outcome = "failure"
		}
		snapshotBuildDuration.WithLabelValues(ss.Namespace, ss.Spec.RuntimeRef.Name, outcome).
			Observe(time.Since(start).Seconds())
	}()

	nodeIP := getNodeInternalIP(node)

	sc.emitEvent(ss, corev1.EventTypeNormal, "SnapshotBuildStarted",
		"Starting WarmForkSnapshot build on node %s", node.Name)

	// Step 1: Write CacheState=Building placeholder (two-phase write).
	// If this fails, abort: proceeding without a Redis record would create an invisible build
	// that can never be tracked, cleaned up, or timed out by the stale-build GC.
	if err := sc.storeClient.StoreSnapshot(ctx, ss.Namespace, ss.Name, &types.SnapshotInfo{
		SnapStartUID: string(ss.UID),
		CacheState:   string(runtimev1alpha1.CacheStateBuilding),
		NodeName:     node.Name,
		NodeIP:       nodeIP,
		StartedAt:    time.Now(),
	}); err != nil {
		return fmt.Errorf("store Building placeholder for node %s: %w", node.Name, err)
	}

	// Step 2: Create template sandbox on this node
	sandbox, err := sc.buildTemplateSandbox(ss, ci, node)
	if err != nil {
		sc.markNodeFailed(ctx, ss, node)
		return fmt.Errorf("build template sandbox: %w", err)
	}
	sandboxUnstructured, err := sandboxToUnstructured(sandbox)
	if err != nil {
		sc.markNodeFailed(ctx, ss, node)
		return fmt.Errorf("convert sandbox: %w", err)
	}
	if _, err = sc.dynamicClient.Resource(SandboxGVR).Namespace(ss.Namespace).Create(
		ctx, sandboxUnstructured, metav1.CreateOptions{}); err != nil {
		sc.markNodeFailed(ctx, ss, node)
		return fmt.Errorf("create template sandbox: %w", err)
	}

	// Ensure sandbox cleanup regardless of outcome
	defer func() {
		delCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		_ = sc.dynamicClient.Resource(SandboxGVR).Namespace(ss.Namespace).Delete(
			delCtx, sandbox.Name, metav1.DeleteOptions{})
	}()

	// Step 3: Wait for sandbox Running
	sandboxRunning, err := sc.waitSandboxRunning(ctx, ss.Namespace, sandbox.Name)
	if err != nil {
		sc.markNodeFailed(ctx, ss, node)
		return fmt.Errorf("wait sandbox running: %w", err)
	}

	// Step 4: Resolve image digest from pod status
	imageDigest, err := sc.resolveImageDigest(ctx, sandboxRunning)
	if err != nil {
		sc.markNodeFailed(ctx, ss, node)
		return fmt.Errorf("resolve image digest: %w", err)
	}

	// Compute template key
	checkpoint := ss.Spec.Checkpoint
	if checkpoint == "" {
		checkpoint = "InterpreterReady"
	}
	templateKey := computeTemplateKey(imageDigest, checkpoint, ci)

	// Get sandbox pod IP for probing /runtime/status
	podName := sandbox.Name
	if podNameFromAnnotation, exists := sandboxRunning.Annotations["agents.x-k8s.io/sandbox-pod-name"]; exists {
		podName = podNameFromAnnotation
	}
	podIP, err := sc.k8sClient.GetSandboxPodIP(ctx, ss.Namespace, sandbox.Name, podName)
	if err != nil {
		sc.markNodeFailed(ctx, ss, node)
		return fmt.Errorf("get sandbox pod IP: %w", err)
	}

	// Step 5: Poll /runtime/status until safeToSnapshot=true.
	// errProtocolNotSupported means the runtime doesn't implement the protocol at all;
	// surface it as a permanent ProtocolNotSupported condition so the controller stops retrying.
	statusURL := fmt.Sprintf("http://%s:8080/runtime/status", podIP)
	if err := sc.waitSafeToSnapshot(ctx, statusURL, checkpoint); err != nil {
		sc.markNodeFailed(ctx, ss, node)
		if errors.Is(err, errProtocolNotSupported) {
			return fmt.Errorf("%w", errProtocolNotSupported)
		}
		if strings.Contains(err.Error(), "userStateLoaded") || strings.Contains(err.Error(), "activeTasks") {
			sc.emitEvent(ss, corev1.EventTypeWarning, "SnapshotContaminationDetected",
				"Snapshot build refused on node %s: runtime has user state or active tasks", node.Name)
		}
		return fmt.Errorf("wait safe to snapshot: %w", err)
	}

	// Step 6: Call Kuasar Admin API to create template
	kuasarClient := sc.newKuasarClient(ctx, nodeIP)
	owner := map[string]string{
		"namespace":      ss.Namespace,
		"runtime_name":   ss.Spec.RuntimeRef.Name,
		"snap_start_uid": string(ss.UID),
		"node_name":      node.Name,
	}
	templateID, err := kuasarClient.CreateTemplate(ctx, sandbox.Name, templateKey, owner)
	if err != nil {
		sc.markNodeFailed(ctx, ss, node)
		return fmt.Errorf("create template: %w", err)
	}

	// Step 7: Write CacheState=LocalReady to Redis (two-phase write complete).
	// All version fields are mandatory for restore selection; Workload Manager rejects
	// any entry with empty version fields to block stale pre-upgrade placements.
	specHash := computeSpecHashNoImage(ci)
	resolvedCheckpoint := checkpoint
	if err := sc.storeClient.StoreSnapshot(ctx, ss.Namespace, ss.Name, &types.SnapshotInfo{
		TemplateID:        templateID,
		TemplateKey:       templateKey,
		SnapStartUID:      string(ss.UID),
		CacheState:        string(runtimev1alpha1.CacheStateLocalReady),
		SpecHash:          specHash,
		ImageRef:          ci.Spec.Template.Image,
		ImageDigest:       imageDigest,
		Checkpoint:        resolvedCheckpoint,
		ProtocolVersion:   "1",
		RuntimeGeneration: ci.Generation,
		NodeName:          node.Name,
		NodeIP:            nodeIP,
		CreatedAt:         time.Now(),
	}); err != nil {
		// Template exists in Kuasar but cannot be tracked in Redis.
		// Mark this node as Failed so restores never select it;
		// the orphan GC will delete the dangling Kuasar template on the next sweep.
		sc.markNodeFailed(ctx, ss, node)
		return fmt.Errorf("store Ready snapshot metadata for node %s: %w", node.Name, err)
	}

	klog.Infof("SnapshotController: snapshot ready on node %s for %s/%s (templateID=%s, key=%s)",
		node.Name, ss.Namespace, ss.Spec.RuntimeRef.Name, templateID, templateKey)
	// Step 8: template sandbox is deleted by the deferred cleanup above.
	return nil
}

// markNodeFailed marks a node's snapshot CacheState as Failed in Redis.
func (sc *SnapshotController) markNodeFailed(ctx context.Context, ss *runtimev1alpha1.SnapStart, node *corev1.Node) {
	_ = sc.storeClient.StoreSnapshot(ctx, ss.Namespace, ss.Name, &types.SnapshotInfo{
		SnapStartUID: string(ss.UID),
		CacheState:   string(runtimev1alpha1.CacheStateFailed),
		NodeName:     node.Name,
		NodeIP:       getNodeInternalIP(node),
		FailedAt:     time.Now(),
	})
}

// buildTemplateSandbox creates a Sandbox object for snapshot construction on a specific node.
func (sc *SnapshotController) buildTemplateSandbox(
	ss *runtimev1alpha1.SnapStart,
	ci *runtimev1alpha1.CodeInterpreter,
	node *corev1.Node,
) (*sandboxv1alpha1.Sandbox, error) {
	sandboxName := fmt.Sprintf("%s-snapshot-%s", ci.Name, RandString(6))
	sessionID := fmt.Sprintf("snapshot-%s", RandString(8))

	if err := validateSnapshotTemplateEnv(ci.Spec.Template.Environment); err != nil {
		return nil, err
	}
	envVars := buildCodeInterpreterEnvVars(ci.Spec.Template.Environment, ci.Spec.AuthMode)
	envVars = append(envVars, corev1.EnvVar{
		Name:  "AGENTCUBE_SNAPSTART_BUILD",
		Value: "true",
	})

	runtimeClassName := ci.Spec.Template.RuntimeClassName
	if runtimeClassName != nil && *runtimeClassName == "" {
		runtimeClassName = nil
	}

	pullPolicy := ci.Spec.Template.ImagePullPolicy

	podSpec := corev1.PodSpec{
		NodeName:         node.Name,
		RuntimeClassName: runtimeClassName,
		ImagePullSecrets: ci.Spec.Template.ImagePullSecrets,
		Containers: []corev1.Container{
			{
				Name:            "code-interpreter",
				Image:           ci.Spec.Template.Image,
				ImagePullPolicy: pullPolicy,
				Env:             envVars,
				Command:         ci.Spec.Template.Command,
				Args:            ci.Spec.Template.Args,
				Resources:       ci.Spec.Template.Resources,
			},
		},
	}

	shutdownTime := metav1.NewTime(time.Now().Add(30 * time.Minute))
	return &sandboxv1alpha1.Sandbox{
		TypeMeta: metav1.TypeMeta{
			APIVersion: "agents.x-k8s.io/v1alpha1",
			Kind:       "Sandbox",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:      sandboxName,
			Namespace: ss.Namespace,
			Labels: map[string]string{
				SessionIdLabelKey:    sessionID,
				WorkloadNameLabelKey: ci.Name,
			},
			Annotations: map[string]string{
				types.AnnotationTemplateSandbox:              "true",
				"kuasar.io/warm-fork-ready-protocol-version": "1",
			},
		},
		Spec: sandboxv1alpha1.SandboxSpec{
			PodTemplate: sandboxv1alpha1.PodTemplate{
				Spec: podSpec,
				ObjectMeta: sandboxv1alpha1.PodMetadata{
					Labels: map[string]string{
						SessionIdLabelKey: sessionID,
					},
					Annotations: map[string]string{
						"kuasar.io/warm-fork-ready-protocol-version": "1",
					},
				},
			},
			Lifecycle: sandboxv1alpha1.Lifecycle{
				ShutdownTime: &shutdownTime,
			},
		},
	}, nil
}

// waitSandboxRunning polls until the sandbox reaches Running status.
func (sc *SnapshotController) waitSandboxRunning(ctx context.Context, namespace, sandboxName string) (*sandboxv1alpha1.Sandbox, error) {
	deadline := time.Now().Add(snapshotReadyProbeTimeout)
	for {
		if time.Now().After(deadline) {
			return nil, fmt.Errorf("timed out waiting for sandbox %s/%s to become running", namespace, sandboxName)
		}

		timer := time.NewTimer(sc.probeInterval)
		select {
		case <-ctx.Done():
			timer.Stop()
			return nil, ctx.Err()
		case <-timer.C:
		}

		obj, err := sc.dynamicClient.Resource(SandboxGVR).Namespace(namespace).Get(ctx, sandboxName, metav1.GetOptions{})
		if err != nil {
			if apierrors.IsNotFound(err) {
				return nil, fmt.Errorf("sandbox %s/%s not found", namespace, sandboxName)
			}
			klog.V(4).Infof("SnapshotController: error getting sandbox %s/%s: %v", namespace, sandboxName, err)
			continue
		}

		var sandbox sandboxv1alpha1.Sandbox
		if err := unstructuredToSandbox(obj, &sandbox); err != nil {
			klog.V(4).Infof("SnapshotController: error converting sandbox: %v", err)
			continue
		}

		if getSandboxStatus(&sandbox) == sandboxStatusReady {
			return &sandbox, nil
		}
	}
}

// resolveImageDigest reads the actual image digest from a running sandbox's pod status.
// Returns an error if the digest cannot be resolved; callers must not proceed with
// an untrusted "unknown" key that would make different images share the same template.
func (sc *SnapshotController) resolveImageDigest(ctx context.Context, sandbox *sandboxv1alpha1.Sandbox) (string, error) {
	podName := sandbox.Name
	if n, ok := sandbox.Annotations["agents.x-k8s.io/sandbox-pod-name"]; ok {
		podName = n
	}
	pod, err := sc.clientset.CoreV1().Pods(sandbox.Namespace).Get(ctx, podName, metav1.GetOptions{})
	if err != nil {
		return "", fmt.Errorf("get pod %s/%s to resolve image digest: %w", sandbox.Namespace, podName, err)
	}
	for _, cs := range pod.Status.ContainerStatuses {
		if cs.ImageID != "" {
			// imageID format: <registry>/<image>@sha256:<hash>
			parts := strings.Split(cs.ImageID, "@sha256:")
			if len(parts) == 2 {
				digest := parts[1]
				if len(digest) >= 16 {
					return "sha256-" + digest[:16], nil
				}
				return "sha256-" + digest, nil
			}
			// Non-standard imageID: refuse rather than truncate to 16 chars.
			// A truncated prefix could collide between two different images, producing a
			// template key that silently maps multiple images to the same snapshot.
			return "", fmt.Errorf("imageID %q does not match expected '<image>@sha256:<hash>' format; "+
				"cannot derive a unique template key", cs.ImageID)
		}
	}
	return "", fmt.Errorf("no imageID in containerStatuses for pod %s/%s (pod may not be Running yet)", sandbox.Namespace, podName)
}

// errProtocolNotSupported is returned by waitSafeToSnapshot when the runtime persistently
// returns 404 on /runtime/status, indicating it does not implement the SnapStart protocol.
// Unlike transient errors, this is permanent — callers must set ProtocolNotSupported condition
// and stop retrying.
var errProtocolNotSupported = fmt.Errorf("runtime does not implement /runtime/status (ProtocolNotSupported)")

// waitSafeToSnapshot polls the given statusURL until safeToSnapshot=true.
// The caller is responsible for constructing the URL (typically http://{podIP}:8080/runtime/status).
// Returns errProtocolNotSupported on persistent 404/500/bad-JSON so callers can
// distinguish permanent protocol errors from transient failures.
func (sc *SnapshotController) waitSafeToSnapshot(ctx context.Context, statusURL, expectedCheckpoint string) error {
	url := statusURL
	deadline := time.Now().Add(snapshotReadyProbeTimeout)
	httpClient := &http.Client{Timeout: 5 * time.Second}
	consecutiveNotFound := 0
	consecutiveProtoErrors := 0

	for {
		if time.Now().After(deadline) {
			return fmt.Errorf("timed out waiting for safeToSnapshot=true at %s", url)
		}

		timer := time.NewTimer(sc.probeInterval)
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-timer.C:
		}

		reqCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
		req, err := http.NewRequestWithContext(reqCtx, http.MethodGet, url, nil)
		if err != nil {
			cancel()
			continue
		}

		httpResp, err := httpClient.Do(req)
		cancel()
		if err != nil {
			klog.V(4).Infof("SnapshotController: probe %s: %v", url, err)
			consecutiveNotFound = 0 // network error is transient, reset counter
			consecutiveProtoErrors = 0
			continue
		}

		if httpResp.StatusCode == http.StatusNotFound {
			klog.V(2).Infof("SnapshotController: /runtime/status returned 404 on %s (%d/%d)",
				url, consecutiveNotFound+1, notFoundThreshold)
			httpResp.Body.Close()
			consecutiveNotFound++
			if consecutiveNotFound >= notFoundThreshold {
				return errProtocolNotSupported
			}
			continue
		}
		consecutiveNotFound = 0

		if httpResp.StatusCode != http.StatusOK {
			httpResp.Body.Close()
			consecutiveProtoErrors++
			if consecutiveProtoErrors >= protoErrorThreshold {
				return fmt.Errorf("%w: persistent non-200 status %d", errProtocolNotSupported, httpResp.StatusCode)
			}
			continue
		}

		body, err := io.ReadAll(httpResp.Body)
		httpResp.Body.Close()
		if err != nil {
			continue
		}

		var status runtimeStatusResponse
		if err := json.Unmarshal(body, &status); err != nil {
			consecutiveProtoErrors++
			if consecutiveProtoErrors >= protoErrorThreshold {
				return fmt.Errorf("%w: persistent JSON parse failure on /runtime/status", errProtocolNotSupported)
			}
			continue
		}
		consecutiveProtoErrors = 0

		if status.Checkpoint == expectedCheckpoint && status.SafeToSnapshot {
			// Reject snapshots with user state or active tasks — they would contaminate
			// every future session restored from this template.
			if status.UserStateLoaded {
				return fmt.Errorf("refusing snapshot: userStateLoaded=true indicates runtime has user-specific state that would contaminate the snapshot")
			}
			if status.ActiveTasks > 0 {
				return fmt.Errorf("refusing snapshot: activeTasks=%d > 0 indicates active work in progress", status.ActiveTasks)
			}
			return nil
		}
		klog.V(4).Infof("SnapshotController: waiting for safeToSnapshot: checkpoint=%s (want %s), safeToSnapshot=%v",
			status.Checkpoint, expectedCheckpoint, status.SafeToSnapshot)
	}
}

// cleanupSnapshot deletes all Redis metadata and Kuasar templates for a SnapStart.
func (sc *SnapshotController) cleanupSnapshot(ctx context.Context, namespace, snapStartName string) error {
	infos, err := sc.storeClient.GetSnapshotNodes(ctx, namespace, snapStartName)
	if err != nil {
		return fmt.Errorf("get snapshot nodes: %w", err)
	}

	// Step 1: Delete Redis metadata to stop new restores
	if err := sc.storeClient.DeleteAllSnapshots(ctx, namespace, snapStartName); err != nil {
		return fmt.Errorf("delete all snapshots from store: %w", err)
	}

	// Step 2: Delete templates on each node
	for _, info := range infos {
		if info.TemplateID == "" {
			continue
		}
		if err := sc.waitAndDeleteTemplate(ctx, info); err != nil {
			klog.Errorf("SnapshotController: cleanup failed for template %s on %s: %v",
				info.TemplateID, info.NodeIP, err)
			// Non-fatal: orphan GC will clean up
		}
	}
	return nil
}

// waitAndDeleteTemplate retries deleting a template until lease_count reaches zero.
func (sc *SnapshotController) waitAndDeleteTemplate(ctx context.Context, info *types.SnapshotInfo) error {
	deadline := time.Now().Add(maxSessionTTL)
	backoff := wait.Backoff{
		Duration: 10 * time.Second,
		Factor:   1.5,
		Jitter:   0.2,
		Steps:    20,
	}
	kuasarClient := sc.newKuasarClient(ctx, info.NodeIP)
	return wait.ExponentialBackoffWithContext(ctx, backoff, func(ctx context.Context) (bool, error) {
		if time.Now().After(deadline) {
			klog.Warningf("SnapshotController: lease still held after %v for template %s; orphan GC will clean up",
				maxSessionTTL, info.TemplateID)
			return true, nil
		}
		deleteErr := kuasarClient.DeleteTemplate(ctx, info.TemplateID)
		if deleteErr == nil {
			return true, nil
		}
		if isTemplateInUse(deleteErr) {
			return false, nil
		}
		if isNotFound(deleteErr) {
			return true, nil
		}
		return false, nil
	})
}

// handleDeletion cleans up snapshot resources and removes the finalizer.
func (sc *SnapshotController) handleDeletion(ctx context.Context, ss *runtimev1alpha1.SnapStart) error {
	if !hasFinalizer(ss.Finalizers, types.SnapshotFinalizer) {
		return nil
	}
	if err := sc.cleanupSnapshot(ctx, ss.Namespace, ss.Name); err != nil {
		return fmt.Errorf("cleanup snapshot: %w", err)
	}
	sc.indexer.remove(ss)
	return sc.removeFinalizer(ctx, ss)
}

// getEligibleNodes returns Ready nodes with the kuasar-snapstart label.
// Used by orphan GC which needs all Kuasar nodes regardless of runtime constraints.
func (sc *SnapshotController) getEligibleNodes(ctx context.Context) ([]*corev1.Node, error) {
	nodeList, err := sc.clientset.CoreV1().Nodes().List(ctx, metav1.ListOptions{
		LabelSelector: types.LabelKuasarSnapstart + "=true",
	})
	if err != nil {
		return nil, fmt.Errorf("list nodes: %w", err)
	}
	var eligible []*corev1.Node
	for i := range nodeList.Items {
		if isNodeReady(&nodeList.Items[i]) {
			eligible = append(eligible, &nodeList.Items[i])
		}
	}
	return eligible, nil
}

// getEligibleNodesForCI returns Ready kuasar-snapstart nodes that also satisfy the
// runtime's scheduling constraints (runtimeClassName → RuntimeClass.scheduling.nodeSelector).
// The build node must be a node where the runtime pod can also be scheduled; a snapshot
// built on an ineligible node would never be used by a restore sandbox.
func (sc *SnapshotController) getEligibleNodesForCI(ctx context.Context, ci *runtimev1alpha1.CodeInterpreter) ([]*corev1.Node, error) {
	base, err := sc.getEligibleNodes(ctx)
	if err != nil {
		return nil, err
	}

	if ci.Spec.Template.RuntimeClassName == nil || *ci.Spec.Template.RuntimeClassName == "" {
		return base, nil
	}

	rc, err := sc.clientset.NodeV1().RuntimeClasses().Get(ctx, *ci.Spec.Template.RuntimeClassName, metav1.GetOptions{})
	if err != nil {
		if apierrors.IsNotFound(err) {
			// RuntimeClass not defined: treat as having no scheduling constraints.
			return base, nil
		}
		// API error: return error so the caller can decide whether to block or degrade.
		return nil, fmt.Errorf("get RuntimeClass %q for node eligibility filter: %w", *ci.Spec.Template.RuntimeClassName, err)
	}
	if rc.Scheduling == nil || len(rc.Scheduling.NodeSelector) == 0 {
		return base, nil
	}

	var eligible []*corev1.Node
	for _, node := range base {
		if nodeMatchesLabelSelector(node, rc.Scheduling.NodeSelector) {
			eligible = append(eligible, node)
		}
	}
	return eligible, nil
}

// nodeMatchesLabelSelector returns true if all key=value pairs in selector are present in node labels.
func nodeMatchesLabelSelector(node *corev1.Node, selector map[string]string) bool {
	for k, v := range selector {
		if node.Labels[k] != v {
			return false
		}
	}
	return true
}

// isNodeReady returns true if the node condition Ready=True.
func isNodeReady(node *corev1.Node) bool {
	for _, c := range node.Status.Conditions {
		if c.Type == corev1.NodeReady {
			return c.Status == corev1.ConditionTrue
		}
	}
	return false
}

// getNodeInternalIP returns the internal IP address of the node.
func getNodeInternalIP(node *corev1.Node) string {
	for _, addr := range node.Status.Addresses {
		if addr.Type == corev1.NodeInternalIP {
			return addr.Address
		}
	}
	return ""
}

// computeTemplateKey computes the WarmFork template key.
// format: fork:{image_digest_short}:{checkpoint}:{spec_hash}
// imageDigest must be the resolved digest (from resolveImageDigest), not a mutable tag.
// spec_hash is computed from the full resolved digest to include image identity.
func computeTemplateKey(imageDigest, checkpoint string, ci *runtimev1alpha1.CodeInterpreter) string {
	specHash := computeSpecHashWithImage(imageDigest, ci)
	return fmt.Sprintf("fork:%s:%s:%s", imageDigest, checkpoint, specHash)
}

type specHashEnv struct {
	Name  string `json:"name"`
	Value string `json:"value"`
}

type specHashInput struct {
	Image        string        `json:"image"`
	Command      []string      `json:"command"`
	Args         []string      `json:"args"`
	Env          []specHashEnv `json:"env"`
	AuthMode     string        `json:"authMode"`
	Resources    interface{}   `json:"resources"`
	RuntimeClass string        `json:"runtimeClass"`
}

// computeSpecHashWithImage computes a content hash that includes the resolved image digest.
// Used when building a templateKey; requires a live digest (not a mutable tag).
func computeSpecHashWithImage(imageDigest string, ci *runtimev1alpha1.CodeInterpreter) string {
	envs := collectSortedEnvs(ci)
	runtimeClass := ""
	if ci.Spec.Template.RuntimeClassName != nil {
		runtimeClass = *ci.Spec.Template.RuntimeClassName
	}
	input := specHashInput{
		Image:        imageDigest,
		Command:      ci.Spec.Template.Command,
		Args:         ci.Spec.Template.Args,
		Env:          envs,
		AuthMode:     string(ci.Spec.AuthMode),
		Resources:    ci.Spec.Template.Resources,
		RuntimeClass: runtimeClass,
	}
	b, _ := json.Marshal(input)
	h := sha256.Sum256(b)
	return fmt.Sprintf("spec-%x", h[:8])
}

// computeSpecHashNoImage computes a content hash of the non-image spec fields only.
// Used for reconcile-time drift detection: unlike computeSpecHashWithImage, this can
// be re-computed from the current CI without requiring a live image resolve.
func computeSpecHashNoImage(ci *runtimev1alpha1.CodeInterpreter) string {
	envs := collectSortedEnvs(ci)
	runtimeClass := ""
	if ci.Spec.Template.RuntimeClassName != nil {
		runtimeClass = *ci.Spec.Template.RuntimeClassName
	}
	input := specHashInput{
		Image:        "", // intentionally omitted; captured separately in templateKey prefix
		Command:      ci.Spec.Template.Command,
		Args:         ci.Spec.Template.Args,
		Env:          envs,
		AuthMode:     string(ci.Spec.AuthMode),
		Resources:    ci.Spec.Template.Resources,
		RuntimeClass: runtimeClass,
	}
	b, _ := json.Marshal(input)
	h := sha256.Sum256(b)
	return fmt.Sprintf("spec-%x", h[:8])
}

func collectSortedEnvs(ci *runtimev1alpha1.CodeInterpreter) []specHashEnv {
	envs := make([]specHashEnv, 0, len(ci.Spec.Template.Environment))
	for _, e := range ci.Spec.Template.Environment {
		envs = append(envs, specHashEnv{Name: e.Name, Value: e.Value})
	}
	sort.Slice(envs, func(i, j int) bool { return envs[i].Name < envs[j].Name })
	return envs
}

// patchStatusForSnapStart patches the SnapStart status subresource with full state including
// per-node placement (read from Redis using the SnapStart name as key), artifact type, and conditions.
// conditions may be nil.
func (sc *SnapshotController) patchStatusForSnapStart(
	ctx context.Context,
	ss *runtimev1alpha1.SnapStart,
	activeMode runtimev1alpha1.SessionStartupMode,
	phase runtimev1alpha1.SnapshotPhase,
	msg string,
	conditions []metav1.Condition,
) error {
	return sc.patchStatusByName(ctx, ss.Namespace, ss.Name, activeMode, phase, msg, conditions, false)
}

func (sc *SnapshotController) patchReadyStatusForSnapStart(
	ctx context.Context,
	ss *runtimev1alpha1.SnapStart,
	resetReadyState bool,
	phase runtimev1alpha1.SnapshotPhase,
	msg string,
	conditions []metav1.Condition,
) error {
	return sc.patchStatusByName(ctx, ss.Namespace, ss.Name, runtimev1alpha1.SessionStartupModeSnapshot,
		phase, msg, conditions, resetReadyState)
}

func (sc *SnapshotController) patchStatusByName(
	ctx context.Context,
	namespace, snapStartName string,
	activeMode runtimev1alpha1.SessionStartupMode,
	phase runtimev1alpha1.SnapshotPhase,
	msg string,
	conditions []metav1.Condition,
	resetReadyState bool,
) error {
	// Read per-node data from Redis to compute aggregate counts for CRD status.
	snapshotInfos, err := sc.storeClient.GetSnapshotNodes(ctx, namespace, snapStartName)
	if err != nil {
		klog.Warningf("SnapshotController: patchStatus %s/%s: GetSnapshotNodes failed: %v; counts will be zero in status",
			namespace, snapStartName, err)
	}

	eligible, ready, failed, unavailable := countSnapshotNodes(snapshotInfos)
	snapshotPatch := map[string]interface{}{
		"phase":            string(phase),
		"artifact":         map[string]interface{}{"distribution": string(runtimev1alpha1.DistributionModeNodeLocal)},
		"eligibleNodes":    eligible,
		"readyNodes":       ready,
		"failedNodes":      failed,
		"unavailableNodes": unavailable,
	}
	// Always write failedReason so stale values are cleared when transitioning away from Failed.
	if phase == runtimev1alpha1.SnapshotPhaseFailed {
		snapshotPatch["failedReason"] = msg
	} else {
		snapshotPatch["failedReason"] = ""
	}
	if phase == runtimev1alpha1.SnapshotPhaseReady && resetReadyState {
		snapshotPatch["readyAt"] = time.Now().UTC().Format(time.RFC3339)
		snapshotPatch["restoreFailureCount"] = 0 // reset counter after a successful rebuild
		snapshotPatch["buildFailureCount"] = 0
		snapshotPatch["nextBuildRetryAt"] = nil
	}

	statusMap := map[string]interface{}{
		"activeMode": string(activeMode),
		"message":    msg,
		"snapshot":   snapshotPatch,
		// Always write conditions so stale Degraded/ProtocolNotSupported entries are cleared
		// when the SnapStart transitions to a clean state. JSON Merge Patch does not remove
		// array elements by omission; we must send the new (possibly empty) slice explicitly.
		"conditions": conditionsToInterface(conditions),
	}

	patchBytes, err := json.Marshal(map[string]interface{}{"status": statusMap})
	if err != nil {
		return fmt.Errorf("marshal status patch: %w", err)
	}

	_, err = sc.dynamicClient.Resource(SnapStartGVR).Namespace(namespace).
		Patch(ctx, snapStartName, k8stypes.MergePatchType, patchBytes, metav1.PatchOptions{}, "status")
	if err != nil {
		return fmt.Errorf("patch SnapStart status %s/%s: %w", namespace, snapStartName, err)
	}
	return nil
}

func (sc *SnapshotController) recordRetryableBuildFailure(ctx context.Context, ss *runtimev1alpha1.SnapStart, msg string) error {
	count := int32(1)
	if ss.Status.Snapshot != nil {
		count += ss.Status.Snapshot.BuildFailureCount
	}
	delay := 10 * time.Second * time.Duration(1<<(count-1))
	retryAt := time.Now().Add(delay).UTC().Format(time.RFC3339)
	if count >= 3 {
		retryAt = ""
	}
	snapshotPatch := map[string]interface{}{
		"phase":             string(runtimev1alpha1.SnapshotPhaseFailed),
		"failedReason":      msg,
		"buildFailureCount": count,
	}
	if retryAt == "" {
		snapshotPatch["nextBuildRetryAt"] = nil
	} else {
		snapshotPatch["nextBuildRetryAt"] = retryAt
	}
	patchBytes, err := json.Marshal(map[string]interface{}{
		"status": map[string]interface{}{
			"activeMode": string(runtimev1alpha1.SessionStartupModeCold),
			"message":    msg,
			"snapshot":   snapshotPatch,
		},
	})
	if err != nil {
		return err
	}
	_, err = sc.dynamicClient.Resource(SnapStartGVR).Namespace(ss.Namespace).
		Patch(ctx, ss.Name, k8stypes.MergePatchType, patchBytes, metav1.PatchOptions{}, "status")
	return err
}

func (sc *SnapshotController) resetBuildRetryState(ctx context.Context, ss *runtimev1alpha1.SnapStart) error {
	patchBytes, err := json.Marshal(map[string]interface{}{
		"status": map[string]interface{}{
			"snapshot": map[string]interface{}{
				"buildFailureCount": 0,
				"nextBuildRetryAt":  nil,
			},
		},
	})
	if err != nil {
		return err
	}
	_, err = sc.dynamicClient.Resource(SnapStartGVR).Namespace(ss.Namespace).
		Patch(ctx, ss.Name, k8stypes.MergePatchType, patchBytes, metav1.PatchOptions{}, "status")
	return err
}

// countSnapshotNodes derives aggregate node counts from Redis SnapshotInfo records.
// CRD status stores only these four integers; per-node details remain in Redis only.
// Unavailable/Invalidated nodes are NOT counted as eligible — they have left the active pool.
func countSnapshotNodes(infos []*types.SnapshotInfo) (eligible, ready, failed, unavailable int32) {
	for _, info := range infos {
		switch runtimev1alpha1.CacheState(info.CacheState) {
		case runtimev1alpha1.CacheStateLocalReady:
			eligible++
			ready++
		case runtimev1alpha1.CacheStateFailed:
			eligible++
			failed++
		case runtimev1alpha1.CacheStateBuilding:
			eligible++
		case runtimev1alpha1.CacheStateUnavailable, runtimev1alpha1.CacheStateInvalidated:
			unavailable++
		}
	}
	return
}

// conditionsToInterface converts conditions to JSON-serialisable interface{} for merge patch.
func conditionsToInterface(conditions []metav1.Condition) interface{} {
	b, _ := json.Marshal(conditions)
	var out interface{}
	_ = json.Unmarshal(b, &out)
	return out
}

// addFinalizer adds the snapshot finalizer to a SnapStart.
func (sc *SnapshotController) addFinalizer(ctx context.Context, ss *runtimev1alpha1.SnapStart) error {
	newFinalizers := append(ss.Finalizers, types.SnapshotFinalizer) //nolint:gocritic
	patch := map[string]interface{}{
		"metadata": map[string]interface{}{
			"finalizers": newFinalizers,
		},
	}
	patchBytes, err := json.Marshal(patch)
	if err != nil {
		return err
	}
	_, err = sc.dynamicClient.Resource(SnapStartGVR).Namespace(ss.Namespace).
		Patch(ctx, ss.Name, k8stypes.MergePatchType, patchBytes, metav1.PatchOptions{})
	return err
}

// removeFinalizer removes the snapshot finalizer from a SnapStart.
func (sc *SnapshotController) removeFinalizer(ctx context.Context, ss *runtimev1alpha1.SnapStart) error {
	newFinalizers := make([]string, 0, len(ss.Finalizers))
	for _, f := range ss.Finalizers {
		if f != types.SnapshotFinalizer {
			newFinalizers = append(newFinalizers, f)
		}
	}
	patch := map[string]interface{}{
		"metadata": map[string]interface{}{
			"finalizers": newFinalizers,
		},
	}
	patchBytes, err := json.Marshal(patch)
	if err != nil {
		return err
	}
	_, err = sc.dynamicClient.Resource(SnapStartGVR).Namespace(ss.Namespace).
		Patch(ctx, ss.Name, k8stypes.MergePatchType, patchBytes, metav1.PatchOptions{})
	return err
}

// removeAnnotation removes a specific annotation from a SnapStart by JSON Merge Patch.
func (sc *SnapshotController) removeAnnotation(ctx context.Context, namespace, name, annotation string, currentAnnotations map[string]string) error {
	annotations := make(map[string]interface{}, len(currentAnnotations))
	for k, v := range currentAnnotations {
		if k != annotation {
			annotations[k] = v
		}
	}
	annotations[annotation] = nil // null in JSON Merge Patch means delete
	patch := map[string]interface{}{
		"metadata": map[string]interface{}{
			"annotations": annotations,
		},
	}
	patchBytes, err := json.Marshal(patch)
	if err != nil {
		return err
	}
	_, err = sc.dynamicClient.Resource(SnapStartGVR).Namespace(namespace).
		Patch(ctx, name, k8stypes.MergePatchType, patchBytes, metav1.PatchOptions{})
	return err
}

// hasFinalizer returns true if the finalizer is in the list.
func hasFinalizer(finalizers []string, finalizer string) bool {
	for _, f := range finalizers {
		if f == finalizer {
			return true
		}
	}
	return false
}

// getCodeInterpreter retrieves a CodeInterpreter from the informer cache.
func (sc *SnapshotController) getCodeInterpreter(namespace, name string) (*runtimev1alpha1.CodeInterpreter, error) {
	key := namespace + "/" + name
	obj, exists, err := sc.informers.CodeInterpreterInformer.GetStore().GetByKey(key)
	if err != nil {
		return nil, fmt.Errorf("get CodeInterpreter %s from informer: %w", key, err)
	}
	if !exists {
		return nil, fmt.Errorf("CodeInterpreter %s not found", key)
	}
	unstructuredObj, ok := obj.(*unstructured.Unstructured)
	if !ok {
		return nil, fmt.Errorf("unexpected type %T for CodeInterpreter", obj)
	}
	var ci runtimev1alpha1.CodeInterpreter
	b, err := unstructuredObj.MarshalJSON()
	if err != nil {
		return nil, err
	}
	if err := json.Unmarshal(b, &ci); err != nil {
		return nil, err
	}
	return &ci, nil
}

// syncNodeAvailability compares current eligible nodes with Redis entries and marks
// nodes that are no longer eligible (NotReady or label removed) as Unavailable.
// Called inside reconcile() so errors propagate through the existing backoff mechanism.
func (sc *SnapshotController) syncNodeAvailability(ctx context.Context, ss *runtimev1alpha1.SnapStart) bool {
	changed := false
	ci, err := sc.getCodeInterpreter(ss.Namespace, ss.Spec.RuntimeRef.Name)
	if err != nil {
		return false // CI gone; cleanup is handled by onRuntimeDeleted
	}

	// Build set of currently eligible and Ready nodes.
	eligible, err := sc.getEligibleNodesForCI(ctx, ci)
	if err != nil {
		klog.V(4).Infof("SnapshotController: syncNodeAvailability %s/%s: list eligible nodes: %v", ss.Namespace, ss.Name, err)
		return false
	}
	eligibleReady := make(map[string]bool, len(eligible))
	for _, n := range eligible {
		if isNodeReady(n) {
			eligibleReady[n.Name] = true
		}
	}

	infos, err := sc.storeClient.GetSnapshotNodes(ctx, ss.Namespace, ss.Name)
	if err != nil {
		return false
	}
	for _, info := range infos {
		if eligibleReady[info.NodeName] {
			// Node has returned to eligible+Ready. If it was previously marked Unavailable,
			// delete its Redis entry so it is treated as a new node and a fresh snapshot
			// build is triggered (the template may no longer exist after the node's absence).
			if runtimev1alpha1.CacheState(info.CacheState) == runtimev1alpha1.CacheStateUnavailable {
				if delErr := sc.storeClient.DeleteSnapshot(ctx, ss.Namespace, ss.Name, info.NodeName); delErr != nil {
					klog.Warningf("SnapshotController: syncNodeAvailability: clear recovered node %s for %s/%s: %v",
						info.NodeName, ss.Namespace, ss.Name, delErr)
				} else {
					changed = true
					klog.Infof("SnapshotController: node %s recovered for %s/%s; cleared stale Unavailable entry to trigger rebuild",
						info.NodeName, ss.Namespace, ss.Name)
				}
			}
			continue
		}
		switch runtimev1alpha1.CacheState(info.CacheState) {
		case runtimev1alpha1.CacheStateUnavailable, runtimev1alpha1.CacheStateBuilding:
			// already Unavailable or mid-build; leave as-is
		default:
			updated := *info
			updated.CacheState = string(runtimev1alpha1.CacheStateUnavailable)
			if storeErr := sc.storeClient.StoreSnapshot(ctx, ss.Namespace, ss.Name, &updated); storeErr != nil {
				klog.Warningf("SnapshotController: syncNodeAvailability: mark %s Unavailable for %s/%s: %v",
					info.NodeName, ss.Namespace, ss.Name, storeErr)
			} else {
				changed = true
				klog.Infof("SnapshotController: node %s is no longer eligible for %s/%s; marked Unavailable",
					info.NodeName, ss.Namespace, ss.Name)
			}
		}
	}
	return changed
}

// onRuntimeDeleted handles CodeInterpreter/AgentRuntime deletion events.
func (sc *SnapshotController) onRuntimeDeleted(ctx context.Context, namespace, name string) {
	snapStarts := sc.indexer.getByRuntime(namespace, name)
	for _, ss := range snapStarts {
		if cleanErr := sc.cleanupSnapshot(ctx, ss.Namespace, ss.Name); cleanErr != nil {
			klog.Errorf("SnapshotController: cleanup on runtime deletion %s/%s: %v", namespace, name, cleanErr)
		}
		if patchErr := sc.patchStatusForSnapStart(ctx, ss,
			runtimev1alpha1.SessionStartupModeCold, runtimev1alpha1.SnapshotPhaseFailed,
			"runtimeRef not found: referenced CodeInterpreter has been deleted", nil); patchErr != nil {
			klog.Errorf("SnapshotController: update status on runtime deletion %s/%s: %v", namespace, name, patchErr)
		}
	}
}

// runOrphanGC periodically sweeps stale Building entries and untracked Kuasar templates.
func (sc *SnapshotController) runOrphanGC(ctx context.Context) {
	ticker := time.NewTicker(orphanGCInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			sc.sweepStaleBuildingEntries(ctx)
			sc.sweepOrphanTemplates(ctx)
		case <-ctx.Done():
			return
		}
	}
}

// sweepStaleBuildingEntries resets any per-node snapshot entries that have been stuck in
// Building phase beyond buildingTimeout. These arise when the Workload Manager crashes
// mid-build, leaving an orphaned placeholder that would otherwise persist in Redis forever
// and prevent the node from ever being selected for restores.
func (sc *SnapshotController) sweepStaleBuildingEntries(ctx context.Context) {
	keys, err := sc.storeClient.ListAllSnapshotKeys(ctx)
	if err != nil {
		klog.Warningf("SnapshotController: stale-build GC: list snapshot keys: %v", err)
		return
	}
	cutoff := time.Now().Add(-buildingTimeout)
	for _, kv := range keys {
		ns, name := kv[0], kv[1]
		infos, err := sc.storeClient.GetSnapshotNodes(ctx, ns, name)
		if err != nil {
			klog.Warningf("SnapshotController: stale-build GC: get nodes for %s/%s: %v", ns, name, err)
			continue
		}
		for _, info := range infos {
			if info.CacheState != string(runtimev1alpha1.CacheStateBuilding) {
				continue
			}
			if info.StartedAt.IsZero() || !info.StartedAt.Before(cutoff) {
				continue
			}
			klog.Warningf("SnapshotController: stale-build GC: resetting stuck Building entry "+
				"node=%s snapstart=%s/%s started=%s (exceeded buildingTimeout of %s)",
				info.NodeName, ns, name, info.StartedAt.Format(time.RFC3339), buildingTimeout)
			if resetErr := sc.storeClient.StoreSnapshot(ctx, ns, name, &types.SnapshotInfo{
				SnapStartUID: info.SnapStartUID, // preserve UID so restore-side filter still works
				CacheState:   string(runtimev1alpha1.CacheStateFailed),
				NodeName:     info.NodeName,
				NodeIP:       info.NodeIP,
			}); resetErr != nil {
				klog.Warningf("SnapshotController: stale-build GC: reset %s in %s/%s: %v",
					info.NodeName, ns, name, resetErr)
			} else {
				snapshotStaleBuildResets.WithLabelValues(ns, name).Inc()
			}
		}
	}
}

// sweepOrphanTemplates diffs Redis-tracked template IDs against Kuasar-reported ones.
func (sc *SnapshotController) sweepOrphanTemplates(ctx context.Context) {
	trackedIDs, err := sc.storeClient.ListSnapshotTemplateIDs(ctx)
	if err != nil {
		klog.Warningf("SnapshotController: orphan GC: list tracked IDs: %v", err)
		return
	}
	tracked := make(map[string]bool, len(trackedIDs))
	for _, id := range trackedIDs {
		tracked[id] = true
	}

	nodes, err := sc.getEligibleNodes(ctx)
	if err != nil {
		klog.Warningf("SnapshotController: orphan GC: list nodes: %v", err)
		return
	}

	for _, node := range nodes {
		nodeIP := getNodeInternalIP(node)
		kuasarClient := sc.newKuasarClient(ctx, nodeIP)
		templates, listErr := kuasarClient.ListTemplates(ctx)
		if listErr != nil {
			klog.Warningf("SnapshotController: orphan GC: list templates on %s: %v", node.Name, listErr)
			continue
		}
		for _, t := range templates {
			if t.Owner["snap_start_uid"] == "" {
				continue // not managed by AgentCube
			}
			if !tracked[t.ID] && time.Since(t.CreatedAt) > orphanGracePeriod {
				klog.Infof("SnapshotController: orphan GC: deleting untracked template %s (owner=%v) on node %s",
					t.ID, t.Owner, node.Name)
				_ = kuasarClient.DeleteTemplate(ctx, t.ID)
			}
		}
	}
}

// unstructuredToSnapStart converts an unstructured object to a SnapStart.
func unstructuredToSnapStart(obj *unstructured.Unstructured, ss *runtimev1alpha1.SnapStart) error {
	b, err := obj.MarshalJSON()
	if err != nil {
		return err
	}
	return json.Unmarshal(b, ss)
}

// unstructuredToSandbox converts an unstructured object to a Sandbox.
func unstructuredToSandbox(obj *unstructured.Unstructured, s *sandboxv1alpha1.Sandbox) error {
	b, err := obj.MarshalJSON()
	if err != nil {
		return err
	}
	return json.Unmarshal(b, s)
}

// sandboxToUnstructured converts a Sandbox to an unstructured object.
func sandboxToUnstructured(sandbox *sandboxv1alpha1.Sandbox) (*unstructured.Unstructured, error) {
	b, err := json.Marshal(sandbox)
	if err != nil {
		return nil, err
	}
	var obj map[string]interface{}
	if err := json.Unmarshal(b, &obj); err != nil {
		return nil, err
	}
	return &unstructured.Unstructured{Object: obj}, nil
}
