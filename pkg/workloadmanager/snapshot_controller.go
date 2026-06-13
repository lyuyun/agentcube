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
	"errors"
	"fmt"
	"strings"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/builder"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/predicate"

	sandboxv1alpha1 "sigs.k8s.io/agent-sandbox/api/v1alpha1"
	extensionsv1alpha1 "sigs.k8s.io/agent-sandbox/extensions/api/v1alpha1"

	runtimev1alpha1 "github.com/volcano-sh/agentcube/pkg/apis/runtime/v1alpha1"
	"github.com/volcano-sh/agentcube/pkg/store"
)

const (
	snapshotFinalizer = "agentcube.volcano.sh/snapshot-protection"
	maxInt32Value     = 1<<31 - 1
)

// SandboxSnapshotReconciler reconciles SandboxSnapshot objects.
// Mode-specific logic is fully delegated to the registered SnapshotModeHandler.
type SandboxSnapshotReconciler struct {
	client.Client
	ArtifactStore store.ArtifactStore
	Recorder      record.EventRecorder
	Handlers      map[runtimev1alpha1.SandboxSnapshotMode]SnapshotModeHandler
}

func (r *SandboxSnapshotReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	ss := &runtimev1alpha1.SandboxSnapshot{}
	if err := r.Get(ctx, req.NamespacedName, ss); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}

	if !ss.DeletionTimestamp.IsZero() {
		return r.reconcileDelete(ctx, ss)
	}

	if !controllerutil.ContainsFinalizer(ss, snapshotFinalizer) {
		controllerutil.AddFinalizer(ss, snapshotFinalizer)
		if err := r.Update(ctx, ss); err != nil {
			return ctrl.Result{}, err
		}
		return ctrl.Result{}, nil
	}

	sc := &runtimev1alpha1.SnapshotClass{}
	if err := r.Get(ctx, types.NamespacedName{Name: ss.Spec.SnapshotClassName}, sc); err != nil {
		if apierrors.IsNotFound(err) {
			r.Recorder.Event(ss, corev1.EventTypeWarning, "SnapshotClassNotFound", "SnapshotClass not found: "+ss.Spec.SnapshotClassName)
			return ctrl.Result{RequeueAfter: 30 * time.Second}, nil
		}
		return ctrl.Result{}, err
	}

	handler, ok := r.Handlers[ss.Spec.SnapshotMode]
	if !ok {
		r.Recorder.Event(ss, corev1.EventTypeWarning, "UnsupportedSnapshotMode", "unsupported snapshotMode: "+string(ss.Spec.SnapshotMode))
		return ctrl.Result{RequeueAfter: 30 * time.Second}, nil
	}
	if !containsMode(sc.Spec.SupportedSnapshotModes, ss.Spec.SnapshotMode) {
		r.Recorder.Event(ss, corev1.EventTypeWarning, "SnapshotModeNotSupported", fmt.Sprintf("SnapshotClass %q does not support mode %q", sc.Name, ss.Spec.SnapshotMode))
		return ctrl.Result{RequeueAfter: 30 * time.Second}, nil
	}

	return r.reconcileWithHandler(ctx, ss, sc, handler)
}

func (r *SandboxSnapshotReconciler) reconcileWithHandler(ctx context.Context, ss *runtimev1alpha1.SandboxSnapshot, sc *runtimev1alpha1.SnapshotClass, handler SnapshotModeHandler) (ctrl.Result, error) {
	logger := log.FromContext(ctx)

	currentHash, err := handler.ComputeHash(ctx, ss, sc)
	if err != nil {
		return ctrl.Result{}, fmt.Errorf("compute snapshot hash: %w", err)
	}

	ownerKey := store.ArtifactOwnerKey("SandboxSnapshot", ss.Namespace, ss.Name, string(ss.UID))
	manifest, rawVersion, err := loadManifest(ctx, r.ArtifactStore, ownerKey)
	if err != nil {
		return ctrl.Result{}, err
	}

	rawVersion, err = handler.PrepareArtifactSet(ctx, ss, sc, manifest, ownerKey, rawVersion, currentHash)
	if err != nil {
		return ctrl.Result{}, err
	}

	rawVersion, err = r.reconcileTasksAndArtifacts(ctx, ss, sc, manifest, ownerKey, rawVersion, handler)
	if err != nil {
		return ctrl.Result{}, err
	}

	if manifest.PendingSetRef.SnapshotKey != "" {
		pending, ok := manifest.ArtifactSets[manifest.PendingSetRef.SnapshotKey]
		if ok && handler.ReadyToPromote(pending) {
			logger.Info("promoting pending artifact set to active", "snapshot", ss.Name, "snapshotKey", pending.SnapshotKey)
			r.Recorder.Event(ss, corev1.EventTypeNormal, "SandboxSnapshotPromoted", "background rebuild completed; switching to new artifact set")
			if manifest.ActiveSetRef.SnapshotKey != "" {
				delete(manifest.ArtifactSets, manifest.ActiveSetRef.SnapshotKey)
			}
			manifest.ActiveSetRef = manifest.PendingSetRef
			manifest.PendingSetRef = store.SnapshotArtifactSetRef{}
			ss.Status.ReadyAt = nil
			if _, err = saveManifest(ctx, r.ArtifactStore, ownerKey, manifest, rawVersion); err != nil {
				return ctrl.Result{}, err
			}
		}
	}

	result, err := r.aggregateAndUpdateStatus(ctx, ss, manifest)
	if err != nil {
		return ctrl.Result{}, err
	}
	// Cleanup completed tasks only after status has been aggregated (design §5.5 ordering).
	if manifest.ActiveSetRef.SnapshotKey != "" {
		r.cleanupCompletedTasks(ctx, ss, manifest.ActiveSetRef.SnapshotKey, handler)
	}
	return result, nil
}

func (r *SandboxSnapshotReconciler) reconcileDelete(ctx context.Context, ss *runtimev1alpha1.SandboxSnapshot) (ctrl.Result, error) {
	logger := log.FromContext(ctx)

	ownerKey := store.ArtifactOwnerKey("SandboxSnapshot", ss.Namespace, ss.Name, string(ss.UID))
	if err := r.ArtifactStore.DeleteManifest(ctx, ownerKey); err != nil {
		return ctrl.Result{}, fmt.Errorf("delete artifact manifest: %w", err)
	}
	logger.Info("deleted artifact manifest", "snapshot", ss.Name)

	if handler, ok := r.Handlers[ss.Spec.SnapshotMode]; ok {
		if err := handler.CleanupAll(ctx, ss); err != nil {
			return ctrl.Result{}, err
		}
	}

	controllerutil.RemoveFinalizer(ss, snapshotFinalizer)
	return ctrl.Result{}, r.Update(ctx, ss)
}

func (r *SandboxSnapshotReconciler) reconcileTasksAndArtifacts(ctx context.Context, ss *runtimev1alpha1.SandboxSnapshot, sc *runtimev1alpha1.SnapshotClass, manifest *store.SnapshotArtifactManifest, ownerKey, rawVersion string, handler SnapshotModeHandler) (string, error) {
	workingKey := manifest.PendingSetRef.SnapshotKey
	if workingKey == "" {
		workingKey = manifest.ActiveSetRef.SnapshotKey
	}
	if workingKey == "" {
		return rawVersion, nil
	}
	artifactSet, exists := manifest.ArtifactSets[workingKey]
	if !exists {
		return rawVersion, nil
	}

	taskList, err := r.listSnapshotTasks(ctx, ss, workingKey)
	if err != nil {
		return rawVersion, err
	}
	rawVersion, artifactSet, err = r.syncArtifactStatus(ctx, manifest, ownerKey, rawVersion, workingKey, artifactSet, taskList)
	if err != nil {
		return rawVersion, err
	}

	rawVersion, err = handler.EnsureTasks(ctx, ss, sc, manifest, ownerKey, rawVersion, workingKey, artifactSet)
	if err != nil {
		return rawVersion, err
	}

	return rawVersion, nil
}

func (r *SandboxSnapshotReconciler) cleanupCompletedTasks(ctx context.Context, ss *runtimev1alpha1.SandboxSnapshot, snapshotKey string, handler SnapshotModeHandler) {
	logger := log.FromContext(ctx)

	taskList, err := r.listSnapshotTasks(ctx, ss, snapshotKey)
	if err != nil {
		logger.Error(err, "list completed tasks for cleanup")
		return
	}

	for i := range taskList.Items {
		task := &taskList.Items[i]
		phase := task.Status.Phase
		if phase != runtimev1alpha1.SnapshotArtifactPhaseReady {
			continue
		}
		if err := handler.CleanupTask(ctx, ss, task); err != nil {
			logger.Error(err, "mode-specific task cleanup failed", "task", task.Name)
		}
		if err := r.Delete(ctx, task); err != nil && !apierrors.IsNotFound(err) {
			logger.Error(err, "delete completed task", "name", task.Name)
		}
	}
}

// taskSnapshotRefIndexKey is the field index used to look up SandboxSnapshotTasks by
// their owning SandboxSnapshot name.
const taskSnapshotRefIndexKey = "spec.snapshotRef.name"

// taskTargetNodeIndexKey is the field index used to look up SandboxSnapshotTasks by
// their target node name.
const taskTargetNodeIndexKey = "spec.targetNodeName"

func (r *SandboxSnapshotReconciler) listSnapshotTasks(ctx context.Context, ss *runtimev1alpha1.SandboxSnapshot, snapshotKey string) (*runtimev1alpha1.SandboxSnapshotTaskList, error) {
	all := &runtimev1alpha1.SandboxSnapshotTaskList{}
	if err := r.List(ctx, all, client.InNamespace(ss.Namespace),
		client.MatchingFields{taskSnapshotRefIndexKey: ss.Name},
	); err != nil {
		return nil, fmt.Errorf("list snapshot tasks: %w", err)
	}
	// Filter to the active snapshotKey in-memory; the field index scopes to owner only.
	filtered := &runtimev1alpha1.SandboxSnapshotTaskList{}
	for i := range all.Items {
		if all.Items[i].Spec.SnapshotKey == snapshotKey {
			filtered.Items = append(filtered.Items, all.Items[i])
		}
	}
	return filtered, nil
}

func (r *SandboxSnapshotReconciler) syncArtifactStatus(ctx context.Context, manifest *store.SnapshotArtifactManifest, ownerKey, rawVersion, workingKey string, artifactSet store.SnapshotArtifactSet, taskList *runtimev1alpha1.SandboxSnapshotTaskList) (string, store.SnapshotArtifactSet, error) {
	tasksByNode := make(map[string]*runtimev1alpha1.SandboxSnapshotTask, len(taskList.Items))
	for i := range taskList.Items {
		t := &taskList.Items[i]
		tasksByNode[t.Spec.TargetNodeName] = t
	}

	changed := false
	for i := range artifactSet.Artifacts {
		art := &artifactSet.Artifacts[i]
		task, hasTask := tasksByNode[art.NodeName]
		if !hasTask {
			continue
		}
		if updateArtifactFromTask(art, task) {
			changed = true
		}
	}
	if !changed {
		return rawVersion, artifactSet, nil
	}
	manifest.ArtifactSets[workingKey] = artifactSet
	rawVersion, err := saveManifest(ctx, r.ArtifactStore, ownerKey, manifest, rawVersion)
	return rawVersion, artifactSet, err
}

func updateArtifactFromTask(art *store.SnapshotArtifact, task *runtimev1alpha1.SandboxSnapshotTask) bool {
	newPhase := store.SnapshotArtifactPhase(task.Status.Phase)
	phaseChanged := newPhase != "" && newPhase != art.Phase
	messageChanged := task.Status.Message != art.Message
	if !phaseChanged && !messageChanged {
		return false
	}
	if phaseChanged {
		art.Phase = newPhase
		if newPhase == store.SnapshotArtifactPhaseReady {
			now := time.Now()
			if art.CreatedAt == nil {
				art.CreatedAt = &now
			}
			recordBuildDuration(art.ProviderName, string(task.Spec.SnapshotMode), task.CreationTimestamp.Time)
		}
		recordArtifactPhaseTransition(art.ProviderName, newPhase)
	}
	art.Message = task.Status.Message
	return true
}


func (r *SandboxSnapshotReconciler) aggregateAndUpdateStatus(ctx context.Context, ss *runtimev1alpha1.SandboxSnapshot, manifest *store.SnapshotArtifactManifest) (ctrl.Result, error) {
	activeSet := activeArtifactSet(manifest)
	pendingSet := pendingArtifactSet(manifest)
	status := r.buildSnapshotStatus(ss, activeSet, pendingSet)

	if status.Phase == runtimev1alpha1.SandboxSnapshotPhaseReady && ss.Status.Phase != runtimev1alpha1.SandboxSnapshotPhaseReady {
		r.Recorder.Event(ss, corev1.EventTypeNormal, "SandboxSnapshotReady", "at least one artifact is available")
	}
	if status.Phase == runtimev1alpha1.SandboxSnapshotPhaseReady &&
		(status.FailedNodeCount > 0 || status.UnavailableNodeCount > 0) &&
		(ss.Status.FailedNodeCount != status.FailedNodeCount || ss.Status.UnavailableNodeCount != status.UnavailableNodeCount) {
		r.Recorder.Event(ss, corev1.EventTypeWarning, "SandboxSnapshotDegraded",
			fmt.Sprintf("%d/%d nodes have failed or unavailable artifacts", status.FailedNodeCount+status.UnavailableNodeCount, status.TargetNodeCount))
	}
	if status.FailedNodeCount == status.TargetNodeCount && status.TargetNodeCount > 0 &&
		ss.Status.FailedNodeCount != status.FailedNodeCount {
		r.Recorder.Event(ss, corev1.EventTypeWarning, "AllArtifactsFailed", "all artifact builds failed")
	}
	if status.Phase == runtimev1alpha1.SandboxSnapshotPhaseCreating &&
		status.Message != "" && status.Message != ss.Status.Message {
		r.Recorder.Event(ss, corev1.EventTypeWarning, "ArtifactBuildError", status.Message)
	}

	if status.Phase != runtimev1alpha1.SandboxSnapshotPhaseReady {
		return r.patchSnapshotStatus(ctx, ss, status, ctrl.Result{RequeueAfter: 15 * time.Second})
	}
	return r.patchSnapshotStatus(ctx, ss, status, ctrl.Result{})
}

func (r *SandboxSnapshotReconciler) buildSnapshotStatus(ss *runtimev1alpha1.SandboxSnapshot, activeSet, pendingSet *store.SnapshotArtifactSet) runtimev1alpha1.SandboxSnapshotStatus {
	var workingArtifacts []store.SnapshotArtifact
	if activeSet != nil {
		workingArtifacts = activeSet.Artifacts
	} else if pendingSet != nil {
		workingArtifacts = pendingSet.Artifacts
	}

	status := runtimev1alpha1.SandboxSnapshotStatus{
		TargetNodeCount: int32Len(workingArtifacts),
	}
	countArtifactPhases(workingArtifacts, &status)

	total := status.TargetNodeCount
	switch {
	case total == 0:
		status.Phase = runtimev1alpha1.SandboxSnapshotPhasePending
	case status.ReadyNodeCount > 0 && activeSet != nil:
		status.Phase = runtimev1alpha1.SandboxSnapshotPhaseReady
		if ss.Status.ReadyAt == nil {
			now := metav1.Now()
			status.ReadyAt = &now
		} else {
			status.ReadyAt = ss.Status.ReadyAt
		}
	case status.FailedNodeCount == total && total > 0:
		status.Phase = runtimev1alpha1.SandboxSnapshotPhaseCreating
		status.Message = aggregateArtifactMessages(workingArtifacts)
	default:
		status.Phase = runtimev1alpha1.SandboxSnapshotPhaseCreating
		status.Message = aggregateArtifactMessages(workingArtifacts)
	}
	return status
}

func aggregateArtifactMessages(artifacts []store.SnapshotArtifact) string {
	var parts []string
	for _, art := range artifacts {
		if art.Message != "" {
			parts = append(parts, art.NodeName+": "+art.Message)
		}
	}
	return strings.Join(parts, "; ")
}

func (r *SandboxSnapshotReconciler) patchSnapshotStatus(ctx context.Context, ss *runtimev1alpha1.SandboxSnapshot, status runtimev1alpha1.SandboxSnapshotStatus, result ctrl.Result) (ctrl.Result, error) {
	if snapshotStatusEqual(ss.Status, status) {
		return result, nil
	}
	patch := client.MergeFrom(ss.DeepCopy())
	ss.Status = status
	if err := r.Status().Patch(ctx, ss, patch); err != nil {
		return ctrl.Result{}, fmt.Errorf("patch snapshot status: %w", err)
	}
	return result, nil
}


// -- Shared manifest helpers --

func loadManifest(ctx context.Context, as store.ArtifactStore, ownerKey string) (*store.SnapshotArtifactManifest, string, error) {
	manifest, err := as.GetManifest(ctx, ownerKey)
	if err != nil {
		return nil, "", fmt.Errorf("get artifact manifest: %w", err)
	}
	if manifest == nil {
		return &store.SnapshotArtifactManifest{}, "", nil
	}
	raw, err := json.Marshal(manifest)
	if err != nil {
		return nil, "", fmt.Errorf("marshal manifest for version: %w", err)
	}
	return manifest, string(raw), nil
}

func saveManifest(ctx context.Context, as store.ArtifactStore, ownerKey string, manifest *store.SnapshotArtifactManifest, version string) (string, error) {
	if err := as.PutManifest(ctx, ownerKey, manifest, version); err != nil {
		if errors.Is(err, store.ErrArtifactStoreConflict) {
			return version, fmt.Errorf("artifact store conflict (will retry): %w", err)
		}
		return version, fmt.Errorf("put artifact manifest: %w", err)
	}
	raw, err := json.Marshal(manifest)
	if err != nil {
		return version, fmt.Errorf("marshal manifest for version token: %w", err)
	}
	return string(raw), nil
}


func startNewArtifactSet(ss *runtimev1alpha1.SandboxSnapshot, manifest *store.SnapshotArtifactManifest, snapshotHash string) string {
	manifest.RebuildSeq++
	snapshotKey := buildSnapshotKey(ss, manifest.RebuildSeq)
	if manifest.ArtifactSets == nil {
		manifest.ArtifactSets = make(map[string]store.SnapshotArtifactSet)
	}
	manifest.ArtifactSets[snapshotKey] = store.SnapshotArtifactSet{
		SnapshotKey:  snapshotKey,
		SnapshotHash: snapshotHash,
	}
	return snapshotKey
}

// -- Shared key/label utilities --

func buildSnapshotKey(ss *runtimev1alpha1.SandboxSnapshot, rebuildSeq int32) string {
	mode := strings.ToLower(string(ss.Spec.SnapshotMode))
	// Build suffix first so we know how many chars are left for the name prefix.
	// The full key is used as a label value and must not exceed 63 characters.
	suffix := fmt.Sprintf("-%s-g%d-r%d", mode, ss.Generation, rebuildSeq)
	name := normalizeLabel(ss.Name)
	if maxLen := 63 - len(suffix); len(name) > maxLen {
		if maxLen < 0 {
			maxLen = 0
		}
		name = strings.TrimRight(name[:maxLen], "-")
	}
	return name + suffix
}

func normalizeLabel(s string) string {
	var b strings.Builder
	for _, c := range strings.ToLower(s) {
		if (c >= 'a' && c <= 'z') || (c >= '0' && c <= '9') || c == '-' {
			b.WriteRune(c)
		} else {
			b.WriteRune('-')
		}
	}
	result := strings.Trim(b.String(), "-")
	if len(result) > 63 {
		result = result[:63]
	}
	return result
}

// -- Shared manifest accessors --

func activeArtifactSet(manifest *store.SnapshotArtifactManifest) *store.SnapshotArtifactSet {
	if manifest.ActiveSetRef.SnapshotKey == "" {
		return nil
	}
	s, ok := manifest.ArtifactSets[manifest.ActiveSetRef.SnapshotKey]
	if !ok {
		return nil
	}
	return &s
}

func pendingArtifactSet(manifest *store.SnapshotArtifactManifest) *store.SnapshotArtifactSet {
	if manifest.PendingSetRef.SnapshotKey == "" {
		return nil
	}
	s, ok := manifest.ArtifactSets[manifest.PendingSetRef.SnapshotKey]
	if !ok {
		return nil
	}
	return &s
}

// -- Shared status helpers --

func snapshotStatusEqual(a, b runtimev1alpha1.SandboxSnapshotStatus) bool {
	return a.Phase == b.Phase &&
		a.TargetNodeCount == b.TargetNodeCount &&
		a.CreatingNodeCount == b.CreatingNodeCount &&
		a.ReadyNodeCount == b.ReadyNodeCount &&
		a.FailedNodeCount == b.FailedNodeCount &&
		a.UnavailableNodeCount == b.UnavailableNodeCount &&
		a.Message == b.Message
}

func countArtifactPhases(artifacts []store.SnapshotArtifact, status *runtimev1alpha1.SandboxSnapshotStatus) {
	for _, art := range artifacts {
		switch art.Phase {
		case store.SnapshotArtifactPhaseCreating:
			status.CreatingNodeCount++
		case store.SnapshotArtifactPhaseReady:
			status.ReadyNodeCount++
		case store.SnapshotArtifactPhaseFailed:
			status.FailedNodeCount++
		case store.SnapshotArtifactPhaseUnavailable:
			status.UnavailableNodeCount++
		}
	}
}

func int32Len(artifacts []store.SnapshotArtifact) int32 {
	count := int32(0)
	for range artifacts {
		if count == maxInt32Value {
			return maxInt32Value
		}
		count++
	}
	return count
}

func containsMode(modes []runtimev1alpha1.SandboxSnapshotMode, mode runtimev1alpha1.SandboxSnapshotMode) bool {
	for _, m := range modes {
		if m == mode {
			return true
		}
	}
	return false
}

// snapshotSourceRefIndexKey is the field index used to look up SandboxSnapshots by sourceRef name.
const snapshotSourceRefIndexKey = "spec.sourceRef.name"

// SetupWithManager registers the controller and initializes mode handlers.
func (r *SandboxSnapshotReconciler) SetupWithManager(mgr ctrl.Manager) error {
	r.Recorder = mgr.GetEventRecorderFor("sandbox-snapshot-controller")
	r.Handlers = map[runtimev1alpha1.SandboxSnapshotMode]SnapshotModeHandler{
		runtimev1alpha1.SandboxSnapshotModeFork: &ForkModeHandler{
			Client:        r.Client,
			ArtifactStore: r.ArtifactStore,
			Recorder:      r.Recorder,
			Scheme:        mgr.GetScheme(),
		},
	}

	ctx := context.Background()
	if err := mgr.GetFieldIndexer().IndexField(ctx, &runtimev1alpha1.SandboxSnapshot{}, snapshotSourceRefIndexKey, func(obj client.Object) []string {
		ss := obj.(*runtimev1alpha1.SandboxSnapshot)
		return []string{ss.Spec.SourceRef.Name}
	}); err != nil {
		return fmt.Errorf("setup sourceRef field index: %w", err)
	}
	if err := mgr.GetFieldIndexer().IndexField(ctx, &runtimev1alpha1.SandboxSnapshotTask{}, taskSnapshotRefIndexKey, func(obj client.Object) []string {
		task := obj.(*runtimev1alpha1.SandboxSnapshotTask)
		return []string{task.Spec.SnapshotRef.Name}
	}); err != nil {
		return fmt.Errorf("setup task snapshotRef field index: %w", err)
	}
	if err := mgr.GetFieldIndexer().IndexField(ctx, &runtimev1alpha1.SandboxSnapshotTask{}, taskTargetNodeIndexKey, func(obj client.Object) []string {
		task := obj.(*runtimev1alpha1.SandboxSnapshotTask)
		return []string{task.Spec.TargetNodeName}
	}); err != nil {
		return fmt.Errorf("setup task targetNode field index: %w", err)
	}

	// templateToSnapshotMapper re-enqueues all SandboxSnapshots that reference a changed SandboxTemplate.
	templateToSnapshotMapper := handler.EnqueueRequestsFromMapFunc(func(ctx context.Context, obj client.Object) []ctrl.Request {
		snapshotList := &runtimev1alpha1.SandboxSnapshotList{}
		if err := r.List(ctx, snapshotList,
			client.InNamespace(obj.GetNamespace()),
			client.MatchingFields{snapshotSourceRefIndexKey: obj.GetName()},
		); err != nil {
			return nil
		}
		reqs := make([]ctrl.Request, 0, len(snapshotList.Items))
		for _, ss := range snapshotList.Items {
			reqs = append(reqs, ctrl.Request{NamespacedName: types.NamespacedName{
				Namespace: ss.Namespace,
				Name:      ss.Name,
			}})
		}
		return reqs
	})

	// snapshotClassToSnapshotMapper re-enqueues all SandboxSnapshots that reference a changed SnapshotClass.
	// SnapshotClass is cluster-scoped, so the mapper scans all snapshots and filters in-memory.
	snapshotClassToSnapshotMapper := handler.EnqueueRequestsFromMapFunc(func(ctx context.Context, obj client.Object) []ctrl.Request {
		return snapshotClassToSnapshotRequests(ctx, r.Client, obj.GetName())
	})

	snapshotClassPredicate := predicate.Funcs{
		CreateFunc: func(event.CreateEvent) bool { return true },
		DeleteFunc: func(event.DeleteEvent) bool { return true },
		UpdateFunc: func(e event.UpdateEvent) bool {
			oldObj, ok1 := e.ObjectOld.(*runtimev1alpha1.SnapshotClass)
			newObj, ok2 := e.ObjectNew.(*runtimev1alpha1.SnapshotClass)
			if !ok1 || !ok2 {
				return false
			}
			return oldObj.Generation != newObj.Generation
		},
		GenericFunc: func(event.GenericEvent) bool { return false },
	}

	// nodeToSnapshotMapper re-enqueues all SandboxSnapshots on any node change.
	// Listing all snapshots ensures that a newly-joined node (which has no tasks yet)
	// also triggers artifact builds for existing snapshots.
	nodeToSnapshotMapper := handler.EnqueueRequestsFromMapFunc(func(ctx context.Context, _ client.Object) []ctrl.Request {
		snapshotList := &runtimev1alpha1.SandboxSnapshotList{}
		if err := r.List(ctx, snapshotList); err != nil {
			return nil
		}
		reqs := make([]ctrl.Request, 0, len(snapshotList.Items))
		for _, ss := range snapshotList.Items {
			reqs = append(reqs, ctrl.Request{NamespacedName: types.NamespacedName{
				Namespace: ss.Namespace,
				Name:      ss.Name,
			}})
		}
		return reqs
	})

	// nodeChangedPredicate triggers only when node labels or Ready condition changes.
	nodeChangedPredicate := predicate.Funcs{
		UpdateFunc: func(e event.UpdateEvent) bool {
			oldNode, ok1 := e.ObjectOld.(*corev1.Node)
			newNode, ok2 := e.ObjectNew.(*corev1.Node)
			if !ok1 || !ok2 {
				return false
			}
			if !labelsEqual(oldNode.Labels, newNode.Labels) {
				return true
			}
			return nodeReadyStatusChanged(oldNode, newNode)
		},
		CreateFunc:  func(event.CreateEvent) bool { return true },
		DeleteFunc:  func(event.DeleteEvent) bool { return true },
		GenericFunc: func(event.GenericEvent) bool { return false },
	}

	return ctrl.NewControllerManagedBy(mgr).
		For(&runtimev1alpha1.SandboxSnapshot{}).
		Owns(&runtimev1alpha1.SandboxSnapshotTask{}).
		Owns(&sandboxv1alpha1.Sandbox{}).
		Watches(&runtimev1alpha1.SnapshotClass{}, snapshotClassToSnapshotMapper,
			builder.WithPredicates(snapshotClassPredicate)).
		Watches(&extensionsv1alpha1.SandboxTemplate{}, templateToSnapshotMapper,
			builder.WithPredicates(predicate.GenerationChangedPredicate{})).
		Watches(&corev1.Node{}, nodeToSnapshotMapper,
			builder.WithPredicates(nodeChangedPredicate)).
		Complete(r)
}

func labelsEqual(a, b map[string]string) bool {
	if len(a) != len(b) {
		return false
	}
	for k, v := range a {
		if b[k] != v {
			return false
		}
	}
	return true
}

func nodeReadyStatusChanged(old, new *corev1.Node) bool {
	oldReady := nodeIsReady(old)
	newReady := nodeIsReady(new)
	return oldReady != newReady
}

func snapshotClassToSnapshotRequests(ctx context.Context, c client.Client, className string) []ctrl.Request {
	snapshotList := &runtimev1alpha1.SandboxSnapshotList{}
	if err := c.List(ctx, snapshotList); err != nil {
		return nil
	}
	reqs := make([]ctrl.Request, 0, len(snapshotList.Items))
	for _, ss := range snapshotList.Items {
		if ss.Spec.SnapshotClassName != className {
			continue
		}
		reqs = append(reqs, ctrl.Request{NamespacedName: types.NamespacedName{
			Namespace: ss.Namespace,
			Name:      ss.Name,
		}})
	}
	return reqs
}
