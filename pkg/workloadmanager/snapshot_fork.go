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
	"fmt"
	"sort"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	k8sruntime "k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/log"

	nodev1 "k8s.io/api/node/v1"
	sandboxv1alpha1 "sigs.k8s.io/agent-sandbox/api/v1alpha1"
	extensionsv1alpha1 "sigs.k8s.io/agent-sandbox/extensions/api/v1alpha1"

	"k8s.io/klog/v2"

	runtimev1alpha1 "github.com/volcano-sh/agentcube/pkg/apis/runtime/v1alpha1"
	"github.com/volcano-sh/agentcube/pkg/store"
)

// ForkModeHandler implements SnapshotModeHandler for Fork-mode snapshots.
// It creates a per-node build Sandbox from a SandboxTemplate, snapshots it,
// then makes the artifact available for 1:N forking by new sessions.
type ForkModeHandler struct {
	Client        client.Client
	ArtifactStore store.ArtifactStore
	Recorder      record.EventRecorder
	Scheme        *k8sruntime.Scheme
}

func (h *ForkModeHandler) ComputeHash(ctx context.Context, ss *runtimev1alpha1.SandboxSnapshot, sc *runtimev1alpha1.SnapshotClass) (string, error) {
	tmpl := &extensionsv1alpha1.SandboxTemplate{}
	if err := h.Client.Get(ctx, types.NamespacedName{Name: ss.Spec.SourceRef.Name, Namespace: ss.Namespace}, tmpl); err != nil {
		if apierrors.IsNotFound(err) {
			return "", fmt.Errorf("source SandboxTemplate %q not found", ss.Spec.SourceRef.Name)
		}
		return "", fmt.Errorf("get source SandboxTemplate %q: %w", ss.Spec.SourceRef.Name, err)
	}
	// sourceUID must be the SandboxTemplate UID, not the SandboxSnapshot UID (design §6).
	return computeSnapshotHash(ss, tmpl.UID, tmpl.Spec.PodTemplate.Spec, sc)
}

func (h *ForkModeHandler) PrepareArtifactSet(ctx context.Context, ss *runtimev1alpha1.SandboxSnapshot, _ *runtimev1alpha1.SnapshotClass, manifest *store.SnapshotArtifactManifest, ownerKey, rawVersion, currentHash string) (string, error) {
	var err error
	rawVersion, err = h.clearStaleActiveSet(ctx, ss, manifest, ownerKey, rawVersion, currentHash)
	if err != nil {
		return rawVersion, err
	}
	rawVersion, err = h.maybeStartBackgroundRebuild(ctx, ss, manifest, ownerKey, rawVersion, currentHash)
	if err != nil {
		return rawVersion, err
	}
	return h.ensurePendingSet(ctx, ss, manifest, ownerKey, rawVersion, currentHash)
}

func (h *ForkModeHandler) EnsureTasks(ctx context.Context, ss *runtimev1alpha1.SandboxSnapshot, sc *runtimev1alpha1.SnapshotClass, manifest *store.SnapshotArtifactManifest, ownerKey, rawVersion, workingKey string, artifactSet store.SnapshotArtifactSet) (string, error) {
	// Reset failed artifacts whose retry backoff has elapsed so they can be re-dispatched.
	retryDue := retryDueNodes(artifactSet.Artifacts)
	if len(retryDue) > 0 {
		kept := artifactSet.Artifacts[:0]
		for _, art := range artifactSet.Artifacts {
			if _, due := retryDue[art.NodeName]; !due {
				kept = append(kept, art)
			}
		}
		artifactSet.Artifacts = kept
		manifest.ArtifactSets[workingKey] = artifactSet
		var err error
		rawVersion, err = saveManifest(ctx, h.ArtifactStore, ownerKey, manifest, rawVersion)
		if err != nil {
			return rawVersion, err
		}
	}

	targetNodes, err := h.selectTargetNodes(ctx, ss, sc)
	if err != nil {
		return rawVersion, fmt.Errorf("select target nodes: %w", err)
	}
	coveredNodes := coveredArtifactNodes(artifactSet.Artifacts)
	addedArtifacts := false
	for _, nodeName := range targetNodes {
		if _, covered := coveredNodes[nodeName]; covered {
			continue
		}
		created, err := h.ensureBuildSandboxAndTask(ctx, ss, sc, artifactSet.SnapshotKey, artifactSet.SnapshotHash, nodeName)
		if err != nil {
			log.FromContext(ctx).Error(err, "failed to ensure build sandbox and task", "node", nodeName)
			continue
		}
		if !created {
			// Task deletion is in progress; artifact will be added on the next reconcile.
			continue
		}
		artifactSet.Artifacts = append(artifactSet.Artifacts, newCreatingArtifact(sc.Spec.ProviderName, nodeName, artifactSet))
		addedArtifacts = true
	}
	if !addedArtifacts {
		return rawVersion, nil
	}
	manifest.ArtifactSets[workingKey] = artifactSet
	return saveManifest(ctx, h.ArtifactStore, ownerKey, manifest, rawVersion)
}

// retryDueNodes returns the set of node names whose failed artifacts have passed their retry time.
func retryDueNodes(artifacts []store.SnapshotArtifact) map[string]struct{} {
	now := time.Now()
	due := make(map[string]struct{})
	for _, art := range artifacts {
		if art.Phase != store.SnapshotArtifactPhaseFailed {
			continue
		}
		if art.Retry == nil || art.Retry.NextRetryAt == nil {
			continue
		}
		if !now.Before(*art.Retry.NextRetryAt) {
			due[art.NodeName] = struct{}{}
		}
	}
	return due
}

func (h *ForkModeHandler) ReadyToPromote(pending store.SnapshotArtifactSet) bool {
	return anyNodeArtifactReady(pending.Artifacts)
}

func (h *ForkModeHandler) CleanupTask(ctx context.Context, ss *runtimev1alpha1.SandboxSnapshot, task *runtimev1alpha1.SandboxSnapshotTask) error {
	sbName := buildSandboxName(ss.Name, task.Spec.TargetNodeName)
	buildSandbox := &sandboxv1alpha1.Sandbox{}
	if err := h.Client.Get(ctx, types.NamespacedName{Name: sbName, Namespace: ss.Namespace}, buildSandbox); err != nil {
		if apierrors.IsNotFound(err) {
			return nil
		}
		return fmt.Errorf("get build sandbox %s: %w", sbName, err)
	}
	if err := h.Client.Delete(ctx, buildSandbox); err != nil && !apierrors.IsNotFound(err) {
		return fmt.Errorf("delete build sandbox %s: %w", sbName, err)
	}
	return nil
}

func (h *ForkModeHandler) CleanupAll(ctx context.Context, ss *runtimev1alpha1.SandboxSnapshot) error {
	// Enumerate build Sandboxes via the tasks that own them (design §4.5, §5.4).
	// SandboxSnapshotTask.spec.targetSandboxRef is the authoritative reference to each
	// build Sandbox; the task is the correct navigation point, not a label scan.
	// Kubernetes GC (ownerReferences) provides a safety net for anything this loop misses.
	taskList := &runtimev1alpha1.SandboxSnapshotTaskList{}
	if err := h.Client.List(ctx, taskList,
		client.InNamespace(ss.Namespace),
		client.MatchingFields{taskSnapshotRefIndexKey: ss.Name},
	); err != nil {
		return fmt.Errorf("list snapshot tasks for cleanup: %w", err)
	}
	for i := range taskList.Items {
		task := &taskList.Items[i]
		if task.Spec.SnapshotUID != ss.UID {
			continue
		}
		sbName := task.Spec.TargetSandboxRef.Name
		if sbName == "" {
			continue
		}
		sb := &sandboxv1alpha1.Sandbox{}
		if err := h.Client.Get(ctx, types.NamespacedName{Name: sbName, Namespace: ss.Namespace}, sb); err != nil {
			if apierrors.IsNotFound(err) {
				continue
			}
			return fmt.Errorf("get build sandbox %s: %w", sbName, err)
		}
		if err := h.Client.Delete(ctx, sb); err != nil && !apierrors.IsNotFound(err) {
			return fmt.Errorf("delete build sandbox %s: %w", sbName, err)
		}
	}
	return nil
}

func (h *ForkModeHandler) clearStaleActiveSet(ctx context.Context, ss *runtimev1alpha1.SandboxSnapshot, manifest *store.SnapshotArtifactManifest, ownerKey, rawVersion, currentHash string) (string, error) {
	if !forkRebuildsOnSourceChange(ss) {
		return rawVersion, nil
	}
	activeSet := activeArtifactSet(manifest)
	if activeSet == nil || activeSet.SnapshotHash == currentHash {
		return rawVersion, nil
	}
	log.FromContext(ctx).Info("snapshot hash changed, clearing active artifact set", "snapshot", ss.Name)
	h.Recorder.Event(ss, corev1.EventTypeNormal, "SandboxSnapshotRebuilding", "source change detected; clearing active artifact set")
	manifest.ActiveSetRef = store.SnapshotArtifactSetRef{}
	delete(manifest.ArtifactSets, activeSet.SnapshotKey)
	return saveManifest(ctx, h.ArtifactStore, ownerKey, manifest, rawVersion)
}

func (h *ForkModeHandler) maybeStartBackgroundRebuild(ctx context.Context, ss *runtimev1alpha1.SandboxSnapshot, manifest *store.SnapshotArtifactManifest, ownerKey, rawVersion, currentHash string) (string, error) {
	if ss.Spec.ForkPolicy == nil || ss.Spec.ForkPolicy.RebuildAfter == nil || ss.Status.ReadyAt == nil {
		return rawVersion, nil
	}
	if activeArtifactSet(manifest) == nil || manifest.PendingSetRef.SnapshotKey != "" {
		return rawVersion, nil
	}
	if time.Since(ss.Status.ReadyAt.Time) <= ss.Spec.ForkPolicy.RebuildAfter.Duration {
		return rawVersion, nil
	}
	log.FromContext(ctx).Info("rebuildAfter elapsed, starting background replacement", "snapshot", ss.Name)
	h.Recorder.Event(ss, corev1.EventTypeNormal, "SandboxSnapshotRebuilding", "rebuildAfter elapsed; starting background replacement")
	pendingKey := startNewArtifactSet(ss, manifest, currentHash)
	manifest.PendingSetRef = store.SnapshotArtifactSetRef{SnapshotKey: pendingKey}
	return saveManifest(ctx, h.ArtifactStore, ownerKey, manifest, rawVersion)
}

func (h *ForkModeHandler) ensurePendingSet(ctx context.Context, ss *runtimev1alpha1.SandboxSnapshot, manifest *store.SnapshotArtifactManifest, ownerKey, rawVersion, currentHash string) (string, error) {
	if activeArtifactSet(manifest) != nil || manifest.ActiveSetRef.SnapshotKey != "" || manifest.PendingSetRef.SnapshotKey != "" {
		return rawVersion, nil
	}
	log.FromContext(ctx).Info("no active artifact set, starting initial build", "snapshot", ss.Name)
	h.Recorder.Event(ss, corev1.EventTypeNormal, "SandboxSnapshotCreating", "starting initial snapshot build")
	pendingKey := startNewArtifactSet(ss, manifest, currentHash)
	manifest.PendingSetRef = store.SnapshotArtifactSetRef{SnapshotKey: pendingKey}
	return saveManifest(ctx, h.ArtifactStore, ownerKey, manifest, rawVersion)
}

func (h *ForkModeHandler) selectTargetNodes(ctx context.Context, ss *runtimev1alpha1.SandboxSnapshot, sc *runtimev1alpha1.SnapshotClass) ([]string, error) {
	tmpl := &extensionsv1alpha1.SandboxTemplate{}
	if err := h.Client.Get(ctx, types.NamespacedName{Name: ss.Spec.SourceRef.Name, Namespace: ss.Namespace}, tmpl); err != nil {
		if apierrors.IsNotFound(err) {
			return nil, fmt.Errorf("source SandboxTemplate %q not found", ss.Spec.SourceRef.Name)
		}
		return nil, fmt.Errorf("get source SandboxTemplate %q: %w", ss.Spec.SourceRef.Name, err)
	}
	podSpec := &tmpl.Spec.PodTemplate.Spec

	// Fetch RuntimeClass scheduling constraints when specified.
	var runtimeClassNodeSelector map[string]string
	var runtimeClassTolerations []corev1.Toleration
	if podSpec.RuntimeClassName != nil && *podSpec.RuntimeClassName != "" {
		rc := &nodev1.RuntimeClass{}
		if err := h.Client.Get(ctx, types.NamespacedName{Name: *podSpec.RuntimeClassName}, rc); err != nil {
			if !apierrors.IsNotFound(err) {
				return nil, fmt.Errorf("get RuntimeClass %q: %w", *podSpec.RuntimeClassName, err)
			}
		} else if rc.Scheduling != nil {
			runtimeClassNodeSelector = rc.Scheduling.NodeSelector
			runtimeClassTolerations = rc.Scheduling.Tolerations
		}
	}

	// Build merged nodeSelector: SnapshotClass + SandboxTemplate + RuntimeClass (all must match).
	merged := make(map[string]string)
	for k, v := range sc.Spec.NodeSelector {
		merged[k] = v
	}
	for k, v := range podSpec.NodeSelector {
		merged[k] = v
	}
	for k, v := range runtimeClassNodeSelector {
		merged[k] = v
	}

	// Build effective tolerations: pod tolerations ∪ RuntimeClass tolerations.
	effectiveTolerations := append(podSpec.Tolerations, runtimeClassTolerations...)

	capLabel := runtimev1alpha1.SnapshotProviderLabelPrefix + sc.Spec.ProviderName

	// Explicit nodeName pins to a single node; validate all constraints still apply.
	if podSpec.NodeName != "" {
		node := &corev1.Node{}
		if err := h.Client.Get(ctx, types.NamespacedName{Name: podSpec.NodeName}, node); err != nil {
			if apierrors.IsNotFound(err) {
				return nil, nil
			}
			return nil, fmt.Errorf("get pinned node %q: %w", podSpec.NodeName, err)
		}
		if !nodeIsReady(node) || node.Labels[capLabel] != "true" {
			return nil, nil
		}
		for k, v := range merged {
			if node.Labels[k] != v {
				return nil, nil
			}
		}
		if !nodeToleratesTaints(effectiveTolerations, node.Spec.Taints) {
			return nil, nil
		}
		return []string{podSpec.NodeName}, nil
	}

	nodeList := &corev1.NodeList{}
	if err := h.Client.List(ctx, nodeList, &client.ListOptions{LabelSelector: labels.SelectorFromSet(merged)}); err != nil {
		return nil, fmt.Errorf("list nodes: %w", err)
	}

	var result []string
	for i := range nodeList.Items {
		node := &nodeList.Items[i]
		if !nodeIsReady(node) {
			continue
		}
		if node.Labels[capLabel] != "true" {
			continue
		}
		if podSpec.Affinity != nil && !matchNodeAffinity(node, podSpec.Affinity.NodeAffinity) {
			continue
		}
		if !nodeToleratesTaints(effectiveTolerations, node.Spec.Taints) {
			continue
		}
		result = append(result, node.Name)
	}
	return result, nil
}

// matchNodeAffinity checks if a node satisfies the required node affinity of a pod.
func matchNodeAffinity(node *corev1.Node, affinity *corev1.NodeAffinity) bool {
	if affinity == nil || affinity.RequiredDuringSchedulingIgnoredDuringExecution == nil {
		return true
	}
	// NodeSelectorTerms are OR'd.
	for _, term := range affinity.RequiredDuringSchedulingIgnoredDuringExecution.NodeSelectorTerms {
		if matchNodeSelectorTerm(node, term) {
			return true
		}
	}
	return false
}

func matchNodeSelectorTerm(node *corev1.Node, term corev1.NodeSelectorTerm) bool {
	for _, req := range term.MatchExpressions {
		if !matchNodeSelectorRequirement(node.Labels, req) {
			return false
		}
	}
	for _, req := range term.MatchFields {
		if !matchNodeField(node, req) {
			return false
		}
	}
	return true
}

// matchNodeField evaluates a single MatchFields requirement against a node.
// Only metadata.name is supported; other fields are treated as always-satisfied.
func matchNodeField(node *corev1.Node, req corev1.NodeSelectorRequirement) bool {
	if req.Key != "metadata.name" {
		return true
	}
	switch req.Operator {
	case corev1.NodeSelectorOpIn:
		for _, v := range req.Values {
			if v == node.Name {
				return true
			}
		}
		return false
	case corev1.NodeSelectorOpNotIn:
		for _, v := range req.Values {
			if v == node.Name {
				return false
			}
		}
		return true
	default:
		return true
	}
}

func matchNodeSelectorRequirement(nodeLabels map[string]string, req corev1.NodeSelectorRequirement) bool {
	val, exists := nodeLabels[req.Key]
	switch req.Operator {
	case corev1.NodeSelectorOpIn:
		if !exists {
			return false
		}
		for _, v := range req.Values {
			if v == val {
				return true
			}
		}
		return false
	case corev1.NodeSelectorOpNotIn:
		if !exists {
			return true
		}
		for _, v := range req.Values {
			if v == val {
				return false
			}
		}
		return true
	case corev1.NodeSelectorOpExists:
		return exists
	case corev1.NodeSelectorOpDoesNotExist:
		return !exists
	default:
		return true
	}
}

// nodeToleratesTaints returns true only when all node taints are tolerated.
func nodeToleratesTaints(tolerations []corev1.Toleration, taints []corev1.Taint) bool {
	for _, taint := range taints {
		if !taintTolerated(taint, tolerations) {
			return false
		}
	}
	return true
}

func taintTolerated(taint corev1.Taint, tolerations []corev1.Toleration) bool {
	for _, t := range tolerations {
		if len(t.Effect) > 0 && t.Effect != taint.Effect {
			continue
		}
		if t.Operator == corev1.TolerationOpExists {
			if t.Key == "" || t.Key == taint.Key {
				return true
			}
			continue
		}
		if t.Key == taint.Key && t.Value == taint.Value {
			return true
		}
	}
	return false
}

// ensureBuildSandboxAndTask creates the build Sandbox and SandboxSnapshotTask for the given node.
// Returns (true, nil) when a new task was successfully created, (false, nil) when the caller
// should wait for a next reconcile (e.g. a terminal task is still terminating).
func (h *ForkModeHandler) ensureBuildSandboxAndTask(ctx context.Context, ss *runtimev1alpha1.SandboxSnapshot, sc *runtimev1alpha1.SnapshotClass, snapshotKey, snapshotHash, nodeName string) (bool, error) {
	sbName := buildSandboxName(ss.Name, nodeName)
	taskName := buildTaskName(nodeName, snapshotKey)

	existingTask := &runtimev1alpha1.SandboxSnapshotTask{}
	err := h.Client.Get(ctx, types.NamespacedName{Name: taskName, Namespace: ss.Namespace}, existingTask)
	if err == nil {
		phase := existingTask.Status.Phase
		if phase == runtimev1alpha1.SnapshotArtifactPhaseFailed || phase == runtimev1alpha1.SnapshotArtifactPhaseUnavailable {
			// Task is terminal. If already terminating, wait for GC before recreating.
			if existingTask.DeletionTimestamp != nil {
				return false, nil
			}
			if err := h.Client.Delete(ctx, existingTask); err != nil && !apierrors.IsNotFound(err) {
				return false, fmt.Errorf("delete terminal task for retry %s: %w", taskName, err)
			}
			// Deletion is now in flight; a new task will be created on the next reconcile.
			return false, nil
		}
		// Non-terminal task already exists; just ensure the build sandbox is present.
		_, err = h.ensureBuildSandbox(ctx, ss, snapshotKey, nodeName, sbName)
		return false, err
	}
	if !apierrors.IsNotFound(err) {
		return false, fmt.Errorf("get snapshot task %s: %w", taskName, err)
	}

	buildSandbox, err := h.ensureBuildSandbox(ctx, ss, snapshotKey, nodeName, sbName)
	if err != nil {
		return false, err
	}

	task := &runtimev1alpha1.SandboxSnapshotTask{
		ObjectMeta: metaWithLabels(taskName, ss.Namespace, nil),
		Spec: runtimev1alpha1.SandboxSnapshotTaskSpec{
			SnapshotRef: corev1.TypedLocalObjectReference{
				APIGroup: ptr.To(runtimev1alpha1.GroupVersion.Group),
				Kind:     "SandboxSnapshot",
				Name:     ss.Name,
			},
			SnapshotUID:  ss.UID,
			SnapshotMode: ss.Spec.SnapshotMode,
			TargetSandboxRef: corev1.TypedLocalObjectReference{
				APIGroup: ptr.To("agents.x-k8s.io"),
				Kind:     "Sandbox",
				Name:     buildSandbox.Name,
			},
			TargetNodeName: nodeName,
			ProviderName:   sc.Spec.ProviderName,
			SnapshotKey:    snapshotKey,
			SnapshotHash:   snapshotHash,
		},
	}
	if err := controllerutil.SetControllerReference(ss, task, h.Scheme); err != nil {
		return false, fmt.Errorf("set controller reference on task: %w", err)
	}
	if err := h.Client.Create(ctx, task); err != nil && !apierrors.IsAlreadyExists(err) {
		return false, fmt.Errorf("create snapshot task %s: %w", taskName, err)
	}
	return true, nil
}

func (h *ForkModeHandler) ensureBuildSandbox(ctx context.Context, ss *runtimev1alpha1.SandboxSnapshot, snapshotKey, nodeName, sbName string) (*sandboxv1alpha1.Sandbox, error) {
	buildSandbox := &sandboxv1alpha1.Sandbox{}
	err := h.Client.Get(ctx, types.NamespacedName{Name: sbName, Namespace: ss.Namespace}, buildSandbox)
	if apierrors.IsNotFound(err) {
		tmpl := &extensionsv1alpha1.SandboxTemplate{}
		if err := h.Client.Get(ctx, types.NamespacedName{Name: ss.Spec.SourceRef.Name, Namespace: ss.Namespace}, tmpl); err != nil {
			return nil, fmt.Errorf("get source SandboxTemplate: %w", err)
		}
		podSpec := tmpl.Spec.PodTemplate.Spec.DeepCopy()
		podSpec.NodeName = nodeName
		// Signal the runtime to reach a fork-safe point before reporting readiness to the driver.
		for i := range podSpec.Containers {
			podSpec.Containers[i].Env = append(podSpec.Containers[i].Env, corev1.EnvVar{
				Name:  "AGENTCUBE_SNAPSTART_BUILD_MODE",
				Value: "true",
			})
		}

		buildSandbox = &sandboxv1alpha1.Sandbox{
			ObjectMeta: metaWithLabels(sbName, ss.Namespace, nil),
			Spec: sandboxv1alpha1.SandboxSpec{
				PodTemplate: sandboxv1alpha1.PodTemplate{Spec: *podSpec},
				Replicas:    ptr.To[int32](1),
			},
		}
		if err := controllerutil.SetControllerReference(ss, buildSandbox, h.Scheme); err != nil {
			return nil, fmt.Errorf("set controller reference on build sandbox: %w", err)
		}
		if err := h.Client.Create(ctx, buildSandbox); err != nil && !apierrors.IsAlreadyExists(err) {
			return nil, fmt.Errorf("create build sandbox %s: %w", sbName, err)
		}
		if err := h.Client.Get(ctx, types.NamespacedName{Name: sbName, Namespace: ss.Namespace}, buildSandbox); err != nil {
			return nil, fmt.Errorf("re-fetch build sandbox: %w", err)
		}
	} else if err != nil {
		return nil, fmt.Errorf("get build sandbox %s: %w", sbName, err)
	}
	return buildSandbox, nil
}

// -- Fork-specific helper functions --

func forkRebuildsOnSourceChange(ss *runtimev1alpha1.SandboxSnapshot) bool {
	if ss.Spec.ForkPolicy == nil || ss.Spec.ForkPolicy.RebuildOnSourceChange == nil {
		return true
	}
	return *ss.Spec.ForkPolicy.RebuildOnSourceChange
}

func anyNodeArtifactReady(artifacts []store.SnapshotArtifact) bool {
	for _, a := range artifacts {
		if a.Phase == store.SnapshotArtifactPhaseReady {
			return true
		}
	}
	return false
}

// validatedSnapshotKey returns the artifact set's SnapshotKey when at least one
// Ready artifact passes all required validations (design §5.6 step 5):
// provider name present, snapshot hash consistent, snapshot key consistent.
// Returns an empty string when no valid artifact is found.
func validatedSnapshotKey(set store.SnapshotArtifactSet) string {
	for _, art := range set.Artifacts {
		if art.Phase != store.SnapshotArtifactPhaseReady {
			continue
		}
		if art.ProviderName == "" {
			continue
		}
		if art.SnapshotHash != set.SnapshotHash {
			continue
		}
		if art.SnapshotKey != set.SnapshotKey {
			continue
		}
		return set.SnapshotKey
	}
	return ""
}

func coveredArtifactNodes(artifacts []store.SnapshotArtifact) map[string]struct{} {
	covered := make(map[string]struct{}, len(artifacts))
	for _, art := range artifacts {
		covered[art.NodeName] = struct{}{}
	}
	return covered
}

func newCreatingArtifact(providerName, nodeName string, artifactSet store.SnapshotArtifactSet) store.SnapshotArtifact {
	return store.SnapshotArtifact{
		ProviderName: providerName,
		NodeName:     nodeName,
		Phase:        store.SnapshotArtifactPhaseCreating,
		SnapshotKey:  artifactSet.SnapshotKey,
		SnapshotHash: artifactSet.SnapshotHash,
	}
}

func buildSandboxName(snapshotName, nodeName string) string {
	return fmt.Sprintf("%s-build-%s", normalizeLabel(snapshotName), normalizeLabel(nodeName))
}

func buildTaskName(nodeName, snapshotKey string) string {
	return fmt.Sprintf("%s-%s", normalizeLabel(snapshotKey), normalizeLabel(nodeName))
}

func nodeIsReady(node *corev1.Node) bool {
	for _, cond := range node.Status.Conditions {
		if cond.Type == corev1.NodeReady {
			return cond.Status == corev1.ConditionTrue
		}
	}
	return false
}

// snapshotHashInput is the stable serialization for hash computation.
type snapshotHashInput struct {
	SnapshotMode    string            `json:"snapshotMode"`
	SourceNamespace string            `json:"sourceNamespace"`
	SourceName      string            `json:"sourceName"`
	SourceUID       string            `json:"sourceUID"`
	PodTemplateSpec corev1.PodSpec    `json:"podTemplateSpec"`
	SnapshotClass   snapshotClassHash `json:"snapshotClass"`
}

type snapshotClassHash struct {
	Name         string `json:"name"`
	ProviderName string `json:"providerName"`
}

func computeSnapshotHash(ss *runtimev1alpha1.SandboxSnapshot, sourceUID types.UID, podSpec corev1.PodSpec, sc *runtimev1alpha1.SnapshotClass) (string, error) {
	input := snapshotHashInput{
		SnapshotMode:    string(ss.Spec.SnapshotMode),
		SourceNamespace: ss.Namespace,
		SourceName:      ss.Spec.SourceRef.Name,
		SourceUID:       string(sourceUID),
		PodTemplateSpec: normalizePodSpec(podSpec),
		SnapshotClass: snapshotClassHash{
			Name:         sc.Name,
			ProviderName: sc.Spec.ProviderName,
		},
	}
	data, err := json.Marshal(input)
	if err != nil {
		return "", err
	}
	h := sha256.Sum256(data)
	return fmt.Sprintf("sha256:%x", h), nil
}

func normalizePodSpec(spec corev1.PodSpec) corev1.PodSpec {
	s := spec.DeepCopy()
	s.NodeName = ""
	sort.Slice(s.Tolerations, func(i, j int) bool {
		a, b := s.Tolerations[i], s.Tolerations[j]
		if a.Key != b.Key {
			return a.Key < b.Key
		}
		if a.Operator != b.Operator {
			return a.Operator < b.Operator
		}
		if a.Value != b.Value {
			return a.Value < b.Value
		}
		if a.Effect != b.Effect {
			return a.Effect < b.Effect
		}
		// nil and ptr(0) both evaluate to 0 but marshal differently;
		// treat nil < non-nil so the sort order is fully deterministic.
		aNil := a.TolerationSeconds == nil
		bNil := b.TolerationSeconds == nil
		if aNil != bNil {
			return aNil
		}
		if !aNil {
			return *a.TolerationSeconds < *b.TolerationSeconds
		}
		return false
	})
	return *s
}

func metaWithLabels(name, namespace string, lbls map[string]string) metav1.ObjectMeta {
	return metav1.ObjectMeta{
		Name:      name,
		Namespace: namespace,
		Labels:    lbls,
	}
}

// lookupActiveForkSnapshotKey returns the active snapshot key for a Fork-mode
// SandboxSnapshot whose sourceRef matches sandboxTemplateName.
// Returns an empty string when no Ready artifact is found.
// Errors from the artifact store are logged and treated as cache-miss so session
// creation falls back to cold start rather than failing.
func lookupActiveForkSnapshotKey(
	ctx context.Context,
	k8sClient client.Client,
	artifactStore store.ArtifactStore,
	namespace, sandboxTemplateName string,
) string {
	snapshotList := &runtimev1alpha1.SandboxSnapshotList{}
	if err := k8sClient.List(ctx, snapshotList,
		client.InNamespace(namespace),
		client.MatchingFields{snapshotSourceRefIndexKey: sandboxTemplateName},
	); err != nil {
		klog.V(4).InfoS("snapshot lookup: failed to list snapshots, falling back to cold start",
			"namespace", namespace, "error", err)
		return ""
	}

	// Prefer the most recently created snapshot when multiple match.
	sort.Slice(snapshotList.Items, func(i, j int) bool {
		return snapshotList.Items[j].CreationTimestamp.Before(&snapshotList.Items[i].CreationTimestamp)
	})

	for i := range snapshotList.Items {
		ss := &snapshotList.Items[i]
		if ss.Spec.SnapshotMode != runtimev1alpha1.SandboxSnapshotModeFork {
			continue
		}
		if ss.Status.Phase != runtimev1alpha1.SandboxSnapshotPhaseReady {
			continue
		}

		ownerKey := store.ArtifactOwnerKey("SandboxSnapshot", ss.Namespace, ss.Name, string(ss.UID))
		manifest, err := artifactStore.GetManifest(ctx, ownerKey)
		if err != nil {
			klog.V(4).InfoS("snapshot lookup: artifact store error, falling back to cold start",
				"snapshot", ss.Name, "error", err)
			continue
		}
		if manifest == nil || manifest.ActiveSetRef.SnapshotKey == "" {
			continue
		}
		activeSet, ok := manifest.ArtifactSets[manifest.ActiveSetRef.SnapshotKey]
		if !ok {
			continue
		}

		if validatedSnapshotKey(activeSet) != "" {
			klog.V(4).InfoS("snapshot lookup: found active fork snapshot key",
				"snapshot", ss.Name, "snapshotKey", manifest.ActiveSetRef.SnapshotKey)
			return manifest.ActiveSetRef.SnapshotKey
		}
	}
	return ""
}
