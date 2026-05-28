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
	"net/http"
	"time"

	"github.com/gin-gonic/gin"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	k8stypes "k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/util/retry"
	"k8s.io/klog/v2"
	sandboxv1alpha1 "sigs.k8s.io/agent-sandbox/api/v1alpha1"
	"sigs.k8s.io/agent-sandbox/controllers"
	extensionsv1alpha1 "sigs.k8s.io/agent-sandbox/extensions/api/v1alpha1"

	"github.com/volcano-sh/agentcube/pkg/api"
	runtimev1alpha1 "github.com/volcano-sh/agentcube/pkg/apis/runtime/v1alpha1"
	"github.com/volcano-sh/agentcube/pkg/common/types"
	"github.com/volcano-sh/agentcube/pkg/store"
)

// errSandboxCreationTimeout is returned when the internal sandbox-ready wait exceeds the 2-minute deadline.
var errSandboxCreationTimeout = errors.New("sandbox creation timed out")

// storeCleanupTimeout is the maximum duration allowed to clean up a store placeholder.
const storeCleanupTimeout = 30 * time.Second

// isContextError reports whether err is a context cancellation or deadline error.
func isContextError(err error) bool {
	return errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded)
}

// handleHealth handles health check requests
func (s *Server) handleHealth(c *gin.Context) {
	respondJSON(c, http.StatusOK, map[string]string{
		"status": "healthy",
	})
}

// handleAgentRuntimeCreate handles AgentRuntime sandbox creation requests.
func (s *Server) handleAgentRuntimeCreate(c *gin.Context) {
	s.handleSandboxCreate(c, types.AgentRuntimeKind)
}

// handleCodeInterpreterCreate handles CodeInterpreter sandbox creation requests.
func (s *Server) handleCodeInterpreterCreate(c *gin.Context) {
	s.handleSandboxCreate(c, types.CodeInterpreterKind)
}

// extractUserK8sClient extracts user information from the context and creates a user-specific Kubernetes client.
// It returns the dynamic client for the user and an error if authentication fails or client creation fails.
func (s *Server) extractUserK8sClient(c *gin.Context) (dynamic.Interface, error) {
	// Extract user information from context
	userToken, userNamespace, _, serviceAccountName := extractUserInfo(c)
	if userToken == "" || userNamespace == "" || serviceAccountName == "" {
		return nil, errors.New("unable to extract user credentials")
	}

	// Create sandbox using user's K8s client
	userClient, err := s.k8sClient.GetOrCreateUserK8sClient(userToken, userNamespace, serviceAccountName)
	if err != nil {
		klog.Infof("create user client failed: %v", err)
		return nil, fmt.Errorf("create user client failed: %w", err)
	}
	return userClient.dynamicClient, nil
}

// selectReadySnapshotNode picks a per-node snapshot record that is usable for restore:
//   - CacheState == LocalReady (Phase 1 NodeLocal requirement per design §6.3)
//   - Node is currently Ready in the informer cache
func (s *Server) selectReadySnapshotNode(infos []*types.SnapshotInfo) *types.SnapshotInfo {
	for _, info := range infos {
		if info.CacheState != string(runtimev1alpha1.CacheStateLocalReady) {
			klog.V(3).Infof("selectReadySnapshotNode: node %s phase=Ready but cacheState=%s (want LocalReady); skipping",
				info.NodeName, info.CacheState)
			continue
		}
		if !s.isNodeCurrentlyReady(info.NodeName) {
			klog.V(3).Infof("selectReadySnapshotNode: node %s has a Ready snapshot but node is not currently Ready; skipping",
				info.NodeName)
			continue
		}
		return info
	}
	return nil
}

// isNodeCurrentlyReady returns true if the named node exists in the informer cache and is Ready.
func (s *Server) isNodeCurrentlyReady(nodeName string) bool {
	if s.informers.NodeInformer == nil {
		return true // informer not available; assume Ready to avoid blocking restores
	}
	obj, exists, err := s.informers.NodeInformer.GetStore().GetByKey(nodeName)
	if err != nil || !exists {
		return false
	}
	node, ok := obj.(*corev1.Node)
	if !ok {
		return false
	}
	return isNodeReady(node)
}

// injectKuasarRestoreAnnotations adds WarmFork restore protocol annotations to the
// sandbox PodTemplate so that Kuasar sandboxer performs snapshot restore on start.
// These must be present before the Sandbox CR is submitted to the API server.
func injectKuasarRestoreAnnotations(sandbox *sandboxv1alpha1.Sandbox, snap *types.SnapshotInfo, sessionID string) {
	if sandbox.Spec.PodTemplate.ObjectMeta.Annotations == nil {
		sandbox.Spec.PodTemplate.ObjectMeta.Annotations = make(map[string]string)
	}
	ann := sandbox.Spec.PodTemplate.ObjectMeta.Annotations

	ann["kuasar.io/snapshot-type"] = "warm-fork"
	ann["kuasar.io/template-key"] = snap.TemplateKey
	ann["kuasar.io/task-id"] = sessionID

	// Build task-context: workspace path is derived from sessionID.
	taskCtxBytes, _ := json.Marshal(map[string]string{"workspace": "/workspace/" + sessionID})
	ann["kuasar.io/task-context"] = string(taskCtxBytes)

	// Propagate session-specific env overrides so picod's COMMIT handler can apply them.
	// We collect only env vars that are not baked into the snapshot template.
	for _, c := range sandbox.Spec.PodTemplate.Spec.Containers {
		for _, e := range c.Env {
			if isSessionSpecificEnv(e) {
				ann["kuasar.io/task-env/"+e.Name] = e.Value
			}
		}
	}
}

// snapshotVersionGate holds the current runtime's version parameters used to validate
// Redis snapshot entries before restore.
// All fields except RuntimeGeneration are mandatory (non-empty required).
// Stale-generation entries are rejected upstream via SnapStartUID matching.
type snapshotVersionGate struct {
	SpecHash          string
	ImageRef          string
	Checkpoint        string
	ProtocolVersion   string
	RuntimeGeneration int64
}

// filterSnapshotsByVersion rejects Redis snapshot entries that do not match the current
// runtime version or are missing mandatory version fields. Stale-generation entries are
// already rejected upstream by SnapStartUID matching.
//
// Rejection rules:
//  1. Any of SpecHash, ImageRef, Checkpoint, ProtocolVersion is empty → reject (pre-upgrade entry).
//  2. SpecHash mismatch → reject (spec drift: args/env/resources/runtimeClass changed).
//  3. ImageRef mismatch → reject (image reference changed).
//  4. Checkpoint mismatch → reject.
//  5. ProtocolVersion mismatch → reject.
//  6. RuntimeGeneration non-zero and mismatches → reject.
func filterSnapshotsByVersion(infos []*types.SnapshotInfo, gate snapshotVersionGate) []*types.SnapshotInfo {
	var result []*types.SnapshotInfo
	for _, info := range infos {
		if info.SpecHash == "" || info.ImageRef == "" || info.Checkpoint == "" || info.ProtocolVersion == "" {
			klog.V(3).Infof("filterSnapshotsByVersion: node %s: missing mandatory version fields; rejecting stale entry",
				info.NodeName)
			continue
		}
		if info.SpecHash != gate.SpecHash {
			klog.V(3).Infof("filterSnapshotsByVersion: node %s: specHash mismatch (stored=%s current=%s); skipping",
				info.NodeName, info.SpecHash, gate.SpecHash)
			continue
		}
		if info.ImageRef != gate.ImageRef {
			klog.V(3).Infof("filterSnapshotsByVersion: node %s: image mismatch (stored=%s current=%s); skipping",
				info.NodeName, info.ImageRef, gate.ImageRef)
			continue
		}
		if info.Checkpoint != gate.Checkpoint {
			klog.V(3).Infof("filterSnapshotsByVersion: node %s: checkpoint mismatch (stored=%s current=%s); skipping",
				info.NodeName, info.Checkpoint, gate.Checkpoint)
			continue
		}
		if info.ProtocolVersion != gate.ProtocolVersion {
			klog.V(3).Infof("filterSnapshotsByVersion: node %s: protocolVersion mismatch (stored=%s current=%s); skipping",
				info.NodeName, info.ProtocolVersion, gate.ProtocolVersion)
			continue
		}
		if info.RuntimeGeneration != 0 && info.RuntimeGeneration != gate.RuntimeGeneration {
			klog.V(3).Infof("filterSnapshotsByVersion: node %s: runtimeGeneration mismatch (stored=%d current=%d); skipping",
				info.NodeName, info.RuntimeGeneration, gate.RuntimeGeneration)
			continue
		}
		result = append(result, info)
	}
	return result
}

// filterSnapshotsByEligibleNodes returns only snapshot infos whose node satisfies the
// current runtime's scheduling constraints (kuasar-snapstart label + runtimeClass nodeSelector).
// On error, returns nil to force cold start — restoring to a node that may no longer satisfy
// the runtime's scheduling constraints is unsafe, so we conservatively reject all placements.
func (s *Server) filterSnapshotsByEligibleNodes(ctx context.Context, infos []*types.SnapshotInfo, ci *runtimev1alpha1.CodeInterpreter) []*types.SnapshotInfo {
	eligible, err := s.snapshotController.getEligibleNodesForCI(ctx, ci)
	if err != nil {
		klog.Warningf("filterSnapshotsByEligibleNodes: cannot determine eligible nodes for %s/%s: %v; falling back to cold start",
			ci.Namespace, ci.Name, err)
		return nil
	}
	eligibleSet := make(map[string]bool, len(eligible))
	for _, n := range eligible {
		eligibleSet[n.Name] = true
	}
	var result []*types.SnapshotInfo
	for _, info := range infos {
		if eligibleSet[info.NodeName] {
			result = append(result, info)
		} else {
			klog.V(3).Infof("filterSnapshotsByEligibleNodes: node %s not eligible for %s/%s; skipping",
				info.NodeName, ci.Namespace, ci.Name)
		}
	}
	return result
}

// isSessionSpecificEnv returns true for env vars that carry per-session identity and
// must be injected via the WarmFork PREPARE message rather than being baked into the snapshot.
func isSessionSpecificEnv(e corev1.EnvVar) bool {
	switch e.Name {
	case "PICOD_AUTH_PUBLIC_KEY":
		return true
	}
	return false
}

// handleSandboxCreate handles sandbox creation given a specific kind.
func (s *Server) handleSandboxCreate(c *gin.Context, kind string) {
	sandboxReq := &types.CreateSandboxRequest{}
	if err := c.ShouldBindJSON(sandboxReq); err != nil {
		klog.Errorf("parse request body failed: %v", err)
		respondError(c, http.StatusBadRequest, "Invalid request body")
		return
	}

	sandboxReq.Kind = kind

	if err := sandboxReq.Validate(); err != nil {
		klog.Errorf("request body validation failed: %v", err)
		respondError(c, http.StatusBadRequest, err.Error())
		return
	}

	// Query snapshot availability for CodeInterpreter (before building sandbox object).
	// forceDirectSandbox=true bypasses WarmPool SandboxClaim so we can bind nodeName.
	// SnapStart indexer is checked first; Redis is only queried when activeMode=Snapshot.
	// Redis key is the SnapStart name (not the runtime name) to prevent stale data after
	// delete/recreate of a SnapStart object.
	var readySnap *types.SnapshotInfo
	if kind == types.CodeInterpreterKind && s.snapshotController != nil {
		snapStarts := s.snapshotController.indexer.getByRuntime(sandboxReq.Namespace, sandboxReq.Name)
		if len(snapStarts) == 0 {
			klog.V(3).Infof("handleSandboxCreate: no SnapStart found for %s/%s; cold start",
				sandboxReq.Namespace, sandboxReq.Name)
		} else {
			ss := snapStarts[0]
			// ActiveMode is the authoritative restore gate set by SnapshotController.
			if ss.Status.ActiveMode != runtimev1alpha1.SessionStartupModeSnapshot {
				klog.V(3).Infof("handleSandboxCreate: SnapStart for %s/%s activeMode=%s; falling back to cold start",
					sandboxReq.Namespace, sandboxReq.Name, ss.Status.ActiveMode)
			} else {
				snapInfos, snapErr := s.storeClient.GetSnapshotNodes(c.Request.Context(), sandboxReq.Namespace, ss.Name)
				if snapErr != nil {
					klog.Warningf("handleSandboxCreate: GetSnapshotNodes %s/%s failed (falling back to cold start): %v",
						sandboxReq.Namespace, ss.Name, snapErr)
				} else if len(snapInfos) > 0 {
					// Reject entries whose SnapStartUID doesn't match the current SnapStart UID.
					// This prevents restore from stale data left by a prior delete/recreate cycle.
					// Entries with empty UID (built before UID tracking) are always rejected
					// to avoid reusing data of unknown provenance.
					var current []*types.SnapshotInfo
					for _, info := range snapInfos {
						if info.SnapStartUID == string(ss.UID) {
							current = append(current, info)
						}
					}
					ci, ciErr := s.snapshotController.getCodeInterpreter(sandboxReq.Namespace, sandboxReq.Name)
					if ciErr != nil {
						klog.Warningf("handleSandboxCreate: cannot get CI %s/%s for version gating, skipping snapshot: %v",
							sandboxReq.Namespace, sandboxReq.Name, ciErr)
					} else {
						checkpoint := ss.Spec.Checkpoint
						if checkpoint == "" {
							checkpoint = "InterpreterReady"
						}
						gate := snapshotVersionGate{
							SpecHash:          computeSpecHashNoImage(ci),
							ImageRef:          ci.Spec.Template.Image,
							Checkpoint:        checkpoint,
							ProtocolVersion:   "1",
							RuntimeGeneration: ci.Generation,
						}
						current = filterSnapshotsByVersion(current, gate)
						current = s.filterSnapshotsByEligibleNodes(c.Request.Context(), current, ci)
						readySnap = s.selectReadySnapshotNode(current)
					}
				}
			}
		}
	}

	// SAR pre-check for snapshot path: when a snapshot is available it forces "sandboxes/create"
	// (bypassing the warm-pool SandboxClaim). If the user lacks that permission, treat the
	// snapshot as unavailable and fall back to the cold/warm path, where the SAR will be
	// re-evaluated against the actual resource being created (sandboxclaims or sandboxes).
	if readySnap != nil && s.config.EnableAuth {
		preCheckClient, clientErr := s.extractUserK8sClient(c)
		if clientErr != nil {
			respondError(c, http.StatusUnauthorized, clientErr.Error())
			return
		}
		if sarErr := s.checkResourceCreatePermission(c.Request.Context(), preCheckClient,
			sandboxReq.Namespace, "sandboxes"); sarErr != nil {
			klog.Infof("handleSandboxCreate: SAR denied sandboxes/create for %s/%s; "+
				"treating snapshot as unavailable and falling back to cold/warm: %v",
				sandboxReq.Namespace, sandboxReq.Name, sarErr)
			readySnap = nil
		}
	}

	forceDirectSandbox := readySnap != nil

	var sandbox *sandboxv1alpha1.Sandbox
	var sandboxClaim *extensionsv1alpha1.SandboxClaim
	var sandboxEntry *sandboxEntry
	var err error
	switch sandboxReq.Kind {
	case types.AgentRuntimeKind:
		sandbox, sandboxEntry, err = buildSandboxByAgentRuntime(sandboxReq.Namespace, sandboxReq.Name, s.informers)
	case types.CodeInterpreterKind:
		sandbox, sandboxClaim, sandboxEntry, err = buildSandboxByCodeInterpreter(sandboxReq.Namespace, sandboxReq.Name, s.informers, forceDirectSandbox)
	}

	if err != nil {
		klog.Errorf("build sandbox failed %s/%s: %v", sandboxReq.Namespace, sandboxReq.Name, err)
		if errors.Is(err, api.ErrAgentRuntimeNotFound) || errors.Is(err, api.ErrCodeInterpreterNotFound) {
			respondError(c, http.StatusNotFound, err.Error())
		} else {
			respondError(c, http.StatusInternalServerError, "internal server error")
		}
		return
	}

	// Track restore attempt for metrics.
	if readySnap != nil {
		snapshotRestoreTotal.WithLabelValues(sandboxReq.Namespace, sandboxReq.Name, "attempted").Inc()
	}

	// Inject snapshot annotations when a ready snapshot is available.
	if readySnap != nil {
		if sandbox.Annotations == nil {
			sandbox.Annotations = make(map[string]string)
		}
		// AgentCube-layer annotations: restore identity + observability.
		sandbox.Annotations[types.AnnotationSnapshotTemplateID] = readySnap.TemplateID
		sandbox.Annotations[types.AnnotationRestoredFromSnapshot] = readySnap.TemplateKey
		// Bind the sandbox to the node that holds the snapshot (snapshot is node-local).
		sandbox.Spec.PodTemplate.Spec.NodeName = readySnap.NodeName
		// Kuasar protocol annotations: must be in PodTemplate before Sandbox CR creation
		// so the sandboxer sees them when it starts the VM and sends PREPARE.
		injectKuasarRestoreAnnotations(sandbox, readySnap, sandboxEntry.SessionID)
		klog.Infof("handleSandboxCreate: using snapshot template %s on node %s for %s/%s",
			readySnap.TemplateID, readySnap.NodeName, sandboxReq.Namespace, sandboxReq.Name)
	}

	// Calculate sandbox name and namespace before creating
	sandboxName := sandbox.Name
	namespace := sandbox.Namespace

	dynamicClient := s.k8sClient.dynamicClient
	if s.config.EnableAuth {
		userDynamicClient, errExtractClient := s.extractUserK8sClient(c)
		if errExtractClient != nil {
			klog.Infof("extract user k8s client failed: %v", errExtractClient)
			respondError(c, http.StatusUnauthorized, errExtractClient.Error())
			return
		}
		dynamicClient = userDynamicClient

		// Issue 9: Explicit SAR — determine the actual resource type first (based on snapshot
		// availability), then check create permission for that exact type so the SAR resource
		// matches what will actually be created.
		sarResource := "sandboxclaims"
		if forceDirectSandbox || sandboxClaim == nil {
			sarResource = "sandboxes"
		}
		if sarErr := s.checkResourceCreatePermission(c.Request.Context(), userDynamicClient,
			sandboxReq.Namespace, sarResource); sarErr != nil {
			klog.Infof("SAR denied create %s in %s: %v", sarResource, sandboxReq.Namespace, sarErr)
			respondError(c, http.StatusForbidden, sarErr.Error())
			return
		}
	}

	// CRITICAL: Register watcher BEFORE creating sandbox
	// This ensures we don't miss the Running state notification
	resultChan := s.sandboxController.WatchSandboxOnce(c.Request.Context(), namespace, sandboxName)
	// Ensure cleanup is called when function returns to prevent memory leak
	defer s.sandboxController.UnWatchSandbox(namespace, sandboxName)

	response, err := s.createSandbox(c.Request.Context(), dynamicClient, sandbox, sandboxClaim, sandboxEntry, resultChan)
	if err != nil && readySnap != nil {
		snapshotRestoreTotal.WithLabelValues(sandboxReq.Namespace, sandboxReq.Name, "failure").Inc()
		// Snapshot restore failed — increment the failure counter on the SnapStart object so
		// SnapshotController can trigger Invalidated + rebuild after 3 consecutive failures.
		// This is best-effort: a PATCH failure here is non-fatal (fallback still proceeds).
		if incrErr := s.incrementRestoreFailureCount(c.Request.Context(), sandboxReq.Namespace, sandboxReq.Name); incrErr != nil {
			klog.Warningf("handleSandboxCreate: failed to increment restoreFailureCount for %s/%s: %v",
				sandboxReq.Namespace, sandboxReq.Name, incrErr)
		}

		// Fallback to cold start. Clear readySnap so that success metrics and events
		// are not attributed to a snapshot restore that actually failed.
		s.emitSnapStartWarning(sandboxReq.Namespace, sandboxReq.Name, "SandboxRestoreFallback",
			"snapshot restore failed for %s/%s; falling back to cold start: %v",
			sandboxReq.Namespace, sandboxReq.Name, err)
		klog.Warningf("snapshot restore failed for %s/%s, falling back to cold start: %v",
			sandboxReq.Namespace, sandboxReq.Name, err)
		readySnap = nil
		sandbox, sandboxClaim, sandboxEntry, err = buildSandboxByCodeInterpreter(sandboxReq.Namespace, sandboxReq.Name, s.informers, false)
		if err == nil {
			sandboxName = sandbox.Name
			namespace = sandbox.Namespace
			resultChan2 := s.sandboxController.WatchSandboxOnce(c.Request.Context(), namespace, sandboxName)
			defer s.sandboxController.UnWatchSandbox(namespace, sandboxName)
			response, err = s.createSandbox(c.Request.Context(), dynamicClient, sandbox, sandboxClaim, sandboxEntry, resultChan2)
		}
	}
	if err != nil {
		// Client disconnected — abort with 499 so logs/metrics reflect the cancellation.
		if errors.Is(err, context.Canceled) {
			klog.Warningf("create sandbox aborted %s/%s: client disconnected", sandbox.Namespace, sandbox.Name)
			c.AbortWithStatus(499)
			return
		}
		// Deadline exceeded — client may still be connected; return 504 so they get a meaningful response.
		if errors.Is(err, context.DeadlineExceeded) {
			klog.Warningf("create sandbox timed out %s/%s: request deadline exceeded", sandbox.Namespace, sandbox.Name)
			respondError(c, http.StatusGatewayTimeout, "request timed out")
			return
		}
		// Internal sandbox-ready wait timed out; surface as 504 rather than a generic 500.
		if errors.Is(err, errSandboxCreationTimeout) {
			klog.Warningf("create sandbox timed out %s/%s: sandbox did not become ready within deadline", sandbox.Namespace, sandbox.Name)
			respondError(c, http.StatusGatewayTimeout, err.Error())
			return
		}
		klog.Errorf("create sandbox failed %s/%s: %v", sandbox.Namespace, sandbox.Name, err)
		// Internal errors (store, K8s API) must not leak system details to callers;
		// sandbox-level failures (terminal pod state, timeout) are safe to surface.
		msg := err.Error()
		if apierrors.IsInternalError(err) {
			msg = "internal server error"
		}
		respondError(c, http.StatusInternalServerError, msg)
		return
	}

	if readySnap != nil {
		snapshotRestoreTotal.WithLabelValues(sandboxReq.Namespace, sandboxReq.Name, "success").Inc()
		if resetErr := s.resetRestoreFailureCount(c.Request.Context(), sandboxReq.Namespace, sandboxReq.Name); resetErr != nil {
			klog.Warningf("handleSandboxCreate: failed to reset restoreFailureCount for %s/%s: %v",
				sandboxReq.Namespace, sandboxReq.Name, resetErr)
		}
		s.emitSnapStartEvent(sandboxReq.Namespace, sandboxReq.Name, "SandboxRestoredFromSnapshot",
			"sandbox restored from WarmForkSnapshot template %s on node %s",
			readySnap.TemplateKey, readySnap.NodeName)
	}
	respondJSON(c, http.StatusOK, response)
}

func (s *Server) resetRestoreFailureCount(ctx context.Context, namespace, runtimeName string) error {
	snapStarts := s.snapshotController.indexer.getByRuntime(namespace, runtimeName)
	if len(snapStarts) == 0 || snapStarts[0].Status.Snapshot == nil ||
		snapStarts[0].Status.Snapshot.RestoreFailureCount == 0 || s.k8sClient == nil ||
		s.k8sClient.dynamicClient == nil {
		return nil
	}
	patchBytes, err := json.Marshal(map[string]interface{}{
		"status": map[string]interface{}{
			"snapshot": map[string]interface{}{"restoreFailureCount": 0},
		},
	})
	if err != nil {
		return err
	}
	_, err = s.k8sClient.dynamicClient.Resource(SnapStartGVR).Namespace(namespace).
		Patch(ctx, snapStarts[0].Name, k8stypes.MergePatchType, patchBytes, metav1.PatchOptions{}, "status")
	return err
}

// createK8sResources creates the K8s sandbox or sandbox claim resource.
func (s *Server) createK8sResources(ctx context.Context, dynamicClient dynamic.Interface, sandbox *sandboxv1alpha1.Sandbox, sandboxClaim *extensionsv1alpha1.SandboxClaim) error {
	if sandboxClaim != nil {
		if err := createSandboxClaim(ctx, dynamicClient, sandboxClaim); err != nil {
			if isContextError(err) {
				return err
			}
			return api.NewInternalError(fmt.Errorf("create sandbox claim %s/%s failed: %w", sandboxClaim.Namespace, sandboxClaim.Name, err))
		}
	} else {
		if _, err := createSandbox(ctx, dynamicClient, sandbox); err != nil {
			if isContextError(err) {
				return err
			}
			return api.NewInternalError(fmt.Errorf("failed to create sandbox: %w", err))
		}
	}
	return nil
}

// createSandbox performs sandbox creation and returns the response payload or an error with an HTTP status code.
func (s *Server) createSandbox(ctx context.Context, dynamicClient dynamic.Interface, sandbox *sandboxv1alpha1.Sandbox, sandboxClaim *extensionsv1alpha1.SandboxClaim, sandboxEntry *sandboxEntry, resultChan <-chan SandboxStatusUpdate) (*types.CreateSandboxResponse, error) {
	placeholder := buildSandboxPlaceHolder(sandbox, sandboxEntry)
	if err := s.storeClient.StoreSandbox(ctx, placeholder); err != nil {
		if isContextError(err) {
			return nil, err
		}
		return nil, api.NewInternalError(fmt.Errorf("store sandbox placeholder failed: %w", err))
	}

	// Register rollback right after the placeholder is stored so that a K8s
	// creation failure does not leave an orphaned store entry.
	needRollbackSandbox := true
	defer func() {
		if !needRollbackSandbox {
			return
		}
		s.rollbackSandboxCreation(dynamicClient, sandbox, sandboxClaim, sandboxEntry.SessionID)
	}()

	if err := s.createK8sResources(ctx, dynamicClient, sandbox, sandboxClaim); err != nil {
		return nil, err
	}

	// Use NewTimer so we can stop it explicitly when another branch wins,
	// preventing the runtime from retaining the timer until it fires.
	timer := time.NewTimer(2 * time.Minute) // consistent with router settings

	var createdSandbox *sandboxv1alpha1.Sandbox
	select {
	case result := <-resultChan:
		timer.Stop()
		createdSandbox = result.Sandbox
		klog.V(2).Infof("sandbox %s/%s reported ready, verifying entrypoints", createdSandbox.Namespace, createdSandbox.Name)
	case <-ctx.Done():
		timer.Stop()
		klog.Warningf("sandbox %s/%s wait canceled: %v", sandbox.Namespace, sandbox.Name, ctx.Err())
		return nil, ctx.Err()
	case <-timer.C:
		klog.Warningf("sandbox %s/%s create timed out", sandbox.Namespace, sandbox.Name)
		return nil, errSandboxCreationTimeout
	}

	// agent-sandbox create pod with same name as sandbox if no warmpool is used
	// so here we try to get pod IP by sandbox name first
	// if warmpool is used, the pod name is stored in sandbox's annotation `agents.x-k8s.io/sandbox-pod-name`
	// https://github.com/kubernetes-sigs/agent-sandbox/blob/3ab7fbcd85ad0d75c6e632ecd14bcaeda5e76e1e/controllers/sandbox_controller.go#L465
	sandboxPodName := sandbox.Name
	if podName, exists := createdSandbox.Annotations[controllers.SandboxPodNameAnnotation]; exists {
		sandboxPodName = podName
	}

	podIP, err := s.k8sClient.GetSandboxPodIP(ctx, sandbox.Namespace, sandbox.Name, sandboxPodName)
	if err != nil {
		if isContextError(err) {
			return nil, err
		}
		return nil, api.NewInternalError(fmt.Errorf("failed to get sandbox %s/%s pod IP: %w", sandbox.Namespace, sandbox.Name, err))
	}
	if err := s.waitForSandboxEntryPointsReady(ctx, podIP, sandboxEntry); err != nil {
		if isContextError(err) {
			return nil, err
		}
		return nil, api.NewInternalError(fmt.Errorf("failed to verify sandbox %s/%s entrypoints: %w", sandbox.Namespace, sandbox.Name, err))
	}

	storeCacheInfo := buildSandboxInfo(createdSandbox, podIP, sandboxEntry)

	response := &types.CreateSandboxResponse{
		Kind:        storeCacheInfo.Kind,
		SessionID:   sandboxEntry.SessionID,
		SandboxID:   storeCacheInfo.SandboxID,
		SandboxName: sandbox.Name,
		EntryPoints: storeCacheInfo.EntryPoints,
	}

	if err := s.storeClient.UpdateSandbox(ctx, storeCacheInfo); err != nil {
		if isContextError(err) {
			return nil, err
		}
		return nil, api.NewInternalError(fmt.Errorf("update store cache failed: %w", err))
	}

	needRollbackSandbox = false
	klog.V(2).Infof("init sandbox %s/%s successfully, kind: %s, sessionID: %s", createdSandbox.Namespace,
		createdSandbox.Name, createdSandbox.Kind, sandboxEntry.SessionID)
	return response, nil
}

// rollbackSandboxCreation deletes the sandbox (or sandbox claim) and its store
// placeholder when creation fails. It runs in a fresh context so that a
// canceled request context does not prevent cleanup.
func (s *Server) rollbackSandboxCreation(dynamicClient dynamic.Interface, sandbox *sandboxv1alpha1.Sandbox, sandboxClaim *extensionsv1alpha1.SandboxClaim, sessionID string) {
	ctxTimeout, cancel := context.WithTimeout(context.Background(), storeCleanupTimeout)
	defer cancel()
	if sandboxClaim != nil {
		if err := deleteSandboxClaim(ctxTimeout, dynamicClient, sandboxClaim.Namespace, sandboxClaim.Name); err != nil {
			klog.Infof("sandbox claim %s/%s rollback failed: %v", sandboxClaim.Namespace, sandboxClaim.Name, err)
		} else {
			klog.Infof("sandbox claim %s/%s rollback succeeded", sandboxClaim.Namespace, sandboxClaim.Name)
		}
	} else {
		if err := deleteSandbox(ctxTimeout, dynamicClient, sandbox.Namespace, sandbox.Name); err != nil {
			klog.Infof("sandbox %s/%s rollback failed: %v", sandbox.Namespace, sandbox.Name, err)
		} else {
			klog.Infof("sandbox %s/%s rollback succeeded", sandbox.Namespace, sandbox.Name)
		}
	}
	if delErr := s.storeClient.DeleteSandboxBySessionID(ctxTimeout, sessionID); delErr != nil {
		klog.Infof("sandbox %s/%s store placeholder cleanup failed: %v", sandbox.Namespace, sandbox.Name, delErr)
	}
}

// handleDeleteSandbox handles sandbox deletion requests
func (s *Server) handleDeleteSandbox(c *gin.Context) {
	sessionID := c.Param("sessionId")
	// Query sandbox from store
	sandbox, err := s.storeClient.GetSandboxBySessionID(c.Request.Context(), sessionID)
	if err != nil {
		if errors.Is(err, store.ErrNotFound) {
			respondError(c, http.StatusNotFound, fmt.Sprintf("Session ID %s not found, maybe already deleted", sessionID))
			return
		}
		klog.Errorf("get sandbox from store by sessionID %s failed: %v", sessionID, err)
		respondError(c, http.StatusInternalServerError, "internal server error")
		return
	}

	dynamicClient := s.k8sClient.dynamicClient
	if s.config.EnableAuth {
		userDynamicClient, err := s.extractUserK8sClient(c)
		if err != nil {
			respondError(c, http.StatusUnauthorized, err.Error())
			return
		}
		dynamicClient = userDynamicClient
	}

	if sandbox.Kind == types.SandboxClaimsKind {
		err = deleteSandboxClaim(c.Request.Context(), dynamicClient, sandbox.SandboxNamespace, sandbox.Name)
		if err != nil {
			if apierrors.IsNotFound(err) {
				// Already deleted, consider as success
				klog.Infof("sandbox claim %s/%s already deleted", sandbox.SandboxNamespace, sandbox.Name)
			} else {
				klog.Errorf("failed to delete sandbox claim %s/%s: %v", sandbox.SandboxNamespace, sandbox.Name, err)
				respondError(c, http.StatusInternalServerError, "internal server error")
				return
			}
		}
	} else {
		err = deleteSandbox(c.Request.Context(), dynamicClient, sandbox.SandboxNamespace, sandbox.Name)
		if err != nil {
			if apierrors.IsNotFound(err) {
				// Already deleted, consider as success
				klog.Infof("sandbox %s/%s already deleted", sandbox.SandboxNamespace, sandbox.Name)
			} else {
				klog.Errorf("failed to delete sandbox %s/%s: %v", sandbox.SandboxNamespace, sandbox.Name, err)
				respondError(c, http.StatusInternalServerError, "internal server error")
				return
			}
		}
	}

	// Use a detached context for the store delete so a client disconnect
	// after K8s deletion doesn't orphan the store entry.
	deleteCtx, cancel := context.WithTimeout(context.Background(), storeCleanupTimeout)
	defer cancel()

	// Delete sandbox from store
	err = s.storeClient.DeleteSandboxBySessionID(deleteCtx, sessionID)
	if err != nil {
		klog.Errorf("delete %s %s/%s from store by sessionID %s failed: %v", sandbox.Kind, sandbox.SandboxNamespace, sandbox.Name, sessionID, err)
		respondError(c, http.StatusInternalServerError, "internal server error")
		return
	}

	klog.Infof("delete %s %s/%s successfully, sessionID: %v ", sandbox.Kind, sandbox.SandboxNamespace, sandbox.Name, sandbox.SessionID)
	respondJSON(c, http.StatusOK, map[string]string{
		"message": "Sandbox deleted successfully",
	})
}

// checkResourceCreatePermission issues a SelfSubjectAccessReview to verify that the
// caller (bound to dynamicClient) can create the given resource in the given namespace.
func (s *Server) checkResourceCreatePermission(ctx context.Context, dynamicClient dynamic.Interface, namespace, resource string) error {
	sarGVR := sarGroupVersionResource()
	sarObj := buildSelfSubjectAccessReview(namespace, resource)
	result, err := dynamicClient.Resource(sarGVR).Create(ctx, sarObj, metav1.CreateOptions{})
	if err != nil {
		return fmt.Errorf("SelfSubjectAccessReview failed: %w", err)
	}
	allowed, _, _ := unstructured.NestedBool(result.Object, "status", "allowed")
	if !allowed {
		return fmt.Errorf("forbidden: no create permission on %s in namespace %s", resource, namespace)
	}
	return nil
}

func sarGroupVersionResource() schema.GroupVersionResource {
	return schema.GroupVersionResource{
		Group:    "authorization.k8s.io",
		Version:  "v1",
		Resource: "selfsubjectaccessreviews",
	}
}

func buildSelfSubjectAccessReview(namespace, resource string) *unstructured.Unstructured {
	// sandboxclaims live under extensions.agents.x-k8s.io; all other resources under agents.x-k8s.io.
	group := "agents.x-k8s.io"
	if resource == "sandboxclaims" {
		group = "extensions.agents.x-k8s.io"
	}
	return &unstructured.Unstructured{
		Object: map[string]interface{}{
			"apiVersion": "authorization.k8s.io/v1",
			"kind":       "SelfSubjectAccessReview",
			"spec": map[string]interface{}{
				"resourceAttributes": map[string]interface{}{
					"namespace": namespace,
					"verb":      "create",
					"resource":  resource,
					"group":     group,
				},
			},
		},
	}
}

// incrementRestoreFailureCount increments SnapStart.status.snapshot.restoreFailureCount
// for the SnapStart that references the given CodeInterpreter (namespace/name).
// The SnapshotController informer watches this field and triggers Invalidated + rebuild when it reaches 3.
// Uses optimistic concurrency (read → increment → patch with resourceVersion) to avoid
// losing concurrent increments. Retries up to 5 times on 409 Conflict.
func (s *Server) incrementRestoreFailureCount(ctx context.Context, namespace, runtimeName string) error {
	// Use indexer as a fast hint for the SnapStart name.
	// Fall back to an API server list if the indexer hasn't synced yet (e.g., after restart).
	snapStarts := s.snapshotController.indexer.getByRuntime(namespace, runtimeName)
	var ssName string
	if len(snapStarts) > 0 {
		ssName = snapStarts[0].Name
	} else {
		list, listErr := s.k8sClient.dynamicClient.Resource(SnapStartGVR).Namespace(namespace).
			List(ctx, metav1.ListOptions{})
		if listErr != nil || list == nil {
			return nil
		}
		for i := range list.Items {
			ref, _, _ := unstructured.NestedString(list.Items[i].Object, "spec", "runtimeRef", "name")
			if ref == runtimeName {
				ssName = list.Items[i].GetName()
				break
			}
		}
	}
	if ssName == "" {
		return nil
	}

	// RetryOnConflict performs a read–increment–patch loop, retrying on 409 Conflict
	// so concurrent restore failures each contribute their increment to the counter.
	return retry.RetryOnConflict(retry.DefaultRetry, func() error {
		obj, err := s.k8sClient.dynamicClient.Resource(SnapStartGVR).Namespace(namespace).
			Get(ctx, ssName, metav1.GetOptions{})
		if err != nil {
			return fmt.Errorf("get SnapStart %s/%s for failure count: %w", namespace, ssName, err)
		}
		var ss runtimev1alpha1.SnapStart
		if err := unstructuredToSnapStart(obj, &ss); err != nil {
			return fmt.Errorf("parse SnapStart %s/%s: %w", namespace, ssName, err)
		}
		currentCount := int32(0)
		if ss.Status.Snapshot != nil {
			currentCount = ss.Status.Snapshot.RestoreFailureCount
		}
		newCount := currentCount + 1

		// Include metadata.resourceVersion so the API server rejects concurrent writes (409 Conflict),
		// enabling retry.RetryOnConflict to re-read and retry with the updated count.
		patch := map[string]interface{}{
			"metadata": map[string]interface{}{
				"resourceVersion": obj.GetResourceVersion(),
			},
			"status": map[string]interface{}{
				"snapshot": map[string]interface{}{
					"restoreFailureCount": newCount,
				},
			},
		}
		patchBytes, err := json.Marshal(patch)
		if err != nil {
			return fmt.Errorf("marshal restoreFailureCount patch: %w", err)
		}
		_, err = s.k8sClient.dynamicClient.Resource(SnapStartGVR).Namespace(namespace).
			Patch(ctx, ssName, k8stypes.MergePatchType, patchBytes, metav1.PatchOptions{}, "status")
		if err != nil {
			return err
		}
		klog.V(2).Infof("incrementRestoreFailureCount: %s/%s restoreFailureCount=%d", namespace, ssName, newCount)
		return nil
	})
}

// emitSnapStartEvent emits a Normal Kubernetes event on the first SnapStart found for the runtime.
func (s *Server) emitSnapStartEvent(namespace, runtimeName, reason, msgFmt string, args ...interface{}) {
	if s.snapshotController == nil || s.snapshotController.recorder == nil {
		return
	}
	snapStarts := s.snapshotController.indexer.getByRuntime(namespace, runtimeName)
	if len(snapStarts) == 0 {
		return
	}
	s.snapshotController.recorder.Eventf(snapStarts[0], corev1.EventTypeNormal, reason, msgFmt, args...)
}

// emitSnapStartWarning emits a Warning Kubernetes event on the first SnapStart found for the runtime.
func (s *Server) emitSnapStartWarning(namespace, runtimeName, reason, msgFmt string, args ...interface{}) {
	if s.snapshotController == nil || s.snapshotController.recorder == nil {
		return
	}
	snapStarts := s.snapshotController.indexer.getByRuntime(namespace, runtimeName)
	if len(snapStarts) == 0 {
		return
	}
	s.snapshotController.recorder.Eventf(snapStarts[0], corev1.EventTypeWarning, reason, msgFmt, args...)
}
