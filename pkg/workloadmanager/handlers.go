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

// isSessionSpecificEnv returns true for env vars that carry per-session identity and
// must be injected via the WarmFork PREPARE message rather than being baked into the snapshot.
func isSessionSpecificEnv(e corev1.EnvVar) bool {
	switch e.Name {
	case "PICOD_AUTH_PUBLIC_KEY":
		return true
	}
	return false
}

// snapshotVersionGate holds the current runtime's version parameters used to validate
// Redis snapshot entries before attaching restore intent.
// All fields except RuntimeGeneration are mandatory (non-empty required).
type snapshotVersionGate struct {
	SpecHash          string
	ImageRef          string
	Checkpoint        string
	ProtocolVersion   string
	RuntimeGeneration int64
}

// filterSnapshotsByVersion rejects Redis snapshot entries whose version fields do not
// match the current runtime. This is a synchronous guard against the SnapshotController
// convergence window: a CI spec update (image, args, resources) immediately makes old
// Redis entries stale, but SnapshotController may not have set activeMode=Cold yet.
// Without this check, attachWorkloadSnapStartIntent could attach a stale templateKey
// built from an older runtime spec.
//
// Rejection rules:
//  1. Any mandatory field (SpecHash, ImageRef, Checkpoint, ProtocolVersion) is empty → reject.
//  2. SpecHash mismatch → reject (args/env/resources/runtimeClass changed).
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

// attachWorkloadSnapStartIntent sets the AgentCube-layer logical SnapStart intent on a
// newly created direct Sandbox and propagates the Kuasar-facing protocol annotations to
// its PodTemplate. It must never be called for SandboxClaim-bound sandboxes (WarmPool
// hit path) — those are already running and must not be restored again.
//
// If no SnapStart is active for this runtime, or version validation rejects all Redis
// entries, the function returns without modifying the sandbox and it cold-starts normally.
func (s *Server) attachWorkloadSnapStartIntent(ctx context.Context, sandbox *sandboxv1alpha1.Sandbox, namespace, ciName, sessionID string) {
	if s.snapshotController == nil || s.storeClient == nil {
		return
	}
	snapStarts := s.snapshotController.indexer.getByRuntime(namespace, ciName)
	if len(snapStarts) == 0 {
		return
	}
	ss := snapStarts[0]
	if ss.Status.ActiveMode != runtimev1alpha1.SessionStartupModeSnapshot {
		klog.V(3).Infof("attachWorkloadSnapStartIntent: SnapStart %s/%s activeMode=%s; skipping",
			namespace, ss.Name, ss.Status.ActiveMode)
		return
	}

	snapInfos, err := s.storeClient.GetSnapshotNodes(ctx, namespace, ss.Name)
	if err != nil {
		klog.Warningf("attachWorkloadSnapStartIntent: GetSnapshotNodes %s/%s failed; skipping: %v",
			namespace, ss.Name, err)
		return
	}

	// Reject entries whose SnapStartUID doesn't match the current SnapStart.
	var current []*types.SnapshotInfo
	for _, info := range snapInfos {
		if info.SnapStartUID == string(ss.UID) {
			current = append(current, info)
		}
	}

	// Guard against the SnapshotController convergence window: a CI spec update makes
	// old Redis entries stale immediately, but activeMode may not have been set to Cold
	// yet. filterSnapshotsByVersion rejects any entry whose build inputs no longer match
	// the current runtime, preventing restore from a mismatched snapshot template.
	ci, ciErr := s.snapshotController.getCodeInterpreter(namespace, ciName)
	if ciErr != nil {
		klog.Warningf("attachWorkloadSnapStartIntent: cannot get CI %s/%s for version gating; skipping: %v",
			namespace, ciName, ciErr)
		return
	}
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

	// Get the templateKey from any LocalReady placement. All LocalReady placements for the
	// same logical artifact carry the same templateKey; WM does not select a specific node.
	// Kuasar resolves the templateKey → node-local template ID on the actual startup node.
	var templateKey string
	for _, info := range current {
		if info.CacheState == string(runtimev1alpha1.CacheStateLocalReady) {
			templateKey = info.TemplateKey
			break
		}
	}
	if templateKey == "" {
		klog.V(3).Infof("attachWorkloadSnapStartIntent: no version-valid LocalReady placement for %s/%s; skipping",
			namespace, ss.Name)
		return
	}

	// AgentCube-layer logical intent on the Sandbox ObjectMeta.
	if sandbox.Annotations == nil {
		sandbox.Annotations = make(map[string]string)
	}
	sandbox.Annotations[types.AnnotationSnapStartRef] = namespace + "/" + ss.Name
	sandbox.Annotations[types.AnnotationSnapStartUID] = string(ss.UID)

	// Kuasar-facing protocol annotations on the PodTemplate.
	// These annotations rely on two unverified Kuasar contracts (Issue 6):
	//   1. kuasar.io/template-key is resolved to a node-local template ID by Kuasar at
	//      startup time; WM must not embed a node-local template_id or force a restore node.
	//   2. When no compatible local template exists, Kuasar cold-starts the same Sandbox
	//      transparently rather than failing it (confirmed by maintainers, no contract test yet).
	if sandbox.Spec.PodTemplate.ObjectMeta.Annotations == nil {
		sandbox.Spec.PodTemplate.ObjectMeta.Annotations = make(map[string]string)
	}
	ann := sandbox.Spec.PodTemplate.ObjectMeta.Annotations
	ann["kuasar.io/snapshot-type"] = "warm-fork"
	ann["kuasar.io/template-key"] = templateKey
	ann["kuasar.io/task-id"] = sessionID
	// TODO(lyuyun): propagate a per-sandbox restore-failure policy annotation to Kuasar
	// once the Kuasar annotation key/value contract is confirmed. This will allow
	// SnapStart.spec.onRestoreFailure (ColdStart|Fail) to override the Kuasar global switch.
	taskCtxBytes, _ := json.Marshal(map[string]string{"workspace": "/workspace/" + sessionID})
	ann["kuasar.io/task-context"] = string(taskCtxBytes)
	for _, c := range sandbox.Spec.PodTemplate.Spec.Containers {
		for _, e := range c.Env {
			if isSessionSpecificEnv(e) {
				ann["kuasar.io/task-env/"+e.Name] = e.Value
			}
		}
	}

	klog.V(2).Infof("attachWorkloadSnapStartIntent: attached SnapStart intent %s/%s (templateKey=%s) to sandbox %s/%s",
		namespace, ss.Name, templateKey, sandbox.Namespace, sandbox.Name)
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

	var sandbox *sandboxv1alpha1.Sandbox
	var sandboxClaim *extensionsv1alpha1.SandboxClaim
	var sandboxEntry *sandboxEntry
	var err error
	switch sandboxReq.Kind {
	case types.AgentRuntimeKind:
		sandbox, sandboxEntry, err = buildSandboxByAgentRuntime(sandboxReq.Namespace, sandboxReq.Name, s.informers)
	case types.CodeInterpreterKind:
		sandbox, sandboxClaim, sandboxEntry, err = buildSandboxByCodeInterpreter(sandboxReq.Namespace, sandboxReq.Name, s.informers)
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

	// Attach logical SnapStart intent to newly created direct Sandboxes only.
	// SandboxClaim-bound sandboxes (WarmPool hit) are already running — Kuasar has
	// already started them and must not be asked to restore again.
	// SnapStart and SandboxWarmPool are orthogonal: WarmPool refill Sandboxes receive
	// SnapStart intent too (created as direct Sandboxes by the refill controller).
	//
	// snapStartIntentAttached records whether a restore was attempted so the failure
	// counter can be incremented or reset on the outcome path below.
	var snapStartIntentAttached bool
	if kind == types.CodeInterpreterKind && sandboxClaim == nil {
		s.attachWorkloadSnapStartIntent(c.Request.Context(), sandbox, sandboxReq.Namespace, sandboxReq.Name, sandboxEntry.SessionID)
		snapStartIntentAttached = sandbox.Annotations[types.AnnotationSnapStartRef] != ""
	}

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

		sarResource := "sandboxclaims"
		if sandboxClaim == nil {
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
	defer s.sandboxController.UnWatchSandbox(namespace, sandboxName)

	response, err := s.createSandbox(c.Request.Context(), dynamicClient, sandbox, sandboxClaim, sandboxEntry, resultChan)
	if err != nil {
		if errors.Is(err, context.Canceled) {
			klog.Warningf("create sandbox aborted %s/%s: client disconnected", sandbox.Namespace, sandbox.Name)
			c.AbortWithStatus(499)
			return
		}
		if errors.Is(err, context.DeadlineExceeded) {
			klog.Warningf("create sandbox timed out %s/%s: request deadline exceeded", sandbox.Namespace, sandbox.Name)
			respondError(c, http.StatusGatewayTimeout, "request timed out")
			return
		}
		if errors.Is(err, errSandboxCreationTimeout) {
			klog.Warningf("create sandbox timed out %s/%s: sandbox did not become ready within deadline", sandbox.Namespace, sandbox.Name)
			respondError(c, http.StatusGatewayTimeout, err.Error())
			return
		}
		klog.Errorf("create sandbox failed %s/%s: %v", sandbox.Namespace, sandbox.Name, err)
		msg := err.Error()
		if apierrors.IsInternalError(err) {
			msg = "internal server error"
		}
		respondError(c, http.StatusInternalServerError, msg)
		return
	}

	// TODO(lyuyun): wire up incrementRestoreFailureCount / resetRestoreFailureCount and
	// emit SandboxRestoredFromSnapshot / SandboxRestoreFallback events once Kuasar reports
	// restore results (restored / cold_start_fallback / failure) back to AgentCube.
	// WM cannot distinguish these outcomes from Sandbox readiness state alone.
	_ = snapStartIntentAttached

	respondJSON(c, http.StatusOK, response)
}

// resetRestoreFailureCount resets SnapStart.status.snapshot.restoreFailureCount to 0
// on a successful session creation so transient failures do not accumulate toward the
// rebuild threshold.
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
