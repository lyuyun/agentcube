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

// Package controller implements the node-agent side of the snapshot build path.
// It watches SandboxSnapshotTask objects assigned to this node and drives
// snapshot creation through the registered SnapshotDriver.
package controller

import (
	"context"
	"fmt"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
	sandboxv1alpha1 "sigs.k8s.io/agent-sandbox/api/v1alpha1"
	sandboxcontrollers "sigs.k8s.io/agent-sandbox/controllers"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/builder"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/predicate"

	agentdriver "github.com/volcano-sh/agentcube/pkg/agentd/driver"
	runtimev1alpha1 "github.com/volcano-sh/agentcube/pkg/apis/runtime/v1alpha1"
)

const (
	// retryMinInterval is the initial delay between driver.Create attempts.
	retryMinInterval = 10 * time.Second
	// retryMaxInterval is the maximum delay between driver.Create attempts.
	retryMaxInterval = 300 * time.Second
)

// SnapshotTaskController watches SandboxSnapshotTask objects assigned to this node
// and drives snapshot creation via the single registered SnapshotDriver.
type SnapshotTaskController struct {
	client.Client
	// APIReader bypasses the informer cache for one-shot lookups (e.g. Pod UID),
	// avoiding the need for list/watch RBAC on those resources.
	APIReader client.Reader
	Scheme    *runtime.Scheme
	Recorder  record.EventRecorder
	NodeName  string
	Driver    agentdriver.SnapshotDriver
}

func (r *SnapshotTaskController) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	logger := log.FromContext(ctx)

	task := &runtimev1alpha1.SandboxSnapshotTask{}
	if err := r.Get(ctx, req.NamespacedName, task); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}

	// Only handle tasks targeting this node.
	if task.Spec.TargetNodeName != r.NodeName {
		return ctrl.Result{}, nil
	}

	// Skip tasks that have already reached Ready.
	if task.Status.Phase == runtimev1alpha1.SnapshotArtifactPhaseReady {
		return ctrl.Result{}, nil
	}

	// Validate required task fields (design §14 point 5).
	if done, err := r.validateTask(ctx, task); done || err != nil {
		return ctrl.Result{}, err
	}

	// Verify this task targets our driver's provider name.
	info, err := r.Driver.GetPluginInfo(ctx)
	if err != nil {
		return ctrl.Result{RequeueAfter: driverRetryInterval(task.CreationTimestamp.Time)}, nil
	}
	if task.Spec.ProviderName != info.Name {
		logger.Info("task providerName does not match driver, skipping",
			"task", task.Name, "taskProvider", task.Spec.ProviderName, "driverName", info.Name)
		return ctrl.Result{}, nil
	}

	// Verify the driver supports the requested snapshot mode.
	caps, err := r.Driver.GetPluginCapabilities(ctx)
	if err != nil {
		return ctrl.Result{RequeueAfter: driverRetryInterval(task.CreationTimestamp.Time)}, nil
	}
	if !capSupportsMode(caps, task.Spec.SnapshotMode) {
		msg := fmt.Sprintf("driver %q does not support snapshot mode %q", info.Name, task.Spec.SnapshotMode)
		logger.Info(msg+", retrying", "task", task.Name)
		r.Recorder.Event(task, corev1.EventTypeWarning, "SnapshotModeNotSupported", msg)
		if err := r.reportCreating(ctx, task, msg); err != nil {
			logger.Error(err, "patch task status for unsupported mode", "task", task.Name)
		}
		return ctrl.Result{RequeueAfter: driverRetryInterval(task.CreationTimestamp.Time)}, nil
	}

	sandbox, result, done, err := r.validateTargetSandbox(ctx, task)
	if done || err != nil {
		return result, err
	}

	podUID, err := r.resolvePodUID(ctx, sandbox)
	if err != nil {
		msg := fmt.Sprintf("resolve pod UID for sandbox %s: %v", sandbox.Name, err)
		logger.Info(msg+", retrying", "task", task.Name)
		r.Recorder.Event(task, corev1.EventTypeWarning, "PodUIDNotFound", msg)
		if patchErr := r.reportCreating(ctx, task, msg); patchErr != nil {
			logger.Error(patchErr, "patch task status", "task", task.Name)
		}
		return ctrl.Result{RequeueAfter: 5 * time.Second}, nil
	}

	logger.Info("calling snapshot driver", "task", task.Name, "provider", info.Name, "podUID", podUID)

	snapshot, err := r.Driver.CreateSnapshot(ctx, agentdriver.CreateSnapshotRequest{
		TaskRef: corev1.ObjectReference{
			APIVersion: runtimev1alpha1.GroupVersion.String(),
			Kind:       "SandboxSnapshotTask",
			Namespace:  task.Namespace,
			Name:       task.Name,
			UID:        task.UID,
		},
		TargetSandboxRef: task.Spec.TargetSandboxRef,
		TargetNodeName:   task.Spec.TargetNodeName,
		SnapshotName:     task.Spec.SnapshotKey,
		SnapshotHash:     task.Spec.SnapshotHash,
		Mode:             task.Spec.SnapshotMode,
		PodUID:           podUID,
	})
	if err != nil {
		logger.Error(err, "snapshot driver create failed, retrying", "task", task.Name)
		r.Recorder.Event(task, corev1.EventTypeWarning, "DriverCreateFailed", err.Error())
		if patchErr := r.reportCreating(ctx, task, err.Error()); patchErr != nil {
			logger.Error(patchErr, "patch task status after driver create failure", "task", task.Name)
		}
		return ctrl.Result{RequeueAfter: driverRetryInterval(task.CreationTimestamp.Time)}, nil
	}

	logger.Info("snapshot created", "task", task.Name, "snapshotName", snapshot.SnapshotName)
	r.Recorder.Event(task, corev1.EventTypeNormal, "SnapshotCreated", "snapshot driver create succeeded")
	return r.reportReady(ctx, task)
}

// validateTask checks that required task fields are present (design §14 point 5).
// Target node and driver are validated separately in Reconcile; stale-task safety
// is provided by ownerReference cascade deletion, not by re-fetching the snapshot.
func (r *SnapshotTaskController) validateTask(ctx context.Context, task *runtimev1alpha1.SandboxSnapshotTask) (bool, error) {
	logger := log.FromContext(ctx)
	if task.Spec.SnapshotUID == "" || task.Spec.SnapshotKey == "" || task.Spec.SnapshotHash == "" {
		logger.Info("task missing required fields, skipping", "task", task.Name)
		return true, nil
	}
	return false, nil
}

// validateTargetSandbox waits until the target Sandbox has the Ready condition before
// allowing the driver to proceed. All snapshot modes require a running sandbox.
func (r *SnapshotTaskController) validateTargetSandbox(ctx context.Context, task *runtimev1alpha1.SandboxSnapshotTask) (*sandboxv1alpha1.Sandbox, ctrl.Result, bool, error) {
	sandbox := &sandboxv1alpha1.Sandbox{}
	err := r.Get(ctx, types.NamespacedName{Name: task.Spec.TargetSandboxRef.Name, Namespace: task.Namespace}, sandbox)
	if err != nil {
		if apierrors.IsNotFound(err) {
			return nil, ctrl.Result{RequeueAfter: 5 * time.Second}, true, nil
		}
		return nil, ctrl.Result{}, true, fmt.Errorf("get sandbox %s/%s: %w", task.Namespace, task.Spec.TargetSandboxRef.Name, err)
	}
	for _, cond := range sandbox.Status.Conditions {
		if cond.Type == string(sandboxv1alpha1.SandboxConditionReady) && cond.Status == metav1.ConditionTrue {
			return sandbox, ctrl.Result{}, false, nil
		}
	}
	return nil, ctrl.Result{RequeueAfter: 5 * time.Second}, true, nil
}

// resolvePodUID returns the UID of the running Pod backing the given Sandbox.
// Without warm pool the pod name equals the sandbox name; with warm pool it is
// stored in the agents.x-k8s.io/pod-name annotation.
func (r *SnapshotTaskController) resolvePodUID(ctx context.Context, sandbox *sandboxv1alpha1.Sandbox) (string, error) {
	podName := sandbox.Name
	if annotated, ok := sandbox.Annotations[sandboxcontrollers.SandboxPodNameAnnotation]; ok && annotated != "" {
		podName = annotated
	}

	pod := &corev1.Pod{}
	if err := r.APIReader.Get(ctx, types.NamespacedName{Name: podName, Namespace: sandbox.Namespace}, pod); err != nil {
		return "", fmt.Errorf("get pod %s/%s for sandbox %s: %w", sandbox.Namespace, podName, sandbox.Name, err)
	}
	if pod.Status.Phase != corev1.PodRunning || string(pod.UID) == "" {
		return "", fmt.Errorf("pod %s/%s is not running (phase=%s)", sandbox.Namespace, podName, pod.Status.Phase)
	}
	return string(pod.UID), nil
}

func (r *SnapshotTaskController) reportReady(ctx context.Context, task *runtimev1alpha1.SandboxSnapshotTask) (ctrl.Result, error) {
	patch := client.MergeFrom(task.DeepCopy())
	now := metav1.Now()
	task.Status.Phase = runtimev1alpha1.SnapshotArtifactPhaseReady
	task.Status.Message = ""
	task.Status.ObservedAt = &now
	return ctrl.Result{}, r.Status().Patch(ctx, task, patch)
}

func (r *SnapshotTaskController) reportCreating(ctx context.Context, task *runtimev1alpha1.SandboxSnapshotTask, msg string) error {
	if task.Status.Phase == runtimev1alpha1.SnapshotArtifactPhaseCreating && task.Status.Message == msg {
		return nil
	}
	patch := client.MergeFrom(task.DeepCopy())
	task.Status.Phase = runtimev1alpha1.SnapshotArtifactPhaseCreating
	task.Status.Message = msg
	return r.Status().Patch(ctx, task, patch)
}

// driverRetryInterval returns the next retry delay using task age as a
// stateless proxy for attempt count: doubles every minute from
// retryMinInterval, capped at retryMaxInterval.
func driverRetryInterval(createdAt time.Time) time.Duration {
	steps := int(time.Since(createdAt) / time.Minute)
	d := retryMinInterval
	for i := 0; i < steps && d < retryMaxInterval; i++ {
		d *= 2
	}
	if d > retryMaxInterval {
		return retryMaxInterval
	}
	return d
}

// capSupportsMode checks whether the capability list includes support for the given mode.
func capSupportsMode(caps []agentdriver.PluginCapability, mode runtimev1alpha1.SandboxSnapshotMode) bool {
	want := agentdriver.PluginCapabilityUnknown
	switch mode {
	case runtimev1alpha1.SandboxSnapshotModeFork:
		want = agentdriver.PluginCapabilityWarmFork
	case runtimev1alpha1.SandboxSnapshotModeResume:
		want = agentdriver.PluginCapabilityContinuation
	}
	for _, c := range caps {
		if c.Type == want {
			return true
		}
	}
	return false
}

// SetupWithManager registers the controller with the Manager.
func (r *SnapshotTaskController) SetupWithManager(mgr ctrl.Manager) error {
	r.Recorder = mgr.GetEventRecorderFor("snapshot-task-controller")

	nodeFilter := predicate.NewPredicateFuncs(func(obj client.Object) bool {
		task, ok := obj.(*runtimev1alpha1.SandboxSnapshotTask)
		if !ok {
			return false
		}
		return task.Spec.TargetNodeName == r.NodeName
	})

	return ctrl.NewControllerManagedBy(mgr).
		For(&runtimev1alpha1.SandboxSnapshotTask{}, builder.WithPredicates(nodeFilter)).
		Complete(r)
}
