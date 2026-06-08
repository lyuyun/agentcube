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
// snapshot creation through registered SnapshotDrivers.
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
	sandboxv1alpha1 "sigs.k8s.io/agent-sandbox/api/v1alpha1"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/builder"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/predicate"

	agentdriver "github.com/volcano-sh/agentcube/pkg/agentd/driver"
	runtimev1alpha1 "github.com/volcano-sh/agentcube/pkg/apis/runtime/v1alpha1"
)

// taskBuildDeadline is the maximum time a SandboxSnapshotTask may remain
// in a non-terminal phase before the node agent marks it Failed.
const taskBuildDeadline = 10 * time.Minute

// SnapshotTaskController watches SandboxSnapshotTask objects assigned to this node
// and drives snapshot creation via the registered SnapshotDriver.
type SnapshotTaskController struct {
	client.Client
	Scheme   *runtime.Scheme
	NodeName string
	Drivers  map[string]agentdriver.SnapshotDriver
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

	// Skip tasks that have already reached a terminal phase.
	if task.Status.Phase == runtimev1alpha1.SnapshotArtifactPhaseReady ||
		task.Status.Phase == runtimev1alpha1.SnapshotArtifactPhaseFailed ||
		task.Status.Phase == runtimev1alpha1.SnapshotArtifactPhaseUnavailable {
		return ctrl.Result{}, nil
	}

	// Enforce an absolute build deadline to prevent hung tasks.
	if !task.CreationTimestamp.IsZero() && time.Since(task.CreationTimestamp.Time) > taskBuildDeadline {
		return r.reportFailed(ctx, task, fmt.Sprintf("snapshot build deadline exceeded (%s)", taskBuildDeadline))
	}

	// Validate required task fields (design §14 point 5).
	if done, err := r.validateTask(ctx, task); done || err != nil {
		return ctrl.Result{}, err
	}

	// Select driver.
	driver, ok := r.Drivers[task.Spec.ProviderName]
	if !ok {
		return r.reportFailed(ctx, task, fmt.Sprintf("no driver registered for provider %q", task.Spec.ProviderName))
	}

	// Validate driver capabilities.
	caps := driver.Capabilities(ctx)
	if !containsMode(caps.SnapshotModes, task.Spec.SnapshotMode) {
		return r.reportFailed(ctx, task, fmt.Sprintf("driver does not support snapshot mode %q", task.Spec.SnapshotMode))
	}

	if result, done, err := r.validateTargetSandbox(ctx, task); done || err != nil {
		return result, err
	}

	logger.Info("calling snapshot driver", "task", task.Name, "provider", task.Spec.ProviderName)

	artifact, err := driver.Create(ctx, agentdriver.SnapshotDriverCreateRequest{
		TaskRef: corev1.ObjectReference{
			APIVersion: runtimev1alpha1.GroupVersion.String(),
			Kind:       "SandboxSnapshotTask",
			Namespace:  task.Namespace,
			Name:       task.Name,
			UID:        task.UID,
		},
		TargetSandboxRef: task.Spec.TargetSandboxRef,
		TargetNodeName:   task.Spec.TargetNodeName,
		SnapshotMode:     task.Spec.SnapshotMode,
		ProviderName:     task.Spec.ProviderName,
		SnapshotKey:      task.Spec.SnapshotKey,
		SnapshotHash:     task.Spec.SnapshotHash,
	})
	if err != nil {
		logger.Error(err, "snapshot driver create failed", "task", task.Name)
		return r.reportFailed(ctx, task, err.Error())
	}

	logger.Info("snapshot driver create succeeded", "task", task.Name, "snapshotKey", artifact.SnapshotKey)
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
func (r *SnapshotTaskController) validateTargetSandbox(ctx context.Context, task *runtimev1alpha1.SandboxSnapshotTask) (ctrl.Result, bool, error) {
	sandbox := &sandboxv1alpha1.Sandbox{}
	err := r.Get(ctx, types.NamespacedName{Name: task.Spec.TargetSandboxRef.Name, Namespace: task.Namespace}, sandbox)
	if err != nil {
		if apierrors.IsNotFound(err) {
			return ctrl.Result{RequeueAfter: 5 * time.Second}, true, nil
		}
		return ctrl.Result{}, true, fmt.Errorf("get sandbox %s/%s: %w", task.Namespace, task.Spec.TargetSandboxRef.Name, err)
	}
	for _, cond := range sandbox.Status.Conditions {
		if cond.Type == string(sandboxv1alpha1.SandboxConditionReady) && cond.Status == metav1.ConditionTrue {
			return ctrl.Result{}, false, nil
		}
	}
	return ctrl.Result{RequeueAfter: 5 * time.Second}, true, nil
}

func (r *SnapshotTaskController) reportReady(ctx context.Context, task *runtimev1alpha1.SandboxSnapshotTask) (ctrl.Result, error) {
	return ctrl.Result{}, r.patchTaskStatus(ctx, task, runtimev1alpha1.SnapshotArtifactPhaseReady, "")
}

func (r *SnapshotTaskController) reportFailed(ctx context.Context, task *runtimev1alpha1.SandboxSnapshotTask, msg string) (ctrl.Result, error) {
	return ctrl.Result{}, r.patchTaskStatus(ctx, task, runtimev1alpha1.SnapshotArtifactPhaseFailed, msg)
}

func (r *SnapshotTaskController) patchTaskStatus(ctx context.Context, task *runtimev1alpha1.SandboxSnapshotTask, phase runtimev1alpha1.SnapshotArtifactPhase, msg string) error {
	patch := client.MergeFrom(task.DeepCopy())
	now := metav1.Now()
	task.Status.Phase = phase
	task.Status.Message = msg
	task.Status.ObservedAt = &now
	return r.Status().Patch(ctx, task, patch)
}

func containsMode(modes []runtimev1alpha1.SandboxSnapshotMode, mode runtimev1alpha1.SandboxSnapshotMode) bool {
	for _, m := range modes {
		if m == mode {
			return true
		}
	}
	return false
}

// SetupWithManager registers the controller with the Manager.
func (r *SnapshotTaskController) SetupWithManager(mgr ctrl.Manager) error {
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
