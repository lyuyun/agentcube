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

package agentd

import (
	"context"
	"sync"
	"time"

	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/klog/v2"
	sandboxv1alpha1 "sigs.k8s.io/agent-sandbox/api/v1alpha1"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	runtimev1alpha1 "github.com/volcano-sh/agentcube/pkg/apis/runtime/v1alpha1"
	"github.com/volcano-sh/agentcube/pkg/workloadmanager"
)

// Reconciler reconciles a Sandbox object
type Reconciler struct {
	client.Client
	Scheme  *runtime.Scheme
	Drivers map[string]SnapshotDriver

	// restoredUIDs tracks which Sandbox UIDs have already had a restore attempted
	// in this process lifetime. Lost on restart; drivers must be idempotent.
	restoreMu    sync.Mutex
	restoredUIDs map[types.UID]struct{}
}

func (r *Reconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	sandbox := &sandboxv1alpha1.Sandbox{}
	err := r.Get(ctx, req.NamespacedName, sandbox)
	if err != nil {
		if errors.IsNotFound(err) {
			return ctrl.Result{}, nil
		}
		return ctrl.Result{}, err
	}

	r.maybeRestore(ctx, sandbox)

	lastActivityStr, exists := sandbox.Annotations[workloadmanager.LastActivityAnnotationKey]
	var lastActivity time.Time
	if exists && lastActivityStr != "" {
		lastActivity, err = time.Parse(time.RFC3339, lastActivityStr)
		if err != nil {
			return ctrl.Result{RequeueAfter: 30 * time.Second}, err
		}

		expirationTime := lastActivity.Add(workloadmanager.DefaultSandboxIdleTimeout)

		// Use custom idle timeout if defined.
		// Ignore invalid, zero, or negative values and fall back to the default timeout.
		if timeoutStr, exists := sandbox.Annotations[workloadmanager.IdleTimeoutAnnotationKey]; exists && timeoutStr != "" {
			if customTimeout, err := time.ParseDuration(timeoutStr); err == nil && customTimeout > 0 {
				expirationTime = lastActivity.Add(customTimeout)
			}
		}
		// Delete sandbox if expired
		if time.Now().After(expirationTime) {
			if err := r.Delete(ctx, sandbox); err != nil {
				if !errors.IsNotFound(err) {
					return ctrl.Result{}, err
				}
			}
		} else {
			return ctrl.Result{RequeueAfter: time.Until(expirationTime)}, nil
		}
	}

	return ctrl.Result{}, nil
}

// maybeRestore checks whether the Sandbox carries a snapshot restore intent and, if so,
// calls each registered driver once per Sandbox UID. The in-memory dedup set is lost on
// restart; drivers must handle duplicate calls idempotently.
// Restore errors are absorbed so the sandbox falls back to a cold start.
func (r *Reconciler) maybeRestore(ctx context.Context, sandbox *sandboxv1alpha1.Sandbox) {
	if len(r.Drivers) == 0 {
		return
	}
	if r.alreadyRestored(sandbox.UID) {
		return
	}
	snapshotKey := sandbox.Spec.PodTemplate.ObjectMeta.Annotations[runtimev1alpha1.SnapshotKeyAnnotation]
	if snapshotKey == "" {
		return
	}
	req := SnapshotDriverRestoreRequest{
		SandboxName:  sandbox.Name,
		Namespace:    sandbox.Namespace,
		SnapshotKey:  snapshotKey,
		SnapshotMode: runtimev1alpha1.SandboxSnapshotModeFork,
	}
	// Phase 1: single provider. Try all registered drivers; first attempt wins.
	for _, driver := range r.Drivers {
		if err := driver.Restore(ctx, req); err != nil {
			klog.V(2).InfoS("agentd: snapshot restore failed, falling back to cold start",
				"sandbox", sandbox.Name, "snapshotKey", snapshotKey, "driver", driver.Name(), "error", err)
		}
		break
	}
	r.markRestored(sandbox.UID)
}

func (r *Reconciler) alreadyRestored(uid types.UID) bool {
	r.restoreMu.Lock()
	defer r.restoreMu.Unlock()
	_, ok := r.restoredUIDs[uid]
	return ok
}

func (r *Reconciler) markRestored(uid types.UID) {
	r.restoreMu.Lock()
	defer r.restoreMu.Unlock()
	if r.restoredUIDs == nil {
		r.restoredUIDs = make(map[types.UID]struct{})
	}
	r.restoredUIDs[uid] = struct{}{}
}

// SetupWithManager sets up the controller with the Manager.
func (r *Reconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&sandboxv1alpha1.Sandbox{}).
		Complete(r)
}
