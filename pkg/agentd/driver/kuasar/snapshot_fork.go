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

package kuasar

import (
	"context"
	"fmt"
	"strings"

	"k8s.io/klog/v2"

	agentdriver "github.com/volcano-sh/agentcube/pkg/agentd/driver"
	runtimev1alpha1 "github.com/volcano-sh/agentcube/pkg/apis/runtime/v1alpha1"
)

// --- template-create ---

type snapshotForkCreateRequest struct {
	Action       string `json:"action"`
	SandboxID    string `json:"sandbox_id"`
	Key          string `json:"key"`
	SnapshotType string `json:"snapshot_type"`
}

type snapshotForkCreateResponse struct {
	TemplateID   string `json:"template_id"`
	Key          string `json:"key"`
	SnapshotType string `json:"snapshot_type"`
}

// createSnapshotFork creates a Fork-mode snapshot via the Kuasar admin socket.
//
// The sandboxer internally:
//  1. Verifies the pod annotation kuasar.io/warm-fork-ready-protocol-version=1.
//  2. Probes the workload readiness socket (CheckInjectSocket) to confirm the
//     process is in the quiescent state (blocking on accept()).
//  3. Pauses the VM, copies disk images, captures the VMM snapshot, and resumes.
//
// Returns a Ready artifact only after all steps complete. The Kuasar template_id
// is not surfaced in the returned artifact; it stays in the driver-internal
// kuasarArtifact type accessible via listSnapshotFork.
func (d *Driver) createSnapshotFork(ctx context.Context, req agentdriver.SnapshotDriverCreateRequest) (*agentdriver.SnapshotDriverArtifact, error) {
	klog.V(2).InfoS("kuasar driver: creating Fork snapshot",
		"snapshotKey", req.SnapshotKey,
		"sandbox", req.TargetSandboxRef.Name)

	sandboxID, err := d.resolveSandboxID(ctx, req.TaskRef.Namespace, req.TargetSandboxRef.Name)
	if err != nil {
		return nil, fmt.Errorf("kuasar driver: resolve sandbox ID for pod %s/%s: %w",
			req.TaskRef.Namespace, req.TargetSandboxRef.Name, err)
	}

	klog.V(2).InfoS("kuasar driver: resolved Kuasar sandbox ID",
		"sandbox", req.TargetSandboxRef.Name,
		"kuasarSandboxID", sandboxID)

	var resp snapshotForkCreateResponse
	if err := d.adminRPC(ctx, snapshotTimeout, snapshotForkCreateRequest{
		Action:       "template-create",
		SandboxID:    sandboxID,
		Key:          req.SnapshotKey,
		SnapshotType: "warm_fork",
	}, &resp); err != nil {
		return nil, fmt.Errorf("kuasar driver: template-create for sandbox %s (kuasarID=%s): %w",
			req.TargetSandboxRef.Name, sandboxID, err)
	}

	klog.V(2).InfoS("kuasar driver: Fork snapshot created",
		"snapshotKey", req.SnapshotKey,
		"templateID", resp.TemplateID)

	return &agentdriver.SnapshotDriverArtifact{
		ProviderName: ProviderName,
		SnapshotKey:  req.SnapshotKey,
		SnapshotHash: req.SnapshotHash,
	}, nil
}

// --- pool-gc ---

type snapshotForkDeleteRequest struct {
	Action     string `json:"action"`
	Kind       string `json:"kind"`
	TemplateID string `json:"template_id"`
}

// deleteSnapshotFork removes a Fork template from the Kuasar pool by template_id.
func (d *Driver) deleteSnapshotFork(ctx context.Context, templateID string) error {
	return d.adminRPC(ctx, adminTimeout, snapshotForkDeleteRequest{
		Action:     "pool-gc",
		Kind:       "warm_fork",
		TemplateID: templateID,
	}, nil)
}

// --- template-list ---

type snapshotForkListRequest struct {
	Action string `json:"action"`
}

type snapshotForkListResponse struct {
	Templates []adminTemplate `json:"templates"`
}

// listSnapshotFork returns all Fork artifacts known to the Kuasar sandboxer on this node
// as driver-internal kuasarArtifact records (carrying TemplateID).
func (d *Driver) listSnapshotFork(ctx context.Context) ([]kuasarArtifact, error) {
	var resp snapshotForkListResponse
	if err := d.adminRPC(ctx, adminTimeout, snapshotForkListRequest{Action: "template-list"}, &resp); err != nil {
		return nil, err
	}

	var artifacts []kuasarArtifact
	for _, t := range resp.Templates {
		if t.SnapshotType != "warm_fork" {
			continue
		}
		artifacts = append(artifacts, kuasarArtifact{
			Mode:        runtimev1alpha1.SandboxSnapshotModeFork,
			SnapshotKey: t.Key,
			TemplateID:  t.TemplateID,
		})
	}
	return artifacts, nil
}

// --- template-get ---

type snapshotForkInspectRequest struct {
	Action     string `json:"action"`
	TemplateID string `json:"template_id"`
}

type snapshotForkInspectResponse struct {
	Template *adminTemplate `json:"template"`
}

// inspectSnapshotFork returns the current status of a Fork artifact by template_id.
func (d *Driver) inspectSnapshotFork(ctx context.Context, templateID string) (*agentdriver.SnapshotDriverArtifactStatus, error) {
	var resp snapshotForkInspectResponse
	if err := d.adminRPC(ctx, adminTimeout, snapshotForkInspectRequest{
		Action:     "template-get",
		TemplateID: templateID,
	}, &resp); err != nil {
		if strings.Contains(err.Error(), "not found") {
			return &agentdriver.SnapshotDriverArtifactStatus{Phase: runtimev1alpha1.SnapshotArtifactPhaseUnavailable}, nil
		}
		return &agentdriver.SnapshotDriverArtifactStatus{Phase: runtimev1alpha1.SnapshotArtifactPhaseFailed}, nil
	}
	if resp.Template == nil || resp.Template.TemplateID == "" {
		return &agentdriver.SnapshotDriverArtifactStatus{Phase: runtimev1alpha1.SnapshotArtifactPhaseUnavailable}, nil
	}
	return &agentdriver.SnapshotDriverArtifactStatus{Phase: runtimev1alpha1.SnapshotArtifactPhaseReady}, nil
}
