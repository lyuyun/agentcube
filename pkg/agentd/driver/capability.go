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

package driver

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes"
	"k8s.io/klog/v2"

	runtimev1alpha1 "github.com/volcano-sh/agentcube/pkg/apis/runtime/v1alpha1"
)

// AdvertiseDriverCapabilities patches the node with snapshot provider capability
// labels derived from the driver's GetPluginInfo and GetPluginCapabilities.
// The label format is:
//
//	agentcube.volcano.sh/snapshot-provider.<driver-name>=true
//
// Stale provider labels (from a previously installed driver) are removed so that
// the node is not selected for snapshot builds by an uninstalled provider.
func AdvertiseDriverCapabilities(ctx context.Context, cs kubernetes.Interface, nodeName string, driver SnapshotDriver) error {
	info, err := driver.GetPluginInfo(ctx)
	if err != nil {
		return fmt.Errorf("get plugin info: %w", err)
	}

	caps, err := driver.GetPluginCapabilities(ctx)
	if err != nil {
		return fmt.Errorf("get plugin capabilities: %w", err)
	}

	// Build the desired provider label: present only when the driver advertises
	// at least one capability.
	desired := make(map[string]string)
	if len(caps) > 0 {
		desired[runtimev1alpha1.SnapshotProviderLabelPrefix+info.Name] = "true"
	}

	// Read the current node to find stale provider labels.
	node, err := cs.CoreV1().Nodes().Get(ctx, nodeName, metav1.GetOptions{})
	if err != nil {
		return fmt.Errorf("get node %s: %w", nodeName, err)
	}

	// Compute the merge-patch: set desired labels, null-delete stale ones.
	patchLabels := make(map[string]interface{})
	for k, v := range desired {
		patchLabels[k] = v
	}
	for k := range node.Labels {
		if strings.HasPrefix(k, runtimev1alpha1.SnapshotProviderLabelPrefix) {
			if _, ok := desired[k]; !ok {
				patchLabels[k] = nil // null in merge-patch removes the key
			}
		}
	}
	if len(patchLabels) == 0 {
		return nil
	}

	data, err := json.Marshal(map[string]interface{}{
		"metadata": map[string]interface{}{"labels": patchLabels},
	})
	if err != nil {
		return fmt.Errorf("marshal label patch: %w", err)
	}
	if _, err := cs.CoreV1().Nodes().Patch(ctx, nodeName, types.MergePatchType, data, metav1.PatchOptions{}); err != nil {
		return fmt.Errorf("patch node %s labels: %w", nodeName, err)
	}

	klog.V(2).InfoS("advertised snapshot driver capabilities on node", "node", nodeName, "labels", patchLabels)
	return nil
}
