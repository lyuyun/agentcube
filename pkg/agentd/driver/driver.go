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

// Package driver defines the Driver and SnapshotDriver interfaces used by all
// node-agent driver implementations.
//
// The design follows the Container Storage Interface (CSI) pattern:
//   - Driver is the Identity Service: every driver implements GetPluginInfo,
//     GetPluginCapabilities, and Probe.
//   - SnapshotDriver extends Driver with snapshot lifecycle operations
//     (CreateSnapshot, DeleteSnapshot, ListSnapshots), analogous to the CSI
//     Controller Service snapshot RPCs.
//   - One agentd process corresponds to one driver (no registry).
package driver

import "context"

// Driver is the Identity Service interface implemented by every node-agent driver.
// It mirrors the CSI Identity Service: GetPluginInfo, GetPluginCapabilities, Probe.
type Driver interface {
	// GetPluginInfo returns the name and version of this driver.
	GetPluginInfo(ctx context.Context) (*PluginInfo, error)

	// GetPluginCapabilities returns the snapshot modes this driver supports.
	GetPluginCapabilities(ctx context.Context) ([]PluginCapability, error)

	// Probe verifies that the driver's backend is reachable and ready.
	Probe(ctx context.Context) error
}

// PluginInfo carries identity details for a driver implementation.
type PluginInfo struct {
	Name    string
	Version string
}

// PluginCapabilityType identifies a snapshot mode capability a driver may advertise.
type PluginCapabilityType int32

const (
	PluginCapabilityUnknown      PluginCapabilityType = 0
	PluginCapabilityWarmFork     PluginCapabilityType = 1 // Fork mode
	PluginCapabilityContinuation PluginCapabilityType = 2 // Resume mode
)

// PluginCapability wraps a PluginCapabilityType, matching the CSI PluginCapability
// message pattern so the type is extensible without breaking callers.
type PluginCapability struct {
	Type PluginCapabilityType
}
