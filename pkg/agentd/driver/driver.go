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

// Package driver defines the Driver interface and capability-specific interfaces
// (e.g. SnapshotDriver) used by all node-agent driver implementations.
// New capability types follow the same pattern: define an interface in a dedicated
// file (e.g. migration.go) that embeds Driver, then add a corresponding helper
// method on Registry (e.g. MigrationDrivers).
package driver

// Driver is the minimal interface implemented by every node-agent driver.
// It carries only the identity; capability-specific interfaces (e.g. SnapshotDriver)
// extend it with the actual operations. Use type assertions or Registry helper
// methods (e.g. Registry.SnapshotDrivers) to obtain a capability-specific view.
type Driver interface {
	// Name returns the stable provider name (e.g. "snapstart.kuasar.io").
	Name() string
}
