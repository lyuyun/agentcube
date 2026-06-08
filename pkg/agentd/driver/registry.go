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

import "fmt"

// DriverFactory is a constructor function for a Driver. Each driver package
// calls RegisterDriverFactory in its init() to enroll itself so that
// BuildDefaultRegistry can instantiate all drivers without main knowing their names.
type DriverFactory func() Driver

var defaultFactories []DriverFactory

// RegisterDriverFactory enrolls a factory in the package-level default list.
// Call this from driver init() functions; never call it after BuildDefaultRegistry.
func RegisterDriverFactory(f DriverFactory) {
	defaultFactories = append(defaultFactories, f)
}

// BuildDefaultRegistry instantiates every factory registered via RegisterDriverFactory
// and returns a ready-to-use Registry. Returns an error if two factories produce
// drivers with the same name.
func BuildDefaultRegistry() (*Registry, error) {
	r := NewRegistry()
	for _, f := range defaultFactories {
		if err := r.Register(f()); err != nil {
			return nil, err
		}
	}
	return r, nil
}

// Registry holds the set of Drivers available on this node, keyed by provider name.
// Use BuildDefaultRegistry to create one from self-registered drivers, or
// NewRegistry + Register for explicit construction (e.g. in tests).
// Capability-specific views are obtained via methods such as SnapshotDrivers.
type Registry struct {
	drivers map[string]Driver
}

// NewRegistry returns an empty registry.
func NewRegistry() *Registry {
	return &Registry{drivers: make(map[string]Driver)}
}

// Register adds a driver to the registry. Returns an error if a driver with
// the same name has already been registered.
func (r *Registry) Register(d Driver) error {
	name := d.Name()
	if _, exists := r.drivers[name]; exists {
		return fmt.Errorf("driver %q already registered", name)
	}
	r.drivers[name] = d
	return nil
}

// Get returns the driver registered under the given name, or false if absent.
func (r *Registry) Get(name string) (Driver, bool) {
	d, ok := r.drivers[name]
	return d, ok
}

// SnapshotDrivers returns a map of all registered drivers that implement
// SnapshotDriver, keyed by provider name. Callers may iterate or pass the
// result to snapshot-specific components without affecting the registry.
func (r *Registry) SnapshotDrivers() map[string]SnapshotDriver {
	out := make(map[string]SnapshotDriver)
	for k, v := range r.drivers {
		if sd, ok := v.(SnapshotDriver); ok {
			out[k] = sd
		}
	}
	return out
}
