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

// Package all imports all built-in snapshot drivers so their init() functions
// run and register themselves with the driver registry. Add new drivers here;
// cmd/agentd/main.go does not need to change.
package all

import (
	// Register the Kuasar SnapStart driver.
	_ "github.com/volcano-sh/agentcube/pkg/agentd/driver/kuasar"
)
