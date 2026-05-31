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

import "github.com/prometheus/client_golang/prometheus"

var (
	// snapshotBuildDuration measures how long each per-node snapshot build takes.
	// Labels: namespace, runtime (CodeInterpreter name), outcome (success|failure).
	snapshotBuildDuration = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "agentcube",
			Subsystem: "snapstart",
			Name:      "build_duration_seconds",
			Help:      "Duration of per-node WarmForkSnapshot build operations.",
			Buckets:   []float64{10, 30, 60, 120, 300, 600, 900},
		},
		[]string{"namespace", "runtime", "outcome"},
	)

	// snapshotStaleBuildResets counts Building entries reset by the stale-build GC sweep.
	// Labels: namespace, runtime (CodeInterpreter name).
	snapshotStaleBuildResets = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "agentcube",
			Subsystem: "snapstart",
			Name:      "stale_build_resets_total",
			Help:      "Total Building entries reset by the stale-build GC (exceeded buildingTimeout).",
		},
		[]string{"namespace", "runtime"},
	)
)

func init() {
	prometheus.MustRegister(
		snapshotBuildDuration,
		snapshotStaleBuildResets,
	)
}
