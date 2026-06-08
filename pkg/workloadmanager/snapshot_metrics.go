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

import (
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"sigs.k8s.io/controller-runtime/pkg/metrics"

	"github.com/volcano-sh/agentcube/pkg/store"
)

var (
	snapshotBuildDurationSeconds = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "agentcube_snapshot_build_duration_seconds",
			Help:    "Duration of snapshot artifact builds from task creation to Ready phase.",
			Buckets: []float64{5, 15, 30, 60, 120, 300, 600},
		},
		[]string{"provider", "mode"},
	)

	snapshotArtifactPhaseTransitionsTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "agentcube_snapshot_artifact_phase_transitions_total",
			Help: "Total snapshot artifact transitions to each phase.",
		},
		[]string{"provider", "phase"},
	)

	snapshotRebuildTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "agentcube_snapshot_rebuild_total",
			Help: "Total snapshot rebuild operations initiated, by reason (source_change, rebuild_after).",
		},
		[]string{"reason"},
	)

	snapshotArtifactRemovedTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "agentcube_snapshot_artifact_removed_total",
			Help: "Total snapshot artifacts removed when an active set is replaced or a snapshot is deleted.",
		},
		[]string{"provider"},
	)
)

func init() {
	metrics.Registry.MustRegister(
		snapshotBuildDurationSeconds,
		snapshotArtifactPhaseTransitionsTotal,
		snapshotRebuildTotal,
		snapshotArtifactRemovedTotal,
	)
}

func recordArtifactPhaseTransition(providerName string, phase store.SnapshotArtifactPhase) {
	snapshotArtifactPhaseTransitionsTotal.WithLabelValues(providerName, string(phase)).Inc()
}

func recordBuildDuration(providerName, mode string, buildStart time.Time) {
	snapshotBuildDurationSeconds.WithLabelValues(providerName, mode).Observe(time.Since(buildStart).Seconds())
}

func recordRebuild(reason string) {
	snapshotRebuildTotal.WithLabelValues(reason).Inc()
}

func recordArtifactRemoved(providerName string) {
	snapshotArtifactRemovedTotal.WithLabelValues(providerName).Inc()
}
