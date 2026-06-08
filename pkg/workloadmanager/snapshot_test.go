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
	"strings"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/utils/ptr"

	runtimev1alpha1 "github.com/volcano-sh/agentcube/pkg/apis/runtime/v1alpha1"
	"github.com/volcano-sh/agentcube/pkg/store"
)

// -- buildSnapshotKey --

func TestBuildSnapshotKey_NeverExceedsLabelLimit(t *testing.T) {
	cases := []struct {
		name       string
		ssName     string
		generation int64
		seq        int32
	}{
		{"short name", "mysnap", 1, 0},
		{"exactly 63 char name", strings.Repeat("a", 63), 1, 0},
		{"long name", strings.Repeat("x", 200), 1, 0},
		{"long name large counters", strings.Repeat("x", 200), 9999, 9999},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			ss := &runtimev1alpha1.SandboxSnapshot{
				ObjectMeta: metav1.ObjectMeta{Name: c.ssName, Generation: c.generation},
				Spec:       runtimev1alpha1.SandboxSnapshotSpec{SnapshotMode: runtimev1alpha1.SandboxSnapshotModeFork},
			}
			key := buildSnapshotKey(ss, c.seq)
			if len(key) > 63 {
				t.Errorf("key length %d > 63: %q", len(key), key)
			}
			if strings.HasSuffix(key, "-") {
				t.Errorf("key must not end with '-': %q", key)
			}
		})
	}
}

func TestBuildSnapshotKey_ContainsExpectedSuffix(t *testing.T) {
	ss := &runtimev1alpha1.SandboxSnapshot{
		ObjectMeta: metav1.ObjectMeta{Name: "mysnap", Generation: 3},
		Spec:       runtimev1alpha1.SandboxSnapshotSpec{SnapshotMode: runtimev1alpha1.SandboxSnapshotModeFork},
	}
	key := buildSnapshotKey(ss, 2)
	if !strings.HasSuffix(key, "-fork-g3-r2") {
		t.Errorf("expected suffix -fork-g3-r2, got %q", key)
	}
}

// -- normalizeLabel --

func TestNormalizeLabel(t *testing.T) {
	cases := []struct {
		input string
		want  string
	}{
		{"hello", "hello"},
		{"Hello-World", "hello-world"},
		{"foo_bar", "foo-bar"},
		{"--leading", "leading"},
		{"trailing--", "trailing"},
		{"a" + strings.Repeat("b", 100), "a" + strings.Repeat("b", 62)},
		{"", ""},
		{"---", ""},
	}
	for _, c := range cases {
		got := normalizeLabel(c.input)
		if got != c.want {
			t.Errorf("normalizeLabel(%q) = %q, want %q", c.input, got, c.want)
		}
		if len(got) > 63 {
			t.Errorf("normalizeLabel(%q) length %d > 63", c.input, len(got))
		}
	}
}

// -- computeSnapshotHash --

const testTemplateUID = types.UID("tmpl-uid-1")

func TestComputeSnapshotHash_Deterministic(t *testing.T) {
	ss := makeTestSandboxSnapshot("snap1", "default")
	sc := makeTestSnapshotClass("class1", "kuasar")
	podSpec := corev1.PodSpec{
		Containers: []corev1.Container{{Name: "main", Image: "python:3.11"}},
	}

	h1, err := computeSnapshotHash(ss, testTemplateUID, podSpec, sc)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	h2, err := computeSnapshotHash(ss, testTemplateUID, podSpec, sc)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if h1 != h2 {
		t.Errorf("hash not deterministic: %q != %q", h1, h2)
	}
}

func TestComputeSnapshotHash_UsesTemplateUID(t *testing.T) {
	ss := makeTestSandboxSnapshot("snap1", "default")
	sc := makeTestSnapshotClass("class1", "kuasar")
	podSpec := corev1.PodSpec{Containers: []corev1.Container{{Name: "c", Image: "img"}}}

	h1, _ := computeSnapshotHash(ss, types.UID("tmpl-uid-A"), podSpec, sc)
	h2, _ := computeSnapshotHash(ss, types.UID("tmpl-uid-B"), podSpec, sc)
	if h1 == h2 {
		t.Error("different template UIDs must produce different hashes")
	}
	// Same template UID but different snapshot UID must NOT change the hash.
	ss2 := makeTestSandboxSnapshot("snap1", "default")
	ss2.UID = "snapshot-uid-different"
	h3, _ := computeSnapshotHash(ss2, types.UID("tmpl-uid-A"), podSpec, sc)
	if h1 != h3 {
		t.Error("snapshot UID must not affect the hash; only template UID should")
	}
}

func TestComputeSnapshotHash_TolerationOrderIndependent(t *testing.T) {
	ss := makeTestSandboxSnapshot("snap1", "default")
	sc := makeTestSnapshotClass("class1", "kuasar")

	tolA := corev1.Toleration{Key: "aaa", Operator: corev1.TolerationOpEqual, Value: "v1", Effect: corev1.TaintEffectNoSchedule}
	tolB := corev1.Toleration{Key: "bbb", Operator: corev1.TolerationOpExists, Effect: corev1.TaintEffectNoExecute}

	h1, _ := computeSnapshotHash(ss, testTemplateUID, corev1.PodSpec{Tolerations: []corev1.Toleration{tolA, tolB}}, sc)
	h2, _ := computeSnapshotHash(ss, testTemplateUID, corev1.PodSpec{Tolerations: []corev1.Toleration{tolB, tolA}}, sc)
	if h1 != h2 {
		t.Errorf("hash differs with different toleration order: %q != %q", h1, h2)
	}
}

func TestComputeSnapshotHash_TolerationTieBreaker(t *testing.T) {
	ss := makeTestSandboxSnapshot("snap1", "default")
	sc := makeTestSnapshotClass("class1", "kuasar")

	tolA := corev1.Toleration{Key: "k", Operator: corev1.TolerationOpEqual, Value: "v1"}
	tolB := corev1.Toleration{Key: "k", Operator: corev1.TolerationOpEqual, Value: "v2"}

	h1, _ := computeSnapshotHash(ss, testTemplateUID, corev1.PodSpec{Tolerations: []corev1.Toleration{tolA, tolB}}, sc)
	h2, _ := computeSnapshotHash(ss, testTemplateUID, corev1.PodSpec{Tolerations: []corev1.Toleration{tolB, tolA}}, sc)
	if h1 != h2 {
		t.Errorf("tie-breaker not working: same-key tolerations produce different hash")
	}
}

func TestComputeSnapshotHash_TolerationSecondsOrdering(t *testing.T) {
	ss := makeTestSandboxSnapshot("snap1", "default")
	sc := makeTestSnapshotClass("class1", "kuasar")

	tolA := corev1.Toleration{Key: "k", TolerationSeconds: ptr.To[int64](10)}
	tolB := corev1.Toleration{Key: "k", TolerationSeconds: ptr.To[int64](20)}

	h1, _ := computeSnapshotHash(ss, testTemplateUID, corev1.PodSpec{Tolerations: []corev1.Toleration{tolA, tolB}}, sc)
	h2, _ := computeSnapshotHash(ss, testTemplateUID, corev1.PodSpec{Tolerations: []corev1.Toleration{tolB, tolA}}, sc)
	if h1 != h2 {
		t.Errorf("TolerationSeconds tie-breaker not working")
	}
}

func TestComputeSnapshotHash_TolerationNilVsPtrZeroSeconds(t *testing.T) {
	ss := makeTestSandboxSnapshot("snap1", "default")
	sc := makeTestSnapshotClass("class1", "kuasar")

	nilSec := corev1.Toleration{Key: "k", TolerationSeconds: nil}
	zeroSec := corev1.Toleration{Key: "k", TolerationSeconds: ptr.To[int64](0)}

	// Different input orders must produce the same hash (stable sort).
	h1, _ := computeSnapshotHash(ss, testTemplateUID, corev1.PodSpec{Tolerations: []corev1.Toleration{nilSec, zeroSec}}, sc)
	h2, _ := computeSnapshotHash(ss, testTemplateUID, corev1.PodSpec{Tolerations: []corev1.Toleration{zeroSec, nilSec}}, sc)
	if h1 != h2 {
		t.Errorf("nil vs ptr(0) TolerationSeconds should sort stably: %q != %q", h1, h2)
	}
}

func TestComputeSnapshotHash_NodeNameIgnored(t *testing.T) {
	ss := makeTestSandboxSnapshot("snap1", "default")
	sc := makeTestSnapshotClass("class1", "kuasar")

	base := corev1.PodSpec{Containers: []corev1.Container{{Name: "c", Image: "img"}}}
	withNode := base.DeepCopy()
	withNode.NodeName = "worker-7"

	h1, _ := computeSnapshotHash(ss, testTemplateUID, base, sc)
	h2, _ := computeSnapshotHash(ss, testTemplateUID, *withNode, sc)
	if h1 != h2 {
		t.Errorf("NodeName should be stripped before hashing but affects hash")
	}
}

// -- anyNodeArtifactReady --

func TestAnyNodeArtifactReady(t *testing.T) {
	ready := makeArtifact("node1", store.SnapshotArtifactPhaseReady)
	creating := makeArtifact("node2", store.SnapshotArtifactPhaseCreating)
	failed := makeArtifact("node3", store.SnapshotArtifactPhaseFailed)

	if anyNodeArtifactReady(nil) {
		t.Error("nil slice: want false")
	}
	if anyNodeArtifactReady([]store.SnapshotArtifact{creating, failed}) {
		t.Error("no ready artifact: want false")
	}
	if !anyNodeArtifactReady([]store.SnapshotArtifact{creating, ready}) {
		t.Error("one ready artifact: want true")
	}
	if !anyNodeArtifactReady([]store.SnapshotArtifact{ready}) {
		t.Error("all ready: want true")
	}
}

// -- helpers --

func makeTestSandboxSnapshot(name, ns string) *runtimev1alpha1.SandboxSnapshot {
	return &runtimev1alpha1.SandboxSnapshot{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: ns, UID: "uid-1"},
		Spec: runtimev1alpha1.SandboxSnapshotSpec{
			SnapshotMode: runtimev1alpha1.SandboxSnapshotModeFork,
			SourceRef:    corev1.TypedLocalObjectReference{Name: "tmpl1"},
		},
	}
}

func makeTestSnapshotClass(name, provider string) *runtimev1alpha1.SnapshotClass {
	return &runtimev1alpha1.SnapshotClass{
		ObjectMeta: metav1.ObjectMeta{Name: name},
		Spec:       runtimev1alpha1.SnapshotClassSpec{ProviderName: provider},
	}
}

func makeArtifact(nodeName string, phase store.SnapshotArtifactPhase) store.SnapshotArtifact {
	return store.SnapshotArtifact{NodeName: nodeName, Phase: phase}
}
