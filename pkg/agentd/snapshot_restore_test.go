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
	"errors"
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	sandboxv1alpha1 "sigs.k8s.io/agent-sandbox/api/v1alpha1"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	runtimev1alpha1 "github.com/volcano-sh/agentcube/pkg/apis/runtime/v1alpha1"
)

const (
	testNamespace = "default"
	testSandbox   = "sb"
	testKey       = "snap-key-1"
	testProvider  = "test-provider"
)

type mockSnapshotDriver struct {
	providerName string
	restoreErr   error
	restoreCalls int
}

func (m *mockSnapshotDriver) Name() string { return m.providerName }
func (m *mockSnapshotDriver) Capabilities(_ context.Context) SnapshotDriverCapabilities {
	return SnapshotDriverCapabilities{}
}
func (m *mockSnapshotDriver) Create(_ context.Context, _ SnapshotDriverCreateRequest) (*SnapshotDriverArtifact, error) {
	return nil, nil
}
func (m *mockSnapshotDriver) Delete(_ context.Context, _ SnapshotDriverArtifact) error { return nil }
func (m *mockSnapshotDriver) List(_ context.Context) ([]SnapshotDriverArtifact, error) {
	return nil, nil
}
func (m *mockSnapshotDriver) Inspect(_ context.Context, _ SnapshotDriverArtifact) (*SnapshotDriverArtifactStatus, error) {
	return nil, nil
}
func (m *mockSnapshotDriver) Restore(_ context.Context, _ SnapshotDriverRestoreRequest) error {
	m.restoreCalls++
	return m.restoreErr
}

func setupTestScheme() *runtime.Scheme {
	s := runtime.NewScheme()
	utilruntime.Must(clientgoscheme.AddToScheme(s))
	utilruntime.Must(sandboxv1alpha1.AddToScheme(s))
	utilruntime.Must(runtimev1alpha1.AddToScheme(s))
	return s
}

func newRestoreReconciler(sc *runtime.Scheme, driver *mockSnapshotDriver, objs ...runtime.Object) *Reconciler {
	builder := fake.NewClientBuilder().WithScheme(sc)
	for _, o := range objs {
		builder = builder.WithRuntimeObjects(o)
	}
	return &Reconciler{
		Client:  builder.Build(),
		Scheme:  sc,
		Drivers: map[string]SnapshotDriver{driver.providerName: driver},
	}
}

func sandboxWithSnapshotKey(name, uid, snapshotKey string) *sandboxv1alpha1.Sandbox {
	return &sandboxv1alpha1.Sandbox{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: testNamespace, UID: types.UID(uid)},
		Spec: sandboxv1alpha1.SandboxSpec{
			PodTemplate: sandboxv1alpha1.PodTemplate{
				ObjectMeta: sandboxv1alpha1.PodMetadata{
					Annotations: map[string]string{
						runtimev1alpha1.SnapshotKeyAnnotation: snapshotKey,
					},
				},
			},
		},
	}
}

// -- Tests --

// No snapshot-key annotation → maybeRestore is a no-op.
func TestMaybeRestore_NoAnnotation(t *testing.T) {
	sc := setupTestScheme()
	driver := &mockSnapshotDriver{providerName: testProvider}
	sandbox := &sandboxv1alpha1.Sandbox{
		ObjectMeta: metav1.ObjectMeta{Name: testSandbox, Namespace: testNamespace, UID: "uid-1"},
	}
	r := newRestoreReconciler(sc, driver, sandbox)
	r.maybeRestore(context.Background(), sandbox)
	if driver.restoreCalls != 0 {
		t.Errorf("expected 0 restore calls, got %d", driver.restoreCalls)
	}
}

// No drivers registered → skip.
func TestMaybeRestore_NoDrivers(t *testing.T) {
	sc := setupTestScheme()
	sandbox := sandboxWithSnapshotKey(testSandbox, "uid-1", testKey)
	r := &Reconciler{
		Client:  fake.NewClientBuilder().WithScheme(sc).WithRuntimeObjects(sandbox).Build(),
		Scheme:  sc,
		Drivers: map[string]SnapshotDriver{},
	}
	r.maybeRestore(context.Background(), sandbox)
	if r.alreadyRestored(sandbox.UID) {
		t.Error("UID must not be marked when no drivers registered")
	}
}

// UID already in the in-memory set → skip without calling driver.
func TestMaybeRestore_AlreadyRestored_InMemory(t *testing.T) {
	sc := setupTestScheme()
	driver := &mockSnapshotDriver{providerName: testProvider}
	sandbox := sandboxWithSnapshotKey(testSandbox, "uid-1", testKey)
	r := newRestoreReconciler(sc, driver, sandbox)

	// First call: restore happens.
	r.maybeRestore(context.Background(), sandbox)
	if driver.restoreCalls != 1 {
		t.Fatalf("expected 1 restore call on first invocation, got %d", driver.restoreCalls)
	}

	// Second call: UID already tracked, skip.
	r.maybeRestore(context.Background(), sandbox)
	if driver.restoreCalls != 1 {
		t.Errorf("expected still 1 restore call after second invocation, got %d", driver.restoreCalls)
	}
}

// Driver returns error → cold-start fallback; UID still marked to prevent infinite retry.
func TestMaybeRestore_DriverError_ColdStartFallback(t *testing.T) {
	sc := setupTestScheme()
	driver := &mockSnapshotDriver{providerName: testProvider, restoreErr: errors.New("restore failed")}
	sandbox := sandboxWithSnapshotKey(testSandbox, "uid-1", testKey)
	r := newRestoreReconciler(sc, driver, sandbox)

	r.maybeRestore(context.Background(), sandbox)
	if driver.restoreCalls != 1 {
		t.Errorf("expected 1 restore call, got %d", driver.restoreCalls)
	}
	if !r.alreadyRestored(sandbox.UID) {
		t.Error("UID must be marked even after driver error to prevent infinite retry")
	}
}

// Happy path: driver succeeds, UID is tracked.
func TestMaybeRestore_Success(t *testing.T) {
	sc := setupTestScheme()
	driver := &mockSnapshotDriver{providerName: testProvider}
	sandbox := sandboxWithSnapshotKey(testSandbox, "uid-1", testKey)
	r := newRestoreReconciler(sc, driver, sandbox)

	r.maybeRestore(context.Background(), sandbox)
	if driver.restoreCalls != 1 {
		t.Errorf("expected 1 restore call, got %d", driver.restoreCalls)
	}
	if !r.alreadyRestored(sandbox.UID) {
		t.Error("UID must be marked after successful restore")
	}
}

// Multiple Reconcile calls for the same Sandbox UID must trigger Restore exactly once.
func TestMaybeRestore_Idempotent_ViaReconcile(t *testing.T) {
	sc := setupTestScheme()
	driver := &mockSnapshotDriver{providerName: testProvider}
	sandbox := sandboxWithSnapshotKey(testSandbox, "uid-1", testKey)
	r := newRestoreReconciler(sc, driver, sandbox)

	req := reconcile.Request{NamespacedName: types.NamespacedName{Name: testSandbox, Namespace: testNamespace}}
	for i := 0; i < 3; i++ {
		if _, err := r.Reconcile(context.Background(), req); err != nil {
			t.Fatalf("reconcile %d: %v", i, err)
		}
	}
	if driver.restoreCalls != 1 {
		t.Errorf("expected exactly 1 restore call across 3 reconciles, got %d", driver.restoreCalls)
	}
}

// Different Sandbox UIDs each get exactly one restore call.
func TestMaybeRestore_DifferentUIDs_EachRestored(t *testing.T) {
	sc := setupTestScheme()
	driver := &mockSnapshotDriver{providerName: testProvider}
	sb1 := sandboxWithSnapshotKey("sb1", "uid-1", testKey)
	sb2 := sandboxWithSnapshotKey("sb2", "uid-2", testKey)
	r := newRestoreReconciler(sc, driver, sb1, sb2)

	r.maybeRestore(context.Background(), sb1)
	r.maybeRestore(context.Background(), sb2)
	if driver.restoreCalls != 2 {
		t.Errorf("expected 2 restore calls for 2 different UIDs, got %d", driver.restoreCalls)
	}
}
