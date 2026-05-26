# AgentCube SnapStart Design

> Status: Draft
> Version: v0.1

---

## 1. Background and Motivation

AgentCube currently creates a sandbox from cold state for every session:
create -> wait for Ready -> handle requests -> destroy by TTL/idle GC.

| Runtime type | Cold-start bottleneck | Typical latency breakdown |
|---|---|---|
| Code Interpreter | VM boot + picod startup + package imports | VM: 0.3-0.5 s / picod kernel: 2-5 s / numpy+pandas: 1.5-5 s |
| Browser Agent | VM boot + Chromium startup + CDP readiness | VM: 0.3-0.5 s / Chromium: 3-8 s / CDP: 1-3 s |

> **Data sources** (baseline: cloud-hypervisor VMM, excluding Kubernetes scheduling latency)
>
> - **VM boot 0.3-0.5 s**: Kuasar's benchmark report for cloud-hypervisor v28.2 measured serial sandbox startup at 300-360 ms, roughly one third of Kata Containers v2.5.2 on the same hardware. Source: [Kuasar Benchmark Test Report](https://kuasar.io/blog/benchmark-test-report/).
> - **numpy + pandas 1.5-5 s**: Python community reference values put unconstrained cold imports around 100 ms for numpy and around 200 ms for pandas; the table estimates a 5-10x amplification under 100-300 mCPU microVM limits. This is an illustrative estimate, not a strict benchmark. Actual latency depends on Python version, numpy/pandas versions, and host I/O. Replace this with Phase 1 measurements after implementation. Source: [PEP 810 lazy-import analysis](https://byteiota.com/python-lazy-imports-speed-up-startup-with-pep-810/).
> - **Chromium headless 3-8 s**: Chrome documentation and community benchmarks put unconstrained headless startup around 800 ms-1.2 s; the table estimates a 4-6x amplification under 200-500 mCPU limits. Sources: [Chrome Headless documentation](https://developer.chrome.com/docs/chromium/headless), [WebScraping.AI headless performance comparison](https://webscraping.ai/faq/headless-chromium/what-are-the-performance-differences-between-headless-chromium-and-other-browsers).
> - **Snapshot restore reference**: Modal reports CRIU copy-mode memory snapshots with p50 1.05 s for an `import torch` case, compared with roughly 5 s cold start. AWS Lambda SnapStart documents sub-second startup qualitatively but does not publish millisecond-level p50/p99 numbers. For a 256 MB picod snapshot on local SSD with copy mode, this design estimates roughly 0.5-1.5 s. Sources: [Modal Memory Snapshots](https://modal.com/blog/mem-snapshots), [AWS Lambda SnapStart documentation](https://docs.aws.amazon.com/lambda/latest/dg/snapstart.html).

The key point is that VM boot is no longer the dominant bottleneck. With cloud-hypervisor, VM boot can be around hundreds of milliseconds; the user-visible delay mostly comes from runtime initialization: package imports, kernel startup, Chromium startup, CDP readiness, font/certificate initialization, and similar work.

This design uses Kuasar **WarmForkSnapshot**. A runtime is initialized once, reaches a ready-waiting checkpoint, and is snapshotted. Later sessions restore from that checkpoint. The target is to reduce session startup latency from **5-12 s to 0.5-2 s**.

**Support scope**: SnapStart only supports the Kuasar VMM path with the cloud-hypervisor backend. It does not support Kata Containers or other VMM implementations. WarmForkSnapshot is a Kuasar-specific process-level memory snapshot protocol based on an inject socket and copy-on-write clone. If a cluster mixes VMM implementations, SnapStart only applies to CodeInterpreter / AgentRuntime workloads using a Kuasar RuntimeClass; other workloads automatically fall back to cold start.

---

## 2. Kuasar Capability Fit

Kuasar exposes three snapshot types. Their value for AgentCube differs significantly.

### 2.1 EnvironmentSnapshot

EnvironmentSnapshot captures a booted VM and prepared environment, but no running runtime process. After restore, the container still starts from the image entrypoint.

| Aspect | Assessment |
|---|---|
| Saved latency | Only VM boot, around 0.3-0.5 s. Runtime initialization still runs again. |
| Suitable workload | Lightweight runtimes whose startup is already below 1 s and whose bottleneck is pure VM boot. |
| Code Interpreter | Not suitable. The bottleneck is package import and kernel startup. |
| Browser Agent | Not suitable. The bottleneck is Chromium startup and CDP readiness. |

Conclusion: EnvironmentSnapshot has limited user-visible value for AgentCube. It can be used in the first part of Phase 1 to validate the restore transaction model, network hot-plug, and Kuasar Admin API integration, but it is not the main acceleration feature.

### 2.2 WarmForkSnapshot

WarmForkSnapshot captures a process in a ready-waiting state. After restore, Kuasar injects instance-specific identity and task context. Each restored instance receives an independent copy-on-write memory view and an independent network namespace.

| Aspect | Assessment |
|---|---|
| Saved latency | VM boot plus all runtime initialization. |
| Code Interpreter | Core target. picod waits at `InterpreterReady`; restore injects `session_id` and workspace. |
| Browser Agent | Core target. Chromium and the Browser Agent wait at `BrowserReady`; restore injects start URL, proxy, identity, and workspace. |
| Sharing model | One template lease can serve many restores; each instance gets independent memory and runtime state. |

Conclusion: WarmForkSnapshot is the core mechanism and should be the first user-visible SnapStart capability.

### 2.3 ContinuationSnapshot

ContinuationSnapshot preserves a full process state and network identity. It is consumed one-to-one and is closer to pause/resume for an existing session.

| Aspect | Assessment |
|---|---|
| Suitable workload | Idle sleep/resume for the same user session, preserving variables and runtime state. |
| Cost | Requires CNI-level network identity control for cross-node restore; implementation complexity is high. |
| Demand | Most AgentCube use cases need fast new sessions rather than strict stateful resume. |

Conclusion: ContinuationSnapshot is out of scope for this design. A future session-resume feature can be evaluated separately.

### 2.4 Memory Restore Mode

Kuasar supports restore modes such as `copy`, `ondemand`, `filebackend`, and `externaluffd`. These are infrastructure-level tuning choices. AgentCube does not expose them in the SnapStart API.

---

## 3. Key Design Decisions

### 3.1 Decision Summary

| Decision point | Choice | Rationale |
|---|---|---|
| Primary snapshot type | WarmForkSnapshot | It removes runtime initialization latency, the real bottleneck. |
| Phase 1 bootstrap type | EnvironmentSnapshot first, then WarmForkSnapshot | Validates the restore path before picod changes are complete. |
| ContinuationSnapshot | Out of scope | High complexity and no immediate requirement. |
| Memory restore mode | Not exposed | Managed globally by Kuasar infrastructure. |
| API shape | Independent `SnapStart` CRD | Snapshot lifecycle, finalizers, per-node metadata, build jobs, and GC deserve a separate resource. |
| Snapshot construction | Job-like controller semantics | Snapshot build is asynchronous, retryable, observable, and stateful. |
| WarmPool interaction | Mutually exclusive in Phase 1 | Combining SandboxWarmPool and SnapStart requires separate operational semantics. |
| Snapshot storage | Artifact-aware model; node-local backend in Phase 1 | Phase 1 stores Kuasar templates on node-local disk, but API/status and metadata model the result as a snapshot artifact plus restore placement so future Kuasar cross-node distribution can fit without changing the user-facing SnapStart API. |
| Lifecycle cleanup | Finalizer + two-phase metadata + annotation-triggered rebuild | Covers deletion, orphan cleanup, and manual rebuild. |

### 3.2 API Shape: Embedded Field vs. Independent SnapStart CRD

Industry systems differ because their deployment boundaries differ:

| Product | Mechanism | Independent runtime definition | Snapshot optional | Independent snapshot resource |
|---|---|---|---|---|
| AWS Lambda SnapStart | VMM memory snapshot | No; function version is the deployment unit | Yes | No |
| E2B | VMM memory snapshot | No; template is the only sandbox entrypoint | No | No |
| Daytona | Snapshot | No independent cold runtime path | No | Yes, but it is also the runtime entrypoint |
| Modal | CRIU memory snapshot | No; function is the deployment unit | Yes | No |
| GKE AgentSandbox | Kubernetes Pod warm pool | Yes, via SandboxTemplate | N/A | N/A |
| AgentCube | Kuasar WarmForkSnapshot | Yes, CodeInterpreter / AgentRuntime can cold start | Yes | Yes |

AgentCube differs from Lambda, Modal, E2B, and Daytona because CodeInterpreter / AgentRuntime remain valid runtime definitions without SnapStart. SnapStart is an optional optimization layer on top of an existing runtime. This favors a separate Kubernetes resource.

Embedding snapshot fields into CodeInterpreter would mix workload definition with snapshot lifecycle. A snapshot has its own state machine, finalizer, build job, per-node artifacts, invalidation behavior, and orphan cleanup. A dedicated `SnapStart` CRD keeps responsibilities separate.

### 3.3 Candidate API Shapes

**Option A: Embedded field in CodeInterpreter / AgentRuntime**

```yaml
apiVersion: runtime.agentcube.volcano.sh/v1alpha1
kind: CodeInterpreter
metadata:
  name: python-interpreter
spec:
  sessionStartup:
    mode: Snapshot
    checkpoint: InterpreterReady
```

This is compact but makes the workload object responsible for snapshot lifecycle and finalizer cleanup.

**Option B: CodeInterpreter references SnapStart**

```yaml
apiVersion: runtime.agentcube.volcano.sh/v1alpha1
kind: CodeInterpreter
metadata:
  name: python-interpreter
spec:
  snapStartRef:
    name: python-snapstart
```

This introduces a separate object, but the runtime object still becomes snapshot-aware.

**Option C: SnapStart references runtimeRef**

```yaml
apiVersion: runtime.agentcube.volcano.sh/v1alpha1
kind: SnapStart
metadata:
  name: python-snapstart
spec:
  runtimeRef:
    kind: CodeInterpreter
    name: python-interpreter
  checkpoint: InterpreterReady
```

This is the selected option. CodeInterpreter stays a pure workload definition. SnapStart owns snapshot lifecycle and refers to the runtime.

### 3.4 Snapshot Build Job Semantics

Snapshot construction is treated like a job:

1. Read the referenced runtime.
2. Compute the normalized spec hash from runtime spec, checkpoint, and protocol version.
3. Create a build sandbox.
4. Wait for the build sandbox / Pod to become Running and read the resolved image digest.
5. Compute the final template key from image digest, checkpoint, and spec hash.
6. Wait until the runtime reaches the requested checkpoint.
7. Ask Kuasar to create the template.
8. Write metadata using a two-phase protocol.
9. Update `SnapStart.status`.

The controller must expose build phase, active mode, invalidation reason, per-node availability, and failure details.

### 3.5 SnapStart and WarmPool

Kubernetes WarmPool and SnapStart solve different problems:

| Mechanism | What is pre-created | Main cost | Main benefit |
|---|---|---|---|
| SandboxWarmPool | Scheduled Pod / sandbox slot | Full Pod resources per warm slot | Avoids scheduling/image/container startup delay |
| SnapStart | Runtime memory template | Snapshot storage and restore cost | Avoids runtime initialization delay |
| SnapStart WarmPool | Restored runtime instances | CoW memory and idle CPU | Avoids restore latency too |

In Phase 1, SnapStart and `spec.warmPoolSize` are mutually exclusive. A later phase can define a combined model with clear metrics and cost attribution.

### 3.6 High-Level Architecture

```mermaid
graph TB
    Admin(["Platform Administrator"])
    User(["End User"])

    subgraph AC["AgentCube Components"]
        CI["CodeInterpreter CR\n(pure workload definition, no snapshot fields)"]
        SS["SnapStart CR *\nruntimeRef -> CodeInterpreter\ncheckpoint / invalidation"]
        SC["SnapshotController *\nsnapshot lifecycle management"]
        WM["Workload Manager\ntryAnnotateWithSnapshot()"]
        SR["SandboxReconciler *\ndetects snapshot-template-id annotation\nbranches restore / cold-start path"]
        Router["AgentCube Router"]
    end

    subgraph Infra["Infrastructure"]
        Redis[("Redis / ValKey\nsnapshot metadata\nsession information")]
        Agentd["agentd\n+ Kuasar Admin Proxy *"]
        Kuasar["Kuasar VMM\ncloud-hypervisor"]
        SnapFile[("Snapshot files\nnode-local disk")]
    end

    subgraph Sandboxes["Sandboxes"]
        TmplSbx["Snapshot build sandbox\npicod @ InterpreterReady\n* Job semantics: exits after build\ncannot serve external traffic by protocol"]
        SessionSbx["Session Sandbox\npicod @ Running\nserves user traffic"]
    end

    %% Control plane: one-shot snapshot template build job
    Admin -->|"kubectl apply CodeInterpreter"| CI
    Admin -->|"kubectl apply SnapStart"| SS
    SS -->|"1. reconcile (watch SnapStart)"| SC
    SC -->|"2. read CodeInterpreter (via runtimeRef)"| CI
    SC -->|"3. create snapshot build sandbox"| TmplSbx
    TmplSbx -->|"4. /runtime/status\ncheckpoint=InterpreterReady\nsafeToSnapshot=true"| SC
    SC -->|"5. template-create\nsnapshot_type=warm_fork"| Agentd
    Agentd -->|"Kuasar Admin API"| Kuasar
    Kuasar --> SnapFile
    SC -->|"6. HSET snapshot metadata (per node)\nactiveMode=Snapshot"| Redis
    SC -->|"7. delete"| TmplSbx
    SC -->|"8. update SnapStart.status\nactiveMode=Snapshot"| SS

    %% Data plane: fast session creation path
    User -->|"HTTP request\nwithout x-agentcube-session-id"| Router
    Router -->|"9. CreateSandbox"| WM
    WM -->|"10. query snapshot metadata"| Redis
    WM -->|"11. create Sandbox CR\nsnapshot-template-id annotation\n+ node affinity"| SR
    SR -->|"12. detect annotation\nWarmForkSnapshot restore"| Kuasar
    Kuasar -->|"13. CoW restore + hot-plug network ns"| SessionSbx
    WM -->|"14. write session information"| Redis
    Router -->|"15. forward request"| SessionSbx

    %% Fallback path
    WM -.->|"snapshot unavailable\ncreate Sandbox CR without annotation"| SR
    SR -.->|"cold-start path (existing logic)"| Kuasar
```

Legend: `*` marks components or capabilities added or extended by this design, including SnapshotController, SandboxReconciler, and the agentd Kuasar Admin Proxy. Steps 1-8 are the control-plane snapshot build flow, which has one-shot job semantics. Steps 9-15 are the data-plane fast session creation path. Dashed edges are the cold-start fallback path. Redis writes use Hash format, such as `HSET snapshot:{ns}:{name} {node_name} {...}`, so per-node restore placements can coexist for one snapshot artifact.

Key design characteristics: CodeInterpreter remains a pure workload definition, while snapshot acceleration is layered through an independent SnapStart object. Both the restore path and the cold-start path go through Sandbox CR -> SandboxReconciler -> Kuasar. The only differences are whether the Sandbox CR carries the `snapshot-template-id` annotation and whether node affinity is constrained by the selected placement. The agentd Admin Proxy participates only in control-plane template operations such as `template-create` and `delete-template`; it is not on the session creation data path.

Main components:

| Component | Responsibility |
|---|---|
| `SnapStart` CRD | User-facing snapshot acceleration configuration and status. |
| SnapshotController | Builds templates, tracks status, handles invalidation and GC. |
| Workload Manager | Creates sessions and tries to select a snapshot template before cold start. |
| SandboxReconciler | Restores a sandbox when snapshot annotations are present. |
| agentd Kuasar proxy | Provides node-local access to Kuasar Admin API through a controlled HTTP proxy. |
| Store / Redis | Stores snapshot artifact metadata, local template IDs, and per-node restore availability. |
| Runtime process | Implements ready-waiting status and inject socket protocol. |

---

## 4. End-to-End User Flows

### 4.1 Flow A: Platform Administrator Enables SnapStart

The runtime remains a normal workload definition:

```yaml
apiVersion: runtime.agentcube.volcano.sh/v1alpha1
kind: CodeInterpreter
metadata:
  name: python-interpreter
spec:
  ports:
    - pathPrefix: "/"
      port: 8080
      protocol: HTTP
  template:
    image: ghcr.io/volcano-sh/picod:latest
    imagePullPolicy: IfNotPresent
    args:
      - --workspace=/workspace
      - --preload=numpy,pandas,matplotlib
    resources:
      requests:
        cpu: "100m"
        memory: "256Mi"
      limits:
        cpu: "1"
        memory: "1Gi"
  sessionTimeout: "15m"
  maxSessionDuration: "8h"
```

SnapStart is configured as a separate object:

```yaml
apiVersion: runtime.agentcube.volcano.sh/v1alpha1
kind: SnapStart
metadata:
  name: python-snapstart
spec:
  runtimeRef:
    kind: CodeInterpreter
    name: python-interpreter
  checkpoint: InterpreterReady
  artifact:
    distribution: NodeLocal
  placement:
    strategy: MinimumReady
    minReadyNodes: 1
  invalidation:
    onImageDigestChange: true
    onArgsChange: true
    maxAge: 24h
```

The controller builds templates according to the active placement policy and updates status. In Phase 1 the artifact is backed by node-local Kuasar templates, so per-node `localTemplateID` values stay in Redis/ValKey while the CRD status exposes the global artifact identity and placement summary. A typical status includes:

```yaml
status:
  activeMode: Snapshot
  message: "Snapshot ready. New sessions will use fast startup (~0.5-2s)."
  snapshot:
    phase: Ready
    templateKey: "fork:sha256-a1b2c3:InterpreterReady:args-7d9f2e"
    artifact:
      id: "snap-python-8f3a1b9c"
      storageClass: NodeLocal
      digest: "sha256:..."
    placement:
      readyNodes: 2
      eligibleNodes: 5
    readyAt: "2026-05-25T10:00:00Z"
```

### 4.2 Flow B: End User Creates a Session

User code does not change:

```python
from agentcube import CodeInterpreter

ci = CodeInterpreter(name="python-interpreter")
session = ci.create_session()
session.run("import numpy as np")
```

Internally, Workload Manager:

1. Authenticates and authorizes the request exactly as before.
2. Resolves the runtime.
3. Finds a ready SnapStart template for that runtime.
4. Creates a Sandbox with restore annotations and node affinity.
5. SandboxReconciler drives Kuasar restore.
6. SandboxReconciler creates the restore Pod with Kuasar protocol annotations before Pod creation.
7. Kuasar sandboxer sends PREPARE, the runtime replies READY, Kuasar sends COMMIT, and the runtime sends STARTED.
8. Session becomes ready.

If no valid template is available, the request falls back to cold start.

### 4.3 Flow C: Automatic Rebuild After Invalidation

Runtime image, command, arguments, environment, resource configuration, protocol version, or checkpoint changes can invalidate a template.

The controller computes a new template key and switches state:

```text
Ready(old key) -> Invalidated -> Creating(new key) -> Ready(new key)
```

During rebuild, existing sessions continue to run. New sessions do not reuse the old template; `activeMode=Cold`, so all new sessions fall back to cold start until the new snapshot is Ready.

### 4.4 Flow D: Complete Template Lifecycle

The lifecycle must cover:

| Event | Handling |
|---|---|
| SnapStart deletion | Finalizer cleans Kuasar template files and Redis metadata before object deletion. |
| Referenced runtime deletion | SnapshotController detects it, cleans templates, and marks SnapStart failed. |
| Controller crash during build | Two-phase metadata allows orphan detection and cleanup. |
| Manual rebuild | `agentcube.volcano.sh/force-rebuild: "true"` annotation triggers one rebuild; the annotation is removed only after rebuild succeeds, so failures keep retrying. |
| Node NotReady | Mark node template unavailable after grace period. |
| Node recovery | Verify template existence with `list-templates`; rebuild if missing. |
| Node deletion | Remove node metadata and rebuild on remaining eligible nodes. |

---

## 5. API Design

### 5.1 SnapStart Spec

```go
type SnapStartSpec struct {
    RuntimeRef RuntimeReference `json:"runtimeRef"`
    Checkpoint string `json:"checkpoint"`
    Invalidation *SnapStartInvalidation `json:"invalidation,omitempty"`
    Artifact *SnapStartArtifactSpec `json:"artifact,omitempty"`
    Placement *SnapStartPlacementSpec `json:"placement,omitempty"`
    SnapStartWarmPool *SnapStartWarmPoolSpec `json:"snapStartWarmPool,omitempty"`
}

type RuntimeReference struct {
    // Kind is CodeInterpreter or AgentRuntime.
    // +kubebuilder:validation:Enum=CodeInterpreter;AgentRuntime
    Kind string `json:"kind"`
    // Name is the referenced runtime object name in the same namespace.
    Name string `json:"name"`
}

type SnapStartInvalidation struct {
    // Defaults to true. Pointer bool is required so explicit false is preserved.
    OnImageDigestChange *bool `json:"onImageDigestChange,omitempty"`
    // Defaults to true. Command/args changes invalidate the snapshot.
    OnArgsChange *bool `json:"onArgsChange,omitempty"`
    // Defaults to 24h.
    MaxAge *metav1.Duration `json:"maxAge,omitempty"`
}

type SnapshotArtifactDistribution string

const (
    // NodeLocal is the Phase 1 implementation: Kuasar templates are materialized
    // on restore-capable nodes and are not exported as a global artifact.
    SnapshotArtifactDistributionNodeLocal SnapshotArtifactDistribution = "NodeLocal"
    // LazyRemote is a future mode: a global artifact exists, and a node can pull
    // or materialize it only when restore demand reaches that node.
    SnapshotArtifactDistributionLazyRemote SnapshotArtifactDistribution = "LazyRemote"
    // PreDistribute is a future mode: SnapshotController proactively distributes
    // or materializes the artifact to selected placements.
    SnapshotArtifactDistributionPreDistribute SnapshotArtifactDistribution = "PreDistribute"
)

type SnapStartArtifactSpec struct {
    // Distribution controls how the snapshot artifact is stored and distributed.
    // Defaults to NodeLocal in Phase 1.
    Distribution SnapshotArtifactDistribution `json:"distribution,omitempty"`
}

type SnapshotPlacementStrategy string

const (
    SnapshotPlacementStrategyMinimumReady SnapshotPlacementStrategy = "MinimumReady"
    SnapshotPlacementStrategyAllEligible  SnapshotPlacementStrategy = "AllEligible"
)

type SnapStartPlacementSpec struct {
    // Strategy controls how aggressively SnapshotController materializes restore placements.
    // Defaults to MinimumReady.
    Strategy SnapshotPlacementStrategy `json:"strategy,omitempty"`
    // MinReadyNodes is used by MinimumReady. Defaults to 1.
    MinReadyNodes int32 `json:"minReadyNodes,omitempty"`
    // MaxReadyNodes optionally caps eager materialization. Zero means no explicit cap.
    MaxReadyNodes int32 `json:"maxReadyNodes,omitempty"`
}

type SnapStartWarmPoolSpec struct {
    // Enabled activates the Kuasar VMM-layer pre-restored sandbox pool.
    Enabled bool `json:"enabled"`
    // Size is the target number of pre-restored ready sandboxes.
    // +kubebuilder:validation:Minimum=1
    // +kubebuilder:validation:Maximum=20
    Size int32 `json:"size,omitempty"`
}
```

Key fields:

| Field | Description |
|---|---|
| `runtimeRef` | References a CodeInterpreter or AgentRuntime. |
| `checkpoint` | Runtime checkpoint, such as `InterpreterReady` or `BrowserReady`. |
| `invalidation` | Controls rebuild behavior when runtime inputs change. |
| `artifact` | Controls artifact storage/distribution intent. Phase 1 supports only `NodeLocal`; `LazyRemote` and `PreDistribute` are future Kuasar distributed artifact modes. |
| `placement` | Controls how many restore placements are materialized eagerly. Phase 1 defaults to `MinimumReady` so a snapshot can become useful before every eligible node has a local template. |
| `snapStartWarmPool` | Optional Kuasar VMM-layer pre-restored sandbox pool. It is distinct from CodeInterpreter `spec.warmPoolSize`. |

`artifact` and `placement` intentionally describe different layers. `artifact.distribution` answers how the snapshot product is stored and distributed. `placement.strategy` answers which nodes should be made restorable and how aggressively. For example, future `artifact.distribution=LazyRemote` with `placement.strategy=MinimumReady` means the global artifact may exist remotely, but only a minimum number of nodes are eagerly materialized; other nodes can materialize lazily if selected.

### 5.2 SnapStart Status

```go
type SessionStartupMode string

const (
    SessionStartupModeCold     SessionStartupMode = "Cold"
    SessionStartupModeSnapshot SessionStartupMode = "Snapshot"
)

type SnapStartStatus struct {
    ActiveMode SessionStartupMode `json:"activeMode"`
    Snapshot *SnapshotStatus `json:"snapshot,omitempty"`
    Conditions []metav1.Condition `json:"conditions,omitempty"`
    Message string `json:"message,omitempty"`
}

type SnapshotPhase string

const (
    SnapshotPhasePending     SnapshotPhase = "Pending"
    SnapshotPhaseCreating    SnapshotPhase = "Creating"
    SnapshotPhaseReady       SnapshotPhase = "Ready"
    SnapshotPhaseFailed      SnapshotPhase = "Failed"
    SnapshotPhaseInvalidated SnapshotPhase = "Invalidated"
)

type SnapshotStatus struct {
    Phase SnapshotPhase `json:"phase"`
    TemplateKey string `json:"templateKey,omitempty"`
    Artifact *SnapshotArtifactStatus `json:"artifact,omitempty"`
    Placement *SnapshotPlacementStatus `json:"placement,omitempty"`
    ReadyAt *metav1.Time `json:"readyAt,omitempty"`
    FailedReason string `json:"failedReason,omitempty"`
    RestoreFailureCount int32 `json:"restoreFailureCount,omitempty"`
}

type SnapshotArtifactStorageClass string

const (
    SnapshotArtifactStorageClassNodeLocal   SnapshotArtifactStorageClass = "NodeLocal"
    SnapshotArtifactStorageClassDistributed SnapshotArtifactStorageClass = "Distributed"
)

type SnapshotArtifactStatus struct {
    ID string `json:"id,omitempty"`
    StorageClass SnapshotArtifactStorageClass `json:"storageClass,omitempty"`
    Digest string `json:"digest,omitempty"`
    URI string `json:"uri,omitempty"`
}

type SnapshotPlacementStatus struct {
    ReadyNodes int32 `json:"readyNodes,omitempty"`
    EligibleNodes int32 `json:"eligibleNodes,omitempty"`
}
```

Important status concepts:

| Field | Meaning |
|---|---|
| `activeMode` | `Cold` or `Snapshot`. |
| `snapshot.phase` | CRD-level aggregate phase: `Pending`, `Creating`, `Ready`, `Invalidated`, or `Failed`. Per-node Redis phases include `Building`, `Ready`, `Failed`, `Unavailable`, and `Invalidated`. |
| `snapshot.templateKey` | Deterministic key for the build inputs. |
| `snapshot.artifact` | Global artifact identity and storage class. Phase 1 uses `storageClass=NodeLocal`; future Kuasar versions can use `Distributed` with a URI/digest. |
| `snapshot.placement` | User-visible placement summary, such as ready nodes versus eligible nodes. Per-node template IDs remain in Redis/ValKey. |
| `snapshot.restoreFailureCount` | Consecutive restore failures. SnapshotController marks the snapshot Invalidated when this reaches 3, and resets it after a successful rebuild. |
| `conditions` | User-facing readiness and degraded reasons. |
| `message` | Human-readable current state and next steps. |

### 5.3 Template Key

The template key must change whenever restored memory would no longer match runtime semantics.

Format:

```text
fork:{image_digest_short}:{checkpoint}:{spec_hash}
```

Example:

```text
fork:sha256-a1b2c3d4e5f6:InterpreterReady:spec-8f3a1b9c
```

Inputs include:

| Input | Reason |
|---|---|
| Runtime kind/name/namespace | Identifies the source runtime. |
| Image digest | Tags are mutable; digest captures actual image content. |
| Command, args, env | Runtime initialization inputs. |
| Resources | Can affect process and cgroup behavior. |
| Checkpoint | Different checkpoints are different memory states. |
| Protocol version | Inject protocol changes can break restore compatibility. |
| SnapStart build version | Controller/runtime behavior changes can require rebuild. |

`spec_hash` is computed over the normalized startup spec that affects process memory:

```json
{
  "image": "<full_digest>",
  "command": ["<cmd>", "..."],
  "args": ["<arg>", "..."],
  "env": [{"name": "K", "value": "V"}],
  "authMode": "<picod|none>",
  "resources": {
    "requests": {"cpu": "...", "memory": "..."},
    "limits": {"cpu": "...", "memory": "..."}
  },
  "runtimeClass": "<name>"
}
```

Normalization rules:

| Field | Rule |
|---|---|
| `image` | Use the resolved full digest from the running build Pod, not the tag from spec. |
| `command` | Preserve order. |
| `args` | Preserve order; do not sort. |
| `env` | Sort by env name because environment variable order is normally not semantically meaningful. |
| `resources` | Include requests and limits. |
| `runtimeClass` | Include because VMM/runtime selection affects restore compatibility. |

Fields intentionally excluded from `spec_hash`: labels, annotations, `imagePullSecrets`, K8s Pod-layer `warmPoolSize`, `sessionTimeout`, and `maxSessionDuration`. They affect scheduling, pull behavior, or session policy, but not the initialized process memory captured in the snapshot.

`args` must preserve order and must not be sorted. CLI arguments have ordering semantics; for example, `--preload=numpy,pandas` and `--preload=pandas,numpy` can produce different import order and different process memory. Sorting args during hash computation could map different runtime semantics to the same key and reuse a polluted snapshot.

For tagged images, the build sandbox should use `imagePullPolicy: Always` so the controller can observe the resolved image digest. For digest-pinned images, `IfNotPresent` is allowed.

---

## 6. Internal Implementation

### 6.1 New Controller

Add SnapshotController with informers for:

| Resource | Purpose |
|---|---|
| SnapStart | Primary reconcile target. |
| CodeInterpreter / AgentRuntime | Detect spec changes and deletion through reverse index. |
| Sandbox / Pod | Track build sandbox and resolved image digest. |
| Node | Maintain per-node availability and rebuild after node changes. |

### 6.2 Template Build Flow

Build flow:

```text
Reconcile SnapStart
  -> validate runtimeRef
  -> compute normalized spec hash
  -> create/update Building metadata placeholder
  -> create build sandbox
  -> wait for build sandbox / Pod Running
  -> read resolved image digest from Pod status
  -> compute final template key
  -> wait for runtime checkpoint
  -> call Kuasar template-create
  -> write Ready metadata
  -> update SnapStart status
```

The final template key is computed only after the build sandbox is running, because tag images must be resolved from the actual Pod status (`containerStatuses[].imageID`). The controller can compute the normalized spec hash before creating the build sandbox, but it cannot compute the final `fork:{image_digest_short}:{checkpoint}:{spec_hash}` key until the resolved digest is known.

The build sandbox must include:

| Item | Purpose |
|---|---|
| `AGENTCUBE_SNAPSTART_BUILD=true` | Tells runtime to enter build/checkpoint mode. |
| `kuasar.io/warm-fork-ready-protocol-version: "1"` | Enables Kuasar WarmFork readiness protocol. |
| Runtime-specific checkpoint config | Selects `InterpreterReady` or `BrowserReady`. |

Snapshot sources are restricted to SnapshotController-managed build sandboxes. A normal cold-start session sandbox must never be promoted into a snapshot source, even when it was created as fallback after a restore miss or restore failure. User sessions can contain caller identity, task context, workspace writes, executed code, open network state, or other contamination. Reusing them as snapshot sources would violate the checkpoint cleanliness contract. When placements need to be replenished, SnapshotController must create a fresh build sandbox with build-mode environment and Job semantics, wait for a clean checkpoint, create the template/artifact, and delete that build sandbox.

Build and restore placement semantics:

| Rule | Meaning |
|---|---|
| Eligible nodes | Nodes must be Ready, advertise Kuasar SnapStart capability, match the runtime class, and satisfy scheduling constraints. |
| Artifact distribution | `spec.artifact.distribution` declares the intended storage/distribution mode. Phase 1 only supports `NodeLocal`; future modes include `LazyRemote` and `PreDistribute`. |
| Snapshot artifact | The logical result is a snapshot artifact identified in status by `artifact.id`, `templateKey`, and optional digest/URI. |
| Phase 1 backend | `artifact.storageClass=NodeLocal`; each selected node owns a Kuasar local template for the same logical artifact. |
| NodeLocal build node rule | In `NodeLocal`, the build node is also the restore node, so it must be in the runtime eligible node set. SnapshotController must reject or invalidate any local placement whose node no longer satisfies runtime scheduling constraints. |
| Distributed build node rule | In future `Distributed` modes, the artifact build node may come from a separate build pool and may be outside the runtime eligible node set. It must not be published as a restore placement unless it also satisfies runtime scheduling constraints. |
| Build placement | SnapshotController chooses where to materialize local templates. Phase 1 may use all eligible nodes or a smaller policy such as minimum ready nodes; the API/status must not assume all nodes are always built eagerly. |
| Restore placement | Workload Manager selects a Ready placement entry and binds the Sandbox to the node that can restore it. Phase 1 selects only entries with `cacheState=LocalReady`; future distributed artifacts may allow lazy pull before restore. |
| Concurrency | SnapshotController may build or materialize placements in parallel. |
| Partial failure | If at least one node becomes Ready, the aggregate `SnapStart.status.snapshot.phase` can be `Ready` and `activeMode=Snapshot`; failed nodes are reported through `Degraded` condition and per-node Redis state. |
| Total failure | If no selected placement can become restorable, aggregate phase becomes `Failed` and `activeMode=Cold`. |
| Ready node shortage | If Ready template node count is lower than eligible node count, set `Degraded=True` with a reason such as `Ready nodes: N/M`. |

This separation is intentional. Phase 1 implements the placement entries with node-local Kuasar template files. Future Kuasar support for cross-node artifact distribution should replace only the placement materialization backend, not the SnapStart CRD contract or the session creation path.

### 6.3 Session Create Path

`handleSandboxCreate()` remains scoped, but the RBAC ordering is important. It must first resolve the runtime and query SnapStart availability so it can determine the actual Kubernetes resource type to create: `sandboxes` when a ready snapshot forces the direct restore path, or `sandboxclaims` when the existing SandboxWarmPool path is used. Only after this decision should it perform SAR. This keeps the SAR resource type aligned with the resource that `buildSandboxByCodeInterpreter()` actually creates.

```go
ci := getCIFromInformer(...)
readySnap := selectReadySnapshot(...)
forceDirectSandbox := readySnap != nil

sarResource := "sandboxclaims"
if forceDirectSandbox || ci.Spec.WarmPoolSize == 0 {
    sarResource = "sandboxes"
}
checkResourceCreatePermission(ctx, userDynamicClient, namespace, sarResource)

sandbox := buildSandboxByCodeInterpreter(ci, req, forceDirectSandbox)
if readySnap != nil {
    annotateWithSnapshot(sandbox, readySnap)
}
createSandbox(ctx, dynamicClient, sandbox, req)
```

The restore path must not bypass RBAC. The key constraint is that snapshot availability is checked before SAR, because SAR must validate the same resource type that the request will actually create.

The restore path and cold-start path both reuse the existing `createSandbox()` transaction:

1. Write a Redis SETNX placeholder for the session.
2. Create the Sandbox CR.
3. Watch until the Sandbox reaches Running.
4. Write final session information to Redis.
5. On any failure, delete the Sandbox CR and remove the Redis placeholder.

If a snapshot restore attempt fails, `createSandbox()` completes rollback for the annotated Sandbox first. Workload Manager then creates a new, unannotated cold-start Sandbox through an independent `createSandbox()` transaction. This prevents orphan Sandbox CRs and avoids reusing a partially written Redis session record.

The fallback cold-start Sandbox remains a business session only. Workload Manager must not mark it as a build sandbox, must not call Kuasar `template-create` from it, and must not hand it back to SnapshotController as a snapshot source. Any replacement placement build triggered by the failure is a separate SnapshotController reconcile using a new clean build sandbox.

When a ready snapshot placement is selected, `forceDirectSandbox=true` bypasses the `warmPoolSize > 0 -> SandboxClaim` branch. Snapshot restore requires a direct Sandbox CR because the controller must attach the `snapshot-template-id` annotation and constrain `spec.nodeName` to a node whose placement is currently restorable. In Phase 1 this means the node owns a local Kuasar template. With future distributed artifacts, it can also mean the node has already materialized or can synchronously materialize the artifact before restore.

### 6.4 Restore Sandbox Lifecycle

SandboxReconciler detects restore annotations and follows a restore-specific state machine:

```text
Sandbox created with snapshot annotations
  -> bind to template node
  -> SandboxReconciler translates AgentCube annotations into Kuasar annotations before Pod creation
  -> Kuasar restores from WarmForkSnapshot
  -> sandboxer sends PREPARE to the runtime
  -> runtime replies READY
  -> sandboxer sends COMMIT
  -> runtime sends STARTED
  -> Sandbox Ready
```

If restore fails, the instance is destroyed. Depending on failure reason, the request either retries cold start or reports an error.

### 6.5 SandboxInfo Extension

Only minimal fields should be added:

| Field | Purpose |
|---|---|
| `RestoredFromSnapshot` | Records the Kuasar template key if this sandbox was restored from a snapshot. It is empty on the cold-start path and is used only for observability. |

### 6.6 Store Interface

Snapshot placement metadata is stored as a Redis / ValKey Hash:

```text
key:   snapshot:{namespace}:{runtime_name}
field: {node_name}
value: SnapshotPlacementInfo JSON
```

Each field represents one restore placement for the logical snapshot artifact. Phase 1 uses one field per node because Kuasar templates are node-local. Future distributed artifacts can keep the same key/field shape and change only `storageClass` / `cacheState`.

The store must expose concrete snapshot placement operations:

```go
type SnapshotStore interface {
    StoreSnapshot(ctx context.Context, info *SnapshotPlacementInfo) error
    GetSnapshotNodes(ctx context.Context, namespace, runtimeName string) ([]*SnapshotPlacementInfo, error)
    DeleteSnapshot(ctx context.Context, namespace, runtimeName, nodeName string) error
    DeleteAllSnapshots(ctx context.Context, namespace, runtimeName string) error
    ListSnapshotTemplateIDs(ctx context.Context) ([]string, error)
}

type SnapshotArtifactStorageClass string

const (
    SnapshotArtifactStorageClassNodeLocal   SnapshotArtifactStorageClass = "NodeLocal"
    SnapshotArtifactStorageClassDistributed SnapshotArtifactStorageClass = "Distributed"
)

type SnapshotCacheState string

const (
    SnapshotCacheStateBuilding        SnapshotCacheState = "Building"
    SnapshotCacheStateRemoteAvailable SnapshotCacheState = "RemoteAvailable"
    SnapshotCacheStatePulling         SnapshotCacheState = "Pulling"
    SnapshotCacheStateLocalReady      SnapshotCacheState = "LocalReady"
    SnapshotCacheStateFailed          SnapshotCacheState = "Failed"
    SnapshotCacheStateUnavailable     SnapshotCacheState = "Unavailable"
    SnapshotCacheStateInvalidated     SnapshotCacheState = "Invalidated"
)

type SnapshotPlacementInfo struct {
    Namespace   string               `json:"namespace"`
    RuntimeName string               `json:"runtimeName"`
    NodeName    string               `json:"nodeName"`
    NodeIP      string               `json:"nodeIP"`
    ArtifactID  string               `json:"artifactId,omitempty"`
    StorageClass SnapshotArtifactStorageClass `json:"storageClass,omitempty"`
    LocalTemplateID string             `json:"localTemplateId,omitempty"`
    TemplateKey string               `json:"templateKey,omitempty"`
    ArtifactURI string                `json:"artifactUri,omitempty"`
    ArtifactDigest string             `json:"artifactDigest,omitempty"`
    Phase       PerNodeSnapshotPhase `json:"phase"`
    CacheState  SnapshotCacheState   `json:"cacheState,omitempty"`
    StartedAt   time.Time            `json:"startedAt,omitempty"`
    ReadyAt     time.Time            `json:"readyAt,omitempty"`
    Reason      string               `json:"reason,omitempty"`
}
```

The operations map to the following behavior:

| Operation | Purpose |
|---|---|
| `StoreSnapshot` with `phase=Building` / `cacheState=Building` | Two-phase placeholder before Kuasar `template-create` or future artifact materialization; prevents orphan ambiguity after controller crash. |
| `StoreSnapshot` with `phase=Ready` / `cacheState=LocalReady` | Publish usable placement metadata for restore selection. |
| `GetSnapshotNodes` | Session creation lookup and cleanup enumeration. |
| `DeleteSnapshot` | Remove one node field, such as after node deletion or per-node invalidation. |
| `DeleteAllSnapshots` | Remove Redis metadata first during cleanup to stop new restores. |
| `ListSnapshotTemplateIDs` | Support Phase 1 orphan GC by diffing `localTemplateId` values against Kuasar `list-templates`. |

### 6.7 Lifecycle Management

SnapStart deletion uses a finalizer:

```text
delete SnapStart
  -> finalizer runs
  -> list metadata
  -> delete Redis metadata first to stop Workload Manager from starting new restores
  -> retry Kuasar delete-template on relevant nodes until lease_count reaches zero
  -> remove finalizer
```

Cleanup failure modes:

| Scenario | Handling |
|---|---|
| Active sessions still hold template leases | `delete-template` returns `template_in_use`; SnapshotController retries with backoff until `lease_count=0`. |
| Lease never releases, such as an abnormal hanging session | Wait until the configured max session TTL, then stop blocking finalizer; orphan GC deletes the template later once `lease_count=0`. |
| SnapshotController crashes during cleanup | Finalizer replay is idempotent; Redis `DeleteAllSnapshots` and Kuasar `delete-template` tolerate repeated attempts and not-found results. |
| Node is offline during cleanup | Log and skip that node for now; orphan GC scans again when the node returns. |

This tradeoff avoids keeping a SnapStart object blocked forever. File deletion is precise when leases release normally, while orphan GC handles long-tail leftovers.

When `runtimeRef` is deleted, the controller cleans templates and marks the SnapStart failed. This avoids keeping snapshots for a runtime definition that no longer exists.

Orphan cleanup uses:

1. Two-phase metadata: `Building` before Kuasar create, `Ready` after success.
2. Periodic `list-templates` from nodes.
3. Reconciliation between Kuasar files and Redis metadata.
4. Conservative deletion only when no valid owner exists.

Node lifecycle rules:

| Node event | Handling |
|---|---|
| Node NotReady beyond grace period | Mark the per-node snapshot phase `Unavailable`; `tryAnnotateWithSnapshot()` excludes that node. |
| Node recovers | Call Kuasar `list-templates` to verify the template still exists; mark Ready if present, otherwise rebuild on that node. |
| Node deleted | Delete that node's Redis Hash field and build replacement templates on remaining eligible nodes if needed. |
| Ready node count below eligible count | Keep serving from Ready nodes, set `Degraded=True`, and emit/record the Ready node ratio. |

Manual rebuild uses:

```yaml
metadata:
  annotations:
    agentcube.volcano.sh/force-rebuild: "true"
```

The controller removes this annotation only after the rebuild succeeds. If cleanup or rebuild fails, the annotation remains so the next reconcile retries automatically.

### 6.8 One Runtime, One SnapStart

Phase 1 allows only one SnapStart per runtime. This prevents ambiguous template selection and invalidation behavior.

The controller enforces uniqueness by indexing SnapStart objects by `(runtimeRef.namespace, runtimeRef.kind, runtimeRef.name)`.

---

## 7. Ready-Waiting Protocol

The runtime must expose a deterministic checkpoint where it is initialized but has no session identity or task state.

Standard checkpoint values:

| Runtime | Checkpoint |
|---|---|
| picod / Code Interpreter | `InterpreterReady` |
| Browser Agent | `BrowserReady` |

### 7.0 picod Prerequisite

Current picod behavior may not preload a Jupyter kernel in a way that fully eliminates import latency. The Jupyter kernel model is a separate work item. SnapStart protocol can still be implemented first, but the full latency target for Code Interpreter depends on the picod kernel preload work.

### 7.1 Checkpoint Semantics

`InterpreterReady` means:

| Requirement | Meaning |
|---|---|
| Runtime process started | picod is running. |
| Kernel/runtime initialized | Required preload work is complete. |
| No task identity | No session ID, prompt, user code, or credential is bound. |
| Workspace not bound | Workspace path is injected after restore. |
| Inject socket waiting | Runtime blocks on the WarmFork readiness socket. |

`BrowserReady` means:

| Requirement | Meaning |
|---|---|
| Browser Agent started | Control process is ready. |
| Browser runtime prewarmed | Chromium and CDP are ready for the selected BrowserWarmFork design. |
| No task identity | No task ID, URL, credential, or session state is bound. |
| No external connection | No business network state is carried into the snapshot. |
| Inject socket waiting | Runtime blocks on the WarmFork readiness socket. |

### 7.2 `GET /runtime/status`

Add a runtime status endpoint:

```json
{
  "checkpoint": "InterpreterReady",
  "safeToSnapshot": true,
  "userStateLoaded": false,
  "activeTasks": 0,
  "details": {
    "interpreterState": "idle",
    "workspaceEmpty": true,
    "preloadedPackages": ["numpy", "pandas", "matplotlib"]
  }
}
```

SnapshotController polls this endpoint during build. `safeToSnapshot=true` requires `userStateLoaded=false`, `activeTasks=0`, and an empty workspace. If polling sees `safeToSnapshot=false`, `userStateLoaded=true`, or `activeTasks>0`, the controller rejects snapshot creation and emits `SnapshotContaminationDetected`.

### 7.3 picod Quiescent-State Steps

picod reaches `InterpreterReady` through an explicit ready-waiting sequence:

1. Start picod and complete runtime preload work.
2. Bind and listen on the inject socket, defaulting to `/run/warmfork-readiness.sock`.
3. Enter Waiting state before accepting any user task.
4. Set internal waiting flags with atomic visibility so the HTTP status goroutine observes the same state as the main runtime goroutine.
5. Return 503 or 425 for business APIs such as code execution and file mutation while Waiting.
6. Report `safeToSnapshot=true` from `GET /runtime/status` only after the inject socket is listening, no user state is loaded, `activeTasks=0`, and workspace state is clean.
7. Block on `accept()` until Kuasar connects for probe or restore injection.

This ordering is part of the protocol contract. A runtime must not report `safeToSnapshot=true` before the inject socket is ready and the Waiting state is visible to all request-handling goroutines.

### 7.4 Restore Injection

Kuasar injects task identity through the WarmFork protocol:

Wire format: every message is encoded as a 4-byte big-endian uint32 length prefix followed by a JSON body. The maximum JSON body size is 4 MiB.

Inject socket: the default path is `/run/warmfork-readiness.sock`. It can be overridden by env `WARMFORK_READINESS_SOCKET` or annotation `kuasar.io/warm-fork-readiness-socket`. The workload binds/listens on this socket, and the Kuasar sandboxer connects to it.

The workload first sends capabilities:

```json
{
  "type": "CAPABILITIES",
  "protocol_version": "1",
  "supported_features": ["prepare", "commit", "cancel"]
}
```

The sandboxer may open a probe connection before snapshot creation, read `CAPABILITIES`, and close the connection without sending PREPARE. The workload must treat EOF after CAPABILITIES as a probe, return to `accept()`, and leave internal state unchanged.

During restore, the sandboxer sends PREPARE:

```json
{
  "type": "PREPARE",
  "task_id": "session-abc",
  "context": "{\"workspace\":\"/workspace/session-abc\"}",
  "env_overrides": {
    "AGENTCUBE_SESSION_ID": "session-abc"
  }
}
```

Runtime behavior:

1. Send CAPABILITIES after the sandboxer connects.
2. Receive PREPARE.
3. Validate payload.
4. Bind workspace, environment overrides, and instance identity.
5. Reply READY.
6. Receive COMMIT.
7. Reseed process-local PRNG state.
8. Send STARTED.
9. Start serving the task.

Security-critical PRNG reseed requirement: because CoW clone can duplicate process-local PRNG state across all restored instances, the COMMIT handler must reseed all non-kernel PRNGs before STARTED. This includes sources such as Go `math/rand` global state and Python's `random` module. The seed must come from kernel-backed entropy, for example `crypto/rand.Read`. This is a safety requirement, not a performance optimization.

CANCEL handling: the sandboxer may send `{"type":"CANCEL","reason":"..."}` before PREPARE or after READY, for example on PREPARE timeout or multi-process barrier failure. The runtime exits cleanly without replying, and Kuasar discards the restored instance.

### 7.5 Pollution Detection

Before snapshot, the runtime must reject polluted state:

| Pollution | Example |
|---|---|
| Task identity | Existing session ID, prompt, URL, or credential. |
| Workspace binding | Current working directory or open file under a task workspace. |
| External network | Open TCP/WebSocket/DNS state. |
| In-flight request | Running user code or CDP command. |
| Shared writable state | Profile/cache/tmp files shared across instances. |

---

## 8. Kuasar Interface Layer

### 8.1 AgentCube Access to Kuasar Admin API

Workload Manager should not talk to Kuasar node sockets directly. Add an HTTP proxy in `agentd`:

```text
Workload Manager -> agentd on target node -> Kuasar Admin socket
```

The proxy provides:

| Capability | Purpose |
|---|---|
| Authentication and authorization | Prevent arbitrary access to node-local Kuasar API. |
| Operation whitelist | Only expose required snapshot operations. |
| Request validation | Restrict paths, template IDs, and payload size. |
| Audit logs | Record template create/delete/list and restore operations. |

Phase 1 minimum security requirements:

| Requirement | Implementation |
|---|---|
| Listen address | Listen only on the agentd Pod IP, not `hostNetwork` and not `0.0.0.0`. |
| Network isolation | NetworkPolicy allows only Workload Manager Pods to access agentd `:9090`. |
| Operation whitelist | Forward only `template-create`, `delete-template`, and `list-templates`; reject all other actions. |
| Caller authentication | Bearer token stored in a Kubernetes Secret and mounted into Workload Manager. |

Recommended production hardening includes mTLS between Workload Manager and agentd, structured audit logs for every Kuasar Admin call, and rate limiting to prevent accidental request storms.

### 8.2 Minimal Kuasar Admin Contract

`template-create` request fields:

| Field | Meaning |
|---|---|
| `sandbox_id` | Build sandbox to snapshot. |
| `snapshot_type` | `warm_fork` for WarmForkSnapshot, or `environment` for EnvironmentSnapshot. |
| `key` | AgentCube-computed template key, such as `fork:sha256-a1b2c3d4e5f6:InterpreterReady:spec-8f3a1b9c`. |
| `owner` | Metadata used for orphan GC attribution: namespace, runtime name, SnapStart UID, and node name. |

Example:

```json
{
  "action": "template-create",
  "sandbox_id": "<sandbox_id>",
  "snapshot_type": "warm_fork",
  "key": "<computed_template_key>",
  "owner": {
    "namespace": "<namespace>",
    "runtime_name": "<codeinterpreter_name>",
    "snap_start_uid": "<snapstart_uid>",
    "node_name": "<node_name>"
  }
}
```

`list-templates` response fields:

| Field | Meaning |
|---|---|
| `template_id` | Kuasar-assigned template identifier. |
| `key` | AgentCube-computed template key supplied at `template-create` time. |
| `snapshot_type` | Snapshot type. |
| `created_at` | Creation timestamp. |
| `lease_count` | Number of active restores using this template. |
| `owner` | Owner metadata supplied at `template-create` time. |
| `labels` | Optional labels returned by Kuasar. |

AgentCube orphan GC must first filter templates whose `owner["snap_start_uid"]` belongs to this AgentCube installation, then diff them against Redis metadata, then apply a grace-period check before deletion. Templates without owner metadata are outside AgentCube GC scope.

`delete-template` must reject deletion when `lease_count > 0`, unless an explicit force path is added later.

### 8.3 Pod Annotations

Build sandbox annotations:

| Annotation | Purpose |
|---|---|
| `kuasar.io/snapshot-type: "warm-fork"` | Tells Kuasar this is a WarmFork snapshot build sandbox. |
| `kuasar.io/warm-fork-ready-protocol-version: "1"` | Enables WarmFork readiness protocol. |
| `agentcube.volcano.sh/template-sandbox: "true"` | Marks the sandbox as build-only; it is deleted after snapshot creation. |

Restore Sandbox CR annotations added by Workload Manager:

| Annotation | Purpose |
|---|---|
| `agentcube.volcano.sh/snapshot-template-id` | Selects the restore path in SandboxReconciler. |
| `agentcube.volcano.sh/restored-from-snapshot` | Stores the AgentCube template key for observability. |

Kuasar protocol annotations written by SandboxReconciler before Pod creation:

| Annotation | Purpose |
|---|---|
| `kuasar.io/snapshot-type: "warm-fork"` | Tells Kuasar to restore from a WarmFork template. |
| `kuasar.io/template-key` | Template key converted from `agentcube.volcano.sh/restored-from-snapshot`. |
| `kuasar.io/task-id` | Injected session/task ID. |
| `kuasar.io/task-context` | Opaque JSON context, including the workspace path. |
| `kuasar.io/task-env/<NAME>` | Environment override passed to runtime. |
| `kuasar.io/warm-fork-readiness-socket` | Optional socket path override. |

---

## 9. State Machines

### 9.1 Snapshot State Machine

```text
Pending
  -> Creating
  -> Ready
  -> Invalidated
  -> Pending
  -> Creating
  -> Ready

Creating -> Failed
Ready -> Invalidated
```

`Pending`, `Creating`, `Ready`, `Failed`, and `Invalidated` are CRD-level aggregate phases. Per-node template state is stored in Redis and can use phases such as `Building`, `Ready`, `Failed`, `Unavailable`, and `Invalidated`. The two layers must not be mixed in `SnapStart.status.snapshot.phase`.

### 9.2 Sandbox Lifecycle

Cold-start path:

```text
Create Sandbox
  -> schedule Pod
  -> VM boot
  -> container start
  -> runtime init
  -> Ready
```

Restore path:

```text
Create Sandbox with snapshot annotations
  -> select node with ready template
  -> Kuasar restore
  -> sandboxer sends PREPARE
  -> runtime replies READY
  -> sandboxer sends COMMIT
  -> runtime sends STARTED
  -> Ready
```

---

## 10. SnapStart WarmPool

SnapStart WarmPool is optional and not required for Phase 1.

It pre-restores a number of instances from a snapshot template so session creation can avoid both cold start and restore time. It costs memory and idle CPU, so it must be explicitly configured and observable.

Use it only when:

| Condition | Reason |
|---|---|
| Restore latency is still user-visible | WarmPool removes restore from the critical path. |
| Traffic is bursty and predictable | Pre-restored slots can absorb bursts. |
| Resource budget is acceptable | Warm slots consume memory and CPU. |

---

## 11. State Isolation

Every restored session must have isolated mutable state:

| State | Isolation rule |
|---|---|
| Workspace | Per session. |
| Environment overrides | Per restore. |
| Browser profile/cache/tmp/download | Per instance. |
| Logs | Per instance or tagged by restore ID. |
| Network namespace | Per sandbox. |
| Credentials | Injected only after restore. |

The snapshot must not include user credentials, task prompts, URLs, cookies, tokens, or tenant-specific files.

---

## 12. Failure Handling and Fallback

All fallback is transparent to users: the worst user-visible behavior is slower session creation through cold start, not a failed request.

**Failure type 1: retryable build failure, up to three attempts with exponential backoff**

| Failure | Handling |
|---|---|
| Snapshot creation fails, such as Kuasar error or template-create timeout | `snapshot.phase=Failed`, `activeMode=Cold`; all sessions fall back to cold start; retry up to 3 times with exponential backoff, then stop until an administrator triggers force rebuild. |
| Snapshot build sandbox startup timeout | Same 3-retry policy. |
| Kuasar Admin Proxy unreachable | Same 3-retry policy; emit `SnapshotFailed` and operational alert. |
| Image or args invalidates the snapshot | Rebuild automatically; retry count is reset per rebuild; transition period uses `activeMode=Cold`. |

**Failure type 2: ProtocolNotSupported, no retry**

| Failure | Handling |
|---|---|
| Runtime does not implement `/runtime/status`, such as persistent 404 | Stop snapshot build; set `snapshot.phase=Failed`, condition `ProtocolNotSupported=True`, and `activeMode=Cold`; do not retry automatically; emit `SnapshotFailed` with reason `ProtocolNotSupported`. |
| `/runtime/status` keeps returning non-JSON or 500 | Treat as `ProtocolNotSupported`. |

`ProtocolNotSupported` means the runtime image lacks the required protocol implementation, so retrying will not change the outcome. Kuasar errors and network failures are transient; those use bounded retry.

**Failure type 3: restore path failure**

| Failure | Handling |
|---|---|
| SandboxReconciler fails to restore through Kuasar or the Sandbox CR does not reach Running | The current request falls back to cold start with a normal Sandbox CR. Consecutive restore failures increment `SnapStart.status.snapshot.restoreFailureCount`; after 3 failures, SnapshotController marks the snapshot `Invalidated` and rebuilds it. |

`RestoreFailureCount` is persisted in `SnapStart.status.snapshot`, not memory or Redis, so controller restarts do not lose it. Workload Manager PATCHes this count during fallback; SnapshotController watches it and triggers rebuild when the count reaches 3. A successful rebuild resets it to 0.

Fallback cold-start sessions are never snapshot candidates. They may trigger counters, events, or a later SnapshotController reconcile, but the replacement snapshot must always come from a new build sandbox that has not served user traffic.

**Failure type 4: infrastructure failure**

| Failure | Handling |
|---|---|
| Redis unavailable | Snapshot metadata cannot be read; fall back to cold start through existing store health handling. |

Fallback must be observable. Silent fallback hides performance regressions and should emit events and metrics.

---

## 13. Observability

### 13.1 Kubernetes Events

Emit events such as:

| Event | Meaning |
|---|---|
| `SnapshotBuildStarted` | Build sandbox created. |
| `SnapshotReady` | Template is ready on one or more nodes. |
| `SnapshotFailed` | Snapshot creation failed, including failedReason. |
| `SnapshotInvalidated` | Key changed or maxAge expired; `activeMode` switches back to `Cold`. |
| `SandboxRestoredFromSnapshot` | A Sandbox CR with `snapshot-template-id` reaches Running and SandboxReconciler confirms restore completion. |
| `SandboxRestoreFallback` | Restore-path Sandbox CR fails to reach Running and Workload Manager creates a normal cold-start Sandbox CR. |
| `SnapshotContaminationDetected` | Runtime did not reach a safe snapshot state, such as `safeToSnapshot=false`. |

### 13.2 Metrics

Recommended metrics:

| Metric | Labels |
|---|---|
| `agentcube_snapstart_build_total` | `result`, `runtime_kind`, `checkpoint` |
| `agentcube_snapstart_build_duration_seconds` | `runtime_kind`, `checkpoint` |
| `agentcube_snapstart_restore_total` | `result`, `node`, `runtime_kind` |
| `agentcube_snapstart_restore_duration_seconds` | `node`, `runtime_kind` |
| `agentcube_session_startup_duration_seconds` | `path=cold/snapshot/snapstart_warmpool/sandbox_warmpool` |
| `agentcube_snapstart_template_ready` | `snapstart`, `node` |
| `agentcube_snapstart_artifact_ready` | `snapstart`, `storage_class` |
| `agentcube_snapstart_placement_cache_state` | `snapstart`, `node`, `cache_state` |
| `agentcube_snapstart_fallback_total` | `reason`, `runtime_kind` |

Operational metrics should preserve latency attribution:

| Dimension | Requirement |
|---|---|
| Startup path | Always label session startup with `cold`, `snapshot`, `snapstart_warmpool`, or `sandbox_warmpool`. |
| Restore failure reason | Count fallback by reason so restore failures, missing metadata, unavailable nodes, and protocol failures are distinguishable. |
| Node readiness | Expose ready template count versus eligible node count for `Degraded` diagnosis. |
| Latency breakdown | Track scheduling, restore, and runtime-ready portions separately where implementation can observe them. |

---

## 14. Code Change Areas

### 14.1 File-Level Change List

Expected changes:

| File | Change |
|---|---|
| `pkg/apis/runtime/v1alpha1/snapstart_types.go` | Add `SnapStart` CRD types: `SnapStartSpec`, `SnapStartStatus`, `SnapshotStatus`, `SnapshotArtifactStatus`, `SnapshotPlacementStatus`, `SnapStartArtifactSpec`, `SnapStartPlacementSpec`, `PerNodeSnapshotPhase`, `SnapStartWarmPoolSpec`, `RuntimeReference`; include `Conditions []metav1.Condition` and `RestoreFailureCount`. |
| `pkg/common/types/sandbox.go` | Add `SandboxInfo.RestoredFromSnapshot` for observability; cold-start path leaves it empty. |
| `pkg/store/interface.go` | Add artifact-aware snapshot placement operations: `StoreSnapshot`, `GetSnapshotNodes`, `DeleteSnapshot`, `DeleteAllSnapshots`, and `ListSnapshotTemplateIDs`. |
| `pkg/store/store_redis.go` / `store_valkey.go` | Implement snapshot placement Hash storage: `snapshot:{ns}:{runtime_name}` with field `{node_name}` and value `SnapshotPlacementInfo`. |
| `pkg/workloadmanager/workload_builder.go` | Add `forceDirectSandbox bool` to `buildSandboxByCodeInterpreter()`; when true, bypass `warmPoolSize > 0 -> SandboxClaim` and build a direct Sandbox CR. |
| `pkg/workloadmanager/server.go` | Start SnapshotController goroutine from `Start()`. |
| `pkg/workloadmanager/handlers.go` | In `handleSandboxCreate()`: query snapshot availability first, derive `forceDirectSandbox`, run SAR against the actual resource type, build Sandbox with `forceDirectSandbox`, inject snapshot annotations, and reuse existing `createSandbox()` transaction. |
| `pkg/workloadmanager/sandbox_reconciler.go` | Add restore branch for `snapshot-template-id`; drive Kuasar WarmForkSnapshot restore and convert AgentCube annotations to Kuasar annotations before Pod creation. |
| `pkg/workloadmanager/snapshot_controller.go` | Add SnapshotController: watch SnapStart, maintain reverse runtimeRef index, detect conflicts, run per-node build jobs, manage conditions/status, and handle node changes. |
| `pkg/workloadmanager/kuasar_client.go` | Add Kuasar Admin API client for `template-create`, `delete-template`, and `list-templates`. |
| `pkg/workloadmanager/admission_webhook.go` | Add validating webhook for runtimeRef uniqueness and Phase 1 `runtimeRef.kind=CodeInterpreter` enforcement. |
| `pkg/agentd/kuasar_proxy.go` | Add Kuasar Admin HTTP Proxy: Pod IP listener, action whitelist, Bearer token authentication. |
| `pkg/agentd/agentd.go` | No lifecycle change required; restore and cold-start sandboxes share deletion/TTL handling. |
| `pkg/picod/server.go` | Add `GET /runtime/status`; business APIs return 503/425 while Waiting. |
| `pkg/picod/execute.go` | Implement SnapStart protocol: bind inject socket, `accept()`, signal Waiting, set `safeToSnapshot=true`; persistent kernel work remains separate. |

### 14.2 Phase 1 runtimeRef Scope

Phase 1 supports:

| Runtime kind | Scope |
|---|---|
| CodeInterpreter | Full Phase 1 target. |
| AgentRuntime | Rejected by the validating webhook in Phase 1 with HTTP 400 and message `AgentRuntime SnapStart is Phase 2 only`. Cold-start AgentRuntime behavior is unaffected. |

### 14.3 RBAC

Required permissions:

| Subject | Resource | Verbs | Purpose |
|---|---|---|---|
| SnapshotController / Workload Manager ServiceAccount | `snapstarts` | get, list, watch, update, patch | Watch SnapStart CRD and update annotations/finalizers. |
| SnapshotController / Workload Manager ServiceAccount | `snapstarts/status` | update, patch | Update `SnapStart.status`. |
| SnapshotController / Workload Manager ServiceAccount | `codeinterpreters`, `agentruntimes` | get, list, watch | Resolve `runtimeRef` and watch referenced runtime changes. |
| SnapshotController / Workload Manager ServiceAccount | `sandboxes` | create, delete, get, list, watch | Create/delete snapshot build sandboxes. |
| SnapshotController / Workload Manager ServiceAccount | `nodes` | list, watch | Enumerate eligible nodes and handle node lifecycle. |
| SnapshotController / Workload Manager ServiceAccount | `pods` | get, list | Read `containerStatuses[].imageID` if agent-sandbox status does not expose it. |
| SnapshotController / Workload Manager ServiceAccount | `events` | create | Emit SnapshotBuildStarted / SnapshotReady / SnapshotFailed events. |
| SnapshotController / Workload Manager ServiceAccount | `secrets` | get | Read agentd proxy Bearer token. |
| Workload Manager request path | `selfsubjectaccessreviews` | create | Check caller permission for `sandboxes` or `sandboxclaims` after snapshot availability determines the actual resource type. |
| Workload Manager request path | `networkpolicies` | get | Optional audit that agentd proxy NetworkPolicy is configured. |
| agentd DaemonSet | hostPath `/run/vmm-sandboxer-admin.sock` | mount in Pod spec | Access Kuasar Admin Unix socket; this is not an RBAC resource. |

---

## 15. Evolution Plan

### 15.1 Phase 1: EnvironmentSnapshot Validation + Code Interpreter WarmForkSnapshot

Goals:

- Validate restore transaction model, Kuasar Admin API calls, and network hot-plug through EnvironmentSnapshot.
- Implement ready-waiting protocol in picod.
- Implement full SnapshotController build flow.
- Create sessions through the restore path.
- Deliver the first user-visible acceleration path for Code Interpreter.

Deliverables:

- `SnapStart` CRD and generated clients.
- SnapshotController skeleton and then full build implementation.
- Artifact-aware `SnapStartArtifactSpec`, `SnapshotStatus`, and `SnapshotPlacementInfo` metadata, with `artifact.distribution=NodeLocal` / `storageClass=NodeLocal` as the Phase 1 backend.
- Placement policy support with `MinimumReady` as the default and `AllEligible` available for small clusters or strict locality requirements.
- `agentd` Kuasar Admin HTTP proxy.
- `tryAnnotateWithSnapshot()` in session creation.
- SandboxReconciler restore path.
- picod `GET /runtime/status` and inject socket protocol.
- E2E coverage for restore path and cold-start fallback.

Acceptance criteria:

- `make gen-check` passes.
- SnapStart status transitions correctly.
- `status.snapshot.artifact` and `status.snapshot.placement` summarize the logical artifact and ready/eligible node counts without exposing per-node template IDs in CRD status.
- Existing CodeInterpreter behavior is unchanged when SnapStart is absent.
- Two sessions have fully isolated workspaces.
- Image changes invalidate and rebuild snapshots.
- Code Interpreter WarmForkSnapshot runs end to end.
- Browser Agent protocol validation reaches `BrowserReady` as a Phase 2 entry condition.

Performance note: Phase 1 must prove the restore path end to end and provide measured session startup latency. The full target of removing Python import latency depends on the separate picod Jupyter kernel/preload work; before that work lands, protocol correctness is a stronger Phase 1 acceptance requirement than the final 0.5-2 s latency target.

### 15.2 Phase 2: Browser Agent WarmForkSnapshot

**Design Definition**

BrowserWarmForkSnapshot is not "pause an already busy Chrome and resume it later". It is a provably clean prewarmed browser snapshot:

> Chromium is prewarmed and the Browser Agent is prewarmed, but there is no task identity, no business page, no external connection, no in-flight CDP command, no shared writable profile/cache, and no unsafe reuse of random or identity state.

**Five-Layer Architecture**

```text
Control Plane        AgentCube Workload Manager / SnapshotController
Runtime Plane        Kuasar Sandboxer / VMM / Snapshot Manager
Guest Control Plane  Guest Agent / Restore Agent
Browser Control      Browser Agent / Browser Supervisor
Browser Runtime      Chromium process tree
```

Restore order:

```text
restore VM
  -> guest agent first-resume
  -> RestoreInjection: entropy + instance identity + writable delta
  -> Browser Agent releases restore gate
  -> BrowserThaw: rebuild CDP session + create new context/page
  -> TaskInjection through Kuasar PREPARE/COMMIT
  -> start request
```

The system must not allow Chromium to immediately continue task execution after VM restore without injection.

**Strict Quiescence Constraints**

| # | Constraint | Meaning |
|---|---|---|
| 1 | task-free | No task ID, prompt, URL, credential, or session binding. |
| 2 | page-free | No business page; at most neutral pages such as `about:blank`. |
| 3 | network-free | No external TCP/UDP/WebSocket connection or DNS intermediate state. |
| 4 | cdp-drained | No in-flight CDP command; no active screencast, tracing, download, or network interception. |
| 5 | profile-clean | No task profile write in progress and no cross-instance profile lock. |
| 6 | runtime-write-isolated | Profile/cache/tmp/download/log are redirected to per-instance writable delta after restore. |
| 7 | entropy-safe | Per-instance entropy reseed completes before thaw. |
| 8 | identity-safe | Instance ID, restore ID, hostname, and workspace are injected before thaw. |
| 9 | thaw-controlled | Chromium cannot access external network or create business contexts before RestoreInjection completes. |

`Page.setWebLifecycleState(frozen)` is only a page-level hint. It does not replace process-tree quiescence.

**Browser Agent Final Implementation**

**Full BrowserWarmFork (Phase 2 target)**

```text
Snapshot content:
  Chromium running + all 9 constraints satisfied
  no business page, no external connection, CDP drained, neutral page frozen
  Browser Agent blocked at WarmForkReady gate

After restore:
  reseed -> identity inject -> BrowserThaw
  -> rebuild CDP session + create new context/page
  -> TaskInjection
```

Kuasar does not block this design. Kuasar only checks the readiness protocol, where the container main process is blocked on inject socket `accept()`. Chromium subprocesses being physically running does not affect Kuasar's snapshot decision. The 9 constraints are AgentCube Browser Agent safety requirements, not Kuasar protocol requirements.

**Browser Agent State Machine**

```text
STARTING -> BROWSER_PREWARMING -> FREEZE_PREPARING -> QUIESCENT_CHECKING
-> WARMFORK_READY_WAITING  <- snapshot point
-> RESTORED -> WAIT_RESTORE_INJECTION -> RESTORE_READY
-> WAIT_TASK_INJECTION -> TASK_RUNNING
```

**Required Local RPCs / Commands**

```protobuf
PrepareForSnapshot {
  close_all_pages, freeze_neutral_pages, drain_cdp, flush_profile, verify_no_network
}

AssertQuiescent -> QuiescentReport {
  task_free, no_business_pages, no_external_network, cdp_drained,
  profile_clean, no_downloads, no_tracing, no_screencast, warnings[]
}

RestoreInjectAck {
  snapshot_id, restore_id, instance_id, entropy[],
  profile_dir, cache_dir, tmp_dir, download_dir, workspace_dir
}

ThawForTask
```

**BrowserFreezePrepare Actions**

1. Stop task intake: set `accepting_tasks = false` and wait until `current_task == nil`.
2. Close business pages: enumerate targets and close all non-neutral targets; keep only `about:blank`.
3. Drain CDP: wait until `cdp_inflight == 0`; stop tracing, screencast, fetch, and network interception.
4. Freeze neutral page for full BrowserWarmFork: call `Page.setWebLifecycleState { state: "frozen" }` as a page-level hint.
5. Block external network during build: only loopback CDP and vsock control are allowed.

**RestoreInjection**

Before BrowserThaw, the guest agent writes:

```json
{
  "snapshot_id": "browser-strict-v1",
  "restore_id": "r-20260527-001",
  "instance_id": "browser-r-20260527-001",
  "entropy_reseeded": true,
  "identity_applied": true,
  "profile_dir": "/run/browser-profile/browser-r-20260527-001",
  "cache_dir": "/run/browser-cache/browser-r-20260527-001",
  "tmp_dir": "/tmp/browser-agent/browser-r-20260527-001",
  "download_dir": "/workspace/browser-r-20260527-001/downloads"
}
```

The Browser Agent reads `/run/agentcube/restore.json`, releases the restore gate, and executes BrowserThaw.

**Quiescent Validator**

| Check type | Tool | Requirement |
|---|---|---|
| Process | `ps -ef` | Browser Agent and Chromium process tree exist; no previous task process remains. |
| Network | `ss -tunap` | Only loopback CDP, vsock, and guest-agent socket are allowed. |
| Files | `lsof -p <chromium-pids>` | No task workspace file, download temp file, or shared writable template file is held. |
| CDP | Browser Agent internal state | `cdp_inflight=0`, neutral target only, and no download/tracing/screencast/fetch interception. |

All validator checks must pass before `WarmForkReady = true`.

**Storage Isolation**

```text
Read-only in snapshot:
  /opt/chromium
  /opt/ms-playwright
  /usr/share/fonts
  /etc/ssl/certs
  /var/cache/fontconfig
  /opt/browser-profile-template

Per-instance writable:
  /run/browser-profile/${INSTANCE_ID}
  /run/browser-cache/${INSTANCE_ID}
  /tmp/browser-agent/${INSTANCE_ID}
  /workspace/${TASK_ID}
  /var/log/browser-agent/${INSTANCE_ID}

Forbidden:
  sharing one user-data-dir, disk-cache-dir, or download dir across instances
```

Recommended Chromium build-stage flags:

```text
--headless=new --remote-debugging-address=127.0.0.1 --remote-debugging-port=0
--no-first-run --disable-background-networking --disable-component-update
--disable-sync --disable-default-apps --disable-extensions
--metrics-recording-only --disable-crash-reporter
--user-data-dir=/opt/browser-neutral-profile
```

**Failure Policy**

Browser strict-mode validation failures are hard failures for the BrowserWarmFork template, not silent downgrade. SnapshotController may explicitly select EnvironmentSnapshot as a separate fallback mode only when it records an event explaining why BrowserWarmFork was rejected.

| Failure point | Handling |
|---|---|
| External connection during build | Fail snapshot build. |
| CDP inflight is not zero | Fail snapshot build. |
| Business page remains | Close and retry once; fail if still present. |
| Entropy injection fails after restore | Destroy instance; do not accept task. |
| Identity injection fails after restore | Destroy instance; do not accept task. |
| Post-restore validator fails | Destroy instance; do not accept task. |
| BrowserWarmFork rejected and EnvironmentSnapshot selected as separate fallback mode | Allowed only with an explicit SnapshotController event such as `BrowserWarmFork rejected: reason=<cause>`. |

**Security Boundary**

BrowserWarmForkSnapshot does not save user session state. It only saves a task-free, identity-free, externally disconnected browser runtime skeleton. Tenant, session, and task identity are injected only after restore. All mutable runtime state must be instance-exclusive.

Prerequisites:

- Phase 1 Code Interpreter WarmForkSnapshot is complete and the protocol interface is stable.
- Kuasar readiness protocol is confirmed to support full BrowserWarmFork.
- AgentCube Browser Agent reference image is implemented.

### 15.3 Future: Distributed Kuasar Snapshot Artifacts

Phase 1 treats Kuasar templates as node-local materializations of one logical snapshot artifact. Future Kuasar support for cross-node artifact distribution and restore should use the same SnapStart API shape and replace only the placement materialization backend.

The target model is:

```text
SnapStart
  -> snapshot.artifact: global identity, digest, storage class, optional URI
  -> snapshot.placement: where the artifact is currently restorable
  -> Redis/ValKey placement records: per-node cache/materialization state
```

Storage classes:

| Storage class | Meaning |
|---|---|
| `NodeLocal` | Phase 1 backend. Each restorable node has a local Kuasar template ID. |
| `Distributed` | Future backend. A global artifact exists in object storage, registry storage, or another Kuasar-managed artifact store and can be pulled/materialized on target nodes. |

Spec-level distribution modes:

| Distribution | Implementation phase | Meaning |
|---|---|---|
| `NodeLocal` | Phase 1 | No global artifact is required. SnapshotController materializes Kuasar templates directly on selected nodes. |
| `LazyRemote` | Future | SnapshotController creates or records a global artifact. Nodes may materialize it only when selected for restore or when background policy decides it is hot. |
| `PreDistribute` | Future | SnapshotController proactively materializes the artifact to the selected placement set, bounded by placement strategy and capacity. |

The mapping is:

| Spec intent | Status storage class | Typical placement state |
|---|---|---|
| `artifact.distribution=NodeLocal` | `NodeLocal` | `LocalReady` after local Kuasar `template-create`. |
| `artifact.distribution=LazyRemote` | `Distributed` | `RemoteAvailable` globally, then `Pulling` -> `LocalReady` on selected nodes. |
| `artifact.distribution=PreDistribute` | `Distributed` | Background materialization drives selected nodes to `LocalReady`. |

Build-node and restore-node roles:

| Mode | Artifact build node | Restore placement node |
|---|---|---|
| `NodeLocal` | Must be a runtime eligible node. The local Kuasar template cannot be restored elsewhere. | Same node as the build node; must still satisfy runtime scheduling constraints at restore time. |
| `LazyRemote` | May be a dedicated artifact build node outside the runtime eligible node set, if Kuasar supports exportable artifacts. | Must be in the runtime eligible node set and must pass artifact compatibility checks before `LocalReady`. |
| `PreDistribute` | May be a dedicated artifact build node outside the runtime eligible node set. | Must be selected from runtime eligible nodes by placement strategy and materialized before it becomes `LocalReady`. |

For distributed artifacts, `artifactBuildNode` and `restorePlacementNode` are separate concepts. The build node creates or exports the artifact; it is not a scheduling promise for user sessions. Only restore placement nodes can be selected by Workload Manager, and every restore placement node must satisfy the referenced runtime's scheduling constraints.

Before a distributed placement transitions to `LocalReady`, SnapshotController must verify artifact compatibility for that node. At minimum this includes CPU model compatibility, cloud-hypervisor/Kuasar version compatibility, kernel and VMM snapshot format compatibility, and runtime class compatibility. If compatibility cannot be verified, the placement remains `Failed` or `Unavailable` and must be excluded from restore selection.

Placement cache states:

| Cache state | Restore behavior |
|---|---|
| `LocalReady` | Restore can start immediately on this node. |
| `RemoteAvailable` | Artifact exists globally, but this node has not materialized it yet. Restore selector may choose another node or trigger lazy materialization. |
| `Pulling` | Artifact materialization is in progress on this node. |
| `Failed` | Materialization failed on this node; selector excludes it until retry. |
| `Unavailable` | Node is NotReady or temporarily excluded. |

Evolution path:

1. Phase 1 accepts only `artifact.distribution=NodeLocal` and records `storageClass=NodeLocal`.
2. When Kuasar exposes export/import or pullable artifact semantics, enable `LazyRemote`; SnapshotController writes a global artifact URI/digest into `status.snapshot.artifact`.
3. Restore selector prefers `LocalReady`, can optionally choose `RemoteAvailable` if lazy materialization fits the request latency budget, and falls back to cold start when no placement can restore.
4. Enable `PreDistribute` for high-SLA runtimes; background warm distribution pre-materializes hot artifacts based on restore frequency, node capacity, and cache pressure.

This keeps the session creation path stable: Workload Manager still selects a restore placement, creates a direct Sandbox CR, and SandboxReconciler drives Kuasar restore. The only change is whether the selected placement is already local or first needs artifact materialization.

---

## 16. Open Questions

| # | Question | Scope | Decision point |
|---|---|---|---|
| Q1 | Kuasar Admin API access path: decided to add Kuasar Admin HTTP Proxy in `agentd`; no new component. | Phase 1 architecture | Closed |
| Q2 | API shape: decided to use independent lightweight `SnapStart` CRD with `runtimeRef`. | API design | Closed |
| Q3 | picod runtime model: Jupyter kernel migration is a separate work item. SnapStart protocol can proceed independently. | Phase 1 picod | Closed |
| Q4 | Image digest in template key: read resolved `imageID` after build sandbox is running; verify whether existing CRD status exposes it. | Phase 1 controller | Implementation validation |
| Q5 | Node-local storage lifecycle: node NotReady, recovery, deletion, and rebuild rules are defined. The metadata model is artifact-aware so future Kuasar distributed artifacts can replace the node-local backend without changing the SnapStart API. | Phase 1 reliability and future distribution | Closed |
| Q6 | Two-layer WarmPool operational boundary: define metrics, cost attribution, alerts, and latency attribution before combining. | Future Phase 2 | Before Phase 2 design |
| Q7 | Missing lifecycle paths are covered: deletion finalizer, runtimeRef deletion, manual rebuild, and orphan GC. Manual force-rebuild uses the Section 6.7 behavior: keep the annotation on failure and remove it only after rebuild succeeds. | Phase 1 completeness | Closed |
| Q8 | Browser Agent reference image: Phase 2 needs an official image with Chromium and BrowserWarmFork protocol support. | Phase 2 Browser Agent | Before Phase 2 development |
| Q9 | Browser Agent BoringSSL reseed validation: verify Chromium/Node random output after restore from the same snapshot. | Phase 2 Browser Agent | Reference image development |
| Q10 | Kuasar subprocess constraint for Browser Agent: closed; Kuasar only requires readiness protocol and does not inspect browser subprocess quiescence. | Phase 2 Browser Agent | Closed |
| Q11 | Kuasar inject protocol integration: closed after checking warmfork workload behavior, annotations, socket path, and wire format. | Phase 1 implementation | Closed |
| Q12 | Kuasar distributed artifact support: expose future intent through `spec.artifact.distribution=LazyRemote/PreDistribute` and status through `snapshot.artifact.storageClass=Distributed`; concrete export/import or lazy-pull API depends on Kuasar roadmap. | Future distributed restore | Track with Kuasar artifact API |
