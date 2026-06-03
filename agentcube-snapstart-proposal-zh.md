# AgentCube SnapStart 特性规格设计

> 状态：Draft  
> 版本：v0.2  

---

## 1. 背景与目标

AgentCube 当前 session sandbox 生命周期为：

```text
创建 Sandbox → 等待 Ready → 处理请求 → TTL / Idle GC 销毁
```

每个新 session 默认从镜像冷启动。对于 Code Interpreter 和 Browser Agent，主要耗时
不是 VM boot，而是 runtime 进程初始化，例如 Python 包加载、Jupyter kernel 初始化、
Chromium 启动和 CDP 就绪。

本设计利用底层 runtime / VMM 的快照能力，在 runtime 初始化完成且尚未加载用户状态时
创建可复用的启动基线。后续新 session 可以从基线恢复，避免重复执行昂贵的初始化流程。

从能力形态上，快照可以服务两类场景：

```text
启动基线快照：
在 runtime 初始化完成且尚未加载用户状态时创建，可供多个新 session 复用。

单 session 状态快照：
从已有 session sandbox 捕获可恢复状态，只用于恢复该 session。
```

第一阶段聚焦启动基线快照。单 session 状态快照作为同一抽象下的后续演进目标，要求
API 和 artifact 模型预留 1:1 恢复语义，但不要求第一阶段完整实现暂停与恢复。

### 1.1 非目标

以下能力不在本设计的第一阶段范围内：

- 保留某个已有 session 用户状态的暂停与恢复；
- 保留活动 TCP 连接；
- 跨节点迁移已有 session；
- 向用户暴露底层 Kuasar、Kata 或其他 VMM 私有参数；
- 要求用户手工创建 `SandboxTemplate`、`SandboxClaim`、`SnapshotBuildTask` 或 Fork build
  `Sandbox`。

### 1.2 快照使用模式

两类快照通过不同的使用模式区分生命周期：

| 维度 | `Fork` | `Resume` |
|---|---|---|
| 来源 | 无用户状态的 runtime 模板 | 已运行的具体 session sandbox |
| 用途 | 加速新 session 创建 | 暂停并恢复一个已有 session |
| 用户状态 | 必须不存在 | 必须保留 |
| 使用方式 | 1:N fork | 1:1 resume |
| 替换触发条件 | 模板变化、定时重建 | 每次保存状态创建新的快照对象 |
| 调度 | 可在多个节点分别构建 | 通常绑定 artifact 所在节点 |

目标设计需要让两种模式复用同一套快照构建、artifact 管理和 restore 注入流程。Fork 通过
重建策略维护可复用启动基线；Resume 每次保存 session 状态时创建新的快照对象，artifact
清理由系统级生命周期策略处理。

---

## 2. 关键设计原则

### 2.1 控制面不感知业务 Runtime 类型

AgentCube 将持续扩展新的 runtime 类型，例如 Code Interpreter、Browser Agent、通用
`AgentRuntime` 和第三方 runtime CRD。

`SandboxSnapshotController` 不应直接建模这些业务 Runtime kind。否则每新增一种 runtime，
都需要修改 SandboxSnapshot CRD schema、webhook、Controller 逻辑或 adapter registry。

Fork mode 的快照构建输入统一收敛到 agent-sandbox 标准 `SandboxTemplate`：

```text
业务 Runtime CR
→ 对应的业务 Runtime Controller
→ 自动生成标准 SandboxTemplate
→ SandboxSnapshot(snapshotMode=Fork) 引用 SandboxTemplate
```

Resume mode 引用已有 `Sandbox`。`SandboxSnapshotController` 只理解 agent-sandbox 资源，
不理解上游业务 Runtime 类型。

### 2.2 控制面不直接调用底层 runtime / VMM

AgentCube 控制面不直接调用 runtime 或 VMM 私有 API。快照操作通过内部
`SnapshotBuildTask` 资源下发：

```text
SandboxSnapshotController
→ 创建标准 spec 的 SnapshotBuildTask
→ 节点侧 agent watch 分配给本节点的 SnapshotBuildTask
→ 节点侧 SnapshotDriver 调用本机 runtime / VMM
```

### 2.3 SnapshotBuildTask 是节点任务载体

`SnapshotBuildTask` 是 `SandboxSnapshotController` 创建的内部 CRD，承载：

- 构建任务声明；
- 目标节点绑定；
- 目标 sandbox 引用；
- provider、artifact key 和 build hash；
- 节点 agent 回报的 artifact phase 和 URI。

`Fork` mode 中，任务 target 是从源 `SandboxTemplate` 创建的 build `Sandbox`。`Resume`
mode 中，任务 target 是已有源 `Sandbox`。节点 agent watch `SnapshotBuildTask`，并根据
`spec.targetNodeName` 过滤分配给本节点的任务。

### 2.4 快照删除采用最终一致语义

Phase 1 使用 `artifactLocality=NodeLocal`，因此 artifact 删除采用最终一致语义。
`SandboxSnapshot` 删除或 artifact 记录被替换时：

```text
控制面先移除 artifact 记录
→ 新 session 立即停止使用旧 artifact
→ 节点 agent 后台 GC 最终删除本地 artifact
```

控制面不要求同步等待所有节点完成物理删除。未来的远端 artifact locality 可以使用自己的
清理机制。如果需要严格审计每个节点的删除结果，再引入节点级报告资源。

---

## 3. 抽象分层

### 3.1 总体架构

```text
┌──────────────────────────────────────────────────────────────────┐
│  用户 API                                                        │
│  CodeInterpreter / AgentRuntime / 第三方 Runtime                 │
│  SandboxSnapshot / SnapshotClass                                │
└──────────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌──────────────────────────────────────────────────────────────────┐
│  业务 Runtime Controller                                         │
│  为 session 创建维护标准 SandboxTemplate                         │
│  可负责 runtime 特有的默认值、session payload 构造                │
└──────────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌──────────────────────────────────┐ artifact store ┌─────────────────────────────┐
│ SandboxSnapshotController        │<------------->│ Workload Manager            │
│ watch Snapshot / Template /      │ Controller    │ 读取 Ready artifact         │
│ Sandbox / Node / BuildTask       │ 唯一写入者    │ 注入 restore intent         │
│ 构建 artifact 并聚合 status      │               │ 添加 restore intent         │
└──────────────────────────────────┘               └─────────────────────────────┘
              │                                                   │
        SnapshotBuildTask                               session Sandbox
              ▼                                                   ▼
┌──────────────────────────────────┐               ┌─────────────────────────────┐
│ Node Agent                       │               │ K8s Runtime / VMM           │
│ watch SnapshotBuildTask          │               │ 消费 restore intent         │
│ SnapshotDriver 和 artifact GC    │               │ Kuasar / Kata / 其他实现    │
└──────────────────────────────────┘               └─────────────────────────────┘
              │
              ▼
┌──────────────────────────────────┐
│ K8s Runtime / VMM                │
│ 运行 Fork build sandbox 和 VMM API│
│ Kuasar / Kata / 其他实现         │
└──────────────────────────────────┘
```

### 3.2 业务 Runtime Controller

每种业务 runtime 自己负责将用户配置转换成标准 `SandboxTemplate`。

例如：

```text
CodeInterpreterController → SandboxTemplate
BrowserRuntimeController  → SandboxTemplate
第三方 Runtime Controller → SandboxTemplate
```

`SandboxTemplate` 是派生资源，不要求用户手工创建。建议添加：

```yaml
metadata:
  ownerReferences:
  - kind: CodeInterpreter
    name: python
  labels:
    agentcube.volcano.sh/managed-by: code-interpreter-controller
```

业务 Runtime 删除后，由 Kubernetes owner reference 自动清理模板。Controller reconcile
覆盖派生模板的手工漂移；后续可以通过 admission webhook 拒绝用户修改托管模板。

业务 Runtime 集成层还负责 runtime 特有输入。这些输入由业务集成层承载，
`SandboxSnapshotController` 只消费标准 `SandboxTemplate` 和 `SandboxSnapshot` 契约：

- 构建态环境变量；
- session restore payload；
- 鉴权信息注入。

`SandboxSnapshotController` 不依赖 adapter registry，也不根据业务 Runtime kind 分派。
任何业务特有转换都留在业务 Runtime Controller 或 Workload Manager 的业务集成层。

### 3.3 SandboxSnapshotController

`SandboxSnapshotController` 是全局状态机的唯一所有者，负责：

1. watch `SandboxSnapshot`、`SandboxTemplate`、`Sandbox`、Node 和 `SnapshotBuildTask`；
2. `Fork` mode 读取标准 `SandboxTemplate`，`Resume` mode 读取源 `Sandbox`；
3. 计算不可变 `buildHash`；
4. 根据 `SnapshotClass.nodeSelector` 和 source 调度约束筛选 target nodes；
5. 为缺少有效 artifact 的节点创建 `SnapshotBuildTask`；
6. watch `SnapshotBuildTask.status`，写入 artifact store；
7. 聚合更新 `SandboxSnapshot.status`；
8. 在 build hash 变化、手动重建或 Controller 检测到 artifact 不可用时移除旧 artifact
   记录，并按 `rebuildAfter` 执行后台定时替换；
9. 先从 artifact store 移除旧 artifact 记录，立即阻止新 restore；
10. 管理重试退避和 Kubernetes Event。

Controller 不负责：

- 直接调用 Kuasar / Kata 等 runtime API；
- 同步删除节点本地或远端快照 artifact；
- 解析 `CodeInterpreter`、`BrowserRuntime` 等业务 CRD。

### 3.4 Node Agent

Node Agent 部署在支持快照能力的节点，负责：

1. 维护本节点 snapshot provider capability label；
2. watch 分配给本节点的 `SnapshotBuildTask`；
3. 根据 `SnapshotBuildTask.spec.providerName` 选择进程内 driver；
4. 调用本机 runtime / VMM 创建 artifact；
5. 将本地 artifact phase 和 URI 回写到 `SnapshotBuildTask.status`；
6. 枚举本地 artifact 并执行最终一致 GC；
7. 校验 artifact owner UID，避免同名资源重建后错误复用；
8. 不参与 session restore 数据面路径。

Node Agent 不应直接 patch `SandboxSnapshot.status`。多个节点并发写同一对象会导致
`resourceVersion` 冲突、字段所有权混乱和 RBAC 权限扩大。`SandboxSnapshotController`
是该 status 的唯一写入者。

### 3.5 SnapshotDriver

SnapshotDriver 是 Node Agent 内部的进程内接口，不是远程 RPC：

```go
type SnapshotDriver interface {
    Name() string
    Capabilities(ctx context.Context) SnapshotDriverCapabilities

    Create(ctx context.Context, req SnapshotDriverCreateRequest) (*SnapshotDriverArtifact, error)
    Delete(ctx context.Context, artifact SnapshotDriverArtifact) error
    List(ctx context.Context) ([]SnapshotDriverArtifact, error)
    Inspect(ctx context.Context, artifact SnapshotDriverArtifact) (*SnapshotDriverArtifactStatus, error)
}

type SnapshotDriverCapabilities struct {
    SnapshotModes           []SandboxSnapshotMode
    ArtifactLocalities      []ArtifactLocality
    ProviderProtocolVersion string
}

type SnapshotDriverCreateRequest struct {
    TaskRef                 corev1.ObjectReference
    TargetRef               corev1.TypedLocalObjectReference
    TargetNodeName          string
    SnapshotMode            SandboxSnapshotMode
    ProviderName            string
    ProviderProtocolVersion string
    ArtifactKey             string
    BuildHash               string
}
```

`SnapshotBuildTask` 仍然是 Kubernetes task API。Node Agent reconciler watch 该 CRD，
校验 provider 和 target 状态后，将其转换为 `SnapshotDriverCreateRequest`，再调用进程内
driver。driver 不应直接读取 Kubernetes 对象，也不应依赖 `SnapshotBuildTask` 的
metadata/status。

节点侧 driver registry 示例：

```go
drivers := map[string]SnapshotDriver{
    "snapstart.kuasar.io": newKuasarDriver(...),
}
```

Kuasar driver 内部调用本地 Unix socket。其他 driver 可以调用 Kata、Firecracker 或其他
runtime API。AgentCube 控制面无需理解这些私有协议。

`ProviderName` 来自 `SnapshotBuildTask.spec.providerName`。Node Agent 使用它选择本地
`SnapshotDriver`，并将其写入 `SnapshotArtifact.ProviderName` 作为 artifact 元数据。

`SnapshotDriver.Create()` 负责 artifact 创建前所需的底层 readiness handshake。

`SnapshotDriverArtifact` 和 `SnapshotDriverArtifactStatus` 是 driver 私有的 artifact 表达，
可以描述节点本地或远端 artifact。Node Agent 将其执行结果转换为 artifact store 中定义的
AgentCube 标准 `SnapshotArtifact` 记录。

### 3.6 K8s Runtime / VMM 兼容层

创建快照与恢复 session 是两条不同路径：

- 创建快照：Node Agent watch `SnapshotBuildTask` 后调用 driver；
- 恢复 session：K8s runtime 在创建 Pod sandbox 前消费标准 restore intent。

构建阶段的 AgentCube 标准构建元数据由 Node Agent 内的 `SnapshotDriver` 映射为 VMM 私有
快照创建 API。恢复阶段的 AgentCube 标准 restore intent 由 K8s runtime 兼容层映射为 VMM
私有 restore 协议。Node Agent 不参与 session restore 数据面路径。

如果 session 需要从快照恢复，restore intent 必须在 Pod sandbox 创建前已经存在。不要依赖
Node Agent 在发现 Pod 后再 patch restore annotation，否则会与 CRI 创建流程产生竞态。

每种 K8s runtime 兼容层负责将 AgentCube 标准 restore intent 转换为自己的私有恢复
协议。这是跨系统兼容性契约，不是 AgentCube 进程内接口。实际实现可以位于 CRI runtime、
shim 或 VMM 集成层，不要求编译进 AgentCube Node Agent。

---

## 4. API 设计

### 4.1 SandboxSnapshot CRD

`SandboxSnapshot` 表示一个 agent-sandbox 执行环境快照声明。第 1 节中的两种使用模式
在 API 中由 `snapshotMode` 表达：`Fork` 用于从同一基线派生多个新 session，`Resume` 用于从
已有 session 创建会话状态快照并恢复该 session。

```yaml
apiVersion: runtime.agentcube.volcano.sh/v1alpha1
kind: SandboxSnapshot
metadata:
  name: python-ready
spec:
  snapshotMode: Fork
  sourceRef:
    apiGroup: extensions.agents.x-k8s.io
    kind: SandboxTemplate
    name: python
  snapshotClassName: kuasar
  forkPolicy:
    rebuildOnSourceChange: true
    rebuildAfter: 24h
```

```yaml
apiVersion: runtime.agentcube.volcano.sh/v1alpha1
kind: SandboxSnapshot
metadata:
  name: session-abc-resume
spec:
  snapshotMode: Resume
  sourceRef:
    apiGroup: agents.x-k8s.io
    kind: Sandbox
    name: session-abc
  snapshotClassName: kuasar
```

建议类型：

```go
type SandboxSnapshotSpec struct {
    // +kubebuilder:validation:Required
    SnapshotMode      SandboxSnapshotMode              `json:"snapshotMode"`
    // +kubebuilder:validation:Required
    SourceRef         corev1.TypedLocalObjectReference `json:"sourceRef"`
    // +kubebuilder:validation:Required
    SnapshotClassName string                           `json:"snapshotClassName"`
    ForkPolicy        *SandboxSnapshotForkPolicy       `json:"forkPolicy,omitempty"`
}

// +kubebuilder:validation:Enum=Fork;Resume
type SandboxSnapshotMode string

const (
    SandboxSnapshotModeFork   SandboxSnapshotMode = "Fork"
    SandboxSnapshotModeResume SandboxSnapshotMode = "Resume"
)

type SandboxSnapshotForkPolicy struct {
    // +kubebuilder:default=true
    RebuildOnSourceChange *bool            `json:"rebuildOnSourceChange,omitempty"`
    RebuildAfter          *metav1.Duration `json:"rebuildAfter,omitempty"`
}

```

`snapshotMode=Fork` 支持标准 `SandboxTemplate` 作为 `sourceRef`。`snapshotMode=Resume`
支持标准 `Sandbox` 作为 `sourceRef`。`sourceRef` 使用 Kubernetes 标准
`corev1.TypedLocalObjectReference`；`apiGroup` 和 `kind` 用于明确引用类型并为受控演进
预留空间，不用于动态解析任意业务 Runtime CRD。

`forkPolicy` 直接描述触发重新构建可复用基线的条件：

- `rebuildOnSourceChange`：生成的 `SandboxTemplate` 发生变化时重建，包括 image 引用、command、
  args、env、resources、runtime class、volume 或安全上下文；未设置时默认等同于 `true`；
- `rebuildAfter`：启动基线保持 Ready 达到该时长后重新构建。

`Resume` mode 使用固定构建行为：创建对象即触发一次快照构建。restore 是数据面动作；
`SandboxSnapshot.status.phase` 描述快照 artifact 的创建和可用性。如果同一个 session 后续
还需要再次保存状态，调用方应创建新的 `SandboxSnapshot(snapshotMode=Resume)` 对象。
artifact GC 由系统级清理策略控制。如果 `snapshotMode=Resume` 时设置了 `forkPolicy`，
admission webhook 应拒绝该对象。

镜像升级必须显式更新 `SandboxTemplate` 中的 image 引用。不支持 image 引用不变、仅
镜像仓库中同 tag 指向新 digest 的隐式更新，例如 `picod:latest` 指向新的 digest。建议
使用 digest-pinned 或带版本号的 image 引用。

### 4.2 SandboxSnapshot Status

Status 只保存聚合状态，不保存 per-node artifact 详情：

```yaml
status:
  phase: Ready
  targetNodeCount: 3
  creatingNodeCount: 0
  readyNodeCount: 2
  failedNodeCount: 1
  unavailableNodeCount: 0
  readyAt: "2026-06-02T08:00:00Z"
```

建议类型：

```go
type SandboxSnapshotPhase string

const (
    SandboxSnapshotPhasePending     SandboxSnapshotPhase = "Pending"
    SandboxSnapshotPhaseCreating    SandboxSnapshotPhase = "Creating"
    SandboxSnapshotPhaseReady       SandboxSnapshotPhase = "Ready"
    SandboxSnapshotPhaseFailed      SandboxSnapshotPhase = "Failed"
)

type SandboxSnapshotStatus struct {
    Phase                       SandboxSnapshotPhase `json:"phase"`
    TargetNodeCount             int32                  `json:"targetNodeCount"`
    CreatingNodeCount           int32                  `json:"creatingNodeCount"`
    ReadyNodeCount              int32                  `json:"readyNodeCount"`
    FailedNodeCount             int32                  `json:"failedNodeCount"`
    UnavailableNodeCount        int32                  `json:"unavailableNodeCount"`
    ReadyAt                     *metav1.Time           `json:"readyAt,omitempty"`
    Message                     string                 `json:"message,omitempty"`
}
```

这些字段描述 active artifact version 的节点覆盖情况，不表示 artifact 对象总数。
`TargetNodeCount` 表示 Controller 期望覆盖的节点数。`CreatingNodeCount`、`ReadyNodeCount`、
`FailedNodeCount` 和 `UnavailableNodeCount` 对每个目标节点最多计一次，且总和等于
`TargetNodeCount`。替换版本由 artifact store 单独记录，在提升为 active 版本前不影响当前可用性。

当 `ReadyNodeCount > 0` 时，snapshot 可用于 restore。`Phase=Ready` 表示 active artifact
set 至少有一个 Ready artifact。

对于 `Resume` mode，目标集合通常是源 Sandbox 当前所在节点，因此这些计数字段通常描述
一个节点。

降级覆盖情况由节点计数字段推导，不默认重复写入 condition。Controller 通过
Kubernetes Event 表达部分节点失败等面向人的状态变化。

### 4.3 SnapshotClass CRD

`SnapshotClass` 描述底层 provider 能力，由集群管理员维护：

```yaml
apiVersion: runtime.agentcube.volcano.sh/v1alpha1
kind: SnapshotClass
metadata:
  name: kuasar
spec:
  providerName: snapstart.kuasar.io
  providerProtocolVersion: v1
  supportedModes:
  - Fork
  - Resume
  artifactLocality: NodeLocal
  nodeSelector:
    agentcube.volcano.sh/snapshot-provider.snapstart.kuasar.io: "true"
```

建议类型：

```go
type SnapshotClassSpec struct {
    // +kubebuilder:validation:Required
    ProviderName            string                `json:"providerName"`
    // +kubebuilder:validation:Required
    ProviderProtocolVersion string                `json:"providerProtocolVersion"`
    // +kubebuilder:validation:MinItems=1
    SupportedModes          []SandboxSnapshotMode `json:"supportedModes"`
    // +kubebuilder:validation:Required
    ArtifactLocality        ArtifactLocality      `json:"artifactLocality"`
    NodeSelector            map[string]string     `json:"nodeSelector,omitempty"`
}

// +kubebuilder:validation:Enum=NodeLocal
type ArtifactLocality string

const (
    ArtifactLocalityNodeLocal ArtifactLocality = "NodeLocal"
)
```

该示例 class 声明共享的 Kuasar 能力：

```text
supportedModes = [Fork, Resume]
artifactLocality = NodeLocal
```

API 模型包含 `snapshotMode=Resume`，Phase 1 实现使用 `snapshotMode=Fork` 和
`artifactLocality=NodeLocal`。

`SandboxSnapshot.spec.snapshotMode` 必须包含在 `SnapshotClass.spec.supportedModes` 中。
admission webhook 同时拒绝不支持的 `sourceRef.kind`，并拒绝在 `snapshotMode=Resume` 时设置
`forkPolicy`。
`providerName` 是控制面和 Node Agent 使用的稳定 provider 标识，会映射到 Node Agent
本地 `SnapshotDriver` 实现；它不表示远程 provider 服务。`artifactLocality=NodeLocal`
表示 artifact 只能在创建它的节点上使用，不表示系统会分发或复制 artifact。后续可以增加
新的 provider 和 artifact locality mode，但不向业务用户暴露 VMM 私有配置。

`supportedModes` 是能力集合，因为实际使用模式由 `SandboxSnapshot.spec.snapshotMode`
选择。`artifactLocality` 保持单值，因为它为该 class 选择一种 artifact 放置和清理策略。
如果同一个 provider 后续支持远端 artifact，应新增类似 `kuasar-remote` 的 class，而不是在
一个 class 中混合多种 locality 策略。

### 4.4 Artifact Store

artifact 元数据保存在 artifact store 中，不写入 CRD status。第一阶段继续使用
Redis / Valkey：

```text
key   = snapshot:{ownerKind}:{namespace}:{ownerName}:{ownerUID}
value = JSON-encoded owner-specific store record
```

通用结构：

```go
type SnapshotArtifactSet struct {
    ArtifactKey string             `json:"artifactKey"`
    BuildHash   string             `json:"buildHash"`
    Artifacts   []SnapshotArtifact `json:"artifacts,omitempty"`
    CreatedAt   time.Time          `json:"createdAt,omitempty"`
}

type SnapshotArtifact struct {
    ProviderName            string                `json:"providerName"`
    ProviderProtocolVersion string                `json:"providerProtocolVersion"`
    NodeName                string                `json:"nodeName,omitempty"`
    Phase                   SnapshotArtifactPhase `json:"phase"`
    ArtifactURI             string                `json:"artifactURI,omitempty"`
    ArtifactKey             string                `json:"artifactKey"`
    BuildHash               string                `json:"buildHash"`
    CreatedAt               *time.Time            `json:"createdAt,omitempty"`
    Retry                   *SnapshotBuildRetry   `json:"retry,omitempty"`
    Message                 string                `json:"message,omitempty"`
}

type SnapshotBuildRetry struct {
    FailureCount int32      `json:"failureCount,omitempty"`
    LastFailedAt *time.Time `json:"lastFailedAt,omitempty"`
    NextRetryAt  *time.Time `json:"nextRetryAt,omitempty"`
}

type SnapshotArtifactPhase string

const (
    SnapshotArtifactPhaseCreating    SnapshotArtifactPhase = "Creating"
    SnapshotArtifactPhaseReady       SnapshotArtifactPhase = "Ready"
    SnapshotArtifactPhaseFailed      SnapshotArtifactPhase = "Failed"
    SnapshotArtifactPhaseUnavailable SnapshotArtifactPhase = "Unavailable"
)

type SnapshotArtifactSetRef struct {
    ArtifactKey string `json:"artifactKey,omitempty"`
}

type SnapshotArtifactManifest struct {
    OwnerRef      metav1.OwnerReference          `json:"ownerRef"`
    // ArtifactSets 以 ArtifactKey 为 key，保存对应的 artifact set。
    // 该 map key 由 SandboxSnapshotController 生成，且必须与
    // SnapshotArtifactSet.ArtifactKey 一致。
    ArtifactSets  map[string]SnapshotArtifactSet `json:"artifactSets,omitempty"`
    ActiveSetRef  SnapshotArtifactSetRef         `json:"activeSetRef,omitempty"`
    PendingSetRef SnapshotArtifactSetRef         `json:"pendingSetRef,omitempty"`
}
```

其中：

- `OwnerRef`：使用 Kubernetes 标准 `metav1.OwnerReference`；namespace 已包含在 artifact
  store key 中，不在 value 中重复保存；
- `ArtifactSets`：以 `ArtifactKey` 为 key 保存对应的 artifact set，使同一节点可以临时
  保留属于不同逻辑版本的多个 artifact；
- `ActiveSetRef` 和 `PendingSetRef`：通过 `ArtifactKey` 指向 `ArtifactSets` 中的条目；
- `Artifacts`：保存同一个 artifact set 下的具体 artifact。对于 `NodeLocal` artifact，
  `NodeName` 必填，且同一个 `SnapshotArtifactSet` 内每个 node 最多对应一个 artifact；
- `ArtifactKey`：由 `SandboxSnapshotController` 生成，用于标识跨节点一致的逻辑快照版本。
  在可行时应保持可读，例如 `<normalized-snapshot-name>-<mode>-g<generation>-r<rebuildSeq>`。
  它用于日志、排查和 artifact 分组，但不是兼容性边界。实际校验仍以 Snapshot UID、
  `BuildHash`、`ProviderName` 和 `ProviderProtocolVersion` 为准；
- `ArtifactURI`：以 runtime 可消费的 URI 形式标识快照 artifact，且必须包含 scheme。对于
  `NodeLocal` artifact，标准逻辑 URI 是 `local://snapshot/<artifact-key>`；runtime 兼容层在
  Sandbox 实际调度到的节点上将其解析为节点本地物理 artifact。未来远端 artifact locality
  可以使用 `s3://`、`https://`、`oci://` 或 `pvc://` 等 scheme。AgentCube 将该 URI 作为
  opaque 值保存并透传。`ArtifactURI` 仅在 `Phase=Ready` 时设置；非 Ready 阶段必须为空；
- `CreatedAt`：node artifact 创建完成后设置；artifact 仍在创建中或尚未产生可用产物时
  不设置；
- `Retry`：记录该 artifact 的构建重试状态。将 retry 字段内聚在嵌套结构中，避免与
  artifact 身份字段混在同一层；
- `ProviderName`：标识创建该 artifact 的 snapshot provider；
- `ProviderProtocolVersion`：记录创建 artifact 时使用的 provider 构建协议版本，并在注入
  restore intent 前校验；
- `BuildHash`：用于阻止恢复与当前构建输入不兼容的旧 artifact。

示例：

```text
BuildHash   = sha256:def456
ArtifactKey = python-ready-fork-g12-r1
ArtifactURI = local://snapshot/python-ready-fork-g12-r1
```

`SnapshotArtifactManifest` 是一个 `SandboxSnapshot` 对应的 owner 维度 artifact store
逻辑清单。它记录 artifact set、具体 artifact 条目，以及 restore 和 rebuild 流程使用的
active/pending artifact set 引用。具体实现可以存为单个 Redis / Valkey value，也可以拆分为
artifact set 记录和 artifact 条目。它是控制面内部数据记录。

`SnapshotArtifactPhase` 可以在不同 Snapshot CRD 间复用，因为它描述节点本地 artifact 的
生命周期阶段。`Creating` 表示 artifact 构建已被声明或正在执行，但尚不能用于 restore。
`SandboxSnapshotPhase` 被 `Fork` 和 `Resume` 两种 mode 共享；它描述快照 artifact 的创建
和可用性，不描述数据面 restore 是否完成。

`SandboxSnapshotController` 是其 `SnapshotArtifactManifest` 的唯一写入者。Node Agent
仅通过 `SnapshotBuildTask.status` 上报本地 artifact phase，不直接写 artifact store。合法读者
包括 `SandboxSnapshotController` 和 Workload Manager：前者读取自身写入的数据用于重建决策，
后者在注入 restore intent 前只使用 `ActiveSetRef.ArtifactKey` 下 phase 为 Ready
的 artifact。

Controller 使用 Redis / Valkey transaction 或 compare-and-set 原子更新 store record。
`Fork` mode 中，定时重建期间保留 `ActiveSetRef.ArtifactKey`，同时填充
`PendingSetRef.ArtifactKey`；source 变化时，先清空 `ActiveSetRef`，再构建不兼容的新版本。
`Resume` mode 中，active artifact 表示保存下来的 session 状态。artifact 重试状态统一由
Controller 在 `SnapshotArtifact.Retry` 中读写。

`Resume` mode 通常只使用 `ActiveSetRef`；`PendingSetRef` 主要用于 `Fork` mode
中的兼容后台替换。

后续 SandboxSnapshot 流程中的 `activeSetRef.artifactKey` 和 `pendingSetRef.artifactKey`
是 active/pending artifact set 引用的简写。

### 4.5 SnapshotBuildTask CRD

`SnapshotBuildTask` 是内部 CRD，是 `Fork` 和 `Resume` 构建统一使用的节点侧任务对象。
用户不创建该对象。

```yaml
apiVersion: runtime.agentcube.volcano.sh/v1alpha1
kind: SnapshotBuildTask
metadata:
  name: python-ready-node-a-r1
  ownerReferences:
  - apiVersion: runtime.agentcube.volcano.sh/v1alpha1
    kind: SandboxSnapshot
    name: python-ready
    uid: ...
spec:
  snapshotRef:
    name: python-ready
  snapshotUID: ...
  snapshotMode: Fork
  targetRef:
    apiGroup: agents.x-k8s.io
    kind: Sandbox
    name: python-ready-build-node-a
  targetNodeName: node-a
  providerName: snapstart.kuasar.io
  providerProtocolVersion: v1
  artifactKey: python-ready-fork-g12-r1
  buildHash: sha256:def456
status:
  phase: Ready
  artifactURI: local://snapshot/python-ready-fork-g12-r1
  observedAt: "2026-06-02T08:00:00Z"
```

建议类型：

```go
type SnapshotBuildTaskSpec struct {
    SnapshotRef             corev1.LocalObjectReference      `json:"snapshotRef"`
    SnapshotUID             types.UID                        `json:"snapshotUID"`
    SnapshotMode            SandboxSnapshotMode              `json:"snapshotMode"`
    TargetRef               corev1.TypedLocalObjectReference `json:"targetRef"`
    TargetNodeName          string                           `json:"targetNodeName"`
    ProviderName            string                           `json:"providerName"`
    ProviderProtocolVersion string                           `json:"providerProtocolVersion"`
    ArtifactKey             string                           `json:"artifactKey"`
    BuildHash               string                           `json:"buildHash"`
}

type SnapshotBuildTaskStatus struct {
    Phase       SnapshotArtifactPhase `json:"phase,omitempty"`
    ArtifactURI string                `json:"artifactURI,omitempty"`
    Message     string                `json:"message,omitempty"`
    ObservedAt  *metav1.Time         `json:"observedAt,omitempty"`
}
```

`targetRef` 是被创建快照的 sandbox：

- `Fork`：`SandboxSnapshotController` 从源 `SandboxTemplate` 创建的 build `Sandbox`；
- `Resume`：已有源 session `Sandbox`。

`SnapshotBuildTask.status.phase` 是 Node Agent 对本节点 snapshot artifact 的回报。Controller
创建任务时先在 artifact store 中写入 `Creating` 条目。Node Agent 在
`SnapshotDriver.Create()` 返回后回写 `Ready` 或 `Failed`。Controller 可以根据节点丢失、task
超时或 capability 不匹配等控制面信号推导 `Unavailable`。

构建阶段协议字段放在 `SnapshotBuildTask` 上，而不是放在 target `Sandbox` annotation 上：

- 构建意图写入 `SnapshotBuildTask.spec`；
- 构建结果写入 `SnapshotBuildTask.status`；
- Fork build `Sandbox` 只是 `targetRef` 引用的 runtime target；
- session `Sandbox` 只保留 runtime 兼容层消费的 restore intent annotation。

Node Agent 只处理 `spec.targetNodeName` 等于本节点名称的任务，并且只写
`SnapshotBuildTask.status`。它不写 artifact store，也不写 `SandboxSnapshot.status`。

### 4.6 session Sandbox restore intent

Workload Manager 创建 session Sandbox 前注入：

```text
agentcube.volcano.sh/snapshot-artifact-uri = ...
```

K8s runtime 在创建 Pod sandbox 时消费该 AgentCube 标准 annotation。
runtime 兼容层由 session Sandbox 的 `runtimeClassName` 或底层 runtime 实现决定。
`snapshot-artifact-uri` 使用 runtime 可消费的逻辑 artifact URI。对于 `NodeLocal`，取值为
`local://snapshot/<artifact-key>`。Kuasar 私有 `kuasar.io/*` annotation 只能存在于 Kuasar
兼容层内部。

在 session `Sandbox` 上，`snapshot-artifact-uri` 存在即表示请求 restore。构建结果使用
`SnapshotBuildTask.status.artifactURI` 表达。restore 兼容性由 runtime 兼容层和 artifact URI
scheme 定义。

---

## 5. 端到端流程

### 5.1 用户定义 Runtime

用户创建业务 Runtime：

```yaml
apiVersion: runtime.agentcube.volcano.sh/v1alpha1
kind: CodeInterpreter
metadata:
  name: python
spec:
  template:
    image: picod:v1
    args: [--preload=numpy,pandas]
```

`CodeInterpreterController` 自动生成标准模板：

```yaml
apiVersion: extensions.agents.x-k8s.io/v1alpha1
kind: SandboxTemplate
metadata:
  name: python
  ownerReferences:
  - apiVersion: runtime.agentcube.volcano.sh/v1alpha1
    kind: CodeInterpreter
    name: python
  labels:
    agentcube.volcano.sh/managed-by: code-interpreter-controller
spec:
  podTemplate:
    spec:
      runtimeClassName: kuasar
      containers:
      - name: code-interpreter
        image: picod:v1
        args: [--preload=numpy,pandas]
```

用户无需手工创建 `SandboxTemplate`。

### 5.2 管理员启用 Fork Snapshot

管理员创建：

```yaml
apiVersion: runtime.agentcube.volcano.sh/v1alpha1
kind: SandboxSnapshot
metadata:
  name: python-ready
spec:
  snapshotMode: Fork
  sourceRef:
    apiGroup: extensions.agents.x-k8s.io
    kind: SandboxTemplate
    name: python
  snapshotClassName: kuasar
  forkPolicy:
    rebuildOnSourceChange: true
    rebuildAfter: 24h
```

### 5.3 用户或 Workload Manager 创建 Resume Snapshot

当需要暂停并恢复某个已有 session 时，业务侧创建 `snapshotMode=Resume` 的
`SandboxSnapshot`：

```yaml
apiVersion: runtime.agentcube.volcano.sh/v1alpha1
kind: SandboxSnapshot
metadata:
  name: session-abc-resume
spec:
  snapshotMode: Resume
  sourceRef:
    apiGroup: agents.x-k8s.io
    kind: Sandbox
    name: session-abc
  snapshotClassName: kuasar
```

### 5.4 Controller 创建构建任务

`Fork` mode 中，`SandboxSnapshotController`：

```text
1. 读取 SandboxTemplate。
2. 计算 buildHash，并生成新的 pending artifact set 引用。
3. 读取 SnapshotClass。
4. 根据 SnapshotClass.nodeSelector 和 source 调度约束筛选 target nodes。
5. 查询 artifact store。
6. 为每个缺少有效 artifact 的目标节点创建一个 build Sandbox。
7. 每个 build Sandbox 使用 podTemplate.spec.nodeName 绑定目标节点。
8. 为每个 build Sandbox 创建一个 SnapshotBuildTask。
9. 将 SnapshotBuildTask.targetRef 指向 build Sandbox。
```

`Resume` mode 中，`SandboxSnapshotController`：

```text
1. 读取源 Sandbox。
2. 根据源 Sandbox 身份、runtime 相关 spec 和 provider protocol 计算 buildHash。
3. 选择当前承载该 Sandbox 的节点。
4. 为该节点创建一个 SnapshotBuildTask。
5. 将 SnapshotBuildTask.targetRef 指向源 Sandbox。
6. task 成功后，将生成的 artifact 记录到 active artifact set 引用下。
```

### 5.5 Node Agent 创建 artifact

目标节点上的 Node Agent：

```text
1. watch spec.targetNodeName=<本节点> 的 SnapshotBuildTask。
2. 读取 task.spec.targetRef。
3. Fork 模式等待目标 build Sandbox 进入 Running。
4. Resume 模式校验源 Sandbox 仍运行在本节点。
5. 调用本机 SnapshotDriver.Create()；driver 内部完成底层 readiness handshake。
6. 将 artifact URI 和 artifact phase 回写到 SnapshotBuildTask.status。
```

`SandboxSnapshotController` watch 到 `SnapshotBuildTask.status` 后：

```text
1. 写入 artifact store。
2. 聚合 SandboxSnapshot.status。
3. artifact set 可用后，将 PendingSetRef 提升为 ActiveSetRef。
4. 删除已完成的 SnapshotBuildTask。
5. Fork task 完成后删除临时 build Sandbox。
```

只要至少一个 artifact Ready：

```yaml
status:
  phase: Ready
  readyNodeCount: 1
```

### 5.6 从 Fork Snapshot 创建新 Session

用户仍然通过原有 Router / SDK 创建 session，不感知 SandboxSnapshot：

```text
Router
→ Workload Manager
→ 创建 session Sandbox
```

Workload Manager：

```text
1. 根据 session 使用的 SandboxTemplate 查找可用 SandboxSnapshot(snapshotMode=Fork)。
2. 从 artifact store 读取 SnapshotArtifactManifest。
3. 将 manifest.ActiveSetRef.ArtifactKey 解析为 active SnapshotArtifactSet。
4. 确认 active set 至少存在一个 Ready artifact。
5. 校验 Snapshot UID、provider name、provider 协议版本、artifactKey 和 buildHash。
6. 存在有效 artifact：使用 active set 的逻辑 artifact URI 注入 restore intent。
7. 不存在有效 artifact：创建普通 Sandbox，冷启动。
```

Workload Manager 从 artifact store 中的 active artifact set 生成 restore intent。对于
`NodeLocal`，`snapshot-artifact-uri` annotation 使用 `local://snapshot/<artifact-key>`，
由 runtime 在实际调度节点上解析。

底层 K8s runtime 在 Pod sandbox 创建前消费 restore intent：

```text
存在 snapshot-artifact-uri
→ runtime 尝试从 ArtifactURI restore

不存在 snapshot-artifact-uri
→ 普通冷启动

注入 restore intent 后 restore 失败
→ runtime 按自身实现处理失败
```

Router 和 SDK 不需要区分 restore 与冷启动。

### 5.7 从 Resume Snapshot 恢复 Session

恢复已有 session 时，Workload Manager：

```text
1. 查找为该 session 创建的 Ready SandboxSnapshot(snapshotMode=Resume)。
2. 从 artifact store 读取 SnapshotArtifactManifest。
3. 将 manifest.ActiveSetRef.ArtifactKey 解析为 active SnapshotArtifactSet。
4. 确认 active set 存在 Ready artifact。
5. 校验 Snapshot UID、provider name、provider 协议版本、artifactKey 和 buildHash。
6. 注入 resume restore intent。
7. 数据面 restore 结果不改变 SandboxSnapshot status。
```

没有组件向 agent-sandbox 对象回写通用 restore 结果。如果 restore 失败，本次 resume 从调用方
视角失败。业务层如果允许丢弃旧 session 状态，应通过普通 session 创建路径创建新 session。

### 5.8 模板变化与自动重建

业务 Runtime spec 更新后：

```text
业务 Runtime Controller 更新 SandboxTemplate
→ build hash 变化
→ SandboxSnapshotController 清空 ActiveSetRef，并移除旧 artifact 记录
→ 新 session 暂时冷启动
→ 创建新的 build Sandbox 和 SnapshotBuildTask
→ 新 artifact Ready
→ 新 session 开始使用新 artifact
→ Node Agent 后台 GC 删除旧 artifact
```

source 发生不兼容变化时，`SandboxSnapshotController` 先清空 `ActiveSetRef`，再创建替换
构建任务。新 session 在替换 artifact set Ready 前走冷启动。

### 5.9 删除 SandboxSnapshot

管理员删除 `SandboxSnapshot`：

```text
SandboxSnapshotController
→ 从 artifact store 移除该 Snapshot 的所有 artifact 记录
→ 新 session 立即停止使用旧 artifact
→ 删除 SandboxSnapshot

Node Agent 本地 GC
→ 枚举 artifact owner
→ 发现 SandboxSnapshot UID 已不存在
→ 最终删除本地 artifact
```

第一阶段 Finalizer 只保护控制面元数据清理，不同步等待全部节点本地文件删除。

### 5.10 定时重建

`rebuildAfter` 触发后台替换，不会先移除当前可用 artifact：

```text
SandboxSnapshot 保持 Ready
→ Controller 生成 PendingSetRef，并创建新的 build Sandbox 和 SnapshotBuildTask
→ ActiveSetRef 下的 artifact 继续服务 session
→ 新 artifact Ready
→ Controller 原子地将 PendingSetRef 提升为 ActiveSetRef
→ Node Agent 后台 GC 删除旧 artifact
```

这与 source 变化不同。source 变化会使旧 artifact 与新模板不兼容，因此必须立即移除旧
artifact 记录；新 artifact Ready 前，新 session 暂时冷启动。

---

## 6. Build Hash 与版本校验

`buildHash` 标识会影响 artifact 内容或 restore 兼容性的构建输入。
`SandboxSnapshotController` 在创建新的 `SnapshotArtifactSet` 前计算该值。语义相同的输入
必须得到相同的 hash。

建议输入模型：

```text
BuildHashInput {
  snapshotMode
  sourceIdentity
  sourcePodTemplateSpec
  snapshotClass
}
```

`sourceIdentity` 按 mode 定义：

- `Fork`：源 `SandboxTemplate` 的 namespace、name 和 UID；
- `Resume`：源 `Sandbox` 的 namespace、name 和 UID。

`sourcePodTemplateSpec` 按 mode 定义：

- `Fork`：规范化后的 `SandboxTemplate.spec.podTemplate.spec`；
- `Resume`：规范化后的源 `Sandbox.spec.podTemplate.spec`。

`snapshotClass` 包含：

- `snapshotClassName`；
- `SnapshotClass.spec.providerName`；
- `SnapshotClass.spec.providerProtocolVersion`；
- `SnapshotClass.spec.artifactLocality`。

Controller 使用稳定 JSON serializer 序列化 `BuildHashInput`，并计算：

```text
sha256(stableJSON(BuildHashInput))
```

规范化规则：

- 纳入规范化后的完整 pod template spec，而不是维护容易遗漏字段的 pod 字段白名单；
- 不包含对象 `status`；
- 除显式列出的 source identity 字段外，不包含 Kubernetes metadata；
- 不包含 `resourceVersion`、`generation`、`managedFields` 或 `creationTimestamp`；
- 序列化前对 map key 排序；
- 保留 list 顺序，因为 Kubernetes 中 containers、initContainers、volumes、env、
  volumeMounts 和 tolerations 等字段的 list 顺序具有语义；
- 不解引用 Secret、ConfigMap、PVC 或 projected volume 的内容，只 hash pod template spec
  中出现的引用。

`buildHash` 是 restore 兼容性边界，不是被捕获 runtime 内存状态的 hash。

不支持 image 引用不变、仅镜像仓库中同 tag 指向新 digest 的隐式更新。镜像升级必须
显式更新模板中的 image 引用。建议使用 digest-pinned 或带版本号的 image 引用。

---

## 7. 快照 Readiness 与 Restore 注入

### 7.1 分层职责

readiness 和 restore 注入是通用控制面之下的扩展点：

| 层次 | 职责 |
|---|---|
| `SandboxSnapshotController` | 创建构建任务并聚合结果，不理解任何 runtime 私有协议 |
| `SnapshotDriver` | 执行 runtime / VMM 特有 readiness handshake 和 artifact 操作 |
| Runtime 兼容层 | 将 AgentCube 标准 restore intent 转换为私有恢复协议 |

`SandboxSnapshot` 只声明快照来源，不声明 runtime 内部的快照时机。具体何时具备快照条件
由对应 `SnapshotDriver` 判断。增加新 runtime 时不应修改 CRD 或 `SandboxSnapshotController`。

### 7.2 Driver Readiness

driver 的 `Create()` 实现自行判断何时可以创建底层 artifact。对于 Kuasar Fork snapshot，
Kuasar 集成层使用 inject socket readiness probe；runtime 先通过该 socket 返回
capabilities，再由 driver 创建 artifact。

其他 driver 可以使用 shim API、VMM API、guest-agent handshake；如果 create API 本身
已经保证 readiness，也可以不增加额外握手。AgentCube 不强制定义通用 HTTP endpoint。

### 7.3 标准 Restore Intent

AgentCube 标准 restore intent 保持最小化：

```text
agentcube.volcano.sh/snapshot-artifact-uri = <ArtifactURI>
```

该 annotation 存在表示 runtime 应在 Pod sandbox 创建阶段尝试从指定 artifact URI restore。
该 annotation 不存在时，runtime 走普通冷启动路径。该 annotation 只作用于 session `Sandbox`
restore intent；构建结果使用 `SnapshotBuildTask.status.artifactURI` 表达。AgentCube 将
artifact URI 作为 opaque 值处理；URI scheme 支持范围由 runtime 兼容层定义。对于
`NodeLocal` artifact，标准逻辑 URI 是 `local://snapshot/<artifact-key>`。

如果存在业务 session 上下文，它不属于标准 restore intent，由业务 runtime 集成层自行处理。

### 7.4 Runtime 兼容层

Runtime 兼容层负责将 AgentCube restore intent 映射为自己的 VMM 私有 restore 协议。兼容层
由 session Sandbox 的 `runtimeClassName` 或底层 runtime 实现决定；使用
`snapshot-artifact-uri` 作为 runtime 可消费的 artifact URI。对于 `NodeLocal`，兼容层在
Sandbox 实际调度到的节点上解析 `local://snapshot/<artifact-key>`，然后按 runtime 的恢复策略
执行 restore、冷启动或失败。兼容性由 runtime 兼容层和 artifact URI scheme 定义。在 Pod
sandbox 创建路径上，兼容层使用 Sandbox annotation 和 runtime-local state。AgentCube 不调用
restore API，也不定义进程内 restore 接口。

对于 Kuasar，兼容层复用现有 inject socket 流程：

```text
CAPABILITIES -> PREPARE -> READY -> COMMIT -> STARTED
```

其他 runtime 在 CRI runtime、shim 或 VMM 集成层实现等价兼容能力。通用 Workload Manager
和 `SandboxSnapshotController` 无需修改。

### 7.5 接入非 Kuasar Runtime

非 Kuasar runtime 通过相同边界接入：

| 扩展点 | 是否必需 | 所在位置 |
|---|---|---|
| 生成 `SandboxTemplate` 和 session payload 的业务 Runtime Controller | 是 | 业务控制面 |
| 使用稳定 provider 名称注册的 `SnapshotDriver` | 是 | Node Agent |
| 消费 AgentCube 标准 restore intent 的 runtime 兼容层 | 是 | CRI runtime、shim 或 VMM 集成层 |
| 选择 provider capability 和 target nodes 的 `SnapshotClass` | 是 | 集群配置 |

新增 runtime 时，`SandboxSnapshot`、`SnapshotBuildTask` API、artifact 结构和
`SandboxSnapshotController` 保持稳定。第一阶段使用进程内 registry，因此交付新 driver
仍可能需要重新构建或扩展 Node Agent 发布物。后续可以引入插件机制调整交付方式，同时保持
上述契约稳定。

---

## 8. SandboxTemplate、SandboxClaim 与 WarmPool

### 8.1 SandboxTemplate 是标准 Runtime 模板

`SandboxTemplate` 是业务 Runtime Controller 生成并维护的标准 runtime 执行模板。
用户创建业务 runtime 对象，controller 派生对应的 `SandboxTemplate`。

任何支持 session 创建的 runtime 都稳定 reconcile 一个 `SandboxTemplate`，不依赖 WarmPool
或 SnapStart 是否开启。`SandboxSnapshot` Fork mode 只消费该模板，不反向触发模板生成。

### 8.2 SandboxClaim 属于 WarmPool 路径

`SandboxClaim` 是 WarmPool session 创建路径中的内部资源，用户无需手工创建。

当用户配置：

```yaml
kind: CodeInterpreter
spec:
  warmPoolSize: 3
```

业务 Runtime Controller 自动生成或更新 WarmPool 专属资源：

```text
SandboxWarmPool
```

`SandboxTemplate` 已经作为 runtime 的标准执行模板存在，`SandboxWarmPool` 引用该模板。

当新 session 选择 WarmPool 路径时，Workload Manager 自动创建：

```text
SandboxClaim
```

`SandboxClaim` 随后从 `SandboxWarmPool` 中领取一个已预创建的 Sandbox。

### 8.3 Phase 1 Session 路径选择

Phase 1 保持 Snapshot restore 路径和 WarmPool 路径独立。一次 session 创建请求只选择
其中一条路径。

```text
Snapshot restore 路径：
Workload Manager 直接创建带 restore intent 的新 Sandbox

WarmPool 路径：
SandboxClaim 领取已预热 Sandbox
```

两条路径共享业务 Runtime Controller 生成的 `SandboxTemplate`。

如果业务 runtime 在 Phase 1 同时启用 WarmPool 和 Snapshot 配置，业务 runtime 集成层或
Workload Manager 根据 runtime 策略选择 session 路径。默认策略是：存在可用 warm slot 的
请求优先使用 WarmPool；否则 Snapshot restore 路径可以直接创建带 restore intent 的新
Sandbox。

Phase 1 中，WarmPool 补充槽位不使用 Snapshot restore。WarmPool + Snapshot 的组合可以放到
后续阶段单独设计，例如：

```text
SandboxWarmPool 补充空闲槽位
→ 创建 Sandbox
→ 从 SandboxSnapshot 恢复
→ SandboxClaim 领取已恢复完成的 Sandbox
```

后续组合仍使用标准 restore intent annotation。对于 `NodeLocal` artifact，runtime 兼容层在
refill Sandbox 实际调度到的节点上解析 `local://snapshot/<artifact-key>`。

---

## 9. 节点发现与调度

### 9.1 Capability Label

Node Agent 启动后，根据本机已注册 provider 维护 Node label：

```text
agentcube.volcano.sh/snapshot-provider.snapstart.kuasar.io=true
```

`SnapshotClass.nodeSelector` 使用该 label 筛选节点。如果所选 provider 实现的
`SnapshotDriverCapabilities.ProviderProtocolVersion` 与 `SnapshotClass.spec.providerProtocolVersion`
不一致，Node Agent 必须拒绝构建任务。

provider label 只是发现提示，不是兼容性证明。Controller 和 Node Agent 仍需根据
`SnapshotClass` 与 `SnapshotDriverCapabilities` 校验 provider name、provider protocol
version、snapshot mode 和 artifact locality。

### 9.2 构建节点筛选

`SandboxSnapshotController` 负责筛选 target build nodes。

`Fork` mode 中，target build node 必须同时满足：

1. Node `Ready=True`；
2. 匹配 `SnapshotClass.nodeSelector`；
3. 支持选定的 provider name、provider protocol version、snapshot mode 和 artifact locality；
4. 满足源 pod template 的 RuntimeClass 调度约束；
5. 满足源 pod template 中的硬调度约束，例如 node selector、required node affinity、
   tolerations 和显式 `nodeName`。

`Resume` mode 中，target build node 是当前承载源 Sandbox 的节点。如果源 Sandbox 没有
分配节点或已经不再运行，Controller 应将该 snapshot 标记为失败。

### 9.3 session 恢复调度

Workload Manager 保持 session 调度与 per-node artifact 可用性解耦。`SandboxSnapshot`
Ready 且存在 active artifact set 时，Workload Manager 在创建 session Sandbox 前注入
restore intent，并由 Kubernetes scheduler 正常完成调度。

对于 `NodeLocal` artifact，restore intent URI 是 `local://snapshot/<artifact-key>`。
Runtime 兼容层在 Sandbox 实际调度到的节点上解析该逻辑 URI。如果该节点存在对应本地
artifact，runtime 可以从该 artifact restore；否则 runtime 按自身恢复策略冷启动或失败。

`SnapshotArtifact.NodeName` 仍保存在 artifact store 中，用于 per-node status 聚合和 GC。
session 调度由 Kubernetes scheduler 负责。

---

## 10. Artifact GC

### 10.1 Owner 元数据

snapshot artifact 记录：

```text
ownerReference:
  apiVersion
  kind
  name
  uid
artifactKey
buildHash
createdAt
providerName
```

namespace 已包含在 artifact store key 和 GC 查询上下文中，不在 Kubernetes
`OwnerReference` value 中重复保存。

对于 `NodeLocal` artifact，这些元数据也保存在节点本地物理 artifact 中，便于 Node Agent
将 runtime artifact 与控制面记录匹配。对于远端 artifact locality，owner 元数据根据 provider
契约保存在 artifact 记录或远端 backend 元数据中。

### 10.2 NodeLocal Artifact GC

Node Agent 定期扫描节点本地 driver artifacts：

```text
列出本地 driver artifacts
→ 查询 Snapshot owner UID 是否仍存在
→ 查询 artifact 是否仍存在对应的 artifact store 记录
→ owner 不存在或 artifact 记录已移除：删除本地 artifact
→ 保留宽限期，避免误删刚创建但尚未写入 store 的 artifact
```

远端 artifact 的物理清理由 provider 决定。AgentCube 从 artifact store 移除 artifact 记录；
provider 或远端 backend 根据自身保留和删除契约完成最终物理清理。

### 10.3 异常场景

| 场景 | 处理 |
|---|---|
| Controller 在 artifact 创建后、写 artifact store 前崩溃 | Node Agent 宽限期后 GC 未追踪 artifact |
| Snapshot owner 删除 | artifact 记录先删除；Node Agent 最终删除 artifact |
| 节点 NotReady | Controller 将该节点上的 artifact 标记为 `Unavailable`；其他 Ready artifact 仍可使用 |
| 节点恢复 | Controller 校验该节点 artifact，例如创建 validation task 或使用 driver inspection；校验成功后重新标记为 `Ready` |
| 节点永久删除 | 控制面删除 artifact 记录；无需等待物理文件清理 |
| build hash 变化 | 旧 artifact 记录立即移除；旧 artifact 后台 GC |

---

## 11. 状态机

### 11.1 SandboxSnapshot

`Fork` mode：

```text
Pending
  │
  │ 创建 build Sandbox 和 SnapshotBuildTask
  ▼
Creating
  ├───────────────┐
  │ 至少一个 Ready │ 所有节点失败
  ▼               ▼
Ready           Failed
  │
  │ source 不兼容变化 / active artifact 不可用
  └──────────────▶ Creating

Ready
  │
  │ rebuildAfter 后台替换
  └──────────────▶ Ready
```

active artifact 不兼容或不可用时，Controller 移除旧 artifact 记录并直接进入
`Creating`。执行兼容的定时后台替换时，pending 版本构建期间保持 `Ready`。

`Phase=Ready` 的条件：

```text
至少一个 artifact.phase == Ready
且 artifact.buildHash == 当前 build hash
```

`Resume` mode：

```text
Pending
  │
  │ 在源 Sandbox 所在节点创建 SnapshotBuildTask
  ▼
Creating
  ├───────────────┐
  │ artifact Ready │ 构建失败
  ▼               ▼
Ready           Failed
```

`Phase=Ready` 表示 resume artifact 为 Ready。数据面 restore 不改变
`SandboxSnapshot` phase。

### 11.2 SnapshotBuildTask

```text
创建
→ 设置 targetNodeName
→ 解析 targetRef
→ Fork：目标 build Sandbox Running
→ Resume：源 Sandbox 仍在目标节点
→ Node Agent driver.Create()
→ driver readiness handshake 成功
→ artifact phase Ready | Failed
→ Node Agent 写 task status
→ Controller 收集 task status
→ Controller 删除已完成 task
→ Fork：Controller 删除临时 build Sandbox
```

### 11.3 session Sandbox

```text
创建 session
→ active artifact set 可用？
   ├─ 否：普通 Sandbox 冷启动
   └─ 是：注入带逻辑 artifact URI 的 restore intent
          → runtime restore
          → restore 或 runtime-level fallback
```

---

## 12. 故障处理

| 故障 | 行为 |
|---|---|
| Fork mode 无 target node | `SandboxSnapshot.status.phase=Failed`，session 冷启动 |
| Resume 源 Sandbox 不存在或不再运行 | `SandboxSnapshot.status.phase=Failed`，resume 失败 |
| Fork target build Sandbox 无法 Running | artifact 构建失败，按退避策略重试 |
| SnapshotBuildTask 超时 | artifact 构建失败，按退避策略重试 |
| driver readiness handshake 不支持或失败 | artifact 构建失败，上报 driver 特有原因 |
| driver 创建 artifact 失败 | 当前节点 artifact Failed；其他 Ready artifact 仍可服务 |
| 部分节点失败 | 至少一个 artifact Ready 时 `Phase=Ready`；节点计数字段展示覆盖不完整；发出 `SandboxSnapshotDegraded` Event 供观测 |
| artifact store 不可用 | session 使用普通冷启动路径，直到 artifact store 可用且校验成功 |
| 注入 restore intent 后 restore 失败 | runtime 按自身实现处理失败；通用控制面不记录 restore 结果 |
| Controller 检测到 artifact 不可用 | 移除 artifact 记录并重建 |

---

## 13. 可观测性

### 13.1 Kubernetes Events

| Event | 含义 |
|---|---|
| `SandboxSnapshotCreating` | 开始创建启动基线 |
| `SandboxSnapshotReady` | 至少一个 artifact 可用 |
| `SandboxSnapshotDegraded` | 部分 target node 构建失败或不可用 |
| `SandboxSnapshotRebuilding` | source 变化或手动触发后开始构建替换 artifact |
| `SandboxSnapshotFailed` | 所有 artifact 构建失败 |

### 13.2 Metrics

建议指标：

```text
agentcube_sandbox_snapshot_build_duration_seconds
agentcube_sandbox_snapshot_artifacts{phase=...}
agentcube_sandbox_snapshot_target_node_count
agentcube_sandbox_snapshot_ready_node_count
agentcube_snapshot_build_task_total{phase=...}
agentcube_snapshot_restore_intent_total{mode=...,result=cold_start|restore_intent}
agentcube_sandbox_snapshot_rebuild_total{reason=...}
agentcube_snapshot_artifact_gc_total{driver=...,reason=...}
```

---

## 14. 安全边界

1. snapshot-build artifact 只包含 runtime bootstrap 状态。用户状态、活动任务和 session
   凭证保留在可复用构建 artifact 之外。Phase 1 依赖 SnapshotDriver readiness handshake，
   以及用户请求到达 runtime 前的构建时机。
2. session 特有凭证通过 session 路径注入，而不是写入 Fork mode 的可复用 artifact。
3. runtime 特有 session context 由业务 runtime 集成层处理，并保留在通用 snapshot artifact
   之外。
4. Node Agent 仅处理 `spec.targetNodeName=<本节点>` 的 `SnapshotBuildTask`。
5. Node Agent 回写 `SnapshotBuildTask.status` 前必须校验：
   - `SandboxSnapshot UID`；
   - artifact key；
   - build hash；
   - driver；
   - target node。
6. Node Agent 不拥有修改 `SandboxSnapshot.status` 的 RBAC 权限。
7. 本地 artifact 删除必须校验 owner UID，避免删除其他控制面实例或手工创建的资源。

---

## 15. 与当前代码的差异

当前仓库中的实现属于旧版原型，迁移时需要逐步替换：

| 当前实现 | 目标设计 |
|---|---|
| `SnapStart` CRD | 带 `snapshotMode` 的 `SandboxSnapshot` CRD |
| `runtimeRef → CodeInterpreter / AgentRuntime` | `sourceRef → SandboxTemplate` |
| SnapshotController 解析 CodeInterpreter | 业务 Runtime Controller 自动生成模板；SandboxSnapshotController 只读模板 |
| SnapshotController 直连 `agentd` Kuasar Proxy | Controller 创建 `SnapshotBuildTask`；Node Agent watch 并 reconcile |
| 控制面理解 `templateID`、lease、`kuasar.io/*` | Node Agent 内部 SnapshotDriver 理解私有协议 |
| `SnapStart.status.activeMode` | `SandboxSnapshot.status.phase` 和节点计数字段 |
| Redis `SnapshotInfo` Kuasar 字段 | 通用 `SnapshotArtifactManifest`、`SnapshotArtifactSet` 和 `SnapshotArtifact` |
| Workload Manager 写 `kuasar.io/*` | Workload Manager 写 AgentCube 标准 restore intent |
| Controller 使用 build Sandbox annotation 作为节点任务状态 | 内部 `SnapshotBuildTask.status` |
| Controller 同步清理节点本地或远端快照 artifact | artifact 记录立即移除 + provider 特定的最终一致清理 |
| CodeInterpreter 仅在 `warmPoolSize > 0` 时创建 `SandboxTemplate` | Runtime Controller 始终 reconcile 稳定的 `SandboxTemplate`；`warmPoolSize` 只控制 `SandboxWarmPool` |

本次设计文档更新不修改代码。

---

## 16. 分阶段落地计划

### Phase 1：抽象重构与 Kuasar 接入

1. 新增通用 `SandboxSnapshot` 和 `SnapshotClass` CRD；第一阶段先实现 `Fork` mode。
2. CodeInterpreterController 始终 reconcile 标准 `SandboxTemplate`。
3. 实现 `SandboxSnapshotController`。
4. 新增内部 `SnapshotBuildTask` CRD 和 Controller reconcile。
5. 将 `agentd` 改造为 watch 本节点 `SnapshotBuildTask`。
6. 在 Node Agent 内实现 Kuasar `SnapshotDriver`。
7. 将 artifact store 泛化为 `SnapshotArtifactManifest`、`SnapshotArtifactSet` 和 `SnapshotArtifact`。
8. Workload Manager 改用 AgentCube 标准 restore intent。
9. 实现 Node Agent 本地 artifact GC。

### Phase 2：通用 AgentRuntime 与 Browser Agent

1. AgentRuntimeController 自动生成 `SandboxTemplate`。
2. BrowserRuntimeController 自动生成 `SandboxTemplate`。
3. 验证每种 driver 的 readiness handshake 和 restore 注入契约。
4. 增加新 runtime 时不修改 SandboxSnapshotController。

### Phase 3：WarmPool 组合

```text
SandboxWarmPool 补充槽位
→ 新 Sandbox 从 SandboxSnapshot 恢复
→ SandboxClaim 分配已恢复槽位
```

### Phase 4：Resume Mode

实现 `Resume` mode，支持从已有 session Sandbox 创建会话状态快照。复用同一个
`SandboxSnapshot` CRD、Controller、SnapshotDriver、artifact store 和 restore intent。
restore 完成是数据面结果；GC 属于系统级 artifact 清理职责。

---

## 17. 开放问题

1. `SnapshotClass` 是否需要独立 capability status，还是第一阶段仅依赖 Node label？
2. AgentCube 标准 session restore annotation 是否由 agent-sandbox 上游接受，还是先由兼容层转换？
3. artifact store 长期保留 Redis / Valkey，还是演进为专用 CRD 或持久化服务？
4. Node Agent 查询 Snapshot owner UID 的 GC 权限边界如何最小化？
