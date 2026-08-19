# mydocker 2.0 架构

**状态：** Proposed。M0 仅定义边界；尚未实现任何 2.0 运行时行为。

本文档是跨功能架构的主要依据。生命周期、隔离、存储/网络、守护进程/API
以及面向集群的详细行为，分别记录在 [`features/`](features/) 下的对应文档中。

## 1. 目标与非目标

### 目标

mydocker 2.0 要回答的是：一个 Linux 节点如何以正确、可恢复、可诊断且可度量的
方式创建并管理容器化工作负载。它将：

- 将稳定的 Sandbox 与每一次具体的 Container Attempt 分离；
- 明确生命周期转换和所有权；
- 使用守护进程而非短生命周期 CLI 作为生命周期权威；
- 使用 cgroup v2 和父子资源层级；
- 持久化带版本的状态，并在重启后将其与宿主机实际状态进行协调；
- 让部分失败的回滚与重试行为可测试；
- 暴露版本化本地 API，供未来的节点代理调用；
- 先建立正确性和可靠性，再度量并优化性能。

之所以需要完全重写，是因为旧版的状态、PID 所有权、命令编码、cgroup v1
假设以及清理行为都与单体 CLI 紧密耦合。在验证新生命周期模型之前保持这些格式
兼容，会反过来限制新模型。Legacy 版本仍可通过 `v0.1.0-legacy` 定位；选择性复用
必须建立新的边界并配套测试。

### 非目标

2.0 初期不提供：

- 调度、放置、心跳、租约、etcd、集群控制器或节点代理；
- 多节点 overlay 网络或集群 API 服务端；
- 并发边车容器、init container 或完整的 Pod 语义；
- Kubernetes CRI、kubelet 集成或兼容性声明；
- 完整的 OCI 或 containerd 协议兼容性；
- rootless 运行；
- 在所有权和可达性规则设计完成前进行垃圾回收；
- 在可复现基线定位出真实瓶颈前进行优化。

未来的集群项目回答另一个独立问题：期望的工作负载如何跨多个节点放置、协调和
恢复。它消费本地 API，而不会把运行时内部实现扩散到控制平面。

## 2. 组件模型

```mermaid
flowchart TD
    CLI[mydocker CLI] -->|UDS、版本化本地 API| Daemon[mydockerd]
    Daemon --> Engine[engine 编排]
    Engine --> SandboxSvc[Sandbox 服务]
    Engine --> ContainerSvc[Container 服务]
    Engine --> ImageSvc[镜像 / snapshot 协调]
    Engine --> NetworkSvc[Sandbox 网络协调]
    Engine --> Store[版本化状态存储]
    Engine --> Observe[事件 / 日志 / 指标]
    SandboxSvc --> Runtime[mydocker-runtime]
    ContainerSvc --> Runtime
    Runtime --> Process[进程 / namespace / mount]
    Runtime --> Cgroup[cgroup v2]
    Runtime --> Rootfs[rootfs / bundle]
    Daemon --> Shim[每个 Sandbox 一个 shim 或 supervisor]
    Shim --> Keeper[namespace keeper]
    Shim --> Attempt[Container Attempt]
    Agent[未来的 mydocker-agent] -.->|同一版本化 API| Daemon
```

依赖通过窄接口指向内层。宿主机相关实现可以依赖 Linux 原语；状态和 engine 包
依赖接口，而不依赖评测工具或未来的集群类型。

## 3. 核心领域模型

以下是语义定义，而不是 Go 声明。

| 对象 | 含义 |
| --- | --- |
| `Sandbox` | 稳定的工作负载身份，以及共享环境资源的所有者 |
| `SandboxSpec` | 初期不可变的 hostname、DNS、labels、网络及规范工作负载资源 |
| `SandboxStatus` | 观测到的生命周期状态、网络/cgroup 挂接、keeper 身份及 conditions |
| `Container` | Sandbox 下单次执行请求的 API/持久化聚合对象 |
| `Container Attempt` | 面向内核的执行子记录，包含进程和执行资源 |
| `ContainerSpec` | bundle、argv、环境、mounts、rootfs/image，以及解析后强制限制的副本 |
| `ContainerStatus` | 聚合 phase，以及对其规范 Attempt 结果的原子投影 |
| `Resources` | Sandbox spec 中的值，分别包含 requests、limits 和进程限制 |
| `Generation` | 权威组件接受的单调递增期望 spec 修订号 |
| `ObservedGeneration` | 其影响已被协调并报告的最高 generation |
| `OperationID` | 客户端生成的身份，用于标识跨重试和多个 stage 的一次外部生命周期意图 |
| `Event` | 关于某次 operation 或资源转换的有序事实，包含结果和原因类别 |

ID 只用于标识记录；它们不能证明某个宿主机进程或内核资源仍属于该记录。进程
身份必须包含更强的证据，例如 pidfd、进程启动身份，或 supervisor 持有的句柄。

首版 API 保持 Sandbox 和 Container 规格不可变：create 将 `generation = 1`，且仅在
该规格协调完成后，`observed_generation` 才变为 `1`。Start、kill、stop 和 delete
会改变生命周期 phase，但不会增加 spec generation。未来显式的 update API 必须使用
expected-generation 前置条件并递增 generation；在此之前不存在隐式变更或乱序 spec
更新。集群 Task/Assignment generation 属于独立的版本域。Generation 永远不能替代
生命周期 phase 或 event sequence。

## 4. Sandbox 与 Container Attempt 的边界

### 所有权

在初始 API 中，每个用户可见的 `Container` 恰好有一个 `Container Attempt`。
Container 记录拥有不可变的请求/spec 以及查询/日志身份；其 Attempt 拥有内核执行
及执行结果。`CreateContainer` 同时创建二者并返回两个 ID。`GetContainer`、
`ListContainers`、日志和事件都显式暴露 Attempt ID。终态之后的工作负载重试，
会在同一 Sandbox 下创建新的 Container 和 Attempt，而不会修改旧记录。这个一对一
规则为未来一个稳定 Container 对应多个 Attempts 留出空间，同时不会让当前尚不支持
的基数关系产生歧义。

| Sandbox 拥有 | Container Attempt 拥有 |
| --- | --- |
| 稳定 ID 和生命周期 | Attempt ID 和生命周期 |
| UTS、IPC 和 network namespaces | 默认拥有 PID 和 mount namespaces |
| Hostname 和 DNS 配置 | 用户进程及结构化 argv/environment |
| 网络挂接、IP、路由、端口 | 受 OCI 启发的 bundle 和 rootfs/snapshot |
| 父 cgroup 和规范 `Resources` | 子 cgroup 和解析后的强制限制 |
| Labels 和 keeper/supervisor 身份 | 规范 stdout/stderr、退出码、signal、OOM 结果 |

Sandbox 不是空壳。它在顺序执行的多个 Attempts 之间保持工作负载身份和网络可达性，
提供 namespace keeper 边界，并在父 cgroup 下汇总资源计量。这样可以自然映射到未来
集群中的 Task 或工作负载副本，而无需让运行时理解调度。

### 初始基数

一个 Ready Sandbox 最多可以有一对活跃的 Container/Attempt（`creating`、
`created` 或 `running`）。终态 Container/Attempt 记录可以继续供查询，同时允许创建
后续组合。在引入多容器共享之前，将活跃执行串行化可使清理、重试、资源
计量和恢复保持可控。

```text
Sandbox S1 (stable network identity)
├── Container C1 / Attempt A1: stopped, OOM
├── Container C2 / Attempt A2: stopped, start failed
└── Container C3 / Attempt A3: running
```

默认情况下，每个 Attempt 拥有自己的 PID namespace，因此替换执行时会获得全新的
PID 1 和进程树。UTS、IPC 和 network namespaces 属于 Sandbox，因为它们描述共享的
工作负载环境。keeper 或 supervisor 必须独立于用户进程持有这些 namespaces。

并发容器、边车容器、init containers 和共享 PID namespaces 需要新的顺序与
失败语义。它们属于未来扩展，初始模型不隐含这些能力，也不能据此声称兼容 CRI。

操作契约见 [lifecycle-sandbox.md](features/lifecycle-sandbox.md)。

## 5. 生命周期状态机

### Sandbox

```mermaid
stateDiagram-v2
    [*] --> creating: CreateSandbox
    creating --> ready: 资源已持久化并验证
    creating --> [*]: 创建失败，回滚已验证
    ready --> stopping: StopSandbox
    stopping --> stopped: keeper 已静止，无活跃 Attempt
    stopped --> [*]: RemoveSandbox
```

规范的外部 phases 为：

```text
absent -> creating -> ready -> stopping -> stopped -> absent
```

失败是某个 phase/operation 上的 condition，而不是额外的生命周期 phase。create
失败会保持 `creating`，并带有失败/清理 condition，直到确认回滚达到 `absent`。
stop 失败会保持 `stopping`，直到重试/协调达到 `stopped`。清理未完成时阻止冲突操作。

`StopSandbox` 要求不存在活跃的 Container/Attempt。`RemoveSandbox` 要求 Sandbox
处于 `stopped`，且每条 Container 记录都已被显式删除；初始 API 不做级联删除。资源
记录被删除后，operation/event 历史可以按自身的保留策略继续存在。Sandbox 的
network/namespace/cgroup 元数据最后删除，且只在确认宿主机上对应资源不存在后删除。

`stopped` 是静止但保留的 Sandbox：它不接受新的 Attempt，而其 keeper 可以仅为持有
既有 namespaces 继续存活，直到执行 `RemoveSandbox`。Remove 会停止 keeper 并销毁
这些保留资源；Stop 不会先销毁它们，再要求 Remove 重复同一工作。

### Container Attempt

```mermaid
stateDiagram-v2
    [*] --> creating: CreateContainer
    creating --> created: bundle、rootfs、cgroup 和 init 已准备
    creating --> stopped: 创建失败，回滚已验证
    created --> running: StartContainer
    created --> stopped: 启动前删除 / 不可重试的启动失败
    running --> stopped: 退出或 KillContainer
    stopped --> [*]: DeleteContainer
```

规范的外部 phases 为：

```text
absent -> creating -> created -> running -> stopped -> absent
```

Start 使用 gate：创建过程准备 init 进程，但不允许工作负载运行；start 释放 gate，
并确认进入 running。报告 `stopped` 前需捕获退出码、signal 和 OOM 证据。

create 失败仅在所有已获取的 Attempt 资源都完成回滚后才进入 `stopped`；其
Container/operation 记录保留失败结果，直至 delete。若 gate/init 仍处于可安全重试
状态，start 失败后保持 `created`，否则清理至 `stopped`。delete 失败会保持
`stopped` 并带有清理 condition；在确认拆除完成前绝不报告 `absent`。

### 转换规则

- 必要时，持久转换要在产生非幂等宿主机副作用前记录意图。
- 每个建立步骤都在回滚栈上注册其逆操作。
- 回滚按逆序执行、记录每个失败，并且可以安全重试。
- 客户端在首次发送前生成 operation ID。使用该 ID 和相同规范请求指纹的重复请求，
  会返回/恢复已存储的结果；用不同请求体复用该 ID 会被拒绝。
- 对已达成状态重复执行 delete/stop 会成功返回，且不会重复产生副作用。
- 同一资源上的冲突操作会被串行化或被显式拒绝。
- 守护进程重启后加载持久意图、观察宿主机资源并协调差异；它绝不只信任元数据。
- 恢复可以继续执行、回滚或附加失败/清理 condition，但不能凭空宣称成功或终态。

## 6. 职责与依赖方向

### CLI

校验用户输入、发送结构化请求、流式传输响应，并将类型化错误映射为退出状态。它不
拥有任何守护进程状态，也不能监督后台工作负载。

### `mydockerd`

拥有本地 API、资源串行化、持久元数据、Sandbox/Container 协调、事件/日志/指标
发布，以及重启后的协调。

### Engine

实现用例顺序、状态机守卫、幂等性、回滚栈，以及 runtime、storage、network 和 state
接口之间的协调。它不会通过 CLI 字符串发出临时拼装的 shell 命令。

### `mydocker-runtime`

理解 bundle、process、namespace、mount、rootfs、cgroup 挂接以及底层生命周期原语。
它不理解 Task、Node、心跳、调度器、租约、assignment 或集群期望状态。

### Shim 或 supervisor

每个 Sandbox 对应一个长期运行实例；按设计持有 namespace 或进程身份，收集 exit
status，并在守护进程重启后保持可重连。namespace keeper 由 shim 自身还是其子进程
担任，是后续里程碑的实现决策。

### 网络

通过幂等操作创建和拆除 Sandbox 范围的 network namespaces、veth/bridge 挂接、
本地 IP 分配、路由和端口映射。

### 存储与 snapshot

按 digest 解析内容，准备不可变的下层 rootfs 和每个 Attempt 的可写 snapshot，完成
挂载，并仅在依赖挂载全部消失后将其拆除。

### 状态存储

提供原子且带版本的记录，以及 operation/event 排序。它不是未经验证 PID 的缓存。
M0 有意不决定持久化技术。

### 可观测性

在生命周期边界发出结构化事件、日志和低基数指标。它自身不定义 benchmark
事实。

### 未来的 agent

监听 assignments、协调期望/实际状态、报告节点/工作负载状态，并调用版本化
本地 API。它不能导入 engine/runtime 内部实现，也不能绕过 `mydockerd` 修改本地文件、
PIDs、cgroups 或 namespaces。

## 7. 状态、事件与度量边界

每个公共生命周期 operation 预留以下上下文：

```text
request_id
operation_id
trace context
resource kind and resource ID
operation type
event sequence
stage
result
reason class
wall-clock timestamp
monotonic duration where available
generation
observed_generation
```

`request_id` 关联一次传输请求。客户端在首次发送前创建 `operation_id`；它标识持久
生命周期意图，即使首次响应丢失及后续发生传输重试也保持不变。服务端在规定的保留
期内，将其绑定到 operation type、target 和规范请求指纹。一次 operation 会按适用
情况发出有序的 stage events，例如 `validate`、`persist_intent`、`prepare_rootfs`、
`attach_cgroup`、`configure_network`、`release_start_gate` 和 `persist_result`。

事件支持恢复、诊断和阶段时长分析，但在实现时必须说明其保留期和持久性
等级。runtime 元数据中的时间戳记录一个事实；它不会自动成为精确的 benchmark
观测值。

两类计时保持分离。调用方可见的 API 延迟，使用同一评测进程的单调时钟，从发送前
一刻计至观察到响应/已确认结果。守护进程的 operation/stage 时长使用该守护进程的
单调时钟，从接受请求计至其收到/确认终态；不得跨进程相减 supervisor 的时间戳。
跨进程或跨节点时，不得把未同步时间戳当作共享同一个时钟而直接相减。追踪可以展示
因果分解，但必须记录时钟限制。

精确的指标边界在 [evaluation/README.md](../evaluation/README.md) 中定义。

## 8. 可观测性与 benchmark 的区别

| 机制 | 主要问题 | 身份策略 |
| --- | --- | --- |
| 指标 | 当前发生了什么聚合趋势或速率？ | 仅使用低基数 labels |
| 结构化日志 | 这个具体资源/operation 发生了什么？ | 允许将 ID 和详细错误作为 fields |
| 追踪 | 时间和失败在组件间如何传播？ | trace/span context 加具体 ID |
| Benchmark 评测工具 | 受控场景在已记录条件下表现如何？ | 在原始结果中记录实验/sample 身份 |

Prometheus labels 不得包含 `sandbox_id`、`container_id`、`task_id`、
`operation_id`、镜像 digest 或完整错误字符串。在词汇表定义后，有界 labels 可以
包含 operation type、stage、result 和 reason class。

一次 scrape 会聚合观测，且可能漏掉短暂事件；它不是精确延迟 sample 的唯一
来源。benchmark 评测工具调用公共 API、定义 sample 边界、记录失败和原始结果，
并固定环境。

## 9. 数据目录

计划的宿主机布局：

```text
/var/lib/mydocker/   durable, restart-relevant state
/run/mydocker/       boot-scoped sockets, locks, pidfds/handles and transient state
```

持久数据可以包括带 schema version 的 Sandbox/Container 记录、operation/event 状态、
内容元数据、snapshots 和持久网络分配。仅运行期的数据可以包括 UDS、进程句柄、
临时 mount 协调和锁。

守护进程必须容忍 `/run/mydocker` 在重启后消失，并根据持久意图和宿主机观测重建它。
选定状态存储后，必须明确原子写入/重命名和目录同步要求。Secrets、日志和大体积内容
在实现前需要明确的所有权与保留规则。

## 10. 目标代码与评测布局

以下是目标布局，并非 M0 创建的骨架：

```text
cmd/
├── mydocker/
├── mydockerd/
├── mydocker-runtime/
└── mydocker-shim/
internal/
├── engine/
├── sandbox/
├── container/
├── runtime/
├── cgroupv2/
├── snapshot/
├── network/
├── state/
└── observability/
api/
└── runtime/v1/
pkg/
└── client/

evaluation/
├── scenarios/
├── workloads/
├── harness/
├── results/
└── profiles/
```

只在有真实实现、测试或实验数据时创建目录。生产代码绝不导入评测工具。评测工具
通过公共 API 做端到端度量；包级 Go benchmark 仅作为微基准。故障注入使用接口
包装器、测试替身或明确仅供测试的构建，
而不是在生产路径中散布随机条件。

## 11. 分支策略

概念上的三阶段模型仍是 Legacy、单节点 2.0、再到集群。实际仓库名称和拓扑才是
权威依据：

| 阶段 | 实际 ref | 规则 |
| --- | --- | --- |
| Legacy 教程版 | `origin/legacy/v1`、`v0.1.0-legacy` | 冻结的参考版本；不进行 2.0 开发 |
| 2.0 runtime/engine | 从现有空 `main` 根创建的 `mydocker-2.0` | 单节点代码、API、可靠性、评测 |
| 集群 | 未来的 `mydocker-cluster` | 仅从已验证的 2.0 tag/commit 派生 |

原始方案把 Legacy 分支称为 `master`，但此仓库没有 `master`。`main` 是用于 2.0
初始化的独立空根，并不是 Legacy commit。M0 不虚构或重命名分支，不合并无关
的根，不重写历史、不推送 refs，也不改变远端默认分支。

创建集群需要稳定的 Sandbox 和 Container 生命周期、版本化本地 API、经过测试的
守护进程恢复、已记录的 runtime 基线，以及明确的 alpha tag 或 commit。runtime 修复
必须先落到 `mydocker-2.0` 并通过验证；仅限集群的控制器/调度器变更留在集群
中。API 变更在 2.0 中设计。

每个集群结果都记录其准确的 mydocker 基础 commit。不同分支、机器、kernel 或场景的
结果，若未经受控实验且未明确列出差异，不得声称可比。

## 12. 关键决策

| ID | 决策 | 影响 |
| --- | --- | --- |
| D1 | 完全重写 | 不以兼容 Legacy 代码/状态/CLI 为目标 |
| D2 | Sandbox 是一等资源 | 网络身份、共享 namespaces 和父 cgroup 可跨顺序 Attempts 保持 |
| D3 | 初期只允许一个活跃 Attempt | 模型成熟过程中，生命周期和恢复保持确定性 |
| D4 | 仅支持 cgroup v2 | 不为 Legacy v1 controller 文件提供兼容层 |
| D5 | CLI 调用 daemon API | 后台生命周期拥有长期运行的权威组件 |
| D6 | 集群使用版本化本地 API | 控制平面无法绕过节点所有权边界 |
| D7 | Requests 与 limits 分离 | 不混淆调度意图与本地强制执行 |
| D8 | 目前不实现 CRI | 借鉴资源模型不意味着协议兼容 |
| D9 | 评测优先，而非优化优先 | 先定义边界、验证行为、建立基线、分析性能剖析数据，再优化 |
| D10 | 测试类别保持分离 | 正确性、benchmark、压力、故障和 profiling 证据不可互换 |

这些决策集中保留在此处，而不拆分成独立 ADR 文件。未来的变更需要同时更新此表，
以及归属该变更的 feature/evaluation 契约。
