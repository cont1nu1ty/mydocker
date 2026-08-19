# Daemon、恢复、可观测性与本地 API

## 状态

**Proposed。** M0 预留契约和边界；当前尚无 daemon、API、shim、状态存储、
指标或链路追踪实现。

## 目的

让 `mydockerd` 成为生命周期、持久状态、恢复和可观测操作的长驻单节点权威。
提供带版本的本地 API，在不暴露运行时内部实现的前提下，既服务当前 CLI，也服务
未来的节点 agent。

## 范围

- CLI 作为通过 Unix domain socket（UDS）访问服务的客户端；
- 带版本的 Sandbox、Container、Image、log、stats 和 event API；
- 请求/操作身份、generation、event 排序和类型化结果；
- 结构化日志、低基数指标和预留的链路追踪上下文；
- 回滚、setup/teardown 对称性以及 daemon 重启后的调谐；
- 每个 Sandbox 独享的 shim/supervisor 或 keeper、可靠的进程身份、退出状态持久化，
  以及对归本系统所有的孤儿资源进行清理。

远程多租户 API、身份认证、scheduler/control-plane 服务、CRI、具体传输协议
技术和生产 SLO 不在 M0 范围内。

## 核心对象与流程

### 权威边界

CLI 校验表示层输入并调用 `mydockerd`。它不会 fork 分离运行的工作负载，不会持有
等待该工作负载的 goroutine，不会编辑状态文件，也不会操作主机
namespace/cgroup/网络。

`mydockerd` 监听 `/run/mydocker` 下受权限控制的 UDS，校验 API 版本、请求
以及客户端生成的 operation ID，串行化资源操作、持久化意图、调用 engine 服务，
并返回类型化状态/错误。

engine 协调：

- `SandboxService`：管理稳定环境的生命周期；
- `ContainerService`：管理初始的一对一 Container/Attempt 生命周期；
- `ImageService`：管理解析、导入和内容状态；
- 存储/网络/运行时接口；
- state、operation/event、log、stats 和 metric 的存储/数据流。

### 计划中的本地 API

Sandbox 服务：

```text
CreateSandbox
StopSandbox
RemoveSandbox
GetSandbox
ListSandboxes
```

Container 服务：

```text
CreateContainer
StartContainer
KillContainer
DeleteContainer
GetContainer
ListContainers
```

流式传输/观测：

```text
StreamLogs
StreamStats
WatchEvents
GetOperation
```

Image 操作预留在 `ImageService` 下，将在存储里程碑中具体定义。`run`
仍然由多个生命周期调用组合而成。

API 从首次实现起即带版本。版本化不仅针对路径字符串，还覆盖字段含义、state enum、
重试/幂等行为、error code 和流式恢复语义。

初期，`CreateContainer` 创建一个 API/持久化 Container 聚合体和恰好一个
面向内核的 Attempt，并返回两者的 ID。Container 查询、列表项、log、stats
和 event 均暴露这两个身份。工作负载在终态之后重试时会发起新的
`CreateContainer`；同一创建请求的传输重试则复用原始 operation ID，
并返回或恢复同一对资源。

### 身份与收敛

- `request_id` 关联一次传输尝试。
- 客户端在首次发送前创建 `operation_id`；即使响应丢失或发生任意次数重试，它仍标识
  同一个持久化生命周期意图。
- 服务端将 operation ID 的作用域限定在其 API 权威范围内，并将其绑定到 operation
  type、target 和 canonical request fingerprint；不匹配时返回类型化错误。
- 初始 API 的 create 为不可变 spec 分配 `generation = 1`。
- 只有在该 create spec 完成 reconciliation 后，`observed_generation` 才推进到 `1`。
- 生命周期 phase 变化不会递增 spec generation；未来显式的 update API 必须使用
  expected-generation 前置条件。
- `event_sequence` 对持久化排序作用域内的 event 进行排序。
- tracing 实现后，trace context 在 CLI/agent、daemon、engine 和 supervisor 之间传播。

一个操作可以发出多个 stage event，其中包含 operation type、stage、result、
有界 reason class、wall-clock timestamp，以及在有效情况下使用的 monotonic duration。

### Supervisor 与进程身份

每个 Sandbox 独享的 shim/supervisor（或由其控制的 keeper）保留共享 namespace，
监管活跃的 Attempt，捕获退出状态，并支持 daemon 重连。具体的进程拓扑推迟决定，
但不能仅依赖 daemon 或用户进程持续存活。

在可用时使用 pidfd；否则使用包含进程启动信息和所有权、且经过同等强度验证的身份。
仅凭持久化的整数 PID，不能授权发送 signal、加入 namespace、接管或删除。

## 关键设计

- 在产生无法安全重新发现的副作用之前，先持久化状态转换意图和操作身份。
- setup 与 teardown 共用显式资源步骤和幂等逆操作。
- 单一回滚栈记录资源获取顺序；回滚按逆序执行。
- 错误按 operation/stage 和有界 reason class 类型化；详细 cause chain 保留在
  log/event 中。
- 生命周期 API 定义重试语义；传输重试绝不会静默重复外部副作用。
- operation record 保留 fingerprint、stage 和 result，其保留时间足以覆盖声明的
  客户端重试行为。`GetOperation` 读取该记录；相同的重试或 reconciler 可以恢复
  未完成的操作。
- 状态记录使用 schema 版本和原子更新语义。
- event stream 在实现时定义排序作用域、resume token、gap 行为和保留策略。
- log 持久化足够的 Attempt 身份和 stream position，以支持 daemon 重连。
- 只有在证明本地所有权并检查持久化意图后，才能执行孤儿资源清理。

## 故障与恢复

启动时，`mydockerd` 在任何修改前先执行只读发现阶段。它加载持久化的 Sandbox、
Attempt 和进行中的 operation record；重新连接 supervisor；检查进程身份、
namespace、cgroup、mount、snapshot、link、IPAM 和运行时状态；然后创建调谐计划。

对于每个资源，它可以：

- 确认期望状态/实际状态一致，并推进 `observed_generation`；
- 恢复一个幂等的 setup/teardown 阶段；
- 回滚未完成的创建；
- 接管身份得到强验证的运行中资源；
- 从 supervisor 捕获此前未持久化的退出结果；
- 附加需要运维人员安全处置的失败/未知清理 condition。

系统不会把证据缺失当作成功。通过原子存储/schema 校验检测部分写入的元数据。
响应丢失通过 operation lookup 和幂等重试处理。回滚失败保持可见，并由调谐流程重试。

supervisor 崩溃、daemon 崩溃和主机重启具有不同的恢复预期。重启可能销毁 `/run`
和所有进程，但持久状态仍然存在；daemon 必须报告已确认的终态结果，或附加
显式的 `lost` condition，并按策略清理归本系统所有的持久资源，而不能声称
工作负载仍在运行。

## 可观测性与评测点

### 结构化日志

日志可以包含 request、operation、Sandbox、Container/Attempt、trace/span、stage、
generation、有界 reason class 和详细 error 字段。具体 ID 和诊断上下文应记录在此。

### 指标（Proposed）

在实现之前，名称保持 **Proposed** 状态：

```text
sandbox_create_duration_seconds
container_create_duration_seconds
container_start_duration_seconds
lifecycle_operations_total
lifecycle_failures_total
container_running
container_exits_total
container_oom_total
rollback_total
rollback_failures_total
```

label 仅限 operation、stage、result、signal class 和 reason class 等有界值。禁止将
Sandbox ID、Container/Attempt ID、Task ID、operation ID、image digest 和完整错误
字符串用作 label。详细身份应记录在 log/trace 中。

指标提供聚合趋势；由于抓取时机和聚合会改变语义，它们不是精确基准测试样本的唯一
来源。评测工具使用自己的单调时钟测量调用方可见的 API 延迟。Proposed 耗时指标使用
daemon 时钟测量 daemon operation/stage 时间；两类数据分别报告。

### 必测场景

- 重复请求和响应丢失时的幂等性；
- 每个 persistence/setup/rollback 阶段发生故障；
- 每个生命周期阶段中 daemon 重启；
- supervisor 退出、用户进程收到 `SIGKILL`，以及退出状态持久化；
- 不产生错误所有权判断的孤儿资源检测；
- event sequence/resume 和日志重连行为；
- metric label-cardinality 校验；
- 完整的 cold start 和 warm start 阶段分解。

指标定义和实验规则见
[evaluation/README.md](../../evaluation/README.md)。

## 未来集群兼容性

`mydocker-agent` 通过 UDS 或经过审慎版本化的节点本地传输，使用同一个带版本
的本地 API。它提供稳定的、由客户端生成的 operation ID、规范的 Sandbox
`Resources`、镜像摘要和 trace context；它观测 Sandbox 和
Container/Attempt 的 status/event。cluster generation 只通过显式的带版本字段
转换，不与本地不可变 spec generation 混淆。agent 绝不导入 `internal/runtime`、
向 PID 发送 signal，或直接修改元数据/cgroup。

Assignment 的 at-least-once delivery 依赖本地幂等性。若集群 API 演进需要新的
本地语义，必须先在 `mydocker-2.0` 中完成设计、实现和验证。运行时阶段延迟
与 Assignment 延迟、controller 延迟仍需分别报告。

## 验收条件

仅在满足以下条件时，此功能才达到 **Verified** 状态：

- CLI 可以退出，同时 daemon/supervisor 正确持有分离运行的生命周期；
- API 版本、类型化错误、operation ID 和重试契约具备自动化测试；
- 首次响应丢失重试、请求 fingerprint 不匹配、保留和 operation lookup 具备自动化测试；
- immutable generation/observed-generation 能够持久化并完成 reconciliation，
  且不会因生命周期 operation 而递增；
- event ordering/resume 和结构化日志身份经过测试；
- 重启后的调谐覆盖每个持久化生命周期阶段；
- exit code、signal、OOM 和孤儿资源状态在 daemon 重连后仍然保留；
- 回滚的主故障/次生故障保持可观测且可重试；
- Proposed 指标通过有界 label 审查和正确性测试；
- 公共 API 基准测试样本符合文档规定的 cold/warm 边界。

## 未决问题

- 初始传输/编码（例如 HTTP+JSON 或其他本地协议）。
- 状态存储实现，以及 operation/event 的持久化粒度。
- operation-result 保留时长、存储上限和客户端重试窗口。
- shim/keeper 进程拓扑和重连协议。
- 最低内核版本和 pidfd 回退策略。
- event/log 保留、反压和 stream-resume token。
- UDS 授权和未来的本地多用户策略。
