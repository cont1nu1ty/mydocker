# Daemon、恢复、可观测性与本地 API

## 状态

**In progress。** 当前已有 FileStore、engine、shim 协议、带版本的 HTTP/JSON UDS
API、`pkg/client`、JSON CLI、operation/event/log 查询、启动恢复、终态 watcher、
kill-deadline watcher 和只调用公共 API 的评测工具，并具有纯 Go/注入依赖测试。
FileStore 已实现有界 operation/event retention、原子 compaction 和显式 resume gap。

这仍不是生产可用声明：`LinuxShimLauncher` 在 UDS 绑定前明确以
`ErrLauncherIncomplete` 失败关闭，真实 rootful lifecycle/daemon restart 尚未在一次性
VM 上验收，固定 identity/envelope 上限也尚无在线 rollover。metrics、stats、tracing
和 Image API 仍是后续 Proposed/Planned 能力。

## 目的

让 `mydockerd` 成为生命周期、持久状态、恢复和可观测操作的长驻单节点权威。
提供带版本的本地 API，在不暴露运行时内部实现的前提下，既服务当前 CLI，也服务
未来的节点 agent。

## 范围

- CLI 作为通过 Unix domain socket（UDS）访问服务的客户端；
- 已实现带版本的 Sandbox、Container、operation、分页 log 和 event API；Image、stats
  和真正的流式 watch 属于后续里程碑；
- 请求/操作身份、generation、event 排序和类型化结果；
- 结构化日志、低基数指标和预留的链路追踪上下文；
- 回滚、setup/teardown 对称性以及 daemon 重启后的调谐；
- 每个 Sandbox 独享的 shim/supervisor 或 keeper、可靠的进程身份、退出状态持久化，
  以及对归本系统所有的孤儿资源进行清理。

远程多租户 API、身份认证、scheduler/control-plane 服务、CRI 和生产 SLO 不在 M3
范围内。当前传输已固定为受权限控制的 Unix socket 上的 HTTP/JSON v1。

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
- 后续 `ImageService`：管理镜像引用到 digest 的映射、导入、查询、删除和可用性检查；
- 当前精简 provider，以及后续 content/unpack/snapshot/mount/bundle、network 和 runtime 接口；
- 已实现的 state、operation/event、log 数据流，以及后续 stats/metric 数据流。

### 当前本地 API 与后续扩展

以下 Sandbox/Container 调用、`GetOperation`、分页 `EventsAfter` 和按
Container/Attempt 绑定的 `LogsAfter` 已由 HTTP/JSON UDS v1、公共客户端和 CLI 实现；
生产 launcher 未完成，因此这里只声明传输/服务契约通过纯测试，不声明真实 workload
已经运行。

Sandbox 服务：

```text
CreateSandbox
StopSandbox
DeleteSandbox
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

当前分页观测：

```text
LogsAfter
EventsAfter
GetOperation
```

后续流式观测和 Image 服务（Proposed/Planned）：

```text
StreamLogs
StreamStats
WatchEvents
ImportImage
EnsureImage
GetImage
ListImages
RemoveImage
```

这些操作只消费 OCI image；不会隐式提供 `BuildImage`、`CommitContainer`、
`PushImage`。可选的未来 `PullImage` 必须按 digest 获取，并保持与本地导入相同的
内容验证契约。详细边界见
[image-filesystem.md](image-filesystem.md)。`run` 仍然由多个生命周期调用组合而成。

API 从首次实现起即带版本。版本化不仅针对路径字符串，还覆盖字段含义、state enum、
重试/幂等行为、error code 和流式恢复语义。

初期，`CreateContainer` 创建一个 API/持久化 Container 聚合体和恰好一个
面向内核的 Attempt，并返回两者的 ID。Container 查询、列表项、log、stats
和 event 均暴露这两个身份。工作负载在终态之后重试时会发起新的
`CreateContainer`；同一创建请求的传输重试则复用原始 operation ID，
并在完整 replay window 内返回或恢复同一对资源；窗口外返回 `operation_expired`。

### 身份与收敛

- `request_id` 关联一次传输尝试。
- 客户端在首次发送前创建 `operation_id`；响应丢失后的传输重试继续使用同一 ID。
  完整响应只在声明的 replay window 内可重放；窗口外仍记住该 identity，但不再返回结果。
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

### M3 operation/event 保留与 resume gap

保留策略按确定性的 store-local 顺序和数量工作，不使用 wall-clock TTL。生产默认值是：

| 项目 | 默认上限 | 行为 |
| --- | ---: | --- |
| 完整终态 operation | `1024` | 保留完整 canonical terminal response，供同 ID 精确重放 |
| 全部 operation identity | `65536` | active/full record 与 retired tombstone 的总数；active 永不淘汰 |
| event suffix | `8192` | 始终保留全局 sequence 连续的最新后缀 |
| FileStore envelope | `64 MiB` | 启动前以 fstat 拒绝 oversized 文件，读取再受 LimitReader 约束；commit 编码也受同一上限 |

终态 record 离开完整 replay window 时，FileStore 在同一候选事务中将其转换为固定大小的
tombstone。tombstone 保存 operation ID 的 SHA-256、type、target、request fingerprint、
reason、终态顺序和可用的末次 event sequence，但不保存响应。之后任何相同 ID（即使请求
内容不同）都返回 `operation_expired`（HTTP `410`、CLI exit `4`），不会退化为 not-found、
binding-mismatch 或新 intent；客户端不得自动换新 ID 重做未知是否已经产生效果的操作。

event API 使用 opaque resume token。空 token 是显式 reset，从当前最早保留 event 开始；
非空 token 若位于连续 suffix 之前或超过最新已提交 sequence，返回 `resume_gap`
（HTTP `410`、CLI exit `4`）。客户端可以选择记录数据缺口/无效 future cursor 并以空
token 重新观察，但不能把该错误当作“没有新事件”。

compaction、event base sequence、terminal order、tombstone 与资源状态一起进入同一个
copy-on-write FileStore snapshot，只有原子替换和目录持久化成功后才可见。旧 schema-v1
snapshot 经完整校验后原子迁移到 schema v2。达到 `65536` identity 时，新 intent 在任何
宿主机副作用前返回非 retryable `resource_exhausted`（HTTP `507`、CLI exit `5`）；达到
`64 MiB` 时整个 candidate commit 也原子拒绝。当前没有在线 rollover/归档，所以这些
上限消除了无界增长，却也构成明确的 daemon 生命周期边界。

### Supervisor 与进程身份

目标拓扑由每个 Sandbox 独享的 shim/supervisor（或其控制的 keeper）保留共享
namespace、监管活跃 Attempt、捕获退出状态并支持 daemon 重连。当前已实现 shim 协议、
owner-bound config/artifact、终态读取、deferred-rootfs ACK、namespace reattach 和注入
launcher 测试；生产 process factory 的启动/readiness/crash-rediscovery/作用时控制闭环
与 M5 无缝重连尚未完成。最终进程拓扑不能仅依赖 daemon 或用户进程持续存活。

在可用时使用 pidfd；否则使用包含进程启动信息和所有权、且经过同等强度验证的身份。
仅凭持久化的整数 PID，不能授权发送 signal、加入 namespace、接管或删除。

### M3 工作负载日志持久性与定位

当前 M3 日志文件按 `ContainerID + AttemptID` 绑定帧身份，shim 持有唯一 append
writer 锁。首次创建文件后，writer 在成功返回前同步私有父目录；每个已确认 append
先同步带校验和的 prepared frame，再写入并同步固定 commit marker，只有两个 barrier
均成功才发布 cursor。reader 与 append 通过独立的短 OFD byte-range lock 捕获已提交
文件边界，shim 的 lifetime writer lock 仍保持独占且不阻塞 daemon 读。首个同步失败
只会留下没有 commit marker 的尾部；任一 barrier 失败后 writer 保持提交锁并禁止后续
append，正常 Close 会先截断到 confirmed size 并同步，修复失败则不释放 descriptor。
writer reopen 仅截断并同步不完整最终帧，
完整损坏、身份变化或顺序变化均失败关闭。

daemon 不复开 writer，也不从 API 接受日志路径。`CreateContainer` 成功响应前把精确
Attempt 身份注册到经校验的 Container-create owner key；locator 仅从 daemon 启动时
配置且固定身份的 private runtime root 和 owner token 派生
`owners/<owner-token>/workload.log`。daemon 重启丢失进程内注册时，只从同一 root 下
权限、所有者、单链接和字段均通过校验的 init shim config 恢复该映射，并要求 config
中的所有 artifact path 与内部派生值完全相同。

daemon reader 不获取 shim 的 lifetime writer 锁，也不把 path 或 fd 暴露给 service/API。
它为每次读取重新执行 no-follow 打开、path-to-descriptor identity 校验和固定文件大小
快照扫描，随后关闭描述符；scanner 从头流式验证 cursor、sequence、identity 和 checksum，
只保留 `after` 之后至 page limit 的 payload，并复用至多一个单帧 scratch buffer；writer
内存只保留最后 cursor、分 stream sequence 和 confirmed size，不随完整日志历史增长。
并发 append 的不完整最终帧只在该次只读快照中忽略且不会被 reader 截断，下一次读取
重新打开后可见新的已提交帧。完整 corruption 作为有界
`internal` Log API 错误返回，响应只包含 Container/Attempt、stream、cursor、sequence、
payload 和 checksum。

`DeleteContainer` 在 mutation 前捕获权威 `ContainerID + AttemptID`；成功及其 terminal
replay 仅幂等清除这个精确的进程内 log owner/source 映射，避免已删除 Attempt 持续占用
registry。注册自带单调的进程内 revision，Delete 只清除 mutation 前捕获且仍未变化的
revision，因此并发 terminal replay 不会误删后来复用相同 ID 的新 owner；同一
`ContainerID + AttemptID` 在后续新 create owner 下安全重新注册。

## 关键设计

- 在产生无法安全重新发现的副作用之前，先持久化状态转换意图和操作身份。
- setup 与 teardown 共用显式资源步骤和幂等逆操作。
- 单一回滚栈记录资源获取顺序；回滚按逆序执行。
- 错误按 operation/stage 和有界 reason class 类型化；详细 cause chain 保留在
  log/event 中。
- 生命周期 API 定义重试语义；传输重试绝不会静默重复外部副作用。
- active operation 永不因保留策略淘汰；完整终态 record 按上述窗口保留 fingerprint、
  stage、result 和 response，窗口外转为拒绝 ID 复用的 tombstone。
- 状态记录使用 schema 版本和原子更新语义。
- event stream 已定义全局排序、opaque resume token、连续 suffix 和显式 gap 行为。
- log 持久化足够的 Attempt 身份和 stream position，以支持 daemon 重连。
- 只有在证明本地所有权并检查持久化意图后，才能执行孤儿资源清理。

## 故障与恢复

当前 `mydockerd` 在绑定 UDS 前运行一次启动 recovery：加载持久化 Sandbox、Attempt 和
active operation，先经 provider 观察/计划，再执行可恢复阶段。上线后 terminal watcher
投影自然退出，独立 kill-deadline watcher 只扫描 active Kill 并恢复已持久的 escalation
deadline；这不是持续运行的全量 `Reconcile`。这些路径已用注入 provider 测试，尚未在
真实 namespace、cgroup、mount 或 PID 1 上验收，也不声称已实现 M5 supervisor 无缝重连。

目标恢复流程仍要求先只读检查进程身份、namespace、cgroup、mount、snapshot、link、
IPAM 和 runtime 状态，再创建调谐计划。

对于每个资源，它可以：

- 确认期望状态/实际状态一致，并推进 `observed_generation`；
- 恢复一个幂等的 setup/teardown 阶段；
- 回滚未完成的创建；
- 接管身份得到强验证的运行中资源；
- 从 supervisor 捕获此前未持久化的退出结果；
- 附加需要运维人员安全处置的失败/未知清理 condition。

系统不会把证据缺失当作成功。通过原子存储/schema 校验检测部分写入的元数据。
响应丢失通过 operation lookup 和幂等重试处理。回滚失败保持可见，并由调谐流程重试。

supervisor 崩溃、daemon 崩溃和主机重启具有不同的目标恢复预期。重启可能销毁 `/run`
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
`Resources`、镜像摘要和 trace context；在创建本地资源前调用 `EnsureImage`，并观测
Sandbox 和 Container/Attempt 的 status/event。cluster generation 只通过显式的带版本字段
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

- 达到 operation identity/envelope 硬上限后的在线 rollover、归档、导出/导入和运维流程。
- 生产 `LinuxShimLauncher` 的 keeper/init 进程拓扑、namespace join 和重连协议。
- 最低内核版本和 pidfd 回退策略。
- workload log 的长期保留/删除、反压，以及后续 stream 协议；当前只有有界分页读取。
- stats、低基数 metrics 和 tracing 的具体实现与版本化 API。
- M4 Image API 与 content/snapshot 生命周期。
- UDS 授权和未来的本地多用户策略。
