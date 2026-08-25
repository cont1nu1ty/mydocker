# Sandbox 与 Container Attempt 生命周期

## 状态

**M3 Verified（精简 provider 范围）。** M1 已验证纯领域对象/FSM、一对一与单活跃 Attempt 不变量、结构化
进程参数、强身份验证端口、operation 幂等性、event、rollback、事务状态边界和两阶段
生命周期协调器。M3 已实现版本化 UDS API、持久 engine/provider 编排、daemon recovery、
`network=none/loopback`、hostname/DNS、共享 prepared-rootfs catalog 及 cold/warm 评测工具；
非特权完整控制面测试已覆盖 API 到重启恢复，2026-08-25 的
[rootful 套件](../../integration/rootful/README.md)又通过真实 namespace/cgroup/process、
daemon reopen、signal、exit 与 OOM 生命周期。该状态不覆盖 M4+ per-Attempt snapshot/
完整网络，也不覆盖 M5 长期可靠性或 hostile-workload 安全。

## 目的

提供可靠的本地 workload 生命周期，使 Sandbox 具有稳定身份，并能承载一系列具体的
Container Attempts。该契约在 retry、部分失败、进程退出、daemon restart 和 cleanup
期间都必须保持正确。

跨领域的所有权与状态规则以 [architecture.md](../architecture.md) 为准。度量定义以
[evaluation/README.md](../../evaluation/README.md) 为准。

## 范围

本功能涵盖：

- Sandbox 的 create、inspect、stop 和 remove；
- Container Attempt 的 create、start、inspect、kill 和 delete；
- `run` 作为便捷组合，而非独立的状态机；
- exit code、signal、OOM、graceful stop、retry、rollback 和 recovery；
- 初始的 one-active-Attempt 约束，以及之后连续执行的 Attempts。

目前不涵盖 concurrent sidecars、init containers、共享 PID namespace、CRI、kubelet
或完整的 Pod 生命周期语义。

## 核心对象与流程

### 资源所有权

当前 M3 中，Sandbox 拥有稳定身份、UTS/IPC/network namespaces、hostname/DNS 设置、
父 cgroup、labels，以及 keeper 身份；M4C 才增加 network attachment、地址和 port
mappings。Container Attempt 拥有其进程、mount/PID namespaces、子 cgroup、输出、
退出码、退出信号和 OOM 结果。在文件系统维度，M3 Attempt 只拥有由配置过的共享
prepared-rootfs source 建立的 mount 视图及其 owner receipt；它不拥有或删除该共享
source，也没有每 Attempt 独享的 snapshot。Snapshot、独享 writable layer 和版本化
bundle 是 M4B 的未来能力。

因此，Sandbox 是一个保留的 workload environment，而不是空包装器。当一个 Attempt
停止、后续 Attempt 被创建时，其网络身份和共享 namespaces 仍然存在。

初期，一个面向 API 的 `Container` aggregate 恰好对应一个面向 kernel 的
`Container Attempt`。Container 保留 immutable spec、query/log identity 和 atomic
status projection；Attempt 拥有 process、mount 视图、namespaces/cgroup，并且是输出及
exit/OOM outcome 的规范权威。`CreateContainer` 会返回两个 ID。后续 workload retry
会在同一 Sandbox 中创建新的 Container/Attempt pair，而不会修改 terminal record。

### 状态机

Sandbox 的规范 phases：

```text
absent -> creating -> ready -> stopping -> stopped -> absent
```

Container Attempt 的规范 phases：

```text
absent -> creating -> created -> running -> stopped -> absent
```

失败（Failure）是当前 phase 和 operation 上的 condition，而不是另一个 phase。它需要执行
rollback 或 reconciliation，且不得被静默转换为 `absent`、`ready` 或 `running`。
完整 transition rules 和图示见
[architecture.md](../architecture.md#5-生命周期状态机)。

### 一个 active Attempt

处于 `creating`、`created` 或 `running` 的 Attempt 为 active Attempt。初期，一个
Sandbox 最多只能有一个 active Attempt。daemon 会串行处理同一 Sandbox 上相互冲突的
operations；在前一个 Attempt 达到 terminal state 且释放必要资源前，拒绝新的 Attempt。

当后续 pair 复用同一个 Ready Sandbox 时，已停止的 Container/Attempt history 可以继续
供查询。两个 ID 和结果均不可变；Sandbox 的 current Container/Attempt references
以原子方式推进。移除 Sandbox 前，必须显式删除这些 Container records；需要保留更久的
审计历史则由 operation/event retention policy 管理。

### 操作

`CreateSandbox` 校验 spec、持久化 intent、创建父 cgroup 和由 keeper 持有的
UTS/IPC/network namespaces、配置 hostname/DNS/network、持久化 observed state，
并且只有在验证后才报告 `ready`。

`StopSandbox` 要求不存在 active Attempt（`creating`、`created` 或 `running`），
使 attachments 和 keeper 停止活动，并记录 `stopped`。keeper 继续存活只为在 Remove
之前保留其拥有的 namespaces；stopped Sandbox 不接受新的 Attempt。初始 API 不执行
隐式 cascade。

`RemoveSandbox` 要求 Sandbox 为 `stopped` 且 Container records 数量为零；只有验证
host 上已经不存在相关资源后才执行移除。资源已不存在时重复执行 removal 会成功。

`CreateContainer` 要求 Sandbox 为 Ready 且不存在 active Attempt。它会创建一一对应的
Container/Attempt records，将 Sandbox 的 resource limits 解析进 Container spec，解析
daemon 配置的 prepared-rootfs ID，并准备 Attempt-owned mount 视图/receipt、子 cgroup、
PID namespace、init process 和 start gate；只有 `created` 状态完成持久化后才返回两个
ID。它不会执行 workload，也不会创建 M4B 的 image-derived snapshot 或 bundle。

`StartContainer` 释放 start gate，并确认状态已转为 `running`。收到请求响应不足以作为
证据；daemon 需要来自 process/supervisor 的确认。

`GetContainer`（即 `state` operation）返回 Container 和 Attempt IDs、immutable spec
generation、observed generation、phase、经过验证的 process identity、outcome、
stream references 和 conditions，且不修改状态。`ListContainers` 列出同样的
aggregates；log/event records 始终携带两个 ID。

`KillContainer` 通过 supervisor 或 strong process handle 发送经过校验的信号。
Graceful stop 会发送已配置的 termination signal，等待声明的 grace period；如果进程
仍在运行，再升级为 `SIGKILL`。该操作报告真实结果。

`DeleteContainer` 要求状态为 `created` 或 `stopped`；它会按依赖顺序拆除 Attempt
拥有的资源，然后移除一一对应的 Container/Attempt metadata。此前的 delete 失败会保留
`stopped` 状态并附带 cleanup condition；使用相同 operation ID 的 retry 或 background
reconciler 会继续执行 teardown。`GetOperation` 只报告该进度，相互冲突的 operations
仍然被阻塞。M1 协调器返回 operation 级的 stage/result；逐项 rollback progress 保存在
Store 的 OperationRecord 中。M3 的 v1 API 已投影稳定的 operation identity、target、
stage/result/reason 和终态，但有意不公开 provider receipt 或逐项 rollback descriptor。
验证资源不存在后重复执行 deletion 会成功。

`run` 是客户端或 daemon 中的编排，其等价过程为：

```text
CreateSandbox (when needed)
-> CreateContainer
-> StartContainer
-> optionally attach/wait
```

它不会绕过 operation IDs、persistence 或各个 lifecycle rules。

## 关键设计

### 结果模型

workload 退出时，supervisor 会持久化规范的 Attempt outcome，`ContainerStatus` 再以
原子方式投影该 outcome：

- 正常退出时记录 exit code；
- 因信号退出时记录 terminating signal；
- OOM status 基于 cgroup v2 evidence，而不是根据一个 exit code 推断；
- 带 clock semantics 的 start 和 finish facts；
- 最后一个 operation/event sequence 和 reason class。

实际进入过 `running` 的 workload 只有捕获 outcome 后才会变为 `stopped`；未知 outcome
是显式 condition，不得伪造成成功。启动前删除的 Attempt 使用
`not_applicable` 表示从未运行，不要求虚构 exit evidence；后续 metadata 删除仍要求
外部确认其资源已经不存在。

### 幂等性与重试

| 请求情形 | 契约 |
| --- | --- |
| operation ID 相同，operation 已完成且仍在完整 replay window | 返回已存储的结果 |
| operation ID 相同，但已离开完整 replay window | 返回 typed `operation_expired`；禁止创建新的 intent |
| operation ID 相同，operation 未完成 | 继续/协调同一 operation |
| operation ID 相同，请求 fingerprint 不同 | 拒绝误用 idempotency key |
| operation ID 不同，目标状态已经达到 | 返回当前状态，不产生重复副作用 |
| 新的冲突 operation | 串行处理，或返回 typed conflict 并拒绝 |
| Delete 的目标已经不存在 | 验证不存在后成功 |
| Kill 的目标已经停止 | 返回 terminal outcome；不向已复用的 PID 发送信号 |

客户端在首次发送前生成 operation ID。服务端在定义明确的 retention window 内持久化其
operation type、target、canonical request fingerprint、stage 和 result。Transport
retry——包括第一次响应丢失后的 retry——会复用该 ID，不会被视为新的 lifecycle intent。
`GetOperation` 报告该 record；内容相同的 retry 或 background reconciler 会在它未完成时
继续执行。Lifecycle operations 不会增加 immutable spec generation。

### 删除顺序

Attempt teardown 按以下依赖执行：

```text
stop/confirm process
-> capture outcome and close streams
-> detach child cgroup
-> unmount Attempt-owned mounts and prepared-rootfs view
-> remove child cgroup
-> persist absence and remove Container/Attempt metadata
```

M4B 实现后，删除 versioned bundle 和释放 per-Attempt snapshot/writable layer 才会插入
上述依赖顺序；当前 M3 绝不把共享 prepared-rootfs source 当作 Attempt 数据删除。

Sandbox removal 按以下顺序执行：

```text
verify Sandbox stopped and zero Container records
-> stop namespace keeper
-> release UTS/IPC/network namespaces
-> remove parent cgroup
-> persist absence and remove Sandbox metadata
```

M4C 实现后，移除 owned port rules/routes/veth 并释放 IP allocation 的步骤必须发生在
停止 namespace keeper 之前；这些资源在当前 M3 精简网络中并不存在。

具体实现可以细化这些步骤，但在保留下来的 intent 尚不足以完成 recovery 前，绝不能
删除 metadata。

## 故障与恢复

每个 setup step 都在 rollback stack 中注册一个幂等的 inverse。发生失败时，rollback
必须先封存 stack，并在执行第一个 inverse 前将 `started` 进度与 operation 原子持久化；
daemon 恢复已封存的 stack 后不得再登记新的 inverse。随后 rollback 按逆序运行，并同时
记录 primary error 和每个 rollback error。成功的 cleanup 必须经过
验证；如果仍有 host resource 遗留，则当前 phase 应保留 failure/cleanup condition，
而不是错误地报告 terminal 或 absent phase。

daemon restart 后，当前 reconciliation 会将 durable intent 与 shim/process identity、
namespaces、cgroups、Attempt mount view/receipt 和 M3 精简网络状态进行比较。系统根据
最后一个 durable stage 继续 operation、执行 rollback、接管已验证的资源，或标记需要
operator 处理的 condition。删除 orphan 前必须证明其所有权。M4B/M4C 实现后才把
snapshots、network attachments、IPAM 和 port rules 纳入相同恢复原则。

supervisor 会持久化 exit outcome，并保持 namespace/process handles 可供重新连接。
持久化的裸 PID 绝不足以用于向进程发送信号或接管进程。

## 可观测性与评测点

每个 operation 都会产生 operation ID 和 stage events。必需的生命周期指标包括：

- Sandbox cold create latency：从接受 `CreateSandbox` 到经过验证的 `ready`；
- Container create latency：从接受 `CreateContainer` 到 `created`；
- Container start latency：从接受 `StartContainer` 到确认 `running`；
- full cold start：从新 Sandbox 加新 Attempt 到 workload 进入 `running`；
- warm Attempt restart：从现有 Ready Sandbox 加新 Attempt 到 `running`；
- full lifecycle throughput：从 create Sandbox 到 remove Sandbox；
- 现有 Ready Sandbox 内的 Attempt-only throughput；
- repeated-operation correctness 和 idempotent retry results。

Fault scenarios 覆盖每个 setup/teardown/persistence boundary 上的故障、重复请求、
进程 `SIGKILL`、daemon crash 和响应丢失。Stress scenarios 跟踪 processes、cgroups、
mounts、interfaces、FDs、goroutines、metadata 和 daemon memory。

## 未来集群兼容性

未来的 Task/workload replica 对应一个稳定 Sandbox 和一系列一一对应的
Container/Attempt pairs。Assignment delivery 是 at least once，因此本地 API 必须具备
operation identity、spec generation 和 idempotency。Transport redelivery 会复用同一
Attempt intent；terminal outcome 之后的 workload retry 会创建下一组 pair。集群报告
runtime outcome，但不拥有本地 PIDs 或资源。

未来可以用显式的 membership、ordering 和 shared-resource semantics 替换 one-active
规则，以此扩展 Sandbox 来支持 concurrent containers。只有实现这些语义后，未来的 CRI
adapter 才能将外部请求转换为本地 API。当前设计不兼容 CRI。

## 验收条件

只有自动化测试证明以下各项后，本功能才达到 Verified：

- 每一种合法和非法状态转换；
- Container/Attempt 一一对应的身份、one-active 约束和连续历史；
- 结构化 argv/environment 的 fidelity；
- 真实的 exit code、signal、OOM 和 graceful-stop 行为；
- 重复请求下 create/start/kill/delete 的幂等性；
- 每个注入的 setup failure 都会触发逆序 rollback；
- 完整的删除顺序，且不留下可验证归属的残余资源；
- 每个 durable stage 上的 daemon restart reconciliation；
- 由可复现 harness 记录的 cold/warm 指标边界。

## 未决问题

- M3 已选择分离的 Sandbox keeper 与长期 init wrapper；M5 是否扩展为可无缝重连的
  per-Sandbox supervisor，以及重连失败时的最终 orphan policy，仍待真实故障矩阵决定。
- 当前 FileStore schema v3 提供原子 snapshot、event ordering、旧事件计时迁移、最近 `1024` 个终态
  operation 的精确响应、最多 `65536` 个 identity/tombstone 及最近 `8192` 个 event；
  达到 identity/envelope 上限后的在线 rollover、归档和运维迁移仍未实现。
- Kill API/CLI 要求显式 signal、grace period 和 escalation signal，不选择隐式默认值；
  未来高级策略是否由更上层控制面提供仍待决定。
- 历史 Container/Attempt 记录与 workload log 的长期 retention、删除、反压和导出策略。
