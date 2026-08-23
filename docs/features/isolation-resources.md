# 隔离与资源

## 状态

**Implemented, privileged verification pending。** M2 已实现 `internal/isolation`、
`internal/cgroupv2`、`internal/ownership` 和 `internal/provider` 中的隔离原语、
cgroup v2 管理、宿主机资源所有权收据以及 provider 契约，并把分阶段
checkpoint、receipt adoption 和失败后 rollback 接入 M1 的状态/生命周期边界。

上述行为已通过测试替身和临时文件系统验证的纯单元测试、race detector 和
`go vet`。真实 namespace、mount、PID 1/`/proc`、cgroup membership、CPU quota、
memory/OOM 与 pids enforcement 的特权集成验收尚未运行，因此 M2 不得标记为
`Verified` 或 `Complete`。

## 目的

创建可验证的进程、文件系统和网络隔离，并通过 cgroup v2 强制执行本地资源 limits，
同时保持 Sandbox/Attempt 所有权模型以及安全、幂等的 cleanup。

## 范围

- PID、mount、UTS、IPC 和 network namespaces；
- 创建新 namespace，或加入经所有者验证的现有 namespace；
- `pivot_root`、`/proc`、device tmpfs 和 mount propagation；
- rootful Linux preflight 和故障处理；
- 使用 CPU、memory 和 pids controls 的 cgroup v2 父子层级；
- 相互独立的 request 与 limit 字段、OOM attribution 和 cleanup。

User namespaces、rootless 运行、cgroup v1 兼容、device policy、NUMA placement 和
多容器共享 PID 的语义不在初始范围内。

## 核心对象与流程

### Namespace 所有权

| Namespace | 默认所有者 | 原因 |
| --- | --- | --- |
| UTS | Sandbox | 在多个 Attempts 间保持稳定 hostname |
| IPC | Sandbox | 共享 workload environment |
| Network | Sandbox | 保持稳定的 attachment 和 IP identity |
| PID | Container Attempt | 每次执行使用全新的 PID 1 和 process tree |
| Mount | Container Attempt | rootfs 和 mounts 随每次执行创建与销毁 |

新 Sandbox 会创建 UTS/IPC/network namespaces，并交由 keeper 或 supervisor 通过
strong handle 持有。新 Attempt 先加入这些 Sandbox namespaces，再创建新的 PID 和 mount
namespaces。加入操作只接受从自有状态中解析出的 handle；如果没有验证 process identity，
绝不直接打开 `/proc/<persisted-pid>/ns/*`。

当前实现用 runtime-only pidfd 和可序列化的 boot ID、`/proc` start time、
cgroup path 与 executable 组成 strong process identity；任何信号、cgroup attach 或
namespace 打开前都会重新验证。Namespace handle 只能从这种已验证进程获得，
并绑定 nsfs、inode、namespace type 和 owner evidence。`setns` 只在锁定的
OS thread 上执行，session 持有并验证原 namespace descriptor 后才恢复；
`unshare` 和会改变 root 上下文的操作必须位于专用 runtime helper，不得从
`mydockerd` 的普通 goroutine 调用。

初始 M2/M3 process profile 还固定了一个跨里程碑契约：Attempt helper 是
PID namespace 内长期存活且**不执行 `exec`** 的 init wrapper（PID 1）。M2 只提供
strong identity、start gate、cgroup 和 provider 边界；M3 才交付具体 wrapper 与 daemon
编排。释放 gate 后，由 wrapper `fork` 并 `exec` workload child；wrapper 负责回收
后代、关联 workload exit/OOM evidence，并在重新验证 child identity 后转发信号。
`KindInitProcess` receipt 始终绑定 wrapper 的稳定 executable，不会因 workload
`exec` 改写成可变 receipt。这样 gated start 之后 `/proc/<pid>/exe` 不会变化并使
wrapper 的 strong identity 失效。

PID 1 child 的 readiness 分成两个持久边界。child 首先只验证自己确实是 PID 1，且
active PID/mount namespace inode 与 bootstrap 一致；此时不得 mount 或 pivot。daemon
必须把 init、PID namespace 和 mount namespace receipt 分别原子 checkpoint 后，才可
显式要求 child 执行一次性的 `PrepareRoot`。`RootfsRequest` 同时要求这三项同 owner
receipt；rootfs 失败或成功后都禁止在同一 helper 上重放 pivot，恢复只能 inspect 或
清理。rootfs receipt 与 cgroup attachment 均持久化后，Start 才能按
`attach_cgroup -> release_start_gate -> observe_process` 顺序推进。
attachment observation 的 canonical evidence 必须同时绑定 OwnerKey、精确 Attempt
cgroup receipt 和精确 init receipt；gate release request 还必须携带同 owner 的 rootfs
receipt，不能复用另一 Attempt 的合法 membership digest。

未来的多容器共享可以增加显式 PID-namespace policy；这不会改变初始默认值。

### Mount/root 配置

Attempt 的 mount namespace 首先将适当的 root propagation 设置为 private 或 slave，
防止 mount events 逸出至 host。随后按以下步骤配置：

```text
verify prepared rootfs
-> bind rootfs to itself as a mount point
-> create private old-root directory
-> pivot_root
-> chdir /
-> detach old root
-> remove old-root directory
-> mount a fresh /proc for the new PID namespace
-> mount only the explicitly allowed device/tmpfs paths
```

每个 syscall 都会将错误返回给 orchestration。所有必需的 mounts 和 isolation checks
通过之前，workload start gate 始终保持关闭。`/proc` 必须反映 Attempt 的 PID namespace；
teardown 时必须先卸载 nested/bind mounts，再卸载 rootfs。

runtime 初期仅支持 Linux 且以 rootful 模式运行。当前的 read-only preflight
验证 root 权限、cgroup v2 filesystem、配置的 cgroup root、所需 namespace 的
nsfs descriptor 和 pidfd 存活探测。当调用方声明要运行 privileged test 时，
preflight 还必须同时收到显式 opt-in 和“已确认为一次性环境”的标记，否则
失败关闭。cgroup manager 另外要求专用 delegated root 存在且为非符号链接
目录，并可用 `cpu`/`memory`/`pids` controllers。

### cgroup v2 层级

已实现的逻辑布局：

```text
delegated-mydocker-root/
└── sandbox-<sha256(id)>/             process-free Sandbox controller parent
    ├── keeper/                       fixed Sandbox keeper leaf
    └── attempt-<sha256(id)>/         Container Attempt leaf
```

配置的 delegated root 和每个 Sandbox parent 在启用 child controllers 前都必须
确认 `cgroup.procs` 为空。Sandbox parent 自身从不承载进程：初始 profile 要求
namespace keeper 直接启动在固定 `keeper/` leaf，Attempt init wrapper 与其 workload
descendants 直接启动在对应的 `attempt-<sha256(id)>/` leaf；二者是 siblings。这样满足
cgroup v2 no-internal-process 约束，同时 controller 与 descendant accounting 仍归属
Sandbox 层级。keeper overhead 计入 Sandbox descendant aggregate；独立 keeper leaf
保留以后单独报告其开销的明确边界。

`KeeperPath`、`CreateKeeper`、`RemoveKeeper`、`KeeperMembership` 和
`ConfirmKeeperProcess` 提供最小的确定性 leaf API。membership confirmation 与
Attempt 一样只读，不会把 receipt 已绑定的进程迁移到 `cgroup.procs`；launcher
必须在捕获 strong process evidence 前把进程安全地创建在目标 leaf。

预留以下资源字段：

```text
CPURequestMilli
CPULimitMilli
MemoryRequestBytes
MemoryLimitBytes
PidsLimit
```

规范且版本化的 `Resources` 值存放在 `SandboxSpec` 中。Requests 保留在稳定的 Sandbox
上，代表未来的 scheduling/accounting intent。执行 `CreateContainer` 时，daemon 解析
默认值，将 `ResolvedResourceLimits` 复制到 immutable `ContainerSpec`；Attempt 子
cgroup 只消费这份已解析副本。该持久/API JSON 对 CPU 和 memory 分别保存
`*_unlimited` 布尔值与可空的数值，并始终保存具体 `pids_limit`；因此
`max` 和默认值在 clone、持久化与返回中都是显式事实，不需要重读 Sandbox
原始请求来推断。初始 API 不允许 Container 覆盖 Sandbox resource policy。因此，
连续 Attempts 会继承同一 policy，除非未来通过显式 Sandbox update 增加 spec generation。

所有字段都是采用所述单位的可选整数。request 不存在表示不保留调度资源。CPU 或 memory
limit 不存在表示在 delegated parent 范围内使用 cgroup `max`。`PidsLimit` 不存在时
使用已固定的 daemon safety default `1024`。提供的 memory byte 和 pids 值必须
为正数；CPU limit 在固定 `100000` µs period 下必须至少为 `10m`，因为
Linux `cpu.max` quota 的最小可用值为 `1000` µs。`1m`–`9m` 会在 domain/API
验证阶段被拒绝，不得等到创建 cgroup 后才失败。
如果两侧都存在，则 CPU request 不得超过 CPU limit，memory request 不得超过 memory
limit。在多核 host 上，CPU milli 可以超过 1000。

Requests 本身不强制执行 quota，cgroup 实现不会把 request 字段写入任何
controller。Limits 会转换为 `cpu.max`、`memory.max` 和 `pids.max`。
`cpu.max` 的 period 固定为 `100000` µs，CPU milli 采用向上取整的转换：
`quota = ceil(CPULimitMilli * 100000 / 1000)`；CPU limit 缺省时写入
`max 100000`。Memory limit 缺省时为 `max`，`PidsLimit` 缺省时为
`1024`。由于真实 cgroup v2 可以将非页对齐 `memory.max` 向上规整为 host
`PAGE_SIZE` 倍数，cgroup manager 从 `HostProbe` 获取 page size，写入请求值后将
`ceil(value / PAGE_SIZE) * PAGE_SIZE` 作为 canonical readback 期望。返回给上层并应持久
的是读回的 effective 值，而不是伪造原始非对齐值未被 kernel 调整。CPU、
memory 和 pids 语义读回不一致时，创建失败并回滚新建子 cgroup。

## 关键设计

- 只支持 cgroup v2 unified hierarchy；在不支持的 host 上让 preflight 失败。
- 创建一个不承载进程的 Sandbox controller parent，在其下创建固定 keeper leaf
  和每个 Attempt 的独立 sibling leaf。
- 将 cgroup identity/path 作为 metadata 持久化，但使用前必须验证。
- 释放 workload start gate 前应用 controls。
- 将 controller write 和 process attachment 失败视为生命周期故障。
- strong process evidence 已包含 cgroup identity，因此 `AttachProcess` 和 keeper
  confirmation 会在只读 membership observation 前后各验证一次进程，并要求 PID
  保持一致；退出、PID reuse 或任一次 evidence failure 都会失败关闭。它们只确认
  PID 已存在于目标 `cgroup.procs`，绝不写入
  `cgroup.procs` 迁移捕获后的进程。真实 launcher 必须在捕获 ProcessHandle/receipt
  之前，通过专用 launcher、`clone3(CLONE_INTO_CGROUP)` 或等价安全启动协议
  将 helper 创建在目标 Attempt cgroup 中。
- 在 kernel 提供验证能力时，读回 effective/current values。
- 资源 API 值使用稳定单位；将 kernel 字符串格式化限制在 cgroup 实现内部。
- 保持 `SandboxSpec.Resources` 的权威性，并持久化用于强制执行的已解析 Attempt
  limit 副本。
- 不得将 CPU request 用作 CPU quota，也不得将 memory request 用作 `memory.max`。
- 使用 cgroup v2 events 和 Attempt timing/identity 判断 OOM，不得仅凭 exit code。
- 将 peak/current counters 作为 observations，而不是 lifecycle truth。
- 对宿主机资源使用 deterministic owner key 和带证据的 receipt；operation 的
  rollback descriptor 在每一个 provider 副作用后与 stage event 一起原子 checkpoint。
- Linux M2 create 从无副作用的 `persist_intent` 开始；每次事务最多追加一个 receipt，
  不能把多个 acquisition 压进同一个未持久化崩溃窗口。
- 只有在状态事务中完成 receipt adoption 后，宿主机资源所有权才从
  operation rollback 转移到 Sandbox/Attempt inventory；失败操作必须先完成已验证
  rollback，不得只改写 metadata。

## 故障与恢复

Namespace 和 cgroup 步骤都参与 lifecycle rollback stack。在启动前失败的 Attempt 会通过
其 strong handle 被 kill、unmount 和 detach，并按逆序移除其子 cgroup。只有所有 Attempts
都不存在后，才能停止 keeper，并按 `Attempt leaf -> keeper leaf -> Sandbox parent`
的拥有关系逐层清理。每一步只删除已验证、空且无子节点的 exact path；禁止以递归删除
代替 leaf-to-parent cleanup。

Removal 是幂等的。`EBUSY`、仍有进程的 cgroups、遗留 mounts 或存活 processes 会保留
可恢复的 failure/cleanup condition 和详细 event；不得隐藏这些情况，也不得通过递归删除
host 文件来处理。Reconciliation 只枚举配置 root 下自有的 paths/handles，根据 durable
state 进行验证，并且绝不删除未知的 cgroup 或 namespace。

M2 本身只交付 checkpoint/receipt/reverification 与 Linux 原语，不交付 daemon。
M3 已有经过纯测试的 daemon/engine/shim 协调与启动恢复，但生产 Linux launcher、
可无缝重连 supervisor 和真实 rootful restart 验收仍未完成，因此不声称已经接管
真实 workload。恢复只使用已持久 receipt 重新发现并作用时验证资源；无法证明身份时
报告显式的 `unknown`/`orphan` condition，而不是信任裸 PID 或可变路径。

## 可观测性与评测点

正确性场景包括：

- namespace inode 相互隔离，并按预期共享 UTS/IPC/network；
- Attempt 内的 PID 1 和 `/proc` visibility；
- mount propagation containment 和 old-root absence；
- 受控负载下的 CPU quota 行为；
- memory limit 和 OOM classification；
- pids 限制的强制执行；
- process-free parent、keeper/Attempt sibling membership 和 leaf-to-parent cleanup；
- 拒绝加入无法通过身份验证的现有 namespace；
- 注入任意 namespace/cgroup failure 后的 cleanup。

Resource overhead 会度量每个 Sandbox 的 supervisor memory/cgroup cost，以及每个 Attempt
的 process/cgroup/mount cost。Stress tests 会比较重复执行生命周期前后的 cgroup、mount、
zombie、FD、goroutine 和 daemon RSS 数量；测试允许有文档说明的 caches 和 delayed
cleanup，而不会假定每个 counter 都能立即归零。

常规 correctness tests 不断言不稳定的 latency thresholds。正确性得到确认后，
benchmarks 可以记录 namespace/cgroup stage duration。

### 当前验证边界

已在非特权路径上验证：

- 通过 fake `Ops` 验证 namespace create/join/restore、pidfd 强身份、信号重验证与
  rootfs 操作顺序和故障传播；
- 通过 fake/临时 cgroup filesystem 验证 process-free Sandbox parent、keeper/Attempt
  sibling leaves、controller 写入/读回、leaf-to-parent 精确清理、
  requests 不 enforcement、`8193` bytes 在 `4096` page size 下读回 `12288`、
  双重身份验证包围的只读 membership、PID reuse 失败关闭与 OOM counter delta；
- 通过状态和生命周期单元测试验证 checkpoint、receipt 不可变性/adoption、
  单次 acquisition acknowledgement、Start 必经 attach/gate/observe、failure rollback
  完成前不得提交失败终态，以及 provider 契约的所有者绑定；
- 上述包通过常规单元测试、race detector 和 `go vet`。

当前主机是普通、非一次性的裸机开发环境，当前进程无特权内核能力，
cgroup v2 子树也未委托所需 controllers；而且未收到对该宿主机运行高风险实验的
显式授权。按仓库安全规则，本次未运行任何真实 `unshare`/`setns`、
mount/`pivot_root`、cgroup controller 写入、信号、OOM/quota/PID 1、故障或压力场景。

特权验收只能在专用、一次性 Linux VM 或等价隔离宿主机放行。该环境需要：

- rootful 执行与所需 namespace/mount/pidfd 内核能力；
- 专用且确认由 mydocker 所有的 cgroup v2 delegated root，可用并可启用
  `cpu`/`memory`/`pids` controllers；
- 与宿主机的 mount/cgroup/process 资源隔离，并允许销毁和重建整个验收环境；
- 显式的 privileged-test opt-in，preflight 全部通过后才运行集成、故障和压力测试。

## 未来集群兼容性

集群调度使用 `CPURequestMilli` 和 `MemoryRequestBytes`；node-local enforcement 使用
`CPULimitMilli`、`MemoryLimitBytes` 和 `PidsLimit`。agent 创建 Sandbox 时会传递完整、
规范的 `Resources` 值。后续 Container 创建引用该 Sandbox policy；agent 不会发送可独立
修改的 limit 副本，也不会写入 cgroup 文件。本地 API 会随 observed status 一同报告
解析后的 effective limits。

Resource-aware scheduling policy、overcommit、Spread 和 Bin-Packing 属于集群关注点。
父子 cgroup accounting 和 limit correctness 仍属于 2.0 runtime 关注点。

## 验收条件

只有满足以下条件后，本功能才达到 Verified：

- supported-host preflight 能区分 cgroup v2 和必需的 namespace features；
- new/join namespace paths 都有正向和负向 integration tests；
- 在隔离 host 中验证 `pivot_root`、`/proc`、propagation 和 teardown；
- CPU、memory/OOM 和 pids controls 通过确定性的 correctness scenarios；
- requests 与 limits 分别持久化，且前者绝不会隐式改变 enforcement；
- absent/default values、units、ranges、request-versus-limit validation 和解析后的
  Container limit 副本通过 API/persistence tests；
- process-free parent 与 keeper/Attempt sibling membership 在 daemon restart
  reconciliation 后保持正确；
- 注入 setup failures 后可以 rollback，且不泄漏自有的 mount/cgroup/process；
- stress observations 表明资源行为有界且可以解释。

## 未决问题

- systemd 和非 systemd host 上确切的 cgroup root/delegation 契约。
- `memory.high` 是否作为单独的 enforcement field 暴露，而不是从
  `MemoryRequestBytes` 推断。
- 后续 milestones 中最小的 `/dev`、capability、seccomp 和 LSM policy。
