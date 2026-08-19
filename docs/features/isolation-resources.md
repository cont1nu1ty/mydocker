# 隔离与资源

## 状态

**Proposed。** M0 定义 Linux 和资源契约；尚不存在 2.0 隔离代码。

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

runtime 初期仅支持 Linux 且以 rootful 模式运行。Preflight 会检查 kernel features、
mount propagation、cgroup v2、controllers、filesystem support、必需的 tools 或 syscalls，
以及当前环境是否明确获准运行 privileged tests。

### cgroup v2 层级

计划中的布局：

```text
mydocker.slice-or-root/
└── sandbox-<id>/             Sandbox parent cgroup
    └── attempt-<id>/         Container Attempt child cgroup
```

daemon 通过已知的 delegated parent 启用 controllers，并处理 cgroup v2 的
no-internal-process 约束。Sandbox supervisors 的放置方式由明确的 accounting policy
决定；用户 workload processes 归入 Attempt 子 cgroup。

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
默认值，将 limit fields 复制到 immutable `ContainerSpec`；Attempt 子 cgroup 强制执行
这份副本。初始 API 不允许 Container 覆盖 Sandbox resource policy。因此，连续 Attempts
会继承同一 policy，除非未来通过显式 Sandbox update 增加 spec generation。

所有字段都是采用所述单位的可选整数。request 不存在表示不保留调度资源。CPU 或 memory
limit 不存在表示在 delegated parent 范围内使用 cgroup `max`。`PidsLimit` 不存在时
使用有文档说明的 daemon safety default；解析后的整数或显式 `max` 必须被持久化并返回，
而不是隐藏起来。提供的 CPU milli、byte 和 pids 值必须为正数；零或负数均无效。
如果两侧都存在，则 CPU request 不得超过 CPU limit，memory request 不得超过 memory
limit。在多核 host 上，CPU milli 可以超过 1000。

Requests 本身不强制执行 quota。Limits 会转换为 `cpu.max`、`memory.max` 和 `pids.max`
等 cgroup v2 controls；mapping、rounding 和 effective-value readback rules 必须形成文档
并经过测试。

## 关键设计

- 只支持 cgroup v2 unified hierarchy；在不支持的 host 上让 preflight 失败。
- 创建一个 Sandbox 父 cgroup，并为每个 Attempt 创建独立的子 cgroup。
- 将 cgroup identity/path 作为 metadata 持久化，但使用前必须验证。
- 释放 workload start gate 前应用 controls。
- 将 controller write 和 process attachment 失败视为生命周期故障。
- 在 kernel 提供验证能力时，读回 effective/current values。
- 资源 API 值使用稳定单位；将 kernel 字符串格式化限制在 cgroup 实现内部。
- 保持 `SandboxSpec.Resources` 的权威性，并持久化用于强制执行的已解析 Attempt
  limit 副本。
- 不得将 CPU request 用作 CPU quota，也不得将 memory request 用作 `memory.max`。
- 使用 cgroup v2 events 和 Attempt timing/identity 判断 OOM，不得仅凭 exit code。
- 将 peak/current counters 作为 observations，而不是 lifecycle truth。

## 故障与恢复

Namespace 和 cgroup 步骤都参与 lifecycle rollback stack。在启动前失败的 Attempt 会通过
其 strong handle 被 kill、unmount 和 detach，并按逆序移除其子 cgroup。只有所有 Attempts
都不存在后，才清理 Sandbox namespaces 和父 cgroup。

Removal 是幂等的。`EBUSY`、仍有进程的 cgroups、遗留 mounts 或存活 processes 会保留
可恢复的 failure/cleanup condition 和详细 event；不得隐藏这些情况，也不得通过递归删除
host 文件来处理。Reconciliation 只枚举配置 root 下自有的 paths/handles，根据 durable
state 进行验证，并且绝不删除未知的 cgroup 或 namespace。

如果 daemon 在 Attempt 运行期间重启，supervisor/strong process handle 可用于接管并
继续收集 outcome。如果无法证明身份，daemon 会报告显式的 `unknown`/`orphan`
condition，并遵循明确的安全策略。

## 可观测性与评测点

正确性场景包括：

- namespace inode 相互隔离，并按预期共享 UTS/IPC/network；
- Attempt 内的 PID 1 和 `/proc` visibility；
- mount propagation containment 和 old-root absence；
- 受控负载下的 CPU quota 行为；
- memory limit 和 OOM classification；
- pids 限制的强制执行；
- 父子 cgroup membership 和 cleanup；
- 拒绝加入无法通过身份验证的现有 namespace；
- 注入任意 namespace/cgroup failure 后的 cleanup。

Resource overhead 会度量每个 Sandbox 的 supervisor memory/cgroup cost，以及每个 Attempt
的 process/cgroup/mount cost。Stress tests 会比较重复执行生命周期前后的 cgroup、mount、
zombie、FD、goroutine 和 daemon RSS 数量；测试允许有文档说明的 caches 和 delayed
cleanup，而不会假定每个 counter 都能立即归零。

常规 correctness tests 不断言不稳定的 latency thresholds。正确性得到确认后，
benchmarks 可以记录 namespace/cgroup stage duration。

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
- 父子 membership 在 daemon restart reconciliation 后保持正确；
- 注入 setup failures 后可以 rollback，且不泄漏自有的 mount/cgroup/process；
- stress observations 表明资源行为有界且可以解释。

## 未决问题

- systemd 和非 systemd host 上确切的 cgroup root/delegation 契约。
- supervisor 的放置方式，以及其 overhead 是否计入 Sandbox parent。
- 初始 `cpu.max` period 和 validation/rounding rules。
- 省略 `PidsLimit` 时初始的 daemon safety default。
- `memory.high` 是否作为单独的 enforcement field 暴露，而不是从
  `MemoryRequestBytes` 推断。
- 后续 milestones 中最小的 `/dev`、capability、seccomp 和 LSM policy。
