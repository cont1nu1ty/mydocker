# mydocker 路线图

本文档规定里程碑顺序和完成门槛。详细设计见 [architecture.md](architecture.md)，
功能契约见 [`features/`](features/)，度量规则见
[evaluation/README.md](../evaluation/README.md)。

允许使用的进度状态为 `Not started`、`In progress`、`Implemented`、`Measured`
和 `Verified`。只有里程碑规定的行为及验收检查全部通过后，才可以标记为
`Verified`；仅有文档并不能验证未来的运行时行为。

## 总览

| 里程碑 | 状态 | 产出 |
| --- | --- | --- |
| M0 仓库初始化 | Verified | 已审计的基线与可度量架构契约 |
| M1 生命周期基础 | Verified | 纯领域模型、状态及 operation 契约 |
| M2 隔离与 cgroup v2 | Implemented | rootful 隔离与资源强制执行；特权验收待完成 |
| M3 守护进程与本地 API | In progress | daemon/API/CLI/launcher/恢复/评测基础已编码；特权验收与 prepared-rootfs 隔离待完成 |
| M4A 镜像与内容 | Not started | OCI Image Layout 导入、内容寻址与 layer unpack |
| M4B 文件系统与 Snapshot | Not started | OverlayFS snapshot、rootfs、mount 与 bundle |
| M4C Sandbox 网络 | Not started | 稳定的 netns、veth/bridge、IPAM 与 port mapping |
| M5 监督与可靠性 | Not started | 重连、崩溃一致性、压力/故障覆盖 |
| M6 性能与发布基线 | Not started | 实测基线与证据驱动的优化 |
| C0 派生 mycluster | Not started | 仅在运行时通过门槛后创建 `mydocker-cluster` 分支 |
| C1 控制面基础 | Not started | API/存储/控制器/代理基础 |
| C2 调度器 | Not started | 资源感知的放置与模拟 |
| C3 协调与幂等性 | Not started | 在重试下实现期望/实际状态收敛 |
| C4 故障恢复 | Not started | Lease、节点故障、重新调度、MTTR |
| C5 集群性能 | Not started | 规模/E2E 基线与受控优化 |

## M0：仓库初始化

**状态：** Verified（仅限仓库/文档初始化）。M0 未实现或验证任何 2.0 运行时行为。

### 功能目标

- 审计实际的 Legacy 实现，不改变其行为。
- 固定带注释的 Legacy tag，并安全创建 `mydocker-2.0` 工作分支。
- 建立一个根 `AGENTS.md`、入口 README、架构/功能/路线图/Legacy 文档和评测契约。
- 记录真实分支拓扑，而不是虚构 `master` 分支。

### 正确性验收

- 从 `origin/legacy/v1` 确认 CLI、生命周期、namespace、cgroup、rootfs、
  network/IPAM、状态、测试和 Git 事实。
- 将已确认事实与需要隔离宿主机才能验证的场景分开。
- 所有尚未实现的运行时/集群能力保持标记为 Proposed 或 Planned。
- 只创建允许的 12 份核心文档；不创建代码骨架或虚假结果。

### 可靠性验收

- 保留用户/远端历史；不推送、不重写历史，也不改变默认分支。
- 给实际 Legacy commit 打 tag，而不是给无关的空 2.0 根提交打 tag。
- 不执行任何特权 mount、namespace、cgroup、bridge 或 firewall 操作。
- 记录不执行不安全 Legacy 测试体的原因。

### 度量准备

- 定义 cold Sandbox、Container create/start、cold full start、warm restart，
  以及两种生命周期吞吐量边界。
- 分开正确性、集成、失败、压力、benchmark、profiling 和可观测性验证。
- 定义环境/结果清单、比较规则、泄漏检测方法和故障集合。

### 完成条件

- 核心文件存在，相对链接和 Markdown 结构验证通过，且不存在额外骨架、依赖或结果。
- `git diff --check` 和状态审查通过。
- 如实报告安全的 Go 检查；危险的 Legacy 测试继续跳过。
- 在将 M0 标记为 `Verified` 前审查此清单；运行时里程碑保持 `Not started`。

## M1：生命周期基础

**状态：** Verified。纯 Go 领域/协调层、operation/event、rollback 与事务状态边界已由
单元测试、race detector 和静态检查验证；不包含任何宿主机资源实现。

### 功能目标

- 定义最小化的 Sandbox、Container/Attempt、spec/status、resources、generation、
  operation 和 event 模型。
- 固定初始的一对一 Container/Attempt 规则、不可变的 create generation，以及客户端
  生成 operation ID/request-fingerprint 的契约。
- 实现纯状态机和 `create/start/state/kill/delete` 契约。
- 将 argv/environment 保持为结构化数据。
- 加入 operation ID、stage-event 语义、回滚抽象及持久化接口边界，不涉及任何特权
  Linux 行为。

### 正确性验收

- 表驱动测试覆盖 Sandbox 和 Attempt 的每一个合法/非法转换。
- 强制执行单活跃 Attempt 及顺序 Attempt 不变量。
- 重复 operation IDs 和冲突 operations 具有确定性结果。
- exit outcome 字段和 graceful-stop 策略得到表达，且不捏造数据。

### 可靠性验收

- 每项建模的资源获取都有幂等逆操作和逆序回滚测试。
- state/operation 持久化接口明确原子性和 schema 要求。
- 任何代码路径都不得依据裸 PID 或拼接后的命令字符串授权操作。

### 度量准备

- 定义 operation/stage 名称和 monotonic duration 采样点。
- 测试 event 顺序和 operation 关联，但不作性能声明。
- 建立后续生命周期度量所需的场景契约。

### 完成条件

- 领域模型/状态/operation/回滚单元测试通过，且功能/架构契约与代码一致。
- 不引入 namespace、cgroup、daemon、network 或过早优化。

## M2：隔离与 cgroup v2

**状态：** Implemented, privileged verification pending。宿主机原语、所有权/回滚
契约和 provider 边界已实现，并通过纯单元测试、race detector 和静态检查。
真实 kernel 集成场景尚未在符合安全契约的一次性宿主机上运行，因此本里程碑
尚未达到 `Verified`。

### 功能目标

- 实现 rootful UTS/IPC/network/PID/mount namespace 的创建/加入边界。
- 实现 mount propagation、`pivot_root`、`/proc` 和 rootfs mount 原语。
- 实现 cgroup v2 的 process-free Sandbox controller parent、固定 keeper leaf 和
  sibling Attempt leaves，支持 CPU、memory、pids 控制、request/limit 分离及 OOM 证据。

### 当前实现

- `internal/isolation` 通过窄 `Ops` 边界实现 rootful/cgroup-v2/namespace/pidfd
  read-only preflight、专用 helper 中的 namespace creation、锁定 OS thread 的 join/restore、
  pidfd 加 boot ID/start time/cgroup/executable 强身份，以及 private propagation、
  self-bind、`pivot_root`、old-root detach、新 `/proc` 和最小 `/dev` tmpfs 原语。
- `internal/cgroupv2` 只支持 unified cgroup v2，在专用 delegated root 下使用
  deterministic、process-free Sandbox parent，以及固定 `keeper/` 与
  `attempt-<sha256(id)>/` sibling leaves。parent 只启用 controllers、不承载进程；
  manager 提供 keeper 的创建、精确删除、只读 membership/identity confirmation，
  并在 Attempt leaf 强制 `cpu.max`、`memory.max` 和 `pids.max`，读回 effective values、
  membership、current/events 及 `memory.events.local` OOM 证据。
- `cpu.max` period 已固定为 `100000` µs，CPU milli quota 向上取整；为满足
  kernel `1000` µs quota 下限，domain/API 在任何宿主机副作用前拒绝
  `1m`–`9m` CPU limit，`10m` 是最小可执行值。
- `PidsLimit` 缺省值已固定为 `1024`。Sandbox 保留原始 `Resources`，
  immutable `ContainerSpec.Limits`（`ResolvedResourceLimits`）对 CPU/memory unlimited 以及具体 pids
  默认值进行显式 JSON 序列化和 clone；CPU 和 memory requests 只保存调度
  意图，绝不写入 enforcement controllers。
- cgroup manager 使用 `HostProbe` page size 验证 `memory.max` 向上页对齐的
  canonical effective readback，上层返回/持久 effective 值，不强行宣称非对齐
  请求值原样存在。
- `AttachProcess` 与 keeper confirmation 在 membership read 前后各验证一次
  strong process evidence，并要求 PID 保持一致；它们只读确认目标 leaf membership，
  对退出、PID reuse 或二次验证失败均失败关闭，绝不在捕获 cgroup-bound
  ProcessHandle 后写 `cgroup.procs` 迁移进程；
  后续真实 launcher 必须先把 helper 创建在目标 cgroup，再捕获 receipt。
- `internal/ownership`、state/lifecycle checkpoint、receipt adoption、failure rollback 和
  `internal/provider` 契约已表达每个宿主机副作用的所有者证据、原子阶段
  进度、转移与精确逆操作。
- Linux provider operation 必须先持久化无副作用 intent，随后每个事务最多确认一个
  acquisition receipt；Start 的 `attach_cgroup`、`release_start_gate` 和
  `observe_process` checkpoint 不可跳过。

### M2/M3 process profile 边界

- M2 固定 identity/gate/provider 契约：Attempt init 是 PID namespace 内 PID 1，
  是长期存活且不执行 `exec` 的 wrapper；`KindInitProcess` receipt 永远绑定该
  wrapper 的稳定 executable，不创建随 workload `exec` 变化的 receipt。
- M3 负责实现并编排具体 wrapper。start gate 释放后由 wrapper `fork`/`exec`
  workload child；wrapper 负责 descendant reaping、workload exit/OOM evidence
  关联，以及在作用时验证 child identity 后进行信号转发。
- PID 1 wrapper 先只完成 PID/mount identity readiness；daemon 分别持久化 init、PID 和
  mount receipts 后才发出一次性 rootfs prepare 指令。`RootfsRequest` 必须携带三项
  同 owner evidence；cgroup attachment evidence 必须绑定精确 Attempt cgroup 与 init
  receipts，rootfs receipt 与该 attachment 均确认后才允许释放 gate。
- Sandbox namespace keeper 直接位于固定 `keeper/` leaf，Attempt wrapper 与
  workload descendants 位于 sibling Attempt leaf。M2 的纯原语测试不等同于
  已运行 M3 launcher 或真实 PID 1/cgroup 集成场景。

### 当前验证与待验收项

- 已验证：fake `Ops`、fake/临时 cgroup filesystem（包括空 parent、keeper/Attempt
  siblings、只读双重身份 membership 与精确 leaf-to-parent cleanup）、状态/生命周期
  事务和 provider 契约的普通单元测试、race detector 与 `go vet`。
- 未运行：真实 `unshare`/`setns`、mount/`pivot_root`、PID 1/`/proc`、
  cgroup controller 写入/membership/cleanup、CPU quota、memory/OOM、pids limit、
  特权故障注入与压力场景。
- 跳过原因：当前是普通、非一次性的裸机开发环境，当前进程无特权
  内核能力，cgroup v2 子树未委托所需 controllers，且未收到针对该宿主机的
  高风险实验授权。
- 放行条件：专用、一次性 rootful Linux VM 或等价隔离宿主机，具备所需
  namespace/mount/pidfd 能力和已委托 `cpu`/`memory`/`pids` 的专用 cgroup v2
  root，并对 privileged test 显式 opt-in；必须先通过 read-only preflight。

### 正确性验收

- 隔离宿主机测试验证预期的 namespace 共享/分离及 `/proc` 视图。
- CPU quota、memory limit/OOM、pids limit 和 cgroup membership 场景通过。
- 无效资源 spec、未验证的 namespace handles 和不受支持的宿主机，都必须在工作负载
  启动前失败。

### 可靠性验收

- 每个 namespace/mount/cgroup 阶段的失败都会回滚已拥有资源。
- 清理按 Attempt/keeper leaves 到 Sandbox parent 的顺序逐个执行，保持幂等且
  禁止递归，并拒绝掩盖处于 populated/busy/unknown 状态的宿主机资源。
- 前置检查阻止在不合适的普通宿主机上运行特权测试。

### 度量准备

- 按已定义边界发出 namespace/mount/cgroup 阶段事件。
- 定义每个 Sandbox/Attempt 的资源清单和开销观测方法。
- 建立基线前，不为追求速度而调整 kernel/resource 设置。

### 完成条件

- 单元测试和隔离集成测试在有记录的 cgroup v2 宿主机上通过。
- 不存在 cgroup v1 兼容路径；记录限制条件和 kernel matrix。

## M3：守护进程与本地 API

**状态：** In progress。纯 Go、注入 provider 与生产 Linux launcher 已组成，但真实
rootful lifecycle、daemon restart 和 prepared-rootfs 隔离尚未通过，因此不得标记为
`Verified`。

### 功能目标

- 实现 `mydockerd`、版本化 UDS API、CLI 客户端、持久元数据、operations、
  events/logs 及守护进程协调。
- 让守护进程成为后台生命周期的权威组件。
- 使用显式的精简 provider，在 M1/M2 原语之上实现初始 Sandbox/Attempt 编排：
  预先准备的测试 rootfs、`network=none`/loopback，以及由守护进程启动、用于持有
  Sandbox namespaces、直接位于固定 keeper leaf 的最小 keeper；同时实现长期不
  `exec` 的 Attempt init wrapper（PID namespace 内 PID 1），由其在 start gate
  释放后 `fork`/`exec` workload child，并负责 reap、exit/OOM 关联和经验证的信号
  转发。`KindInitProcess` receipt 始终绑定 wrapper executable。M3 不声称具备 M4A/M4B 的
  image/snapshot 路径或 M4C 的 veth 路径，
  也不声称具备 M5 的可重连 supervisor。
- 为这个具名的精简 provider 加入首个可通过公共 API 运行的评测工具和生命周期基线
  定义（不是优化）。

### 当前实现

- `internal/state.FileStore` 已为 schema/CAS、Sandbox/Container/Attempt、operation、
  rollback 和事件提供独占锁与原子持久边界，并覆盖 close/reopen、
  失败注入和不确定 commit 恢复测试。
- FileStore schema v3 已实现确定性的 count-based retention 与旧 event 计时语义迁移：active operation 永不淘汰；
  最近 `1024` 个终态 operation 保留完整响应，更早记录转换为防 ID 复用的 tombstone；
  最近 `8192` 个 event 构成连续 suffix，过旧或超前的非空 cursor 返回版本化 `resume_gap`。
  完整 identity+tombstone 总数默认最多 `65536`，状态 envelope 最多 `64 MiB`；达到上限
  在副作用前以 `resource_exhausted` 失败关闭。小限额可注入测试，策略不依赖 wall clock。
- `internal/engine`、shim 协议、精简 isolation/cgroup provider 和 owner-bound artifact
  已实现创建、启动、kill、停止、删除、反向 rollback 和 daemon restart
  reconciliation 编排。全量 recovery 只在 UDS 绑定前运行；上线后的终态 watcher
  将自然退出投影为持久 outcome，独立 kill-deadline watcher 只扫描 active Kill，
  使已持久 escalation deadline 不依赖客户端再次请求且不向在线对象写回旧全局快照。
- `api/runtime/v1`、HTTP/JSON UDS server、`pkg/client` 和 JSON CLI 已实现严格
  version/error/request-ID/operation-ID 契约，包括 Sandbox/Container 生命周期、
  operation 查询、事件 resume 和绑定 Container/Attempt 身份的 log cursor。
- events 保留资源与 operation 关联，log frames 绑定精确的 Container/Attempt
  身份；`mydocker-eval` 只经公共 API 运行，
  已实现 cold/warm prepared-rootfs 场景、调用方单调 span、daemon stage-event
  关联、失败后清理尝试、环境快照及持久 JSONL 原始证据。
- Sandbox hostname、DNS 和 `network=none/loopback` 已有 typed validation/provider/fake
  契约；这些测试不表示 UTS、`resolv.conf` 或 loopback 已在真实 kernel 中执行。

### 当前 blocker

- 生产 `LinuxShimLauncher` 已把 cgroup-at-fork/pidfd、namespace reattach、
  UTS/none/loopback 配置、deferred-rootfs ACK、DNS 绑定、跨 pivot artifact descriptor、
  immutable launch intent、keeper/init readiness、crash rediscovery 和作用时
  inspect/remove/signal/resolve 组合为 process factory，并有非特权/注入 seam 测试。
  但这些测试不能证明真实 namespace、PID 1、mount、cgroup 与 daemon crash 组合正确；
  仍需在一次性 rootful VM 中逐项验收。
- M3 的 prepared-rootfs catalog 目前只把 opaque ID 映射到一个受信但共享的目录；它不
  创建每 Attempt snapshot，也没有证明并发/连续 Attempt 之间写入隔离。签入的 M3
  场景必须如实记录 `not-created-prepared-rootfs-shared`，不能将其作为 M4B snapshot
  性能或正确性证据。
- operation/event 的无界增长已由固定窗口、tombstone、原子 compaction 和显式 gap
  消除；但达到 `65536` identity 或 `64 MiB` envelope 后尚无在线 rollover/归档/迁移
  流程。当前会安全拒绝新 intent，而不是无限增长；仍不得据此声称长期生产可用。
- 真实 rootful namespace/mount/cgroup/PID 1/OOM/quota/hostname/DNS/loopback 生命周期和
  daemon restart 场景尚未在一次性 VM 上运行。

### 正确性验收

- API 版本/错误/幂等性测试及 CLI 退出状态测试通过。
- CLI 退出后，后台工作负载继续正确运行，且守护进程/minimal keeper 拥有精简
  生命周期。
- Attempt wrapper 在 gated start 前后保持同一 strong executable identity；workload
  child 的退出、OOM 与信号结果经 wrapper 关联，不通过改写 init receipt 表达。
- 生命周期 operations 和状态查询与持久/观测 generations 一致。
- 结构化 logs/events 保留资源与 operation 的关联。

### 可靠性验收

- 守护进程在精简 provider 每个持久阶段重启时都能安全协调。M5 之前，对于重启前正在
  运行的工作负载，可以安全检测，并在当前 phase 上显式标记 `unknown` condition。
  `stopped` 要求确认进程不存在并遵守 outcome 策略；此阶段尚不声称能无缝重连
  supervisor。
- 部分请求或重复请求不会造成副作用重复。
- 原子元数据测试覆盖写入失败和无效 schema。
- retention 测试覆盖 exact replay window、过期 ID、active intent、容量拒绝、event gap、
  FileStore v1/v2→v3 与 event-v2 计时语义迁移、oversized 文件，以及 checksum 正确但
  retention binding 被篡改的文件。

### 度量准备

- 评测工具记录调用方 spans、阶段事件、环境、失败及原始 samples。
- 建立带 `prepared-rootfs+loopback` 标签、可执行的 cold/warm 场景定义；不得与
  M4A–M4C 完整路径直接比较。
- 为 Proposed 的低基数指标加入 label-cardinality 测试。

### 完成条件

- 首个非特权生命周期测试套件和隔离特权生命周期测试套件通过。
- 可复现的基线流程存在；不作性能提升声明。
- 文档说明哪些完整 Ready/Created 资源仍推迟到 M4A–M4C/M5。

## M4A：镜像与内容

**状态：** Not started。

### 功能目标

- 实现 OCI Image Layout 导入、manifest/config/layer digest 校验、
  content-addressable blob store 和版本化镜像元数据。
- 按 manifest 声明的顺序实现 layer unpack，并将可变镜像引用解析为
  一个不可变 digest 后再持久化执行身份。
- 实现 `ImportImage`、`EnsureImage`、`GetImage`、`ListImages` 和 `RemoveImage`
  的最小本地契约，使用预加载 OCI Image Layout 完成首个可运行路径。

### 正确性验收

- 正确 OCI layout 可导入，错误、缺失或 digest 不匹配的 blob 在进入
  snapshot 路径前被明确拒绝。
- 相同 digest 的重复导入和 `EnsureImage` 重试不重复写入内容或生成不一致的元数据；
  get/list/remove 遵守稳定查询、引用保护和显式删除契约。
- layer 顺序、whiteout 输入和路径安全检查有自动化测试覆盖。

### 可靠性验收

- 导入、digest 校验、blob 提交和 layer unpack 的每个失败阶段都有
  逆序回滚与崩溃恢复覆盖。
- 临时内容和版本化元数据采用原子提交；守护进程重启后只协调
  经过 digest 与所有者验证的本地内容。
- 删除受引用的 content/layer 必须失败或按明确的延迟回收策略处理。

### 度量准备

- 记录 `image_import_duration`、`digest_verification_duration`、
  `layer_unpack_duration`、`content_dedup_ratio` 和 `unpacked_disk_usage`。
- 区分 content/layer 缺失与已存在的 cold/warm 路径，并保留原始样本和
  输入 OCI layout 的 digest/环境清单。

### 完成条件

- OCI Image Layout 能经由 digest 校验、内容存储和 layer unpack 形成
  M4B 可消费的本地输入，正确性/失败测试通过。
- benchmark 场景保留原始数据和环境清单，但尚不作为发布性能声明。
- Registry pull 可以后续增强；Dockerfile build、container commit 和 push 不属于
  mydocker 2.0 的完成条件。

## M4B：文件系统与 Snapshot

**状态：** Not started。

### 功能目标

- 以 M4A 经验证的 unpacked layers 为输入，实现每个 Attempt 的
  OverlayFS snapshot、rootfs 和嵌套 mount。
- 实现 mount manager 与 bundle builder，产生供低层 runtime 消费的
  rootfs/bundle，而不让 runtime 解析镜像。
- 实现版本化 snapshot/mount 元数据、引用与 daemon restart recovery。

### 正确性验收

- bundle/path、snapshot 所有权、OverlayFS lower/upper/work/merged 约束和
  mount 验证在受控环境中通过。
- 每个 Attempt 获得独立可写层，共享的不可变 layers 不被容器写入修改。
- 已验证 digest 到 rootfs、bundle 再到运行时输入的关联可追溯。

### 可靠性验收

- snapshot prepare、mount、bundle persistence 和 teardown 的每个失败阶段
  都有逆序回滚与重启协调覆盖。
- unmount 失败不能通过暴露的宿主机 mount 导致递归删除；状态保留为
  可诊断且可重试。
- 守护进程重启后只协调经过所有者、digest 和 mount identity 验证的资源。

### 度量准备

- 记录 `snapshot_prepare_duration`、`overlay_mount_duration` 和
  `bundle_prepare_duration`，不将它们隐式合并为单一启动数字。
- 实现 copy-versus-OverlayFS、cold/warm snapshot、allocated/apparent disk usage
  以及 native/OverlayFS 工作负载场景。
- 建立 cold/warm 启动 benchmark 输入和 content/unpack/snapshot/page-cache 定义。

### 完成条件

- 镜像到 rootfs/snapshot/bundle 的最小路径可通过公共生命周期 API 启动
  workload，正确性、回滚和恢复测试通过。
- 文件系统 benchmark 保留原始数据与环境清单，但尚不作为发布性能声明。

## M4C：Sandbox 网络

**状态：** Not started。

### 功能目标

- 实现由 keeper 持有的 Sandbox network namespace、veth/bridge、本地并发 IPAM、
  routes、DNS inputs 和 port mappings。
- 将网络所有权、持久化状态、生命周期回滚及 daemon recovery 集成。

### 正确性验收

- netns、IP uniqueness、route、DNS 和 port mapping 行为测试在受控环境中通过。
- 顺序 Attempts 保持 Sandbox 网络身份。
- 并发 IPAM 请求具有唯一性和幂等性。

### 可靠性验收

- 每个 network/IPAM/link/route/firewall 失败阶段都有逆序回滚覆盖。
- 原子 IPAM/network 状态能承受注入的写入/崩溃故障。
- 守护进程重启后只协调经过所有者验证的本地网络资源。

### 度量准备

- 实现 Sandbox 网络建立、并发配置和清理观测。
- 建立网络 cold/warm 定义，并与 M4A/M4B 的 content、snapshot 缓存状态分开。

### 完成条件

- 正确性/失败测试套件通过，网络场景保留原始数据和环境清单。
- 不加入多节点 VXLAN 或集群网络。

## M5：监督与可靠性

**状态：** Not started。

### 功能目标

- 用每个 Sandbox 一个、可重连的 shim/supervisor 及最终 namespace-keeper 拓扑，
  替换/扩展 M3 minimal keeper。
- 使用 pidfd 或经过验证的后备身份，持久化 exit outcome，并让守护进程
  与 supervisors 重连。
- 完成崩溃一致的协调和有主孤儿资源清理。

### 正确性验收

- exit code、signal、OOM、log stream 和 process identity 测试通过。
- 重连会保留当前 Attempt 和顺序历史。
- PID reuse/unowned process 测试证明不会意外发信号或收养进程。

### 可靠性验收

- 守护进程、supervisor 和用户进程崩溃矩阵通过。
- 每个 setup/teardown/persistence 失败都具有确定性的恢复行为。
- 长期/重复生命周期压力测试能区分有界缓存与持续泄漏。

### 度量准备

- 记录每个 Sandbox 的 supervisor 开销及恢复阶段时长。
- 实现故障矩阵和资源清单趋势采集。
- 同时定义恢复正确性和时长；绝不单独报告 MTTR。

### 完成条件

- 故障矩阵、重启恢复和资源泄漏压力测试在有记录的环境中达到 Verified，且不存在
  无法解释的、经所有者验证的残留资源。

## M6：性能与发布基线

**状态：** Not started。

### 功能目标

- 建立稳定的 benchmark 环境和节点本地发布基线。
- 度量启动/生命周期、文件系统及内存/资源开销场景。
- 对一个已观测瓶颈进行 profiling，实现一项受控优化，并重复完全相同的 benchmark。
- 只有正确性/可靠性门槛通过后，才生成明确的发布基线 tag。

### 正确性验收

- 所有场景在优化前后都通过语义检查。
- 不得为获得好看的数值而让行为、清理或故障处理退化。

### 可靠性验收

- 压力/故障/重启测试套件在候选 commit 上通过。
- 原始结果包含失败、重试、清理和环境元数据。

### 度量准备

- 固定机器、kernel、Go/build、镜像 digest、存储/网络、工作负载、并发度、
  samples、warm-up 和 profiling 状态。
- 保留原始结果与摘要结果，并记录比较资格和限制。

### 完成条件

- 可复现基线达到 Measured。
- 至少一个瓶颈得到 profile 证据支持。
- 在可比条件下重新运行一项受控优化；如实记录其结果，包括无提升或退化的结果，
  不捏造百分比。
- 发布基线 tag 标识供未来集群使用且已验证的 runtime/API。

## C0：派生 mycluster（`mydocker-cluster` 分支）

**状态：** Not started；该分支不存在。

### 功能目标

从满足下述任一分支创建门槛的明确 `mydocker-2.0` tag 或 commit 派生集群项目。

### 分支创建门槛

C0 有两条可审查的进入路径：

1. **发布门槛：** M6 的发布基线和受控优化验收全部通过。
2. **等效 alpha 门槛：** 经明确、留档的审查批准，并且同一候选
   commit 同时提供以下证据：
   - Sandbox/Attempt 生命周期在公共 API 上稳定，状态转换、清理和单活跃
     Attempt 不变量有自动化测试；
   - 受支持 Linux 环境上的 namespace 和 cgroup v2 最小路径可用，前置检查与
     资源限制测试通过；
   - M4A 的 image/content、M4B 的 snapshot/rootfs 和 M4C 的 Sandbox network
     最小路径可组合启动 workload；
   - 本地 API 已版本化，agent 所需操作可全部经由 API/`pkg/client`
     完成；
   - daemon restart recovery 和 operation 幂等性有自动化测试，重复请求不会
     创建重复的活跃 workload；
   - 已建立可复现的节点本地 baseline，保留候选 commit、场景版本、环境
     清单、原始结果和失败。

等效 alpha 门槛不要求先完成 M6 的 profiling 或最终性能优化。它只允许派生
集群分支，不会将未完成的 M5/M6 自动标记为 `Verified`，也不支持发布性能声明。

### 正确性验收

- Sandbox/Attempt 生命周期和版本化本地 API 稳定且有文档记录。
- Agent 访问可以完全通过 API/`pkg/client` 边界实现。

### 可靠性验收

- 守护进程重启恢复和 operation 幂等性有自动化测试覆盖。
- supervisor/协调行为足够稳定，可以支持 at-least-once Assignment。

### 度量准备

- 存在节点本地基线，且未来每次集群运行都能记录其 runtime 基础 commit。
- Trace/correlation context 跨越未来 agent/本地 API 边界。

### 完成条件

- 发布门槛或上述全部等效 alpha 门槛已在候选 commit 上审查并留档；
  只有此后才能创建 `mydocker-cluster`。

## C1：控制面基础

**状态：** Not started。

### 功能目标

实现集群 API 服务端、基于 etcd 的持久状态、controller、agent、
Node/Task/Assignment 模型和关联上下文。使用 etcd 的事务/watch/lease 边界，
不自研 Raft，也不复刻完整 Kubernetes API/对象。

### 正确性验收

对期望/实际 generations 和本地 Sandbox/Attempt 映射进行端到端测试。

### 可靠性验收

controller/agent 重启和重复投递可以保持状态，且不绕过 `mydockerd`。

### 度量准备

对 Submit、persist、Assignment delivery、agent reconcile、本地 API 以及
controller-observed 边界插桩记录。

### 完成条件

一个小型真实节点流程和一个确定性模拟流程都能在固定 runtime 基础 commits 的条件下
收敛。

## C2：调度器

**状态：** Not started。

### 功能目标

实现 resource request 计量、eligibility、Spread、Bin-Packing 以及确定性模拟器。

### 正确性验收

记录的输入能够生成有效且确定性的放置结果，并拒绝资源不足的节点。

### 可靠性验收

过期的 Node/Task generations 和并发调度具有明确的重试行为。

### 度量准备

为 10、100 和 500 个模拟节点的配置实现 scheduler queue/execution
throughput/latency 和 utilization 场景，但不把场景名称当作结果。

### 完成条件

策略正确性通过，模拟器保真度有明确边界，且原始调度基线数据可复现。

## C3：协调与幂等性

**状态：** Not started。

### 功能目标

实现期望/实际状态协调、at-least-once Assignment、稳定的 operation identity、
状态收敛及重试处理。

### 正确性验收

重复、丢失、延迟和乱序的 requests/statuses 能收敛到最新 generation，且不会产生
重复的存活工作负载。

### 可靠性验收

agent/controller 重启后从持久状态恢复协调。

### 度量准备

记录协调吞吐量、重试负载、收敛延迟、重复量及不一致状态持续时间。

### 完成条件

确定性故障/重试测试套件跨控制面和本地 API 边界通过。

## C4：故障恢复

**状态：** Not started。

### 功能目标

实现心跳、Lease、节点故障检测、隔离策略、重新调度及 controller 恢复。

### 正确性验收

故障/分区/恢复状态机可以阻止隐蔽的重复所有权，并准确报告 Task 结果。

### 可靠性验收

agent 崩溃、controller 重启、RPC 丢失/延迟/重复以及节点分区等故障场景，能按记录的
策略收敛。

### 度量准备

分别度量故障检测、重新调度、节点本地替换启动和 controller 观测，以及总恢复时间（MTTR）。

### 完成条件

故障测试通过，在声明的模拟与真实节点范围内，重复/不一致状态有明确边界，且恢复
结果可复现。

## C5：集群性能

**状态：** Not started。

### 功能目标

建立 scheduler、reconciliation 和 E2E 规模基线；对一个已观测瓶颈进行 profile；
实施并重新度量一项受控优化。

### 正确性验收

在每个实测规模下，placement、lifecycle 和状态收敛都保持正确。

### 可靠性验收

故障行为和恢复保持在实测场景声明的语义范围内；失败/重复工作保留在原始结果中。

### 度量准备

固定 cluster/runtime commits、模拟或真实拓扑、存储/网络设置、到达/资源分布、
samples 及 trace/profiling 状态。

### 完成条件

可复现的调度和 E2E 基线、阶段分解、profile 证据、一项受控优化
结果及限制，均达到 Measured 并完成审查。

## 下一里程碑

M2 实现已落地，但下一个 M2 gate 是在专用一次性环境通过 preflight 后，
运行并记录 namespace、mount、cgroup v2 与资源强制的特权集成验收。
在这些证据齐备前，M2 保持 `Implemented`，不能标记为 `Verified`；后续里程碑的
实现不能代替该 gate。
