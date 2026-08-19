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
| M1 生命周期基础 | Not started | 纯领域模型、状态及 operation 契约 |
| M2 隔离与 cgroup v2 | Not started | rootful 隔离与资源强制执行 |
| M3 守护进程与本地 API | Not started | 长期运行的权威组件和首个可运行评测工具 |
| M4 存储与网络 | Not started | Snapshot/rootfs 与稳定的 Sandbox 网络 |
| M5 监督与可靠性 | Not started | 重连、崩溃一致性、压力/故障覆盖 |
| M6 性能与发布基线 | Not started | 实测基线与证据驱动的优化 |
| C0 创建 mydocker-cluster | Not started | 仅在运行时通过门槛后派生集群项目 |
| C1 控制平面基础 | Not started | API/存储/控制器/代理基础 |
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
- 只创建允许的 11 份核心文档；不创建代码骨架或虚假结果。

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

**状态：** Not started。

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

**状态：** Not started。

### 功能目标

- 实现 rootful UTS/IPC/network/PID/mount namespace 的创建/加入边界。
- 实现 mount propagation、`pivot_root`、`/proc` 和 rootfs mount 原语。
- 实现 cgroup v2 的 Sandbox 父层级和 Attempt 子层级，支持 CPU、memory、pids 控制、
  request/limit 分离及 OOM 证据。

### 正确性验收

- 隔离宿主机测试验证预期的 namespace 共享/分离及 `/proc` 视图。
- CPU quota、memory limit/OOM、pids limit 和 cgroup membership 场景通过。
- 无效资源 spec、未验证的 namespace handles 和不受支持的宿主机，都必须在工作负载
  启动前失败。

### 可靠性验收

- 每个 namespace/mount/cgroup 阶段的失败都会回滚已拥有资源。
- 清理是幂等的，并拒绝掩盖处于 busy/unknown 状态的宿主机资源。
- 前置检查阻止在不合适的普通宿主机上运行特权测试。

### 度量准备

- 按已定义边界发出 namespace/mount/cgroup 阶段事件。
- 定义每个 Sandbox/Attempt 的资源清单和开销观测方法。
- 建立基线前，不为追求速度而调整 kernel/resource 设置。

### 完成条件

- 单元测试和隔离集成测试在有记录的 cgroup v2 宿主机上通过。
- 不存在 cgroup v1 兼容路径；记录限制条件和 kernel matrix。

## M3：守护进程与本地 API

**状态：** Not started。

### 功能目标

- 实现 `mydockerd`、版本化 UDS API、CLI 客户端、持久元数据、operations、
  events/logs 及守护进程协调。
- 让守护进程成为后台生命周期的权威组件。
- 使用显式的精简 provider，在 M1/M2 原语之上实现初始 Sandbox/Attempt 编排：
  预先准备的测试 rootfs、`network=none`/loopback，以及由守护进程启动、用于持有
  Sandbox namespaces 的最小 keeper。M3 不声称具备 M4 的 snapshot/veth 路径，
  也不声称具备 M5 的可重连 supervisor。
- 为这个具名的精简 provider 加入首个可通过公共 API 运行的评测工具和生命周期基线
  定义（不是优化）。

### 正确性验收

- API 版本/错误/幂等性测试及 CLI 退出状态测试通过。
- CLI 退出后，后台工作负载继续正确运行，且守护进程/minimal keeper 拥有精简
  生命周期。
- 生命周期 operations 和状态查询与持久/观测 generations 一致。
- 结构化 logs/events 保留资源与 operation 的关联。

### 可靠性验收

- 守护进程在精简 provider 每个持久阶段重启时都能安全协调。M5 之前，对于重启前正在
  运行的工作负载，可以安全检测，并在当前 phase 上显式标记 `unknown` condition。
  `stopped` 要求确认进程不存在并遵守 outcome 策略；此阶段尚不声称能无缝重连
  supervisor。
- 部分请求或重复请求不会造成副作用重复。
- 原子元数据测试覆盖写入失败和无效 schema。

### 度量准备

- 评测工具记录调用方 spans、阶段事件、环境、失败及原始 samples。
- 建立带 `prepared-rootfs+loopback` 标签、可执行的 cold/warm 场景定义；不得与 M4
  完整路径直接比较。
- 为 Proposed 的低基数指标加入 label-cardinality 测试。

### 完成条件

- 首个非特权生命周期测试套件和隔离特权生命周期测试套件通过。
- 可复现的基线流程存在；不作性能提升声明。
- 文档说明哪些完整 Ready/Created 资源仍推迟到 M4/M5。

## M4：存储与网络

**状态：** Not started。

### 功能目标

- 实现镜像导入/content digest、layer store、每个 Attempt 的 OverlayFS snapshot，
  以及版本化存储元数据。
- 实现由 keeper 持有的 Sandbox network namespace、veth/bridge、本地并发 IPAM、
  routes、DNS inputs 和 port mappings。
- 将存储/网络所有权与生命周期回滚及恢复集成。

### 正确性验收

- digest、bundle/path、snapshot/mount、IP uniqueness、route 和 port 行为测试在受控
  环境中通过。
- 顺序 Attempts 保持 Sandbox 网络身份。
- 并发 IPAM 请求具有唯一性和幂等性。

### 可靠性验收

- 每个存储/网络失败阶段都有逆序回滚覆盖。
- 原子 IPAM/network/storage 状态能承受注入的写入/崩溃故障。
- unmount 失败不能通过暴露的宿主机 mount 导致递归删除。
- 守护进程重启后只协调经过所有者验证的资源。

### 度量准备

- 实现 copy-versus-OverlayFS、cold/warm snapshot、disk 和 native/OverlayFS
  工作负载场景。
- 实现 Sandbox 网络建立/并发/清理观测。
- 建立 cold/warm 启动 benchmark 输入和缓存定义。

### 完成条件

- 正确性/失败测试套件通过；benchmark 场景保留原始数据和环境清单，但尚不作为
  发布性能声明。
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

## C0：创建 `mydocker-cluster`

**状态：** Not started；该分支不存在。

### 功能目标

从明确且已经验证的 `mydocker-2.0` tag 或 commit 派生集群项目。

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

- 达到 M6 发布门槛（或经明确批准、证据等效的 alpha 门槛），只有此后才能创建
  `mydocker-cluster`。

## C1：控制平面基础

**状态：** Not started。

### 功能目标

实现集群 API 服务端、持久存储、controller、agent、Node/Task/Assignment 模型
和关联上下文。

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

确定性故障/重试测试套件跨控制平面和本地 API 边界通过。

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

M0 达到 Verified 后，只进入 M1：定义最小化的 Sandbox 和 Container Attempt 数据
模型、生命周期状态机、operation/event 模型、持久化边界，以及带单元测试的
`create/start/state/kill/delete` 契约。M1 只建立度量点语义，不进行
任何性能优化。
