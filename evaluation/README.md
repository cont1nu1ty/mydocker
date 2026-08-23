# 评测契约

**状态：** In progress。M3 已实现只通过版本化公共 API 工作的 `mydocker-eval`
harness、cold/warm prepared-rootfs 场景、调用方单调时钟跨度、daemon stage-event
关联、失败后清理尝试和逐行 JSONL 原始证据。当前仍没有真实 rootful benchmark
运行、结果、profile 或性能结论；纯 Go 单元测试不能替代这些证据。
生产 `LinuxShimLauncher` 仍返回 `ErrLauncherIncomplete`，使 `mydockerd` 在绑定 UDS
之前失败关闭；因此 harness 命令和已签入场景当前是已测试的接口/证据
生成契约，不是可立即执行的生产 benchmark 流程。

本文档是项目关于正确性、可靠性、测量和性能证据的主要契约。架构定义见
[`docs/architecture.md`](../docs/architecture.md)，功能验收条件见
[`docs/features/`](../docs/features/)。

## 1. 目的

未来 `evaluation/` 将承载：

- correctness 场景和集成测试；
- 可确定复现的故障与恢复场景；
- 持续 stress 和资源泄漏观察；
- 可复现的性能 benchmark；
- 用于解释已测瓶颈的 profiling 产物；
- 人类可读的摘要和机器可读的原始结果。

当前实现位于 [`cmd/mydocker-eval`](cmd/mydocker-eval)，已签入的严格输入位于
[`scenarios/`](scenarios/)。尚未产生真实实验时不创建或提交伪造的 `results/`、
workload 或 profile 产物；M1/M2/M3 的组件正确性测试仍位于对应 Go package。

证据形成顺序是：

```text
定义语义
-> 验证正确性
-> 建立可重复的 baseline
-> 检查方差
-> profile 已观察到的瓶颈
-> 形成假设
-> 进行一项受控修改
-> 重新运行相同 benchmark
-> 记录结论和局限
```

正确性和可靠性的优先级高于好看的性能数字。

### 当前 M3 harness 的用法与边界

在隔离的一次性 Linux/VM 中完成 runtime preflight、启动可用的 `mydockerd`，并将场景中
所有 `replace-*` 与 `unknown` 环境字段替换为本次实验的真实事实后，调用示例如下：
这些是未来 launcher 完成且 rootful 正确性验收通过后的前置条件；当前仓库尚不满足。

```text
go run ./evaluation/cmd/mydocker-eval \
  --socket /run/mydocker/mydockerd.sock \
  --scenario evaluation/scenarios/prepared-rootfs-loopback-cold.json \
  --experiment-id <stable-experiment-id> \
  --output <new-result.jsonl>
```

`--output` 文件以 `0600`、不可覆盖方式新建；每条记录立即编码，运行结束必须成功
同步文件和父目录并关闭两者，任一失败都会使命令失败。`--output -` 是非持久流，flush、关闭和
持久性由下游接收方负责。Harness 在执行任何生命周期副作用之前捕获 event stream
起点，完成后只分页读取该起点之后且属于本次 operation ID 的事件。
FileStore 默认只保留最近 `8192` 个 event；若样本读取返回 `resume_gap`，该样本缺少完整
阶段证据，必须作为失败原样保留，不能在同一样本中用空 token 重置后仍宣称测量完整。
空 token 仅适合显式开始新的观察窗口。

每个成功样本同时保留逐 API `caller_span` 和一个不可拆分的 E2E `caller_span`：

- cold 使用 `cold.create_sandbox_to_running`，从发送 `CreateSandbox` 前到
  `StartContainer` 响应明确投影 `Running`；
- warm 使用 `warm.create_container_to_running`，从已有 Ready Sandbox 中发送
  `CreateContainer` 前到明确投影 `Running`；
- E2E 记录的 `operation_ids` 按请求顺序关联组成该跨度的多个 durable operation；
  daemon stage-event duration 仍单独记录，不能与调用方时间相减或混为一项。

Sandbox/Container ID 和 create operation ID 都在第一次发送前产生。Create 或 Start
返回失败并不能证明服务端没有部分持久化，因此 harness 仍会用已知资源 ID 依次尝试
Kill/Delete/Stop；这些清理各自使用新的幂等 operation ID。若原 operation 尚 active，
清理可能返回冲突；该失败会保留在 JSONL 且命令非零退出，不能把它解释为清理完成。

每条记录携带同一份运行前环境快照：commit/build 设置、工作树状态和有界摘要、kernel、
cgroup v2 路径与 controllers、CPU、memory、结果文件所在 storage、显式 cache 声明、
concurrency、实验开始时间和 timezone。Harness 只读采集 Git/procfs/sysfs；无法读取或
无法可靠推断的字段写为 `unknown`，不会以空值冒充事实。Cache/content/snapshot 字段是
场景声明而非 harness 独立验证；签入场景中的 `unknown` 必须在正式 benchmark 前按真实
preflight 证据更新，否则该运行只能归类为调试记录。

## 2. 评测分类

| 分类 | 回答的问题 | 典型边界 | 不能替代 |
| --- | --- | --- | --- |
| Unit Test | 单个纯函数或小组件是否遵守契约？ | 进程内函数或 package | 内核集成或 E2E 行为 |
| Integration Test | 真实组件与 Linux 资源能否协同工作？ | 隔离主机上的公开/组件 API | 持续可靠性或性能 |
| Failure Test | 受控故障是否产生正确的 rollback、retry 和 recovery？ | 注入阶段到完成 reconciliation | 一般负载测试 |
| Stress Test | 重复或并发使用下是否保持稳定且有界？ | warm-up、baseline、持续操作、等待清理、最终观察 | 精确延迟比较 |
| Benchmark | 已定义场景有多快、占用多少资源？ | 明确的调用方可见或阶段指标 | 正确性证明 |
| Profiling | 已测时间或资源消耗发生在哪里？ | 启用 profiling 的固定 benchmark | 普通 benchmark 结果 |
| Observability Validation | event、log、metric、trace 是否保持承诺的语义？ | 已知操作到发出/查询到记录 | 受控 benchmark harness |

不得混用这些职责。Unit/Integration Test 断言语义结果；Benchmark 记录样本和
失败，但只能在场景正确性通过后执行。普通测试不得包含不稳定的毫秒级延迟门槛。

## 3. mydocker 指标定义

### 生命周期延迟

每个生命周期结果必须声明属于以下两个不可互换的计时族之一：

- **调用方可见 API 延迟：** 同一个 harness 进程使用单调时钟，从第一次请求
  发送前一刻开始，到收到响应或观察到关闭该边界的 Running 确认为止；
- **daemon 操作耗时：** daemon 使用自己的单调时钟，从请求被接受开始，到
  daemon 收到并确认目标状态为止。

下列边界图如果以“请求被接受”开始，描述的是 daemon 操作耗时。同一操作可以
用另一个指标名报告包含传输、排队和响应时间的调用方可见延迟。Daemon 阶段耗时
使用 daemon 时钟；不得用 supervisor 时间戳减去 daemon 或客户端时间戳。
Tracing 可以分解因果跨度，但不能消除时钟限制。

#### Sandbox 冷创建延迟

```text
CreateSandbox 请求被接受
-> Sandbox Ready 得到确认
```

包括父 cgroup、keeper/supervisor 准备、UTS/IPC/network namespace、
hostname/DNS 输入、网络配置、必要持久化和 Ready 验证。不包括 Container
bundle/rootfs 和用户进程启动。

#### Container 创建延迟

```text
CreateContainer 请求被接受
-> Container Attempt Created 得到确认
```

包括本地 `EnsureImage` 可用性检查、snapshot/rootfs 准备、嵌套 mount、子 cgroup、
init/start gate 准备和必要持久化。不包括 `ImportImage` 或可选 `PullImage` 等显式
acquisition operation，也不包括释放 start gate 以及确认用户 workload 进入 Running。

成功样本必须以已验证的 content blob 和 unpacked chain 可用为前置条件，并记录它们、
page cache 和 immutable-layer cache 的状态。内容缺失或损坏属于失败样本，必须保留对应
错误；不能把隐式导入/pull 混入 `CreateContainer` latency。

#### Container 启动延迟

```text
StartContainer 请求被接受
-> Container Running 得到确认
```

测量释放 start gate 并正向确认 Running。若响应在 workload 得到确认前就已写出，
该响应不能关闭样本边界。

#### 完整冷 workload 启动延迟

```text
请求新的 Sandbox
+ 请求新的 Container Attempt
-> workload Running 得到确认
```

该调用方可见 E2E 跨度由 harness 持有：在第一次发送 `CreateSandbox` 前一刻
开始，在同一进程收到或观察到 Running 确认时结束。Daemon 操作/阶段耗时单独
报告。结果必须声明已验证 content/unpacked chain、page cache、rootfs/snapshot 和网络
的 cold/warm 状态；`cold` 不能表示“镜像缺失”，也不能保持隐含。

#### Sandbox 内 warm Attempt 重启延迟

```text
已有 Ready Sandbox
+ 请求新的 Container Attempt
-> workload Running 得到确认
```

前一个 Attempt 已终止，并按场景完成清理。Harness 在发送新的
`CreateContainer` 前一刻开始，在观察到 Running 确认时停止。Sandbox network、
UTS/IPC namespace、父 cgroup 和 keeper 保持不变。Daemon 耗时单独报告；若不
说明边界差异，不得与完整冷启动直接比较。

### 生命周期吞吐

#### 完整 workload 生命周期吞吐

单位时间内完成且成功的以下完整生命周期数量：

```text
CreateSandbox
-> CreateContainer
-> StartContainer
-> Stop/Kill 并观察退出
-> DeleteContainer
-> Stop/RemoveSandbox
```

#### Attempt-only 生命周期吞吐

在预先创建的 Ready Sandbox 内，单位时间完成且成功的 Attempt 循环数量：

```text
CreateContainer
-> StartContainer
-> Stop/Kill 并观察退出
-> DeleteContainer
```

吞吐结果必须报告并发度、持续时间、成功操作、失败操作、重试策略和清理完成情况。
只有高请求速率但未完成清理，不能算作已完成生命周期吞吐。

### 资源开销

计划观察：

- daemon 基础 RSS 和其他已声明的内存指标；
- 每个 Sandbox supervisor/shim 的增量内存；
- 每个 Container Attempt 的增量内存；
- daemon 和 supervisor 的 open FD 数；
- goroutine 数；
- 自有 cgroup 数；
- 自有 mount 数；
- 自有 network namespace/interface/rule 数；
- zombie process 数；
- 持久和临时 metadata record 数。

单资源开销应使用多个资源规模，并报告推导方式（例如回归或 delta），不能只做一次
噪声很大的减法。必须明确内存指标（`RSS`、`PSS`、cgroup current 或其他定义）
和纳入哪些进程；同时记录 cache warm-up 和异步清理等待时间。

### 镜像与文件系统

#### 准备阶段指标

以下名称是 benchmark 原始结果 schema 中的逻辑指标，不是已承诺的
Prometheus metric 名。每个 duration 样本必须记录单位、计时族、精确边界、
operation ID 关联和 cold/warm 状态：

| 逻辑指标 | 精确边界或计算 | 默认证据渠道 |
| --- | --- | --- |
| `image_import_duration` | daemon 接受 `ImportImage` 到经校验的 content、unpacked chain 和 Image 记录原子可用；它是总操作耗时，其中的 digest/unpack 子阶段另报 | harness 收集 daemon 单调操作耗时；另报调用方 `image_import_api_latency` |
| `digest_verification_duration` | 开始流式读取 manifest/config/layer blob 并计算 descriptor digest，到大小/digest 全部确认；解压后 diffID 验证归入 unpack 阶段 | harness 收集同一 daemon 进程发出的阶段 duration |
| `layer_unpack_duration` | 从读取已验证的压缩 layer 到按顺序应用、校验 diffID 并原子发布不可变 unpacked chain | harness 收集 daemon 阶段 duration |
| `snapshot_prepare_duration` | `Snapshotter.Prepare` 开始到 lower/upper/work 资源与持久意图就绪；不包含真实 OverlayFS mount | harness 收集 daemon 阶段 duration |
| `overlay_mount_duration` | 发起受管 OverlayFS mount 到 mountinfo/所有权验证通过 | harness 收集 daemon 阶段 duration |
| `bundle_prepare_duration` | 已验证 rootfs mount 就绪到 bundle/config 原子发布并可供低层 runtime 消费 | harness 收集 daemon 阶段 duration |
| `content_dedup_ratio` | 对固定镜像集合计算 `1 - unique_stored_blob_bytes / logical_referenced_blob_bytes`；必须声明压缩/表观字节口径，逻辑字节为零时样本无效 | harness 在受控导入前后根据 content inventory 计算 |
| `unpacked_disk_usage` | 固定 unpacked chain 发布后的 apparent 与 allocated bytes；分别报告，并声明共享层去重口径 | harness 采集受管路径的资源 inventory |

`image_import_duration` 包含它的 digest verification 和 layer unpack 子阶段，
因此总操作与子阶段不得当作独立样本相加。Snapshot、mount 和 bundle 是
Container create 中的后续阶段，不得与 image import 隐式合并。Harness 不能用
客户端时钟减去 daemon 事件时间戳；它应当保留 daemon 在同一进程中
计算的 monotonic duration。

`cold`/`warm` 不能只记为一个模糊布尔值。镜像/文件系统样本至少分别记录
content blob 存在性、unpacked chain 存在性、snapshot 新建或同一 operation 幂等恢复、
mount 状态和 page-cache 策略。每个新 Attempt 必须创建独享的 writable snapshot、
merged mount 和 bundle；跨 Attempt 只能复用不可变 content/unpacked layers 与允许的
缓存。只有这些维度相同时，才能直接比较对应阶段。

#### Benchmark harness 与 Prometheus 的职责

| 渠道 | 职责 | 不应承载 |
| --- | --- | --- |
| Benchmark harness | 保留上述全部逻辑指标的逐样本原始值、失败、阶段关联、content/snapshot/cache 状态及资源 inventory；计算 dedup 与 disk usage | 在未同时保留场景、环境和原始样本时作出性能结论 |
| Prometheus | 运行期低基数的 operation total/error，以及经基数测试的粗粒度 duration histogram；label 只能使用有限的 operation/stage/outcome 集合 | 单样本 ID、image digest、path、完整 error、cache key、dedup inventory 或精确磁盘扫描 |

正式 benchmark 的规范证据源是 harness 原始结果，不是 Prometheus 抓取值。
Prometheus 可以验证线上可观测性和辅助定位异常，但不要求将上述每个阶段
和高基数上下文都转成常驻时序列。

#### 文件系统工作负载

计划测量：

- 完整 rootfs copy 准备延迟；
- OverlayFS snapshot 准备延迟；
- cold 与 warm snapshot 准备延迟；
- 按声明方法计算的 allocated/apparent disk usage；
- native 与 OverlayFS 顺序写；
- native 与 OverlayFS 随机写；
- native 与 OverlayFS metadata-heavy workload。

对比必须固定 source content digest、filesystem、mount option、storage device、
cache/drop 策略、workload、文件大小/数量、sync 策略、并发度和采样方法。
Page cache 状态和破坏性 cache-control 操作只能在隔离 benchmark 主机上执行，
并且需要明确授权。

### 可靠性

计划验证或测量：

- 依据已获取资源清单验证 rollback 正确性；
- 具有明确起止边界的 rollback duration；
- 等待既定异步时间后的 cleanup completeness；
- 重复生命周期下的资源增长；
- daemon restart reconciliation 结果和耗时；
- operation retry/idempotency 结果；
- exit outcome 保留情况；
- 重复副作用数量和 `unknown`/`orphan` condition 数量。

可靠性统计必须包含失败。若不同时报告 rollback 是否正确，单独的 rollback
duration 没有意义。

## 4. 未来 mycluster 指标（`mydocker-cluster` 分支）

在 cluster 分支存在前，所有 cluster 测量都保持 `Planned`。

Cluster 时钟契约：

- 组件内部的 queue、scheduler、controller 和 agent 耗时使用该组件自己的单调
  时钟，绝不减去另一个进程的时间戳；
- 当协议具有 acknowledgement 时，RPC/delivery round-trip 使用调用方单调时钟；
- 跨组件 E2E/阶段指标由单个实验 observer 提交 Task、观察具有因果标识的里程碑
  event，并用同一个单调时钟标记 event 到达时间；这些跨度包含观察/event delivery
  开销；
- 不得相减来自不同进程或节点的内嵌 wall-clock 时间戳。Distributed trace 用于
  因果分解；跨节点耗时归因必须有记录在案的时钟同步和误差方法。

### 调度和控制面

- scheduler throughput：单位时间持久化的决策数；
- scheduling queue latency：Task eligible/enqueued 到 scheduler evaluation 开始；
- scheduler execution P50/P95/P99：evaluation 开始到决策生成或持久化，必须声明
  精确终点；
- Assignment delivery latency：使用带 acknowledgement 的 distributor caller span，
  或单 observer 的 persisted-event 到 agent-observed-event span，并明确所选边界；
- reconciliation throughput 和重试次数；
- controller 与 scheduler CPU/memory；
- heartbeat/Lease CPU、带宽和 state-store 开销；
- 相同输入下 Spread 与 Bin-Packing 的利用率和决策成本。

### 端到端

```text
SubmitTask 被接受
-> desired state 已持久化
-> 已调度
-> Assignment 已送达
-> agent 开始 reconcile
-> image digest 可用（预加载检查或可选 acquisition）
-> Sandbox Ready
-> snapshot/rootfs/bundle 已准备
-> Container Running
-> controller 观察到 Running 状态
```

端到端评测工具遵循上述单一观察者规则：从发送 `Submit` 前一刻到
观察到相关 `Running` 状态。必须同时报告观察者跨度和组件内部耗时，
绝不能相减远端时间戳。control-plane、image acquisition、snapshot preparation 和
runtime startup 延迟必须分别报告；节点本地阶段也必须与调度、状态存储/RPC、投递
和状态观测延迟分开。SubmitTask-to-Running 可以保留为总边界，但不能把可选 image
pull 隐藏在这一个不透明数字中。

### 故障与恢复

- 故障检测延迟：同一观察者从故障注入到观察到检测事件，
  或使用已命名的组件内部边界；
- 重新调度延迟：检测/决策边界到替换 Assignment；
- 节点本地替换启动延迟；
- 总恢复时间/MTTR：故障边界到已声明的恢复观察点；
- 重复存活的 Task/Attempt 数；
- 期望/实际状态不一致的数量和持续时间；
- 每种故障场景下的 Task 成功率。

每次 cluster 实验都必须记录 mydocker runtime base commit。Simulated-agent 结果要
明确标记，并与真实节点 E2E 结果分开。

## 5. 标准场景

当前签入并通过纯 fake correctness 测试的输入是
`prepared-rootfs-loopback-cold.json` 与 `prepared-rootfs-loopback-warm.json`；
这表示场景/harness 代码存在，不表示真实 kernel 场景已运行或测量。下列名称及其
数字后缀仍是未来配置，不是已完成结果。

### mydocker

```text
cold-sandbox-start
warm-attempt-restart
oci-layout-import
layer-unpack-cold-warm
content-dedup
snapshot-stage-breakdown
concurrent-start-1
concurrent-start-10
concurrent-start-50
concurrent-start-100
lifecycle-stress-100
lifecycle-stress-1000
lifecycle-stress-10000
overlayfs-vs-copy
native-vs-overlayfs
rollback-failure-matrix
daemon-restart-recovery
```

每个已实现场景都要定义前置条件、公开 API 顺序、workload、成功条件、清理条件、
timeout 语义、并发度、cold/warm 状态和记录指标。数字后缀只是请求的迭代/并发
配置，不证明项目已完成该规模测试。

### 未来 mycluster（`mydocker-cluster` 分支）

```text
scheduler-10-nodes
scheduler-100-nodes
scheduler-500-simulated-nodes
tasks-100
tasks-1000
tasks-10000
spread-vs-binpack
agent-crash
controller-restart
rpc-delay
packet-loss
duplicate-rpc
```

Cluster 场景还必须定义 simulated/real node、Task arrival distribution、resource
distribution、scheduler policy、Lease 配置、故障时机和收敛条件。

## 6. 实验环境清单

每次正式 benchmark 必须记录：

```text
experiment_id
scenario name and version
git branch
git commit SHA
runtime base commit (cluster experiments)
dirty worktree status and diff reference when dirty
Go version
build flags, tags, CGO/race/profiling settings
Linux distribution
kernel version and boot parameters relevant to the scenario
CPU model, topology, core count, frequency/governor policy
memory capacity and relevant limits
filesystem type and mount options
storage device/model and relevant cache policy
cgroup mode, delegated root and enabled controllers
network mode, NIC/virtualization and relevant firewall backend
rootfs/image digest
OCI Image Layout digest/checksum and source byte accounting
content present/missing state and unpacked-chain state
snapshot/mount reuse state and page-cache state
Sandbox configuration
Container/Task resource requests and limits
daemon, supervisor and cluster configuration
concurrency
sample count
warm-up count and duration
cold/warm definition and cache state
profiling enabled or disabled
timing family and clock/observation source
background workload/noise controls
start time and timezone
raw result location and checksum/reference
```

清单必须记录对主机做过的实质修改以及如何恢复。缺少关键环境字段的运行只能降级为
调试记录，不能支持正式性能结论。

## 7. 结果格式与状态

当前 M3 harness 先生成不可覆盖的逐行 JSONL 原始观察；每次正式实验还必须保存：

```text
人类可读摘要（README.md 或 summary.md）
+
机器可读原始观察（当前为 result.jsonl）
+
环境清单或其引用
```

原始观察必须保留成功、失败、重试、timeout 和样本顺序；摘要引用原始数据，不能
替代原始数据。

结果至少包含：

- 实验、场景和环境标识；
- commit SHA 和工作树脏状态；
- 指标名、采集渠道（harness、daemon stage event 或 Prometheus）、计时族、
  精确边界/时钟、单位和样本数；
- warm-up 样本排除规则；
- 最小值/最大值和适用的中心统计量；
- 当样本量和指标适用时的 P50/P95/P99；
- 错误、失败数/失败率和重试数；
- 带解释的离群值处理；
- 结论、局限和是否具备可比性。

不得创建虚假样例结果。Roadmap 跟踪只使用 `Not started`、`In progress`、
`Implemented`、`Measured` 和 `Verified`；设计文本可以使用 `Proposed` 或
`Planned`。`Implemented`、`Measured` 和 `Verified` 不是同义词。

## 8. Benchmark 工作流

```text
1. 定义有版本的场景和指标边界。
2. 验证该场景的语义正确性和清理行为。
3. 记录完整环境清单。
4. 按场景执行 warm-up。
5. 建立重复 baseline 并保留原始观察。
6. 检查方差、失败和环境噪声。
7. 只 profile 已证实的瓶颈。
8. 形成一个可证伪的优化假设。
9. 进行一项受控修改。
10. 在相同条件下重新构建并运行相同场景。
11. 比较 effect size、分布、错误和资源行为。
12. 记录结论、无结果和局限。
```

若优化过程中正确性发生变化，接受 benchmark 数据前必须重新验证。更快但错误或
泄漏的生命周期属于回归。

## 9. 性能对比规则

Before/after 运行应尽可能固定：

- machine/VM placement 和 host contention；
- kernel 及 boot/runtime 配置；
- Go version、dependency、build flag、tag 和 binary mode；
- image/rootfs digest 和 filesystem/storage 设置；
- workload、输入数据、生命周期顺序和正确性检查；
- daemon/Sandbox/resource/network 配置；
- concurrency、sample count、warm-up 和 cold/warm 定义；
- profiling/race/debug 设置；
- observation 工具和指标计算版本。

安全可行时，应通过交错或重复 block 等顺序减少时间偏差。若某字段无法固定，必须
列出差异，并避免把变化仅归因于代码。不同分支或环境的数字不同，不能自动称为性能
提升。

在可比较的原始数据和计算可用前，不得报告优化百分比。若使用 confidence 或
uncertainty 方法，必须说明方法。

## 10. 噪声与统计原则

- 不得从单个样本得出正式结论。
- warm-up 观察与正式样本分开。
- 明确区分 cold 和 warm 场景。
- 适用时报告分布或分位数，而不只报告平均值。
- 保留并统计失败或 timeout 样本。
- 没有预先定义并解释的规则及 raw/filtered 双视图，不得删除不利离群值。
- 方差变化时检查机器负载、频率变化、storage/network 噪声、GC 和后台活动。
- 可能存在顺序效应时，随机化或交替安排对比顺序。
- 根据稳定性或不确定性选择 sample count 和 duration，而不是根据期望结果选择。
- 只有稳定受控环境和历史方差存在后，才能定义性能回归门槛。
- 初期普通 CI 不使用易抖动的 wall-clock threshold 作为 hard gate。

## 11. 性能剖析（Profiling）

未来可能使用：

```text
Go pprof
Linux perf
strace
/proc observations
/usr/bin/time or equivalent resource accounting
```

工具可用不代表可以在普通主机上使用。`perf`、`strace`、namespace/mount/cgroup
检查和 workload profiling 都必须遵循环境 preflight 与数据/安全规则。

Benchmark 先于 profiling。Profile 必须记录精确 benchmark 场景、commit、binary、
duration、sampling 配置和开销。启用 profiling 的 latency/throughput 不得与普通
build 直接比较。Profile 用来支持解释；只有重新运行普通 benchmark 后，才能证明
改动产生了提升。

默认不提交大型原始 profile。审查敏感路径和数据后，只提交必要命令/配置、小型必要
产物、摘要和可复现说明。

## 12. 资源泄漏 Stress 方法

标准方法：

```text
preflight 并清理已知测试资源
-> warm up 以初始化有界 cache
-> 记录 baseline inventory
-> 执行重复或并发生命周期操作
-> 验证操作结果
-> 等待已声明时间以完成异步清理
-> 只触发已记录且安全的 reconciliation/GC 动作
-> 记录最终 inventory
-> 比较趋势并检查自有残留资源
```

Inventory 包括：

- mount 和 mount namespace；
- veth/interface、route、firewall rule 和 IP allocation；
- cgroup 和 populated 状态；
- daemon/supervisor/workload process 和 zombie；
- open FD 和 goroutine；
- daemon/supervisor RSS 或其他已声明内存指标；
- 持久 operation/Sandbox/Attempt/snapshot record；
- keeper/shim process 数。

不得简单要求所有计数立即回到零增长。必须分类：

- 有意保留的有界 cache/pool；
- 已记录且最终收敛的延迟清理；
- 具有保留策略的有界历史；
- 与泄漏一致的持续增长或可确认归属的残留。

应随迭代次数或时间设置多个检查点。只有最终 delta 无法区分 warm-up 与持续泄漏。

## 13. 故障注入

故障必须确定、可复现、可关闭、限定到已命名实验，并与正常生产默认路径隔离。
优先使用 interface wrapper、test double 或明确的 test-only build。记录注入点、
触发次数、预期效果以及故障是否实际发生。

### mydocker 故障集合

- rootfs/content/snapshot 配置失败；
- mount 或 bundle 持久化失败；
- network/IPAM/link/route/firewall 配置失败；
- cgroup 创建、控制或挂接失败；
- child/init 进程创建或 start gate 失败；
- 进程收到 `SIGKILL` 或 supervisor 丢失；
- daemon 在每个持久化阶段崩溃；
- metadata 部分写入或写入失败；
- 响应丢失和重复生命周期请求；
- teardown/rollback 逆操作失败。

每个场景都要验证 phase、durable intent、返回/可观察 error、rollback 顺序、残留
资源、restart reconciliation 和 retry outcome。

### 未来 cluster 故障集合

- agent 崩溃和重启；
- controller/scheduler 重启；
- RPC 超时、响应丢失和重复 RPC；
- 丢包、延迟和临时节点网络分区；
- Lease 过期/节点故障以及旧节点返回；
- 过期的 Assignment/status generation。

Cluster 故障必须分别报告检测、重新调度、节点本地启动、controller
观测、总恢复时间、重复存活工作负载和不一致状态。

## 14. 简历与外部表述规则

只有同时满足以下条件，指标才能用于简历、release 或公开性能表述：

- 功能已实现，且 correctness 场景通过；
- benchmark 场景可重复且有版本；
- environment、commit、build、输入和配置均已记录；
- 机器可读原始观察仍可访问；
- before/after 条件可比，或差异已披露；
- profile/hypothesis 能解释受控改动为何应影响该指标；
- profiling 后的普通 benchmark 确认了效果；
- failure behavior、variance 和 limitation 均已报告。

不得预填 `[X]`、`[Y]` 或 `[Z%]` 等占位符。没有与明确范围相匹配的证据时，
不得把项目描述为高性能、低延迟、生产级、高可用或零泄漏。测量出现前，只能说明
指标或场景处于 `Proposed`/`Planned`。
