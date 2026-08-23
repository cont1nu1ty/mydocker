# 镜像消费与文件系统

## 状态

**Proposed。** M0 规定对象、组件与评测边界；2.0 尚无镜像或文件系统实现。

## 目的

mydocker 是单节点容器执行引擎。它接收外部 builder 产出的 OCI Image，把已验证的
镜像内容转换为单个 Container Attempt 可使用的 rootfs 和 bundle，再交给
`mydocker-runtime` 创建 Linux 进程。

本功能负责把“镜像可用”与“Attempt 文件系统已准备”变成可持久恢复、可幂等重试、
可逆序清理且可分阶段度量的事实。

## 范围与非目标

首版范围包括：

- OCI Image Layout 导入，以及 reference 到不可变 manifest digest 的解析；
- manifest、config 和压缩 filesystem layer 的摘要/大小校验；
- 内容寻址 blob store 和相同 blob 去重；
- layer 解压、diffID 校验、有序应用及 OCI whiteout 语义；
- 不可变 image filesystem 和每个 Attempt 独享的 OverlayFS writable snapshot；
- OverlayFS、bind、`/proc`、tmpfs、read-only rootfs 与 mount propagation；
- 由 image config、ContainerSpec 覆盖和节点本地配置生成带版本 bundle；
- 显式镜像查询/删除、失败回滚、daemon 重启恢复以及文件系统评测。

首版不包括 registry 分发、自动 snapshot/content 垃圾回收、通用 snapshotter 插件
系统、多架构 image index、镜像签名、SBOM 或 lazy pull。未来可以增加按 digest 的
registry pull，但它不是初始可用性语义。

以下能力明确不属于 mydocker 2.0 核心范围，也不规划对应 API：

- `BuildImage`：Dockerfile frontend、build graph、build context、layer 生成和构建缓存；
- `CommitContainer`：把运行中容器 writable snapshot 转换为新 OCI image；
- `PushImage` 或 registry server。

若以后研究 writable snapshot 导出，应作为 M6 之后独立的 **Optional Storage
Experiment**，先定义冻结/一致性、OCI whiteout 转换、volume 排除、崩溃清理和引用
关系；它不能成为 2.0 的完成条件。

## 镜像到进程的数据路径

```text
外部 Builder
-> OCI Image Layout
-> ImageService
-> ContentStore
-> LayerUnpacker
-> Snapshotter
-> MountManager
-> BundleBuilder
-> mydocker-runtime
-> Linux process
```

这里有两条相关但不同的操作路径：

```text
ImportImage
-> 解析 layout/index/manifest
-> 原子接收并验证 config/layer blobs
-> 按 layer 顺序准备不可变 image filesystem
-> 原子发布 Image 记录

CreateContainer
-> EnsureImage(manifest digest)
-> Prepare per-Attempt snapshot
-> 获取并应用 MountSpecs
-> 挂载 rootfs 和嵌套 mounts
-> 构建带版本 bundle
-> mydocker-runtime create（进程仍受 start gate 约束）
```

`ImportImage` 与 `CreateContainer` 使用各自的 operation ID 和阶段事件。导入成功不
创建 Sandbox/Container；创建 Attempt 也不会隐式修改镜像 reference。

## 核心对象

| 对象 | 身份与语义 |
| --- | --- |
| Image reference | 用户可读的 name/tag；它是可变映射，不能作为已创建 Attempt 的执行身份 |
| Image | 用户和 engine 可见的记录，包含 reference、manifest digest、config digest、有序 layer descriptors 和可用状态 |
| Manifest digest | 解析后持久化到 Container/Attempt spec 的不可变镜像身份 |
| Content descriptor | digest、size 和 media type；描述一个预期 blob，而不是证明 blob 已完整落盘 |
| Content blob | ContentStore 中经摘要和大小校验、按 digest 寻址的不可变字节 |
| Layer digest | 压缩 layer blob 的内容身份；必须与 descriptor 匹配 |
| diffID / chain identity | 解压后 layer 的摘要，以及由有序父子 diffID 派生的文件系统链身份 |
| Unpacked layer | 已安全解压并实现 OCI layer/whiteout 语义后原子发布的不可变结果 |
| Snapshot | 一个 immutable parent chain 加一个由单个 Attempt 独享的 upper/work 状态 |
| Rootfs | Snapshot 经真实 mount 后呈现给该 Attempt 的合并文件系统视图 |
| Mount | 内核中的挂接事实；必须有 owner、target、依赖顺序和可观测状态 |
| Bundle | image config、ContainerSpec 覆盖、rootfs、资源与 namespace 配置的带版本 runtime 输入 |

相同 manifest digest 可以被多个 reference 指向，也可以被多个 Attempt 复用其不可变
内容与 unpacked layers；任何 Attempt 的 writable layer、merged mount 和 bundle 都不得
被另一 Attempt 隐式复用。

## 组件与 API 契约

所有修改状态的操作都接收 operation context，并使用规范请求 fingerprint 约束重试。
重复请求只能恢复或返回相同意图的结果；同一 operation ID 携带不同输入必须被拒绝。

### ImageService

ImageService 是用户、CLI、engine 和未来 node agent 看到的唯一镜像对象边界。它拥有
reference 映射、Image 元数据和可用状态，协调 ContentStore 与 LayerUnpacker，但不
直接执行 mount、构建 bundle 或启动进程。

首版公开操作：

| 操作 | 输入 | 成功后的保证 |
| --- | --- | --- |
| `ImportImage` | 归本系统允许读取的 OCI Image Layout、可选 expected digest/reference | manifest/config/layers 全部校验并完成所需 unpack；Image 记录最后原子发布 |
| `EnsureImage` | manifest digest | 对应 Image、全部必需 blob 和 immutable unpacked chain 已验证可用于 `Snapshotter.Prepare` |
| `GetImage` | reference 或 digest | 返回解析后的 digest、descriptor、引用和 content/unpack 完整状态，不产生获取副作用 |
| `ListImages` | 有界过滤与分页条件 | 返回稳定排序的 Image 元数据和可用状态，不扫描未受管目录来臆造记录 |
| `RemoveImage` | reference 或 digest 及适用的并发前置条件 | 删除目标映射/记录；仍被 reference、导入 operation 或 snapshot 使用时拒绝破坏性删除 |

初始 `EnsureImage` 是只针对 digest 的本地可用性检查，不执行网络获取。缺失、不完整或
摘要不匹配返回类型化 `ImageUnavailable`/`ContentInvalid`，且不得继续创建 Snapshot。
未来可选的 `PullImage(digest)` 必须是显式 acquisition operation；不能让 mutable tag
在重试时解析成不同内容。

重新导入相同已验证 digest 必须复用内容且安全成功。reference 指向变化是显式、原子
的元数据更新，不能回写已创建 Attempt 的 digest。按 reference 删除只删除映射；按
digest 删除 Image 记录前必须证明没有引用或 snapshot 使用它。首版没有自动 GC，因而
`RemoveImage` 不得顺便递归猜测并删除可能共享的 blob/unpacked layer。

### ContentStore

ContentStore 只管理 `digest -> immutable blob`。它不理解 tag、layer 顺序、Snapshot、
Task 或 Container 生命周期。

最小内部接口：

| 操作 | 契约 |
| --- | --- |
| `Put(descriptor, reader)` | 写入本系统 staging 文件，同时校验 size/digest；同步并原子发布，已存在的完整同 digest blob 直接复用 |
| `Open(digest)` | 只打开已经发布且标记完整的 blob；staging/截断内容不可见 |
| `Stat(digest)` | 返回 size、media type、完整性及受管路径信息，不把“文件存在”等同于“已验证” |
| `Delete(digest)` | 仅在调用方已通过引用前置条件且能证明所有权时显式删除；首版不进行可达性 GC |

manifest、config 和压缩 layers 都作为 blob 管理。media type/size 属于 descriptor
校验的一部分；不支持的 digest algorithm、media type 或压缩格式必须显式拒绝。
任何原子发布都遵循临时写入、文件同步、原子重命名和目录同步要求；不完整元数据或
截断 JSON 不能成为唯一的期望状态。

### LayerUnpacker

LayerUnpacker 把已验证的压缩 layer blob 变成不可变文件系统链。它不修改 Image
reference，不创建 Attempt upperdir，不执行真实 mount，也不生成 runtime bundle。

最小内部接口：

| 操作 | 契约 |
| --- | --- |
| `Unpack(parentChainID, descriptor, expectedDiffID)` | 从 ContentStore 读取 blob，校验解压流 diffID，基于 parent 顺序应用 layer，原子发布新的 immutable chain |
| `Get(chainID)` | 只返回已经完整发布、所有权可验证的 unpacked 结果 |
| `Remove(chainID)` | 仅在没有子 chain、Image 或 Snapshot 引用时显式删除；不承担自动 GC |

解包必须：

- 处理普通 whiteout 与 opaque directory，使最终有序文件系统视图符合声明的 OCI
  layer 语义；具体落盘表示可以实现相关，但 observable 结果不能改变；
- 对每个 tar entry 规范化相对路径，拒绝绝对路径、`..`、symlink/hardlink 越界和
  任何会写出受管根目录的操作；
- 保存受支持的 mode、UID/GID、symlink、hardlink 和必要 xattr；遇到未支持但会改变
  文件系统语义的类型/属性时失败，而不是静默丢弃；
- 只通过结构化文件系统 API 解包，不拼装 shell 命令；
- 写入受管 staging 目录，失败时只清理本次 operation 获取的路径，完成验证后才以
  immutable 状态发布。

### Snapshotter

Snapshotter 管理 image filesystem 与 per-Attempt writable state 的关系。它理解
snapshot key、parent chain、upper/work 和 mount specification，但不理解 image tag、
Task、scheduler、Sandbox 网络或用户 CLI，也不执行 bundle/runtime 操作。

首版接口有意保持最小：

| 操作 | 契约 |
| --- | --- |
| `Prepare(snapshotKey, parentChainID, attemptID)` | 为唯一 Attempt 原子保留 snapshot 身份并准备独享 upper/work；同一规范重试幂等 |
| `Mounts(snapshotKey)` | 返回结构化 `MountSpec`（lowerdirs、upperdir、workdir、options），不把它拼为 shell 字符串 |
| `Usage(snapshotKey)` | 按声明方法报告 writable/allocated usage；不得把共享 lower layer 全量重复计入每个 Attempt |
| `Remove(snapshotKey)` | 仅在所有真实 mounts 已消失后移除 snapshot 数据；busy/未知 owner 时保持可恢复状态 |

首版不复制 containerd 的通用 plugin API，也不提供把 writable snapshot 转成镜像的
`Commit`。Snapshot 元数据必须保存 Attempt owner、parent chain、目录身份和期望状态，
但一条路径或裸 ID 本身不是宿主机资源所有权证明。

### MountManager

MountManager 执行实际 mount/unmount，并维护 owner、target 与依赖顺序。它不解析
镜像、不创建 snapshot 元数据，也不决定 Container 状态转换。

最小内部接口：

| 操作 | 契约 |
| --- | --- |
| `MountRootfs(owner, target, specs)` | 校验受管 target/路径后应用 OverlayFS 等 rootfs mounts，并验证结果 |
| `ApplyNested(owner, rootfs, mounts)` | 依赖有序地应用 bind、`/proc`、tmpfs、read-only 和 propagation 配置 |
| `UnmountAll(owner)` | 先拆嵌套 mounts，再拆 rootfs，逐项记录结果；重复执行安全 |
| `Inspect(owner)` | 对照 mountinfo 和持久意图返回可证明归属、缺失、busy 或 unknown 状态 |

所有 mount 使用 syscall 或结构化参数。target、lower、upper、work 和 bind source 必须
满足各自的路径/所有权策略；破坏性操作前拒绝路径遍历或 symlink 逃逸。unmount 失败
时禁止递归删除仍可能暴露主机或 volume 数据的目录，也不能用 lazy unmount 掩盖未知
使用者。daemon 重启后只接管能由持久 owner 元数据和宿主机观测共同证明的 mount。

### BundleBuilder

BundleBuilder 将已经准备好的执行输入转换为 `mydocker-runtime` 能消费的 bundle。它
不获取/解包镜像，不执行 mount，不创建 namespace/cgroup，也不启动进程。

最小内部接口：

| 操作 | 契约 |
| --- | --- |
| `Build(input)` | 合并 image config、ContainerSpec 显式覆盖、rootfs mount、资源/namespace/cgroup 输入，校验后原子发布每个 Attempt 独享的带版本 bundle |
| `Validate(bundle)` | 拒绝未知 schema、不安全路径、非法 argv/environment、矛盾 mount 或缺失 rootfs 引用 |
| `Remove(attemptID)` | 仅删除归该 Attempt 所有且 runtime 已不再使用的 bundle；重复执行安全 |

image config 提供默认值，ContainerSpec 中明确存在的字段按已定义规则覆盖；argv 和
environment 始终保持结构化，不能经 shell 字符串拼接再解析。Sandbox 所有的 UTS、
IPC/network namespace 输入和 Attempt 所有的 PID/mount namespace、子 cgroup path 由
engine 提供，而不是从镜像推导。

`mydocker-runtime` 最终只接收 bundle、rootfs、process args、mounts、namespaces 和
cgroup path。`ubuntu:24.04`、registry、tag、layer 下载和 image cache 永远不能穿透
这一边界。

## 主机数据边界

计划布局如下，具体名称仍是 **Proposed**：

```text
/var/lib/mydocker/
├── images/         durable references, descriptors and availability records
├── content/        durable digest-addressed blobs and completeness metadata
├── unpacked/       durable immutable unpacked layer/chain data
├── snapshots/      durable snapshot metadata and per-Attempt writable data
└── containers/     versioned Container/Attempt intent and bundle metadata

/run/mydocker/
├── mounts/         transient owned rootfs targets and mount coordination
└── bundles/        reconstructible realized runtime bundles
```

持久记录携带 schema version。`/run/mydocker` 可以在主机重启后消失，因此 realized
bundle、mount target 和协调文件必须能从持久意图与安全的宿主机观测重建。大体积内容、
日志、secrets、bind source 和 volume 数据需要各自明确的所有权与保留策略；它们不能
因为位于某个相邻目录就被当作 snapshot 数据删除。

## 故障、回滚与恢复

准备顺序中的每个步骤先记录足够的 intent/owner，再注册逆操作。CreateContainer
失败时按以下依赖逆序清理本次 operation 获取的资源：

```text
停止/删除未运行的 runtime init
-> 删除 bundle
-> unmount nested mounts
-> unmount rootfs
-> Snapshotter.Remove
```

共享 Image、Content 和 Unpacked Layer 不属于 Attempt 回滚栈。导入失败只清理该导入
创建且尚未发布的 staging 数据；已经完整发布的去重 blob 可以保留，但不得产生一个
虚假的 Available Image 记录。原始错误和每个回滚错误分别输出。

若 unmount 失败，Snapshot 保持可恢复并阻止目录删除。若 daemon 在任一边界崩溃，
恢复流程将持久 Image/Snapshot/Attempt 意图与 content 完整性、受管路径、mountinfo、
runtime/supervisor 状态进行比较，再决定继续、回滚或附加失败/清理 condition。未知的
主机目录或 mount 只报告，不接管、不删除。

首版没有自动 GC。只有显式 API、引用检查和所有权验证共同满足时才允许删除持久
内容；“当前目录扫描未发现引用”不能替代原子引用状态。

## 可观测性与评测点

一次 image/Attempt operation 按适用情况记录以下有序阶段：

```text
image_import
digest_verification
layer_unpack
snapshot_prepare
overlay_mount
bundle_prepare
```

具体资源 ID、image digest 和详细错误进入 structured logs/traces，不能成为
Prometheus labels。大部分导入/文件系统成本由 benchmark harness 采集，不能为了展示
数字把每个高基数样本都转成常驻指标。

评测场景包括：

- OCI layout 导入、摘要校验和 layer unpack 的独立 duration；
- cold snapshot 准备与复用已存在且已验证层的 warm 准备；
- 完整 rootfs copy 与 OverlayFS snapshot 的准备延迟和持久/allocated 磁盘用量；
- 原生文件系统与 OverlayFS 的顺序写、随机写和 metadata-intensive 工作负载；
- `content_dedup_ratio` 与 `unpacked_disk_usage`，并明确逻辑字节、物理字节和共享层
  的计算方法；
- content-read、digest/diffID、unpack、snapshot、mount、bundle/state 发布等确定性
  故障边界下的回滚与重启恢复。

文件系统对比必须固定机器、kernel、文件系统、存储设备、mount options、镜像 digest、
工作负载、cache/cold 定义、样本数、build 与 profiling 状态。微基准不能证明端到端
容器启动性能；准确契约和原始结果元数据见
[evaluation/README.md](../../evaluation/README.md)。

## 未来集群兼容性

Cluster MVP 要求镜像预先通过 OCI Image Layout 导入到每个候选节点：

```text
TaskSpec(image_digest)
-> Scheduler 选择 Node
-> Assignment
-> Node Agent
-> mydockerd.EnsureImage(image_digest)
-> CreateSandbox
-> CreateContainer
```

若 `EnsureImage` 失败，agent 报告 `ImageUnavailable`，不得自己读取 ContentStore、解包
layer 或 mount rootfs。后续可以增加显式 `PullImage(digest)`；届时 control-plane、image
acquisition、snapshot preparation 和 runtime startup latency 分开度量。Image locality
可以成为后期可选 placement signal，但不改变 mydockerd 对节点内容与 snapshot 生命周期
的权威性。

## 验收条件

仅在满足以下条件时，此功能才达到 **Verified** 状态：

- `ImportImage`、`EnsureImage`、`GetImage`、`ListImages` 和 `RemoveImage` 的成功、重试、
  冲突和缺失语义都有测试；
- OCI layout/schema、descriptor、digest、size、diffID 和不支持的 media type 会被校验；
- layer 顺序、whiteout、opaque directory 和路径/链接逃逸具有正确性与恶意输入测试；
- 相同 blob 并发导入只产生一个完整内容，截断/崩溃边界不会发布可用 Image；
- 每个 Attempt 的 upper/work、merged mount 和 bundle 都有唯一且可验证的 owner；
- snapshot/mount/bundle 任一阶段部分失败时按逆序回滚，且不会删除暴露的主机数据；
- daemon 重启能够安全协调归本系统所有的 content、unpack、snapshot、mount 和 bundle；
- cold/warm、copy/OverlayFS、磁盘用量和文件系统 benchmark 可以产生可复现原始结果；
- 公共 API 中不存在 `BuildImage`、`CommitContainer` 或 `PushImage`，且没有隐式 registry
  获取路径。

## 未决问题

- 首版支持的 OCI Image Layout/index、manifest、media type、压缩和 digest algorithm
  子集。
- unpacked chain 的身份和落盘表示；whiteout/opaque 的 observable 语义是必做项，
  其内部表示以及可选 xattr/device node 的支持边界仍需确定。
- Image/Content/Snapshot 元数据事务、引用计数与后续显式删除/垃圾回收策略。
- Bundle schema 与最低限度 OCI Runtime Specification 借鉴范围；不据此声称兼容。
- bind mount/volume/secrets 的来源许可、只读默认值与保留语义。
- 未来是否以及何时增加只按 digest 的 registry pull。
