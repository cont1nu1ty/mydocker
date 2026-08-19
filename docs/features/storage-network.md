# 存储与 Sandbox 网络

## 状态

**Proposed。** M0 规定所有权边界和评测边界；2.0 尚无存储或网络实现。

## 目的

为每个 Container Attempt 提供可复现的 rootfs/快照准备流程，并为每个
Sandbox 提供稳定且可恢复的网络身份。两个子系统都必须支持原子状态、幂等拆除、
逆序回滚以及可度量的阶段边界。

## 范围

存储范围包括受 OCI 启发的 bundle、镜像引用和已验证的摘要、内容/层存储、
OverlayFS 快照、每个 Attempt 独享的可写层、带版本的
Sandbox/Container 元数据，以及持久目录与运行时目录的边界。

网络范围包括由 Sandbox 所有的 network namespace、veth 对、Linux bridge、
本地 IPAM、路由、端口映射、并发分配安全性以及拆除流程。

初始范围不包括 registry 分发、镜像构建、快照垃圾回收、CNI 兼容性、
多节点 VXLAN/overlay 网络、节点 CIDR 分配以及服务负载均衡。

## 核心对象与流程

### 存储对象

- 镜像引用表示用户意图，可能发生变化。
- 解析后的镜像摘要是不可变的内容身份，与 Attempt 规约一同持久化。
- 内容/层存储拥有已验证的 blob 和已解包的不可变层。
- 快照标识一个不可变的下层只读视图，以及一个由单个 Attempt 独享的可写层。
- 受 OCI 启发的 bundle 包含供底层运行时使用的带版本配置和 rootfs 引用；
  “受启发”并不表示完全符合 OCI Runtime Specification。
- Sandbox 元数据记录稳定的环境资源；Container/Attempt 元数据记录 bundle、
  快照、进程和结果资源。

计划的准备流程：

```text
resolve image reference to digest
-> verify/import content
-> ensure immutable layers are available
-> create Attempt snapshot (lower/upper/work)
-> mount OverlayFS at an owned target
-> apply nested mounts in dependency order
-> build versioned bundle
-> persist prepared state atomically
```

可写层属于单个 Attempt，绝不会被后续 Attempt 隐式复用。未来显式的
checkpoint/commit 功能需要定义自己的数据一致性模型。

### 主机数据边界

```text
/var/lib/mydocker/
├── content/        durable digest-addressed content
├── snapshots/      durable snapshot metadata and writable data
├── sandboxes/      versioned Sandbox records
├── containers/     versioned Container/Attempt records
└── network/        durable allocation and attachment intent

/run/mydocker/
├── mydockerd.sock  boot-scoped API socket
├── mounts/         transient owned mount targets/coordination
└── sandboxes/      transient keeper/supervisor handles
```

这些名称处于 **Proposed** 状态。持久记录携带 schema 版本。更新操作根据所选
文件系统的要求，使用存储事务，或采用临时写入、文件同步、原子重命名和目录同步。
截断或不完整的 JSON 文件不得成为期望状态的唯一副本。

初期不实现垃圾回收。在可达性、租约和崩溃恢复得到定义与测试之前，删除必须
显式执行，并由生命周期管理逻辑负责。

### 网络对象

Sandbox 拥有：

- 由 keeper/supervisor（而非用户进程 PID）持有的 network namespace；
- 一个或多个网络接入记录（初始阶段支持一个接入点即可）；
- 已分配 IP、路由、DNS 输入、bridge/veth 身份和端口映射；
- 持久化的分配意图和观测到的接入状态。

计划的本地设置流程：

```text
reserve IP atomically
-> create host/container veth pair
-> attach host peer to managed Linux bridge
-> move peer through an owner-verified namespace handle
-> configure address, loopback, routes and DNS inputs
-> add port/NAT rules through a bounded firewall abstraction
-> verify attachment
-> persist Sandbox Ready network status
```

namespace 不依赖工作负载进程持续存活。连续执行的 Attempt 加入该 Sandbox
namespace 并保留其 IP，除非策略明确要求重新创建 Sandbox。

IPAM 必须在 goroutine 和进程之间串行化操作，拒绝无效的 CIDR/地址，以原子方式
持久化，并提供确定性的保留/释放幂等语义。分配身份包含 Sandbox 和网络，
确保重试不会分配第二个地址。

## 关键设计

### 存储

- 仅在摘要验证通过后才信任内容。
- 将镜像解析与快照准备分离。
- 通过 syscall 或结构化参数执行 mount；绝不重新拼装 shell 命令。
- 将所有路径限制在已配置且归本系统所有的根目录之下，并在破坏性操作前拒绝
  路径遍历或 symlink 逃逸。
- 在产生非幂等副作用之前或同时，记录 mount/快照所有权。
- 先拆除嵌套 mount/bind mount，再拆除 OverlayFS mount，随后释放快照状态。
- 保留足够的元数据，以便崩溃后恢复清理流程。

### 网络

- 网络身份和 namespace 属于 Sandbox，而非 Attempt。
- 接口/规则名称在内核限制范围内具备抗冲突能力，并持久映射到完整资源 ID。
- bridge、link、route、firewall 和 IPAM 操作暴露结构化的幂等 API。
- firewall 变更使用精确且归本系统所有的规则身份，绝不通过含糊的字符串匹配删除。
- setup/teardown 对称；部分完成的 setup 会记录所有已获取资源。
- 并发创建/删除按资源串行化，并且在 daemon 重启前后均保持安全。
- 2.0 不实现多节点 VXLAN。未来的节点 CIDR 由集群作为输入提供，而不是由
  本地运行时分配。
- 所有权和幂等语义稳定后，未来的 CNI 适配器可以将请求转换到本地网络服务；
  当前不作任何 CNI 兼容性声明。

## 故障与恢复

存储回滚只按依赖关系的逆序移除当前操作获取的资源。若 unmount 失败，则禁止递归
删除仍可能暴露主机或卷数据的路径。在确认资源确实不存在之前，快照元数据必须保持
可恢复。

网络回滚按逆序移除归本系统所有的端口规则、路由、link attachment、veth 设备和
IP 保留。每个逆操作在流程部分完成后也必须安全。原始错误与回滚错误分别输出。
只有在确认没有任何归本系统所有的接入点仍可使用该 IP 时，才释放 IP。

daemon 重启后，调谐流程将持久化的快照/分配意图与 mountinfo、归本系统所有的
文件系统路径、namespace handle、link、route、firewall rule 以及
IPAM 状态进行比较。只有能够证明所有权的资源才可被接管。对于未知的主机资源，
系统应报告，而不能趁机删除。

## 可观测性与评测点

存储场景测量：

- 完整 rootfs 复制准备延迟与 OverlayFS 快照准备延迟的对比；
- cold 快照准备与复用已存在且已验证层的对比；
- 按声明的统计方法测量持久磁盘用量和已分配磁盘用量；
- 原生文件系统与 OverlayFS 的顺序写对比；
- 原生文件系统与 OverlayFS 的随机写对比；
- 原生文件系统与 OverlayFS 的元数据密集型工作负载对比。

文件系统对比必须固定文件系统、存储设备、mount 选项、镜像摘要、工作负载、
cache/cold 定义、样本数以及性能剖析状态。微基准测试不能证明端到端启动性能。

网络场景测量：

- Sandbox 网络 setup 阶段延迟；
- 在声明并发度下的并发 setup；
- 分配唯一性和重试幂等性；
- 路由/端口行为正确性；
- teardown 完整性；
- 压力场景下 veth、route、firewall、namespace 和 IP 分配数量的增长情况。

故障矩阵在确定性边界注入 content-read、摘要、快照、mount、IPAM 持久化、
link、route 和 firewall 故障。准确的基准测试契约和结果元数据见
[evaluation/README.md](../../evaluation/README.md)。

## 未来集群兼容性

集群可以提供不可变的镜像摘要以及未来的节点 CIDR/网络配置。本地 daemon 仍是
内容验证、快照生命周期、Sandbox namespace、本地
IP 分配和清理工作的权威。

集群调度/状态可以引用镜像摘要和稳定的 Sandbox IP，但不得直接 mount rootfs、
创建 link 或编辑 IPAM/firewall 状态。多节点网络设计和 CNI 集成属于未来的集群分支。

## 验收条件

仅在满足以下条件时，此功能才达到 **Verified** 状态：

- bundle/schema 解析会拒绝无效输入和逃逸输入；
- 内容摘要验证和快照所有权具备单元测试/集成测试；
- mount/快照部分失败时能够回滚，且不会删除暴露的主机数据；
- 原子状态能够承受注入的截断/崩溃边界；
- Sandbox 网络能够承受连续 Attempt 替换；
- 在已测试场景中，并发 IPAM 绝不会重复分配或丢失分配记录；
- 每一种注入故障发生后，link/route/port teardown 都保持幂等；
- daemon 重启后能够安全调谐归本系统所有的存储/网络资源；
- cold/warm 和 copy/OverlayFS 基准测试定义能够产生可复现的原始结果；
- 压力测试结果能够区分延迟清理、有界缓存和持续性泄漏。

## 未决问题

- 初始镜像导入/传输格式，以及最低限度的 OCI image 支持范围。
- 内容存储和元数据事务的实现方式。
- 快照命名、引用计数和后续垃圾回收策略。
- firewall 后端（`nftables` 或兼容层）及所有权标记。
- 初期只支持一个网络接入点，还是从一开始就设计支持多个接入点的 API。
- 保留 Sandbox 时 DNS 文件的生成和更新语义。
