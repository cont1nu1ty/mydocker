# Sandbox 网络

## 状态

**Proposed。** M0 规定所有权边界和评测边界；2.0 尚无网络实现。

## 目的

为每个 Sandbox 提供稳定且可恢复的节点本地网络身份。网络创建和拆除必须支持原子
状态、并发安全、幂等重试、逆序回滚、daemon 重启协调以及可度量的阶段边界。

网络属于 Sandbox，而不属于单次 Container Attempt。连续 Attempts 加入同一个
Sandbox network namespace，并保留其 IP、路由、DNS 输入和端口映射，除非策略明确
要求删除并重新创建整个 Sandbox。

## 范围与非目标

首版范围包括：

- 由 namespace keeper/supervisor 持有的 Sandbox network namespace；
- 一个网络接入点所需的 veth pair 和受管 Linux bridge；
- 节点本地 IPAM、地址/路由/loopback/DNS 输入；
- 端口映射和有边界、有所有权身份的 firewall 抽象；
- goroutine/进程并发下的分配安全、幂等 setup/teardown 和持久分配意图；
- 部分失败的逆序回滚、daemon 重启恢复、正确性/压力/故障评测。

初始范围不包括 CNI 兼容性、多节点 VXLAN/overlay 网络、节点 CIDR 分配、服务发现、
服务负载均衡或集群网络控制面。未来的集群可以把节点 CIDR/网络配置作为输入，但
mydocker 不自行决定跨节点地址规划。

## 核心对象与所有权

Sandbox 拥有：

- 由 keeper/supervisor（而非用户进程 PID）持有的 network namespace；
- 一个或多个网络接入记录（初始阶段实现一个接入点即可）；
- 已分配 IP、路由、DNS 输入、bridge/veth 身份和端口映射；
- 持久化的分配意图、资源 owner 和观测到的接入状态。

接入记录至少能关联完整 Sandbox ID、网络身份、主机/容器端接口、IP reservation、
route 和 firewall rule。受内核名称长度限制的接口/规则名称必须抗冲突，并持久映射回
完整资源 ID；截短名称本身不能作为所有权证明。

namespace 生命周期不依赖工作负载进程。Sandbox keeper/supervisor 持有稳定句柄，
Attempt 只加入该 namespace。停止或替换用户进程不能顺便删除网络身份。

## 设置与拆除流程

计划的节点本地设置流程：

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

每一步都记录已获取资源并注册逆操作。若 setup 部分失败，按依赖逆序执行：

```text
remove owned port/NAT rules
-> remove owned routes
-> detach/delete owned links
-> remove namespace attachment/handle when no longer needed
-> release IP only after no owned attachment can still use it
```

原始错误与每一个回滚错误分别输出。每个逆操作在前一步只完成一部分时也必须安全；
重复 setup/teardown 不产生第二个 IP、重复规则或误删其他资源。

## IPAM 契约

IPAM 必须：

- 校验 CIDR、保留地址、网关与申请地址，拒绝越界、网络/广播地址及策略禁止值；
- 在 goroutine 和 daemon 进程之间串行化冲突操作；
- 在返回 reservation 前原子持久化，截断文件不能成为唯一分配事实；
- 使用 `(Sandbox ID, network ID)` 作为稳定分配身份，使同一意图重试返回同一地址；
- 对 reservation 提供确定性的 reserve/get/release 幂等语义；
- 只有在能证明该 Sandbox 的所有 owned attachment 已不可使用地址时才释放；
- 区分期望 reservation、观测到的内核配置和等待清理的异常状态。

并发创建/删除按网络与 Sandbox 资源串行化。一次 reservation 已持久化但后续 veth
创建失败时，它保持可恢复的 operation 状态，直到逆序回滚确认释放成功；系统不能
先忘记记录，再假定地址已经空闲。

## Link、路由、DNS 与端口映射

- bridge、link、route、firewall 和 IPAM 操作暴露结构化、幂等的内部 API，绝不通过
  重新拼装 shell 字符串表达网络意图。
- veth 两端、bridge membership、namespace 内地址/loopback/route 都必须经过创建后
  验证；“系统调用未报错”不自动等于 Sandbox 网络 Ready。
- DNS 输入属于 Sandbox 稳定环境。实际文件/绑定的生成、更新和 keeper 重启语义必须
  在实现前确定，且不能依赖某次 Attempt 的 rootfs 永久存在。
- firewall 变更使用精确且归本系统所有的 rule identity；绝不通过含糊的字符串匹配、
  端口号或不完整 comment 删除规则。
- 端口映射冲突在产生部分规则前检测或以可回滚事务处理；冲突必须返回类型化错误，
  不能静默覆盖已有映射。
- setup 与 teardown 对称，持久记录覆盖每个已获取资源，而不是只记录最终成功状态。

2.0 不实现多节点 VXLAN。未来的节点 CIDR 由集群作为输入提供，而不是由本地 runtime
分配。所有权和幂等语义稳定后，未来的 CNI adapter 可以把请求转换到本地网络服务；
当前不作任何 CNI 兼容性声明。

## 主机数据边界

```text
/var/lib/mydocker/
├── network/        durable allocation, attachment and firewall intent
└── sandboxes/      versioned Sandbox records and network status

/run/mydocker/
└── sandboxes/      transient keeper/supervisor namespace handles
```

这些名称处于 **Proposed** 状态。持久记录携带 schema version，并使用选定状态存储的
事务，或临时写入、文件同步、原子重命名与目录同步。`/run/mydocker` 可以在主机重启后
消失，因此 namespace handle 和其他 transient 协调状态必须能依据持久意图安全重建；
裸 PID 或一个同名接口不能作为重连依据。

## 故障与恢复

网络故障矩阵在确定性边界注入 IPAM 持久化、namespace handle、link create/move、
bridge attach、address、route、DNS 和 firewall 错误。每个阶段都验证：

- 当前 operation 只回滚自己已经获取且能证明所有权的资源；
- setup/teardown 重试不会产生重复地址、link、route 或 rule；
- teardown 的某一步失败时，后续恢复仍保留足够的 owner/intent；
- IP 只在最后一个可能使用它的 owned attachment 已确认消失后释放；
- 原始失败与回滚失败不会互相覆盖。

daemon 重启后，调谐流程将持久分配/接入意图与 namespace handles、links、bridge
membership、addresses、routes、firewall rules 和 IPAM 状态比较。只有能够证明归
mydocker 所有的资源才可被接管或删除。对于未知主机资源，系统报告 conflict/unknown，
不能为了达到期望拓扑而猜测删除。

keeper/supervisor 重启或丢失时，恢复行为必须区分“namespace 仍由可信句柄持有”、
“namespace 已消失可安全重建”和“身份无法证明”三类情况。未进行强身份验证前不得
通过持久化裸 PID 进入 namespace。

## 可观测性与评测点

网络场景测量：

- Sandbox network setup 各阶段及整体延迟；
- 在声明并发度下的并发 setup；
- 分配唯一性和 transport/operation 重试幂等性；
- namespace、bridge、address、route、DNS 输入和 port mapping 行为正确性；
- teardown 完整性以及 daemon 重启后的恢复时间/结果；
- 压力场景下 veth、route、firewall、namespace 和 IP reservation 数量的增长情况。

评测必须固定 kernel、network namespace/bridge/firewall 后端、CIDR、端口集合、构建、
并发度、工作负载、样本数和 profiling 状态。压力测试要区分延迟清理、有界缓存和持续
泄漏；benchmark 不能代替网络行为正确性测试。

Sandbox ID、完整接口身份、IP、operation ID 和详细错误进入 structured logs/traces，
不能成为无界 Prometheus labels。准确的基准测试契约和结果元数据见
[evaluation/README.md](../../evaluation/README.md)。

## 未来集群兼容性

集群可以提供未来的节点 CIDR、network ID 或网络配置，并引用 Sandbox 的稳定 IP；
本地 daemon 仍是 Sandbox namespace、节点 IP 分配、link/route/firewall 状态和清理的
权威来源。

集群 scheduler/controller/agent 不得直接创建 link、进入 namespace 或编辑
IPAM/firewall 状态。多节点数据面、service networking、CNI 集成和节点 CIDR 控制属于
未来集群分支；它们只能通过版本化本地 API 与 mydocker 的网络边界交互。

## 验收条件

仅在满足以下条件时，此功能才达到 **Verified** 状态：

- namespace 由可信 keeper/supervisor 持有，不依赖某个用户进程 PID；
- Sandbox 能承受连续 Attempt 替换，并保持规定的 IP、route、DNS 和 port identity；
- 无效 CIDR/地址、端口冲突和无法证明的 owner 输入会被拒绝；
- 在已测试的 goroutine/进程并发和重试场景中，IPAM 不重复分配或丢失 reservation；
- link/bridge/address/route/firewall 任一注入故障后，逆序 rollback 和再次 teardown 均
  保持幂等；
- daemon 重启后能够安全调谐归本系统所有的 namespace、link、route、rule 和 IPAM
  资源，且不删除未知主机资源；
- setup、concurrency、correctness、teardown、recovery 和 stress 场景可以产生可复现
  的原始结果；
- 压力测试能够区分延迟清理、有界缓存和持续性资源泄漏。

## 未决问题

- firewall 后端（`nftables` 或兼容层）及其原子更新/所有权标记方式。
- 初期只支持一个网络接入点，还是从 API schema 起支持多个接入点。
- 受管 bridge 的创建、复用、配置漂移与 daemon 升级语义。
- 保留 Sandbox 时 DNS 文件/绑定的生成、更新和恢复语义。
- 节点重启导致 network namespace 消失时，原 IP reservation 的重建或失败策略。
- 未来 cluster 提供节点 CIDR 和可选 CNI adapter 时的版本化输入边界。
