# mydocker 仓库说明

## 适用范围

- 本说明适用于整个仓库。
- 本文件应保持简短且便于执行；详细设计应放在 `docs/` 下。
- 将 `v0.1.0-legacy` 和 `origin/legacy/v1` 视为只读的 legacy 参考。

## 项目使命

- mydocker 2.0 是一次彻底重写，而不是渐进式兼容版本。
- 目标是实现一个仅支持 Linux、以 rootful 模式运行的单节点容器执行引擎。
- mydocker 消费 OCI 镜像并负责 image-to-process 链路；它不构建镜像。
- `Sandbox` 是一等资源，而不是 ID 包装器。
- `Container Attempt` 表示稳定 Sandbox 中的一次执行。
- 正确性优先于可靠性、度量、优化和功能数量。
- 可复现的度量本身就是交付物；只有建立基线后才能优化。
- 集群功能应位于未来从已验证的 2.0 revision 派生出的分支中。
- 不要在 2.0 中加入 scheduler、heartbeat、etcd、placement 或多节点网络。

## 首先阅读

- 先阅读 `README.md`，了解仓库状态和导航。
- 涉及跨领域边界或领域变更时，阅读 `docs/architecture.md`。
- 变更 milestone 范围前，阅读 `docs/roadmap.md`。
- 处理聚焦的功能任务时，只阅读 `docs/features/` 下相关的文件。
- 涉及测试、可靠性、指标、benchmark 或 profiling 时，阅读 `evaluation/README.md`。
- 引用或选择性移植 legacy 代码前，阅读 `docs/legacy.md`。
- 小型文档修正不要求阅读所有设计文档。

## 架构不变量

- CLI 是客户端；它不得拥有 detached workload 的生命周期。
- `mydockerd` 是单节点生命周期和元数据的权威来源。
- Sandbox 拥有稳定身份、网络身份、共享 UTS/IPC/network namespaces、
  hostname/DNS 配置、labels、port mappings 和父 cgroup。
- Container Attempt 拥有其进程、rootfs/snapshot、PID 和 mount namespaces、
  子 cgroup、输出流、退出码、信号和 OOM 结果。
- 初期每个 API Container 恰好对应一个 Attempt；workload retry 会在同一
  Sandbox 下创建一组新的 Container/Attempt，而 transport retry 会复用原 operation。
- 初期，一个 Ready Sandbox 最多只能有一个 active Container Attempt。
- 连续的 Attempts 可以复用 Sandbox 身份及其保留的资源。
- 默认情况下，PID namespace 归 Container Attempt 所有。
- 新的资源控制代码只以 cgroup v2 为目标。
- CPU 和内存 requests 描述调度意图；limits 描述强制约束。
- Request 和 limit 字段在 API 与持久化数据中必须始终分开。
- `SandboxSpec.Resources` 是规范来源；解析后的 limits 会复制到每个 Attempt。
- 生命周期状态变更必须明确、经过校验，并可持久恢复。
- 创建和清理必须对称；部分创建失败时需要按逆序回滚。
- 未进行更强的进程身份校验前，绝不能信任持久化的裸 PID。
- 公共生命周期操作必须具备幂等性，或精确定义 retry 行为。
- 生产代码不得依赖 evaluation harness。
- 未来的集群组件只能调用版本化的本地 API 或 `pkg/client`。
- 集群 agent 不得导入 runtime 内部实现，也不得直接修改本地状态。
- 集群工作所需的 API 变更必须先在 2.0 中完成设计和验证。
- Dockerfile build、核心 `docker commit`、image push 和 registry server 不属于 2.0
  完成标准。
- 不得声称完全兼容 OCI、CRI、Kubernetes 或 containerd。

## 度量不变量

- 在建立经过正确性检查的基线前，不得优化性能。
- 性能结论必须注明 commit、环境、场景和原始结果。
- 优化前后的度量必须使用相同的机器、构建、workload 和设置；否则必须
  列出每一项重要差异，并避免作出因果结论。
- 启动延迟必须说明边界，以及场景是 cold 还是 warm。
- 不得隐式合并 Sandbox 创建、Container 创建与 Container 启动。
- 每个外部生命周期操作使用一个 operation ID，并在其中记录分阶段事件。
- 客户端在首次发送前创建 operation ID；服务端将其绑定到规范的请求
  fingerprint，并拒绝内容不匹配的复用。
- Runtime timestamps 是诊断元数据，不能自动视为 benchmark 样本。
- 在可用时，同一进程内使用 monotonic duration 语义。
- 不得对未经同步的跨进程或跨节点 wall-clock timestamps 直接相减。
- Prometheus labels 必须保持低基数。
- metrics 的 labels 绝不能使用 Sandbox ID、Container ID、Task ID、Operation ID、
  image digest 或完整错误字符串。
- 具体 ID 和详细错误应放在 structured logs 或 traces 中。
- 常规测试不得依赖不稳定的毫秒级延迟阈值。
- Benchmark 衡量性能，但不能证明行为正确。
- Stress tests 检查持续运行行为和资源增长情况。
- Fault tests 验证受控故障下的 rollback、retry 和 recovery。
- Profiling 用于解释已观察到的瓶颈，不能取代 benchmark。
- 启用 profiling 的结果不能与普通 benchmark 结果直接比较。
- 绝不能虚构 P50/P95/P99、throughput、overhead、MTTR 或提升百分比。
- 未经度量的数值不得出现在 release 或简历描述中。

## 开发工作流

- 编辑前检查当前分支和工作树。
- 保留用户的变更，不要在补丁中夹带无关工作。
- 变更实现前，理解相关实现及其设计文档。
- 每项任务都应足够小，以便独立 review 和验证。
- 不得创建空的生产目录、占位 Go 文件或虚假 API。
- 只有在真实代码、测试或实验确有需要时才新增目录。
- 行为变更必须同时更新测试和负责该行为的功能文档。
- 与性能相关的变更如果改变了指标或场景契约，必须更新
  `evaluation/README.md`。
- 状态或 API schema 变更必须定义 versioning、migration 和 recovery 行为。
- 复杂的生命周期、持久化或 API 工作可以使用一个任务专属计划。
- 不要创建全局 PLANS 归档、ADR 层级或文档模板系统。
- 遇到不安全的部分状态时，应返回明确错误，而不是只记录日志后继续。
- 将 argv 和 environment 保持为结构化数据；不要拼接再拆分 shell 字符串。
- 故障注入应确定、可移除，并与生产默认值隔离。
- 标准库或现有依赖已经足够时，不要增加新依赖。
- 未经项目明确批准，不得变更 Go module 的 major version。
- 新增或修改的每个具名 Go 函数和方法都必须在声明前添加注释，说明其功能、预期
  调用方或用途，以及重要副作用、幂等性或不变量；注释不得只复述函数名。
- 测试函数和测试辅助函数的注释应说明所验证的场景或契约；极短匿名闭包可以例外。
- 导出标识符的注释应以标识符名称开头，并符合 Go 文档注释约定。

## 验证与安全

- Go 代码变更应运行适用的 `gofmt`、`go test` 和 `go vet`。
- 启动特权 integration tests 前，必须进行明确的环境 preflight。
- mount、namespace、cgroup、bridge、veth 和 firewall 测试只能在一次性、
  有文档说明且专门用于此类测试的环境中运行。
- 绝不能在普通开发主机上运行破坏性或特权实验。
- 将 correctness、integration、fault、stress、benchmark 和 profiling 命令分开。
- 记录跳过的验证及具体原因。
- 报告预先存在的失败；不得为了让 M0 通过而隐藏或改写 legacy 行为。
- 不得使用 `git reset --hard`、`git clean -fd`、force push 或 history rewrite。
- 除非收到要求，否则不得 push、修改远程默认分支或创建 cluster 分支。

## 最终回复

- 概述变更的文件和重要设计决策。
- 列出验证命令、退出状态和结果分类。
- 说明哪些验证未运行以及原因。
- 准确报告已知问题，包括预先存在的失败。
- 建议下一个 milestone，但不要自动开始。
