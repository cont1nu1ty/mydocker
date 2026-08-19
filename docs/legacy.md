# Legacy mydocker 审计

**状态：** Legacy。本文件是范围有限的 M0 审计，并非逐个函数的迁移计划。

## 审计来源与仓库基线

审计开始时检出的 `main` 是空的 root commit `b1a4a2e`，commit message 为
`chore: initialize mydocker 2.0`。legacy 实现直接在 `origin/legacy/v1` 的
commit `357b19e` 上接受检查，现已通过 annotated tag `v0.1.0-legacy` 固定在本地。

这两个 commit 属于两条彼此独立、没有 merge base 的 root histories。本地和远程
仓库都不存在 `master` 分支。远程默认分支仍为 `main`；M0 没有 push、rename、merge
或更改远程 refs。

legacy tree 有 39 个 tracked entries，包括 32 个 Go 文件、两个测试文件、一个已提交的
`.DS_Store`，以及一个预构建的 `mydocker` 二进制文件。它包含 `go.mod`、`go.sum`、
Makefile、根目录 CLI 文件，以及 `cgroups/`、`container/`、`network/`、`nsenter/`、
`utils/`、`constant/` 和 `example/` 目录。它没有 README 或 benchmark。

## 已确认的实现范围

以下陈述均从 commit `357b19e` 中得到确认，并非根据项目 brief 推断。

### CLI 与生命周期

- `urfave/cli` v1 应用暴露 `run`、内部 `init`、`commit`、`ps`、`logs`、
  `exec`、`stop`、`rm` 和 `network` 命令。
- `run` 通过 `/proc/self/exe init` 创建子进程，准备 workspace，应用资源控制，
  按需连接网络，写入 JSON 状态，通过 pipe 发送 workload 命令；之后只在 foreground
  模式下等待。
- detached 模式的等待和清理被放在一个由短命 CLI 拥有的 goroutine 中。CLI 返回后，
  没有 daemon 或可重新连接的 supervisor 继续拥有该 goroutine。
- 持久容器状态以 JSON 形式保存在 `/var/lib/mydocker/containers/<id>/` 下，其中包含
  host PID 字符串、status、command 字符串、volume、network、ports 和 IP。
- `stop` 检查这个裸 PID 并向其发送信号，随后立即记录 `stopped`；它既不等待进程退出，
  也不升级信号或验证进程身份。
- `ps` 只报告持久化状态，不将其与 host 上的实际进程状态进行 reconciliation。

### 进程与 namespace 配置

- 创建时通过 clone flags 请求新的 UTS、PID、mount、network 和 IPC namespaces。
  没有 user namespace，因此预期以 rootful 模式运行。
- Init 将 `/` 标记为 private，bind-mount 新 root，调用 `pivot_root`，卸载旧 root，
  并挂载 `/proc` 和 tmpfs `/dev`。
- 多处 mount 错误仅被记录或忽略，而没有返回给调用方，因此调用方无法可靠地区分
  完整隔离与部分配置。
- `exec` 使用 CGO constructor，根据持久化 PID 为 IPC、UTS、network、PID 和 mount
  namespaces 调用 `setns`。

### 命令与进程身份

- `run` 使用空格连接 argv，init 再按空格拆分；包含空格或引号的参数无法往返保持，
  这种编码有歧义，并非保持长度的结构化 argv 协议。
- `exec` 将命令连接后放入 environment variable，C constructor 再通过 `system`
  调用它，从而重新引入 shell 解释。
- C 路径在单个 `setns` 失败后仍会继续，并以成功状态退出，无法保留命令的真实结果。
- 元数据中没有 pidfd、process start identity、exit code、signal、OOM result、schema
  version、generation 或 operation identity。因此无法检测 PID 复用。

### 资源控制

- 资源管理器通过解析 `/proc/self/mountinfo` 并写入 `tasks`、`cpu.cfs_quota_us`
  和 `memory.limit_in_bytes` 等文件，实现 cgroup v1 风格的 `cpu`、`cpuset` 和
  `memory` subsystems。
- 每个容器都使用同一个 `mydocker-cgroup` 路径，而不是 Sandbox 父 cgroup 和
  每个 Attempt 独立的子 cgroup。
- Subsystem 失败会被记录，但 manager methods 仍返回成功；调用方也会忽略返回值。
- 不存在 cgroup v2 unified hierarchy 模型、request/limit 分离、pids controller、
  OOM attribution，也没有在 `stop`/`rm` 时进行可靠清理。

### Rootfs 与存储

- Image 预期是 `/var/lib/mydocker/image/` 下的 tar 文件。
- 每个容器都在 `/var/lib/mydocker/overlay2/` 下准备 lower、upper、work 和 merged
  目录，解压 image 并挂载 OverlayFS。
- Volume 使用 host bind mounts，并从一个 `host:container` 字符串中解析。
- Workspace 函数通常在配置或卸载出错后记录日志并继续。卸载失败后，teardown 仍可能
  删除目录，而且 target paths 没有限制 traversal。
- `commit` 命令将 merged 目录归档为新的 tar image。

### 网络与 IPAM

- network package 实现 bridge driver、veth attachment、routes、namespace 配置、
  iptables SNAT/DNAT 和 port mapping。
- 网络定义和 IP bitmap 持久化在 `/var/lib/mydocker/network/` 下。
- IPAM flow 针对一个 JSON 文件执行 read/modify/truncate/write，没有 interprocess
  lock、atomic rename、schema version 或 crash recovery。
- Allocation、bridge/veth 创建、namespace 配置和 firewall rules 并未组成一个
  transactional rollback sequence。在接受检查的路径中，disconnect 不会释放容器 IP。
- 网络所有权与用户进程 PID 耦合，而不是与 Sandbox 持有的 network namespace 绑定。

### 错误与清理行为

- create 路径依次执行 workspace、process、cgroup、network、metadata 和 command 步骤，
  却没有统一的逆序 rollback stack。
- 许多 CLI actions 调用的函数只记录错误后便返回 `nil`，因此 exit status 无法稳定用于
  自动化正确性检查。
- ID 是使用 `math/rand` 生成的十位十进制数字，没有冲突检查，也没有稳定的
  加密安全或单调递增身份方案。

## 现有测试与评测

现有测试只有 `network/bridge_driver_test.go` 和 `network/ipam_test.go`。

- Bridge tests 会在真实 host 上创建、连接、断开和删除 bridge/veth 配置。它们属于
  privileged integration tests，却没有 preflight 或 isolation guard。
- IPAM tests 使用 `/var/lib/mydocker` 下固定的生产路径；既非 hermetic，也不具备
  concurrency safety。exhaustion loop 会把预期的 exhausted state 当作 fatal error。
- 没有针对 lifecycle transitions、argv fidelity、PID identity、cgroup behavior、
  rollback、daemon recovery、resource leaks 或 restart idempotency 的测试。
- 未发现 benchmark、stress harness、fault-injection harness、metrics、tracing、
  profiling workflow 或可复现的结果格式。

由于现有测试可能修改 host 网络和固定的 `/var/lib` 状态，M0 不会在这台开发主机上运行
完整的 legacy suite。这是一项测试安全属性，不代表该 suite 会通过或失败。

仅构建的安全检查在 2.0 工作树之外、由 `357b19e` 生成的临时 archive 中运行，环境为
启用 CGO 的 `go1.26.6-X:nodwarf5 linux/amd64`。`go mod verify`、`go build ./...`、
`go vet ./...` 以及 network test binary 的编译均通过。没有运行任何 test body。
`CGO_ENABLED=0 go build ./...` 失败，因为 `nsenter` 只有由 CGO 支持的 Go 文件；
因此 legacy binary 需要 C toolchain。Makefile 的 `run` target 使用 `go run main.go`，
这会漏掉 `main` package 中的其他文件，不能代表有效的完整程序调用方式。

## 为何 2.0 要彻底重写

2.0 所需的核心边界——long-lived lifecycle authority、Sandbox 与 Attempt 的所有权划分、
cgroup v2 hierarchy、持久 operations/events、已验证的 process identity、结构化 argv、
atomic state、rollback、reconciliation 和可度量的 stage boundaries——在 legacy 设计中
都未独立存在。保留其 CLI、state files、PID assumptions、IPAM format 或 rootfs layout，
会迫使新模型继承含糊的故障语义。

可以参考 legacy 代码中的 Linux syscall sequencing、bridge/netlink mechanics、
OverlayFS experiments 和基础 CLI ideas。任何代码复用均为可选，并且要求：

- 边界足够小，且符合新的所有权模型；
- 不依赖 legacy state 或 command encoding；
- 具备自动化 correctness tests 和 failure tests；
- 明确传播错误并执行幂等清理；
- 有证据表明复用比重新实现更简单、更安全。

代码复用比例不构成目标。M0 不复制、删除或修改 legacy 业务代码，也不承诺兼容性。

## 有待后续验证

以下内容必须在隔离的 Linux 环境中验证，或完成相应实现后，才能声明为 Verified：

- 使用其声明的 Go 1.17 toolchain 和目标 CGO 配置时，legacy 的确切构建结果
  （上述现代 toolchain 下仅构建的结果并非 compatibility matrix）；
- 在 cgroup v1 与 cgroup v2 host 上的行为；
- 不同 kernel 上 OverlayFS、`pivot_root`、`/proc` 和 mount propagation 的行为；
- CLI 退出和 host reboot 之后 detached process 与清理的行为；
- PID 1 signal behavior、exit-code preservation、OOM attribution 和 PID reuse；
- concurrent IP allocation、duplicate IDs 和 partial state writes；
- 每个 setup stage 失败后的 rollback 完整性；
- 重复 lifecycle operations 后不存在遗留的 mounts、cgroups、veth devices、
  firewall rules、processes 和 metadata。

这些都是验证目标，并不表示 legacy 实现已满足或未满足每一个场景。
