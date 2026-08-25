# M3 rootful 一次性主机验收

**当前验证状态：Verified（M3 rootful 正确性范围，2026-08-25）。** 普通安全 gate、
单元测试、race、带 `mydocker_rootful` tag 的编译检查和 `go vet` 已通过；完整 tagged
lifecycle 也已在任务专用、执行后可销毁的 KVM 来宾机中真实运行并通过。该状态不表示
可在普通宿主机运行测试，不表示 hostile-workload 隔离、跨 kernel 兼容性、长期 stress
或性能已经验证。

本目录说明 `cmd/mydockerd` 中带 `linux && mydocker_rootful` build tag 的真实内核
验收套件。它组合生产 `FileStore -> Engine -> slim/cgroupv2/isolation -> mydocker-shim`、
HTTP/JSON UDS 与 `pkg/client`，不是 fake provider 测试，也不是普通开发机测试。
套件 rootfs 和 workload 必须由测试操作者完全控制；当前 M3 没有 capability/seccomp
安全 profile，本套件不能用于执行不受信代码或证明 hostile-workload 隔离。

## 2026-08-25 验收记录

最终验收使用 Ubuntu 24.04.4 LTS 官方 cloud image、Linux
`6.8.0-137-generic` x86_64、KVM、unified cgroup v2（`cpu memory pids`）与 Go `1.22.2`。
下载镜像的 SHA-256 为
`6e40c07ae715f744f84af0bec76415cc1987dd115b4b8de437818561f01a3733`，与 Canonical
发布的校验值一致。源码在传入来宾机后逐文件校验；最终复跑直接使用生产
`openProductionRuntime` 和 `newProductionServer`，没有测试专用 provider 或诊断
service 包装。

下文所列命令以 exit `0` 完成，三个子场景全部 `PASS`：

- `none_exit_pid1`：真实 namespace、prepared-rootfs self-bind/`pivot_root`、新 `/proc`
  中的 PID 1、workload exit `23`、日志/events 与 cgroup 清理；
- `loopback_daemon_reopen_signal`：hostname/DNS、loopback readback、500m CPU、64 MiB
  memory、64 pids 的 controller 读回、keeper/Attempt sibling membership、daemon
  关闭后从同一 FileStore 重开，以及作用时身份校验后的 `SIGTERM`；
- `memory_oom_attribution`：128 MiB `memory.max` 下的固定内存压力 workload，公共终态的
  `SIGKILL + oom=true` 与同一 Attempt `memory.events.local` 的 `oom`/OOM-kill 增量一致。

测试返回后，专用 cgroup root 的 `cgroup.procs` 为空且没有 child cgroup，工作根下没有
残留 mount 或 `mydocker-shim` 进程。运行耗时只属于测试日志中的诊断信息，本记录不把它
作为 benchmark 样本，也不提供 P50/P95/P99 或性能结论。

## 安全边界

只能在专门创建、测试后直接销毁的一次性 rootful Linux VM 中运行。不要在工作站、共享
主机、CI runner 宿主机、容器宿主机或有其他 workload 的 cgroup hierarchy 中用 `sudo`
试跑。测试会创建 namespace、mount、进程与 cgroup，并执行 `pivot_root`。

即使使用 build tag，以下条件全部满足前测试也不会产生副作用：

- 两个独立 opt-in 都精确等于 `1`；
- disposable 声明与固定文本完全相同；
- effective UID 为 0；
- `isolation.ValidatePrivilegedTest`、`isolation.PreflightSystem` 与生产进程工厂的
  `clone3(CLONE_INTO_CGROUP|CLONE_PIDFD)` 无子进程探针通过；
- cgroup root 是 `/sys/fs/cgroup` 下、名称以 `mydocker-rootful-test-` 开头的空专用 cgroup，
  没有 member process 或 child cgroup，并可见 `cpu`、`memory`、`pids` controllers；
- work root、prepared rootfs 和 shim 都是 root-owned、非 symlink、非 group/world writable
  的专属路径；
- work root 的绝对路径不超过 8 bytes。这个限制用于保证 64-byte owner token 后面的
  shim control socket 仍能放入 Linux `sockaddr_un`；建议使用 `/mrt`；
- work root 下的保留运行时目录 `rn`、`rl`、`ro` 尚不存在；套件拒绝覆盖或递归清理旧运行；
- work root 内存在内容精确匹配的 `.mydocker-rootful-test-root` marker；
- rootfs 与 shim 都严格位于 work root 下。rootfs 是本测试独占的可写副本，至少包含
  `/bin/sh`、`/bin/sleep` 和非 symlink 的普通文件 `/etc/resolv.conf`。

任何 gate、路径、UID 或 read-only preflight 失败都发生在首次 `mkdir`、daemon 启动和
内核副作用之前。普通 `go test ./...` 不包含 tagged 场景。

## 一次性 VM 准备

以下只是一次性 VM 内的示意步骤；cgroup delegation 的具体父层级应由该 VM 的启动方式
负责，不能原样复制到普通宿主机：

```bash
install -d -o root -g root -m 0700 /mrt
printf 'mydocker disposable rootful test root\n' > /mrt/.mydocker-rootful-test-root
chmod 0600 /mrt/.mydocker-rootful-test-root

# 将独占的最小 rootfs 解包到 /mrt/rootfs；不要指向宿主机 /。
# /mrt/rootfs/bin/sh、/mrt/rootfs/bin/sleep 必须可执行，
# /mrt/rootfs/etc/resolv.conf 必须是 root-owned 普通文件。

install -d -o root -g root -m 0755 /mrt/bin
go build -o /mrt/bin/mydocker-shim ./cmd/mydocker-shim
chown root:root /mrt/bin/mydocker-shim
chmod 0755 /mrt/bin/mydocker-shim

# 由 VM 管理脚本在已启用 cpu/memory/pids delegation 的父 cgroup 下创建：
mkdir /sys/fs/cgroup/mydocker-rootful-test-m3
```

执行前确认专用 cgroup 的 `cgroup.procs` 为空、没有 child directory，且
`cgroup.controllers` 包含 `cpu memory pids`。

```bash
export MYDOCKER_ROOTFUL_TEST=1
export MYDOCKER_ALLOW_PRIVILEGED_TEST=1
export MYDOCKER_DISPOSABLE_ENVIRONMENT=I_UNDERSTAND_MYDOCKER_MUTATES_THIS_DISPOSABLE_VM
export MYDOCKER_ROOTFUL_WORK_ROOT=/mrt
export MYDOCKER_ROOTFUL_CGROUP_ROOT=/sys/fs/cgroup/mydocker-rootful-test-m3
export MYDOCKER_ROOTFUL_ROOTFS=/mrt/rootfs
export MYDOCKER_ROOTFUL_SHIM=/mrt/bin/mydocker-shim

go test -v -count=1 -tags=mydocker_rootful ./cmd/mydockerd \
  -run '^TestRootfulM3ProductionLifecycle$'
```

## 场景与证据

套件按顺序执行三个场景：

1. `network=none`：完整 Sandbox/Container 生命周期；prepared-rootfs pivot；workload 从
   `/proc/1/stat` 证明 namespace 内 PID 1；捕获 exit code 23；校验日志、每个正式
   operation 的 successful `complete` event，以及 Attempt/keeper/Sandbox cgroup 清理。
2. `network=loopback`：配置 `203.0.113.53` 测试 DNS 与 500m CPU、64 MiB memory、
   64 pids limits；workload 从新 rootfs 内读取 hostname 与托管 `/etc/resolv.conf`；测试
   直接读回 `cpu.max`、`memory.max`、`pids.max`，并验证 keeper/Attempt sibling cgroup
   均有 member；优雅关闭 daemon 后用同一 FileStore/runtime root/UDS 配置重新打开；
   验证 Running Attempt 与 start operation 被恢复；通过公共 API 发送 SIGTERM，验证
   signal outcome、完整事件和 leaf-to-parent cgroup 清理。loopback 的 up 状态由生产
   provider 在配置后执行 ioctl readback，任一不一致都会使 Sandbox 创建失败。
3. `memory OOM attribution`：所有 read-only gate 通过后，用当前本机 Go toolchain 在独占
   rootfs 中构建一个 test-only、静态、固定 4 MiB 分块并逐页触碰的内存压力 workload；
   将 Attempt 的 `memory.max` 固定为 128 MiB，要求 workload 先写出启动 marker，随后
   终态同时满足 `captured`、`SIGKILL`、`oom=true`。删除 cgroup 前，套件再通过生产
   cgroup manager 读取同一 Attempt 的 `memory.events.local`，要求 `oom` 与至少一个
   OOM-kill counter 增长，并验证 limits、完整事件和资源清理。helper 只在所有 gate 后
   构建，使用 run evidence 目录内的独立 Go cache，且仅在 inode 身份未变化时从 rootfs
   移除。普通 64 MiB 场景或任意 exit code 仍不能当作 OOM 证据。

每次执行会在 work root 下保留一个 `m3-run-*` 证据目录，便于检查状态与日志；套件不会
用递归删除掩盖失败。成功结束时专用 cgroup root 必须没有 child cgroup。若失败后仍有
未知 mount/process/cgroup 状态，应保留日志并销毁整个 VM，不要把手工递归清理命令迁移
到普通主机工作流。

仅编译 tagged harness、绝不运行特权场景，可在普通主机使用：

```bash
go test -run '^$' -tags=mydocker_rootful ./cmd/mydockerd
go test -run '^TestLoadRootfulTestEnvironmentFailsClosed$' ./cmd/mydockerd
```
