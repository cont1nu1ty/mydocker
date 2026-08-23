package isolation

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"sync"
)

// fakeOps records every host mutation and supplies deterministic procfs/nsfs evidence to pure tests.
type fakeOps struct {
	mu            sync.Mutex
	euid          int
	pid           int
	tid           int
	files         map[string][]byte
	links         map[string]string
	pathStats     map[string]FileInfo
	statfs        map[string]FileSystemInfo
	fdStats       map[int]FileInfo
	fdFS          map[int]FileSystemInfo
	fdLinks       map[int]string
	currentNS     map[NamespaceType]uint64
	processNS     map[NamespaceType]uint64
	nextFD        int
	fail          map[string]error
	calls         []string
	mutations     []string
	allowedRootFD int
	rootfsFD      int
	rootfsFileFD  int
	setnsInode    uint64
	setnsCalls    int
	setnsFailAt   map[int]error
	unshareNoop   bool
	hostnameValue string
	loopbackValue bool
}

// fakeThreadLocker records logical pin and release transitions without calling runtime.LockOSThread.
type fakeThreadLocker struct {
	locked  bool
	locks   int
	unlocks int
}

// lock marks the pure test helper as pinned to its synthetic OS thread.
func (l *fakeThreadLocker) lock() {
	l.locked = true
	l.locks++
}

// unlock marks an untainted pure test helper as released from its synthetic OS thread.
func (l *fakeThreadLocker) unlock() {
	l.locked = false
	l.unlocks++
}

// runFakeLockedHelper executes one helper action with fake thread pinning and returns its lock record.
func runFakeLockedHelper(ops *fakeOps, action func(*LockedHelper) error) (*fakeThreadLocker, error) {
	locker := &fakeThreadLocker{}
	err := runLockedHelper(context.Background(), ops, locker, action)
	return locker, err
}

// runFakeNamespaceSession executes one namespace action with synthetic thread pinning and bounded cleanup.
func runFakeNamespaceSession(ctx context.Context, ops *fakeOps, handles []*NamespaceHandle, action func(context.Context, *LockedHelper) error) (*fakeThreadLocker, error) {
	locker := &fakeThreadLocker{}
	err := runNamespaceSession(ctx, ops, locker, defaultNamespaceCleanupTimeout, handles, action)
	return locker, err
}

// newFakeOps returns a rootful cgroup-v2 host with valid process and namespace evidence.
func newFakeOps() *fakeOps {
	value := &fakeOps{
		euid:          0,
		pid:           42,
		tid:           4242,
		files:         make(map[string][]byte),
		links:         make(map[string]string),
		pathStats:     make(map[string]FileInfo),
		statfs:        map[string]FileSystemInfo{"/sys/fs/cgroup": {Type: Cgroup2FSMagic}},
		fdStats:       make(map[int]FileInfo),
		fdFS:          make(map[int]FileSystemInfo),
		fdLinks:       make(map[int]string),
		currentNS:     make(map[NamespaceType]uint64),
		processNS:     make(map[NamespaceType]uint64),
		nextFD:        1000,
		fail:          make(map[string]error),
		setnsFailAt:   make(map[int]error),
		allowedRootFD: 800,
		rootfsFD:      801,
		rootfsFileFD:  802,
		hostnameValue: "host-inherited",
	}
	value.files[bootIDPath] = []byte("boot-test-id\n")
	value.setProcessEvidence(123, 777, "/mydocker/sandbox-1/attempt-1", "/usr/bin/workload")
	for index, namespaceType := range []NamespaceType{NamespaceUTS, NamespaceIPC, NamespaceNetwork, NamespacePID, NamespaceMount} {
		value.currentNS[namespaceType] = uint64(100 + index)
		value.processNS[namespaceType] = uint64(200 + index)
	}
	value.fdStats[value.allowedRootFD] = FileInfo{Mode: 0040000, Dev: 1, Ino: 10}
	value.fdStats[value.rootfsFD] = FileInfo{Mode: 0040000, Dev: 1, Ino: 11}
	value.fdStats[value.rootfsFileFD] = FileInfo{Mode: 0100000, Dev: 1, Ino: 12}
	return value
}

// setProcessEvidence replaces the fake process identity used by capture and action-time verification.
func (f *fakeOps) setProcessEvidence(pid int, startTime uint64, cgroupPath, executable string) {
	f.pid = pid
	f.files[fmt.Sprintf("/proc/%d/stat", pid)] = []byte(fakeProcStat(pid, "worker (safe)", startTime))
	f.files[fmt.Sprintf("/proc/%d/cgroup", pid)] = []byte("0::" + cgroupPath + "\n")
	f.links[fmt.Sprintf("/proc/%d/exe", pid)] = executable
}

// fakeProcStat builds enough Linux stat fields to put starttime at field 22.
func fakeProcStat(pid int, comm string, startTime uint64) string {
	fields := make([]string, 20)
	fields[0] = "S"
	for index := 1; index < len(fields); index++ {
		fields[index] = "0"
	}
	fields[19] = strconv.FormatUint(startTime, 10)
	return fmt.Sprintf("%d (%s) %s", pid, comm, strings.Join(fields, " "))
}

// failure returns a configured deterministic fault for one named fake operation.
func (f *fakeOps) failure(name string) error {
	if err := f.fail[name]; err != nil {
		return err
	}
	return nil
}

// record appends one observable call and optionally marks it as a host mutation.
func (f *fakeOps) record(name string, mutation bool) {
	f.calls = append(f.calls, name)
	if mutation {
		f.mutations = append(f.mutations, name)
	}
}

// allocateNamespaceFD creates one fake nsfs descriptor with matching stat and link evidence.
func (f *fakeOps) allocateNamespaceFD(namespaceType NamespaceType, inode uint64) int {
	fd := f.nextFD
	f.nextFD++
	f.fdStats[fd] = FileInfo{Mode: 0100000, Dev: 2, Ino: inode}
	f.fdFS[fd] = FileSystemInfo{Type: NamespaceFSMagic}
	f.fdLinks[fd] = fmt.Sprintf("%s:[%d]", namespaceType, inode)
	return fd
}

// EffectiveUID returns the configured effective user identity.
func (f *fakeOps) EffectiveUID() int { return f.euid }

// ProcessID returns the configured current process identity.
func (f *fakeOps) ProcessID() int { return f.pid }

// ThreadID returns the configured current thread identity.
func (f *fakeOps) ThreadID() int { return f.tid }

// ReadFile returns an independent fake procfs/sysfs payload.
func (f *fakeOps) ReadFile(path string) ([]byte, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.record("read:"+path, false)
	if err := f.failure("read:" + path); err != nil {
		return nil, err
	}
	value, exists := f.files[path]
	if !exists {
		return nil, fmt.Errorf("missing fake file %s", path)
	}
	return append([]byte(nil), value...), nil
}

// Readlink returns configured procfs executable evidence.
func (f *fakeOps) Readlink(path string) (string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.record("readlink:"+path, false)
	if err := f.failure("readlink:" + path); err != nil {
		return "", err
	}
	value, exists := f.links[path]
	if !exists {
		return "", fmt.Errorf("missing fake link %s", path)
	}
	return value, nil
}

// Lstat returns configured final-component path evidence without following links.
func (f *fakeOps) Lstat(path string) (FileInfo, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.record("lstat:"+path, false)
	if err := f.failure("lstat:" + path); err != nil {
		return FileInfo{}, err
	}
	value, exists := f.pathStats[path]
	if !exists {
		return FileInfo{}, fmt.Errorf("missing fake path stat %s", path)
	}
	return value, nil
}

// StatFS returns configured filesystem magic without changing host state.
func (f *fakeOps) StatFS(path string) (FileSystemInfo, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.record("statfs:"+path, false)
	if err := f.failure("statfs:" + path); err != nil {
		return FileSystemInfo{}, err
	}
	value, exists := f.statfs[path]
	if !exists {
		return FileSystemInfo{}, fmt.Errorf("missing fake filesystem %s", path)
	}
	return value, nil
}

// OpenNamespace returns a new fake nsfs descriptor for self or the configured owned process.
func (f *fakeOps) OpenNamespace(path string) (int, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.record("open-ns:"+path, false)
	if err := f.failure("open-ns:" + path); err != nil {
		return -1, err
	}
	parts := strings.Split(path, "/")
	if len(parts) == 0 {
		return -1, fmt.Errorf("invalid namespace path")
	}
	namespaceType := NamespaceType(parts[len(parts)-1])
	if namespaceType == "pid_for_children" {
		namespaceType = NamespacePID
	}
	inode, exists := f.currentNS[namespaceType]
	if strings.Contains(path, fmt.Sprintf("/proc/%d/", f.pid)) {
		inode, exists = f.processNS[namespaceType]
	}
	if !exists {
		return -1, fmt.Errorf("missing fake namespace %s", namespaceType)
	}
	return f.allocateNamespaceFD(namespaceType, inode), nil
}

// OpenDirectoryNoSymlink returns the configured safe ownership-root descriptor.
func (f *fakeOps) OpenDirectoryNoSymlink(path string) (int, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.record("open-root:"+path, false)
	if err := f.failure("open-root:" + path); err != nil {
		return -1, err
	}
	return f.allowedRootFD, nil
}

// OpenDirectoryBeneath returns the configured safe rootfs descriptor.
func (f *fakeOps) OpenDirectoryBeneath(base, target string) (int, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	name := "open-beneath:" + base + ":" + target
	f.record(name, false)
	if err := f.failure(name); err != nil {
		return -1, err
	}
	return f.rootfsFD, nil
}

// OpenFileBeneath returns the configured existing regular-file descriptor used by DNS bind tests.
func (f *fakeOps) OpenFileBeneath(base, target string) (int, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	name := "open-file-beneath:" + base + ":" + target
	f.record(name, false)
	if err := f.failure(name); err != nil {
		return -1, err
	}
	if _, found := f.fdStats[f.rootfsFileFD]; !found {
		f.fdStats[f.rootfsFileFD] = FileInfo{Mode: 0100000, Dev: 1, Ino: 12}
	}
	return f.rootfsFileFD, nil
}

// OpenDirectoryAt records descriptor-relative rootfs resolution and returns the configured rootfs descriptor.
func (f *fakeOps) OpenDirectoryAt(baseFD int, relative string) (int, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	name := fmt.Sprintf("open-directory-at:%d:%s", baseFD, relative)
	f.record(name, false)
	if err := f.failure(name); err != nil {
		return -1, err
	}
	return f.rootfsFD, nil
}

// OpenFileAt records descriptor-relative file resolution and returns the configured DNS target descriptor.
func (f *fakeOps) OpenFileAt(baseFD int, relative string) (int, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	name := fmt.Sprintf("open-file-at:%d:%s", baseFD, relative)
	f.record(name, false)
	if err := f.failure(name); err != nil {
		return -1, err
	}
	if _, found := f.fdStats[f.rootfsFileFD]; !found {
		f.fdStats[f.rootfsFileFD] = FileInfo{Mode: 0100000, Dev: 1, Ino: 12}
	}
	return f.rootfsFileFD, nil
}

// Fstat returns descriptor inode and mode evidence.
func (f *fakeOps) Fstat(fd int) (FileInfo, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.record(fmt.Sprintf("fstat:%d", fd), false)
	if err := f.failure(fmt.Sprintf("fstat:%d", fd)); err != nil {
		return FileInfo{}, err
	}
	value, exists := f.fdStats[fd]
	if !exists {
		return FileInfo{}, fmt.Errorf("missing fake stat fd %d", fd)
	}
	return value, nil
}

// FstatFS returns descriptor filesystem identity.
func (f *fakeOps) FstatFS(fd int) (FileSystemInfo, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.record(fmt.Sprintf("fstatfs:%d", fd), false)
	if err := f.failure(fmt.Sprintf("fstatfs:%d", fd)); err != nil {
		return FileSystemInfo{}, err
	}
	value, exists := f.fdFS[fd]
	if !exists {
		return FileSystemInfo{}, fmt.Errorf("missing fake fstatfs fd %d", fd)
	}
	return value, nil
}

// ReadlinkFD returns the fake kernel spelling for one namespace descriptor.
func (f *fakeOps) ReadlinkFD(fd int) (string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.record(fmt.Sprintf("readlink-fd:%d", fd), false)
	if err := f.failure(fmt.Sprintf("readlink-fd:%d", fd)); err != nil {
		return "", err
	}
	value, exists := f.fdLinks[fd]
	if !exists {
		return "", fmt.Errorf("missing fake descriptor link %d", fd)
	}
	return value, nil
}

// Close records descriptor release and removes dynamic fake descriptors.
func (f *fakeOps) Close(fd int) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	name := fmt.Sprintf("close:%d", fd)
	f.record(name, false)
	if err := f.failure(name); err != nil {
		return err
	}
	delete(f.fdStats, fd)
	delete(f.fdFS, fd)
	delete(f.fdLinks, fd)
	delete(f.files, fmt.Sprintf("/proc/self/fdinfo/%d", fd))
	return nil
}

// Dup returns an independently tracked fake descriptor with identical stat, filesystem, and link evidence.
func (f *fakeOps) Dup(fd int) (int, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	name := fmt.Sprintf("dup:%d", fd)
	f.record(name, false)
	if err := f.failure(name); err != nil {
		return -1, err
	}
	duplicate := f.nextFD
	f.nextFD++
	f.fdStats[duplicate] = f.fdStats[fd]
	f.fdFS[duplicate] = f.fdFS[fd]
	f.fdLinks[duplicate] = f.fdLinks[fd]
	return duplicate, nil
}

// PidfdOpen returns a stable fake pidfd after recording the requested PID.
func (f *fakeOps) PidfdOpen(pid int) (int, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	name := fmt.Sprintf("pidfd-open:%d", pid)
	f.record(name, false)
	if err := f.failure(name); err != nil {
		return -1, err
	}
	fd := f.nextFD
	f.nextFD++
	f.files[fmt.Sprintf("/proc/self/fdinfo/%d", fd)] = []byte(fmt.Sprintf("pos:\t0\nflags:\t02000002\nPid:\t%d\n", pid))
	return fd, nil
}

// PidfdSendSignal records liveness probes separately from nonzero signal mutations.
func (f *fakeOps) PidfdSendSignal(pidfd, signal int) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	name := fmt.Sprintf("pidfd-signal:%d:%d", pidfd, signal)
	f.record(name, signal != 0)
	if err := f.failure(fmt.Sprintf("pidfd-signal:%d", signal)); err != nil {
		return err
	}
	return nil
}

// setns updates only the fake current-thread namespace map and records the mutation.
func (f *fakeOps) setns(fd, namespaceFlag int) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	name := fmt.Sprintf("setns:%d:%d", fd, namespaceFlag)
	f.record(name, true)
	f.setnsCalls++
	if err := f.failure("setns"); err != nil {
		return err
	}
	if err := f.setnsFailAt[f.setnsCalls]; err != nil {
		return err
	}
	kind, inode, err := parseNamespaceLink(f.fdLinks[fd])
	if err != nil {
		return err
	}
	if f.setnsInode != 0 {
		inode = f.setnsInode
		f.setnsInode = 0
	}
	f.currentNS[kind] = inode
	return nil
}

// unshare records creation of a fake namespace.
func (f *fakeOps) unshare(flags int) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	name := fmt.Sprintf("unshare:%d", flags)
	f.record(name, true)
	if err := f.failure("unshare"); err != nil {
		return err
	}
	if f.unshareNoop {
		return nil
	}
	for _, namespaceType := range []NamespaceType{NamespaceUTS, NamespaceIPC, NamespaceNetwork, NamespacePID, NamespaceMount} {
		flag, err := namespaceCloneFlag(namespaceType)
		if err != nil {
			return err
		}
		if flags&flag != 0 {
			f.currentNS[namespaceType] += 1_000
		}
	}
	return nil
}

// hostname returns the fake active UTS nodename without recording a mutation.
func (f *fakeOps) hostname() (string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.record("hostname", false)
	if err := f.failure("hostname"); err != nil {
		return "", err
	}
	return f.hostnameValue, nil
}

// setHostname replaces the fake active UTS nodename and records the privileged mutation.
func (f *fakeOps) setHostname(hostname string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.record("sethostname:"+hostname, true)
	if err := f.failure("sethostname"); err != nil {
		return err
	}
	f.hostnameValue = hostname
	return nil
}

// loopbackUp returns the fake active network namespace's loopback state.
func (f *fakeOps) loopbackUp() (bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.record("loopback", false)
	if err := f.failure("loopback"); err != nil {
		return false, err
	}
	return f.loopbackValue, nil
}

// setLoopbackUp replaces the fake loopback state and records the privileged mutation.
func (f *fakeOps) setLoopbackUp(up bool) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.record("set-loopback:"+strconv.FormatBool(up), true)
	if err := f.failure("set-loopback"); err != nil {
		return err
	}
	f.loopbackValue = up
	return nil
}

// mount records one fake mount in exact invocation order.
func (f *fakeOps) mount(source, target, filesystem string, flags uintptr, data string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	name := fmt.Sprintf("mount:%s:%s:%s:%d:%s", source, target, filesystem, flags, data)
	f.record(name, true)
	return f.failure("mount:" + target)
}

// unmount records one fake detach operation.
func (f *fakeOps) unmount(target string, flags int) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	name := fmt.Sprintf("unmount:%s:%d", target, flags)
	f.record(name, true)
	return f.failure("unmount:" + target)
}

// pivotRoot records the fake pivot boundary.
func (f *fakeOps) pivotRoot(newRoot, putOld string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	name := "pivot:" + newRoot + ":" + putOld
	f.record(name, true)
	return f.failure("pivot")
}

// mkdir records one fake rootfs directory creation.
func (f *fakeOps) mkdir(path string, mode uint32) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	name := fmt.Sprintf("mkdir:%s:%o", path, mode)
	f.record(name, true)
	return f.failure("mkdir:" + path)
}

// remove records one fake empty-directory removal.
func (f *fakeOps) remove(path string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	name := "remove:" + path
	f.record(name, true)
	return f.failure("remove:" + path)
}

// chdir records one fake runtime-helper working-directory change.
func (f *fakeOps) chdir(path string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	name := "chdir:" + path
	f.record(name, true)
	return f.failure("chdir:" + path)
}
