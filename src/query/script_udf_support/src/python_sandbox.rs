// Copyright 2021 Datafuse Labs
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// Unprivileged OS sandbox of a Python UDF worker, entered after the
// interpreter has started and before any user code runs:
//
// - Landlock: files are readable only below the Python installation, system
//   libraries and a few device/proc entries, and writable only below the
//   worker's private directory (its cwd and `TMPDIR`).
// - seccomp: no sockets (no network, no Unix sockets), no `execve`, no
//   signals to processes outside the worker, no ptrace/io_uring/bpf/mount/
//   namespace or kernel-module syscalls, no path-based permission changes.
//
// Both need no privileges and work inside containers. Failure to enter the
// sandbox is fatal: the worker reports the error and never runs user code.
//
// macOS has neither mechanism and is only used for local development: workers
// run unconfined there (the query node logs a warning at startup).

#[cfg(target_os = "linux")]
use std::path::Path;
#[cfg(not(target_os = "macos"))]
use std::path::PathBuf;

/// Whether workers on this platform run user code inside the sandbox.
pub const PYTHON_SANDBOX_ENFORCED: bool = !cfg!(target_os = "macos");

/// Confine the calling worker before it runs user code. Must run while the
/// process is single-threaded.
#[cfg(not(target_os = "macos"))]
pub fn sandbox_python_worker() -> Result<(), String> {
    enter_python_sandbox(&python_read_paths()?)
}

/// Local development only: no Landlock/seccomp on macOS.
#[cfg(target_os = "macos")]
pub fn sandbox_python_worker() -> Result<(), String> {
    Ok(())
}

/// Directories and files the Python runtime reads after initialization.
/// Missing entries are skipped; they differ between distributions.
#[cfg(target_os = "linux")]
const SYSTEM_READ_PATHS: &[&str] = &[
    "/lib",
    "/lib32",
    "/lib64",
    "/usr/lib",
    "/usr/lib32",
    "/usr/lib64",
    "/usr/local/lib",
    "/usr/share/zoneinfo",
    "/etc/ld.so.cache",
    "/etc/localtime",
    "/dev/zero",
    "/dev/random",
    "/dev/urandom",
    "/proc/cpuinfo",
    "/sys/devices/system/cpu",
];

/// Paths Python needs to import modules: the stdlib, `lib-dynload` and
/// site-packages of the (virtual) environment and of its base installation,
/// plus every `sys.path` entry.
#[cfg(not(target_os = "macos"))]
fn python_read_paths() -> Result<Vec<PathBuf>, String> {
    use pyo3::types::PyAnyMethods;
    use pyo3::types::PyDict;
    use pyo3::types::PyDictMethods;

    pyo3::Python::attach(|py| {
        // One namespace: comprehensions cannot see a separate locals dict.
        let namespace = PyDict::new(py);
        py.run(
            cr#"
import sys, sysconfig
paths = set(p for p in sys.path if p)
for base, platbase in ((sys.prefix, sys.exec_prefix), (sys.base_prefix, sys.base_exec_prefix)):
    scheme = sysconfig.get_paths(vars={
        'base': base, 'platbase': platbase,
        'installed_base': base, 'installed_platbase': platbase,
    })
    paths.update(scheme[k] for k in ('stdlib', 'platstdlib', 'purelib', 'platlib'))
paths = sorted(paths)
"#,
            Some(&namespace),
            None,
        )?;
        namespace
            .get_item("paths")?
            .ok_or_else(|| pyo3::exceptions::PyKeyError::new_err("paths"))?
            .extract::<Vec<String>>()
    })
    .map(|paths| paths.into_iter().map(PathBuf::from).collect())
    .map_err(|err| format!("cannot resolve Python paths: {err}"))
}

/// Confine the calling process. Must run while it is single-threaded: a
/// Landlock domain only covers the calling thread and its future children.
#[cfg(not(target_os = "macos"))]
fn enter_python_sandbox(python_paths: &[PathBuf]) -> Result<(), String> {
    let private_dir = std::env::current_dir()
        .and_then(std::fs::canonicalize)
        .map_err(|err| format!("cannot resolve worker directory: {err}"))?;
    if private_dir.parent().is_none() {
        return Err("worker directory must not be the filesystem root".into());
    }
    let mut read_only: Vec<PathBuf> = python_paths.to_vec();
    if let Some(paths) = std::env::var_os("LD_LIBRARY_PATH") {
        read_only.extend(std::env::split_paths(&paths));
    }
    enter_impl(read_only, private_dir).map_err(|err| {
        format!("Python UDF sandbox is unavailable, refusing to run user code: {err}")
    })
}

#[cfg(not(any(target_os = "linux", target_os = "macos")))]
fn enter_impl(_read_only: Vec<PathBuf>, _private_dir: PathBuf) -> Result<(), String> {
    Err("Python UDF workers can only be sandboxed on Linux (Landlock and seccomp)".into())
}

#[cfg(target_os = "linux")]
fn enter_impl(mut read_only: Vec<PathBuf>, private_dir: PathBuf) -> Result<(), String> {
    let threads = std::fs::read_dir("/proc/self/task")
        .map_err(|err| format!("cannot count threads: {err}"))?
        .count();
    if threads != 1 {
        return Err(format!(
            "worker has {threads} threads; the sandbox must be entered single-threaded"
        ));
    }
    read_only.extend(SYSTEM_READ_PATHS.iter().map(PathBuf::from));
    // Resolves to /proc/<pid>: other processes' entries stay unreadable.
    read_only.push(PathBuf::from("/proc/self"));

    let abi = landlock::abi().map_err(|err| {
        format!("Landlock is not available (needs Linux 5.13+ with 'landlock' in the LSM list): {err}")
    })?;
    let ruleset = landlock::Ruleset::new(abi).map_err(|err| format!("Landlock: {err}"))?;
    for path in &read_only {
        // An operator-provided search path of "/" would expose everything.
        if path.as_os_str().is_empty() || is_root(path) {
            log::warn!("Python UDF sandbox ignores read path {path:?}");
            continue;
        }
        ruleset
            .allow(path, landlock::READ)
            .map_err(|err| format!("Landlock rule for {path:?}: {err}"))?;
    }
    let dev_null = Path::new("/dev/null");
    ruleset
        .allow(dev_null, landlock::READ | landlock::WRITE)
        .map_err(|err| format!("Landlock rule for {dev_null:?}: {err}"))?;
    if !ruleset
        .allow(&private_dir, landlock::ALL)
        .map_err(|err| format!("Landlock rule for {private_dir:?}: {err}"))?
    {
        return Err(format!("worker directory {private_dir:?} does not exist"));
    }
    ruleset
        .restrict_self()
        .map_err(|err| format!("Landlock: {err}"))?;
    seccomp::install(abi).map_err(|err| format!("seccomp: {err}"))
}

#[cfg(target_os = "linux")]
fn is_root(path: &Path) -> bool {
    std::fs::canonicalize(path).is_ok_and(|path| path.parent().is_none())
}
/// Raw Landlock ABI (include/uapi/linux/landlock.h). Rights unknown to the
/// running kernel are masked out; everything the kernel knows is handled, so
/// any access not granted below is denied.
#[cfg(target_os = "linux")]
mod landlock {
    use std::ffi::CString;
    use std::io;
    use std::os::fd::AsRawFd;
    use std::os::fd::FromRawFd;
    use std::os::fd::OwnedFd;
    use std::os::unix::ffi::OsStrExt;
    use std::path::Path;

    const EXECUTE: u64 = 1 << 0;
    const WRITE_FILE: u64 = 1 << 1;
    const READ_FILE: u64 = 1 << 2;
    const READ_DIR: u64 = 1 << 3;
    /// EXECUTE..=MAKE_SYM: the 13 rights of ABI 1.
    const ABI_1_FS: u64 = (1 << 13) - 1;
    const REFER: u64 = 1 << 13; // ABI 2
    const TRUNCATE: u64 = 1 << 14; // ABI 3
    const IOCTL_DEV: u64 = 1 << 15; // ABI 5
    /// The only rights a rule on a non-directory may carry.
    const FILE_RIGHTS: u64 = EXECUTE | WRITE_FILE | READ_FILE | TRUNCATE | IOCTL_DEV;
    const NET_BIND_TCP: u64 = 1 << 0; // ABI 4
    const NET_CONNECT_TCP: u64 = 1 << 1; // ABI 4
    const SCOPE_ABSTRACT_UNIX_SOCKET: u64 = 1 << 0; // ABI 6
    const SCOPE_SIGNAL: u64 = 1 << 1; // ABI 6
    const CREATE_RULESET_VERSION: u32 = 1 << 0;
    const RULE_PATH_BENEATH: libc::c_int = 1;

    pub const READ: u64 = READ_FILE | READ_DIR;
    pub const WRITE: u64 = WRITE_FILE | TRUNCATE | IOCTL_DEV;
    pub const ALL: u64 = u64::MAX;

    #[repr(C)]
    struct RulesetAttr {
        handled_access_fs: u64,
        handled_access_net: u64,
        scoped: u64,
    }

    #[repr(C, packed)]
    struct PathBeneathAttr {
        allowed_access: u64,
        parent_fd: i32,
    }

    fn check(result: libc::c_long) -> io::Result<libc::c_long> {
        if result < 0 {
            Err(io::Error::last_os_error())
        } else {
            Ok(result)
        }
    }

    pub fn abi() -> io::Result<u32> {
        // SAFETY: the version query takes no attribute and writes no memory.
        let abi = check(unsafe {
            libc::syscall(
                libc::SYS_landlock_create_ruleset,
                std::ptr::null::<RulesetAttr>(),
                0usize,
                CREATE_RULESET_VERSION,
            )
        })?;
        Ok(abi as u32)
    }

    pub struct Ruleset {
        fd: OwnedFd,
        handled_fs: u64,
    }

    impl Ruleset {
        pub fn new(abi: u32) -> io::Result<Self> {
            let mut handled_fs = ABI_1_FS;
            for (min_abi, right) in [(2, REFER), (3, TRUNCATE), (5, IOCTL_DEV)] {
                if abi >= min_abi {
                    handled_fs |= right;
                }
            }
            let attr = RulesetAttr {
                handled_access_fs: handled_fs,
                // No TCP rule is added: handling the rights denies them all.
                handled_access_net: if abi >= 4 {
                    NET_BIND_TCP | NET_CONNECT_TCP
                } else {
                    0
                },
                // Signals and abstract sockets may only reach this domain.
                scoped: if abi >= 6 {
                    SCOPE_ABSTRACT_UNIX_SOCKET | SCOPE_SIGNAL
                } else {
                    0
                },
            };
            // Older kernels reject an attribute larger than they know.
            let size = match abi {
                1..=3 => 8,
                4..=5 => 16,
                _ => std::mem::size_of::<RulesetAttr>(),
            };
            // SAFETY: attr outlives the call and `size` does not exceed it.
            let fd = check(unsafe {
                libc::syscall(libc::SYS_landlock_create_ruleset, &attr, size, 0u32)
            })?;
            // SAFETY: the kernel returned a fresh descriptor we now own.
            let fd = unsafe { OwnedFd::from_raw_fd(fd as i32) };
            Ok(Self { fd, handled_fs })
        }

        /// Grant `access` beneath `path`. Returns `false` if it does not exist.
        pub fn allow(&self, path: &Path, access: u64) -> io::Result<bool> {
            let c_path = CString::new(path.as_os_str().as_bytes())?;
            // SAFETY: c_path is a valid NUL-terminated string.
            let fd = unsafe { libc::open(c_path.as_ptr(), libc::O_PATH | libc::O_CLOEXEC) };
            if fd < 0 {
                let error = io::Error::last_os_error();
                return match error.raw_os_error() {
                    Some(libc::ENOENT | libc::ENOTDIR) => Ok(false),
                    _ => Err(error),
                };
            }
            // SAFETY: open returned a fresh descriptor we now own.
            let fd = unsafe { OwnedFd::from_raw_fd(fd) };
            // SAFETY: stat is plain old data; fstat fully initializes it.
            let mut stat: libc::stat = unsafe { std::mem::zeroed() };
            if unsafe { libc::fstat(fd.as_raw_fd(), &mut stat) } != 0 {
                return Err(io::Error::last_os_error());
            }
            let mut allowed = access & self.handled_fs;
            if stat.st_mode & libc::S_IFMT != libc::S_IFDIR {
                allowed &= FILE_RIGHTS;
            }
            let attr = PathBeneathAttr {
                allowed_access: allowed,
                parent_fd: fd.as_raw_fd(),
            };
            // SAFETY: attr outlives the call; both descriptors are open.
            check(unsafe {
                libc::syscall(
                    libc::SYS_landlock_add_rule,
                    self.fd.as_raw_fd(),
                    RULE_PATH_BENEATH,
                    &attr,
                    0u32,
                )
            })?;
            Ok(true)
        }

        pub fn restrict_self(self) -> io::Result<()> {
            // SAFETY: prctl with integer arguments has no memory side effects.
            if unsafe { libc::prctl(libc::PR_SET_NO_NEW_PRIVS, 1, 0, 0, 0) } != 0 {
                return Err(io::Error::last_os_error());
            }
            // SAFETY: the ruleset descriptor is open.
            check(unsafe { libc::syscall(libc::SYS_landlock_restrict_self, self.fd.as_raw_fd(), 0u32) })?;
            Ok(())
        }
    }
}
/// A deny-list seccomp filter. An allow-list would break arbitrary native
/// extension modules; the dangerous surface is the small set below.
#[cfg(all(
    target_os = "linux",
    any(target_arch = "x86_64", target_arch = "aarch64")
))]
mod seccomp {
    use std::io;

    use libc::c_long;
    use libc::sock_filter;

    const LD_W_ABS: u16 = 0x20; // BPF_LD | BPF_W | BPF_ABS
    const JEQ_K: u16 = 0x15; // BPF_JMP | BPF_JEQ | BPF_K
    #[cfg(target_arch = "x86_64")]
    const JGE_K: u16 = 0x35; // BPF_JMP | BPF_JGE | BPF_K
    const JSET_K: u16 = 0x45; // BPF_JMP | BPF_JSET | BPF_K
    const RET_K: u16 = 0x06; // BPF_RET | BPF_K

    const RET_ALLOW: u32 = 0x7fff_0000;
    const RET_KILL_PROCESS: u32 = 0x8000_0000;
    const RET_ERRNO: u32 = 0x0005_0000;

    // struct seccomp_data offsets; argument low words (little endian) so
    // `int` arguments compare correctly whatever the upper half holds.
    const DATA_NR: u32 = 0;
    const DATA_ARCH: u32 = 4;
    const DATA_ARG0_LOW: u32 = 16;

    #[cfg(target_arch = "x86_64")]
    const AUDIT_ARCH: u32 = 0xC000_003E;
    #[cfg(target_arch = "aarch64")]
    const AUDIT_ARCH: u32 = 0xC000_00B7;
    /// x86_64 x32 ABI syscalls share AUDIT_ARCH_X86_64 and would bypass the
    /// syscall numbers matched below.
    #[cfg(target_arch = "x86_64")]
    const X32_SYSCALL_BIT: u32 = 0x4000_0000;

    /// Not yet exported by libc for every architecture; same number everywhere.
    const SYS_FCHMODAT2: c_long = 452;

    const NAMESPACE_FLAGS: u32 = (libc::CLONE_NEWNS
        | libc::CLONE_NEWCGROUP
        | libc::CLONE_NEWUTS
        | libc::CLONE_NEWIPC
        | libc::CLONE_NEWUSER
        | libc::CLONE_NEWPID
        | libc::CLONE_NEWNET) as u32;

    /// Always rejected with EPERM.
    const DENIED: &[c_long] = &[
        // Spawning programs: they would start outside the Python runtime.
        libc::SYS_execve,
        libc::SYS_execveat,
        // Reading or controlling other processes.
        libc::SYS_ptrace,
        libc::SYS_process_vm_readv,
        libc::SYS_process_vm_writev,
        libc::SYS_pidfd_getfd,
        libc::SYS_pidfd_send_signal,
        libc::SYS_process_madvise,
        libc::SYS_process_mrelease,
        // Thread ids cannot be checked against this process.
        libc::SYS_tkill,
        // io_uring submits socket/file operations that seccomp never sees.
        libc::SYS_io_uring_setup,
        libc::SYS_io_uring_enter,
        libc::SYS_io_uring_register,
        // Kernel attack surface without a use in UDFs.
        libc::SYS_bpf,
        libc::SYS_perf_event_open,
        libc::SYS_userfaultfd,
        libc::SYS_keyctl,
        libc::SYS_add_key,
        libc::SYS_request_key,
        libc::SYS_unshare,
        libc::SYS_setns,
        libc::SYS_mount,
        libc::SYS_umount2,
        libc::SYS_mount_setattr,
        libc::SYS_move_mount,
        libc::SYS_open_tree,
        libc::SYS_fsopen,
        libc::SYS_fsconfig,
        libc::SYS_fsmount,
        libc::SYS_fspick,
        libc::SYS_pivot_root,
        libc::SYS_chroot,
        libc::SYS_open_by_handle_at,
        libc::SYS_name_to_handle_at,
        libc::SYS_fanotify_init,
        libc::SYS_init_module,
        libc::SYS_finit_module,
        libc::SYS_delete_module,
        libc::SYS_kexec_load,
        libc::SYS_kexec_file_load,
        libc::SYS_reboot,
        libc::SYS_swapon,
        libc::SYS_swapoff,
        libc::SYS_acct,
        libc::SYS_quotactl,
        libc::SYS_lookup_dcookie,
        libc::SYS_vhangup,
        // Landlock does not cover metadata: changing mode, owner or extended
        // attributes of query node files by path. fd-based variants remain.
        libc::SYS_fchmodat,
        SYS_FCHMODAT2,
        libc::SYS_fchownat,
        libc::SYS_setxattr,
        libc::SYS_lsetxattr,
        libc::SYS_removexattr,
        libc::SYS_lremovexattr,
        #[cfg(target_arch = "x86_64")]
        libc::SYS_chmod,
        #[cfg(target_arch = "x86_64")]
        libc::SYS_chown,
        #[cfg(target_arch = "x86_64")]
        libc::SYS_lchown,
        #[cfg(target_arch = "x86_64")]
        libc::SYS_iopl,
        #[cfg(target_arch = "x86_64")]
        libc::SYS_ioperm,
        #[cfg(target_arch = "x86_64")]
        libc::SYS_uselib,
    ];

    enum Rule {
        Errno(i32),
        /// kill(): only this process (0, pid) or its own group (-pid).
        Kill,
        /// tgkill()/rt_*sigqueueinfo(): only threads of this process.
        SelfOnly,
        /// clone(): no new namespaces.
        Clone,
    }

    fn stmt(code: u16, k: u32) -> sock_filter {
        sock_filter {
            code,
            jt: 0,
            jf: 0,
            k,
        }
    }

    fn jump(code: u16, k: u32, jt: u8, jf: u8) -> sock_filter {
        sock_filter { code, jt, jf, k }
    }

    fn errno(value: i32) -> sock_filter {
        stmt(RET_K, RET_ERRNO | (value as u32 & 0xffff))
    }

    fn body(rule: &Rule, pid: i32) -> Vec<sock_filter> {
        match rule {
            Rule::Errno(value) => vec![errno(*value)],
            Rule::Kill => vec![
                stmt(LD_W_ABS, DATA_ARG0_LOW),
                jump(JEQ_K, 0, 3, 0),
                jump(JEQ_K, pid as u32, 2, 0),
                jump(JEQ_K, pid.wrapping_neg() as u32, 1, 0),
                errno(libc::EPERM),
                stmt(RET_K, RET_ALLOW),
            ],
            Rule::SelfOnly => vec![
                stmt(LD_W_ABS, DATA_ARG0_LOW),
                jump(JEQ_K, pid as u32, 1, 0),
                errno(libc::EPERM),
                stmt(RET_K, RET_ALLOW),
            ],
            Rule::Clone => vec![
                stmt(LD_W_ABS, DATA_ARG0_LOW),
                jump(JSET_K, NAMESPACE_FLAGS, 0, 1),
                errno(libc::EPERM),
                stmt(RET_K, RET_ALLOW),
            ],
        }
    }

    pub fn program(landlock_abi: u32, pid: i32) -> Vec<sock_filter> {
        let mut rules = vec![
            // No sockets at all: no network and no Unix sockets of other
            // services. The IPC connection exists before the filter.
            (libc::SYS_socket, Rule::Errno(libc::EACCES)),
            // clone3 flags live in memory BPF cannot read. ENOSYS makes libc
            // fall back to clone(), whose flags are checked.
            (libc::SYS_clone3, Rule::Errno(libc::ENOSYS)),
            (libc::SYS_clone, Rule::Clone),
            (libc::SYS_kill, Rule::Kill),
            (libc::SYS_tgkill, Rule::SelfOnly),
            (libc::SYS_rt_sigqueueinfo, Rule::SelfOnly),
            (libc::SYS_rt_tgsigqueueinfo, Rule::SelfOnly),
        ];
        rules.extend(DENIED.iter().map(|nr| (*nr, Rule::Errno(libc::EPERM))));
        if landlock_abi < 3 {
            // Landlock controls truncation from ABI 3 on.
            rules.push((libc::SYS_truncate, Rule::Errno(libc::EPERM)));
        }

        let mut filter = vec![
            stmt(LD_W_ABS, DATA_ARCH),
            jump(JEQ_K, AUDIT_ARCH, 1, 0),
            stmt(RET_K, RET_KILL_PROCESS),
            stmt(LD_W_ABS, DATA_NR),
        ];
        #[cfg(target_arch = "x86_64")]
        filter.extend([
            jump(JGE_K, X32_SYSCALL_BIT, 0, 1),
            errno(libc::ENOSYS),
        ]);
        for (nr, rule) in &rules {
            // Each body ends in a return, so the accumulator still holds the
            // syscall number for the next comparison.
            let body = body(rule, pid);
            filter.push(jump(JEQ_K, *nr as u32, 0, body.len() as u8));
            filter.extend(body);
        }
        filter.push(stmt(RET_K, RET_ALLOW));
        filter
    }

    pub fn install(landlock_abi: u32) -> io::Result<()> {
        // SAFETY: getpid has no preconditions.
        let filter = program(landlock_abi, unsafe { libc::getpid() });
        let prog = libc::sock_fprog {
            len: filter.len() as u16,
            filter: filter.as_ptr() as *mut sock_filter,
        };
        // SAFETY: prog points to `filter`, which outlives the call; the kernel
        // copies the program. TSYNC applies it to every thread.
        let result = unsafe {
            libc::syscall(
                libc::SYS_seccomp,
                libc::SECCOMP_SET_MODE_FILTER,
                libc::SECCOMP_FILTER_FLAG_TSYNC,
                &prog,
            )
        };
        match result {
            0 => Ok(()),
            r if r < 0 => Err(io::Error::last_os_error()),
            tid => Err(io::Error::other(format!("thread {tid} could not be synchronized"))),
        }
    }
}

#[cfg(all(
    target_os = "linux",
    not(any(target_arch = "x86_64", target_arch = "aarch64"))
))]
mod seccomp {
    pub fn install(_landlock_abi: u32) -> std::io::Result<()> {
        Err(std::io::Error::other("unsupported architecture"))
    }
}

#[cfg(all(
    test,
    target_os = "linux",
    any(target_arch = "x86_64", target_arch = "aarch64")
))]
mod tests {
    use libc::c_long;

    use super::seccomp::program;

    const ALLOW: u32 = 0x7fff_0000;
    const KILL_PROCESS: u32 = 0x8000_0000;
    const PID: i32 = 4242;

    fn errno(value: i32) -> u32 {
        0x0005_0000 | value as u32
    }

    /// Minimal classic-BPF interpreter for the instructions the filter uses.
    fn run(filter: &[libc::sock_filter], arch: u32, nr: c_long, arg0: u64) -> u32 {
        let mut data = [0u8; 64];
        data[0..4].copy_from_slice(&(nr as u32).to_le_bytes());
        data[4..8].copy_from_slice(&arch.to_le_bytes());
        data[16..24].copy_from_slice(&arg0.to_le_bytes());
        let (mut pc, mut acc) = (0usize, 0u32);
        loop {
            let insn = filter[pc];
            let (jt, jf) = (insn.jt as usize, insn.jf as usize);
            pc += 1;
            match insn.code {
                0x20 => {
                    let at = insn.k as usize;
                    acc = u32::from_le_bytes(data[at..at + 4].try_into().unwrap());
                }
                0x15 => pc += if acc == insn.k { jt } else { jf },
                0x35 => pc += if acc >= insn.k { jt } else { jf },
                0x45 => pc += if acc & insn.k != 0 { jt } else { jf },
                0x06 => return insn.k,
                code => panic!("unexpected BPF opcode {code:#x}"),
            }
            assert!(pc < filter.len(), "jump out of the program");
        }
    }

    #[cfg(target_arch = "x86_64")]
    const ARCH: u32 = 0xC000_003E;
    #[cfg(target_arch = "aarch64")]
    const ARCH: u32 = 0xC000_00B7;

    #[test]
    fn test_seccomp_program() {
        let filter = program(7, PID);
        assert!(filter.len() < 4096);
        let check = |nr, arg0| run(&filter, ARCH, nr, arg0);

        assert_eq!(run(&filter, 0x4000_0003, libc::SYS_read, 0), KILL_PROCESS);
        #[cfg(target_arch = "x86_64")]
        assert_eq!(check(0x4000_0000 | libc::SYS_socket, 0), errno(libc::ENOSYS));

        assert_eq!(check(libc::SYS_socket, libc::AF_UNIX as u64), errno(libc::EACCES));
        assert_eq!(check(libc::SYS_socketpair, libc::AF_UNIX as u64), ALLOW);
        assert_eq!(check(libc::SYS_execve, 0), errno(libc::EPERM));
        assert_eq!(check(libc::SYS_io_uring_setup, 0), errno(libc::EPERM));
        assert_eq!(check(libc::SYS_ptrace, 0), errno(libc::EPERM));
        assert_eq!(check(libc::SYS_fchmodat, 0), errno(libc::EPERM));
        assert_eq!(check(libc::SYS_openat, 0), ALLOW);
        assert_eq!(check(libc::SYS_truncate, 0), ALLOW);
        assert_eq!(
            run(&program(2, PID), ARCH, libc::SYS_truncate, 0),
            errno(libc::EPERM)
        );

        // The upper half of an `int` argument must not matter.
        let int = |value: i32| value as u32 as u64 | 0xdead_beef_0000_0000;
        for allowed in [0, PID, -PID] {
            assert_eq!(check(libc::SYS_kill, int(allowed)), ALLOW, "kill({allowed})");
        }
        for denied in [1, -1, PID + 1, -(PID + 1)] {
            assert_eq!(check(libc::SYS_kill, int(denied)), errno(libc::EPERM), "kill({denied})");
        }
        assert_eq!(check(libc::SYS_tgkill, int(PID)), ALLOW);
        assert_eq!(check(libc::SYS_tgkill, int(1)), errno(libc::EPERM));
        assert_eq!(check(libc::SYS_tkill, int(PID)), errno(libc::EPERM));

        let thread = (libc::CLONE_VM | libc::CLONE_THREAD | libc::CLONE_SIGHAND) as u64;
        assert_eq!(check(libc::SYS_clone, thread), ALLOW);
        for namespace in [libc::CLONE_NEWUSER, libc::CLONE_NEWNET, libc::CLONE_NEWNS] {
            assert_eq!(
                check(libc::SYS_clone, thread | namespace as u64),
                errno(libc::EPERM)
            );
        }
        assert_eq!(check(libc::SYS_clone3, 0), errno(libc::ENOSYS));
    }
}
