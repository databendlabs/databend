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

use std::fs::File;
use std::io::BufRead;
use std::io::Write;
use std::os::fd::FromRawFd;
use std::os::fd::IntoRawFd;
use std::os::fd::OwnedFd;
#[cfg(test)]
use std::ptr::addr_of_mut;
use std::sync::Mutex;
use std::sync::PoisonError;
use std::time::Duration;

use backtrace::Backtrace;
use backtrace::BacktraceFrame;
use databend_common_base::runtime::Thread;
use databend_common_base::runtime::ThreadTracker;

use crate::panic_hook::captures_frames;

struct CrashHandler {
    write_file: File,
}

impl CrashHandler {
    pub fn create(write_file: File) -> CrashHandler {
        CrashHandler { write_file }
    }

    pub fn recv_signal(&mut self, sig: i32, info: *mut libc::siginfo_t, _uc: *mut libc::c_void) {
        let mut writer = std::io::BufWriter::new(&mut self.write_file);
        let current_query_id = match ThreadTracker::query_id() {
            None => "Unknown",
            Some(query_id) => query_id,
        };

        bincode::serde::encode_into_std_write(sig, &mut writer, bincode::config::standard())
            .unwrap();

        bincode::serde::encode_into_std_write(
            unsafe { (*info).si_code },
            &mut writer,
            bincode::config::standard(),
        )
        .unwrap();

        bincode::serde::encode_into_std_write(
            unsafe { (*info).si_addr() as usize },
            &mut writer,
            bincode::config::standard(),
        )
        .unwrap();

        bincode::serde::encode_into_std_write(
            std::thread::current().id().as_u64(),
            &mut writer,
            bincode::config::standard(),
        )
        .unwrap();

        bincode::serde::encode_into_std_write(
            current_query_id,
            &mut writer,
            bincode::config::standard(),
        )
        .unwrap();

        let mut frames = Vec::with_capacity(50);
        captures_frames(&mut frames);

        bincode::serde::encode_into_std_write(
            frames.len(),
            &mut writer,
            bincode::config::standard(),
        )
        .unwrap();

        writer.flush().unwrap();

        for frame in frames {
            bincode::serde::encode_into_std_write(frame, &mut writer, bincode::config::standard())
                .unwrap();
        }

        writer.flush().unwrap();
        std::thread::sleep(Duration::from_secs(10));
    }
}

static CRASH_HANDLER_LOCK: Mutex<Option<CrashHandler>> = Mutex::new(None);

fn sigsegv_message(si_code: i32, si_addr: usize) -> String {
    let mut address = String::from("null points");

    if si_addr != 0 {
        address = format!("{:#02x?}", si_addr);
    }

    format!(
        "Signal {} ({}), si_code {} ({}), Address {}\n",
        libc::SIGSEGV,
        "SIGSEGV",
        si_code,
        "Unknown", // TODO: SEGV_MAPERR or SEGV_ACCERR
        address,
    )
}

#[cfg(all(test, target_arch = "x86_64"))]
#[repr(C)]
pub struct __jmp_buf([u64; 8]);

#[cfg(all(test, target_arch = "arm"))]
#[repr(C)]
pub struct __jmp_buf([u64; 32]);

#[cfg(all(test, target_arch = "aarch64"))]
#[repr(C)]
pub struct __jmp_buf([u64; 22]);

#[cfg(test)]
#[repr(C)]
pub struct JmpBuffer {
    __jmp_buf: __jmp_buf,
    __fl: u32,
    __ss: [u32; 32],
}

#[cfg(test)]
impl JmpBuffer {
    pub const fn create() -> JmpBuffer {
        #[cfg(target_arch = "x86_64")]
        {
            JmpBuffer {
                __jmp_buf: crate::crash_hook::__jmp_buf([0; 8]),
                __fl: 0,
                __ss: [0; 32],
            }
        }

        #[cfg(target_arch = "arm")]
        {
            JmpBuffer {
                __jmp_buf: crate::crash_hook::__jmp_buf([0; 32]),
                __fl: 0,
                __ss: [0; 32],
            }
        }

        #[cfg(target_arch = "aarch64")]
        {
            JmpBuffer {
                __jmp_buf: crate::crash_hook::__jmp_buf([0; 22]),
                __fl: 0,
                __ss: [0; 32],
            }
        }
    }
}

#[cfg(test)]
extern "C" {
    // https://man7.org/linux/man-pages/man3/sigsetjmp.3p.html
    #[cfg_attr(target_env = "gnu", link_name = "__sigsetjmp")]
    pub fn sigsetjmp(jb: *mut JmpBuffer, save_mask: i32) -> i32;

    // https://man7.org/linux/man-pages/man3/siglongjmp.3p.html
    pub fn siglongjmp(jb: *mut JmpBuffer, val: i32) -> !;
}

#[cfg(test)]
static mut TEST_JMP_BUFFER: JmpBuffer = JmpBuffer::create();

fn sigbus_message(si_code: i32) -> String {
    format!(
        "Signal {} ({}), si_code {} ({})\n",
        libc::SIGBUS,
        "SIGBUS",
        si_code,
        match si_code {
            libc::BUS_ADRALN => "BUS_ADRALN, invalid address alignment",
            libc::BUS_ADRERR => "BUS_ADRERR, non-existent physical address",
            libc::BUS_OBJERR => "BUS_OBJERR, object specific hardware error",
            _ => "Unknown",
        },
    )
}

fn sigill_message(si_code: i32, si_addr: usize) -> String {
    format!(
        "Signal {} ({}), si_code {} ({})， instruction address:{} \n",
        libc::SIGILL,
        "SIGILL",
        si_code,
        "Unknown", /* ILL_ILLOPC ILL_ILLOPN ILL_ILLADR ILL_ILLTRP ILL_PRVOPC ILL_PRVREG ILL_COPROC ILL_BADSTK, */
        match si_addr == 0 {
            true => "null points".to_string(),
            false => format!("{:#02x?}", si_addr),
        },
    )
}

fn sigfpe_message(si_code: i32, si_addr: usize) -> String {
    format!(
        "Signal {} ({}), si_code {} ({})， instruction address:{} \n",
        libc::SIGFPE,
        "SIGFPE",
        si_code,
        "Unknown", /* FPE_INTDIV FPE_INTOVF FPE_FLTDIV FPE_FLTOVF FPE_FLTUND FPE_FLTRES FPE_FLTINV FPE_FLTSUB */
        match si_addr == 0 {
            true => "null points".to_string(),
            false => format!("{:#02x?}", si_addr),
        },
    )
}

fn signal_message(sig: i32, si_code: i32, si_addr: usize) -> String {
    // https://pubs.opengroup.org/onlinepubs/007908799/xsh/signal.h.html
    match sig {
        libc::SIGBUS => sigbus_message(si_code),
        libc::SIGILL => sigill_message(si_code, si_addr),
        libc::SIGSEGV => sigsegv_message(si_code, si_addr),
        libc::SIGFPE => sigfpe_message(si_code, si_addr),
        _ => format!("Signal {}, si_code {}", sig, si_code),
    }
}

unsafe extern "C" fn signal_handler(sig: i32, info: *mut libc::siginfo_t, uc: *mut libc::c_void) {
    let lock = CRASH_HANDLER_LOCK.lock();
    let mut guard = lock.unwrap_or_else(PoisonError::into_inner);

    if let Some(crash_handler) = guard.as_mut() {
        crash_handler.recv_signal(sig, info, uc);
    }

    #[cfg(test)]
    {
        drop(guard);
        siglongjmp(addr_of_mut!(TEST_JMP_BUFFER), 1);
    }

    #[allow(unreachable_code)]
    if sig != libc::SIGTRAP {
        drop(guard);
        let _ = std::io::stderr().flush();
        std::process::exit(1);
    }
}

pub unsafe fn add_signal_handler(signals: Vec<i32>) {
    let mut sa = std::mem::zeroed::<libc::sigaction>();

    sa.sa_flags = libc::SA_ONSTACK | libc::SA_SIGINFO;
    sa.sa_sigaction = signal_handler as usize;

    libc::sigemptyset(&mut sa.sa_mask);

    for signal in &signals {
        libc::sigaddset(&mut sa.sa_mask, *signal);
    }

    for signal in &signals {
        libc::sigaction(*signal, &sa, std::ptr::null_mut());
    }
}

// https://man7.org/linux/man-pages/man2/sigaltstack.2.html
pub unsafe fn add_signal_stack(stack_bytes: usize) {
    let page_size = libc::sysconf(libc::_SC_PAGESIZE) as usize;
    let alloc_size = page_size + stack_bytes;

    let stack_memory_arena = libc::mmap(
        std::ptr::null_mut(),
        alloc_size,
        libc::PROT_NONE,
        libc::MAP_PRIVATE | libc::MAP_ANON,
        -1,
        0,
    );

    if stack_memory_arena == libc::MAP_FAILED {
        return;
    }

    let stack_ptr = (stack_memory_arena as usize + page_size) as *mut libc::c_void;

    if libc::mprotect(stack_ptr, stack_bytes, libc::PROT_READ | libc::PROT_WRITE) != 0 {
        libc::munmap(stack_ptr, alloc_size);
        return;
    }

    let mut new_signal_stack = std::mem::zeroed::<libc::stack_t>();
    new_signal_stack.ss_sp = stack_memory_arena;
    new_signal_stack.ss_size = stack_bytes;
    if libc::sigaltstack(&new_signal_stack, std::ptr::null_mut()) != 0 {
        libc::munmap(stack_ptr, alloc_size);
    }
}

pub fn set_crash_hook(output: File) {
    let lock = CRASH_HANDLER_LOCK.lock();
    let mut guard = lock.unwrap_or_else(PoisonError::into_inner);

    *guard = Some(CrashHandler::create(output));
    unsafe {
        #[cfg(debug_assertions)]
        add_signal_stack(20 * 1024 * 1024);

        #[cfg(not(debug_assertions))]
        add_signal_stack(2 * 1024 * 1024);

        add_signal_handler(vec![
            libc::SIGSEGV,
            libc::SIGILL,
            libc::SIGBUS,
            libc::SIGFPE,
            libc::SIGSYS,
            libc::SIGTRAP,
        ]);
    };
}

#[cfg(not(target_os = "macos"))]
fn open_pipe() -> std::io::Result<(OwnedFd, OwnedFd)> {
    unsafe {
        let mut fds: [libc::c_int; 2] = [0; 2];
        if libc::pipe2(fds.as_mut_ptr(), libc::O_CLOEXEC) != 0 {
            return Err(std::io::Error::last_os_error());
        }
        Ok((OwnedFd::from_raw_fd(fds[0]), OwnedFd::from_raw_fd(fds[1])))
    }
}

#[cfg(target_os = "macos")]
fn open_pipe() -> std::io::Result<(OwnedFd, OwnedFd)> {
    unsafe {
        let mut fds: [libc::c_int; 2] = [0; 2];

        if libc::pipe(fds.as_mut_ptr()) != 0 {
            return Err(std::io::Error::last_os_error());
        }

        if libc::fcntl(fds[0], libc::F_SETFD, libc::FD_CLOEXEC) != 0 {
            return Err(std::io::Error::last_os_error());
        }

        if libc::fcntl(fds[1], libc::F_SETFD, libc::FD_CLOEXEC) != 0 {
            return Err(std::io::Error::last_os_error());
        }

        Ok((OwnedFd::from_raw_fd(fds[0]), OwnedFd::from_raw_fd(fds[1])))
    }
}

pub fn pipe_file() -> std::io::Result<(File, File)> {
    let (ifd, ofd) = open_pipe()?;

    unsafe {
        Ok((
            File::from_raw_fd(ifd.into_raw_fd()),
            File::from_raw_fd(ofd.into_raw_fd()),
        ))
    }
}

pub struct SignalListener;

impl SignalListener {
    pub fn spawn(file: File, crash_version: String) {
        Thread::named_spawn(Some(String::from("SignalListener")), move || {
            let mut reader = std::io::BufReader::new(file);
            while let Ok(true) = reader.has_data_left() {
                let sig: i32 =
                    bincode::serde::decode_from_reader(&mut reader, bincode::config::standard())
                        .unwrap();
                let si_code: i32 =
                    bincode::serde::decode_from_reader(&mut reader, bincode::config::standard())
                        .unwrap();
                let si_addr: usize =
                    bincode::serde::decode_from_reader(&mut reader, bincode::config::standard())
                        .unwrap();

                let crash_thread_id: u64 =
                    bincode::serde::decode_from_reader(&mut reader, bincode::config::standard())
                        .unwrap();
                let crash_query_id: String =
                    bincode::serde::decode_from_reader(&mut reader, bincode::config::standard())
                        .unwrap();
                let frame_size: usize =
                    bincode::serde::decode_from_reader(&mut reader, bincode::config::standard())
                        .unwrap();

                let mut frames = Vec::<BacktraceFrame>::with_capacity(frame_size);
                for _index in 0..frame_size {
                    frames.push(
                        bincode::serde::decode_from_reader(
                            &mut reader,
                            bincode::config::standard(),
                        )
                        .unwrap(),
                    );
                }

                let mut backtrace = Backtrace::from(frames);
                backtrace.resolve();

                eprintln!("{:#^80}", " Crash fault info ");
                eprintln!("PID: {}", std::process::id());
                eprintln!("TID: {}", crash_thread_id);
                eprintln!("Version: {}", crash_version);
                eprintln!("Timestamp(UTC): {}", chrono::Utc::now());
                eprintln!("Timestamp(Local): {}", chrono::Local::now());
                eprintln!("QueryId: {:?}", crash_query_id);
                eprintln!("{}", signal_message(sig, si_code, si_addr));
                eprintln!("Backtrace:\n {:?}", backtrace);

                let _ = std::io::stderr().flush();

                log::error!("{:#^80}", " Crash fault info ");
                log::error!("PID: {}", std::process::id());
                log::error!("TID: {}", crash_thread_id);
                log::error!("Version: {}", crash_version);
                log::error!("Timestamp(UTC): {}", chrono::Utc::now());
                log::error!("Timestamp(Local): {}", chrono::Local::now());
                log::error!("QueryId: {:?}", crash_query_id);
                log::error!("{}", signal_message(sig, si_code, si_addr));
                log::error!("Backtrace:\n {:?}", backtrace);
            }
        });
    }
}

#[cfg(test)]
mod tests {
    use std::ptr::addr_of_mut;

    use backtrace::BacktraceFrame;
    use databend_common_base::runtime::ThreadTracker;

    use crate::crash_hook::pipe_file;
    use crate::crash_hook::sigsetjmp;
    use crate::crash_hook::TEST_JMP_BUFFER;
    use crate::set_crash_hook;

    #[test]
    fn test_crash() {
        unsafe {
            let (input, output) = pipe_file().unwrap();
            set_crash_hook(output);
            let mut reader = std::io::BufReader::new(input);

            for signal in [
                libc::SIGSEGV,
                libc::SIGILL,
                libc::SIGBUS,
                libc::SIGFPE,
                libc::SIGSYS,
                libc::SIGTRAP,
            ] {
                let query_id = format!("Trakcing query id: {}", signal);
                let mut tracking_payload = ThreadTracker::new_tracking_payload();
                tracking_payload.query_id = Some(query_id.clone());

                let _guard = ThreadTracker::tracking(tracking_payload);

                if sigsetjmp(addr_of_mut!(TEST_JMP_BUFFER), 1) == 0 {
                    libc::raise(signal);
                }

                let sig: i32 =
                    bincode::serde::decode_from_reader(&mut reader, bincode::config::standard())
                        .unwrap();
                let _si_code: i32 =
                    bincode::serde::decode_from_reader(&mut reader, bincode::config::standard())
                        .unwrap();
                let _si_addr: usize =
                    bincode::serde::decode_from_reader(&mut reader, bincode::config::standard())
                        .unwrap();

                let _crash_thread_id: u64 =
                    bincode::serde::decode_from_reader(&mut reader, bincode::config::standard())
                        .unwrap();
                let crash_query_id: String =
                    bincode::serde::decode_from_reader(&mut reader, bincode::config::standard())
                        .unwrap();
                let frame_size: usize =
                    bincode::serde::decode_from_reader(&mut reader, bincode::config::standard())
                        .unwrap();

                let mut frames = Vec::<BacktraceFrame>::with_capacity(frame_size);
                for _index in 0..frame_size {
                    frames.push(
                        bincode::serde::decode_from_reader(
                            &mut reader,
                            bincode::config::standard(),
                        )
                        .unwrap(),
                    );
                }

                assert_eq!(sig, signal);
                assert_eq!(crash_query_id, query_id);
                assert!(!frames.is_empty());
            }
        }
    }
}
