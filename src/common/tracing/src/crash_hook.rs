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
use std::io::Read;
use std::io::Write;
use std::os::fd::FromRawFd;
use std::os::fd::IntoRawFd;
use std::os::fd::OwnedFd;
#[cfg(test)]
use std::ptr::addr_of_mut;
use std::sync::Mutex;
use std::sync::PoisonError;
use std::time::Duration;

use databend_common_base::runtime::Thread;
use databend_common_base::runtime::ThreadTracker;
use databend_common_exception::StackTrace;
use jiff::Zoned;
use jiff::tz::TimeZone;

const BUFFER_SIZE: usize = {
    size_of::<i32>() // sig
        + size_of::<i32>() // si_code
        + size_of::<u64>()  // si_addr
        + size_of::<u64>()  // current query id length
        + 36 // max query id length, example: a9ef0b3f-b3a1-4759-b129-e1bcf3a551c0
        + size_of::<u64>()  // frame length
        + size_of::<u64>() * 50 // max frames ip size
};

struct CrashHandler {
    write_file: File,
}

impl CrashHandler {
    pub fn create(write_file: File) -> CrashHandler {
        CrashHandler { write_file }
    }

    pub fn recv_signal(&mut self, sig: i32, info: *mut libc::siginfo_t, _uc: *mut libc::c_void) {
        let mut buffer = [0_u8; BUFFER_SIZE];

        let mut pos = 0;

        fn write_i32(buf: &mut [u8], v: i32, mut pos: usize) -> usize {
            for x in v.to_le_bytes() {
                buf[pos] = x;
                pos += 1;
            }

            pos
        }

        fn write_u64(buf: &mut [u8], v: u64, mut pos: usize) -> usize {
            for x in v.to_le_bytes() {
                buf[pos] = x;
                pos += 1;
            }

            pos
        }

        fn write_string(buf: &mut [u8], v: &str, mut pos: usize, max_length: usize) -> usize {
            let bytes = v.as_bytes();
            let length = std::cmp::min(bytes.len(), max_length);
            pos = write_u64(buf, length as u64, pos);

            #[allow(clippy::needless_range_loop)]
            for index in 0..length {
                buf[pos] = bytes[index];
                pos += 1;
            }

            pos
        }

        let current_query_id = match ThreadTracker::query_id() {
            None => "Unknown",
            Some(query_id) => query_id,
        };

        pos = write_i32(&mut buffer, sig, pos);
        pos = write_i32(&mut buffer, unsafe { (*info).si_code }, pos);
        pos = write_u64(&mut buffer, unsafe { (*info).si_addr() as u64 }, pos);
        pos = write_string(&mut buffer, current_query_id, pos, 36);

        let mut frames_len = 0;
        let mut frames = [0_u64; 50];

        unsafe {
            backtrace::trace_unsynchronized(|frame| {
                frames[frames_len] = frame.ip() as u64;
                frames_len += 1;
                frames_len < 50
            });
        }

        pos = write_u64(&mut buffer, frames_len as u64, pos);
        for frame in frames {
            pos = write_u64(&mut buffer, frame, pos);
        }

        if let Err(e) = self.write_file.write(&buffer) {
            eprintln!("write signal pipe failure");
            let _ = std::io::stderr().flush();
            eprintln!("cause: {:?}", e);
        }

        if let Err(e) = self.write_file.flush() {
            eprintln!("flush signal pipe failure");
            let _ = std::io::stderr().flush();
            eprintln!("cause: {:?}", e);
        }

        std::thread::sleep(Duration::from_secs(4));
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
        match libc::SIG_ERR == libc::signal(sig, libc::SIG_DFL) {
            true => std::process::exit(1),
            false => match libc::raise(sig) {
                0 => {}
                _ => std::process::exit(1),
            },
        }
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

    if std::ptr::eq(stack_memory_arena, libc::MAP_FAILED) {
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
            libc::SIGABRT,
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

fn read_i32(buf: &[u8], pos: usize) -> (i32, usize) {
    let bytes: [u8; 4] = buf[pos..pos + 4].try_into().expect("expect");
    (i32::from_le_bytes(bytes), pos + 4)
}

fn read_u64(buf: &[u8], pos: usize) -> (u64, usize) {
    let bytes: [u8; 8] = buf[pos..pos + 8].try_into().expect("expect");
    (u64::from_le_bytes(bytes), pos + 8)
}

fn read_string(buf: &[u8], pos: usize) -> (&str, usize) {
    unsafe {
        let (len, pos) = read_u64(buf, pos);
        (
            std::str::from_utf8_unchecked(&buf[pos..pos + len as usize]),
            pos + len as usize,
        )
    }
}

pub struct SignalListener;

impl SignalListener {
    pub fn spawn(mut file: File, crash_version: String) {
        Thread::named_spawn(Some(String::from("SignalListener")), move || {
            loop {
                let mut buffer = [0_u8; BUFFER_SIZE];

                if file.read_exact(&mut buffer).is_ok() {
                    let pos = 0;
                    let (sig, pos) = read_i32(&buffer, pos);
                    let (si_code, pos) = read_i32(&buffer, pos);
                    let (si_addr, pos) = read_u64(&buffer, pos);
                    let (crash_query_id, pos) = read_string(&buffer, pos);

                    let (frames_len, mut pos) = read_u64(&buffer, pos);
                    let mut frames = Vec::with_capacity(50);

                    for _ in 0..frames_len {
                        let (ip, new_pos) = read_u64(&buffer, pos);
                        frames.push(ip);
                        pos = new_pos;
                    }

                    let id = std::process::id();
                    let signal_mess = signal_message(sig, si_code, si_addr as usize);
                    let stack_trace = StackTrace::from_ips(&frames);

                    eprintln!(
                        "{:#^80}\n\
                    PID: {}\n\
                    Version: {}\n\
                    Timestamp(UTC): {}\n\
                    Timestamp(Local): {}\n\
                    QueryId: {:?}\n\
                    Signal Message: {}\n\
                    Backtrace:\n{:?}",
                        " Crash fault info ",
                        id,
                        crash_version,
                        Zoned::now().with_time_zone(TimeZone::UTC),
                        Zoned::now(),
                        crash_query_id,
                        signal_mess,
                        stack_trace
                    );
                    log::error!(
                        "{:#^80}\n\
                    PID: {}\n\
                    Version: {}\n\
                    Timestamp(UTC): {}\n\
                    Timestamp(Local): {}\n\
                    QueryId: {:?}\n\
                    Signal Message: {}\n\
                    Backtrace:\n{:?}",
                        " Crash fault info ",
                        id,
                        crash_version,
                        Zoned::now().with_time_zone(TimeZone::UTC),
                        Zoned::now(),
                        crash_query_id,
                        signal_mess,
                        stack_trace
                    );
                }
            }
        });
    }
}

#[cfg(test)]
mod tests {
    use std::io::Read;
    use std::ptr::addr_of_mut;

    use databend_common_base::runtime::ThreadTracker;

    use crate::crash_hook::BUFFER_SIZE;
    use crate::crash_hook::TEST_JMP_BUFFER;
    use crate::crash_hook::pipe_file;
    use crate::crash_hook::read_i32;
    use crate::crash_hook::read_string;
    use crate::crash_hook::read_u64;
    use crate::crash_hook::sigsetjmp;
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

                let mut buffer = [0_u8; BUFFER_SIZE];

                let pos = 0;
                if reader.read_exact(&mut buffer).is_ok() {
                    let (sig, pos) = read_i32(&buffer, pos);
                    let (_si_code, pos) = read_i32(&buffer, pos);
                    let (_si_addr, pos) = read_u64(&buffer, pos);
                    let (crash_query_id, pos) = read_string(&buffer, pos);

                    let (frames_len, mut pos) = read_u64(&buffer, pos);
                    let mut frames = Vec::with_capacity(50);

                    for _index in 0..frames_len {
                        let (ip, new_pos) = read_u64(&buffer, pos);
                        frames.push(ip);
                        pos = new_pos;
                    }

                    // eprintln!("{:?}", StackTrace::from_ips(&frames));
                    assert_eq!(sig, signal);
                    assert_eq!(crash_query_id, query_id);
                    assert!(!frames.is_empty());
                }
            }
        }
    }
}
