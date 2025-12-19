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

use std::collections::HashMap;
use std::collections::hash_map::Entry;
use std::fmt::Debug;
use std::fmt::Formatter;
use std::fmt::Write;
use std::hash::Hash;
use std::hash::Hasher;
use std::sync::Arc;
use std::sync::LazyLock;
use std::sync::Mutex;
use std::sync::PoisonError;
use std::sync::RwLock;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;

// 0: not specified 1: disable 2: enable
pub static USER_SET_ENABLE_BACKTRACE: AtomicUsize = AtomicUsize::new(0);

pub fn set_backtrace(switch: bool) {
    if switch {
        USER_SET_ENABLE_BACKTRACE.store(2, Ordering::Relaxed);
    } else {
        let mut write_guard = STACK_CACHE.write().unwrap_or_else(PoisonError::into_inner);
        write_guard.clear();
        USER_SET_ENABLE_BACKTRACE.store(1, Ordering::Relaxed);
    }
}

fn enable_rust_backtrace() -> bool {
    match USER_SET_ENABLE_BACKTRACE.load(Ordering::Relaxed) {
        0 => {}
        1 => return false,
        _ => return true,
    }

    let enabled = match std::env::var("RUST_LIB_BACKTRACE") {
        Ok(s) => s != "0",
        Err(_) => match std::env::var("RUST_BACKTRACE") {
            Ok(s) => s != "0",
            Err(_) => false,
        },
    };

    USER_SET_ENABLE_BACKTRACE.store(enabled as usize + 1, Ordering::Relaxed);
    enabled
}

pub fn capture() -> StackTrace {
    match enable_rust_backtrace() {
        true => StackTrace::capture(),
        false => StackTrace::no_capture(),
    }
}

#[cfg(target_os = "linux")]
pub struct ResolvedStackFrame {
    pub virtual_address: usize,
    pub physical_address: usize,
    pub symbol: String,
    pub inlined: bool,
    pub file: Option<String>,
    pub line: Option<u32>,
    pub column: Option<u32>,
}

#[derive(serde::Serialize, serde::Deserialize, Debug, Clone)]
pub struct PhysicalAddr {
    pub physical_addr: usize,
    pub library_build_id: Option<Arc<Vec<u8>>>,
}

#[derive(serde::Serialize, serde::Deserialize, Debug, Clone)]
pub enum StackFrame {
    #[cfg(target_os = "linux")]
    Ip(usize),
    #[cfg(target_os = "linux")]
    PhysicalAddr(PhysicalAddr),
    #[cfg(not(target_os = "linux"))]
    Backtrace(backtrace::BacktraceFrame),
}

impl Eq for StackFrame {}

impl PartialEq for StackFrame {
    fn eq(&self, other: &Self) -> bool {
        #[cfg(target_os = "linux")]
        {
            match (&self, &other) {
                (StackFrame::Ip(addr), StackFrame::Ip(other_addr)) => addr == other_addr,
                (StackFrame::PhysicalAddr(addr), StackFrame::PhysicalAddr(other_addr)) => {
                    addr.physical_addr == other_addr.physical_addr
                }
                _ => false,
            }
        }

        #[cfg(not(target_os = "linux"))]
        {
            let StackFrame::Backtrace(addr) = &self;
            let StackFrame::Backtrace(other_addr) = &other;
            std::ptr::eq(addr.ip(), other_addr.ip())
        }
    }
}

impl Hash for StackFrame {
    fn hash<H: Hasher>(&self, state: &mut H) {
        #[cfg(target_os = "linux")]
        {
            match &self {
                StackFrame::Ip(addr) => addr.hash(state),
                StackFrame::PhysicalAddr(addr) => addr.physical_addr.hash(state),
            };
        }

        #[cfg(not(target_os = "linux"))]
        {
            let StackFrame::Backtrace(addr) = &self;
            addr.ip().hash(state)
        }
    }
}

// Rewrite the backtrace on linux ELF using gimli-rs.
//
// Differences from backtrace-rs[https://github.com/rust-lang/backtrace-rs]:
// - Almost lock-free (backtrace-rs requires large-grained locks or frequent lock operations)
// - Symbol resolution is lazy, only resolved when outputting
// - Cache the all stack frames for the stack, not just a single stack frame
// - Output the physical addresses of the stack instead of virtual addresses, even in the absence of symbols (this will help us use backtraces to get cause in the case of split symbol tables)
// - Output inline functions and marked it
//
// What's different from gimli-addr2line[https://github.com/gimli-rs/addr2line](why not use gimli-addr2line):
// - Use aranges to optimize the lookup of DWARF units (if present)
// - gimli-addr2line caches and sorts the symbol tables to speed up symbol lookup, which would introduce locks and caching (but in reality, symbol lookup is a low-frequency operation in databend, and rapid reconstruction based on mmap is sufficient).
#[derive(Clone, serde::Serialize, serde::Deserialize)]
pub struct StackTrace {
    pub(crate) frames: Vec<StackFrame>,
    pub(crate) build_id: Option<Arc<Vec<u8>>>,
}

impl Eq for StackTrace {}

impl PartialEq for StackTrace {
    fn eq(&self, other: &Self) -> bool {
        self.frames == other.frames
    }
}

impl StackTrace {
    pub fn capture() -> StackTrace {
        let mut frames = Vec::with_capacity(50);
        Self::capture_frames(&mut frames);
        StackTrace {
            frames,
            build_id: Self::executable_build_id(),
        }
    }

    pub fn no_capture() -> StackTrace {
        StackTrace {
            frames: vec![],
            build_id: Self::executable_build_id(),
        }
    }

    #[cfg(not(target_os = "linux"))]
    pub fn pre_load_symbol() {
        static INIT_GUARD: LazyLock<backtrace::Backtrace> =
            LazyLock::new(backtrace::Backtrace::new);
        let _frames = INIT_GUARD.frames();
    }

    #[cfg(target_os = "linux")]
    pub fn pre_load_symbol() {
        let _lib_manager = crate::elf::LibraryManager::instance();
    }

    #[cfg(not(target_os = "linux"))]
    pub fn to_physical(&self) -> StackTrace {
        self.clone()
    }

    #[cfg(target_os = "linux")]
    pub fn to_physical(&self) -> StackTrace {
        let frames = crate::elf::LibraryManager::instance().to_physical_frames(&self.frames);

        StackTrace {
            frames,
            build_id: Self::executable_build_id(),
        }
    }

    #[cfg(not(target_os = "linux"))]
    fn executable_build_id() -> Option<Arc<Vec<u8>>> {
        None
    }

    #[cfg(target_os = "linux")]
    fn executable_build_id() -> Option<Arc<Vec<u8>>> {
        crate::elf::LibraryManager::instance().executable_build_id()
    }

    #[cfg(target_os = "linux")]
    pub fn from_ips(frames: &[u64]) -> StackTrace {
        let mut stack_frames = Vec::with_capacity(frames.len());
        for frame in frames {
            stack_frames.push(StackFrame::Ip(*frame as usize));
        }

        StackTrace {
            frames: stack_frames,
            build_id: Self::executable_build_id(),
        }
    }

    #[cfg(not(target_os = "linux"))]
    pub fn from_ips(_: &[u64]) -> StackTrace {
        StackTrace::no_capture()
    }

    #[cfg(not(target_os = "linux"))]
    fn capture_frames(frames: &mut Vec<StackFrame>) {
        unsafe {
            backtrace::trace_unsynchronized(|frame| {
                frames.push(StackFrame::Backtrace(backtrace::BacktraceFrame::from(
                    frame.clone(),
                )));
                frames.len() != frames.capacity()
            });
        }
    }

    #[cfg(target_os = "linux")]
    fn capture_frames(frames: &mut Vec<StackFrame>) {
        // Safety:
        unsafe {
            backtrace::trace_unsynchronized(|frame| {
                frames.push(StackFrame::Ip(frame.ip() as usize));
                frames.len() != frames.capacity()
            });
        }
    }

    #[cfg(not(target_os = "linux"))]
    fn fmt_frames(&self, display_text: &mut String, address: bool) -> std::fmt::Result {
        let mut frames = Vec::with_capacity(self.frames.len());
        for frame in &self.frames {
            let StackFrame::Backtrace(frame) = frame;
            frames.push(frame.clone());
        }

        let mut backtrace = backtrace::Backtrace::from(frames);

        if !address {
            backtrace.resolve();
        }

        writeln!(display_text, "{:?}", backtrace)
    }

    #[cfg(target_os = "linux")]
    fn fmt_frames(&self, f: &mut String, mut address: bool) -> std::fmt::Result {
        if !address {
            let binary_id = crate::elf::LibraryManager::instance().executable_build_id();
            address = match (&binary_id, &self.build_id) {
                (Some(binary_id), Some(build_id)) => binary_id != build_id,
                _ => true,
            };
        }

        let mut idx = 0;
        crate::elf::LibraryManager::instance().resolve_frames(&self.frames, address, |frame| {
            write!(f, "{:4}: {}", idx, frame.symbol)?;

            if frame.inlined {
                write!(f, "[inlined]")?;
            } else if frame.physical_address != frame.virtual_address {
                write!(f, "@{:x}", frame.physical_address)?;
            }

            #[allow(clippy::writeln_empty_string)]
            writeln!(f, "")?;

            if let Some(file) = frame.file {
                write!(f, "             at {}", file)?;

                if let Some(line) = frame.line {
                    write!(f, ":{}", line)?;

                    if let Some(column) = frame.column {
                        write!(f, ":{}", column)?;
                    }
                }

                #[allow(clippy::writeln_empty_string)]
                writeln!(f, "")?;
            }

            idx += 1;
            Ok(())
        })
    }
}

#[allow(clippy::type_complexity)]
static STACK_CACHE: LazyLock<RwLock<HashMap<Vec<StackFrame>, Arc<Mutex<Option<Arc<String>>>>>>> =
    LazyLock::new(|| RwLock::new(HashMap::new()));

impl Debug for StackTrace {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        if self.frames.is_empty() {
            return writeln!(f, "<empty>");
        }

        let mut display_text = {
            let read_guard = STACK_CACHE.read().unwrap_or_else(PoisonError::into_inner);
            read_guard.get(&self.frames).cloned()
        };

        if display_text.is_none() {
            let mut guard = STACK_CACHE.write().unwrap_or_else(PoisonError::into_inner);

            display_text = Some(match guard.entry(self.frames.clone()) {
                Entry::Occupied(v) => v.get().clone(),
                Entry::Vacant(v) => v.insert(Arc::new(Mutex::new(None))).clone(),
            });
        }

        let display_text_lock = display_text.as_ref().unwrap();
        let mut display_guard = display_text_lock
            .lock()
            .unwrap_or_else(PoisonError::into_inner);

        if display_guard.is_none() {
            let mut display_text = String::new();

            self.fmt_frames(&mut display_text, !enable_rust_backtrace())?;
            *display_guard = Some(Arc::new(display_text));
        }

        let display_text = display_guard.as_ref().unwrap().clone();
        drop(display_guard);

        writeln!(f, "{}", display_text)?;
        Ok(())
    }
}
