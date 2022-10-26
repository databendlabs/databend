// Copyright 2022 Datafuse Labs.
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
// Borrow from https://github.com/near/near-memory-tracker.

use std::alloc::GlobalAlloc;
use std::alloc::Layout;
use std::cell::Cell;
use std::os::raw::c_void;
use std::ptr::null_mut;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;

use backtrace::Backtrace;

const MEBIBYTE: usize = 1 << 20;
/// Skip addresses that are above this value.
const SKIP_ADDR_ABOVE: *mut c_void = 0x7000_0000_0000 as *mut c_void;
/// Configure how often should we print stack trace, whenever new record is reached.
pub(crate) static REPORT_USAGE_INTERVAL: AtomicUsize = AtomicUsize::new(usize::MAX);
/// Should be a configurable option.
pub(crate) static ENABLE_STACK_TRACE: AtomicBool = AtomicBool::new(false);
pub(crate) static VERBOSE: AtomicBool = AtomicBool::new(false);

const COUNTERS_SIZE: usize = 16384;
static MEM_SIZE: [AtomicUsize; COUNTERS_SIZE] = unsafe {
    // SAFETY: Rust [guarantees](https://doc.rust-lang.org/stable/std/sync/atomic/struct.AtomicUsize.html)
    // that `usize` and `AtomicUsize` have the same representation.
    std::mem::transmute::<[usize; COUNTERS_SIZE], [AtomicUsize; COUNTERS_SIZE]>(
        [0_usize; COUNTERS_SIZE],
    )
};
static MEM_CNT: [AtomicUsize; COUNTERS_SIZE] = unsafe {
    std::mem::transmute::<[usize; COUNTERS_SIZE], [AtomicUsize; COUNTERS_SIZE]>(
        [0_usize; COUNTERS_SIZE],
    )
};

const CACHE_SIZE: usize = 1 << 20;
static mut SKIP_CACHE: [u8; CACHE_SIZE] = [0; CACHE_SIZE];
static mut CHECKED_CACHE: [u8; CACHE_SIZE] = [0; CACHE_SIZE];

// TODO: Make stack size configurable
const STACK_SIZE: usize = 1;

// TODO: Store stack size
// MAYBE: Make: tid: u16
// MAYBE: split magic into two u32
// MAYBE: make size u32
// MAYBE: make stack list of [u32] to save memory - maybe?
#[derive(Debug)]
#[repr(C)]
pub struct AllocHeader {
    // TODO (magic should be split in two parts, at front and back)
    magic: usize,
    size: usize,
    tid: usize,
    stack: [*mut c_void; STACK_SIZE],
}

impl AllocHeader {
    unsafe fn new(layout: Layout, tid: usize) -> Self {
        Self {
            magic: MAGIC_RUST + STACK_SIZE,
            size: layout.size(),
            tid,
            stack: [null_mut::<c_void>(); STACK_SIZE],
        }
    }

    #[must_use]
    pub fn size(&self) -> usize {
        self.size
    }

    #[must_use]
    pub fn tid(&self) -> usize {
        self.tid
    }

    #[must_use]
    pub fn stack(&self) -> &[*mut c_void; STACK_SIZE] {
        &self.stack
    }

    #[must_use]
    pub fn valid(&self) -> bool {
        self.is_allocated() || self.is_freed()
    }

    #[must_use]
    pub fn is_allocated(&self) -> bool {
        self.magic == (MAGIC_RUST + STACK_SIZE)
    }

    #[must_use]
    pub fn is_freed(&self) -> bool {
        self.magic == MAGIC_RUST + STACK_SIZE + FREED_MAGIC
    }

    pub fn mark_as_freed(&mut self) {
        self.magic = MAGIC_RUST + STACK_SIZE + FREED_MAGIC;
    }
}

const MAGIC_RUST: usize = 0x12_3456_7899_1100;
const FREED_MAGIC: usize = 0x100;

thread_local! {
    static TID: Cell<usize> = Cell::new(0);
    static MEMORY_USAGE_MAX: Cell<usize> = Cell::new(0);
    static MEMORY_USAGE_LAST_REPORT: Cell<usize> = Cell::new(0);
    static NUM_ALLOCATIONS: Cell<usize> = Cell::new(0);
    static IN_TRACE: Cell<usize> = Cell::new(0);
}

#[must_use]
pub fn get_tid() -> usize {
    TID.with(|f| {
        let mut v = f.get();
        if v == 0 {
            // thread::current().id().as_u64() is still unstable
            #[cfg(target_os = "linux")]
            {
                v = nix::unistd::gettid().as_raw() as usize;
            }
            #[cfg(not(target_os = "linux"))]
            {
                static NTHREADS: AtomicUsize = AtomicUsize::new(0);
                v = NTHREADS.fetch_add(1, Ordering::Relaxed) as usize;
            }
            f.set(v)
        }
        v
    })
}

fn murmur64(mut h: u64) -> u64 {
    h ^= h >> 33;
    h = h.overflowing_mul(0xff51_afd7_ed55_8ccd).0;
    h ^= h >> 33;
    h = h.overflowing_mul(0xc4ce_b9fe_1a85_ec53).0;
    h ^= h >> 33;
    h
}

/// TODO: Consider making this configurable
const IGNORE_START: &[&str] = &[
    "__rg_",
    "_ZN10tokio_util",
    "_ZN20reed_solomon_erasure",
    "_ZN3std",
    "_ZN4core",
    "_ZN5actix",
    "_ZN5alloc",
    "_ZN5bytes",
    "_ZN5tokio",
    "_ZN6base64",
    "_ZN6cached",
    "_ZN8smallvec",
    "_ZN9backtrace9backtrace",
    "_ZN9hashbrown",
];

/// TODO: Consider making this configurable
const IGNORE_INSIDE: &[&str] = &[
    "$LT$actix..",
    "$LT$alloc..",
    "$LT$base64..",
    "$LT$cached..",
    "$LT$core..",
    "$LT$hashbrown..",
    "$LT$reed_solomon_erasure..",
    "$LT$serde_json..",
    "$LT$std..",
    "$LT$tokio..",
    "$LT$tokio_util..",
    "$LT$tracing_subscriber..",
    "allocator",
];

fn skip_ptr(addr: *mut c_void) -> bool {
    let mut found = false;
    backtrace::resolve(addr, |symbol| {
        found = found
            || symbol
                .name()
                .and_then(|name| name.as_str())
                .map(|name| {
                    IGNORE_START
                        .iter()
                        .filter(|s: &&&str| name.starts_with(**s))
                        .any(|_| true)
                        || IGNORE_INSIDE
                            .iter()
                            .filter(|s: &&&str| name.contains(**s))
                            .any(|_| true)
                })
                .unwrap_or_default()
    });

    found
}

#[must_use]
pub fn total_memory_usage() -> usize {
    MEM_SIZE.iter().map(|v| v.load(Ordering::Relaxed)).sum()
}

pub fn current_thread_memory_usage() -> usize {
    let tid = get_tid();

    MEM_SIZE[tid % COUNTERS_SIZE].load(Ordering::Relaxed)
}

pub fn thread_memory_usage(tid: usize) -> usize {
    MEM_SIZE[tid % COUNTERS_SIZE].load(Ordering::Relaxed)
}

pub fn thread_memory_count(tid: usize) -> usize {
    MEM_CNT[tid % COUNTERS_SIZE].load(Ordering::Relaxed)
}

pub fn current_thread_peak_memory_usage() -> usize {
    MEMORY_USAGE_MAX.with(Cell::get)
}

pub fn reset_memory_usage_max() {
    let tid = get_tid();
    let memory_usage = MEM_SIZE[tid % COUNTERS_SIZE].load(Ordering::Relaxed);
    MEMORY_USAGE_MAX.with(|x| x.set(memory_usage));
}

pub struct ProxyAllocator<A> {
    inner: A,
}

impl<A> ProxyAllocator<A> {
    pub const fn new(inner: A) -> Self {
        Self { inner }
    }

    /// Enable calling `backtrace` to fill out data
    pub fn enable_stack_trace(&self, value: bool) -> &Self {
        ENABLE_STACK_TRACE.store(value, Ordering::Relaxed);
        self
    }

    pub fn set_report_usage_interval(&self, value: usize) -> &Self {
        REPORT_USAGE_INTERVAL.store(value, Ordering::Relaxed);
        self
    }

    pub fn set_verbose(&self, value: bool) -> &Self {
        VERBOSE.store(value, Ordering::Relaxed);
        self
    }
}

unsafe impl<A: GlobalAlloc> GlobalAlloc for ProxyAllocator<A> {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        let verbose = VERBOSE.load(Ordering::Relaxed);
        let tid = get_tid();
        let memory_usage = MEM_SIZE[tid % COUNTERS_SIZE]
            .fetch_add(layout.size(), Ordering::Relaxed)
            + layout.size();

        MEM_CNT[tid % COUNTERS_SIZE].fetch_add(1, Ordering::Relaxed);

        MEMORY_USAGE_MAX.with(|val| {
            if val.get() < memory_usage {
                val.set(memory_usage);
            }
        });

        let mut header = AllocHeader::new(layout, tid);

        IN_TRACE.with(|in_trace| {
            if in_trace.replace(1) != 0 {
                // Allocation happening within alloc due to backtrace.
                header.stack[0] = usize::MAX as *mut c_void;
                return;
            }
            Self::print_stack_trace_on_memory_spike(layout, tid, memory_usage);
            if ENABLE_STACK_TRACE.load(Ordering::Relaxed) {
                Self::compute_stack_trace(layout, &mut header.stack, verbose);
            }
            if verbose {
                tracing::info!(?header);
            }
            in_trace.set(0);
        });

        let (new_layout, offset) = Layout::new::<AllocHeader>().extend(layout).unwrap();

        let res = self.inner.alloc(new_layout);
        *res.cast::<AllocHeader>() = header;

        res.add(offset)
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        let (new_layout, offset) = Layout::new::<AllocHeader>().extend(layout).unwrap();

        let ptr = ptr.sub(offset);

        let ah = &mut (*(ptr.cast::<AllocHeader>()));
        debug_assert!(ah.is_allocated());
        ah.mark_as_freed();
        let header_tid = ah.tid;

        MEM_SIZE[header_tid % COUNTERS_SIZE].fetch_sub(layout.size(), Ordering::Relaxed);
        MEM_CNT[header_tid % COUNTERS_SIZE].fetch_sub(1, Ordering::Relaxed);

        self.inner.dealloc(ptr, new_layout);
    }
}

impl<A: GlobalAlloc> ProxyAllocator<A> {
    unsafe fn print_stack_trace_on_memory_spike(layout: Layout, tid: usize, memory_usage: usize) {
        MEMORY_USAGE_LAST_REPORT.with(|memory_usage_last_report| {
            if memory_usage
                > REPORT_USAGE_INTERVAL
                    .load(Ordering::Relaxed)
                    .saturating_add(memory_usage_last_report.get())
            {
                memory_usage_last_report.set(memory_usage);
                tracing::warn!(
                    tid,
                    memory_usage_mb = memory_usage / MEBIBYTE,
                    added_mb = layout.size() / MEBIBYTE,
                    bt = ?Backtrace::new(),
                    "reached new record of memory usage",
                );
            }
        });
    }
}

impl<A: GlobalAlloc> ProxyAllocator<A> {
    #[inline]
    unsafe fn compute_stack_trace(
        layout: Layout,
        stack: &mut [*mut c_void; STACK_SIZE],
        verbose: bool,
    ) {
        if Self::should_compute_trace(layout) {
            const MISSING_TRACE: *mut c_void = 2 as *mut c_void;
            stack[0] = MISSING_TRACE;
            backtrace::trace(|frame| {
                let addr = frame.ip();
                stack[0] = addr.cast::<c_void>();
                if addr >= SKIP_ADDR_ABOVE.cast::<c_void>() {
                    true
                } else {
                    let hash = (murmur64(addr as u64) % (8 * CACHE_SIZE as u64)) as usize;
                    let i = hash / 8;
                    let cur_bit = 1 << (hash % 8);
                    if SKIP_CACHE[i] & cur_bit != 0 {
                        true
                    } else if CHECKED_CACHE[i] & cur_bit != 0 {
                        false
                    } else if skip_ptr(addr) {
                        SKIP_CACHE[i] |= cur_bit;
                        true
                    } else {
                        CHECKED_CACHE[i] |= cur_bit;
                        false
                    }
                }
            });
            if verbose {
                tracing::info!(?stack, "STARTED_TRACE");
            }
        } else if verbose {
            tracing::info!(?layout, "TRACING SKIPPED");
        }
    }

    unsafe fn should_compute_trace(layout: Layout) -> bool {
        match layout.size() {
            // 1% of the time
            0..=999 => {
                (murmur64(NUM_ALLOCATIONS.with(|key| {
                    // key.update() is still unstable
                    let val = key.get();
                    key.set(val + 1);
                    val
                }) as u64)
                    % 1024)
                    < 10
            }
            // 100%
            _ => true,
        }
    }
}

pub fn print_memory_stats() {
    tracing::info!(tid = get_tid(), "tid");
    let mut total_cnt: usize = 0;
    let mut total_size: usize = 0;
    for idx in 0..COUNTERS_SIZE {
        let val = MEM_SIZE[idx].load(Ordering::Relaxed);
        if val != 0 {
            let cnt = MEM_CNT[idx].load(Ordering::Relaxed);
            total_cnt += cnt;
            total_size += val;

            tracing::info!(idx, cnt, val, "COUNTERS");
        }
    }
    tracing::info!(total_cnt, total_size, "COUNTERS TOTAL");
}
