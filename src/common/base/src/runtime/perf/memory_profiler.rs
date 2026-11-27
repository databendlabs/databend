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

//! Sampling allocation profiler of a running query or of every query of the
//! process.
//!
//! The global allocator reports every allocated byte of the threads being
//! sampled to `MemoryProfiler::on_alloc`, which records a call stack every
//! `SAMPLE_INTERVAL` bytes on average. A sample stands for the bytes since
//! the previous one, so the profile estimates how many bytes each call stack
//! allocated, including memory freed soon after. It does not show which
//! memory is alive at a given moment.

use std::collections::HashMap;
use std::ffi::c_void;
use std::fmt::Write;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;

use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use log::debug;
use parking_lot::Mutex;

use crate::runtime::CpuProfiler;
use crate::runtime::ProfileScope;
use crate::runtime::ThreadTracker;
use crate::runtime::TrackingPayload;

/// The average number of allocated bytes between two samples of a thread.
pub const SAMPLE_INTERVAL: usize = 512 * 1024;

const MAX_FRAMES: usize = 64;

/// The root frame of samples of a query whose payload has no query id when
/// sampling every query.
const UNKNOWN_QUERY_ID: &str = "(unknown query id)";

/// What the process-wide `MemoryProfiler` samples: `NO_PROFILER`,
/// `ALL_QUERIES`, or the `perf_query_token` of the query whose threads it
/// samples. Claiming it from `NO_PROFILER` is what makes a profiler
/// exclusive.
static PROFILER_SCOPE: AtomicU64 = AtomicU64::new(NO_PROFILER);
const NO_PROFILER: u64 = 0;
const ALL_QUERIES: u64 = u64::MAX;

/// The samples of the running profiler, `None` when no profiler runs.
static SAMPLES: Mutex<Option<HashMap<SampleKey, u64>>> = Mutex::new(None);

/// Whether the profiler samples the payload the current thread runs, the
/// only state read by every allocation. Maintained by `sync_from_payload`
/// on every payload switch.
#[thread_local]
static mut SAMPLING: bool = false;

#[thread_local]
static mut SAMPLER: ThreadSampler = ThreadSampler {
    countdown: 0,
    rng: 0,
    in_sampler: false,
};

struct ThreadSampler {
    /// Bytes to allocate before the next sample.
    countdown: i64,
    rng: u64,
    /// Set while the thread records a sample or reads the samples, so the
    /// allocations made meanwhile are not sampled, and the thread never
    /// waits for the samples lock it already holds.
    in_sampler: bool,
}

impl ThreadSampler {
    /// A random interval with mean `SAMPLE_INTERVAL`, so that the samples
    /// do not alias with periodic allocation patterns.
    fn next_interval(&mut self) -> i64 {
        if self.rng == 0 {
            // Distinct per thread.
            self.rng = (self as *const ThreadSampler as u64) | 1;
        }
        // xorshift64
        self.rng ^= self.rng << 13;
        self.rng ^= self.rng >> 7;
        self.rng ^= self.rng << 17;
        let interval = SAMPLE_INTERVAL as u64;
        (interval / 2 + self.rng % interval) as i64
    }
}

#[derive(Clone, Hash, PartialEq, Eq)]
struct SampleKey {
    /// Only recorded when sampling every query.
    query_id: Option<String>,
    /// The plan node the thread works for.
    plan: Option<(u32, String)>,
    /// Instruction pointers, innermost first.
    frames: Box<[usize]>,
}

/// The allocation profiler of this process, at most one runs at a time.
///
/// Like `CpuProfiler`, a query-scoped profiler samples the threads whose
/// tracking payload carries the query's `perf_query_token`. A process-scoped
/// one samples every thread working for a query, i.e. whose payload has a
/// perf query token or a query id; other threads are not sampled.
///
/// A thread picks up a started or stopped profiler at its next payload
/// switch, i.e. the next time a processor or a task of it is polled, so
/// allocations made by a long running call that started before are missed.
pub struct MemoryProfiler {
    _scope: ScopeClaim,
}

struct ScopeClaim;

impl Drop for ScopeClaim {
    fn drop(&mut self) {
        // Under the samples lock, so that a profiler started meanwhile does
        // not lose its samples.
        let samples = {
            let _suppress = SuppressSampling::enter();
            let mut samples = SAMPLES.lock();
            PROFILER_SCOPE.store(NO_PROFILER, Ordering::Release);
            samples.take()
        };
        drop(samples);
    }
}

impl MemoryProfiler {
    /// Fails if another memory profiler is running.
    pub fn start(scope: ProfileScope) -> Result<MemoryProfiler> {
        let claimed = match scope {
            ProfileScope::Query(token) if token == NO_PROFILER || token == ALL_QUERIES => {
                return Err(ErrorCode::Internal(format!(
                    "Invalid perf query token {token}"
                )));
            }
            ProfileScope::Query(token) => token,
            ProfileScope::Process => ALL_QUERIES,
        };

        // Set up the samples before publishing the scope, the threads only
        // look for the samples once they see the scope.
        let mut samples = SAMPLES.lock();
        if PROFILER_SCOPE.load(Ordering::Acquire) != NO_PROFILER {
            return Err(ErrorCode::Internal(
                "Another memory profiler is running in this process",
            ));
        }
        *samples = Some(HashMap::new());
        PROFILER_SCOPE.store(claimed, Ordering::Release);
        drop(samples);

        debug!("starting memory profiling of {scope:?}");
        Ok(MemoryProfiler { _scope: ScopeClaim })
    }

    /// Reports `size` bytes allocated by the current thread. Called by the
    /// global allocator for every allocation.
    #[inline(always)]
    pub fn on_alloc(size: usize) {
        if unsafe { SAMPLING } {
            Self::on_sampled_alloc(size);
        }
    }

    /// Updates whether the current thread is sampled when it switches to
    /// `payload`.
    #[inline]
    pub fn sync_from_payload(payload: &TrackingPayload) {
        let scope = PROFILER_SCOPE.load(Ordering::Relaxed);
        unsafe {
            SAMPLING = samples_payload(scope, payload);
        }
    }

    #[inline(never)]
    fn on_sampled_alloc(size: usize) {
        let sampler = unsafe { &mut *std::ptr::addr_of_mut!(SAMPLER) };
        if sampler.in_sampler {
            return;
        }

        if sampler.rng == 0 {
            // The first sampled allocation of the thread starts a random
            // interval instead of being sampled.
            sampler.countdown = sampler.next_interval();
        }
        sampler.countdown -= size as i64;
        if sampler.countdown > 0 {
            return;
        }

        // An allocation larger than an interval stands for several samples.
        let mut samples = 0;
        while sampler.countdown <= 0 {
            sampler.countdown += sampler.next_interval();
            samples += 1;
        }

        sampler.in_sampler = true;
        record(samples * SAMPLE_INTERVAL as u64);
        sampler.in_sampler = false;
    }

    /// The samples collected so far as folded stacks: one
    /// `[query_id;]plan;outermost;...;innermost bytes` line per distinct
    /// stack, where `plan` is the plan node the allocating thread worked
    /// for, and `query_id` is only set when sampling every query.
    pub fn dump_folded(&self) -> Result<String> {
        let samples = {
            let _suppress = SuppressSampling::enter();
            let samples = SAMPLES.lock();
            samples
                .as_ref()
                .map(|samples| {
                    samples
                        .iter()
                        .map(|(key, bytes)| (key.clone(), *bytes))
                        .collect::<Vec<_>>()
                })
                .unwrap_or_default()
        };
        debug!("begin to dump {} allocation stacks", samples.len());

        let mut symbols: HashMap<usize, Vec<ResolvedFrame>> = HashMap::new();
        for (key, _) in samples.iter() {
            for ip in key.frames.iter() {
                symbols.entry(*ip).or_insert_with(|| resolve(*ip));
            }
        }

        let mut stacks: HashMap<String, u64> = HashMap::new();
        for (key, bytes) in samples {
            let frames = key
                .frames
                .iter()
                .flat_map(|ip| symbols[ip].iter())
                .collect::<Vec<_>>();
            // Drop the frames of the profiler and the allocator itself.
            let skip = frames
                .iter()
                .position(|frame| !frame.allocator)
                .unwrap_or(frames.len());

            let mut stack = String::new();
            if let Some(query_id) = &key.query_id {
                stack.push_str(&CpuProfiler::folded_frame(query_id));
                stack.push(';');
            }
            match &key.plan {
                Some((id, name)) => {
                    let _ = write!(stack, "{} [#{id}]", CpuProfiler::folded_frame(name));
                }
                None => stack.push_str("(no plan node)"),
            }
            for frame in frames[skip..].iter().rev() {
                stack.push(';');
                stack.push_str(&CpuProfiler::folded_frame(&frame.name));
            }
            *stacks.entry(stack).or_default() += bytes;
        }

        let mut folded = String::new();
        for (stack, bytes) in stacks {
            let _ = writeln!(folded, "{stack} {bytes}");
        }
        Ok(folded)
    }

    /// Render folded allocation stacks as a flamegraph SVG, empty without
    /// samples.
    pub fn render_flamegraph(folded: &str) -> Result<String> {
        let mut options = inferno::flamegraph::Options::default();
        options.title = "Allocations".to_string();
        options.count_name = "bytes".to_string();
        CpuProfiler::render_folded(folded, &mut options)
    }
}

/// Whether a profiler of `scope` samples the threads running `payload`.
#[inline]
fn samples_payload(scope: u64, payload: &TrackingPayload) -> bool {
    match scope {
        NO_PROFILER => false,
        // Some payloads of a query only carry its token, e.g. Flight SQL.
        ALL_QUERIES => payload.perf_query_token != 0 || payload.query_id.is_some(),
        token => payload.perf_query_token == token,
    }
}

/// Records a sample standing for `bytes` allocated by the current thread.
fn record(bytes: u64) {
    // The profiler may have stopped or changed since the thread synced its
    // payload, check before paying for the unwinding.
    let scope = PROFILER_SCOPE.load(Ordering::Acquire);
    let Some(Some((query_id, plan))) = ThreadTracker::with_payload(|payload| {
        if !samples_payload(scope, payload) {
            return None;
        }
        let query_id = match scope {
            ALL_QUERIES => Some(
                payload
                    .query_id
                    .clone()
                    .unwrap_or_else(|| UNKNOWN_QUERY_ID.to_string()),
            ),
            _ => None,
        };
        let plan = payload.profile.as_ref().and_then(|profile| {
            let name = profile.plan_name.clone().unwrap_or_default();
            profile.plan_id.map(|id| (id, name))
        });
        Some((query_id, plan))
    }) else {
        return;
    };

    let mut frames = [0usize; MAX_FRAMES];
    let mut depth = 0;
    {
        // The CPU profiler unwinds the interrupted thread in its SIGPROF
        // handler. Unwinding again while this thread is inside the unwinder
        // may deadlock on the unwinder's locks, so defer the signal.
        let _deferred = DeferCpuProfilerSignal::enter();
        // Safety: the sampler is not reentrant, see `in_sampler`. `trace`
        // serializes unwinding only for the Windows unwinder; on Unix both
        // this and the CPU profiler call `_Unwind_Backtrace`, which is thread
        // safe, unsynchronized. Waiting for that lock here would block the
        // allocating threads on each other and on symbolizing a dump.
        unsafe {
            backtrace::trace_unsynchronized(|frame| {
                frames[depth] = frame.ip() as usize;
                depth += 1;
                depth < MAX_FRAMES
            });
        }
    }

    let key = SampleKey {
        query_id,
        plan,
        frames: frames[..depth].into(),
    };
    if let Some(samples) = SAMPLES.lock().as_mut() {
        *samples.entry(key).or_default() += bytes;
    }
}

/// Blocks SIGPROF, the signal of the CPU profiler, on the current thread
/// while alive. A signal raised meanwhile is delivered once unblocked.
struct DeferCpuProfilerSignal {
    saved: libc::sigset_t,
}

impl DeferCpuProfilerSignal {
    fn enter() -> DeferCpuProfilerSignal {
        unsafe {
            let mut block = std::mem::zeroed::<libc::sigset_t>();
            libc::sigemptyset(&mut block);
            libc::sigaddset(&mut block, libc::SIGPROF);
            let mut saved = std::mem::zeroed::<libc::sigset_t>();
            libc::pthread_sigmask(libc::SIG_BLOCK, &block, &mut saved);
            DeferCpuProfilerSignal { saved }
        }
    }
}

impl Drop for DeferCpuProfilerSignal {
    fn drop(&mut self) {
        unsafe {
            libc::pthread_sigmask(libc::SIG_SETMASK, &self.saved, std::ptr::null_mut());
        }
    }
}

/// Stops sampling the current thread while alive, for code that holds the
/// samples lock and allocates.
struct SuppressSampling {
    saved: bool,
}

impl SuppressSampling {
    fn enter() -> SuppressSampling {
        let sampler = unsafe { &mut *std::ptr::addr_of_mut!(SAMPLER) };
        let saved = std::mem::replace(&mut sampler.in_sampler, true);
        SuppressSampling { saved }
    }
}

impl Drop for SuppressSampling {
    fn drop(&mut self) {
        let sampler = unsafe { &mut *std::ptr::addr_of_mut!(SAMPLER) };
        sampler.in_sampler = self.saved;
    }
}

struct ResolvedFrame {
    name: String,
    /// A frame of the profiler or of the allocator, dropped from the stacks.
    allocator: bool,
}

/// Symbolizes one instruction pointer, innermost inlined function first.
fn resolve(ip: usize) -> Vec<ResolvedFrame> {
    let mut frames = Vec::new();
    backtrace::resolve(ip as *mut c_void, |symbol| {
        if let Some(name) = symbol.name() {
            let name = format!("{name:#}");
            let filename = symbol.filename().and_then(|path| path.to_str());
            frames.push(ResolvedFrame {
                allocator: is_allocator_frame(&name, filename),
                name,
            });
        }
    });

    if frames.is_empty() {
        frames.push(ResolvedFrame {
            name: format!("{ip:#x}"),
            allocator: false,
        });
    }
    frames
}

/// Symbols resolved from debug info may be plain function names without
/// their module path, e.g. `record`, so the source file is checked as well.
fn is_allocator_frame(name: &str, filename: Option<&str>) -> bool {
    const PREFIXES: [&str; 7] = [
        "backtrace::",
        "databend_common_base::runtime::perf::memory_profiler::",
        "databend_common_base::mem_allocator::",
        "__rust",
        "__rg_",
        "alloc::alloc::",
        "std::alloc::",
    ];
    const FILES: [&str; 5] = [
        "/backtrace/",
        "/runtime/perf/memory_profiler.rs",
        "/mem_allocator/",
        "/alloc/src/alloc.rs",
        "/std/src/alloc.rs",
    ];
    // Methods of inherent and trait impls are demangled as `<Type>::method`.
    let name = name.trim_start_matches('<');
    if PREFIXES.iter().any(|prefix| name.starts_with(prefix)) {
        return true;
    }
    filename.is_some_and(|filename| FILES.iter().any(|file| filename.contains(file)))
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::sync::Mutex;

    use super::PROFILER_SCOPE;
    use super::SAMPLE_INTERVAL;
    use super::is_allocator_frame;
    use crate::runtime::MemoryProfiler;
    use crate::runtime::ProfileScope;
    use crate::runtime::QueryPerf;
    use crate::runtime::ThreadTracker;
    use crate::runtime::profile::Profile;

    /// Serializes the tests that start a `MemoryProfiler`.
    static PROFILER_TESTS: Mutex<()> = Mutex::new(());

    fn serial() -> std::sync::MutexGuard<'static, ()> {
        PROFILER_TESTS
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    fn query_payload(query_id: &str, token: u64) -> crate::runtime::TrackingPayload {
        let mut payload = ThreadTracker::new_tracking_payload();
        payload.query_id = Some(query_id.to_string());
        payload.perf_query_token = token;
        payload
    }

    /// The sampled bytes of every folded stack whose root contains `root`.
    fn bytes_under(folded: &str, root: &str) -> u64 {
        folded
            .lines()
            .filter(|line| line.split(';').next().unwrap().contains(root))
            .map(|line| line.rsplit(' ').next().unwrap().parse::<u64>().unwrap())
            .sum()
    }

    #[test]
    fn test_memory_profiler_is_exclusive() {
        let _serial = serial();

        let token = QueryPerf::next_query_token();
        let first = MemoryProfiler::start(ProfileScope::Query(token)).unwrap();
        assert_eq!(
            PROFILER_SCOPE.load(std::sync::atomic::Ordering::Relaxed),
            token
        );
        for scope in [
            ProfileScope::Process,
            ProfileScope::Query(QueryPerf::next_query_token()),
        ] {
            let err = MemoryProfiler::start(scope).err().unwrap();
            assert!(err.message().contains("Another memory profiler is running"));
        }

        drop(first);
        assert_eq!(PROFILER_SCOPE.load(std::sync::atomic::Ordering::Relaxed), 0);
        let second = MemoryProfiler::start(ProfileScope::Process);
        assert!(second.is_ok(), "released when the first profiler drops");
        drop(second);

        assert!(MemoryProfiler::start(ProfileScope::Query(0)).is_err());
        assert!(MemoryProfiler::start(ProfileScope::Query(u64::MAX)).is_err());
    }

    #[test]
    fn test_samples_only_the_profiled_query() {
        let _serial = serial();
        ThreadTracker::init();

        let token = QueryPerf::next_query_token();
        let profiler = MemoryProfiler::start(ProfileScope::Query(token)).unwrap();

        let mut payload = query_payload("mem-profiled", token);
        payload.profile = Some(Arc::new(Profile::create(
            3,
            "HashJoin".to_string(),
            Some(3),
            Some("HashJoin".to_string()),
            None,
            Arc::new("".to_string()),
            Default::default(),
            None,
        )));
        let allocated = 256 * 1024 * 1024;
        {
            let _guard = ThreadTracker::tracking(payload);
            for _ in 0..allocated / 4096 {
                MemoryProfiler::on_alloc(4096);
            }
        }
        {
            // Another query, not sampled.
            let other = query_payload("mem-other", QueryPerf::next_query_token());
            let _guard = ThreadTracker::tracking(other);
            MemoryProfiler::on_alloc(64 * SAMPLE_INTERVAL);
        }
        // Not working for a query, not sampled.
        MemoryProfiler::on_alloc(64 * SAMPLE_INTERVAL);

        let folded = profiler.dump_folded().unwrap();
        let estimated = bytes_under(&folded, "HashJoin [#3]");
        let error = (estimated as f64 - allocated as f64).abs() / allocated as f64;
        assert!(error < 0.05, "estimated {estimated}, allocated {allocated}");
        assert_eq!(bytes_under(&folded, ""), estimated, "{folded}");
        // Query-scoped profiles are not rooted at the query id.
        assert!(!folded.contains("mem-profiled"));
        // The frames of the profiler are dropped. Symbols resolved from
        // debug info may lack their module path, so match the plain names.
        for line in folded.lines() {
            let (stack, _) = line.rsplit_once(' ').unwrap();
            for frame in stack.split(';') {
                assert!(
                    !frame.ends_with("record")
                        && !frame.ends_with("on_sampled_alloc")
                        && !frame.contains("trace_unsynchronized"),
                    "{line}"
                );
            }
        }

        let svg = MemoryProfiler::render_flamegraph(&folded).unwrap();
        assert!(svg.contains("bytes"));
        assert!(svg.contains("HashJoin [#3]"));
    }

    #[test]
    fn test_process_scope_samples_every_query() {
        let _serial = serial();
        ThreadTracker::init();

        let profiler = MemoryProfiler::start(ProfileScope::Process).unwrap();
        for query_id in ["mem-q1", "mem-q2"] {
            let payload = query_payload(query_id, QueryPerf::next_query_token());
            let _guard = ThreadTracker::tracking(payload);
            MemoryProfiler::on_alloc(64 * SAMPLE_INTERVAL);
        }
        {
            // A query whose payload only carries its token, e.g. Flight SQL.
            let mut payload = ThreadTracker::new_tracking_payload();
            payload.perf_query_token = QueryPerf::next_query_token();
            let _guard = ThreadTracker::tracking(payload);
            MemoryProfiler::on_alloc(64 * SAMPLE_INTERVAL);
        }
        {
            // Tracked, but not working for a query.
            let _guard = ThreadTracker::tracking(ThreadTracker::new_tracking_payload());
            MemoryProfiler::on_alloc(64 * SAMPLE_INTERVAL);
        }

        let folded = profiler.dump_folded().unwrap();
        // The intervals are random, 64 samples deviate by about 2.3 samples.
        for query_id in ["mem-q1", "mem-q2", super::UNKNOWN_QUERY_ID] {
            let estimated = bytes_under(&folded, query_id);
            assert!(
                estimated >= 48 * SAMPLE_INTERVAL as u64
                    && estimated <= 80 * SAMPLE_INTERVAL as u64,
                "{query_id}: {estimated}"
            );
            assert!(folded.contains(&format!("{query_id};(no plan node);")));
        }
        drop(profiler);

        // Stopped, nothing is sampled anymore.
        let payload = query_payload("mem-q1", QueryPerf::next_query_token());
        let _guard = ThreadTracker::tracking(payload);
        MemoryProfiler::on_alloc(64 * SAMPLE_INTERVAL);
        assert!(super::SAMPLES.lock().is_none());
    }

    #[test]
    fn test_render_flamegraph() {
        assert_eq!(MemoryProfiler::render_flamegraph("").unwrap(), "");

        let folded = "HashJoin [#3];build 3072\n(no plan node);main 1024\n";
        let svg = MemoryProfiler::render_flamegraph(folded).unwrap();
        assert!(svg.contains("build (3,072 bytes, 75.00%)"), "{svg}");
    }

    #[test]
    fn test_is_allocator_frame() {
        assert!(is_allocator_frame(
            "<databend_common_base::mem_allocator::global::GlobalAllocator<T> as core::alloc::global::GlobalAlloc>::alloc",
            None
        ));
        assert!(is_allocator_frame("__rust_alloc", None));
        assert!(is_allocator_frame("alloc::alloc::exchange_malloc", None));
        assert!(is_allocator_frame(
            "record",
            Some("/databend/src/common/base/src/runtime/perf/memory_profiler.rs")
        ));
        assert!(is_allocator_frame(
            "trace",
            Some("/cargo/git/checkouts/backtrace-rs-1/72265be/src/backtrace/mod.rs")
        ));
        assert!(!is_allocator_frame(
            "alloc::raw_vec::RawVec<T>::grow_one",
            Some("/rustc/library/alloc/src/raw_vec/mod.rs")
        ));
        assert!(!is_allocator_frame(
            "build",
            Some("/databend/src/query/service/src/join/build.rs")
        ));
    }
}
