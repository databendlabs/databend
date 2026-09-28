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

//! Sampling allocation profiler for a single query, used by `EXPLAIN PERF MEMORY`.
//!
//! Threads working for the profiled query carry an [`AllocProfile`] in their tracking payload.
//! The global allocator reports every allocated byte of such threads to [`AllocProfile::on_alloc`],
//! which takes one sample every [`SAMPLE_INTERVAL`] bytes on average. A sample records the call
//! stack and the plan node the thread is working for, and stands for `SAMPLE_INTERVAL` bytes.
//!
//! The profile shows where memory is allocated, including allocations freed soon after. It does
//! not show which memory is alive at a given moment.

use std::collections::HashMap;
use std::ffi::c_void;
use std::sync::Arc;

use parking_lot::Mutex;

use crate::runtime::ThreadTracker;
use crate::runtime::perf::stack_site;

/// The average number of allocated bytes between two samples.
pub const SAMPLE_INTERVAL: usize = 512 * 1024;

const MAX_FRAMES: usize = 64;

/// Whether the current thread works for a query being profiled.
#[thread_local]
static mut PROFILE_FLAG: bool = false;

#[thread_local]
static mut SAMPLER: ThreadSampler = ThreadSampler {
    countdown: 0,
    rng: 0,
    in_sampler: false,
};

struct ThreadSampler {
    countdown: i64,
    rng: u64,
    in_sampler: bool,
}

impl ThreadSampler {
    fn next_interval(&mut self) -> i64 {
        if self.rng == 0 {
            // Seed from the address of the thread local, distinct per thread.
            self.rng = (self as *const _ as u64) | 1;
        }
        // xorshift64
        self.rng ^= self.rng << 13;
        self.rng ^= self.rng >> 7;
        self.rng ^= self.rng << 17;
        // Uniform in [interval / 2, interval * 3 / 2) to avoid aliasing with allocation patterns.
        let interval = SAMPLE_INTERVAL as u64;
        (interval / 2 + self.rng % interval) as i64
    }
}

#[derive(Hash, PartialEq, Eq)]
struct SampleKey {
    plan: Option<u32>,
    /// Instruction pointers, innermost first.
    frames: Box<[usize]>,
}

/// The sampled allocations of one query.
pub struct AllocProfile {
    samples: Mutex<HashMap<SampleKey, u64>>,
    plan_names: Mutex<HashMap<u32, String>>,
}

/// One aggregated call stack of an [`AllocProfile`].
pub struct AllocStack {
    /// The plan node the allocations were made for, `None` outside plan nodes.
    pub plan: Option<(u32, String)>,
    /// Symbolized frames, outermost first.
    pub frames: Vec<String>,
    /// The estimated allocated bytes.
    pub bytes: u64,
}

impl AllocProfile {
    pub fn create() -> Arc<AllocProfile> {
        Arc::new(AllocProfile {
            samples: Mutex::new(HashMap::new()),
            plan_names: Mutex::new(HashMap::new()),
        })
    }

    /// Syncs the thread local flag with the tracking payload of the current thread.
    #[inline]
    pub fn sync_from_payload(enabled: bool) {
        unsafe { PROFILE_FLAG = enabled }
    }

    /// Reports `size` bytes allocated by the current thread.
    #[inline(always)]
    pub fn on_alloc(size: usize) {
        if unsafe { PROFILE_FLAG } {
            Self::on_profiled_alloc(size);
        }
    }

    #[inline(never)]
    fn on_profiled_alloc(size: usize) {
        #[allow(static_mut_refs)]
        let sampler = unsafe { &mut SAMPLER };

        // Allocations made while recording a sample are not sampled.
        if sampler.in_sampler {
            return;
        }

        sampler.countdown -= size as i64;
        if sampler.countdown > 0 {
            return;
        }

        let mut samples = 0;
        while sampler.countdown <= 0 {
            sampler.countdown += sampler.next_interval();
            samples += 1;
        }

        sampler.in_sampler = true;
        Self::record(samples * SAMPLE_INTERVAL as u64);
        sampler.in_sampler = false;
    }

    fn record(bytes: u64) {
        let mut frames = [0usize; MAX_FRAMES];
        let mut depth = 0;
        // Safety: the sampler is not reentrant, see `in_sampler`.
        unsafe {
            backtrace::trace_unsynchronized(|frame| {
                frames[depth] = frame.ip() as usize;
                depth += 1;
                depth < MAX_FRAMES
            });
        }

        ThreadTracker::with_alloc_profile(|profile, plan| {
            if let Some((id, name)) = plan {
                profile
                    .plan_names
                    .lock()
                    .entry(id)
                    .or_insert_with(|| name.to_string());
            }

            let key = SampleKey {
                plan: plan.map(|(id, _)| id),
                frames: frames[..depth].into(),
            };
            *profile.samples.lock().entry(key).or_default() += bytes;
        });
    }

    /// The estimated bytes allocated by the profiled query.
    pub fn total_bytes(&self) -> u64 {
        self.samples.lock().values().sum()
    }

    /// Symbolizes the sampled call stacks.
    pub fn stacks(&self) -> Vec<AllocStack> {
        let samples = std::mem::take(&mut *self.samples.lock());
        let plan_names = self.plan_names.lock().clone();

        let mut symbols: HashMap<usize, Vec<String>> = HashMap::new();
        let mut stacks: HashMap<(Option<u32>, Vec<String>), u64> = HashMap::new();
        for (key, bytes) in samples {
            let mut frames = Vec::with_capacity(key.frames.len());
            for ip in key.frames.iter() {
                frames.extend(
                    symbols
                        .entry(*ip)
                        .or_insert_with(|| resolve(*ip))
                        .iter()
                        .cloned(),
                );
            }

            // Drop the frames of the profiler and the allocator itself.
            let skip = frames
                .iter()
                .position(|frame| !is_allocator_frame(frame))
                .unwrap_or(frames.len());
            let mut frames = frames.split_off(skip);
            frames.reverse();

            *stacks.entry((key.plan, frames)).or_default() += bytes;
        }

        let mut stacks = stacks
            .into_iter()
            .map(|((plan, frames), bytes)| AllocStack {
                plan: plan.map(|id| (id, plan_names.get(&id).cloned().unwrap_or_default())),
                frames,
                bytes,
            })
            .collect::<Vec<_>>();
        stacks.sort_by(|left, right| right.bytes.cmp(&left.bytes));
        stacks
    }
}

/// Samples below this count are dominated by sampling noise.
pub const LOW_CONFIDENCE_SAMPLES: u64 = 5;

/// One row of the tabular summary of an [`AllocProfile`].
#[derive(Debug, Clone, PartialEq)]
pub struct AllocSummaryRow {
    pub level: AllocSummaryLevel,
    /// `None` for the summary row.
    pub plan_node: Option<String>,
    /// The allocation site, only for [`AllocSummaryLevel::Site`].
    pub site: Option<String>,
    pub bytes: u64,
    pub samples: u64,
    /// The share of all bytes for the summary and plan rows, of the plan node for site rows.
    pub share: f64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AllocSummaryLevel {
    Summary,
    Plan,
    Site,
}

impl AllocSummaryLevel {
    pub fn as_str(&self) -> &'static str {
        match self {
            AllocSummaryLevel::Summary => "summary",
            AllocSummaryLevel::Plan => "plan",
            AllocSummaryLevel::Site => "site",
        }
    }
}

/// The label of a plan node in reports, e.g. `HashJoin [#3]`.
pub fn plan_label(plan: &Option<(u32, String)>) -> String {
    match plan {
        Some((id, name)) => format!("{name} [#{id}]"),
        None => "(no plan node)".to_string(),
    }
}

/// The frame a stack's allocation is attributed to in summaries: the innermost Databend frame,
/// which is more telling than the containers of the standard library and third party crates.
pub fn allocation_site(frames: &[String]) -> String {
    stack_site(frames).unwrap_or_default().to_string()
}

/// Summarizes `stacks` into a summary row, then for each plan node, from the largest, a plan row
/// followed by its `sites_per_plan` largest allocation sites.
pub fn summarize_alloc_stacks(
    stacks: &[AllocStack],
    sites_per_plan: usize,
) -> Vec<AllocSummaryRow> {
    let samples = |bytes: u64| bytes / SAMPLE_INTERVAL as u64;
    let share = |part: u64, whole: u64| part as f64 / whole.max(1) as f64;

    let total = stacks.iter().map(|stack| stack.bytes).sum::<u64>();
    let mut plans: HashMap<Option<u32>, (String, u64, HashMap<String, u64>)> = HashMap::new();
    for stack in stacks {
        let (_, bytes, sites) = plans
            .entry(stack.plan.as_ref().map(|(id, _)| *id))
            .or_insert_with(|| (plan_label(&stack.plan), 0, HashMap::new()));
        *bytes += stack.bytes;
        *sites.entry(allocation_site(&stack.frames)).or_default() += stack.bytes;
    }

    let mut plans = plans.into_values().collect::<Vec<_>>();
    plans.sort_by(|left, right| right.1.cmp(&left.1).then_with(|| left.0.cmp(&right.0)));

    let mut rows = vec![AllocSummaryRow {
        level: AllocSummaryLevel::Summary,
        plan_node: None,
        site: None,
        bytes: total,
        samples: samples(total),
        share: 1.0,
    }];
    for (plan, plan_bytes, sites) in plans {
        rows.push(AllocSummaryRow {
            level: AllocSummaryLevel::Plan,
            plan_node: Some(plan.clone()),
            site: None,
            bytes: plan_bytes,
            samples: samples(plan_bytes),
            share: share(plan_bytes, total),
        });

        let mut sites = sites.into_iter().collect::<Vec<_>>();
        sites.sort_by(|left, right| right.1.cmp(&left.1).then_with(|| left.0.cmp(&right.0)));
        rows.extend(
            sites
                .into_iter()
                .take(sites_per_plan)
                .map(|(site, bytes)| AllocSummaryRow {
                    level: AllocSummaryLevel::Site,
                    plan_node: Some(plan.clone()),
                    site: Some(site),
                    bytes,
                    samples: samples(bytes),
                    share: share(bytes, plan_bytes),
                }),
        );
    }
    rows
}

/// Folds `stacks` into `plan;frame;...;frame` lines rooted at the plan nodes, with their bytes,
/// the largest `limit` stacks first.
pub fn alloc_folded_stacks(stacks: &[AllocStack], limit: Option<usize>) -> Vec<(String, u64)> {
    // `;` separates frames in the folded format, it appears in Rust types such as `[u8; 8]`.
    let sanitize = |frame: &str| frame.replace(';', ",");

    let mut lines = stacks
        .iter()
        .map(|stack| {
            let mut frames = Vec::with_capacity(stack.frames.len() + 1);
            frames.push(plan_label(&stack.plan));
            frames.extend(stack.frames.iter().map(|frame| sanitize(frame)));
            (frames.join(";"), stack.bytes)
        })
        .collect::<Vec<_>>();
    lines.sort_by(|left, right| right.1.cmp(&left.1).then_with(|| left.0.cmp(&right.0)));
    lines.truncate(limit.unwrap_or(usize::MAX));
    lines
}

/// Renders `stacks` as a flamegraph SVG, rooted at the plan nodes.
pub fn alloc_flamegraph(stacks: &[AllocStack], title: &str) -> Result<String, String> {
    let lines = alloc_folded_stacks(stacks, None)
        .into_iter()
        .map(|(stack, bytes)| format!("{stack} {bytes}"))
        .collect::<Vec<_>>();

    let mut options = pprof::flamegraph::Options::default();
    options.title = title.to_string();
    options.count_name = "bytes".to_string();

    let mut svg = Vec::new();
    pprof::flamegraph::from_lines(&mut options, lines.iter().map(String::as_str), &mut svg)
        .map_err(|e| format!("failed to render flamegraph: {e}"))?;
    String::from_utf8(svg).map_err(|e| format!("invalid flamegraph svg: {e}"))
}

/// Symbolizes one instruction pointer, inlined functions first.
fn resolve(ip: usize) -> Vec<String> {
    let mut names = Vec::new();
    backtrace::resolve(ip as *mut c_void, |symbol| {
        if let Some(name) = symbol.name() {
            names.push(format!("{name:#}"));
        }
    });

    if names.is_empty() {
        names.push(format!("{ip:#x}"));
    }
    names
}

fn is_allocator_frame(frame: &str) -> bool {
    const PREFIXES: [&str; 7] = [
        "backtrace::",
        "databend_common_base::runtime::memory::alloc_profile::AllocProfile",
        "databend_common_base::mem_allocator",
        "__rust",
        "__rg_",
        "alloc::alloc::",
        "std::alloc::",
    ];
    // Methods of inherent and trait impls are demangled as `<Type>::method`.
    let frame = frame.trim_start_matches('<');
    PREFIXES.iter().any(|prefix| frame.starts_with(prefix))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::runtime::ThreadTracker;

    #[inline(never)]
    fn allocate_in_test(bytes: usize) -> Vec<u8> {
        let vec = Vec::<u8>::with_capacity(bytes);
        AllocProfile::on_alloc(bytes);
        vec
    }

    #[test]
    fn test_sample_weight_is_unbiased() {
        let profile = AllocProfile::create();
        let mut payload = ThreadTracker::new_tracking_payload();
        payload.alloc_profile = Some(profile.clone());
        let _guard = ThreadTracker::tracking(payload);

        let allocated = 256 * 1024 * 1024;
        for _ in 0..allocated / 4096 {
            AllocProfile::on_alloc(4096);
        }

        let estimated = profile.total_bytes() as f64;
        let error = (estimated - allocated as f64).abs() / allocated as f64;
        assert!(error < 0.05, "estimated {estimated}, allocated {allocated}");
    }

    #[test]
    fn test_large_allocation_counts_multiple_samples() {
        let profile = AllocProfile::create();
        let mut payload = ThreadTracker::new_tracking_payload();
        payload.alloc_profile = Some(profile.clone());
        let _guard = ThreadTracker::tracking(payload);

        AllocProfile::on_alloc(64 * SAMPLE_INTERVAL);
        let estimated = profile.total_bytes();
        assert!(
            // The intervals are random, the count deviates by ~2.3 samples (1 sigma).
            estimated >= 48 * SAMPLE_INTERVAL as u64 && estimated <= 80 * SAMPLE_INTERVAL as u64,
            "{estimated}"
        );
    }

    #[test]
    fn test_not_profiled_thread_is_ignored() {
        let profile = AllocProfile::create();
        AllocProfile::on_alloc(64 * SAMPLE_INTERVAL);
        assert_eq!(profile.total_bytes(), 0);
    }

    #[test]
    fn test_stacks_are_symbolized() {
        let profile = AllocProfile::create();
        let mut payload = ThreadTracker::new_tracking_payload();
        payload.alloc_profile = Some(profile.clone());
        {
            let _guard = ThreadTracker::tracking(payload);
            for _ in 0..16 {
                drop(allocate_in_test(SAMPLE_INTERVAL));
            }
        }

        let stacks = profile.stacks();
        assert!(!stacks.is_empty());
        assert!(stacks.iter().all(|stack| stack.plan.is_none()));
        assert!(
            stacks.iter().any(|stack| stack
                .frames
                .iter()
                .any(|frame| frame.contains("allocate_in_test"))),
            "{:?}",
            stacks.iter().map(|x| &x.frames).collect::<Vec<_>>()
        );
        // The profiler frames are dropped.
        assert!(stacks.iter().all(|stack| {
            stack
                .frames
                .last()
                .is_none_or(|frame| !frame.contains("alloc_profile::AllocProfile"))
        }));
    }

    #[test]
    fn test_flamegraph_is_rooted_at_plan_nodes() {
        let stacks = vec![
            AllocStack {
                plan: Some((3, "HashJoin".to_string())),
                frames: vec!["main".to_string(), "build<[u8; 8]>".to_string()],
                bytes: 1024,
            },
            AllocStack {
                plan: None,
                frames: vec!["main".to_string()],
                bytes: 512,
            },
        ];

        let svg = alloc_flamegraph(&stacks, "allocations").unwrap();
        assert!(svg.contains("HashJoin [#3]"));
        assert!(svg.contains("(no plan node)"));
        // `;` is replaced as it separates frames in the folded format.
        assert!(svg.contains("build&lt;[u8, 8]&gt;") || svg.contains("build<[u8, 8]>"));
    }

    fn stack(plan: Option<(u32, &str)>, frames: &[&str], samples: u64) -> AllocStack {
        AllocStack {
            plan: plan.map(|(id, name)| (id, name.to_string())),
            frames: frames.iter().map(|x| x.to_string()).collect(),
            bytes: samples * SAMPLE_INTERVAL as u64,
        }
    }

    #[test]
    fn test_summarize_alloc_stacks() {
        let stacks = vec![
            stack(
                Some((3, "HashJoin")),
                &["main", "databend_query::join::build", "alloc::vec::grow"],
                6,
            ),
            stack(
                Some((3, "HashJoin")),
                &["main", "<databend_query::join::Probe>::gather"],
                2,
            ),
            stack(
                Some((3, "HashJoin")),
                &["other", "databend_query::join::build"],
                4,
            ),
            stack(Some((5, "TableScan")), &["main", "opendal::read"], 3),
            stack(None, &["main"], 1),
        ];

        let rows = summarize_alloc_stacks(&stacks, 1);
        let rendered = rows
            .iter()
            .map(|row| {
                format!(
                    "{} {} {} {} {:.2}",
                    row.level.as_str(),
                    row.plan_node.as_deref().unwrap_or("-"),
                    row.site.as_deref().unwrap_or("-"),
                    row.samples,
                    row.share
                )
            })
            .collect::<Vec<_>>();

        assert_eq!(rendered, vec![
            "summary - - 16 1.00",
            "plan HashJoin [#3] - 12 0.75",
            // Both call paths are merged into the innermost Databend frame.
            "site HashJoin [#3] databend_query::join::build 10 0.83",
            "plan TableScan [#5] - 3 0.19",
            // No Databend frame, the innermost frame is used.
            "site TableScan [#5] opendal::read 3 1.00",
            "plan (no plan node) - 1 0.06",
            "site (no plan node) main 1 1.00",
        ]);
    }

    #[test]
    fn test_alloc_folded_stacks() {
        let stacks = vec![
            stack(Some((3, "HashJoin")), &["main", "build<[u8; 8]>"], 2),
            stack(None, &["main"], 5),
        ];

        let lines = alloc_folded_stacks(&stacks, Some(1));
        assert_eq!(lines, vec![(
            "(no plan node);main".to_string(),
            5 * SAMPLE_INTERVAL as u64
        )]);

        let lines = alloc_folded_stacks(&stacks, None);
        assert_eq!(lines[1].0, "HashJoin [#3];main;build<[u8, 8]>");
    }
}
