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

use std::borrow::Cow;
use std::fmt::Write;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;

use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use log::debug;
use pprof::ProfilerGuard;
use pprof::ProfilerGuardBuilder;
use regex::Regex;

use crate::runtime::TrackingPayload;

/// Whether the profiler samples the current thread, see `CpuProfiler`.
///
/// Read by the profiler's signal handler, so it is a plain thread local kept
/// apart from `ThreadTracker`, maintained by `QueryPerf::sync_from_payload`.
#[thread_local]
static mut PERF_FLAG: bool = false;

/// What the process-wide `CpuProfiler` samples: `NO_PROFILER`, `ALL_THREADS`,
/// or the `perf_query_token` of the query whose threads it samples. Claiming
/// it from `NO_PROFILER` is what makes a profiler exclusive.
static PROFILER_SCOPE: AtomicU64 = AtomicU64::new(NO_PROFILER);
const NO_PROFILER: u64 = 0;
const ALL_THREADS: u64 = u64::MAX;

/// Source of `perf_query_token`s, never `NO_PROFILER` nor `ALL_THREADS`.
static NEXT_PERF_QUERY_TOKEN: AtomicU64 = AtomicU64::new(1);

/// The threads a `CpuProfiler` samples.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ProfileScope {
    /// The threads working for the query with this `perf_query_token`.
    Query(u64),
    /// Every thread of the process.
    Process,
}

/// The CPU profiler of this process, at most one runs at a time.
///
/// Every query gets a `perf_query_token` on each node, carried in the
/// `TrackingPayload` of the threads working for it. On every payload switch,
/// `QueryPerf::sync_from_payload` sets `PERF_FLAG` if the token is the one in
/// `PROFILER_SCOPE`, and a query-scoped profiler skips samples of threads
/// without the flag. A running query is thus picked up at its next processor
/// `process()` or future poll, without cooperation from its pipeline.
pub struct CpuProfiler {
    // Dropped before `_scope`: sampling stops before the scope is released.
    profiler_guard: ProfilerGuard<'static>,
    _scope: ScopeClaim,
}

struct ScopeClaim;

impl Drop for ScopeClaim {
    fn drop(&mut self) {
        PROFILER_SCOPE.store(NO_PROFILER, Ordering::Release);
    }
}

impl CpuProfiler {
    /// Fails if another profiler is running. pprof-rs replaces the filter of a
    /// running profiler before it refuses to start a second one, so pprof-rs
    /// is not called at all then.
    pub fn start(scope: ProfileScope, frequency: i32) -> Result<CpuProfiler> {
        let claimed = match scope {
            ProfileScope::Query(token) if token == NO_PROFILER || token == ALL_THREADS => {
                return Err(ErrorCode::Internal(format!(
                    "Invalid perf query token {token}"
                )));
            }
            ProfileScope::Query(token) => token,
            ProfileScope::Process => ALL_THREADS,
        };
        if PROFILER_SCOPE
            .compare_exchange(NO_PROFILER, claimed, Ordering::AcqRel, Ordering::Relaxed)
            .is_err()
        {
            return Err(ErrorCode::Internal(
                "Another CPU profiler is running in this process (EXPLAIN PERF, admin perf API or /debug/pprof/profile)",
            ));
        }
        let scope_claim = ScopeClaim;

        let mut builder = ProfilerGuardBuilder::default()
            .frequency(frequency)
            .blocklist(&["libc", "libgcc", "pthread", "vdso"]);
        if let ProfileScope::Query(_) = scope {
            builder = builder.set_filter_func(|| !QueryPerf::flag());
        }
        let profiler_guard = builder
            .build()
            .map_err(|e| ErrorCode::Internal(format!("Failed to create profiler, {e}")))?;
        debug!("starting perf of {scope:?} with frequency: {frequency}");
        Ok(CpuProfiler {
            profiler_guard,
            _scope: scope_claim,
        })
    }

    /// The pprof report of the samples collected so far, as sampled.
    pub fn report(&self) -> Result<pprof::Report> {
        self.profiler_guard
            .report()
            .build()
            .map_err(|e| ErrorCode::Internal(format!("Failed to report profiler data, {e}")))
    }

    /// The flamegraph SVG of the samples collected so far.
    pub fn dump_svg(&self) -> Result<String> {
        Self::render_flamegraph(&self.dump_folded()?)
    }

    /// The samples collected so far as folded stacks, the common input of
    /// flamegraph tools: one `thread;outermost;...;innermost count` line per
    /// distinct stack.
    pub fn dump_folded(&self) -> Result<String> {
        let report = self
            .profiler_guard
            .report()
            .frames_post_processor(frames_post_processor())
            .build()
            .map_err(|_e| ErrorCode::Internal("Failed to report profiler data"))?;
        debug!("begin to dump folded stacks");

        let mut folded = String::new();
        for (frames, count) in report.data.iter() {
            folded.push_str(&Self::folded_frame(&frames.thread_name_or_id()));
            for frame in frames.frames.iter().rev() {
                for symbol in frame.iter().rev() {
                    folded.push(';');
                    folded.push_str(&Self::folded_frame(&symbol.name()));
                }
            }
            let _ = writeln!(folded, " {count}");
        }
        Ok(folded)
    }

    /// A frame name for folded stacks. `;` separates frames, but Rust symbols
    /// may contain it, e.g. `[u8; 32]`.
    pub fn folded_frame(name: &str) -> Cow<'_, str> {
        match name.contains(';') {
            true => Cow::Owned(name.replace(';', ",")),
            false => Cow::Borrowed(name),
        }
    }

    /// Render folded stacks as a flamegraph SVG, empty without samples.
    pub fn render_flamegraph(folded: &str) -> Result<String> {
        if folded.is_empty() {
            return Ok(String::new());
        }
        let mut svg = Vec::new();
        let mut options = inferno::flamegraph::Options::default();
        inferno::flamegraph::from_lines(&mut options, folded.lines(), &mut svg)
            .map_err(|e| ErrorCode::Internal(format!("Failed to generate flamegraph SVG, {e}")))?;
        String::from_utf8(svg).map_err(|_e| ErrorCode::Internal("Failed to convert SVG to string"))
    }
}

/// The thread side of `CpuProfiler` and the EXPLAIN PERF report layout.
pub struct QueryPerf;

impl QueryPerf {
    pub fn flag() -> bool {
        unsafe { PERF_FLAG }
    }

    /// Set `PERF_FLAG` for the payload the current thread switches to.
    pub fn sync_from_payload(payload: &TrackingPayload) {
        let scope = PROFILER_SCOPE.load(Ordering::Relaxed);
        unsafe {
            PERF_FLAG = scope != NO_PROFILER && scope == payload.perf_query_token;
        }
    }

    /// A `perf_query_token` for a new query on this node.
    pub fn next_query_token() -> u64 {
        NEXT_PERF_QUERY_TOKEN.fetch_add(1, Ordering::Relaxed)
    }

    pub fn pretty_display(
        node_id: String,
        current_perf: String,
        nodes_perf: impl Iterator<Item = (String, String)>,
    ) -> String {
        const MAIN_TEMPLATE: &str = include_str!("flamegraph_main_template.html");
        const CHILD_TEMPLATE: &str = include_str!("flamegraph_child_template.html");

        fn escape_for_html_attribute(s: &str) -> String {
            s.replace('&', "&amp;")
                .replace('<', "&lt;")
                .replace('>', "&gt;")
                .replace('"', "&quot;")
                .replace('\'', "&#x27;")
        }

        let current_child_html = CHILD_TEMPLATE.replace("{{SVG_CONTENT}}", &current_perf);
        let current_node_iframe = format!(
            r#"<div class="flamegraph-container">
            <div class="flamegraph-header">Current Node: {}</div>
            <div class="flamegraph-content">
                <iframe srcdoc="{}" scrolling="no" onload="resizeIframe(this)"></iframe>
            </div>
        </div>"#,
            node_id,
            escape_for_html_attribute(&current_child_html)
        );
        let other_nodes_iframes: String = nodes_perf
            .map(|(other_node_id, svg_content)| {
                let other_child_html = CHILD_TEMPLATE.replace("{{SVG_CONTENT}}", &svg_content);
                format!(
                    r#"<div class="flamegraph-container">
                    <div class="flamegraph-header">Node: {}</div>
                    <div class="flamegraph-content">
                        <iframe srcdoc="{}" scrolling="no" onload="resizeIframe(this)"></iframe>
                    </div>
                </div>"#,
                    other_node_id,
                    escape_for_html_attribute(&other_child_html)
                )
            })
            .collect::<String>();

        MAIN_TEMPLATE
            .replace("{{CURRENT_NODE_IFRAME}}", &current_node_iframe)
            .replace("{{OTHER_NODES_IFRAMES}}", &other_nodes_iframes)
            .replace("var fluiddrawing = true", "var fluiddrawing = false")
    }
}

const PPROF_TRACE_SYMBOL: &str =
    "<pprof::backtrace::backtrace_rs::Trace as pprof::backtrace::Trace>::trace";

fn frames_post_processor() -> impl Fn(&mut pprof::Frames) {
    // If pprof cannot get thread name, it will use thread id as fallback
    // this will make the flamegraph hard to read, so we rename such threads to "threads"
    let thread_rename = [(Regex::new(r"^\d+$").unwrap(), "threads")];

    move |frames| {
        for (regex, name) in thread_rename.iter() {
            if regex.is_match(&frames.thread_name) {
                frames.thread_name = name.to_string();
            }
        }

        // Remove frames introduced by pprof's own stack collection to keep user stacks clean.
        if let Some(pos) = frames.frames.iter().position(|frame| {
            frame
                .iter()
                .any(|symbol| symbol.name() == PPROF_TRACE_SYMBOL)
        }) {
            frames.frames.drain(..=pos);
        }

        // Mark inlined functions with "(inlined)" suffix
        for inline_frames in frames.frames.iter_mut() {
            if inline_frames.len() <= 1 {
                continue;
            }

            // Mark every symbol except the outermost one as inlined.
            let last_symbol_index = inline_frames.len() - 1;
            for symbol in inline_frames.iter_mut().take(last_symbol_index) {
                let symbol_name = symbol.name();
                if symbol_name.ends_with(" (inlined)") {
                    continue;
                }
                symbol.name = Some(format!("{symbol_name} (inlined)").into_bytes());
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::sync::Mutex;
    use std::sync::atomic::Ordering;

    use super::PROFILER_SCOPE;
    use crate::runtime::CpuProfiler;
    use crate::runtime::ProfileScope;
    use crate::runtime::QueryPerf;
    use crate::runtime::ThreadTracker;
    use crate::runtime::TrackingPayloadExt;

    /// Serializes the tests that claim or set `PROFILER_SCOPE`.
    static PROFILER_SCOPE_TESTS: Mutex<()> = Mutex::new(());

    #[test]
    fn test_folded_frame() {
        assert_eq!(CpuProfiler::folded_frame("process"), "process");
        assert_eq!(
            CpuProfiler::folded_frame("drop_in_place<[u8; 32]>"),
            "drop_in_place<[u8, 32]>"
        );
    }

    #[test]
    fn test_render_flamegraph() {
        assert_eq!(CpuProfiler::render_flamegraph("").unwrap(), "");

        let folded = "threads;execute;process 3\nthreads;execute;drop<[u8, 1]> 1\n";
        let svg = CpuProfiler::render_flamegraph(folded).unwrap();
        assert!(svg.starts_with("<?xml"));
        assert!(svg.contains("process (3 samples, 75.00%)"));
        assert!(svg.contains("drop&lt;[u8, 1]&gt; (1 samples, 25.00%)"));
    }

    #[test]
    fn test_cpu_profiler_is_exclusive() {
        let _serial = PROFILER_SCOPE_TESTS
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());

        // Samples only threads carrying `token`, which no thread does here.
        let token = QueryPerf::next_query_token();
        let first = CpuProfiler::start(ProfileScope::Query(token), 1).unwrap();
        assert_eq!(PROFILER_SCOPE.load(Ordering::Relaxed), token);

        for scope in [
            ProfileScope::Process,
            ProfileScope::Query(QueryPerf::next_query_token()),
        ] {
            let err = CpuProfiler::start(scope, 1).err().unwrap();
            assert!(err.message().contains("Another CPU profiler is running"));
        }
        assert_eq!(PROFILER_SCOPE.load(Ordering::Relaxed), token);

        drop(first);
        assert_eq!(PROFILER_SCOPE.load(Ordering::Relaxed), 0);
        let second = CpuProfiler::start(ProfileScope::Query(token), 1);
        assert!(second.is_ok(), "released when the first profiler drops");
        drop(second);

        assert!(CpuProfiler::start(ProfileScope::Query(0), 1).is_err());
    }

    #[test]
    fn test_perf_flag_follows_profiled_query() {
        let _serial = PROFILER_SCOPE_TESTS
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        ThreadTracker::init();

        let token = QueryPerf::next_query_token();
        let mut payload = ThreadTracker::new_tracking_payload();
        payload.perf_query_token = token;
        let payload = Arc::new(payload);

        {
            let _guard = ThreadTracker::tracking_inner(payload.clone());
            assert!(!QueryPerf::flag(), "no profiler is running");
        }

        // Set by `CpuProfiler::start`, which also starts the process-wide
        // profiler that this test does not need. A running query picks it up
        // at its next payload switch.
        PROFILER_SCOPE.store(token, Ordering::Relaxed);
        {
            let _guard = ThreadTracker::tracking_inner(payload.clone());
            assert!(QueryPerf::flag(), "the query is profiled");

            let inherited = ThreadTracker::tracking_function(QueryPerf::flag);
            assert!(inherited(), "tracking function inherits the token");
        }
        assert!(!QueryPerf::flag(), "restored payload has no token");

        PROFILER_SCOPE.store(QueryPerf::next_query_token(), Ordering::Relaxed);
        {
            let _guard = ThreadTracker::tracking_inner(payload.clone());
            assert!(!QueryPerf::flag(), "another query is profiled");
        }
        PROFILER_SCOPE.store(0, Ordering::Relaxed);
    }

    #[test]
    fn test_tracking_future_perf_inheritance() {
        let _serial = PROFILER_SCOPE_TESTS
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        ThreadTracker::init();

        let token = QueryPerf::next_query_token();
        let mut payload = ThreadTracker::new_tracking_payload();
        payload.perf_query_token = token;

        PROFILER_SCOPE.store(token, Ordering::Relaxed);
        assert!(!QueryPerf::flag(), "outer scope is not the query");
        let flagged = futures::executor::block_on(payload.tracking(async { QueryPerf::flag() }));
        PROFILER_SCOPE.store(0, Ordering::Relaxed);
        assert!(flagged, "tracking future inherits the token");
    }
}
