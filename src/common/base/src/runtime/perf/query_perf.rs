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

use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use log::debug;
use pprof::ProfilerGuard;
use pprof::ProfilerGuardBuilder;
use regex::Regex;

use crate::runtime::ThreadTracker;
use crate::runtime::TrackingGuard;

/// This flag need to be accessed in signal handler,
/// so we use #[thread_local] and put it separately from ThreadTracker
/// try to make sure it is async signal safe
///
/// TrackingPayload.perf_enabled will control PERF_FLAG state
#[thread_local]
static mut PERF_FLAG: bool = false;

pub type QueryPerfGuard = (TrackingGuard, ProfilerGuard<'static>);

/// Dummy struct only used to provide all operations related to PERF_FLAG
pub struct QueryPerf;

impl QueryPerf {
    pub fn flag() -> bool {
        unsafe { PERF_FLAG }
    }

    /// Sync the thread local PERF_FLAG with the given value from TrackingPayload
    /// This allows TrackingPayload to manage perf state while keeping thread_local flag
    pub fn sync_from_payload(perf_enabled: bool) {
        unsafe {
            PERF_FLAG = perf_enabled;
        }
    }

    pub fn start(frequency: i32) -> Result<QueryPerfGuard> {
        let filter_closure = || !QueryPerf::flag();
        let profiler_guard = ProfilerGuardBuilder::default()
            .frequency(frequency)
            .blocklist(&["libc", "libgcc", "pthread", "vdso"])
            .set_filter_func(filter_closure)
            .build()
            .map_err(|_e| ErrorCode::Internal("Failed to create profiler"))?;
        debug!("starting perf with frequency: {}", frequency);
        let mut payload = ThreadTracker::new_tracking_payload();
        payload.perf_enabled = true;
        let flag_guard = ThreadTracker::tracking(payload);
        Ok((flag_guard, profiler_guard))
    }

    pub fn dump(profiler_guard: &ProfilerGuard<'static>) -> Result<String> {
        let reporter = profiler_guard
            .report()
            .frames_post_processor(frames_post_processor())
            .build()
            .map_err(|_e| ErrorCode::Internal("Failed to report profiler data"))?;
        debug!("perf stop, begin to dump flamegraph");
        let mut svg = Vec::new();
        reporter
            .flamegraph(&mut svg)
            .map_err(|_e| ErrorCode::Internal("Failed to generate flamegraph SVG"))?;

        String::from_utf8(svg).map_err(|_e| ErrorCode::Internal("Failed to convert SVG to string"))
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
    use crate::runtime::QueryPerf;
    use crate::runtime::ThreadTracker;

    #[test]
    fn test_tracking_payload_perf_sync() {
        // Initialize thread tracker
        ThreadTracker::init();

        // Test initial state - should be false
        assert!(!QueryPerf::flag(), "Initial perf flag should be false");

        // Create a payload with perf enabled
        let mut payload_with_perf = ThreadTracker::new_tracking_payload();
        payload_with_perf.perf_enabled = true;

        // Apply tracking with perf enabled
        {
            let _guard = ThreadTracker::tracking(payload_with_perf);
            // Perf flag should now be true
            assert!(
                QueryPerf::flag(),
                "Perf flag should be true when payload.perf_enabled is true"
            );
        }

        // After guard drops, should restore previous state (false)
        assert!(
            !QueryPerf::flag(),
            "Perf flag should be restored to false after tracking guard drops"
        );

        // Test with perf disabled
        let payload_without_perf = ThreadTracker::new_tracking_payload();

        {
            let _guard = ThreadTracker::tracking(payload_without_perf);
            // Perf flag should remain false
            assert!(
                !QueryPerf::flag(),
                "Perf flag should be false when payload.perf_enabled is false"
            );
        }

        // Should still be false after guard drops
        assert!(
            !QueryPerf::flag(),
            "Perf flag should remain false after tracking guard drops"
        );
    }

    #[test]
    fn test_tracking_function_perf_inheritance() {
        ThreadTracker::init();

        // Create payload with perf enabled
        let mut payload = ThreadTracker::new_tracking_payload();
        payload.perf_enabled = true;

        {
            let _guard = ThreadTracker::tracking(payload);
            assert!(QueryPerf::flag(), "Perf should be enabled in outer scope");

            // Create a tracking function that should inherit the perf state
            let tracked_fn = ThreadTracker::tracking_function(|| {
                // The function should see the current perf state from TrackingPayload
                QueryPerf::flag()
            });

            let result = tracked_fn();
            assert!(
                result,
                "Tracking function should inherit perf enabled state"
            );
        }
    }

    #[tokio::test]
    async fn test_tracking_future_perf_inheritance() {
        ThreadTracker::init();

        // Create payload with perf enabled
        let mut payload = ThreadTracker::new_tracking_payload();
        payload.perf_enabled = true;

        {
            let _guard = ThreadTracker::tracking(payload);
            assert!(QueryPerf::flag(), "Perf should be enabled in outer scope");

            // Create a tracking future that should inherit the perf state
            let tracked_future = ThreadTracker::tracking_future(async {
                // The future should see the perf state from TrackingPayload
                QueryPerf::flag()
            });

            let result = tracked_future.await;
            assert!(result, "Tracking future should inherit perf enabled state");
        }

        // Test with perf disabled
        let mut payload_disabled = ThreadTracker::new_tracking_payload();
        payload_disabled.perf_enabled = false;

        {
            let _guard = ThreadTracker::tracking(payload_disabled);
            assert!(!QueryPerf::flag(), "Perf should be disabled in outer scope");

            let tracked_future = ThreadTracker::tracking_future(async { QueryPerf::flag() });

            let result = tracked_future.await;
            assert!(
                !result,
                "Tracking future should inherit perf disabled state"
            );
        }
    }
}
