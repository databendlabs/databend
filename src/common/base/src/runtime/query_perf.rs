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

// This flag need to be accessed in signal handler,
// so we use #[thread_local] and put it separately from ThreadTracker
// try to make sure it is async signal safe
#[thread_local]
static mut PERF_FLAG: bool = false;

/// Dummy struct only used to provide all operations related to PERF_FLAG
pub struct QueryPerf;

/// RAII structure used to restore the previous value of PERF_FLAG when it goes out of scope.
pub struct QueryPerfFlagGuard {
    previous: bool,
}

pub enum QueryPerfGuard {
    Both(QueryPerfFlagGuard, ProfilerGuard<'static>),
    Flag(QueryPerfFlagGuard),
}

impl QueryPerf {
    pub fn flag() -> bool {
        unsafe { PERF_FLAG }
    }

    pub fn tracking_inner(new_value: bool) -> QueryPerfFlagGuard {
        unsafe {
            let previous = PERF_FLAG;
            PERF_FLAG = new_value;
            QueryPerfFlagGuard { previous }
        }
    }

    pub fn tracking(new_value: bool) -> QueryPerfGuard {
        QueryPerfGuard::Flag(QueryPerf::tracking_inner(new_value))
    }

    pub fn init() {
        unsafe {
            let _ = PERF_FLAG;
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
        let flag_guard = QueryPerf::tracking_inner(true);
        Ok(QueryPerfGuard::Both(flag_guard, profiler_guard))
    }

    pub fn dump(profiler_guard: &ProfilerGuard<'static>) -> Result<String> {
        let reporter = profiler_guard
            .report()
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

impl Drop for QueryPerfFlagGuard {
    fn drop(&mut self) {
        unsafe {
            PERF_FLAG = self.previous;
        }
    }
}
