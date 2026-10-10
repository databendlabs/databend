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

//! `EXPLAIN PERF` for the admin API: samples the CPU time or the allocations of this node, either
//! for a time range or for an already running query until it finishes.
//!
//! ```text
//! GET /debug/perf/cpu?seconds=10[&frequency=99]
//! GET /debug/perf/cpu?query_id=<id>[&max_seconds=60]
//! GET /debug/perf/memory?seconds=10
//! GET /debug/perf/memory?query_id=<id>[&max_seconds=60]
//!     [&format=html|json][&limit=N]
//! ```
//!
//! `html` is the flamegraph report of `EXPLAIN PERF`, `json` a bounded summary for agents with the
//! rows of the `table` format. Only this node is sampled: to profile a distributed query, call
//! every node running its fragments.

use std::time::Duration;
use std::time::Instant;

use databend_common_base::base::convert_byte_size;
use databend_common_base::runtime::AllocProfile;
use databend_common_base::runtime::AllocStack;
use databend_common_base::runtime::CpuStack;
use databend_common_base::runtime::LOW_CONFIDENCE_SAMPLES;
use databend_common_base::runtime::PerfTargetGuard;
use databend_common_base::runtime::PerfTargets;
use databend_common_base::runtime::QueryPerf;
use databend_common_base::runtime::SAMPLE_INTERVAL;
use databend_common_base::runtime::summarize_alloc_stacks;
use databend_common_base::runtime::summarize_cpu_stacks;
use databend_common_config::GlobalConfig;
use http::StatusCode;
use poem::IntoResponse;
use poem::Response;
use poem::web::Query;
use serde::Deserialize;
use serde::Serialize;
use serde_json::json;

use crate::interpreters::cpu_report_html;
use crate::interpreters::memory_report_html;
use crate::servers::flight::v1::exchange::DataExchangeManager;
use crate::sessions::SessionManager;

const DEFAULT_SECONDS: u64 = 10;
const MAX_SECONDS: u64 = 300;
const DEFAULT_FREQUENCY: i32 = 99;
/// The rows per level in the `json` format by default.
const DEFAULT_JSON_LIMIT: usize = 20;
const POLL_INTERVAL: Duration = Duration::from_millis(100);

#[derive(Deserialize, Debug, Default)]
#[serde(default)]
pub struct PerfRequest {
    /// Samples the whole node for this many seconds.
    seconds: Option<u64>,
    /// Samples the query until it finishes, or for at most `max_seconds`.
    query_id: Option<String>,
    max_seconds: Option<u64>,
    format: Option<String>,
    limit: Option<usize>,
    /// The CPU sampling frequency in Hz.
    frequency: Option<i32>,
}

#[derive(Clone, Copy, PartialEq)]
enum Format {
    Html,
    Json,
}

enum Target {
    Node {
        seconds: u64,
    },
    Query {
        query_id: String,
        max_seconds: Option<u64>,
    },
}

#[derive(Clone, Copy, Serialize)]
#[serde(rename_all = "snake_case")]
enum StopReason {
    Seconds,
    QueryFinished,
    MaxSeconds,
}

struct Request {
    target: Target,
    format: Format,
    limit: Option<usize>,
    frequency: i32,
}

impl PerfRequest {
    fn parse(self) -> poem::Result<Request> {
        let format = match self.format.as_deref().unwrap_or("html") {
            "html" => Format::Html,
            "json" => Format::Json,
            other => {
                return Err(bad_request(format!(
                    "unknown format '{other}', expected html or json"
                )));
            }
        };

        let target = match (self.seconds, self.query_id) {
            (Some(_), Some(_)) => {
                return Err(bad_request("pass either seconds or query_id, not both"));
            }
            (_, Some(query_id)) => {
                if let Some(max_seconds) = self.max_seconds {
                    check_seconds("max_seconds", max_seconds)?;
                }
                Target::Query {
                    query_id,
                    max_seconds: self.max_seconds,
                }
            }
            (seconds, None) => {
                if self.max_seconds.is_some() {
                    return Err(bad_request("max_seconds requires query_id"));
                }
                let seconds = seconds.unwrap_or(DEFAULT_SECONDS);
                check_seconds("seconds", seconds)?;
                Target::Node { seconds }
            }
        };

        let frequency = self.frequency.unwrap_or(DEFAULT_FREQUENCY);
        if !(1..=1000).contains(&frequency) {
            return Err(bad_request("frequency must be in [1, 1000]"));
        }

        Ok(Request {
            target,
            format,
            limit: self.limit,
            frequency,
        })
    }
}

fn check_seconds(name: &str, seconds: u64) -> poem::Result<()> {
    match (1..=MAX_SECONDS).contains(&seconds) {
        true => Ok(()),
        false => Err(bad_request(format!("{name} must be in [1, {MAX_SECONDS}]"))),
    }
}

fn bad_request(message: impl Into<String>) -> poem::Error {
    poem::Error::from_string(message.into(), StatusCode::BAD_REQUEST)
}

fn conflict(message: impl Into<String>) -> poem::Error {
    poem::Error::from_string(message.into(), StatusCode::CONFLICT)
}

/// Whether this node runs `query_id`, as its coordinator or with some of its fragments.
fn is_query_running(query_id: &str) -> bool {
    SessionManager::instance().is_query_running(query_id)
        || DataExchangeManager::instance()
            .get_query_ctx(query_id)
            .is_ok()
}

struct Sampled {
    elapsed: Duration,
    stop_reason: StopReason,
}

/// Waits until the time range ends or the query finishes.
async fn sample(target: &Target) -> Sampled {
    let start = Instant::now();
    let stop_reason = match target {
        Target::Node { seconds } => {
            tokio::time::sleep(Duration::from_secs(*seconds)).await;
            StopReason::Seconds
        }
        Target::Query {
            query_id,
            max_seconds,
        } => {
            let deadline = max_seconds.map(|x| start + Duration::from_secs(x));
            loop {
                if !is_query_running(query_id) {
                    break StopReason::QueryFinished;
                }
                if deadline.is_some_and(|deadline| Instant::now() >= deadline) {
                    break StopReason::MaxSeconds;
                }
                tokio::time::sleep(POLL_INTERVAL).await;
            }
        }
    };
    Sampled {
        elapsed: start.elapsed(),
        stop_reason,
    }
}

fn check_query_running(target: &Target) -> poem::Result<()> {
    match target {
        Target::Query { query_id, .. } if !is_query_running(query_id) => {
            Err(poem::Error::from_string(
                format!("query {query_id} is not running on this node"),
                StatusCode::NOT_FOUND,
            ))
        }
        _ => Ok(()),
    }
}

fn target_json(target: &Target) -> serde_json::Value {
    match target {
        Target::Node { .. } => json!("node"),
        Target::Query { query_id, .. } => json!({ "query_id": query_id }),
    }
}

fn html_response(html: String) -> Response {
    Response::builder()
        .content_type("text/html; charset=utf-8")
        .body(html)
}

fn json_response(value: serde_json::Value) -> Response {
    Response::builder()
        .content_type("application/json")
        .body(value.to_string())
}

/// Samples the call stacks on CPU of the node, or of the threads running a query.
#[poem::handler]
#[async_backtrace::framed]
pub async fn perf_cpu_handler(req: Query<PerfRequest>) -> poem::Result<impl IntoResponse> {
    let req = req.0.parse()?;
    check_query_running(&req.target)?;

    // The query's threads are marked when they switch their tracking payload. The profiler only
    // samples marked threads, unless it samples the whole node.
    let _target_guard = match &req.target {
        Target::Query { query_id, .. } => Some(PerfTargets::register_query_cpu(query_id)),
        Target::Node { .. } => None,
    };
    let filtered = matches!(req.target, Target::Query { .. });
    let profiler = QueryPerf::start_profiler(req.frequency, filtered)
        .map_err(|e| conflict(format!("the CPU profiler is busy: {}", e.message())))?;

    let sampled = sample(&req.target).await;
    let stacks = QueryPerf::stacks(&profiler)
        .map_err(|e| poem::Error::from_string(e.message(), StatusCode::INTERNAL_SERVER_ERROR))?;
    drop(profiler);

    let node_id = GlobalConfig::instance().query.node_id.clone();
    match req.format {
        Format::Html => {
            let html = cpu_report_html(vec![(node_id, stacks)]).map_err(internal)?;
            Ok(html_response(html))
        }
        Format::Json => {
            let limit = req.limit.unwrap_or(DEFAULT_JSON_LIMIT);
            Ok(json_response(cpu_json(
                &req, node_id, &sampled, &stacks, limit,
            )))
        }
    }
}

fn cpu_json(
    req: &Request,
    node_id: String,
    sampled: &Sampled,
    stacks: &[CpuStack],
    limit: usize,
) -> serde_json::Value {
    let total_samples = stacks.iter().map(|stack| stack.samples).sum::<u64>();
    let rows = summarize_cpu_stacks(stacks, limit)
        .into_iter()
        .map(|row| {
            json!({
                "level": row.level.as_str(),
                "function": row.function,
                "path": row.path,
                "self_samples": row.self_samples,
                "total_samples": row.total_samples,
                "share": row.share,
                "low_confidence": row.self_samples < LOW_CONFIDENCE_SAMPLES,
            })
        })
        .collect::<Vec<_>>();
    json!({
        "mode": "cpu",
        "node": node_id,
        "target": target_json(&req.target),
        "elapsed_ms": sampled.elapsed.as_millis() as u64,
        "stop_reason": sampled.stop_reason,
        "frequency_hz": req.frequency,
        "total_samples": total_samples,
        "note": format!(
            "Call stacks on CPU sampled at {} Hz on this node only. 'function' rows rank the \
             functions by self_samples, the samples in the function itself. 'site' rows rank \
             Databend functions by the self_samples of the stacks whose innermost Databend frame \
             they are, i.e. including the library code they call. total_samples also count all \
             callees. share is of all samples by self_samples. path is the Databend callers of the \
             function (innermost first) on the call path with the most samples, with the share of \
             the row when it is reached through other paths too. Up to {limit} rows per level.",
            req.frequency
        ),
        "rows": rows,
    })
}

/// Samples the allocations of the node, or of the threads running a query.
#[poem::handler]
#[async_backtrace::framed]
pub async fn perf_memory_handler(req: Query<PerfRequest>) -> poem::Result<impl IntoResponse> {
    let req = req.0.parse()?;
    check_query_running(&req.target)?;

    let (profile, target_guard): (_, PerfTargetGuard) = match &req.target {
        Target::Query { query_id, .. } => {
            let profile = AllocProfile::create();
            let guard =
                PerfTargets::register_query_alloc(query_id, profile.clone()).map_err(conflict)?;
            (profile, guard)
        }
        Target::Node { .. } => {
            let profile = AllocProfile::create_for_node();
            let guard = PerfTargets::register_node_alloc(profile.clone()).map_err(conflict)?;
            (profile, guard)
        }
    };

    let sampled = sample(&req.target).await;
    drop(target_guard);
    let stacks = profile.stacks();

    let node_id = GlobalConfig::instance().query.node_id.clone();
    match req.format {
        Format::Html => {
            let html = memory_report_html(vec![(node_id, stacks)]).map_err(internal)?;
            Ok(html_response(html))
        }
        Format::Json => {
            let limit = req.limit.unwrap_or(DEFAULT_JSON_LIMIT);
            Ok(json_response(memory_json(
                &req, node_id, &sampled, &stacks, limit,
            )))
        }
    }
}

fn memory_json(
    req: &Request,
    node_id: String,
    sampled: &Sampled,
    stacks: &[AllocStack],
    limit: usize,
) -> serde_json::Value {
    let total_bytes = stacks.iter().map(|stack| stack.bytes).sum::<u64>();
    let rows = summarize_alloc_stacks(stacks, limit)
        .into_iter()
        .map(|row| {
            json!({
                "level": row.level.as_str(),
                "plan_node": row.plan_node,
                "site": row.site,
                "path": row.path,
                "bytes": row.bytes,
                "samples": row.samples,
                "share": row.share,
                "low_confidence": row.samples < LOW_CONFIDENCE_SAMPLES,
            })
        })
        .collect::<Vec<_>>();
    let plan_node = match req.target {
        Target::Node { .. } => "plan_node is prefixed with the query id, ",
        Target::Query { .. } => "",
    };
    json!({
        "mode": "memory",
        "node": node_id,
        "target": target_json(&req.target),
        "elapsed_ms": sampled.elapsed.as_millis() as u64,
        "stop_reason": sampled.stop_reason,
        "sample_interval_bytes": SAMPLE_INTERVAL,
        "total_bytes": total_bytes,
        "total_samples": total_bytes / SAMPLE_INTERVAL as u64,
        "note": format!(
            "Allocations of query threads estimated from one sample every ~{} allocated on this \
             node only; {plan_node}allocations outside queries are not sampled. bytes is the \
             allocation volume, including memory freed soon after, not the memory alive at a \
             given moment. share is of all bytes for 'plan' rows and of the plan node for 'site' \
             rows. A site is the innermost Databend function of the call stacks, path its \
             Databend callers (innermost first) on the call path with the most bytes, with the \
             share of the site when it is reached through other paths too. Up to {limit} sites \
             per plan node.",
            convert_byte_size(SAMPLE_INTERVAL as f64),
        ),
        "rows": rows,
    })
}

fn internal(e: databend_common_exception::ErrorCode) -> poem::Error {
    poem::Error::from_string(e.message(), StatusCode::INTERNAL_SERVER_ERROR)
}
