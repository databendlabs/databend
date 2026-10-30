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

//! On-demand CPU profiling of a running query or of whole nodes.
//!
//! Under `/v1/queries/:query_id/perf` for the threads running a query, and
//! under `/v1/perf` for every thread of every node:
//!
//! - `POST .../start?frequency=&seconds=`
//! - `POST .../stop`
//! - `GET  .../status`
//! - `GET  .../flamegraph`: flamegraph of every node, cumulative since start,
//!   available while profiling and after it stops.
//! - `GET  ...`: a page driving the above, refreshing the flamegraph
//!   periodically. Link to the query page with the `current_query_id` of
//!   `/v1/processlist`.
//!
//! Every request is forwarded to all nodes of the cluster. A node that fails
//! to answer is reported in its status instead of failing the request.

use std::collections::BTreeMap;
use std::collections::HashMap;

use databend_common_base::runtime::QueryPerf;
use databend_common_config::GlobalConfig;
use databend_common_exception::Result;
use databend_common_settings::FlightKeepAliveParams;
use http::StatusCode;
use poem::IntoResponse;
use poem::web::Html;
use poem::web::Json;
use poem::web::Path;
use poem::web::Query;
use serde::Deserialize;
use serde::Serialize;

use crate::clusters::ClusterDiscovery;
use crate::clusters::ClusterHelper;
use crate::clusters::FlightParams;
use crate::servers::flight::v1::actions::PERF;
use crate::servers::flight::v1::actions::PerfNodeStatus;
use crate::servers::flight::v1::actions::PerfRequest;
use crate::servers::flight::v1::actions::PerfState;
use crate::servers::flight::v1::actions::PerfTarget;

const MAX_FREQUENCY: i32 = 999;

struct PerfLimits {
    frequency: i32,
    seconds: u64,
    max_seconds: u64,
}

impl PerfLimits {
    /// Sampling every busy thread costs more than sampling one query, so the
    /// whole process gets a lower frequency and a shorter duration.
    fn of(target: &PerfTarget) -> PerfLimits {
        match target {
            PerfTarget::Query(_) => PerfLimits {
                frequency: 99,
                seconds: 300,
                max_seconds: 3600,
            },
            PerfTarget::Process => PerfLimits {
                frequency: 49,
                seconds: 60,
                max_seconds: 600,
            },
        }
    }
}

#[derive(Deserialize, Default)]
pub struct StartPerfParams {
    pub frequency: Option<i32>,
    pub seconds: Option<u64>,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct PerfResponse {
    pub target: PerfTarget,
    /// Status of every node that runs, ran or failed to profile the target.
    pub nodes: BTreeMap<String, PerfNodeStatus>,
}

#[poem::handler]
#[async_backtrace::framed]
pub async fn start_query_perf(
    Path(query_id): Path<String>,
    params: Option<Query<StartPerfParams>>,
) -> poem::Result<impl IntoResponse> {
    start(PerfTarget::Query(query_id), params).await
}

#[poem::handler]
#[async_backtrace::framed]
pub async fn stop_query_perf(Path(query_id): Path<String>) -> poem::Result<impl IntoResponse> {
    let target = PerfTarget::Query(query_id);
    status_response(target.clone(), PerfRequest::Stop { target }).await
}

#[poem::handler]
#[async_backtrace::framed]
pub async fn query_perf_status(Path(query_id): Path<String>) -> poem::Result<impl IntoResponse> {
    let target = PerfTarget::Query(query_id);
    status_response(target.clone(), PerfRequest::Status { target }).await
}

#[poem::handler]
#[async_backtrace::framed]
pub async fn query_perf_flamegraph(
    Path(query_id): Path<String>,
) -> poem::Result<impl IntoResponse> {
    flamegraph(PerfTarget::Query(query_id)).await
}

#[poem::handler]
#[async_backtrace::framed]
pub async fn query_perf_page(Path(query_id): Path<String>) -> poem::Result<impl IntoResponse> {
    Ok(page(&PerfTarget::Query(query_id)))
}

#[poem::handler]
#[async_backtrace::framed]
pub async fn start_process_perf(
    params: Option<Query<StartPerfParams>>,
) -> poem::Result<impl IntoResponse> {
    start(PerfTarget::Process, params).await
}

#[poem::handler]
#[async_backtrace::framed]
pub async fn stop_process_perf() -> poem::Result<impl IntoResponse> {
    let target = PerfTarget::Process;
    status_response(target.clone(), PerfRequest::Stop { target }).await
}

#[poem::handler]
#[async_backtrace::framed]
pub async fn process_perf_status() -> poem::Result<impl IntoResponse> {
    let target = PerfTarget::Process;
    status_response(target.clone(), PerfRequest::Status { target }).await
}

#[poem::handler]
#[async_backtrace::framed]
pub async fn process_perf_flamegraph() -> poem::Result<impl IntoResponse> {
    flamegraph(PerfTarget::Process).await
}

#[poem::handler]
#[async_backtrace::framed]
pub async fn process_perf_page() -> poem::Result<impl IntoResponse> {
    Ok(page(&PerfTarget::Process))
}

async fn start(
    target: PerfTarget,
    params: Option<Query<StartPerfParams>>,
) -> poem::Result<poem::Response> {
    let params = params.map(|p| p.0).unwrap_or_default();
    let limits = PerfLimits::of(&target);
    let request = PerfRequest::Start {
        target: target.clone(),
        frequency: params
            .frequency
            .unwrap_or(limits.frequency)
            .clamp(1, MAX_FREQUENCY),
        max_seconds: params
            .seconds
            .unwrap_or(limits.seconds)
            .clamp(1, limits.max_seconds),
    };
    status_response(target, request).await
}

async fn status_response(target: PerfTarget, request: PerfRequest) -> poem::Result<poem::Response> {
    let nodes = do_perf(request)
        .await
        .map_err(internal_error)?
        .into_iter()
        .filter(|(_, status)| status.state != PerfState::NotRunning)
        .collect::<BTreeMap<_, _>>();
    let status = match nodes.is_empty() {
        true => StatusCode::NOT_FOUND,
        false => StatusCode::OK,
    };
    Ok(Json(PerfResponse { target, nodes })
        .with_status(status)
        .into_response())
}

async fn flamegraph(target: PerfTarget) -> poem::Result<poem::Response> {
    let request = PerfRequest::Dump {
        target: target.clone(),
    };
    let nodes = do_perf(request).await.map_err(internal_error)?;
    match render_flamegraphs(&GlobalConfig::instance().query.node_id, nodes) {
        Ok(html) => Ok(Html(html).into_response()),
        Err(failures) => Err(poem::Error::from_string(
            format!("No profile of {target} on any node. {failures}"),
            StatusCode::NOT_FOUND,
        )),
    }
}

fn page(target: &PerfTarget) -> Html<String> {
    const PAGE_TEMPLATE: &str = include_str!("perf_page.html");
    let limits = PerfLimits::of(target);
    let title = match target {
        PerfTarget::Query(query_id) => format!("CPU profile of query {query_id}"),
        PerfTarget::Process => "CPU profile of every thread on every node".to_string(),
    };
    Html(
        PAGE_TEMPLATE
            .replace("{{TITLE}}", &escape_html(&title))
            .replace("{{FREQUENCY}}", &limits.frequency.to_string())
            .replace("{{MAX_FREQUENCY}}", &MAX_FREQUENCY.to_string())
            .replace("{{SECONDS}}", &limits.seconds.to_string())
            .replace("{{MAX_SECONDS}}", &limits.max_seconds.to_string()),
    )
}

/// Send `request` to every node concurrently. A node that cannot be reached
/// or fails the request is reported as `Failed`.
async fn do_perf(request: PerfRequest) -> Result<HashMap<String, PerfNodeStatus>> {
    let config = GlobalConfig::instance();
    let cluster = ClusterDiscovery::instance().discover(&config).await?;

    // Rendering a flamegraph symbolizes every sampled stack and may take a
    // while, the other requests answer at once and must not stall the page on
    // an unresponsive node.
    let timeout = match request {
        PerfRequest::Dump { .. } => 60,
        _ => 10,
    };
    let flight_params = FlightParams {
        timeout,
        retry_times: 1,
        retry_interval: 1,
        keep_alive: FlightKeepAliveParams::default(),
        reuse_control_channel: false,
    };
    let requests = cluster.nodes.iter().map(|node| {
        let cluster = cluster.clone();
        let node_id = node.id.clone();
        let message = HashMap::from([(node_id.clone(), request.clone())]);
        async move {
            let status = match cluster
                .do_action::<_, PerfNodeStatus>(PERF, message, flight_params)
                .await
            {
                Ok(mut response) => response
                    .remove(&node_id)
                    .unwrap_or_else(|| PerfNodeStatus::failed("no response".to_string())),
                Err(cause) => PerfNodeStatus::failed(format!("request failed: {cause}")),
            };
            (node_id, status)
        }
    });
    Ok(futures::future::join_all(requests)
        .await
        .into_iter()
        .collect())
}

/// Render the flamegraph of every node, the local node first, with the nodes
/// that failed listed above. Without any flamegraph, return the failures.
fn render_flamegraphs(
    local_node_id: &str,
    nodes: HashMap<String, PerfNodeStatus>,
) -> std::result::Result<String, String> {
    let mut failures = Vec::new();
    let mut flamegraphs = Vec::new();
    for (node_id, status) in nodes {
        let state = match status.state {
            PerfState::Profiling => "profiling",
            PerfState::Stopped => "stopped",
            PerfState::Failed => {
                let message = status.message.unwrap_or_default();
                failures.push(format!("{node_id}: {message}"));
                continue;
            }
            PerfState::NotRunning | PerfState::Idle => continue,
        };
        let Some(flamegraph) = status.flamegraph else {
            let message = status.message.unwrap_or_default();
            failures.push(format!("{node_id}: {message}"));
            continue;
        };
        let mut title = format!("{node_id} ({state}, {}s", status.elapsed_secs);
        if let Some(message) = &status.message {
            title.push_str(", ");
            title.push_str(message);
        }
        title.push(')');
        flamegraphs.push((node_id != local_node_id, escape_html(&title), flamegraph));
    }
    failures.sort();
    flamegraphs.sort();

    let mut flamegraphs = flamegraphs.into_iter().map(|(_, title, svg)| (title, svg));
    let Some((first_title, first_svg)) = flamegraphs.next() else {
        return Err(failures.join("; "));
    };
    let notice = match failures.is_empty() {
        true => String::new(),
        false => format!(
            r#"<div style="max-width:1200px;margin:0 auto 20px auto;color:#cc3300;font-family:monospace;">Failed nodes:<br>{}</div>"#,
            failures
                .iter()
                .map(|failure| escape_html(failure))
                .collect::<Vec<_>>()
                .join("<br>")
        ),
    };
    let html = QueryPerf::pretty_display(first_title, first_svg, flamegraphs);
    Ok(html.replace("{{PERF_COUNTERS_TABLE}}", &notice))
}

fn escape_html(s: &str) -> String {
    s.replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
        .replace('"', "&quot;")
        .replace('\'', "&#x27;")
}

fn internal_error(cause: databend_common_exception::ErrorCode) -> poem::Error {
    poem::Error::from_string(
        format!("Failed to profile. cause: {cause}"),
        StatusCode::INTERNAL_SERVER_ERROR,
    )
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use super::render_flamegraphs;
    use crate::servers::flight::v1::actions::PerfNodeStatus;
    use crate::servers::flight::v1::actions::PerfState;

    fn status(state: PerfState, flamegraph: Option<&str>) -> PerfNodeStatus {
        PerfNodeStatus {
            state,
            elapsed_secs: 7,
            message: None,
            flamegraph: flamegraph.map(str::to_string),
        }
    }

    #[test]
    fn test_render_flamegraphs_local_node_first() {
        let nodes = HashMap::from([
            (
                "a-remote".to_string(),
                status(PerfState::Stopped, Some("<svg>remote</svg>")),
            ),
            (
                "z-local".to_string(),
                status(PerfState::Profiling, Some("<svg>local</svg>")),
            ),
            ("idle".to_string(), status(PerfState::Idle, None)),
            ("gone".to_string(), status(PerfState::NotRunning, None)),
        ]);
        let html = render_flamegraphs("z-local", nodes).unwrap();

        let local = html.find("Current Node: z-local (profiling, 7s)").unwrap();
        let remote = html.find("Node: a-remote (stopped, 7s)").unwrap();
        assert!(local < remote);
        assert!(!html.contains("idle"));
        assert!(!html.contains("gone"));
        assert!(!html.contains("Failed nodes"));
        assert!(!html.contains("{{PERF_COUNTERS_TABLE}}"));
    }

    #[test]
    fn test_render_flamegraphs_lists_failed_nodes() {
        let nodes = HashMap::from([
            (
                "n1".to_string(),
                status(PerfState::Profiling, Some("<svg>n1</svg>")),
            ),
            (
                "n2".to_string(),
                PerfNodeStatus::failed("request failed: <unreachable>".to_string()),
            ),
        ]);
        let html = render_flamegraphs("n1", nodes).unwrap();
        assert!(html.contains("Failed nodes:<br>n2: request failed: &lt;unreachable&gt;"));
    }

    #[test]
    fn test_render_flamegraphs_without_samples() {
        let nodes = HashMap::from([
            ("n1".to_string(), status(PerfState::NotRunning, None)),
            ("n2".to_string(), PerfNodeStatus::failed("busy".to_string())),
        ]);
        assert_eq!(render_flamegraphs("n1", nodes), Err("n2: busy".to_string()));
    }
}
