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

//! `Cluster::do_action` in sequential and concurrent dispatch, and the best-effort
//! `FINISH_QUERY` broadcast that releases a query after a failed commit.

use std::collections::HashMap;
use std::net::TcpListener as StdTcpListener;
use std::sync::Arc;

use databend_base::uniq_id::GlobalUniq;
use databend_common_base::base::get_free_tcp_port;
use databend_common_config::GlobalConfig;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_settings::FlightKeepAliveParams;
use databend_meta_client::types::NodeInfo;
use databend_query::clusters::Cluster;
use databend_query::clusters::ClusterHelper;
use databend_query::clusters::FlightParams;
use databend_query::servers::flight::FlightService;
use databend_query::servers::flight::v1::actions::FINISH_QUERY;
use databend_query::servers::flight::v1::actions::FinishQueryPacket;
use databend_query::servers::flight::v1::actions::finish_query_on_nodes;
use databend_query::test_kits::TestFixture;

fn node(id: &str, flight_address: &str) -> Arc<NodeInfo> {
    let secret = GlobalConfig::instance().query.node_secret.clone();
    Arc::new(NodeInfo::create(
        id.to_string(),
        secret,
        String::new(),
        flight_address.to_string(),
        String::new(),
        String::new(),
        String::new(),
    ))
}

/// A unique node id, so tests do not share cached control channels.
fn node_id(name: &str) -> String {
    format!("cluster-do-action-{name}-{}", GlobalUniq::unique())
}

/// An address nothing listens on.
fn unreachable_address() -> String {
    format!("127.0.0.1:{}", get_free_tcp_port())
}

fn flight_params(concurrent: bool, reuse_control_channel: bool) -> FlightParams {
    FlightParams {
        timeout: 5,
        retry_times: 0,
        retry_interval: 0,
        keep_alive: FlightKeepAliveParams::default(),
        reuse_control_channel,
        concurrent,
    }
}

fn finish_packet() -> FinishQueryPacket {
    FinishQueryPacket {
        query_id: format!("no-such-query-{}", GlobalUniq::unique()),
        cause: "test".to_string(),
    }
}

fn finish_packets(ids: &[&str]) -> HashMap<String, FinishQueryPacket> {
    ids.iter()
        .map(|id| (id.to_string(), finish_packet()))
        .collect()
}

fn all_modes() -> impl Iterator<Item = (bool, bool)> {
    [false, true]
        .into_iter()
        .flat_map(|concurrent| [false, true].map(|reuse| (concurrent, reuse)))
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_do_action_reports_unreachable_node_in_both_modes() -> anyhow::Result<()> {
    let _fixture = TestFixture::setup().await?;
    let (n1, n2) = (node_id("n1"), node_id("n2"));
    // Nothing listens on these ports: both nodes are unreachable, whichever is tried first.
    let nodes = vec![
        node(&n1, &unreachable_address()),
        node(&n2, &unreachable_address()),
    ];
    let cluster = Cluster::create(nodes, n1.clone());

    for (concurrent, reuse) in all_modes() {
        let result: Result<HashMap<String, ()>> = cluster
            .do_action(
                FINISH_QUERY,
                finish_packets(&[&n1, &n2]),
                flight_params(concurrent, reuse),
            )
            .await;

        let error = result.expect_err("an unreachable node must fail the whole action");
        assert_eq!(
            error.code(),
            ErrorCode::CANNOT_CONNECT_NODE,
            "concurrent = {concurrent}, reuse = {reuse}, got {error}"
        );
    }

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_do_action_unknown_node_fails_before_dispatch() -> anyhow::Result<()> {
    let _fixture = TestFixture::setup().await?;
    let n1 = node_id("n1");
    let cluster = Cluster::create(vec![node(&n1, &unreachable_address())], n1.clone());

    for (concurrent, reuse) in all_modes() {
        let result: Result<HashMap<String, ()>> = cluster
            .do_action(
                FINISH_QUERY,
                finish_packets(&[&n1, "missing"]),
                flight_params(concurrent, reuse),
            )
            .await;

        let error = result.expect_err("a node outside the cluster must be rejected");
        assert_eq!(
            error.code(),
            ErrorCode::NOT_FOUND_CLUSTER_NODE,
            "concurrent = {concurrent}, reuse = {reuse}, got {error}"
        );
    }

    Ok(())
}

// The flight server runs on the test runtime, and test globals are only set on the test
// thread, so a server-side handler must not run on another worker thread.
#[tokio::test(flavor = "current_thread")]
async fn test_finish_query_reaches_live_nodes_despite_failed_ones() -> anyhow::Result<()> {
    let _fixture = TestFixture::setup().await?;
    let config = GlobalConfig::instance();

    let listener = StdTcpListener::bind("127.0.0.1:0")?;
    let live_address = listener.local_addr()?;
    drop(listener);
    let mut server = FlightService::create(config.as_ref().clone())?;
    server.start(live_address).await?;

    let (live, dead) = (node_id("live"), node_id("dead"));
    let nodes = vec![
        node(&live, &live_address.to_string()),
        node(&dead, &unreachable_address()),
    ];
    let cluster = Cluster::create(nodes, config.query.node_id.clone());

    for (concurrent, reuse) in all_modes() {
        let params = flight_params(concurrent, reuse);

        // A live node accepts FINISH_QUERY for a query it never received.
        let response: HashMap<String, ()> = cluster
            .do_action(FINISH_QUERY, finish_packets(&[&live]), params)
            .await?;
        assert!(response.contains_key(&live));

        // An unreachable node and a node outside the cluster neither stop nor cancel
        // the request to the live node: only they are reported as failed.
        let targets = [dead.clone(), live.clone(), "missing".to_string()];
        let mut failed = finish_query_on_nodes(&cluster, targets, &finish_packet(), params).await;
        failed.sort_by(|a, b| a.0.cmp(&b.0));
        let failed = failed
            .iter()
            .map(|(id, cause)| (id.as_str(), cause.code()))
            .collect::<Vec<_>>();
        let mut expected = [
            (dead.as_str(), ErrorCode::CANNOT_CONNECT_NODE),
            ("missing", ErrorCode::NOT_FOUND_CLUSTER_NODE),
        ];
        expected.sort();
        assert_eq!(
            failed, expected,
            "concurrent = {concurrent}, reuse = {reuse}"
        );
    }

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_finish_query_is_bounded_on_hung_node() -> anyhow::Result<()> {
    let _fixture = TestFixture::setup().await?;
    let config = GlobalConfig::instance();

    // The kernel completes the TCP handshake from the backlog, but nobody ever reads
    // or answers, as with a node that hangs.
    let hung_listener = StdTcpListener::bind("127.0.0.1:0")?;
    let hung = node_id("hung");
    let cluster = Cluster::create(
        vec![node(&hung, &hung_listener.local_addr()?.to_string())],
        config.query.node_id.clone(),
    );

    for reuse in [false, true] {
        // A long flight timeout, as with the 60s default: the cleanup must not wait
        // for it on the error path.
        let params = FlightParams {
            timeout: 60,
            ..flight_params(false, reuse)
        };
        let started = std::time::Instant::now();
        let failed =
            finish_query_on_nodes(&cluster, [hung.clone()], &finish_packet(), params).await;
        let elapsed = started.elapsed();

        assert_eq!(failed.len(), 1, "reuse = {reuse}");
        assert_eq!(failed[0].0, hung);
        assert!(
            elapsed < std::time::Duration::from_secs(15),
            "reuse = {reuse}, cleanup took {elapsed:?}"
        );
    }

    drop(hung_listener);
    Ok(())
}
