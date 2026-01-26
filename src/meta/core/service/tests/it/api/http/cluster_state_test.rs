// Copyright 2021 Datafuse Labs
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::collections::BTreeMap;
use std::fs::File;
use std::io::Read;
use std::sync::Arc;
use std::time::Duration;

use databend_common_meta_runtime_api::RuntimeApi;
use databend_common_meta_runtime_api::TokioRuntime;
use databend_common_meta_types::Cmd;
use databend_common_meta_types::LogEntry;
use databend_common_meta_types::UpsertKV;
use databend_common_meta_types::node::Node;
use databend_common_meta_types::raft_types::new_log_id;
use databend_common_version::BUILD_INFO;
use databend_meta::api::HttpService;
use databend_meta::meta_node::meta_worker::MetaWorker;
use databend_meta::meta_service::MetaNode;
use http::Method;
use http::StatusCode;
use http::Uri;
use log::info;
use poem::Endpoint;
use poem::Request;
use poem::Route;
use poem::get;
use pretty_assertions::assert_eq;
use test_harness::test;
use tokio::time::Instant;

use crate::testing::meta_service_test_harness;
use crate::tests::service::MetaSrvTestContext;
use crate::tests::tls_constants::TEST_CA_CERT;
use crate::tests::tls_constants::TEST_CN_NAME;
use crate::tests::tls_constants::TEST_SERVER_CERT;
use crate::tests::tls_constants::TEST_SERVER_KEY;

/// Test http API "/cluster/nodes"
#[test(harness = meta_service_test_harness)]
#[fastrace::trace]
async fn test_cluster_nodes() -> anyhow::Result<()> {
    let tc0 = MetaSrvTestContext::new(0);
    let mut tc1 = MetaSrvTestContext::new(1);

    tc1.config.raft_config.single = false;
    tc1.config.raft_config.join = vec![tc0.config.raft_config.raft_api_addr().await?.to_string()];

    let _mn0 = MetaNode::<TokioRuntime>::start(&tc0.config, BUILD_INFO.semver()).await?;

    let runtime1 = TokioRuntime::new_testing("meta-io-rt-ut");
    let mn1 = MetaWorker::create_meta_worker(tc1.config.clone(), Arc::new(runtime1)).await?;
    let meta_handle_1 = Arc::new(mn1);

    let c = tc1.config.clone();
    let res = meta_handle_1
        .request(move |mn| {
            let fu = async move {
                mn.join_cluster(&c.raft_config, c.grpc_api_advertise_address())
                    .await
            };
            Box::pin(fu)
        })
        .await??;

    assert!(res.is_ok());

    let cluster_router = Route::new().at("/cluster/nodes", {
        let mh = meta_handle_1.clone();
        get(poem::endpoint::make(move |_req: Request| {
            let mh = mh.clone();
            async move { HttpService::<TokioRuntime>::nodes_handler(mh).await }
        }))
    });
    let response = cluster_router
        .call(
            Request::builder()
                .uri(Uri::from_static("/cluster/nodes"))
                .method(Method::GET)
                .finish(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);

    let body = response.into_body().into_vec().await.unwrap();
    let nodes: Vec<Node> = serde_json::from_slice(&body).unwrap();
    assert_eq!(nodes.len(), 2);
    Ok(())
}

#[test(harness = meta_service_test_harness)]
#[fastrace::trace]
async fn test_cluster_state() -> anyhow::Result<()> {
    let tc0 = MetaSrvTestContext::new(0);
    let mut tc1 = MetaSrvTestContext::new(1);

    tc1.config.raft_config.single = false;
    tc1.config.raft_config.join = vec![tc0.config.raft_config.raft_api_addr().await?.to_string()];

    let mn0 = MetaNode::<TokioRuntime>::start(&tc0.config, BUILD_INFO.semver()).await?;

    let mn1 = MetaNode::<TokioRuntime>::start(&tc1.config, BUILD_INFO.semver()).await?;
    let _ = mn1
        .join_cluster(
            &tc1.config.raft_config,
            tc1.config.grpc_api_advertise_address(),
        )
        .await?;

    info!("--- write sample data to the cluster ---");
    {
        mn0.write(LogEntry::new(Cmd::UpsertKV(
            UpsertKV::update("foo", b"foo").with_ttl(Duration::from_secs(3600)),
        )))
        .await?;
        mn0.write(LogEntry::new(Cmd::UpsertKV(UpsertKV::update(
            "foo2", b"foo2",
        ))))
        .await?;
        mn0.write(LogEntry::new(Cmd::UpsertKV(UpsertKV::update(
            "foo3", b"foo3",
        ))))
        .await?;
        mn0.write(LogEntry::new(Cmd::UpsertKV(UpsertKV::update(
            "foo4", b"foo4",
        ))))
        .await?;
        mn0.write(LogEntry::new(Cmd::UpsertKV(UpsertKV::update(
            "foo5", b"foo5",
        ))))
        .await?;
    }

    info!("--- trigger snapshot ---");
    {
        mn0.raft.trigger().snapshot().await?;
        mn0.raft
            .wait(Some(Duration::from_secs(1)))
            .snapshot(new_log_id(1, 0, 11), "trigger build snapshot")
            .await?;
    }

    let status = mn0.get_status().await;

    println!(
        "status = {}",
        serde_json::to_string_pretty(&status).unwrap()
    );

    // Assert key fields in the status response
    assert_eq!(status.id, 0);
    assert_eq!(status.state, "Leader");
    assert_eq!(status.is_leader, true);
    assert_eq!(status.current_term, 1);
    assert_eq!(status.last_log_index, 11);
    assert_eq!(status.snapshot_key_count, 6);
    assert_eq!(
        status.snapshot_key_space_stat,
        BTreeMap::from_iter([("exp-".to_string(), 1), ("kv--".to_string(), 5),])
    );
    assert_eq!(status.last_seq, 5);

    // Verify last_applied structure
    assert_eq!(status.last_applied, new_log_id(1, 0, 11));

    // Verify leader information
    assert!(status.leader.is_some());
    let leader = status.leader.unwrap();
    assert_eq!(leader.name, "0");
    assert!(leader.endpoint.to_string().starts_with("localhost:"));

    // Verify voters and non_voters
    assert_eq!(status.voters.len(), 2);
    assert_eq!(status.non_voters.len(), 0);

    // Verify replication status - check that we have replicas for both nodes
    let replication = status.replication.clone().unwrap();
    assert!(replication.contains_key(&0));
    assert!(replication.contains_key(&1));

    // Verify raft_log fields
    assert!(status.raft_log.cache_items > 0);
    assert!(status.raft_log.cache_used_size > 0);
    assert!(status.raft_log.wal_total_size > 0);
    assert_eq!(status.raft_log.wal_closed_chunk_count, 0);
    assert_eq!(status.raft_log.wal_closed_chunk_total_size, 0);

    // status = {
    //     "id": 0,
    //     "binary_version": "v1.2.757-nightly-9cd2f63257-simd(1.88.0-nightly-2025-06-18T01:24:12.760825000Z)",
    //     "data_version": "V004",
    //     "endpoint": "localhost:29000",
    //     "raft_log": {
    //       "cache_items": 7,
    //       "cache_used_size": 578,
    //       "wal_total_size": 1202,
    //       "wal_open_chunk_size": 1202,
    //       "wal_offset": 1202,
    //       "wal_closed_chunk_count": 0,
    //       "wal_closed_chunk_total_size": 0,
    //       "wal_closed_chunk_sizes": {}
    //     },
    //     "snapshot_key_count": 0,
    //     "snapshot_key_space_stat": {
    //       "exp-": 1,
    //       "kv--": 5
    //     },
    //     "state": "Leader",
    //     "is_leader": true,
    //     "current_term": 1,
    //     "last_log_index": 6,
    //     "last_applied": { "leader_id": { "term": 1, "node_id": 0 }, "index": 6 },
    //     "snapshot_last_log_id": null,
    //     "purged": null,
    //     "leader": {
    //       "name": "0",
    //       "endpoint": { "addr": "localhost", "port": 29000 },
    //       "grpc_api_advertise_address": "127.0.0.1:29001"
    //     },
    //     "replication": {
    //       "0": { "leader_id": { "term": 1, "node_id": 0 }, "index": 6 },
    //       "1": { "leader_id": { "term": 1, "node_id": 0 }, "index": 6 }
    //     },
    //     "voters": [
    //       {
    //         "name": "0",
    //         "endpoint": { "addr": "localhost", "port": 29000 },
    //         "grpc_api_advertise_address": "127.0.0.1:29001"
    //       },
    //       {
    //         "name": "1",
    //         "endpoint": { "addr": "localhost", "port": 29003 },
    //         "grpc_api_advertise_address": "127.0.0.1:29004"
    //       }
    //     ],
    //     "non_voters": [],
    //     "last_seq": 0
    //   }

    Ok(())
}

#[test(harness = meta_service_test_harness)]
#[fastrace::trace]
async fn test_http_service_cluster_state() -> anyhow::Result<()> {
    let addr_str = "127.0.0.1:30003";

    let tc0 = MetaSrvTestContext::new(0);
    let mut tc1 = MetaSrvTestContext::new(1);

    tc1.config.raft_config.single = false;
    tc1.config.raft_config.join = vec![tc0.config.raft_config.raft_api_addr().await?.to_string()];
    tc1.config.admin_api_address = addr_str.to_owned();
    tc1.config.admin_tls_server_key = TEST_SERVER_KEY.to_owned();
    tc1.config.admin_tls_server_cert = TEST_SERVER_CERT.to_owned();

    let _meta_node0 = MetaNode::<TokioRuntime>::start(&tc0.config, BUILD_INFO.semver()).await?;

    let runtime1 = TokioRuntime::new_testing("meta-io-rt-ut");
    let meta_handle_1 =
        MetaWorker::create_meta_worker(tc1.config.clone(), Arc::new(runtime1)).await?;
    let meta_handle_1 = Arc::new(meta_handle_1);

    let c = tc1.config.clone();
    let _ = meta_handle_1
        .request(move |mn| {
            let fu = async move {
                mn.join_cluster(&c.raft_config, c.grpc_api_advertise_address())
                    .await
            };
            Box::pin(fu)
        })
        .await??;

    let mut srv = HttpService::create(tc1.config.clone(), meta_handle_1);

    // test cert is issued for "localhost"
    let state_url = || format!("https://{}:30003/v1/cluster/status", TEST_CN_NAME);
    let node_url = || format!("https://{}:30003/v1/cluster/nodes", TEST_CN_NAME);

    // load cert
    let mut buf = Vec::new();
    File::open(TEST_CA_CERT)?.read_to_end(&mut buf)?;
    let cert = reqwest::Certificate::from_pem(&buf).unwrap();

    srv.do_start().await.expect("HTTP: admin api error");

    // kick off
    let client = reqwest::Client::builder()
        .add_root_certificate(cert)
        .build()
        .unwrap();

    info!("--- retry until service is ready or timeout ---");
    {
        let timeout_at = Instant::now() + Duration::from_secs(5);
        while Instant::now() < timeout_at {
            let resp = client.get(state_url()).send().await;
            if resp.is_ok() {
                break;
            }

            tokio::time::sleep(Duration::from_millis(500)).await;
        }
    }

    let resp = client.get(state_url()).send().await;
    assert!(resp.is_ok());

    let resp = resp.unwrap();
    assert!(resp.status().is_success());
    assert_eq!("/v1/cluster/status", resp.url().path());

    let state_json = resp.json::<serde_json::Value>().await.unwrap();
    assert_eq!(state_json["voters"].as_array().unwrap().len(), 2);
    assert_eq!(state_json["non_voters"].as_array().unwrap().len(), 0);
    assert_ne!(state_json["leader"].as_object(), None);

    let resp_nodes = client.get(node_url()).send().await;
    assert!(resp_nodes.is_ok());

    let resp_nodes = resp_nodes.unwrap();
    assert!(resp_nodes.status().is_success());
    assert_eq!("/v1/cluster/nodes", resp_nodes.url().path());

    let result = resp_nodes.json::<Vec<Node>>().await.unwrap();
    assert_eq!(result.len(), 2);
    Ok(())
}
