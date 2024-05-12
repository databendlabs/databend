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

use std::fs::File;
use std::io::Read;
use std::string::String;
use std::time::Duration;

use databend_common_base::base::tokio;
use databend_common_base::base::tokio::time::Instant;
use databend_common_base::base::Stoppable;
use databend_common_meta_types::Node;
use databend_meta::api::http::v1::cluster_state::nodes_handler;
use databend_meta::api::http::v1::cluster_state::status_handler;
use databend_meta::api::HttpService;
use databend_meta::meta_service::MetaNode;
use http::Method;
use http::StatusCode;
use http::Uri;
use log::info;
use poem::get;
use poem::Endpoint;
use poem::EndpointExt;
use poem::Request;
use poem::Route;
use pretty_assertions::assert_eq;
use test_harness::test;

use crate::testing::meta_service_test_harness;
use crate::tests::service::MetaSrvTestContext;
use crate::tests::tls_constants::TEST_CA_CERT;
use crate::tests::tls_constants::TEST_CN_NAME;
use crate::tests::tls_constants::TEST_SERVER_CERT;
use crate::tests::tls_constants::TEST_SERVER_KEY;

/// Test http API "/cluster/nodes"
#[test(harness = meta_service_test_harness)]
#[minitrace::trace]
async fn test_cluster_nodes() -> anyhow::Result<()> {
    let tc0 = MetaSrvTestContext::new(0);
    let mut tc1 = MetaSrvTestContext::new(1);

    tc1.config.raft_config.single = false;
    tc1.config.raft_config.join = vec![tc0.config.raft_config.raft_api_addr().await?.to_string()];

    let _mn0 = MetaNode::start(&tc0.config).await?;

    let mn1 = MetaNode::start(&tc1.config).await?;
    let res = mn1
        .join_cluster(
            &tc1.config.raft_config,
            tc1.config.grpc_api_advertise_address(),
        )
        .await?;
    assert_eq!(Ok(()), res);

    let cluster_router = Route::new()
        .at("/cluster/nodes", get(nodes_handler))
        .data(mn1.clone());
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
#[minitrace::trace]
async fn test_cluster_state() -> anyhow::Result<()> {
    let tc0 = MetaSrvTestContext::new(0);
    let mut tc1 = MetaSrvTestContext::new(1);

    tc1.config.raft_config.single = false;
    tc1.config.raft_config.join = vec![tc0.config.raft_config.raft_api_addr().await?.to_string()];

    let _mn0 = MetaNode::start(&tc0.config).await?;

    let mn1 = MetaNode::start(&tc1.config).await?;
    let _ = mn1
        .join_cluster(
            &tc1.config.raft_config,
            tc1.config.grpc_api_advertise_address(),
        )
        .await?;

    let cluster_router = Route::new()
        .at("/cluster/status", get(status_handler))
        .data(mn1.clone());
    let response = cluster_router
        .call(
            Request::builder()
                .uri(Uri::from_static("/cluster/status"))
                .method(Method::GET)
                .finish(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);

    let body = response.into_body().into_vec().await.unwrap();
    let state = serde_json::from_str::<serde_json::Value>(String::from_utf8_lossy(&body).as_ref())?;
    let voters = state["voters"].as_array().unwrap();
    let non_voters = state["non_voters"].as_array().unwrap();
    let leader = state["leader"].as_object();
    assert_eq!(voters.len(), 2);
    assert_eq!(non_voters.len(), 0);
    assert_ne!(leader, None);

    Ok(())
}

#[test(harness = meta_service_test_harness)]
#[minitrace::trace]
async fn test_http_service_cluster_state() -> anyhow::Result<()> {
    let addr_str = "127.0.0.1:30003";

    let tc0 = MetaSrvTestContext::new(0);
    let mut tc1 = MetaSrvTestContext::new(1);

    tc1.config.raft_config.single = false;
    tc1.config.raft_config.join = vec![tc0.config.raft_config.raft_api_addr().await?.to_string()];
    tc1.config.admin_api_address = addr_str.to_owned();
    tc1.config.admin_tls_server_key = TEST_SERVER_KEY.to_owned();
    tc1.config.admin_tls_server_cert = TEST_SERVER_CERT.to_owned();

    let _meta_node0 = MetaNode::start(&tc0.config).await?;

    let meta_node1 = MetaNode::start(&tc1.config).await?;
    let _ = meta_node1
        .join_cluster(
            &tc1.config.raft_config,
            tc1.config.grpc_api_advertise_address(),
        )
        .await?;

    let mut srv = HttpService::create(tc1.config.clone(), meta_node1);

    // test cert is issued for "localhost"
    let state_url = || format!("https://{}:30003/v1/cluster/status", TEST_CN_NAME);
    let node_url = || format!("https://{}:30003/v1/cluster/nodes", TEST_CN_NAME);

    // load cert
    let mut buf = Vec::new();
    File::open(TEST_CA_CERT)?.read_to_end(&mut buf)?;
    let cert = reqwest::Certificate::from_pem(&buf).unwrap();

    srv.start().await.expect("HTTP: admin api error");

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
