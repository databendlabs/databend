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

use std::fs::File;
use std::io::Read;
use std::sync::Arc;
use std::time::Duration;

use databend_common_version::BUILD_INFO;
use databend_meta::meta_node::meta_worker::MetaWorker;
use databend_meta::meta_service::MetaNode;
use databend_meta_admin::HttpService;
use databend_meta_admin::HttpServiceConfig;
use databend_meta_runtime::DatabendRuntime;
use databend_meta_runtime_api::RuntimeApi;
use databend_meta_test_harness::MetaSrvTestContext;
use databend_meta_test_harness::meta_service_test_harness;
use databend_meta_types::node::Node;
use http::Method;
use http::StatusCode;
use http::Uri;
use log::info;
use poem::Endpoint;
use poem::EndpointExt;
use poem::Request;
use test_harness::test;
use tokio::time::Instant;

use crate::tls_constants::TEST_CA_CERT;
use crate::tls_constants::TEST_CN_NAME;
use crate::tls_constants::TEST_SERVER_CERT;
use crate::tls_constants::TEST_SERVER_KEY;

// TODO(zhihanz) add tls fail case
#[test(harness = meta_service_test_harness::<DatabendRuntime, _, _>)]
#[fastrace::trace]
async fn test_http_service_tls_server() -> anyhow::Result<()> {
    let tc = MetaSrvTestContext::<DatabendRuntime>::new(0);

    let runtime = DatabendRuntime::new_testing("meta-io-rt-ut");
    let mh = MetaWorker::create_meta_worker(tc.config.clone(), Arc::new(runtime)).await?;
    let mh = Arc::new(mh);

    let http_cfg = HttpServiceConfig {
        admin: databend_meta::configs::AdminConfig {
            api_address: tc.admin.api_address.clone(),
            tls: databend_meta::configs::TlsConfig {
                cert: TEST_SERVER_CERT.to_owned(),
                key: TEST_SERVER_KEY.to_owned(),
            },
        },
        config_display: "test config".to_string(),
    };
    let mut srv = HttpService::create(http_cfg, "test-version".to_string(), mh);
    // test cert is issued for "localhost"
    let url = format!(
        "https://{}:{}/v1/health",
        TEST_CN_NAME,
        tc.admin.api_address.split(':').next_back().unwrap()
    );

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
    let resp = client.get(url).send().await;
    let resp = resp.unwrap();
    assert!(resp.status().is_success());
    assert_eq!("/v1/health", resp.url().path());
    Ok(())
}

/// Test http API "/v1/cluster/nodes" using HttpService's build_router()
#[test(harness = meta_service_test_harness::<DatabendRuntime, _, _>)]
#[fastrace::trace]
async fn test_cluster_nodes() -> anyhow::Result<()> {
    let tc0 = MetaSrvTestContext::<DatabendRuntime>::new(0);
    let mut tc1 = MetaSrvTestContext::<DatabendRuntime>::new(1);

    tc1.config.raft_config.single = false;
    tc1.config.raft_config.join = vec![
        tc0.config
            .raft_config
            .raft_api_addr::<DatabendRuntime>()
            .await?
            .to_string(),
    ];

    let _mn0 = MetaNode::<DatabendRuntime>::start(&tc0.config).await?;

    let runtime1 = DatabendRuntime::new_testing("meta-io-rt-ut");
    let mn1 = MetaWorker::create_meta_worker(tc1.config.clone(), Arc::new(runtime1)).await?;
    let meta_handle_1 = Arc::new(mn1);

    let c = tc1.config.clone();
    let res = meta_handle_1
        .request(move |mn| {
            let fu = async move { mn.join_cluster(&c).await };
            Box::pin(fu)
        })
        .await??;

    assert!(res.is_ok());

    // Use HttpService's build_router() to get the actual router
    let http_cfg = HttpServiceConfig {
        admin: tc1.admin.clone(),
        config_display: format!("{:?}", tc1.config),
    };
    let srv = HttpService::create(http_cfg, "test-version".to_string(), meta_handle_1);
    let router = srv.build_router().map_to_response();

    let response = router
        .call(
            Request::builder()
                .uri(Uri::from_static("/v1/cluster/nodes"))
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

#[test(harness = meta_service_test_harness::<DatabendRuntime, _, _>)]
#[fastrace::trace]
async fn test_http_service_cluster_state() -> anyhow::Result<()> {
    let tc0 = MetaSrvTestContext::<DatabendRuntime>::new(0);
    let mut tc1 = MetaSrvTestContext::<DatabendRuntime>::new(1);

    tc1.config.raft_config.single = false;
    tc1.config.raft_config.join = vec![
        tc0.config
            .raft_config
            .raft_api_addr::<DatabendRuntime>()
            .await?
            .to_string(),
    ];
    // tc1.admin already has an OS-assigned port from MetaSrvTestContext::new()
    tc1.admin.tls.key = TEST_SERVER_KEY.to_owned();
    tc1.admin.tls.cert = TEST_SERVER_CERT.to_owned();

    let _meta_node0 = MetaNode::<DatabendRuntime>::start(&tc0.config).await?;

    let runtime1 = DatabendRuntime::new_testing("meta-io-rt-ut");
    let meta_handle_1 =
        MetaWorker::create_meta_worker(tc1.config.clone(), Arc::new(runtime1)).await?;
    let meta_handle_1 = Arc::new(meta_handle_1);

    let c = tc1.config.clone();
    let _ = meta_handle_1
        .request(move |mn| {
            let fu = async move { mn.join_cluster(&c).await };
            Box::pin(fu)
        })
        .await??;

    // Extract port from the OS-assigned address for URL construction
    let admin_port = tc1
        .admin
        .api_address
        .split(':')
        .next_back()
        .expect("admin address should have port")
        .to_string();

    let http_cfg = HttpServiceConfig {
        admin: tc1.admin.clone(),
        config_display: format!("{:?}", tc1.config),
    };
    let mut srv = HttpService::create(http_cfg, BUILD_INFO.semver().to_string(), meta_handle_1);

    // test cert is issued for "localhost"
    let state_url = format!("https://{}:{}/v1/cluster/status", TEST_CN_NAME, admin_port);
    let node_url = format!("https://{}:{}/v1/cluster/nodes", TEST_CN_NAME, admin_port);

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
            let resp = client.get(&state_url).send().await;
            if resp.is_ok() {
                break;
            }

            tokio::time::sleep(Duration::from_millis(500)).await;
        }
    }

    let resp = client.get(&state_url).send().await;
    assert!(resp.is_ok());

    let resp = resp.unwrap();
    assert!(resp.status().is_success());
    assert_eq!("/v1/cluster/status", resp.url().path());

    let state_json = resp.json::<serde_json::Value>().await.unwrap();
    assert_eq!(state_json["voters"].as_array().unwrap().len(), 2);
    assert_eq!(state_json["non_voters"].as_array().unwrap().len(), 0);
    assert_ne!(state_json["leader"].as_object(), None);

    let resp_nodes = client.get(&node_url).send().await;
    assert!(resp_nodes.is_ok());

    let resp_nodes = resp_nodes.unwrap();
    assert!(resp_nodes.status().is_success());
    assert_eq!("/v1/cluster/nodes", resp_nodes.url().path());

    let result = resp_nodes.json::<Vec<Node>>().await.unwrap();
    assert_eq!(result.len(), 2);
    Ok(())
}
