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

use std::time::Duration;

use databend_meta_admin::HttpService;
use databend_meta_admin::HttpServiceConfig;
use databend_meta_runtime::DatabendRuntime;
use databend_meta_sled_store::openraft::async_runtime::watch::WatchReceiver;
use databend_meta_test_harness::meta_service_test_harness;
use databend_meta_test_harness::start_metasrv_cluster;
use log::info;
use pretty_assertions::assert_eq;
use test_harness::test;
use tokio::time::Instant;

/// Start a cluster of 3 nodes,
/// and send transfer leader command to the leader
/// to force it to transfer leadership to node 2.
#[test(harness = meta_service_test_harness::<DatabendRuntime, _, _>)]
#[fastrace::trace]
async fn test_transfer_leader() -> anyhow::Result<()> {
    let tcs = start_metasrv_cluster::<DatabendRuntime>(&[0, 1, 2]).await?;

    let meta0 = tcs[0].grpc_srv.as_ref().unwrap().get_meta_handle();
    let metrics = meta0.handle_raft_metrics().await?.borrow_watched().clone();
    assert_eq!(metrics.current_leader, Some(0));

    let http_cfg = HttpServiceConfig {
        admin: tcs[0].admin.clone(),
        config_display: format!("{:?}", tcs[0].config),
    };
    let mut srv = HttpService::create(http_cfg, "test-version".to_string(), meta0.clone());
    srv.do_start().await.expect("HTTP: admin api error");

    let transfer_url = || {
        format!(
            "http://{}/v1/ctrl/trigger_transfer_leader?to=2",
            &tcs[0].admin.api_address
        )
    };

    let client = reqwest::Client::builder().build().unwrap();

    info!("--- retry until service is ready or timeout ---");
    {
        let timeout_at = Instant::now() + Duration::from_secs(5);
        while Instant::now() < timeout_at {
            let resp = client.get(transfer_url()).send().await;
            info!("transfer_leader resp: {:?}", resp);

            if resp.is_ok() {
                break;
            }

            tokio::time::sleep(Duration::from_millis(500)).await;
        }
    }

    tokio::time::sleep(Duration::from_millis(500)).await;

    let metrics = meta0.handle_raft_metrics().await?.borrow_watched().clone();
    assert_eq!(metrics.current_leader, Some(2));

    {
        let meta1 = tcs[1].grpc_srv.as_ref().unwrap().get_meta_handle();
        let metrics = meta1.handle_raft_metrics().await?.borrow_watched().clone();
        assert_eq!(metrics.current_leader, Some(2));
    }
    {
        let meta2 = tcs[2].grpc_srv.as_ref().unwrap().get_meta_handle();
        let metrics = meta2.handle_raft_metrics().await?.borrow_watched().clone();
        assert_eq!(metrics.current_leader, Some(2));
    }

    Ok(())
}
