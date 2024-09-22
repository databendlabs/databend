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

use databend_common_base::base::tokio;
use databend_common_base::base::tokio::time::Instant;
use databend_common_base::base::Stoppable;
use databend_common_meta_sled_store::openraft::async_runtime::watch::WatchReceiver;
use databend_meta::api::HttpService;
use log::info;
use pretty_assertions::assert_eq;
use test_harness::test;

use crate::testing::meta_service_test_harness;
use crate::tests::start_metasrv_cluster;

/// Start a cluster of 3 nodes,
/// and send transfer leader command to the leader
/// to force it to transfer leadership to node 2.
#[test(harness = meta_service_test_harness)]
#[fastrace::trace]
async fn test_transfer_leader() -> anyhow::Result<()> {
    let tcs = start_metasrv_cluster(&[0, 1, 2]).await?;

    let meta0 = tcs[0].grpc_srv.as_ref().unwrap().get_meta_node();
    let metrics = meta0.raft.metrics().borrow_watched().clone();
    assert_eq!(metrics.current_leader, Some(0));

    let mut srv = HttpService::create(tcs[0].config.clone(), meta0.clone());
    srv.start().await.expect("HTTP: admin api error");

    let transfer_url = || {
        format!(
            "http://{}/v1/ctrl/trigger_transfer_leader?to=2",
            &tcs[0].config.admin_api_address
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

    let metrics = meta0.raft.metrics().borrow_watched().clone();
    assert_eq!(metrics.current_leader, Some(2));

    {
        let meta1 = tcs[1].grpc_srv.as_ref().unwrap().get_meta_node();
        let metrics = meta1.raft.metrics().borrow_watched().clone();
        assert_eq!(metrics.current_leader, Some(2));
    }
    {
        let meta2 = tcs[2].grpc_srv.as_ref().unwrap().get_meta_node();
        let metrics = meta2.raft.metrics().borrow_watched().clone();
        assert_eq!(metrics.current_leader, Some(2));
    }

    Ok(())
}
