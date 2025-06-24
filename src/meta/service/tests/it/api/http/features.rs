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
use databend_common_meta_raft_store::StateMachineFeature;
use databend_common_meta_sled_store::openraft::async_runtime::watch::WatchReceiver;
use databend_meta::api::http::v1::features::FeatureResponse;
use databend_meta::api::HttpService;
use log::info;
use pretty_assertions::assert_eq;
use test_harness::test;

use crate::testing::meta_service_test_harness;
use crate::tests::start_metasrv_cluster;

/// Test features API.
#[test(harness = meta_service_test_harness)]
#[fastrace::trace]
async fn test_features() -> anyhow::Result<()> {
    let tcs = start_metasrv_cluster(&[0, 1, 2]).await?;

    let meta0 = tcs[0].grpc_srv.as_ref().unwrap().get_meta_node();
    let mut srv1 = HttpService::create(tcs[0].config.clone(), meta0.clone());
    srv1.start().await.expect("HTTP: admin api error");

    let meta1 = tcs[1].grpc_srv.as_ref().unwrap().get_meta_node();
    let mut srv1 = HttpService::create(tcs[1].config.clone(), meta1.clone());
    srv1.start().await.expect("HTTP: admin api error");

    let metrics = meta0.raft.metrics().borrow_watched().clone();
    assert_eq!(metrics.current_leader, Some(0));

    let list_features_url = |i: usize| {
        format!(
            "http://{}/v1/features/list",
            &tcs[i].config.admin_api_address
        )
    };

    let set_feature_url = |i: usize, feature: &str, enable: bool| {
        format!(
            "http://{}/v1/features/set?feature={}&enable={}",
            &tcs[i].config.admin_api_address, feature, enable
        )
    };

    let client = reqwest::Client::builder().build().unwrap();

    info!("--- retry until service is ready or timeout ---");
    {
        let timeout_at = Instant::now() + Duration::from_secs(5);
        while Instant::now() < timeout_at {
            let resp = client.get(list_features_url(0)).send().await;
            println!("node-0 list_features resp: {:?}", resp);

            if resp.is_ok() {
                let resp = resp.unwrap();
                let body = resp.text().await.unwrap();
                println!("node-0 list_features body: {}", body);

                let features: FeatureResponse = serde_json::from_str(&body).unwrap();
                assert_eq!(features.features, StateMachineFeature::all());
                assert_eq!(features.enabled, Vec::<String>::new());
                break;
            }

            tokio::time::sleep(Duration::from_millis(500)).await;
        }
    }

    // node-0 set feature
    let resp = client.get(set_feature_url(0, "dummy", true)).send().await;
    println!("node-0 set_feature resp: {:?}", resp);
    let text = resp?.text().await?;
    println!("node-0 set_feature body: {}", text);
    let features: FeatureResponse = serde_json::from_str(&text).unwrap();
    assert_eq!(features.enabled, vec!["dummy".to_string()]);

    // node-0 list features
    let resp = client.get(list_features_url(0)).send().await;
    println!("node-0 set_feature resp: {:?}", resp);
    let text = resp?.text().await?;
    println!("node-0 set_feature body: {}", text);
    let features: FeatureResponse = serde_json::from_str(&text).unwrap();
    assert_eq!(features.enabled, vec!["dummy".to_string()]);

    // node-1 list features
    let resp = client.get(list_features_url(1)).send().await;
    println!("node-1 features resp: {:?}", resp);
    let text = resp?.text().await?;
    println!("node-1 features body: {}", text);
    let features: FeatureResponse = serde_json::from_str(&text).unwrap();
    assert_eq!(features.enabled, vec!["dummy".to_string()]);

    // node-0 set feature false
    let resp = client.get(set_feature_url(0, "dummy", false)).send().await;
    println!("node-0 set_feature resp: {:?}", resp);
    let text = resp?.text().await?;
    println!("node-0 set_feature body: {}", text);
    let features: FeatureResponse = serde_json::from_str(&text).unwrap();
    assert_eq!(features.enabled, Vec::<String>::new());

    Ok(())
}
