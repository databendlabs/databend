// Copyright 2020 Datafuse Labs.
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

use std::net::SocketAddr;

use common_base::tokio;
use metrics::counter;

use crate::metrics::MetricService;

pub static METRIC_TEST: &str = "metrics.test";

#[tokio::test]
async fn test_metric_server() -> common_exception::Result<()> {
    let mut service = MetricService::create();
    let listening = "0.0.0.0:0".parse::<SocketAddr>()?;
    let listening = service.start(listening).await?;
    let client = reqwest::Client::builder().build().unwrap();
    let url = format!("http://{}", listening);
    let resp = client.get(url.clone()).send().await;
    assert!(resp.is_ok());
    let resp = resp.unwrap();
    assert!(resp.status().is_success());
    assert_eq!(resp.text().await.unwrap().find("metrics_test 1"), None);
    counter!(METRIC_TEST, 1);

    let resp = client.get(url).send().await;
    assert!(resp.is_ok());
    let resp = resp.unwrap();
    assert!(resp.status().is_success());
    assert!(resp.text().await.unwrap().contains("metrics_test 1"));

    Ok(())
}
