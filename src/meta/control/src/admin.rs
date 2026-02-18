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

use databend_meta_admin::v1::features::FeatureResponse;
use reqwest::Client;
use serde::Deserialize;
use serde::Serialize;
use serde::de::DeserializeOwned;

/// HTTP client for the `databend-meta` admin API.
pub struct MetaAdminClient {
    client: Client,
    endpoint: String,
}

impl MetaAdminClient {
    pub fn new(addr: &str) -> Self {
        MetaAdminClient {
            client: Client::new(),
            endpoint: format!("http://{}", addr),
        }
    }

    /// Send a GET request and return the response on success,
    /// or an error containing the status code and response body on failure.
    async fn checked_get(&self, path: &str) -> anyhow::Result<reqwest::Response> {
        let resp = self
            .client
            .get(format!("{}{path}", self.endpoint))
            .send()
            .await?;
        let status = resp.status();
        if status.is_success() {
            Ok(resp)
        } else {
            let data = resp.bytes().await?;
            let msg = String::from_utf8_lossy(&data);
            Err(anyhow::anyhow!("status code: {status}, msg: {msg}"))
        }
    }

    /// Send a GET request and deserialize the JSON response.
    async fn get_json<T: DeserializeOwned>(&self, path: &str) -> anyhow::Result<T> {
        let resp = self.checked_get(path).await?;
        Ok(resp.json().await?)
    }

    pub async fn status(&self) -> anyhow::Result<AdminStatusResponse> {
        self.get_json("/v1/cluster/status").await
    }

    /// Transfer raft leadership to the specified node, or to a random voter if `target` is `None`.
    pub async fn transfer_leader(
        &self,
        target: Option<u64>,
    ) -> anyhow::Result<AdminTransferLeaderResponse> {
        let mut path = "/v1/ctrl/trigger_transfer_leader".to_string();
        if let Some(to) = target {
            path = format!("{path}?to={to}");
        }
        self.get_json(&path).await
    }

    /// Trigger a raft snapshot on this node.
    pub async fn trigger_snapshot(&self) -> anyhow::Result<()> {
        self.checked_get("/v1/ctrl/trigger_snapshot").await?;
        Ok(())
    }

    /// Enable or disable a feature flag.
    pub async fn set_feature(
        &self,
        feature: &str,
        enable: bool,
    ) -> anyhow::Result<FeatureResponse> {
        self.get_json(&format!(
            "/v1/features/set?feature={feature}&enable={enable}"
        ))
        .await
    }

    /// List all feature flags and their current states.
    pub async fn list_features(&self) -> anyhow::Result<FeatureResponse> {
        self.get_json("/v1/features/list").await
    }

    /// Retrieve Prometheus-format metrics from this node.
    pub async fn get_metrics(&self) -> anyhow::Result<String> {
        let resp = self.checked_get("/v1/metrics").await?;
        Ok(resp.text().await?)
    }
}

#[derive(Serialize, Deserialize, Debug)]
pub struct AdminStatusResponse {
    pub name: String,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct AdminTransferLeaderResponse {
    pub from: u64,
    pub to: u64,
    pub voter_ids: Vec<u64>,
}
