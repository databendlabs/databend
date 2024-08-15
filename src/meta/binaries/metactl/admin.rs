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

use reqwest::Client;
use serde::Deserialize;

pub struct MetaAdminClient {
    client: Client,
    endpoint: String,
}

impl MetaAdminClient {
    pub fn new(addr: &str) -> Self {
        let client = Client::new();
        MetaAdminClient {
            client,
            endpoint: format!("http://{}", addr),
        }
    }

    pub async fn status(&self) -> anyhow::Result<AdminStatusResponse> {
        let resp = self
            .client
            .get(format!("{}/v1/cluster/status", self.endpoint))
            .send()
            .await?;
        if resp.status().is_success() {
            let result = resp.json::<AdminStatusResponse>().await?;
            Ok(result)
        } else {
            let data = resp.bytes().await?;
            let msg = String::from_utf8_lossy(&data);
            Err(anyhow::anyhow!(
                "status code: {}, msg: {}",
                resp.status(),
                msg
            ))
        }
    }

    pub async fn transfer_leader(
        &self,
        target: Option<u64>,
    ) -> anyhow::Result<AdminTransferLeaderResponse> {
        let resp = match target {
            Some(to) => {
                self.client
                    .get(format!(
                        "{}/v1/ctrl/trigger_transfer_leader?to={}",
                        self.endpoint, to
                    ))
                    .send()
                    .await?
            }
            None => {
                self.client
                    .get(format!("{}/v1/ctrl/trigger_transfer_leader", self.endpoint))
                    .send()
                    .await?
            }
        };
        if resp.status().is_success() {
            let result = resp.json::<AdminTransferLeaderResponse>().await?;
            Ok(result)
        } else {
            let data = resp.bytes().await?;
            let msg = String::from_utf8_lossy(&data);
            Err(anyhow::anyhow!(
                "status code: {}, msg: {}",
                resp.status(),
                msg
            ))
        }
    }
}

#[derive(Deserialize, Debug)]
pub struct AdminStatusResponse {
    pub name: String,
}

#[derive(Deserialize, Debug)]
pub struct AdminTransferLeaderResponse {
    from: u64,
    to: u64,
    voter_ids: Vec<u64>,
}
