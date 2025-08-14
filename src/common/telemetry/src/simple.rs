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

use std::time::Duration;

pub async fn report_node_telemetry(payload: serde_json::Value, endpoint: &str, api_key: &str) {
    let client = match reqwest::Client::builder()
        .timeout(Duration::from_secs(3))
        .connect_timeout(Duration::from_secs(2))
        .build()
    {
        Ok(client) => client,
        Err(_) => return,
    };

    let version = payload
        .get("node")
        .and_then(|node| node.get("version"))
        .and_then(|v| v.as_str())
        .unwrap_or("unknown");

    let mut request = client
        .post(endpoint)
        .header("Content-Type", "application/json")
        .header("User-Agent", format!("Databend/{}", version));

    if !api_key.is_empty() {
        request = request.header("Authorization", format!("Bearer {}", api_key));
    }

    let _ = tokio::time::timeout(Duration::from_secs(3), request.json(&payload).send()).await;
}
