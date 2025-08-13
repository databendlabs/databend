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

use databend_common_version::DATABEND_TELEMETRY_API_KEY;
use databend_common_version::DATABEND_TELEMETRY_ENDPOINT;

pub async fn report_node_telemetry(payload: serde_json::Value) {
    let client = match reqwest::Client::builder()
        .timeout(Duration::from_secs(3))
        .connect_timeout(Duration::from_secs(2))
        .build()
    {
        Ok(client) => client,
        Err(_) => return,
    };

    let version = payload
        .get("version")
        .and_then(|v| v.as_str())
        .unwrap_or("unknown");

    let mut request = client
        .post(DATABEND_TELEMETRY_ENDPOINT)
        .header("Content-Type", "application/json")
        .header("User-Agent", format!("Databend/{}", version));

    #[allow(clippy::const_is_empty)]
    if !DATABEND_TELEMETRY_API_KEY.is_empty() {
        request = request.header(
            "Authorization",
            format!("Bearer {}", DATABEND_TELEMETRY_API_KEY),
        );
    }

    let _ = tokio::time::timeout(Duration::from_secs(3), request.json(&payload).send()).await;
}
