// Copyright 2023 Datafuse Labs.
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

use std::io::Read;

use databend_client::APIClient;

use crate::common::DEFAULT_DSN;

fn get_dsn(presigned: bool) -> String {
    let dsn = option_env!("TEST_DATABEND_DSN").unwrap_or(DEFAULT_DSN);
    if presigned {
        dsn.to_string()
    } else {
        format!("{}&presigned_url_disabled=1", dsn)
    }
}

async fn upload_to_stage(client: APIClient) {
    let mut file = std::fs::File::open("tests/it/data/sample.csv").unwrap();
    let mut buf = Vec::new();
    file.read_to_end(&mut buf).unwrap();
    let path = chrono::Utc::now().format("%Y%m%d%H%M%S").to_string();
    let stage_location = format!("@~/{}/sample.csv", path);
    client
        .upload_to_stage(stage_location.as_str(), bytes::Bytes::from(buf))
        .await
        .unwrap();
}

#[tokio::test]
async fn upload_to_stage_with_presigned() {
    let dsn = get_dsn(true);
    let client = APIClient::from_dsn(&dsn).unwrap();
    upload_to_stage(client).await;
}

#[tokio::test]
async fn upload_to_stage_with_stream() {
    let dsn = get_dsn(false);
    let client = APIClient::from_dsn(&dsn).unwrap();
    upload_to_stage(client).await;
}
