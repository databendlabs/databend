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

use tokio::fs::File;

use databend_client::APIClient;

use crate::common::DEFAULT_DSN;

async fn insert_with_stage(presign: bool) {
    let dsn = option_env!("TEST_DATABEND_DSN").unwrap_or(DEFAULT_DSN);
    let client = if presign {
        APIClient::new(&format!("{}&presign=on", dsn), None)
            .await
            .unwrap()
    } else {
        APIClient::new(&format!("{}&presign=off", dsn), None)
            .await
            .unwrap()
    };

    let file = File::open("tests/core/data/sample.csv").await.unwrap();
    let metadata = file.metadata().await.unwrap();

    let path = chrono::Utc::now().format("%Y%m%d%H%M%S%9f").to_string();
    let stage_location = format!("@~/{}/sample.csv", path);
    let table = if presign {
        format!("sample_insert_presigned_{}", path)
    } else {
        format!("sample_insert_stream_{}", path)
    };

    client
        .upload_to_stage(&stage_location, Box::new(file), metadata.len())
        .await
        .unwrap();
    let sql = format!(
        "CREATE TABLE `{}` (id UInt64, city String, number UInt64)",
        table
    );
    client.query(&sql).await.unwrap();

    let sql = format!("INSERT INTO `{}` VALUES", table);
    let file_format_options = vec![
        ("type", "CSV"),
        ("field_delimiter", ","),
        ("record_delimiter", "\n"),
        ("skip_header", "0"),
        ("quote", "'"),
    ]
    .into_iter()
    .collect();
    let copy_options = vec![("purge", "true")].into_iter().collect();

    client
        .insert_with_stage(&sql, &stage_location, file_format_options, copy_options)
        .await
        .unwrap();

    let sql = format!("SELECT * FROM `{}`", table);
    let resp = client.query(&sql).await.unwrap();
    assert_eq!(resp.data.len(), 6);
    let expect = [
        ["1", "Beijing", "100"],
        ["2", "Shanghai", "80"],
        ["3", "Guangzhou", "60"],
        ["4", "Shenzhen", "70"],
        ["5", "Shenzhen", "55"],
        ["6", "Beijing", "99"],
    ];
    let result = resp
        .data
        .into_iter()
        .map(|row| {
            row.into_iter()
                .map(|v| v.unwrap_or_default())
                .collect::<Vec<_>>()
        })
        .collect::<Vec<_>>();
    assert_eq!(result, expect);

    let sql = format!("DROP TABLE `{}`;", table);
    client.query(&sql).await.unwrap();
}

#[tokio::test]
async fn insert_with_stage_presigned() {
    insert_with_stage(true).await;
}

#[tokio::test]
async fn insert_with_stage_stream() {
    insert_with_stage(false).await;
}
