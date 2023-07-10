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

use std::vec;

use chrono::{NaiveDateTime, Utc};
use databend_driver::new_connection;
use tokio::fs::File;
use tokio_stream::StreamExt;

use crate::common::DEFAULT_DSN;

async fn stream_load(presigned: bool, file_type: &str) {
    let dsn = option_env!("TEST_DATABEND_DSN").unwrap_or(DEFAULT_DSN);
    let client = if presigned {
        new_connection(dsn).unwrap()
    } else {
        new_connection(&format!("{}&presigned_url_disabled=1", dsn)).unwrap()
    };
    let info = client.info().await;
    if info.handler == "FlightSQL" {
        // NOTE: FlightSQL does not support stream load
        return;
    }

    let file = File::open(format!("tests/driver/data/books.{}", file_type))
        .await
        .unwrap();
    let metadata = file.metadata().await.unwrap();

    let path = Utc::now().format("%Y%m%d%H%M%S%9f").to_string();
    let table = if presigned {
        format!("books_stream_load_{}_presigned_{}", file_type, path)
    } else {
        format!("books_stream_load_{}_{}", file_type, path)
    };

    let sql = format!(
        "CREATE TABLE `{}` (
            title VARCHAR NULL,
            author VARCHAR NULL,
            date VARCHAR NULL,
            publish_time TIMESTAMP NULL)",
        table
    );
    client.exec(&sql).await.unwrap();

    let sql = format!("INSERT INTO `{}` VALUES", table);
    let data = Box::new(file);
    let size = metadata.len();
    let file_format_options = match file_type {
        "csv" => None,
        "parquet" => Some(vec![("type", "pqrquet")].into_iter().collect()),
        _ => panic!("unsupported file type"),
    };

    let progress = client
        .stream_load(&sql, data, size, file_format_options, None)
        .await
        .unwrap();
    assert_eq!(progress.write_rows, 3);

    let sql = format!("SELECT * FROM `{}`", table);
    let rows = client.query_iter(&sql).await.unwrap();
    let result: Vec<(String, String, String, NaiveDateTime)> = rows
        .map(|r| {
            println!("{:?}", r);
            r.unwrap().try_into().unwrap()
        })
        .collect()
        .await;

    let expected = vec![
        (
            "Transaction Processing".to_string(),
            "Jim Gray".to_string(),
            "1992".to_string(),
            NaiveDateTime::parse_from_str("2020-01-01 11:11:11.345", "%Y-%m-%d %H:%M:%S%.f")
                .unwrap(),
        ),
        (
            "Readings in Database Systems".to_string(),
            "Michael Stonebraker".to_string(),
            "2004".to_string(),
            NaiveDateTime::parse_from_str("2020-01-01T11:11:11Z", "%Y-%m-%dT%H:%M:%SZ").unwrap(),
        ),
        (
            "Three Body".to_string(),
            "NULL-liucixin".to_string(),
            "2019".to_string(),
            NaiveDateTime::parse_from_str("2019-07-04T00:00:00", "%Y-%m-%dT%H:%M:%S").unwrap(),
        ),
    ];
    assert_eq!(result, expected);

    let sql = format!("DROP TABLE `{}` ALL;", table);
    client.exec(&sql).await.unwrap();
}

#[tokio::test]
async fn stream_load_with_presigned() {
    stream_load(true, "csv").await;
}

#[tokio::test]
async fn stream_load_without_presigned() {
    stream_load(false, "csv").await;
}
