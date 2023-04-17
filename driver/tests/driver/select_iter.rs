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

use databend_driver::{new_connection, Connection};
use tokio_stream::StreamExt;

use crate::common::DEFAULT_DSN;

async fn prepare(name: &str) -> (Box<dyn Connection>, String) {
    let dsn = option_env!("TEST_DATABEND_DSN").unwrap_or(DEFAULT_DSN);
    let table = format!("{}_{}", name, chrono::Utc::now().timestamp());
    let conn = new_connection(dsn).unwrap();
    (conn, table)
}

#[tokio::test]
async fn select_iter() {
    let (conn, table) = prepare("select_iter").await;
    let sql_create = format!(
        "CREATE TABLE `{}` (
		i64 Int64,
		u64 UInt64,
		f64 Float64,
		s   String,
		s2  String,
		d   Date,
		t   DateTime
    );",
        table
    );
    conn.exec(&sql_create).await.unwrap();
    let sql_insert = format!(
        "INSERT INTO `{}` VALUES
        (-1, 1, 1.0, '1', '1', '2011-03-06', '2011-03-06 06:20:00'),
        (-2, 2, 2.0, '2', '2', '2012-05-31', '2012-05-31 11:20:00'),
        (-3, 3, 3.0, '3', '2', '2016-04-04', '2016-04-04 11:30:00')",
        table
    );
    type RowResult = (
        i64,
        u64,
        f64,
        String,
        String,
        chrono::NaiveDate,
        chrono::NaiveDateTime,
    );
    let expected: Vec<RowResult> = vec![
        (
            -1,
            1,
            1.0,
            "1".into(),
            "1".into(),
            chrono::NaiveDate::from_ymd_opt(2011, 3, 6).unwrap(),
            chrono::DateTime::parse_from_rfc3339("2011-03-06T06:20:00Z")
                .unwrap()
                .naive_utc(),
        ),
        (
            -2,
            2,
            2.0,
            "2".into(),
            "2".into(),
            chrono::NaiveDate::from_ymd_opt(2012, 5, 31).unwrap(),
            chrono::DateTime::parse_from_rfc3339("2012-05-31T11:20:00Z")
                .unwrap()
                .naive_utc(),
        ),
        (
            -3,
            3,
            3.0,
            "3".into(),
            "2".into(),
            chrono::NaiveDate::from_ymd_opt(2016, 4, 4).unwrap(),
            chrono::DateTime::parse_from_rfc3339("2016-04-04T11:30:00Z")
                .unwrap()
                .naive_utc(),
        ),
    ];
    conn.exec(&sql_insert).await.unwrap();
    let sql_select = format!("SELECT * FROM `{}`", table);
    let mut rows = conn.query_iter(&sql_select).await.unwrap();
    let mut row_count = 0;
    while let Some(row) = rows.next().await {
        let v: RowResult = row.unwrap().try_into().unwrap();
        assert_eq!(v, expected[row_count]);
        row_count += 1;
    }
}

#[tokio::test]
async fn select_numbers() {
    let (conn, _) = prepare("select_numbers").await;
    let rows = conn.query_iter("select * from NUMBERS(5)").await.unwrap();
    let ret: Vec<u64> = rows
        .map(|r| r.unwrap().try_into().unwrap())
        .collect::<Vec<(u64,)>>()
        .await
        .into_iter()
        .map(|r| r.0)
        .collect();
    assert_eq!(ret, vec![0, 1, 2, 3, 4]);
}

#[tokio::test]
async fn select_sleep() {
    let (conn, _) = prepare("select_sleep").await;
    let mut rows = conn.query_iter("select SLEEP(2);").await.unwrap();
    let mut result = vec![];
    while let Some(row) = rows.next().await {
        let row: (u8,) = row.unwrap().try_into().unwrap();
        result.push(row.0);
    }
    assert_eq!(result, vec![0]);
}
