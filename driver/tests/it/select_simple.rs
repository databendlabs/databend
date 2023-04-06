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

use chrono::{DateTime, NaiveDate, NaiveDateTime};
use databend_driver::{new_connection, Connection};

use crate::common::DEFAULT_DSN;

fn prepare() -> Box<dyn Connection> {
    let dsn = option_env!("TEST_DATABEND_DSN").unwrap_or(DEFAULT_DSN);
    new_connection(dsn).unwrap()
}

#[tokio::test]
async fn select_string() {
    let conn = prepare();
    let row = conn.query_row("select 'hello'").await.unwrap();
    assert!(row.is_some());
    let row = row.unwrap();
    let (val,): (String,) = row.try_into().unwrap();
    assert_eq!(val, "hello");
}

#[tokio::test]
async fn select_boolean() {
    let conn = prepare();
    let row = conn.query_row("select true").await.unwrap();
    assert!(row.is_some());
    let row = row.unwrap();
    let (val,): (bool,) = row.try_into().unwrap();
    assert!(val);
}

#[tokio::test]
async fn select_u16() {
    let conn = prepare();
    let row = conn.query_row("select to_uint16(15532)").await.unwrap();
    assert!(row.is_some());
    let row = row.unwrap();
    let (val,): (u16,) = row.try_into().unwrap();
    assert_eq!(val, 15532);
}

#[tokio::test]
async fn select_f64() {
    let conn = prepare();
    let row = conn
        .query_row("select to_float64(3.1415925)")
        .await
        .unwrap();
    assert!(row.is_some());
    let row = row.unwrap();
    let (val,): (f64,) = row.try_into().unwrap();
    assert_eq!(val, 3.1415925);
}

#[tokio::test]
async fn select_date() {
    let conn = prepare();
    let row = conn
        .query_row("select to_date('2023-03-28')")
        .await
        .unwrap();
    assert!(row.is_some());
    let row = row.unwrap();
    {
        let (val,): (i32,) = row.clone().try_into().unwrap();
        assert_eq!(val, 19444);
    }
    {
        let (val,): (NaiveDate,) = row.try_into().unwrap();
        assert_eq!(val, NaiveDate::from_ymd_opt(2023, 3, 28).unwrap());
    }
}

#[tokio::test]
async fn select_datetime() {
    let conn = prepare();
    let row = conn
        .query_row("select to_datetime('2023-03-28 12:34:56.789')")
        .await
        .unwrap();
    assert!(row.is_some());
    let row = row.unwrap();
    {
        let (val,): (i64,) = row.clone().try_into().unwrap();
        assert_eq!(val, 1680006896789000000);
    }
    {
        let (val,): (NaiveDateTime,) = row.try_into().unwrap();
        assert_eq!(
            val,
            DateTime::parse_from_rfc3339("2023-03-28T12:34:56.789Z")
                .unwrap()
                .naive_utc()
        );
    }
}

#[tokio::test]
async fn select_array() {
    let conn = prepare();
    let row = conn.query_row("select [1, 2, 3, 4, 5]").await.unwrap();
    assert!(row.is_some());
    let row = row.unwrap();
    let (val,): (String,) = row.try_into().unwrap();
    // TODO: fix parse to real array
    assert_eq!(val, "[1,2,3,4,5]");
}

#[tokio::test]
async fn select_sleep() {
    let conn = prepare();
    let row = conn.query_row("select SLEEP(2);").await.unwrap();
    assert!(row.is_some());
    let row = row.unwrap();
    let (val,): (u8,) = row.try_into().unwrap();
    assert_eq!(val, 0);
}
