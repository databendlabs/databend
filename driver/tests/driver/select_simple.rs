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

use std::assert_eq;
use std::collections::HashMap;

use chrono::{DateTime, NaiveDate, NaiveDateTime};
use databend_driver::{Client, Connection, DecimalSize, NumberValue, Value};

use crate::common::DEFAULT_DSN;

async fn prepare() -> Box<dyn Connection> {
    let dsn = option_env!("TEST_DATABEND_DSN").unwrap_or(DEFAULT_DSN);
    let client = Client::new(dsn.to_string());
    client.get_conn().await.unwrap()
}

#[tokio::test]
async fn select_null() {
    {
        let conn = prepare().await;
        conn.exec("DROP TABLE IF EXISTS select_null").await.unwrap();
        conn.exec(
            "CREATE TABLE `select_null` (
            a String,
            b UInt64,
            c String
        );",
        )
        .await
        .unwrap();
        conn.exec("INSERT INTO `select_null` (a) VALUES ('NULL')")
            .await
            .unwrap();
    }
    {
        let dsn = option_env!("TEST_DATABEND_DSN").unwrap_or(DEFAULT_DSN);
        // ignore null to str test for flightsql
        if !dsn.starts_with("databend+flight://") {
            let client = Client::new(format!("{dsn}&format_null_as_str=1"));
            let conn = client.get_conn().await.unwrap();
            let row = conn.query_row("select * from select_null").await.unwrap();
            assert!(row.is_some());
            let row = row.unwrap();
            let (val1, val2, val3): (Option<String>, Option<u64>, Option<String>) =
                row.try_into().unwrap();
            assert_eq!(val1, Some("NULL".to_string()));
            assert_eq!(val2, None);
            assert_eq!(val3, Some("NULL".to_string()));
        }
    }
    {
        let dsn = option_env!("TEST_DATABEND_DSN").unwrap_or(DEFAULT_DSN);
        let client = Client::new(format!("{dsn}&format_null_as_str=0"));
        let conn = client.get_conn().await.unwrap();
        let row = conn.query_row("select * from select_null").await.unwrap();
        assert!(row.is_some());
        let row = row.unwrap();
        let (val1, val2, val3): (Option<String>, Option<u64>, Option<String>) =
            row.try_into().unwrap();
        assert_eq!(val1, Some("NULL".to_string()));
        assert_eq!(val2, None);
        assert_eq!(val3, None);
    }
    {
        let conn = prepare().await;
        conn.exec("DROP TABLE IF EXISTS select_null").await.unwrap();
    }
}

#[tokio::test]
async fn select_string() {
    let conn = prepare().await;
    let row = conn.query_row("select 'hello'").await.unwrap();
    assert!(row.is_some());
    let row = row.unwrap();
    let (val,): (String,) = row.try_into().unwrap();
    assert_eq!(val, "hello");
}

#[tokio::test]
async fn select_boolean() {
    let conn = prepare().await;
    let row = conn.query_row("select true").await.unwrap();
    assert!(row.is_some());
    let row = row.unwrap();
    let (val,): (bool,) = row.try_into().unwrap();
    assert!(val);
}

#[tokio::test]
async fn select_u16() {
    let conn = prepare().await;
    let row = conn.query_row("select to_uint16(15532)").await.unwrap();
    assert!(row.is_some());
    let row = row.unwrap();
    let (val,): (u16,) = row.try_into().unwrap();
    assert_eq!(val, 15532);
}

#[tokio::test]
async fn select_f64() {
    let conn = prepare().await;
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
    let conn = prepare().await;
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
    let conn = prepare().await;
    let row = conn
        .query_row("select to_datetime('2023-03-28 12:34:56.789')")
        .await
        .unwrap();
    assert!(row.is_some());
    let row = row.unwrap();
    {
        let (val,): (i64,) = row.clone().try_into().unwrap();
        assert_eq!(val, 1680006896789000);
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
async fn select_decimal() {
    let conn = prepare().await;
    let row = conn
        .query_row("select 1::Decimal(15,2), 2.0 + 3.0")
        .await
        .unwrap();
    assert!(row.is_some());
    let row = row.unwrap();
    assert_eq!(
        row.values().to_owned(),
        vec![
            Value::Number(NumberValue::Decimal128(
                100i128,
                DecimalSize {
                    precision: 3,
                    scale: 2
                },
            )),
            Value::Number(NumberValue::Decimal128(
                50i128,
                DecimalSize {
                    precision: 2,
                    scale: 1
                },
            )),
        ]
    );
}

#[tokio::test]
async fn select_nullable() {
    let conn = prepare().await;
    let row = conn
        .query_row("select sum(number) from numbers(0)")
        .await
        .unwrap();
    assert!(row.is_some());
    let row = row.unwrap();
    let (val,): (Option<u64>,) = row.try_into().unwrap();
    assert_eq!(val, None);
}

#[tokio::test]
async fn select_nullable_u64() {
    let conn = prepare().await;
    let row = conn
        .query_row("select sum(number) from numbers(100)")
        .await
        .unwrap();
    assert!(row.is_some());
    let row = row.unwrap();
    let (val,): (Option<u64>,) = row.try_into().unwrap();
    assert_eq!(val, Some(4950));
}

#[tokio::test]
async fn select_array() {
    let conn = prepare().await;

    let row1 = conn.query_row("select []").await.unwrap().unwrap();
    let (val1,): (Vec<String>,) = row1.try_into().unwrap();
    assert_eq!(val1, Vec::<String>::new());

    let row2 = conn
        .query_row("select [1, 2, 3, 4, 5]")
        .await
        .unwrap()
        .unwrap();
    let (val2,): (Vec<u8>,) = row2.try_into().unwrap();
    assert_eq!(val2, vec![1, 2, 3, 4, 5]);

    let row3 = conn
        .query_row("select [10::Decimal(15,2), 1.1+2.3]")
        .await
        .unwrap()
        .unwrap();
    let (val3,): (Vec<String>,) = row3.try_into().unwrap();
    assert_eq!(val3, vec!["10.00".to_string(), "3.40".to_string()]);

    let row4 = conn
        .query_row("select [to_binary('xyz')]")
        .await
        .unwrap()
        .unwrap();
    let (val4,): (Vec<Vec<u8>>,) = row4.try_into().unwrap();
    assert_eq!(val4, vec![vec![120, 121, 122]]);
}

#[tokio::test]
async fn select_map() {
    let conn = prepare().await;

    let row1 = conn.query_row("select {}").await.unwrap().unwrap();
    let (val1,): (HashMap<u8, u8>,) = row1.try_into().unwrap();
    assert_eq!(val1, HashMap::new());

    let row2 = conn
        .query_row("select {'k1':'v1','k2':'v2'}")
        .await
        .unwrap()
        .unwrap();
    let (val2,): (HashMap<String, String>,) = row2.try_into().unwrap();
    assert_eq!(
        val2,
        vec![
            ("k1".to_string(), "v1".to_string()),
            ("k2".to_string(), "v2".to_string())
        ]
        .into_iter()
        .collect()
    );

    let row3 = conn
        .query_row("select {'xx':to_date('2020-01-01')}")
        .await
        .unwrap()
        .unwrap();
    let (val3,): (HashMap<String, NaiveDate>,) = row3.try_into().unwrap();
    assert_eq!(
        val3,
        vec![(
            "xx".to_string(),
            NaiveDate::from_ymd_opt(2020, 1, 1).unwrap()
        )]
        .into_iter()
        .collect()
    );

    let row4 = conn
        .query_row("select {1: 'a', 2: 'b'}")
        .await
        .unwrap()
        .unwrap();
    let (val4,): (HashMap<u8, String>,) = row4.try_into().unwrap();
    assert_eq!(
        val4,
        vec![(1, "a".to_string()), (2, "b".to_string())]
            .into_iter()
            .collect()
    );
}

#[tokio::test]
async fn select_tuple() {
    let conn = prepare().await;

    let row1 = conn
        .query_row("select (parse_json('[1,2]'), [1,2], true)")
        .await
        .unwrap()
        .unwrap();
    let (val1,): ((String, Vec<u8>, bool),) = row1.try_into().unwrap();
    assert_eq!(val1, ("[1,2]".to_string(), vec![1, 2], true,));

    let row2 = conn
        .query_row("select (to_binary('xyz'), to_timestamp('2024-10-22 10:11:12'))")
        .await
        .unwrap()
        .unwrap();
    let (val2,): ((Vec<u8>, NaiveDateTime),) = row2.try_into().unwrap();
    assert_eq!(
        val2,
        (
            vec![120, 121, 122],
            DateTime::parse_from_rfc3339("2024-10-22T10:11:12Z")
                .unwrap()
                .naive_utc()
        )
    );
}

#[tokio::test]
async fn select_variant() {
    // TODO:
}

#[tokio::test]
async fn select_bitmap() {
    // TODO:
    // let (conn, _) = prepare("select_bitmap_string").await;
    // let mut rows = conn
    //     .query_iter("select build_bitmap([1,2,3,4,5,6]), 11::String")
    //     .await
    //     .unwrap();
    // let mut result = vec![];
    // while let Some(row) = rows.next().await {
    //     let row: (String, String) = row.unwrap().try_into().unwrap();
    //     assert!(row.0.contains('\0'));
    //     result.push(row.1);
    // }
    // assert_eq!(result, vec!["11".to_string()]);
}

#[tokio::test]
async fn select_geometry() {
    // TODO: response type changed to json after
    // https://github.com/datafuselabs/databend/pull/15214
}

#[tokio::test]
async fn select_multiple_columns() {
    let conn = prepare().await;
    let row = conn
        .query_row("select to_uint8(1), to_float64(2.2), '3'")
        .await
        .unwrap();
    assert!(row.is_some());
    let row = row.unwrap();
    let (v1, v2, v3): (u8, f64, String) = row.try_into().unwrap();
    assert_eq!(v1, 1);
    assert_eq!(v2, 2.2);
    assert_eq!(v3, "3");
}

#[tokio::test]
async fn select_multiple_rows() {
    let conn = prepare().await;
    let row = conn.query_row("select * from numbers(3)").await.unwrap();
    assert!(row.is_some());
    let row = row.unwrap();
    let (val,): (u64,) = row.try_into().unwrap();
    assert_eq!(val, 0);
}

#[tokio::test]
async fn select_sleep() {
    let conn = prepare().await;
    let row = conn.query_row("select SLEEP(3);").await.unwrap();
    assert!(row.is_some());
    let row = row.unwrap();
    let (val,): (u8,) = row.try_into().unwrap();
    assert_eq!(val, 0);
}
