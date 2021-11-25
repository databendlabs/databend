// Copyright 2021 Datafuse Labs.
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

use std::sync::Arc;

use chrono::prelude::*;
use chrono_tz::Tz::UTC;
use chrono_tz::Tz::{self};
use common_clickhouse_srv::row;
use common_clickhouse_srv::types::*;

#[test]
fn test_push_row() {
    let date_value: Date<Tz> = UTC.ymd(2016, 10, 22);
    let date_time_value: DateTime<Tz> = UTC.ymd(2014, 7, 8).and_hms(14, 0, 0);

    let decimal = Decimal::of(2.0_f64, 4);

    let tuple = Value::Tuple(Arc::new(vec![
        Value::Int32(1),
        Value::String(Arc::new(b"text".to_vec())),
        Value::Float64(2.3),
    ]));

    let mut block = Block::<Simple>::new();
    block
        .push(row! {
            i8_field: 1_i8,
            i16_field: 1_i16,
            i32_field: 1_i32,
            i64_field: 1_i64,

            u8_field: 1_u8,
            u16_field: 1_u16,
            u32_field: 1_u32,
            u64_field: 1_u64,

            f32_field: 4.66_f32,
            f64_field: 2.71_f64,

            str_field: "text",
            opt_filed: Some("text"),
            nil_filed: Option::<&str>::None,

            date_field: date_value,
            date_time_field: date_time_value,

            decimal_field: decimal,

            tuple_field: tuple,
        })
        .unwrap();

    assert_eq!(block.row_count(), 1);

    assert_eq!(block.columns()[0].sql_type(), SqlType::Int8);
    assert_eq!(block.columns()[1].sql_type(), SqlType::Int16);
    assert_eq!(block.columns()[2].sql_type(), SqlType::Int32);
    assert_eq!(block.columns()[3].sql_type(), SqlType::Int64);

    assert_eq!(block.columns()[4].sql_type(), SqlType::UInt8);
    assert_eq!(block.columns()[5].sql_type(), SqlType::UInt16);
    assert_eq!(block.columns()[6].sql_type(), SqlType::UInt32);
    assert_eq!(block.columns()[7].sql_type(), SqlType::UInt64);

    assert_eq!(block.columns()[8].sql_type(), SqlType::Float32);
    assert_eq!(block.columns()[9].sql_type(), SqlType::Float64);

    assert_eq!(block.columns()[10].sql_type(), SqlType::String);
    assert_eq!(
        block.columns()[11].sql_type(),
        SqlType::Nullable(SqlType::String.into())
    );
    assert_eq!(
        block.columns()[12].sql_type(),
        SqlType::Nullable(SqlType::String.into())
    );

    assert_eq!(block.columns()[13].sql_type(), SqlType::Date);
    assert_eq!(
        block.columns()[14].sql_type(),
        SqlType::DateTime(DateTimeType::DateTime32)
    );
    assert_eq!(block.columns()[15].sql_type(), SqlType::Decimal(18, 4));

    assert_eq!(
        block.columns()[16].sql_type(),
        SqlType::Tuple(vec![
            SqlType::Int32.into(),
            SqlType::String.into(),
            SqlType::Float64.into()
        ])
    );
}
