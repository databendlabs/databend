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

use chrono::TimeZone;
use chrono_tz::Tz;
use common_clickhouse_srv::types::column::*;
use common_clickhouse_srv::types::*;

#[test]
fn test_tuple_type() {
    let tz = Tz::Zulu;
    let inner = vec![
        Vec::column_from::<ArcColumnWrapper>(vec![tz.ymd(2016, 10, 22), tz.ymd(2017, 11, 23)]),
        Vec::column_from::<ArcColumnWrapper>(vec![1_i32, 2]),
        Vec::column_from::<ArcColumnWrapper>(vec!["hello", "data"]),
    ];

    let mut column = Vec::column_from::<ArcColumnWrapper>(inner);
    assert_eq!(
        SqlType::Tuple(vec![&SqlType::Date, &SqlType::Int32, &SqlType::String]),
        column.sql_type()
    );
    assert_eq!(2, column.len());

    let value = Value::Tuple(Arc::new(vec![
        Value::Date(u16::get_days(tz.ymd(2018, 12, 24)), tz),
        Value::Int32(3),
        Value::String(Arc::new(b"bend".to_vec())),
    ]));
    let column = Arc::get_mut(&mut column).unwrap();
    column.push(value);
    assert_eq!(3, column.len());
    assert_eq!(format!("{}", column.at(2)), "(2018-12-24, 3, bend)");
}
