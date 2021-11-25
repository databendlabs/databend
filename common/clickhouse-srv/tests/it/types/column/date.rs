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

use chrono::TimeZone;
use chrono_tz::Tz;
use common_clickhouse_srv::types::column::*;
use common_clickhouse_srv::types::*;

#[test]
fn test_create_date() {
    let tz = Tz::Zulu;
    let column = Vec::column_from::<ArcColumnWrapper>(vec![tz.ymd(2016, 10, 22)]);
    assert_eq!("2016-10-22UTC", format!("{:#}", column.at(0)));
    assert_eq!(SqlType::Date, column.sql_type());
}

#[test]
fn test_create_date_time() {
    let tz = Tz::Zulu;
    let column = Vec::column_from::<ArcColumnWrapper>(vec![tz.ymd(2016, 10, 22).and_hms(12, 0, 0)]);
    assert_eq!(format!("{}", column.at(0)), "2016-10-22 12:00:00");
}
