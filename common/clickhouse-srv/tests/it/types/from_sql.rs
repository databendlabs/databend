// Copyright 2020 Datafuse Labs.
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

use chrono::prelude::*;
use chrono_tz::Tz;
use common_clickhouse_srv::types::*;

#[test]
fn test_u8() {
    let v = ValueRef::from(42_u8);
    let actual = u8::from_sql(v).unwrap();
    assert_eq!(actual, 42_u8);
}

#[test]
fn test_bad_convert() {
    let v = ValueRef::from(42_u16);
    match u32::from_sql(v) {
        Ok(_) => panic!("should fail"),
        Err(e) => assert_eq!(
            "From SQL error: `SqlType::UInt16 cannot be cast to u32.`".to_string(),
            format!("{}", e)
        ),
    }
}

#[test]
fn null_to_datetime() {
    let null_value = ValueRef::Nullable(Either::Left(
        SqlType::DateTime(DateTimeType::DateTime32).into(),
    ));
    let date = Option::<DateTime<Tz>>::from_sql(null_value);
    assert_eq!(date.unwrap(), None);
}
