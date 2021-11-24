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

use chrono::prelude::*;
use chrono_tz::Tz;
use common_clickhouse_srv::types::column::datetime64::*;

#[test]
fn test_to_datetime() {
    let expected = DateTime::parse_from_rfc3339("2019-01-01T00:00:00-00:00").unwrap();
    let actual = to_datetime(1_546_300_800_000, 3, Tz::UTC);
    assert_eq!(actual, expected)
}

#[test]
fn test_from_datetime() {
    let origin = DateTime::parse_from_rfc3339("2019-01-01T00:00:00-00:00").unwrap();
    let actual = from_datetime(origin, 3);
    assert_eq!(actual, 1_546_300_800_000)
}
