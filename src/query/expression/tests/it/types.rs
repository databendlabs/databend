// Copyright 2022 Datafuse Labs.
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

use chrono_tz::Tz;
use databend_common_expression::types::timestamp::timestamp_to_string;

#[test]
fn test_timestamp_to_string_formats() {
    // Unix timestamp for "2024-01-01 01:02:03" UTC
    let ts = 1_704_070_923;

    let tz = Tz::UTC;

    // Test with a valid format
    let ts_format = "%Y-%m-%d %H:%M:%S";
    assert_eq!(
        timestamp_to_string(ts, tz, ts_format).to_string(),
        "2024-01-01 01:02:03"
    );

    // Test with a format including fraction of a second
    let ts_format = "%Y-%m-%d %H:%M:%S%.6f";
    assert_eq!(
        timestamp_to_string(ts, tz, ts_format).to_string(),
        "2024-01-01 01:02:03.000000"
    );

    // Test with an invalid format (should use default format)
    // let ts_format = "%Y-%Q-%W"; // Invalid format specifiers
    // assert_eq!(
    // timestamp_to_string(ts, tz, ts_format).to_string(),
    // "2024-01-01 01:02:03.000000" // Default format
    // );
    //
}
