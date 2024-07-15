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

use std::ops::Add;

use chrono::DateTime;
use chrono::Datelike;
use chrono::TimeZone;
use chrono::Timelike;
use chrono::Utc;

pub fn trim_timestamp_to_micro_second(ts: DateTime<Utc>) -> DateTime<Utc> {
    Utc.with_ymd_and_hms(
        ts.year(),
        ts.month(),
        ts.day(),
        ts.hour(),
        ts.minute(),
        ts.second(),
    )
    .unwrap()
    .with_nanosecond(ts.timestamp_subsec_micros() * 1_000)
    .unwrap()
}

pub fn monotonically_increased_timestamp(
    timestamp: DateTime<Utc>,
    previous_timestamp: &Option<DateTime<Utc>>,
) -> DateTime<Utc> {
    if let Some(prev_instant) = previous_timestamp {
        // timestamp of the snapshot should always larger than the previous one's
        if prev_instant > &timestamp {
            // if local time is smaller, use the timestamp of previous snapshot, plus 1 ns
            return prev_instant.add(chrono::Duration::nanoseconds(1));
        }
    }
    timestamp
}
