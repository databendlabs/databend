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
use databend_common_base::base::uuid;
use databend_common_base::base::uuid::NoContext;
use databend_common_base::base::uuid::Uuid;
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
            // if local time is smaller, use the timestamp of previous snapshot, plus 1 ms
            return prev_instant.add(chrono::Duration::milliseconds(1));
        }
    }
    timestamp
}

pub fn uuid_from_date_time(ts: DateTime<Utc>) -> Uuid {
    let seconds = ts.timestamp();
    let nanos = ts.timestamp_subsec_nanos();
    let uuid_ts = uuid::Timestamp::from_unix(NoContext, seconds as u64, nanos);
    Uuid::new_v7(uuid_ts)
}
