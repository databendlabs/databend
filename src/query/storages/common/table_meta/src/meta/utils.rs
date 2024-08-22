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

use std::collections::BTreeMap;
use std::ops::Add;

use chrono::DateTime;
use chrono::Datelike;
use chrono::TimeZone;
use chrono::Timelike;
use chrono::Utc;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;

use crate::table::table_storage_prefix;
use crate::table::OPT_KEY_DATABASE_ID;
use crate::table::OPT_KEY_STORAGE_PREFIX;
use crate::table::OPT_KEY_TEMP_PREFIX;

const TEMP_TABLE_STORAGE_PREFIX: &str = "_tmp_tbl";

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

pub fn parse_storage_prefix(options: &BTreeMap<String, String>, table_id: u64) -> Result<String> {
    // if OPT_KE_STORAGE_PREFIX is specified, use it as storage prefix
    if let Some(prefix) = options.get(OPT_KEY_STORAGE_PREFIX) {
        return Ok(prefix.clone());
    }

    // otherwise, use database id and table id as storage prefix

    let db_id = options.get(OPT_KEY_DATABASE_ID).ok_or_else(|| {
        ErrorCode::Internal(format!(
            "Invalid fuse table, table option {} not found",
            OPT_KEY_DATABASE_ID
        ))
    })?;
    let mut prefix = table_storage_prefix(db_id, table_id);
    if let Some(temp_prefix) = options.get(OPT_KEY_TEMP_PREFIX) {
        prefix = format!("{}/{}/{}", TEMP_TABLE_STORAGE_PREFIX, temp_prefix, prefix);
    }
    Ok(prefix)
}
