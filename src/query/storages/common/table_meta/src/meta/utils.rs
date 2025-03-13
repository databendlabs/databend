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
use std::path::Path;
use std::sync::Arc;

use chrono::DateTime;
use chrono::Datelike;
use chrono::Duration;
use chrono::TimeZone;
use chrono::Timelike;
use chrono::Utc;
use databend_common_base::base::uuid;
use databend_common_base::base::uuid::NoContext;
use databend_common_base::base::uuid::Uuid;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;

use crate::table::table_storage_prefix;
use crate::table::OPT_KEY_DATABASE_ID;
use crate::table::OPT_KEY_STORAGE_PREFIX;
use crate::table::OPT_KEY_TEMP_PREFIX;

pub const TEMP_TABLE_STORAGE_PREFIX: &str = "_tmp_tbl";
use crate::meta::TableSnapshot;
use crate::readers::snapshot_reader::TableSnapshotAccessor;
pub const VACUUM2_OBJECT_KEY_PREFIX: &str = "h";

pub fn trim_timestamp_to_milli_second(ts: DateTime<Utc>) -> DateTime<Utc> {
    Utc.with_ymd_and_hms(
        ts.year(),
        ts.month(),
        ts.day(),
        ts.hour(),
        ts.minute(),
        ts.second(),
    )
    .unwrap()
    .with_nanosecond(ts.timestamp_subsec_millis() * 1_000_000)
    .unwrap()
}

pub fn monotonically_increased_timestamp(
    timestamp: DateTime<Utc>,
    previous_timestamp: &Option<DateTime<Utc>>,
) -> DateTime<Utc> {
    let timestamp = trim_timestamp_to_milli_second(timestamp);

    let Some(prev) = previous_timestamp else {
        return timestamp;
    };

    let prev = trim_timestamp_to_milli_second(*prev);

    if prev >= timestamp {
        prev.add(Duration::milliseconds(1))
    } else {
        timestamp
    }
}

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, Copy)]
pub struct TableMetaTimestamps {
    pub segment_block_timestamp: DateTime<Utc>,
    pub snapshot_timestamp: DateTime<Utc>,
}

impl TableMetaTimestamps {
    pub fn new(previous_snapshot: Option<Arc<TableSnapshot>>, delta: Duration) -> Self {
        let snapshot_timestamp =
            monotonically_increased_timestamp(Utc::now(), &previous_snapshot.timestamp());

        let segment_block_timestamp = snapshot_timestamp + delta;

        Self {
            snapshot_timestamp,
            segment_block_timestamp,
        }
    }
}

/// used in ut
impl Default for TableMetaTimestamps {
    fn default() -> Self {
        // for unit test, set delta to 1 hour
        Self::new(None, Duration::hours(1))
    }
}

pub fn uuid_from_date_time(ts: DateTime<Utc>) -> Uuid {
    // only in this range, the order of timestamps is preserved in UUIDs
    // out of range is unlikely to happen, this is just a safe guard
    let range = 0..=0xFFFF_FFFF_FFFF;
    assert!(range.contains(&ts.timestamp_millis()));
    let seconds = ts.timestamp();
    let nanos = ts.timestamp_subsec_nanos();
    let uuid_ts = uuid::Timestamp::from_unix(NoContext, seconds as u64, nanos);
    Uuid::new_v7(uuid_ts)
}

// Extracts the UUID part from the object key.
// For example, given a path like:
//   bucket/root/115/122/_b/g0191114d30fd78b89fae8e5c88327725_v2.parquet
//   bucket/root/115/122/_b/0191114d30fd78b89fae8e5c88327725_v2.parquet
// The function should return: 0191114d30fd78b89fae8e5c88327725
pub fn try_extract_uuid_str_from_path(path: &str) -> databend_common_exception::Result<&str> {
    if let Some(file_stem) = Path::new(path).file_stem() {
        let file_name = file_stem
            .to_str()
            .unwrap() // path is always valid utf8 string
            .split('_')
            .collect::<Vec<&str>>();
        let uuid = trim_object_prefix(file_name[0]);
        Ok(uuid)
    } else {
        Err(ErrorCode::StorageOther(format!(
            "Illegal object key, no file stem found: {}",
            path
        )))
    }
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

#[inline]
pub fn trim_object_prefix(key: &str) -> &str {
    // if object key (the file_name/stem part only) starts with a char which is larger
    // than 'f', strip it off
    if key > "f" {
        &key[1..]
    } else {
        key
    }
}

#[cfg(test)]
#[allow(clippy::items_after_test_module)]
mod tests {
    use chrono::Duration;
    use databend_common_base::base::uuid::Uuid;

    use crate::meta::trim_object_prefix;
    use crate::meta::try_extract_uuid_str_from_path;
    use crate::meta::VACUUM2_OBJECT_KEY_PREFIX;

    #[test]
    fn test_trim_vacuum2_object_prefix() {
        let uuid = Uuid::now_v7();
        assert_eq!(
            trim_object_prefix(&format!("{}{}", VACUUM2_OBJECT_KEY_PREFIX, uuid)),
            uuid.to_string()
        );
        assert_eq!(trim_object_prefix(&uuid.to_string()), uuid.to_string());
    }

    #[test]
    fn test_try_extract_uuid_str_from_path() {
        let test_cases = vec![
            (
                "bucket/root/115/122/_b/g0191114d30fd78b89fae8e5c88327725_v2.parquet",
                "0191114d30fd78b89fae8e5c88327725",
            ),
            (
                "bucket/root/115/122/_b/0191114d30fd78b89fae8e5c88327725_v2.parquet",
                "0191114d30fd78b89fae8e5c88327725",
            ),
        ];

        for (input, expected) in test_cases {
            assert_eq!(try_extract_uuid_str_from_path(input).unwrap(), expected);
        }
    }
    fn assert_order_preserved(
        ts1: chrono::DateTime<chrono::Utc>,
        ts2: chrono::DateTime<chrono::Utc>,
    ) {
        let uuid1 = super::uuid_from_date_time(ts1);
        let uuid2 = super::uuid_from_date_time(ts2);
        assert_eq!(ts1.cmp(&ts2), uuid1.cmp(&uuid2));
    }

    #[test]
    fn test_uuid_from_date_time() {
        let now = chrono::Utc::now();
        assert_order_preserved(now, now + Duration::milliseconds(1));
        assert_order_preserved(now, now - Duration::milliseconds(1));
        assert_order_preserved(now, chrono::DateTime::default());

        let ms = 0xFFFF_FFFF_FFFF;
        let ts = chrono::DateTime::from_timestamp_millis(ms).unwrap();
        assert_order_preserved(now, ts);
        assert_order_preserved(ts - Duration::milliseconds(1), ts);
    }
}
