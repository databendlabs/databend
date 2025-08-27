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
pub struct SnapshotTimestampValidationContext {
    pub table_id: u64,
    pub is_transient: bool,
}

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, Copy)]
pub struct TableMetaTimestamps {
    pub segment_block_timestamp: DateTime<Utc>,
    pub snapshot_timestamp: DateTime<Utc>,
    pub snapshot_timestamp_validation_context: Option<SnapshotTimestampValidationContext>,
}

impl TableMetaTimestamps {
    pub fn new(previous_snapshot: Option<Arc<TableSnapshot>>, delta: Duration) -> Self {
        Self::with_snapshot_timestamp_validation_context(previous_snapshot, delta, None)
    }

    pub fn with_snapshot_timestamp_validation_context(
        previous_snapshot: Option<Arc<TableSnapshot>>,
        delta: Duration,
        snapshot_timestamp_validation_context: Option<SnapshotTimestampValidationContext>,
    ) -> Self {
        let snapshot_timestamp =
            monotonically_increased_timestamp(Utc::now(), &previous_snapshot.timestamp());

        let segment_block_timestamp = snapshot_timestamp + delta;

        Self {
            snapshot_timestamp,
            segment_block_timestamp,
            snapshot_timestamp_validation_context,
        }
    }
}

/// used in ut
#[cfg(test)]
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
            "Invalid fuse table, table option {} not found when parsing storage prefix",
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
    if !key.is_empty() && key.chars().next().unwrap() > 'f' {
        &key[1..]
    } else {
        key
    }
}

#[cfg(test)]
#[allow(clippy::items_after_test_module)]
mod tests {
    use std::io::Cursor;
    use std::io::Error;
    use std::io::ErrorKind;

    use chrono::Duration;
    use databend_common_base::base::uuid::Uuid;
    use databend_common_exception::Result;
    use databend_common_io::prelude::bincode_deserialize_from_slice;
    use databend_common_io::prelude::bincode_serialize_into_buf;

    use crate::meta::trim_object_prefix;
    use crate::meta::try_extract_uuid_str_from_path;
    use crate::meta::AdditionalStatsMeta;
    use crate::meta::Location;
    use crate::meta::RawBlockHLL;
    use crate::meta::TableSnapshotV4;
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
            (
                "bucket/root/115/122/_b/f191114d30fd78b89fae8e5c88327725_v2.parquet",
                "f191114d30fd78b89fae8e5c88327725",
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

    #[test]
    fn test_decode_snapshot() -> Result<()> {
        let data = [
            4, 0, 0, 0, 0, 0, 0, 0, 2, 1, 197, 2, 0, 0, 0, 0, 0, 0, 40, 181, 47, 253, 0, 88, 229,
            21, 0, 230, 233, 147, 72, 240, 144, 184, 13, 204, 131, 11, 131, 51, 218, 138, 145, 68,
            106, 164, 89, 117, 78, 234, 63, 184, 126, 97, 248, 95, 245, 97, 186, 123, 211, 51, 118,
            61, 149, 125, 8, 255, 94, 55, 123, 101, 31, 136, 161, 78, 145, 123, 224, 25, 185, 229,
            169, 61, 131, 219, 176, 66, 191, 145, 72, 187, 22, 93, 124, 234, 77, 97, 84, 22, 136,
            132, 4, 135, 0, 123, 0, 125, 0, 247, 138, 36, 45, 212, 135, 167, 46, 103, 159, 213,
            245, 218, 161, 233, 101, 191, 226, 4, 191, 162, 152, 38, 30, 207, 188, 146, 146, 226,
            245, 190, 5, 89, 27, 38, 126, 18, 229, 172, 143, 7, 50, 104, 170, 7, 149, 14, 161, 163,
            244, 129, 196, 42, 21, 51, 98, 225, 185, 252, 251, 205, 46, 75, 191, 67, 24, 10, 31,
            16, 23, 226, 105, 35, 26, 104, 36, 180, 69, 8, 113, 32, 158, 6, 2, 226, 52, 174, 69,
            136, 103, 231, 143, 109, 119, 145, 114, 204, 168, 145, 2, 7, 141, 23, 59, 48, 100, 128,
            193, 218, 49, 34, 19, 16, 163, 90, 217, 53, 137, 135, 51, 115, 151, 143, 172, 45, 223,
            95, 146, 125, 226, 173, 65, 152, 14, 1, 37, 153, 173, 59, 253, 146, 138, 250, 125, 127,
            41, 91, 153, 215, 220, 163, 44, 177, 214, 247, 139, 9, 208, 169, 49, 125, 205, 182,
            229, 0, 128, 26, 22, 21, 254, 196, 114, 249, 253, 244, 201, 250, 156, 255, 14, 177,
            166, 89, 164, 193, 100, 167, 15, 238, 15, 101, 56, 38, 84, 223, 207, 165, 211, 179,
            168, 169, 188, 160, 183, 11, 196, 105, 79, 140, 69, 206, 178, 145, 221, 244, 25, 70,
            195, 236, 179, 206, 254, 184, 149, 141, 178, 221, 189, 132, 245, 213, 239, 98, 136, 53,
            125, 193, 170, 149, 25, 139, 87, 83, 23, 247, 233, 185, 193, 137, 253, 31, 22, 73, 88,
            168, 191, 223, 12, 252, 169, 173, 217, 178, 140, 40, 227, 243, 61, 195, 18, 214, 136,
            26, 51, 148, 113, 195, 99, 196, 178, 154, 113, 2, 214, 99, 244, 204, 140, 8, 18, 96,
            55, 238, 37, 173, 176, 18, 169, 172, 109, 159, 84, 223, 22, 246, 39, 150, 175, 112,
            152, 253, 177, 156, 89, 219, 110, 107, 157, 51, 175, 253, 157, 139, 151, 190, 116, 10,
            116, 50, 20, 198, 225, 47, 222, 89, 146, 160, 150, 157, 102, 219, 185, 173, 43, 20,
            156, 182, 198, 143, 101, 182, 48, 216, 189, 98, 110, 161, 166, 137, 87, 83, 4, 42, 91,
            188, 74, 201, 80, 120, 159, 157, 203, 171, 169, 235, 245, 39, 150, 223, 41, 246, 109,
            3, 47, 201, 169, 237, 220, 246, 217, 93, 188, 78, 111, 18, 35, 250, 190, 218, 121, 159,
            173, 227, 66, 223, 127, 218, 244, 186, 26, 18, 121, 148, 106, 19, 36, 34, 242, 241, 60,
            17, 229, 54, 77, 235, 232, 247, 205, 13, 12, 85, 207, 3, 69, 31, 38, 33, 14, 233, 74,
            178, 3, 81, 80, 228, 125, 168, 123, 13, 124, 207, 19, 105, 93, 13, 213, 80, 221, 42,
            221, 64, 79, 180, 105, 36, 207, 136, 75, 15, 136, 243, 108, 155, 141, 148, 138, 247,
            252, 120, 244, 188, 5, 65, 66, 253, 95, 128, 109, 56, 155, 140, 147, 169, 140, 157, 50,
            248, 24, 80, 25, 177, 44, 219, 168, 70, 214, 232, 128, 192, 0, 213, 24, 42, 32, 96,
            194, 67, 103, 30, 61, 68, 144, 251, 36, 150, 161, 205, 214, 223, 213, 239, 8, 33, 126,
            76, 116, 131, 66, 49, 55, 102, 224, 24, 62, 251, 222, 76, 104, 185, 129, 238, 225, 158,
            92, 103, 200, 231, 177, 43, 131, 29, 123, 136, 113, 3, 136, 156, 13, 185, 121, 183,
            112, 9, 75, 121, 242, 220, 176, 16, 220, 192, 25, 148, 67, 228, 200, 89, 162, 67, 225,
            152, 209, 129, 198, 62, 12, 30, 216, 10, 64, 23, 14, 54, 203, 93, 85, 57, 212, 255, 24,
            45, 87, 178, 13, 198, 185, 158, 25,
        ];

        let val = TableSnapshotV4::from_slice(&data)?;
        println!("val: {:?}", val);
        let mut buffer = Cursor::new(Vec::new());
        bincode_serialize_into_buf(&mut buffer, &val).unwrap();
        let slice = buffer.get_ref().as_slice();
        let deserialized: TableSnapshotV4 = bincode_deserialize_from_slice(slice).unwrap();
        assert_eq!(val.summary, deserialized.summary);
        Ok(())
    }

    #[test]
    fn test_additional_stats_meta_compatibility() -> Result<()> {
        #[derive(serde::Serialize, serde::Deserialize, Clone, Debug, PartialEq, Eq, Default)]
        struct AdditionalStatsMeta803 {
            /// The size of the stats data in bytes.
            size: u64,
            /// The file location of the stats data.
            location: Location,
            /// An optional HyperLogLog data structure.
            hll: Option<RawBlockHLL>,
            /// The count of the stats rows.
            #[serde(default)]
            row_count: u64,
        }

        // 790 and 797 have the same definition as 801
        #[derive(serde::Serialize, serde::Deserialize, Clone, Debug, PartialEq, Eq, Default)]
        struct AdditionalStatsMeta801 {
            /// The size of the stats data in bytes.
            size: u64,
            /// The file location of the stats data.
            location: Location,
        }

        // Simulate using current PR read data of v803
        {
            let v803 = AdditionalStatsMeta803::default();
            let bytes = rmp_serde::to_vec_named(&v803)
                .map_err(|e| Error::new(ErrorKind::InvalidData, e))?;
            let current: std::result::Result<AdditionalStatsMeta, Error> =
                rmp_serde::from_slice(&bytes).map_err(|e| Error::new(ErrorKind::InvalidData, e));
            // v803 is compatible with current PR
            assert!(current.is_ok());
            eprintln!("current {:#?}", current);
        }

        // Simulate using v803 read data created by current PR when the location is none
        {
            let current = AdditionalStatsMeta {
                hll: Some(RawBlockHLL::default()),
                ..Default::default()
            };
            let bytes = rmp_serde::to_vec_named(&current)
                .map_err(|e| Error::new(ErrorKind::InvalidData, e))?;
            let v803: std::result::Result<AdditionalStatsMeta803, Error> =
                rmp_serde::from_slice(&bytes).map_err(|e| Error::new(ErrorKind::InvalidData, e));
            // v803 is not compatible with current PR when the location is none.
            assert!(v803.is_err());
        }

        // Simulate using v803 read data created by current PR when the location is some
        {
            let current = AdditionalStatsMeta {
                hll: Some(RawBlockHLL::default()),
                location: Some(Location::default()),
                ..Default::default()
            };
            let bytes = rmp_serde::to_vec_named(&current)
                .map_err(|e| Error::new(ErrorKind::InvalidData, e))?;
            let v803: std::result::Result<AdditionalStatsMeta803, Error> =
                rmp_serde::from_slice(&bytes).map_err(|e| Error::new(ErrorKind::InvalidData, e));
            // v803 is compatible with current PR when the location is some.
            assert!(v803.is_ok());
            eprintln!("v803 {:#?}", v803);
        }

        // v790, 797, 801 have been deployed, we MUST support them
        // Simulate using current PR read data of v790, 797, 801
        {
            let v801 = AdditionalStatsMeta801::default();
            let bytes = rmp_serde::to_vec_named(&v801)
                .map_err(|e| Error::new(ErrorKind::InvalidData, e))?;
            let current: std::result::Result<AdditionalStatsMeta, Error> =
                rmp_serde::from_slice(&bytes).map_err(|e| Error::new(ErrorKind::InvalidData, e));
            assert!(current.is_ok());
            eprintln!("current {:#?}", current);
        }

        // Simulate using v790, 797, 801 read data created by current PR
        {
            let current = AdditionalStatsMeta {
                hll: Some(RawBlockHLL::default()),
                ..Default::default()
            };
            let bytes = rmp_serde::to_vec_named(&current)
                .map_err(|e| Error::new(ErrorKind::InvalidData, e))?;
            let current: std::result::Result<AdditionalStatsMeta801, Error> =
                rmp_serde::from_slice(&bytes).map_err(|e| Error::new(ErrorKind::InvalidData, e));
            // current is not compatible with v801 PR when the location is none.
            assert!(current.is_err());
        }

        Ok(())
    }
}
