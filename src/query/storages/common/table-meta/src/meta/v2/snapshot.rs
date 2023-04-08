//  Copyright 2022 Datafuse Labs.
//
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.

use std::ops::Add;

use chrono::DateTime;
use chrono::Utc;
use common_base::base::uuid::Uuid;
use common_expression::converts::from_schema;
use common_expression::TableSchema;
use serde::Deserialize;
use serde::Serialize;

use crate::meta::statistics::FormatVersion;
use crate::meta::v1;
use crate::meta::ClusterKey;
use crate::meta::Location;
use crate::meta::SnapshotId;
use crate::meta::Statistics;
use crate::meta::Versioned;

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct TableSnapshot {
    /// format version of snapshot
    format_version: FormatVersion,

    /// id of snapshot
    pub snapshot_id: SnapshotId,

    /// timestamp of this snapshot
    //  for backward compatibility, `Option` is used
    pub timestamp: Option<DateTime<Utc>>,

    /// previous snapshot
    pub prev_snapshot_id: Option<(SnapshotId, FormatVersion)>,

    /// For each snapshot, we keep a schema for it (in case of schema evolution)
    pub schema: TableSchema,

    /// Summary Statistics
    pub summary: Statistics,

    /// Pointers to SegmentInfos (may be of different format)
    ///
    /// We rely on background merge tasks to keep merging segments, so that
    /// this the size of this vector could be kept reasonable
    pub segments: Vec<Location>,

    // The metadata of the cluster keys.
    pub cluster_key_meta: Option<ClusterKey>,
    pub table_statistics_location: Option<String>,
}

impl TableSnapshot {
    pub fn new(
        snapshot_id: SnapshotId,
        prev_timestamp: &Option<DateTime<Utc>>,
        prev_snapshot_id: Option<(SnapshotId, FormatVersion)>,
        schema: TableSchema,
        summary: Statistics,
        segments: Vec<Location>,
        cluster_key_meta: Option<ClusterKey>,
        table_statistics_location: Option<String>,
    ) -> Self {
        let now = Utc::now();
        // make snapshot timestamp monotonically increased
        let adjusted_timestamp = util::monotonically_increased_timestamp(now, prev_timestamp);

        // trim timestamp to micro seconds
        let trimmed_timestamp = util::trim_timestamp_to_micro_second(adjusted_timestamp);
        let timestamp = Some(trimmed_timestamp);

        Self {
            format_version: TableSnapshot::VERSION,
            snapshot_id,
            timestamp,
            prev_snapshot_id,
            schema,
            summary,
            segments,
            cluster_key_meta,
            table_statistics_location,
        }
    }

    pub fn from_previous(previous: &TableSnapshot) -> Self {
        let id = Uuid::new_v4();
        let clone = previous.clone();
        Self::new(
            id,
            &clone.timestamp,
            Some((clone.snapshot_id, clone.format_version)),
            clone.schema,
            clone.summary,
            clone.segments,
            clone.cluster_key_meta,
            clone.table_statistics_location,
        )
    }

    pub fn format_version(&self) -> u64 {
        self.format_version
    }
}

use super::super::v0;

impl From<v0::TableSnapshot> for TableSnapshot {
    fn from(s: v0::TableSnapshot) -> Self {
        let schema = from_schema(&s.schema);
        let schema = TableSchema::init_if_need(schema);
        let leaf_fields = schema.leaf_fields();
        let summary = Statistics::from_v0(s.summary, &leaf_fields);
        Self {
            format_version: TableSnapshot::VERSION,
            snapshot_id: s.snapshot_id,
            timestamp: None,
            prev_snapshot_id: s.prev_snapshot_id.map(|id| (id, 0)),
            schema,
            summary,
            segments: s.segments.into_iter().map(|l| (l, 0)).collect(),
            cluster_key_meta: None,
            table_statistics_location: None,
        }
    }
}

impl From<v1::TableSnapshot> for TableSnapshot {
    fn from(s: v1::TableSnapshot) -> Self {
        let schema = from_schema(&s.schema);
        let schema = TableSchema::init_if_need(schema);
        let leaf_fields = schema.leaf_fields();
        let summary = Statistics::from_v0(s.summary, &leaf_fields);
        Self {
            format_version: TableSnapshot::VERSION,
            snapshot_id: s.snapshot_id,
            timestamp: None,
            prev_snapshot_id: s.prev_snapshot_id,
            schema,
            summary,
            segments: s.segments,
            cluster_key_meta: s.cluster_key_meta,
            table_statistics_location: s.table_statistics_location,
        }
    }
}

// A memory light version of TableSnapshot(Without segments)
// This *ONLY* used for some optimize operation, like PURGE/FUSE_SNAPSHOT function to avoid OOM.
#[derive(Clone, Debug)]
pub struct TableSnapshotLite {
    pub format_version: FormatVersion,
    pub snapshot_id: SnapshotId,
    pub timestamp: Option<DateTime<Utc>>,
    pub prev_snapshot_id: Option<(SnapshotId, FormatVersion)>,
    pub row_count: u64,
    pub block_count: u64,
    pub index_size: u64,
    pub uncompressed_byte_size: u64,
    pub compressed_byte_size: u64,
    pub segment_count: u64,
}

impl From<&TableSnapshot> for TableSnapshotLite {
    fn from(value: &TableSnapshot) -> Self {
        TableSnapshotLite {
            format_version: value.format_version(),
            snapshot_id: value.snapshot_id,
            timestamp: value.timestamp,
            prev_snapshot_id: value.prev_snapshot_id,
            row_count: value.summary.row_count,
            block_count: value.summary.block_count,
            index_size: value.summary.index_size,
            uncompressed_byte_size: value.summary.uncompressed_byte_size,
            segment_count: value.segments.len() as u64,
            compressed_byte_size: value.summary.compressed_byte_size,
        }
    }
}

mod util {
    use chrono::DateTime;
    use chrono::Datelike;
    use chrono::TimeZone;
    use chrono::Timelike;

    use super::*;
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
}
