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
use common_datavalues::DataSchema;
use serde::Deserialize;
use serde::Serialize;

use crate::storages::fuse::meta::common::ClusterKey;
use crate::storages::fuse::meta::common::FormatVersion;
use crate::storages::fuse::meta::common::Location;
use crate::storages::fuse::meta::common::SnapshotId;
use crate::storages::fuse::meta::common::Statistics;
use crate::storages::fuse::meta::common::Versioned;

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
    pub schema: DataSchema,

    /// Summary Statistics
    pub summary: Statistics,

    /// Pointers to SegmentInfos (may be of different format)
    ///
    /// We rely on background merge tasks to keep merging segments, so that
    /// this the size of this vector could be kept reasonable
    pub segments: Vec<Location>,

    // The metadata of the cluster keys.
    pub cluster_key_meta: Option<ClusterKey>,
}

impl TableSnapshot {
    pub fn new(
        snapshot_id: SnapshotId,
        prev_timestamp: &Option<DateTime<Utc>>,
        prev_snapshot_id: Option<(SnapshotId, FormatVersion)>,
        schema: DataSchema,
        summary: Statistics,
        segments: Vec<Location>,
        cluster_key_meta: Option<ClusterKey>,
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
        }
    }

    pub fn format_version(&self) -> u64 {
        self.format_version
    }
}

use super::super::v0;

impl From<v0::TableSnapshot> for TableSnapshot {
    fn from(s: v0::TableSnapshot) -> Self {
        Self {
            format_version: TableSnapshot::VERSION,
            snapshot_id: s.snapshot_id,
            timestamp: None,
            prev_snapshot_id: s.prev_snapshot_id.map(|id| (id, 0)),
            schema: s.schema,
            summary: s.summary,
            segments: s.segments.into_iter().map(|l| (l, 0)).collect(),
            cluster_key_meta: None,
        }
    }
}

mod util {
    use chrono::Datelike;
    use chrono::TimeZone;
    use chrono::Timelike;

    use super::*;
    pub fn trim_timestamp_to_micro_second(ts: DateTime<Utc>) -> DateTime<Utc> {
        Utc.ymd(ts.year(), ts.month(), ts.day()).and_hms_micro(
            ts.hour(),
            ts.minute(),
            ts.second(),
            ts.timestamp_subsec_micros(),
        )
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
