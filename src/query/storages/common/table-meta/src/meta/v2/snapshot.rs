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

use chrono::DateTime;
use chrono::Utc;
use common_expression::converts::from_schema;
use common_expression::TableSchema;
use serde::Deserialize;
use serde::Serialize;

use crate::meta::monotonically_increased_timestamp;
use crate::meta::trim_timestamp_to_micro_second;
use crate::meta::v0;
use crate::meta::v1;
use crate::meta::ClusterKey;
use crate::meta::FormatVersion;
use crate::meta::Location;
use crate::meta::SnapshotId;
use crate::meta::Statistics;
use crate::meta::Versioned;

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct TableSnapshot {
    /// format version of snapshot
    pub format_version: FormatVersion,

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
        let adjusted_timestamp = monotonically_increased_timestamp(now, prev_timestamp);

        // trim timestamp to micro seconds
        let trimmed_timestamp = trim_timestamp_to_micro_second(adjusted_timestamp);
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
}

impl From<v0::TableSnapshot> for TableSnapshot {
    fn from(s: v0::TableSnapshot) -> Self {
        let schema = from_schema(&s.schema);
        let schema = TableSchema::init_if_need(schema);
        let leaf_fields = schema.leaf_fields();
        let summary = Statistics::from_v0(s.summary, &leaf_fields);
        Self {
            // the is no version before v0, and no versions other then 0 can be converted into v0
            format_version: v0::TableSnapshot::VERSION,
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
            // NOTE: it is important to let the format_version return from here
            // carries the format_version of snapshot being converted.
            format_version: s.format_version,
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
