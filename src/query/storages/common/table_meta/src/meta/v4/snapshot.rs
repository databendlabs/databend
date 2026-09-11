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

use std::collections::HashSet;
use std::io::Cursor;
use std::io::Read;
use std::sync::Arc;

use chrono::DateTime;
use chrono::TimeDelta;
use chrono::Utc;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::TableSchema;
use databend_common_frozen_api::FrozenAPI;
use databend_common_frozen_api::frozen_api;
use databend_common_io::prelude::BinaryRead;
use serde::Deserialize;
use serde::Serialize;

use crate::meta::ClusterKey;
use crate::meta::ClusterKeyInfo;
use crate::meta::FormatVersion;
use crate::meta::Location;
use crate::meta::MetaEncoding;
use crate::meta::SnapshotId;
use crate::meta::Statistics;
use crate::meta::TableMetaTimestamps;
use crate::meta::Versioned;
use crate::meta::format::MetaCompression;
use crate::meta::format::compress;
use crate::meta::format::encode;
use crate::meta::format::read_and_deserialize;
use crate::meta::monotonically_increased_timestamp;
use crate::meta::uuid_from_date_time;
use crate::meta::v2;
use crate::meta::v3;
use crate::readers::snapshot_reader::TableSnapshotAccessor;
use crate::table::ClusterType;

#[frozen_api("9de02316")]
#[derive(Serialize, Deserialize, Clone, Debug, FrozenAPI)]
pub struct TableSnapshot {
    /// format version of TableSnapshot metadata
    ///
    /// Note that:
    ///
    /// - A instance of v3::TableSnapshot may have a value of v2/v1::TableSnapshot::VERSION for this field.
    ///
    ///   That indicates this instance is converted from a v2/v1::TableSnapshot.
    ///
    /// - The meta writers are responsible for only writing down the latest version of TableSnapshot, and
    ///   the format_version being written is of the latest version.
    ///
    ///   e.g. if the current version of TableSnapshot is v3::TableSnapshot, then the format_version
    ///   that will be written down to object storage as part of TableSnapshot table meta data,
    ///   should always be v3::TableSnapshot::VERSION (which is 3)
    pub format_version: FormatVersion,

    /// id of snapshot
    pub snapshot_id: SnapshotId,

    /// timestamp of this snapshot
    //  for backward compatibility, `Option` is used
    pub timestamp: Option<DateTime<Utc>>,

    // The table seq before snapshot commit.
    pub prev_table_seq: Option<u64>,

    /// previous snapshot
    pub prev_snapshot_id: Option<(SnapshotId, FormatVersion)>,

    /// For each snapshot, we keep a schema for it (in case of schema evolution)
    pub schema: TableSchema,

    /// Summary Statistics
    pub summary: Statistics,

    /// Pointers to SegmentInfos (maybe of different format)
    ///
    /// We rely on background merge tasks to keep merging segments, so that
    /// this the size of this vector could be kept reasonable
    pub segments: Vec<Location>,

    /// The metadata of the cluster keys.
    pub cluster_key_meta: Option<ClusterKey>,
    #[serde(default)]
    pub cluster_type: Option<ClusterType>,
    // TODO(zhyass): move table_statistics_location to additional_stats_meta.location.
    pub table_statistics_location: Option<String>,

    /// Cumulative logical UPDATE and DELETE counters. `None` means this snapshot
    /// was written by a version that did not track logical changes.
    // Also cleared by transaction retry when the complete delta is unknown.
    #[serde(default)]
    logical_change_counters: Option<LogicalChangeCounters>,
}

/// Cumulative UPDATE/DELETE counters. Only endpoints with the same known epoch
/// are comparable. Legacy writes and overflow break continuity.
#[derive(Serialize, Deserialize, Clone, Copy, Debug, FrozenAPI)]
pub struct LogicalChangeCounters {
    updated_rows_total: u64,
    deleted_rows_total: u64,
    /// Table seq used when starting this history, before committing its first
    /// snapshot. `None` means unknown: older snapshots or a reset without a fresh seq.
    #[serde(default)]
    epoch: Option<u64>,
}

impl LogicalChangeCounters {
    /// UPDATE and DELETE rows committed between `base` and `self`.
    ///
    /// `Ok(None)` means the delta is unknowable and the caller must fall back
    /// to endpoint/origin-based processing: either endpoint's identity is
    /// unknown, or the two belong to different counting histories. Subtracting
    /// across a restart would report the changes the restart hid as zero.
    pub fn delta_from(&self, base: &Self) -> Result<Option<(u64, u64)>> {
        let (Some(self_epoch), Some(base_epoch)) = (self.epoch, base.epoch) else {
            return Ok(None);
        };
        if self_epoch != base_epoch {
            return Ok(None);
        }
        // Monotonic within one epoch, so a decrease here is a broken invariant
        // rather than a discontinuous history.
        let updated = self
            .updated_rows_total
            .checked_sub(base.updated_rows_total)
            .ok_or_else(|| ErrorCode::Internal("logical updated row counter decreased"))?;
        let deleted = self
            .deleted_rows_total
            .checked_sub(base.deleted_rows_total)
            .ok_or_else(|| ErrorCode::Internal("logical deleted row counter decreased"))?;
        Ok(Some((updated, deleted)))
    }
}

impl TableSnapshot {
    /// Note that table_meta_timestamps is not always equal to prev_timestamp.
    pub fn try_new(
        prev_table_seq: Option<u64>,
        prev_snapshot: Option<Arc<TableSnapshot>>,
        schema: TableSchema,
        mut summary: Statistics,
        segments: Vec<Location>,
        cluster_key_info: Option<ClusterKeyInfo>,
        table_statistics_location: Option<String>,
        table_meta_timestamps: TableMetaTimestamps,
    ) -> Result<Self> {
        let TableMetaTimestamps {
            segment_block_timestamp,
            snapshot_timestamp,
            snapshot_timestamp_validation_context,
        } = table_meta_timestamps;

        let snapshot_timestamp_adjusted =
            monotonically_increased_timestamp(snapshot_timestamp, &prev_snapshot.timestamp());

        if segment_block_timestamp < snapshot_timestamp_adjusted {
            let mut err_msg = format!(
                "Unresolvable conflict: Transaction conflicts with commit at {:?}. Can only merge with commits before {:?}.",
                snapshot_timestamp_adjusted, segment_block_timestamp
            );

            if let Some(ctx) = snapshot_timestamp_validation_context {
                if ctx.is_transient {
                    err_msg.push_str(
                        &format!(" Transient table (ID: {}) detected. Concurrent mutations same transient table likely cause conflicts. Consider using regular tables.", ctx.table_id)
                    );
                } else {
                    let delta = snapshot_timestamp - segment_block_timestamp;
                    if delta < TimeDelta::hours(1) {
                        // TODO give user a doc url, which describes this situation more clearly, such as increasing the value of setting 'max_execute_time_in_seconds' also work, and what the tradeoffs are.
                        err_msg.push_str(&format!(
                            " Conflict window too narrow ({:?}). Consider increasing the value of setting 'data_retention_time_in_days'.",
                            delta
                        ));
                    }
                }
            }

            return Err(ErrorCode::TransactionTimeout(err_msg));
        }

        ensure_segments_unique(&segments)?;

        let (cluster_key_meta, cluster_type) = cluster_key_info
            .map(|metadata| (Some(metadata.cluster_key), Some(metadata.cluster_type)))
            .unwrap_or_default();
        let cluster_key_id = cluster_key_meta.as_ref().map(|(id, _)| *id);
        if summary
            .cluster_stats
            .as_ref()
            .is_some_and(|stats| Some(stats.cluster_key_id) != cluster_key_id)
        {
            summary.cluster_stats = None;
        }
        // Preserve existing totals even without an epoch: older readers still
        // subtract them. Start a new identity, not new totals, at this boundary.
        // Only an absent counter field starts from zero. A missing seq stays unknown.
        let mut counters = prev_snapshot
            .as_ref()
            .and_then(|snapshot| snapshot.logical_change_counters)
            .unwrap_or(LogicalChangeCounters {
                updated_rows_total: 0,
                deleted_rows_total: 0,
                epoch: None,
            });
        counters.epoch = counters.epoch.or(prev_table_seq);
        Ok(Self {
            format_version: TableSnapshot::VERSION,
            snapshot_id: uuid_from_date_time(snapshot_timestamp_adjusted),
            timestamp: Some(snapshot_timestamp_adjusted),
            prev_table_seq,
            prev_snapshot_id: prev_snapshot.snapshot_id(),
            schema,
            summary,
            segments,
            cluster_key_meta,
            cluster_type,
            table_statistics_location,
            logical_change_counters: Some(counters),
        })
    }

    pub fn try_from_previous(
        previous: Arc<TableSnapshot>,
        cluster_key_info: Option<ClusterKeyInfo>,
        prev_table_seq: Option<u64>,
        table_meta_timestamps: TableMetaTimestamps,
    ) -> Result<Self> {
        // the timestamp of the new snapshot will be adjusted by the `new` method
        Self::try_new(
            prev_table_seq,
            Some(previous.clone()),
            previous.schema.clone(),
            previous.summary.clone(),
            previous.segments.clone(),
            cluster_key_info,
            previous.table_statistics_location.clone(),
            table_meta_timestamps,
        )
    }

    /// Serializes the struct to a byte vector.
    ///
    /// The byte vector contains the format version, encoding, compression, and compressed data. The encoding
    /// and compression are set to default values. The data is encoded and compressed.
    ///
    /// # Returns
    ///
    /// A Result containing the serialized data as a byte vector. If any errors occur during
    /// encoding, compression, or writing to the byte vector, an error will be returned.
    pub fn to_bytes(&self) -> Result<Vec<u8>> {
        let encoding = MetaEncoding::MessagePack;
        let compression = MetaCompression::default();

        let data = encode(&encoding, &self)?;
        let data_compress = compress(&compression, data)?;

        let data_size = self.format_version.to_le_bytes().len()
            + 2
            + data_compress.len().to_le_bytes().len()
            + data_compress.len();
        let mut buf = Vec::with_capacity(data_size);

        buf.extend_from_slice(&self.format_version.to_le_bytes());
        buf.push(encoding as u8);
        buf.push(compression as u8);
        buf.extend_from_slice(&data_compress.len().to_le_bytes());

        buf.extend(data_compress);

        Ok(buf)
    }

    /// Reads a snapshot from Vec<u8> and returns a `TableSnapshot` object.
    ///
    /// This function reads the following fields from the stream and constructs a `TableSnapshot` object:
    ///
    /// * `version` (u64): The version number of the snapshot.
    /// * `encoding` (u8): The encoding format used to serialize the snapshot's data.
    /// * `compression` (u8): The compression format used to compress the snapshot's data.
    /// * `snapshot_size` (u64): The size (in bytes) of the compressed snapshot data.
    ///
    /// The function then reads the compressed snapshot data from the stream, decompresses it using
    /// the specified compression format, and deserializes it using the specified encoding format.
    /// Finally, it constructs a `TableSnapshot` object using the deserialized data and returns it.
    pub fn from_slice(buffer: &[u8]) -> Result<TableSnapshot> {
        Self::from_read(Cursor::new(buffer))
    }

    pub fn from_read(mut r: impl Read) -> Result<TableSnapshot> {
        let version = r.read_scalar::<u64>()?;
        assert_eq!(version, TableSnapshot::VERSION);
        let encoding = MetaEncoding::try_from(r.read_scalar::<u8>()?)?;
        let compression = MetaCompression::try_from(r.read_scalar::<u8>()?)?;
        let snapshot_size: u64 = r.read_scalar::<u64>()?;

        let snapshot: TableSnapshot =
            read_and_deserialize(&mut r, snapshot_size, &encoding, &compression)
                .map_err(|x| x.add_message("fail to deserialize table snapshot"))?;
        snapshot.ensure_segments_unique()?;
        Ok(snapshot)
    }

    #[inline]
    pub fn encoding() -> MetaEncoding {
        MetaEncoding::MessagePack
    }

    #[inline]
    pub fn table_statistics_location(&self) -> Option<String> {
        self.table_statistics_location.clone()
    }

    #[inline]
    pub fn ensure_segments_unique(&self) -> Result<()> {
        ensure_segments_unique(&self.segments)
    }

    /// Cumulative logical change counters, or `None` when absent or invalidated.
    ///
    /// The returned totals are only meaningful relative to another snapshot from
    /// the same counting history; compare them via
    /// [`LogicalChangeCounters::delta_from`] rather than reading them directly.
    pub fn logical_change_counters(&self) -> Option<LogicalChangeCounters> {
        self.logical_change_counters
    }

    /// Add increments without resurrecting absent counters. On overflow, restart
    /// both totals and retain this operation's increments. A reused/missing seq
    /// cannot identify a fresh history (e.g. within a transaction), so use None.
    pub fn add_logical_change_delta(&mut self, updated_rows: u64, deleted_rows: u64) {
        let Some(counters) = self.logical_change_counters.as_mut() else {
            return;
        };
        match (
            counters.updated_rows_total.checked_add(updated_rows),
            counters.deleted_rows_total.checked_add(deleted_rows),
        ) {
            (Some(updated), Some(deleted)) => {
                counters.updated_rows_total = updated;
                counters.deleted_rows_total = deleted;
            }
            _ => {
                let epoch = self.prev_table_seq.filter(|seq| {
                    counters
                        .epoch
                        .is_some_and(|previous_epoch| *seq > previous_epoch)
                });
                *counters = LogicalChangeCounters {
                    updated_rows_total: updated_rows,
                    deleted_rows_total: deleted_rows,
                    epoch,
                };
            }
        }
    }

    /// Used when a transaction retry cannot recover its complete logical delta.
    /// Do not inherit the latest snapshot's counters and omit the transaction.
    pub fn invalidate_logical_change_counters(&mut self) {
        self.logical_change_counters = None;
    }
}

fn ensure_segments_unique(segments: &[Location]) -> Result<()> {
    if segments.len() < 2 {
        return Ok(());
    }

    let mut seen = HashSet::with_capacity(segments.len());
    for loc in segments {
        let key = loc.0.as_str();
        if !seen.insert(key) {
            log::warn!(
                "duplicate segment location {} detected while constructing snapshot",
                key
            );
        }
    }
    Ok(())
}

// use the chain of converters, for versions before v3
impl From<v2::TableSnapshot> for TableSnapshot {
    fn from(s: v2::TableSnapshot) -> Self {
        ensure_segments_unique(&s.segments)
            .expect("duplicate segment location found while converting snapshot from v2");
        Self {
            // NOTE: it is important to let the format_version return from here
            // carries the format_version of snapshot being converted.
            format_version: s.format_version,
            snapshot_id: s.snapshot_id,
            timestamp: s.timestamp,
            prev_table_seq: None,
            prev_snapshot_id: s.prev_snapshot_id,
            schema: s.schema,
            summary: s.summary,
            segments: s.segments,
            cluster_key_meta: s.cluster_key_meta,
            cluster_type: None,
            table_statistics_location: s.table_statistics_location,
            logical_change_counters: None,
        }
    }
}

impl<T> From<T> for TableSnapshot
where T: Into<v3::TableSnapshot>
{
    fn from(s: T) -> Self {
        let s: v3::TableSnapshot = s.into();
        ensure_segments_unique(&s.segments)
            .expect("duplicate segment location found while converting snapshot from v3");
        Self {
            // NOTE: it is important to let the format_version return from here
            // carries the format_version of snapshot being converted.
            format_version: s.format_version,
            snapshot_id: s.snapshot_id,
            timestamp: s.timestamp,
            prev_table_seq: None,
            prev_snapshot_id: s.prev_snapshot_id,
            schema: s.schema.into(),
            summary: s.summary.into(),
            segments: s.segments,
            cluster_key_meta: s.cluster_key_meta,
            cluster_type: None,
            table_statistics_location: s.table_statistics_location,
            logical_change_counters: None,
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
    pub bloom_index_size: Option<u64>,
    pub ngram_index_size: Option<u64>,
    pub inverted_index_size: Option<u64>,
    pub vector_index_size: Option<u64>,
    pub virtual_column_size: Option<u64>,
    pub uncompressed_byte_size: u64,
    pub compressed_byte_size: u64,
    pub segment_count: u64,
}

impl From<(&TableSnapshot, FormatVersion)> for TableSnapshotLite {
    fn from((value, ver): (&TableSnapshot, FormatVersion)) -> Self {
        TableSnapshotLite {
            format_version: ver,
            snapshot_id: value.snapshot_id,
            timestamp: value.timestamp,
            prev_snapshot_id: value.prev_snapshot_id,
            row_count: value.summary.row_count,
            block_count: value.summary.block_count,
            index_size: value.summary.index_size,
            bloom_index_size: value.summary.bloom_index_size,
            ngram_index_size: value.summary.ngram_index_size,
            inverted_index_size: value.summary.inverted_index_size,
            vector_index_size: value.summary.vector_index_size,
            virtual_column_size: value.summary.virtual_column_size,
            uncompressed_byte_size: value.summary.uncompressed_byte_size,
            segment_count: value.segments.len() as u64,
            compressed_byte_size: value.summary.compressed_byte_size,
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use databend_common_expression::TableSchema;

    use super::*;
    use crate::meta::ClusterStatistics;

    #[test]
    fn test_try_from_previous_uses_target_cluster_key_info() {
        let cluster_key_info = Some(ClusterKeyInfo::new(
            (1, "(a, b)".to_string()),
            ClusterType::Linear,
        ));
        let cluster_stats = ClusterStatistics::new(1, vec![], vec![], 0);
        let mut previous = TableSnapshot::try_new(
            None,
            None,
            TableSchema::empty(),
            Statistics::default(),
            vec![],
            cluster_key_info.clone(),
            None,
            TableMetaTimestamps::default(),
        )
        .unwrap();
        previous.summary.cluster_stats = Some(cluster_stats.clone());

        let snapshot = TableSnapshot::try_from_previous(
            Arc::new(previous),
            cluster_key_info.clone(),
            None,
            TableMetaTimestamps::default(),
        )
        .unwrap();

        assert_eq!(
            snapshot.cluster_key_meta,
            cluster_key_info.map(|v| v.cluster_key)
        );
        assert_eq!(snapshot.cluster_type, Some(ClusterType::Linear));
        assert_eq!(snapshot.summary.cluster_stats, Some(cluster_stats));
    }

    #[test]
    fn test_try_from_previous_clears_cluster_key_info_without_target_key() {
        let previous = TableSnapshot::try_new(
            None,
            None,
            TableSchema::empty(),
            Statistics::default(),
            vec![],
            Some(ClusterKeyInfo::new(
                (1, "(a, b)".to_string()),
                ClusterType::Hilbert,
            )),
            None,
            TableMetaTimestamps::default(),
        )
        .unwrap();

        let snapshot = TableSnapshot::try_from_previous(
            Arc::new(previous),
            None,
            None,
            TableMetaTimestamps::default(),
        )
        .unwrap();

        assert_eq!(snapshot.cluster_key_meta, None);
        assert_eq!(snapshot.cluster_type, None);
        assert_eq!(snapshot.summary.cluster_stats, None);
    }

    fn snapshot(previous: Option<Arc<TableSnapshot>>) -> TableSnapshot {
        snapshot_at(None, previous)
    }

    fn snapshot_at(
        prev_table_seq: Option<u64>,
        previous: Option<Arc<TableSnapshot>>,
    ) -> TableSnapshot {
        TableSnapshot::try_new(
            prev_table_seq,
            previous,
            TableSchema::default(),
            Statistics::default(),
            vec![],
            None,
            None,
            TableMetaTimestamps::new(None, TimeDelta::hours(1)),
        )
        .unwrap()
    }

    fn strip_counters(snapshot: &TableSnapshot) -> TableSnapshot {
        let mut value = serde_json::to_value(snapshot.clone()).unwrap();
        value
            .as_object_mut()
            .unwrap()
            .remove("logical_change_counters");
        serde_json::from_value(value).unwrap()
    }

    #[test]
    fn test_epochless_writer_handoff_preserves_cumulative_totals() {
        // Model the older counter schema: unknown fields are discarded on read
        // and cannot be serialized back by an older writer.
        #[derive(Serialize, Deserialize)]
        struct EpochlessCounters {
            updated_rows_total: u64,
            deleted_rows_total: u64,
        }

        fn old_writer(snapshot: &TableSnapshot, updated: u64, deleted: u64) -> TableSnapshot {
            let mut value = serde_json::to_value(snapshot).unwrap();
            let mut counters: EpochlessCounters =
                serde_json::from_value(value["logical_change_counters"].clone()).unwrap();
            counters.updated_rows_total += updated;
            counters.deleted_rows_total += deleted;
            value["logical_change_counters"] = serde_json::to_value(counters).unwrap();
            serde_json::from_value(value).unwrap()
        }

        for (base_updated, base_deleted) in [(0, 0), (5, 3)] {
            let mut initial = snapshot_at(Some(10), None);
            initial.add_logical_change_delta(base_updated, base_deleted);
            let base = old_writer(&initial, 0, 0);
            let base_counters = base.logical_change_counters().unwrap();
            assert_eq!(base_counters.epoch, None);
            assert_eq!(base_counters.delta_from(&base_counters).unwrap(), None);
            let old_latest = old_writer(&base, 1, 1);

            // Missing seq must neither fabricate an epoch nor discard totals.
            let unidentified = snapshot_at(None, Some(Arc::new(old_latest.clone())));
            let counters = unidentified.logical_change_counters().unwrap();
            assert_eq!(counters.epoch, None);
            assert_eq!(counters.updated_rows_total, base_updated + 1);
            assert_eq!(counters.deleted_rows_total, base_deleted + 1);

            let mut upgraded = snapshot_at(Some(20), Some(Arc::new(old_latest)));
            upgraded.add_logical_change_delta(2, 3);
            let counters = upgraded.logical_change_counters().unwrap();
            assert_eq!(counters.epoch, Some(20));
            // An old reader ignores epoch and subtracts the totals directly.
            assert_eq!(counters.updated_rows_total - base_updated, 3);
            assert_eq!(counters.deleted_rows_total - base_deleted, 4);
            assert_eq!(counters.delta_from(&base_counters).unwrap(), None);

            // An older writer takes over again and drops the epoch. The next
            // new writer must preserve its increments while assigning a new epoch.
            let old_latest = old_writer(&upgraded, 4, 5);
            let next = snapshot_at(Some(30), Some(Arc::new(old_latest)));
            let decoded = TableSnapshot::from_slice(&next.to_bytes().unwrap()).unwrap();
            let next_counters = decoded.logical_change_counters().unwrap();
            assert_eq!(next_counters.epoch, Some(30));
            assert_eq!(next_counters.updated_rows_total - base_updated, 7);
            assert_eq!(next_counters.deleted_rows_total - base_deleted, 9);
            assert_eq!(next_counters.delta_from(&counters).unwrap(), None);
        }
    }

    #[test]
    fn test_missing_seq_does_not_create_a_known_epoch() {
        let base = snapshot(None);
        let child = snapshot(Some(Arc::new(base.clone())));
        let base_counters = base.logical_change_counters().unwrap();
        assert!(base_counters.epoch.is_none());
        assert_eq!(
            child
                .logical_change_counters()
                .unwrap()
                .delta_from(&base_counters)
                .unwrap(),
            None
        );
    }

    #[test]
    fn test_delta_across_intermediate_snapshots() {
        let mut base = snapshot_at(Some(10), None);
        base.add_logical_change_delta(5, 3);
        let base_counters = base.logical_change_counters().unwrap();
        let mut intermediate = snapshot_at(Some(11), Some(Arc::new(base)));
        intermediate.add_logical_change_delta(2, 1);
        let mut latest = snapshot_at(Some(12), Some(Arc::new(intermediate)));
        latest.add_logical_change_delta(3, 2);
        assert_eq!(
            latest
                .logical_change_counters()
                .unwrap()
                .delta_from(&base_counters)
                .unwrap(),
            Some((5, 3))
        );

        for seq in [None, Some(20)] {
            let unrelated = snapshot_at(seq, None).logical_change_counters().unwrap();
            assert_eq!(unrelated.delta_from(&base_counters).unwrap(), None);
        }
    }

    #[test]
    fn test_counter_overflow_restarts_both_totals() {
        for (updated, deleted) in [(u64::MAX, 0), (0, u64::MAX)] {
            let mut base = snapshot_at(Some(10), None);
            base.add_logical_change_delta(updated, deleted);
            let base_counters = base.logical_change_counters().unwrap();
            let mut child = snapshot_at(Some(20), Some(Arc::new(base)));
            child.add_logical_change_delta(1, 2);
            let counters = child.logical_change_counters().unwrap();
            assert_eq!(counters.epoch, Some(20));
            assert_eq!(counters.updated_rows_total, 1);
            assert_eq!(counters.deleted_rows_total, 2);
            assert_eq!(counters.delta_from(&base_counters).unwrap(), None);

            let decoded = TableSnapshot::from_slice(&child.to_bytes().unwrap()).unwrap();
            let mut descendant = snapshot_at(Some(30), Some(Arc::new(decoded)));
            descendant.add_logical_change_delta(3, 4);
            assert_eq!(
                descendant
                    .logical_change_counters()
                    .unwrap()
                    .delta_from(&counters)
                    .unwrap(),
                Some((3, 4))
            );

            // A second overflow at the same seq must not reuse epoch 20.
            child.add_logical_change_delta(u64::MAX, u64::MAX);
            let reset = child.logical_change_counters().unwrap();
            assert_eq!(reset.epoch, None);
            assert_eq!(reset.updated_rows_total, u64::MAX);
            assert_eq!(reset.deleted_rows_total, u64::MAX);
            assert_eq!(reset.delta_from(&counters).unwrap(), None);
        }
    }

    #[test]
    fn test_overflow_without_fresh_seq_stays_unknown() {
        for seq in [None, Some(9), Some(10)] {
            let mut base = snapshot_at(Some(10), None);
            base.add_logical_change_delta(u64::MAX, 0);
            let mut child = snapshot_at(seq, Some(Arc::new(base)));
            child.add_logical_change_delta(1, 2);
            let counters = child.logical_change_counters().unwrap();
            assert_eq!(counters.epoch, None);
            assert_eq!(counters.updated_rows_total, 1);
            assert_eq!(counters.deleted_rows_total, 2);
            assert_eq!(counters.delta_from(&counters).unwrap(), None);
        }
    }

    #[test]
    fn test_absent_counters_roundtrip_and_restart() {
        let mut base = snapshot_at(Some(10), None);
        base.add_logical_change_delta(7, 9);
        let base_counters = base.logical_change_counters().unwrap();
        let mut invalidated = snapshot_at(Some(20), Some(Arc::new(base.clone())));
        invalidated.invalidate_logical_change_counters();

        // Both a legacy writer and explicit invalidation produce unknown counters.
        for mut absent in [strip_counters(&base), invalidated] {
            absent.add_logical_change_delta(4, 6);
            assert!(absent.logical_change_counters().is_none());
            let decoded = TableSnapshot::from_slice(&absent.to_bytes().unwrap()).unwrap();
            assert!(decoded.logical_change_counters().is_none());

            let restarted = snapshot_at(Some(30), Some(Arc::new(decoded)));
            let counters = restarted.logical_change_counters().unwrap();
            assert_eq!(counters.epoch, Some(30));
            assert_eq!(counters.updated_rows_total, 0);
            assert_eq!(counters.deleted_rows_total, 0);
            assert_eq!(counters.delta_from(&base_counters).unwrap(), None);
        }
    }
}
