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

//! Building the segment zone map.
//!
//! Two independent decisions:
//!
//! 1. *Start*: whether the current snapshot's zone map is worth refreshing. Only
//!    full segments count, estimated from block counts (see
//!    [`estimate_uncovered_full_segments`]), so a trickle of small segments that
//!    compaction will soon merge away does not trigger a rebuild.
//! 2. *Admission*: how many of the uncovered segments to add. The oldest ones
//!    are taken first (appends go to the front of the segment list), in a
//!    proportion that follows how well the rows added by the previous build
//!    survived (see [`next_ratio`]). Rows that were rewritten before the next
//!    build were wasted; a low survival shrinks the next admission so the
//!    youngest, still-changing segments stay out of the zone map until they settle.
//!
//! Every build writes a complete new file: rows of the previous zone map whose
//! segment is still live are copied, admitted segments have their summary read.
//! Both sides stream one row group at a time (see [`crate::segment_zone_map::format`]),
//! so a build holds no more than a row group of either file, plus one bit per
//! segment of the snapshot.

use std::collections::HashMap;
use std::collections::HashSet;
use std::sync::Arc;

use backoff::backoff::Backoff;
use databend_common_catalog::table::Table;
use databend_common_catalog::table::TableExt;
use databend_common_catalog::table_context::TableContext;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_exception::ResultExt;
use databend_common_expression::ColumnId;
use databend_common_io::constants::DEFAULT_BLOCK_PER_SEGMENT;
use databend_storages_common_cache::CachePolicy;
use databend_storages_common_cache::CompactSegmentInfo;
use databend_storages_common_io::ReadSettings;
use databend_storages_common_table_meta::meta::Location;
use databend_storages_common_table_meta::meta::SegmentZoneMapMeta;
use databend_storages_common_table_meta::meta::SnapshotId;
use databend_storages_common_table_meta::meta::TableSnapshot;
use log::info;
use log::warn;

use crate::FUSE_OPT_KEY_BLOCK_PER_SEGMENT;
use crate::FUSE_OPT_KEY_ENABLE_SEGMENT_ZONE_MAP;
use crate::FuseTable;
use crate::io::SegmentsIO;
use crate::operations::set_backoff;
use crate::segment_zone_map::format::SegmentZoneMapFile;
use crate::segment_zone_map::format::SegmentZoneMapReader;
use crate::segment_zone_map::format::SegmentZoneMapWriter;
use crate::segment_zone_map::format::zone_map_columns;

/// Segments whose summaries are read per batch while building.
const BUILD_SEGMENT_BATCH: usize = 1000;
/// Lower bound of the admission ratio: a build always probes at least this share
/// of the uncovered segments, so the next build has rows to learn from.
const RATIO_MIN: f64 = 0.01;
/// How fast the ratio opens up again once admitted rows survive.
const RATIO_DAMPING: f64 = 0.5;
/// Estimated number of full segments among the uncovered ones.
///
/// Models uncovered segments as `F` full segments of `block_per_segment` blocks
/// plus fragments of one block each, and solves for `F`. Fragments are what
/// compaction merges next, so they are discounted without reading any segment.
fn estimate_uncovered_full_segments(blocks: u64, segments: u64, block_per_segment: u64) -> u64 {
    if block_per_segment <= 1 {
        return segments;
    }
    blocks.saturating_sub(segments) / (block_per_segment - 1)
}

/// Admission ratio for this build.
///
/// `survival` is the share of the rows the previous build added whose segment is
/// still live; `None` when it added none. A survival below the previous ratio
/// shrinks the ratio by that factor at once; a survival at or above it moves the
/// ratio towards the survival with damping. Shrinking fast and growing slowly
/// keeps a few wasted rows from recurring while a recovery costs only coverage.
fn next_ratio(previous: f64, survival: Option<f64>) -> f64 {
    let previous = previous.clamp(RATIO_MIN, 1.0);
    let Some(survival) = survival else {
        return previous;
    };
    let survival = survival.clamp(0.0, 1.0);
    let next = match survival < previous {
        true => previous * survival,
        false => previous + RATIO_DAMPING * (survival - previous),
    };
    next.clamp(RATIO_MIN, 1.0)
}

/// Number of uncovered segments to admit: never zero while any is uncovered.
fn admitted_count(uncovered: usize, ratio: f64) -> usize {
    ((uncovered as f64 * ratio).ceil() as usize).min(uncovered)
}

/// Whether a built zone map may be committed on top of `latest`.
#[derive(Debug, PartialEq, Eq)]
enum CommitCheck {
    Proceed,
    /// Another writer replaced the pointer the build started from.
    Superseded,
    /// The table was truncated or wholly rewritten since: no written row is live.
    NoLiveSegment,
}

/// `base` is the pointer of the snapshot the build read, `written` the segment
/// paths in the new file.
fn commit_check<'a>(
    latest: &TableSnapshot,
    base: Option<&SegmentZoneMapMeta>,
    mut written: impl Iterator<Item = &'a str>,
) -> CommitCheck {
    if latest.segment_zone_map.as_ref().map(|m| &m.location) != base.map(|m| &m.location) {
        return CommitCheck::Superseded;
    }
    let live: HashSet<&str> = latest.segments.iter().map(|(p, _)| p.as_str()).collect();
    match written.any(|path| live.contains(path)) {
        true => CommitCheck::Proceed,
        false => CommitCheck::NoLiveSegment,
    }
}

/// Paths of the segments of `snapshot` flagged in `flags`.
fn flagged_paths<'a>(
    snapshot: &'a TableSnapshot,
    flags: &'a [bool],
) -> impl Iterator<Item = &'a str> + Clone {
    let segments = snapshot.segments.iter().zip(flags);
    segments
        .filter(|(_, flag)| **flag)
        .map(|((path, _), _)| path.as_str())
}

/// Outcome of a refresh attempt, for logging.
#[derive(Debug, PartialEq, Eq)]
pub enum RefreshOutcome {
    /// The table does not qualify, or the start decision was negative.
    Skipped,
    /// A new zone map file was written but another writer moved the pointer first.
    Superseded,
    Built {
        rows: u64,
        admitted: usize,
        copied: usize,
    },
}

impl FuseTable {
    /// Whether this table maintains a segment zone map: on unless the table
    /// option `enable_segment_zone_map` is 0.
    pub fn segment_zone_map_enabled(&self) -> bool {
        self.get_option(FUSE_OPT_KEY_ENABLE_SEGMENT_ZONE_MAP, 1u32) != 0
    }

    /// Refresh the segment zone map of the latest snapshot if the start decision
    /// allows it, and commit a snapshot pointing to it.
    ///
    /// `full_segment_threshold`: build when more than this many full segments
    /// are estimated to be uncovered.
    #[async_backtrace::framed]
    pub async fn refresh_segment_zone_map(
        &self,
        ctx: Arc<dyn TableContext>,
        full_segment_threshold: u64,
    ) -> Result<RefreshOutcome> {
        if !self.segment_zone_map_enabled()
            || self.is_column_oriented()
            || ctx.txn_mgr().lock().is_active()
        {
            return Ok(RefreshOutcome::Skipped);
        }
        let policy = ctx.cache_policy();
        let Some(snapshot) = self.read_table_snapshot_with_policy(policy).await? else {
            return Ok(RefreshOutcome::Skipped);
        };
        let columns = zone_map_columns(&snapshot.schema);
        if snapshot.segments.is_empty() || columns.is_empty() {
            return Ok(RefreshOutcome::Skipped);
        }
        let pointer = snapshot.segment_zone_map.as_ref();
        // Snapshot position of every live segment.
        let mut live = HashMap::with_capacity(snapshot.segments.len());
        for (idx, (path, _)) in snapshot.segments.iter().enumerate() {
            live.insert(path.as_str(), idx);
        }

        // The previous zone map; a missing or unreadable one only costs a full rebuild.
        let mut previous = None;
        if let Some(meta) = pointer {
            let settings = ReadSettings::from_ctx(&ctx)?;
            let reader = SegmentZoneMapReader::new(self.operator.clone(), policy, settings);
            match reader.open(meta).await {
                Ok(file) => previous = Some(file),
                Err(e) => warn!(
                    "ignore unreadable segment zone map {}: {e}",
                    meta.location.0
                ),
            }
        }

        // Start decision, from the segment fields of the previous zone map.
        let mut covered = vec![false; snapshot.segments.len()];
        let (mut alive_rows, mut covered_blocks, mut dead_blocks) = (0u64, 0u64, 0u64);
        let (mut fresh_rows, mut fresh_alive) = (0u64, 0u64);
        if let Some(file) = &previous {
            for group in 0..file.num_row_groups() {
                let rows = file.read_row_group(group, &HashSet::new()).await?;
                for row in 0..rows.num_rows() {
                    let idx = live.get(rows.path(row));
                    match idx {
                        Some(idx) => {
                            covered[*idx] = true;
                            alive_rows += 1;
                            covered_blocks += rows.block_count(row);
                        }
                        None => dead_blocks += rows.block_count(row),
                    }
                    if rows.is_fresh(row) {
                        fresh_rows += 1;
                        fresh_alive += u64::from(idx.is_some());
                    }
                }
            }
        }
        let total_segments = snapshot.segments.len() as u64;
        let uncovered_segments = total_segments - alive_rows;
        let uncovered_blocks = snapshot.summary.block_count.saturating_sub(covered_blocks);
        let per_segment =
            self.get_option(FUSE_OPT_KEY_BLOCK_PER_SEGMENT, DEFAULT_BLOCK_PER_SEGMENT);
        let uncovered_full = estimate_uncovered_full_segments(
            uncovered_blocks,
            uncovered_segments,
            per_segment as u64,
        );
        let survival = (fresh_rows > 0).then(|| fresh_alive as f64 / fresh_rows as f64);
        let previous_ratio = previous.as_ref().map_or(1.0, SegmentZoneMapFile::ratio);
        let ratio = next_ratio(previous_ratio, survival);
        let previous_rows = previous.as_ref().map_or(0, |f| f.num_rows() as u64);
        let dead_rows = previous_rows - alive_rows;
        info!(
            "segment zone map of table {}: {total_segments} live segments ({} blocks), {alive_rows} covered, \
             {dead_rows} dead rows ({dead_blocks} blocks), {uncovered_segments} uncovered segments \
             ({uncovered_blocks} blocks, about {uncovered_full} full), threshold {full_segment_threshold}, \
             fresh survival {survival:?}, ratio {previous_ratio} -> {ratio}",
            self.table_info.desc, snapshot.summary.block_count,
        );
        if uncovered_full <= full_segment_threshold {
            return Ok(RefreshOutcome::Skipped);
        }

        // Admission: the oldest uncovered segments first (appends go to the front).
        let uncovered: Vec<usize> = (0..covered.len()).filter(|idx| !covered[*idx]).collect();
        let admit = admitted_count(uncovered.len(), ratio);
        let admitted = &uncovered[uncovered.len() - admit..];
        // `covered` now flags every segment the new file will hold.
        for idx in admitted {
            covered[*idx] = true;
        }

        let timestamps = ctx.get_table_meta_timestamps(self, Some(snapshot.clone()))?;
        let location = self
            .meta_location_generator
            .gen_segment_zone_map_location(timestamps);
        let column_ids = columns.iter().map(|c| c.column_id).collect();
        let mut writer =
            SegmentZoneMapWriter::create(&self.operator, location, columns, ratio).await?;
        let writing = self.write_zone_map_rows(
            ctx.clone(),
            &snapshot,
            previous.as_ref(),
            &column_ids,
            &live,
            admitted,
            &mut writer,
        );
        let copied = match writing.await {
            Ok(copied) => copied,
            Err(e) => {
                writer.abort().await;
                return Err(e);
            }
        };
        let rows = writer.num_rows() as u64;
        let meta = writer.finish().await?;
        info!(
            "built segment zone map {} for table {}: {rows} rows, {admit} admitted, {copied} copied, ratio {ratio}",
            meta.location.0, self.table_info.desc,
        );
        let written = flagged_paths(&snapshot, &covered);
        let committed = self.commit_segment_zone_map(ctx, pointer, &meta, written);
        if committed.await? {
            return Ok(RefreshOutcome::Built {
                rows,
                admitted: admit,
                copied,
            });
        }
        // Nothing points at the file; vacuum would collect it eventually.
        if let Err(e) = self.operator.delete(&meta.location.0).await {
            warn!(
                "failed to remove abandoned segment zone map {}: {e}",
                meta.location.0
            );
        }
        Ok(RefreshOutcome::Superseded)
    }

    /// Write the rows of a new zone map: the rows of `previous` whose segment is
    /// in `live`, copied one row group at a time, then the summaries of the
    /// `admitted` segments of `snapshot`, read in batches. Returns the number of
    /// copied rows.
    async fn write_zone_map_rows(
        &self,
        ctx: Arc<dyn TableContext>,
        snapshot: &TableSnapshot,
        previous: Option<&SegmentZoneMapFile>,
        column_ids: &HashSet<ColumnId>,
        live: &HashMap<&str, usize>,
        admitted: &[usize],
        writer: &mut SegmentZoneMapWriter,
    ) -> Result<usize> {
        let abort_checker = ctx.clone().get_abort_checker();
        let check_aborting = || {
            let check = abort_checker.try_check_aborting();
            check.with_context(|| "build_segment_zone_map")
        };
        let mut copied = 0;
        if let Some(file) = previous {
            for group in 0..file.num_row_groups() {
                check_aborting()?;
                let rows = file.read_row_group(group, column_ids).await?;
                let sources = writer.copy_sources(&rows);
                for row in 0..rows.num_rows() {
                    if live.contains_key(rows.path(row)) {
                        writer.push_copy(&rows, row, &sources).await?;
                        copied += 1;
                    }
                }
            }
        }
        let schema = Arc::new(snapshot.schema.clone());
        let policy = CachePolicy::READ_ONLY.at_most(ctx.cache_policy());
        let segments_io = SegmentsIO::create(ctx, self.operator.clone(), schema);
        for batch in admitted.chunks(BUILD_SEGMENT_BATCH) {
            check_aborting()?;
            let locations: Vec<Location> = batch
                .iter()
                .map(|i| snapshot.segments[*i].clone())
                .collect();
            let summaries =
                segments_io.read_segments::<Arc<CompactSegmentInfo>>(&locations, policy);
            for (location, summary) in locations.iter().zip(summaries.await?) {
                writer.push_summary(&location.0, &summary?.summary).await?;
            }
        }
        Ok(copied)
    }

    /// Commit a snapshot that only points to `meta`, rebasing on concurrent
    /// commits. Any zone map is valid for any snapshot, so a rebase never rebuilds.
    ///
    /// Returns false, without committing, when the pointer the build started
    /// from (`base`) has been replaced by another writer, or when none of the
    /// segments in the file (`written`) is live any more: the table was
    /// truncated or wholly rewritten meanwhile, and the file is useless.
    async fn commit_segment_zone_map<'a>(
        &self,
        ctx: Arc<dyn TableContext>,
        base: Option<&SegmentZoneMapMeta>,
        meta: &SegmentZoneMapMeta,
        written: impl Iterator<Item = &'a str> + Clone,
    ) -> Result<bool> {
        let mut backoff = None;
        loop {
            let table = self.refresh(ctx.as_ref()).await?;
            let table = FuseTable::try_from_table(table.as_ref())?;
            let policy = ctx.cache_policy();
            let Some(latest) = table.read_table_snapshot_with_policy(policy).await? else {
                return Ok(false);
            };
            let check = commit_check(&latest, base, written.clone());
            if check != CommitCheck::Proceed {
                info!(
                    "segment zone map {} not committed: {check:?}",
                    meta.location.0
                );
                return Ok(false);
            }

            let timestamps = ctx.get_table_meta_timestamps(table, Some(latest.clone()))?;
            let seq = Some(table.table_info.ident.seq);
            let cluster_key = table.cluster_key_info();
            let mut snapshot =
                TableSnapshot::try_from_previous(latest.clone(), cluster_key, seq, timestamps)?;
            snapshot.segment_zone_map = Some(meta.clone());
            // Table statistics are fresh only for the snapshot they were taken
            // against. This commit changes no data, so a fresh sidecar is
            // retagged to stay fresh, as metadata-only ALTERs do.
            let generator = &table.meta_location_generator;
            let mut table_statistics = None;
            let stats = table.read_table_snapshot_statistics_with_policy(Some(&latest), policy);
            if let Some(stats) = stats.await? {
                if stats.is_fresh_for(&latest) {
                    let mut stats = stats.as_ref().clone();
                    stats.snapshot_id = latest.snapshot_id;
                    let (id, version) = (SnapshotId::now_v7(), stats.format_version());
                    let location =
                        generator.snapshot_statistics_location_from_uuid(&id, version)?;
                    snapshot.table_statistics_location = Some(location);
                    table_statistics = Some(stats);
                }
            }
            let commit = table.commit_to_meta_server(
                ctx.as_ref(),
                &table.table_info,
                generator,
                snapshot,
                table_statistics,
                &None,
                &table.operator,
            );
            match commit.await {
                Ok(()) => return Ok(true),
                Err(e) if e.code() == ErrorCode::TABLE_VERSION_MISMATCHED => {
                    let backoff = backoff.get_or_insert_with(|| set_backoff(None, None, None));
                    let Some(delay) = backoff.next_backoff() else {
                        return Err(e);
                    };
                    tokio::time::sleep(delay).await;
                }
                Err(e) => return Err(e),
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_estimate_uncovered_full_segments() {
        // Fragments only.
        assert_eq!(estimate_uncovered_full_segments(200, 200, 1000), 0);
        // 200 fragments and 3 full segments.
        assert_eq!(estimate_uncovered_full_segments(3200, 203, 1000), 3);
        // 51 full segments.
        assert_eq!(estimate_uncovered_full_segments(51_000, 51, 1000), 51);
        // Oversized segments count more than once.
        assert_eq!(estimate_uncovered_full_segments(2000, 1, 1000), 2);
        // Degenerate block_per_segment: every segment is full.
        assert_eq!(estimate_uncovered_full_segments(5, 5, 1), 5);
        // Inconsistent inputs never underflow.
        assert_eq!(estimate_uncovered_full_segments(1, 5, 1000), 0);
    }

    #[test]
    fn test_next_ratio() {
        // No sample: unchanged.
        assert_eq!(next_ratio(0.4, None), 0.4);
        // Low survival shrinks multiplicatively.
        assert!((next_ratio(1.0, Some(0.7)) - 0.7).abs() < 1e-9);
        assert!((next_ratio(0.7, Some(0.5)) - 0.35).abs() < 1e-9);
        // Everything died: floored, never zero.
        assert_eq!(next_ratio(1.0, Some(0.0)), RATIO_MIN);
        assert_eq!(next_ratio(RATIO_MIN, Some(0.0)), RATIO_MIN);
        // Recovery is damped.
        assert!((next_ratio(RATIO_MIN, Some(1.0)) - 0.505).abs() < 1e-9);
        assert!((next_ratio(0.505, Some(1.0)) - 0.7525).abs() < 1e-9);
        assert_eq!(next_ratio(1.0, Some(1.0)), 1.0);
        // Out-of-range inputs are clamped.
        assert_eq!(next_ratio(5.0, Some(2.0)), 1.0);
        assert_eq!(next_ratio(0.0, None), RATIO_MIN);
    }

    #[test]
    fn test_commit_check() {
        use chrono::Duration;
        use databend_common_expression::TableSchema;
        use databend_storages_common_table_meta::meta::Statistics;
        use databend_storages_common_table_meta::meta::TableMetaTimestamps;

        let pointer = |path: &str| SegmentZoneMapMeta {
            location: (path.to_string(), 1),
            footer_offset: 0,
        };
        let snapshot = |segments: &[&str], zone_map: Option<&str>| {
            let segments = segments.iter().map(|p| (p.to_string(), 4)).collect();
            let timestamps = TableMetaTimestamps::new(None, Duration::hours(1));
            let (schema, summary) = (TableSchema::default(), Statistics::default());
            let mut snapshot = TableSnapshot::try_new(
                None, None, schema, summary, segments, None, None, timestamps,
            )
            .unwrap();
            snapshot.segment_zone_map = zone_map.map(pointer);
            snapshot
        };
        let check =
            |segments: &[&str], zone_map: Option<&str>, base: Option<&str>, written: &[&str]| {
                commit_check(
                    &snapshot(segments, zone_map),
                    base.map(pointer).as_ref(),
                    written.iter().copied(),
                )
            };

        // Pointer unchanged and a written segment is live.
        assert_eq!(
            check(&["s1", "s2"], Some("p0"), Some("p0"), &["s2", "s9"]),
            CommitCheck::Proceed
        );
        assert_eq!(check(&["s1"], None, None, &["s1"]), CommitCheck::Proceed);
        // Another build, or a truncate, moved the pointer.
        assert_eq!(
            check(&["s1"], Some("p1"), Some("p0"), &["s1"]),
            CommitCheck::Superseded
        );
        assert_eq!(
            check(&[], None, Some("p0"), &["s1"]),
            CommitCheck::Superseded
        );
        assert_eq!(
            check(&["s1"], Some("p1"), None, &["s1"]),
            CommitCheck::Superseded
        );
        // Same pointer, but every covered segment is gone.
        assert_eq!(
            check(&["s3"], Some("p0"), Some("p0"), &["s1", "s2"]),
            CommitCheck::NoLiveSegment
        );
        assert_eq!(check(&[], None, None, &["s1"]), CommitCheck::NoLiveSegment);
    }

    #[test]
    fn test_admitted_count() {
        assert_eq!(admitted_count(0, 1.0), 0);
        assert_eq!(admitted_count(10, 1.0), 10);
        assert_eq!(admitted_count(10, 0.25), 3);
        // Probes at least one segment while any is uncovered.
        assert_eq!(admitted_count(10, RATIO_MIN), 1);
        assert_eq!(admitted_count(1, RATIO_MIN), 1);
    }
}
