//  Copyright 2021 Datafuse Labs.
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

use std::collections::HashMap;
use std::collections::HashSet;
use std::sync::Arc;
use std::time::Instant;

use chrono::DateTime;
use chrono::Duration;
use chrono::Utc;
use common_cache::Cache;
use common_catalog::table_context::TableContext;
use common_exception::ErrorCode;
use common_exception::Result;
use common_storages_table_meta::caches::CacheManager;
use common_storages_table_meta::meta::Location;
use common_storages_table_meta::meta::SnapshotId;
use common_storages_table_meta::meta::TableSnapshotLite;
use tracing::info;
use tracing::warn;

use crate::io::Files;
use crate::io::ListSnapshotLiteOption;
use crate::io::SegmentsIO;
use crate::io::SnapshotsIO;
use crate::FuseTable;

#[derive(Default)]
struct LocationTuple {
    block_location: HashSet<String>,
    bloom_location: HashSet<String>,
}

impl FuseTable {
    pub async fn do_purge(
        &self,
        ctx: &Arc<dyn TableContext>,
        keep_last_snapshot: bool,
    ) -> Result<()> {
        let snapshot_opt = match self.read_table_snapshot().await {
            Err(e) if e.code() == ErrorCode::STORAGE_NOT_FOUND => {
                // concurrent gc: someone else has already collected this snapshot, ignore it
                warn!(
                    "concurrent gc: snapshot {:?} already collected. table: {}, ident {}",
                    self.snapshot_loc().await?,
                    self.table_info.desc,
                    self.table_info.ident,
                );
                return Ok(());
            }
            Err(e) => return Err(e),
            Ok(v) => v,
        };

        // 1. Root snapshot.
        let mut segments_referenced_by_root = HashSet::new();
        let mut locations_referenced_by_root = Default::default();
        let (root_snapshot_id, root_snapshot_ts, root_ts_location_opt) =
            if let Some(ref root_snapshot) = snapshot_opt {
                let segments = root_snapshot.segments.clone();
                locations_referenced_by_root =
                    self.get_block_locations(ctx.clone(), &segments).await?;
                segments_referenced_by_root = HashSet::from_iter(segments);
                (
                    root_snapshot.snapshot_id,
                    root_snapshot.timestamp,
                    root_snapshot.table_statistics_location.clone(),
                )
            } else {
                (SnapshotId::new_v4(), None, None)
            };

        // 2. Get all snapshot(including root snapshot).
        let mut chained_snapshots = vec![];
        let mut all_segment_locations = HashSet::new();
        let mut orphan_snapshots = vec![];

        let mut status_snapshot_scan_count = 0;
        let mut status_snapshot_scan_cost = 0;

        if let Some(root_snapshot_location) = self.snapshot_loc().await? {
            let snapshots_io = SnapshotsIO::create(
                ctx.clone(),
                self.operator.clone(),
                self.snapshot_format_version().await?,
            );

            let start = Instant::now();
            let min_snapshot_timestamp = root_snapshot_ts;
            let mut segments_excluded = &HashSet::new();
            if keep_last_snapshot {
                segments_excluded = &segments_referenced_by_root
            };
            let snapshot_lites_extended = snapshots_io
                .read_snapshot_lites_ext(
                    root_snapshot_location.clone(),
                    None,
                    ListSnapshotLiteOption::NeedSegmentsWithExclusion(Some(segments_excluded)),
                    min_snapshot_timestamp,
                    |x| {
                        self.data_metrics.set_status(&x);
                    },
                )
                .await?;

            chained_snapshots = snapshot_lites_extended.chained_snapshot_lites;

            // partition the orphan snapshots by retention interval
            let partitioned_snapshots = Self::apply_retention_rule(
                ctx.as_ref(),
                min_snapshot_timestamp,
                snapshot_lites_extended.orphan_snapshot_lites,
            )?;

            // filter out segments that still referenced by snapshot that within retention period
            all_segment_locations = Self::filter_out_segments_within_retention(
                partitioned_snapshots
                    .within_retention
                    .into_iter()
                    .map(|snapshot| snapshot.snapshot_id)
                    .collect(),
                snapshot_lites_extended.segment_locations,
            );

            // orphan_snapshots that beyond retention period are allowed to be collected
            orphan_snapshots = partitioned_snapshots.beyond_retention;

            // FIXME: we do not need to write last snapshot hint here(since last snapshot never changed
            // during gc). introduce a dedicated stmt to refresh the hint file instead pls.

            // try keep a hit file of last snapshot
            Self::write_last_snapshot_hint(
                &self.operator,
                &self.meta_location_generator,
                root_snapshot_location,
            )
            .await;

            status_snapshot_scan_count += chained_snapshots.len() + orphan_snapshots.len();
            status_snapshot_scan_cost += start.elapsed().as_secs();
        }

        // 3. Find.
        let mut snapshots_to_be_purged = HashSet::new();
        let mut segments_to_be_purged = HashSet::new();
        let ts_to_be_purged: Vec<String> = vec![];

        // 3.1 Find all the snapshots need to be deleted.
        {
            for snapshot in &chained_snapshots {
                // Skip the root snapshot if the keep_last_snapshot is true.
                if keep_last_snapshot && snapshot.snapshot_id == root_snapshot_id {
                    continue;
                }
                snapshots_to_be_purged.insert((snapshot.snapshot_id, snapshot.format_version));
            }
        }

        // 3.2 Find all the segments need to be deleted.
        {
            for segment in &all_segment_locations {
                // Skip the root snapshot segments if the keep_last_snapshot is true.
                if keep_last_snapshot && segments_referenced_by_root.contains(segment) {
                    continue;
                }
                segments_to_be_purged.insert(segment.clone());
            }
        }

        // 3.3 Find all the table statistic files need to be deleted
        {
            if let Some(root_ts_location) = root_ts_location_opt {
                let start = Instant::now();
                let snapshots_io = SnapshotsIO::create(
                    ctx.clone(),
                    self.operator.clone(),
                    self.snapshot_format_version().await?,
                );
                let ts_to_be_purged = snapshots_io
                    .read_table_statistic_files(&root_ts_location, None)
                    .await?;
                let status_ts_scan_count = ts_to_be_purged.len();
                let status_ts_scan_cost = start.elapsed().as_secs();
                let status = format!(
                    "gc: scan table statistic files:{} takes:{} sec.",
                    status_ts_scan_count, status_ts_scan_cost,
                );
                self.data_metrics.set_status(&status);
                info!(status);
            }
        }

        let chunk_size = ctx.get_settings().get_max_storage_io_requests()? as usize;

        // 4. Purge segments&blocks by chunk size
        {
            let mut status_block_to_be_purged_count = 0;
            let mut status_bloom_to_be_purged_count = 0;
            let mut status_segment_to_be_purged_count = 0;

            let start = Instant::now();
            let segment_locations = Vec::from_iter(segments_to_be_purged);
            for chunk in segment_locations.chunks(chunk_size) {
                let locations = self.get_block_locations(ctx.clone(), chunk).await?;

                // 1. Try to purge block file chunks.
                {
                    let mut block_locations_to_be_pruged = HashSet::new();
                    for loc in &locations.block_location {
                        if keep_last_snapshot
                            && locations_referenced_by_root.block_location.contains(loc)
                        {
                            continue;
                        }
                        block_locations_to_be_pruged.insert(loc.to_string());
                    }
                    status_block_to_be_purged_count += block_locations_to_be_pruged.len();
                    self.try_purge_location_files(ctx.clone(), block_locations_to_be_pruged)
                        .await?;
                }

                // 2. Try to purge bloom index file chunks.
                {
                    let mut bloom_locations_to_be_pruged = HashSet::new();
                    for loc in &locations.bloom_location {
                        if keep_last_snapshot
                            && locations_referenced_by_root.bloom_location.contains(loc)
                        {
                            continue;
                        }
                        bloom_locations_to_be_pruged.insert(loc.to_string());
                    }
                    status_bloom_to_be_purged_count += bloom_locations_to_be_pruged.len();
                    self.try_purge_location_files(ctx.clone(), bloom_locations_to_be_pruged)
                        .await?;
                }

                // 3. Try to purge segment file chunks.
                {
                    let segment_locations_to_be_purged = HashSet::from_iter(
                        chunk
                            .iter()
                            .map(|loc| loc.0.clone())
                            .collect::<Vec<String>>(),
                    );
                    self.try_purge_location_files(ctx.clone(), segment_locations_to_be_purged)
                        .await?;
                }

                // Refresh status.
                {
                    status_segment_to_be_purged_count += chunk.len();
                    let status = format!(
                        "gc: scan snapshot:{} takes:{} sec. block files purged:{}, bloom files purged:{}, segment files purged:{}, take:{} sec",
                        status_snapshot_scan_count,
                        status_snapshot_scan_cost,
                        status_block_to_be_purged_count,
                        status_bloom_to_be_purged_count,
                        status_segment_to_be_purged_count,
                        start.elapsed().as_secs()
                    );
                    self.data_metrics.set_status(&status);
                    info!(status);
                }
            }
        }

        // 5. Purge snapshots by chunk size(max_storage_io_requests).
        {
            let mut status_purged_count = 0;

            let location_gen = self.meta_location_generator();
            let snapshots_to_be_purged_vec = Vec::from_iter(
                snapshots_to_be_purged.into_iter().chain(
                    orphan_snapshots
                        .into_iter()
                        .map(|lite| (lite.snapshot_id, lite.format_version)),
                ),
            );

            // let snapshots_to_be_purged_vec = Vec::from_iter(snapshots_to_be_purged);
            let status_need_purged_count = snapshots_to_be_purged_vec.len();

            let start = Instant::now();
            for chunk in snapshots_to_be_purged_vec.chunks(chunk_size) {
                let mut snapshot_locations_to_be_purged = HashSet::new();
                for (id, ver) in chunk {
                    if let Ok(loc) = location_gen.snapshot_location_from_uuid(id, *ver) {
                        snapshot_locations_to_be_purged.insert(loc);
                    }
                }
                self.try_purge_location_files(ctx.clone(), snapshot_locations_to_be_purged)
                    .await?;

                // Refresh status.
                {
                    status_purged_count += chunk.len();
                    let status = format!(
                        "gc: snapshots need to be purged:{}, have purged:{}, take:{} sec",
                        status_need_purged_count,
                        status_purged_count,
                        start.elapsed().as_secs()
                    );
                    self.data_metrics.set_status(&status);
                    info!(status);
                }
            }
        }

        // 6. Purge table statistic files
        {
            let mut status_purged_count = 0;
            let status_need_purged_count = ts_to_be_purged.len();
            let start = Instant::now();
            for chunk in ts_to_be_purged.chunks(chunk_size) {
                let mut ts_locations_to_be_purged = HashSet::new();
                for file in chunk {
                    ts_locations_to_be_purged.insert(file.clone());
                }
                self.try_purge_location_files(ctx.clone(), ts_locations_to_be_purged)
                    .await?;
                // Refresh status.
                {
                    status_purged_count += chunk.len();
                    let status = format!(
                        "gc: table statistic files need to be purged:{}, have purged:{}, take:{} sec",
                        status_need_purged_count,
                        status_purged_count,
                        start.elapsed().as_secs()
                    );
                    self.data_metrics.set_status(&status);
                    info!(status);
                }
            }
        }

        Ok(())
    }

    // Partition snapshot_lites into two parts
    // - those are beyond retention period
    // - those are within retention period
    fn apply_retention_rule(
        ctx: &dyn TableContext,
        base_timestamp: Option<DateTime<Utc>>,
        snapshot_lites: Vec<TableSnapshotLite>,
    ) -> Result<RetentionPartition> {
        //    let retention_interval = Duration::hours(DEFAULT_RETENTION_PERIOD_HOURS as i64);
        let retention_interval = Duration::hours(ctx.get_settings().get_retention_period()? as i64);
        let retention_point = base_timestamp.map(|s| s - retention_interval);
        let (beyond_retention, within_retention) = snapshot_lites
            .into_iter()
            .partition(|lite| lite.timestamp < retention_point);
        Ok(RetentionPartition {
            beyond_retention,
            within_retention,
        })
    }

    // filter out segments that are referenced by orphan snapshots
    // which are within retention period
    fn filter_out_segments_within_retention(
        // orphan_snapshot_index: impl IntoIterator<Item = SnapshotId>,
        orphan_snapshot_index: HashSet<SnapshotId>,
        mut segment_with_refer_index: HashMap<Location, HashSet<SnapshotId>>,
    ) -> HashSet<Location> {
        // let orphan_snapshot_index_bitmap = RoaringBitmap::from_iter(orphan_snapshot_index);
        // segment_with_refer_index
        //    .retain(|_location, refer_map| orphan_snapshot_index_bitmap.is_disjoint(refer_map));
        segment_with_refer_index
            .retain(|_location, refer_map| orphan_snapshot_index.is_disjoint(refer_map));
        segment_with_refer_index.into_keys().collect()
    }

    // Purge file by location chunks.
    async fn try_purge_location_files(
        &self,
        ctx: Arc<dyn TableContext>,
        locations_to_be_purged: HashSet<String>,
    ) -> Result<()> {
        let fuse_file = Files::create(ctx.clone(), self.operator.clone());
        let locations = Vec::from_iter(locations_to_be_purged);
        self.clean_cache(&locations);
        fuse_file.remove_file_in_batch(&locations).await
    }

    async fn get_block_locations(
        &self,
        ctx: Arc<dyn TableContext>,
        segment_locations: &[Location],
    ) -> Result<LocationTuple> {
        let mut blocks = HashSet::new();
        let mut blooms = HashSet::new();

        let fuse_segments = SegmentsIO::create(ctx.clone(), self.operator.clone());
        let segments = fuse_segments.read_segments(segment_locations).await?;
        for (idx, segment) in segments.iter().enumerate() {
            let segment = segment.clone();
            let segment_info = match segment {
                Err(e) if e.code() == ErrorCode::STORAGE_NOT_FOUND => {
                    let location = &segment_locations[idx];
                    // concurrent gc: someone else has already collected this segment, ignore it
                    warn!(
                        "concurrent gc: segment of location {} already collected. table: {}, ident {}",
                        location.0, self.table_info.desc, self.table_info.ident,
                    );
                    continue;
                }
                Err(e) => return Err(e),
                Ok(v) => v,
            };
            for block_meta in &segment_info.blocks {
                blocks.insert(block_meta.location.0.clone());
                blooms.insert(
                    block_meta
                        .bloom_filter_index_location
                        .clone()
                        .unwrap_or_default()
                        .0,
                );
            }
        }

        Ok(LocationTuple {
            block_location: blocks,
            bloom_location: blooms,
        })
    }

    fn clean_cache(&self, locs: &[String]) {
        if let Some(c) = CacheManager::instance().get_table_segment_cache() {
            let cache = &mut *c.write();
            for loc in locs {
                cache.pop(loc);
            }
        }
    }
}

struct RetentionPartition {
    beyond_retention: Vec<TableSnapshotLite>,
    within_retention: Vec<TableSnapshotLite>,
}
