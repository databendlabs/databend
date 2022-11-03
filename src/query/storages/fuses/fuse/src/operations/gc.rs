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

use std::collections::HashSet;
use std::sync::Arc;
use std::time::Instant;

use common_cache::Cache;
use common_catalog::table_context::TableContext;
use common_exception::ErrorCode;
use common_exception::Result;
use common_fuse_meta::caches::CacheManager;
use common_fuse_meta::meta::Location;
use common_fuse_meta::meta::SnapshotId;
use tracing::info;
use tracing::warn;

use crate::io::Files;
use crate::io::SegmentsIO;
use crate::io::SnapshotsIO;
use crate::FuseTable;

#[derive(Default)]
struct LocationTuple {
    block_location: HashSet<String>,
    bloom_location: HashSet<String>,
}

impl FuseTable {
    pub async fn do_gc(&self, ctx: &Arc<dyn TableContext>, keep_last_snapshot: bool) -> Result<()> {
        let r = self.read_table_snapshot().await;
        let snapshot_opt = match r {
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
        let (root_snapshot_id, root_snapshot_ts) = if let Some(root_snapshot) = snapshot_opt {
            let segments = root_snapshot.segments.clone();
            locations_referenced_by_root = self.get_block_locations(ctx.clone(), &segments).await?;
            segments_referenced_by_root = HashSet::from_iter(segments);
            (root_snapshot.snapshot_id, root_snapshot.timestamp)
        } else {
            (SnapshotId::new_v4(), None)
        };

        // 2. Get all snapshot(including root snapshot).
        let mut all_snapshot_lites = vec![];
        let mut all_segment_locations = HashSet::new();

        let mut status_snapshot_scan_count = 0;
        let mut status_snapshot_scan_cost = 0;
        if let Some(root_snapshot_location) = self.snapshot_loc().await? {
            let snapshots_io = SnapshotsIO::create(
                ctx.clone(),
                self.operator.clone(),
                self.snapshot_format_version().await?,
            );

            let start = Instant::now();
            (all_snapshot_lites, all_segment_locations) = snapshots_io
                .read_snapshot_lites(root_snapshot_location, None, true, root_snapshot_ts, |x| {
                    self.data_metrics.set_status(&x);
                })
                .await?;

            status_snapshot_scan_count += all_snapshot_lites.len();
            status_snapshot_scan_cost += start.elapsed().as_secs();
        }

        // 3. Find.
        let mut snapshots_to_be_purged = HashSet::new();
        let mut segments_to_be_purged = HashSet::new();

        // 3.1 Find all the snapshots need to be deleted.
        {
            for snapshot in &all_snapshot_lites {
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

        let chunk_size = ctx.get_settings().get_max_storage_io_requests()? as usize;

        // 4. Purge segments&blocks by chunk size
        {
            let mut status_block_to_be_purged_count = 0;
            let mut status_bloom_to_be_purged_count = 0;
            let mut status_segment_to_be_purged_count = 0;

            let start = Instant::now();
            let segments_to_be_purged_vec = Vec::from_iter(segments_to_be_purged);
            for chunk in segments_to_be_purged_vec.chunks(chunk_size) {
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
            let status_need_purged_count = snapshots_to_be_purged.len();

            let location_gen = self.meta_location_generator();
            let snapshots_to_be_purged_vec = Vec::from_iter(snapshots_to_be_purged);

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

        Ok(())
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
