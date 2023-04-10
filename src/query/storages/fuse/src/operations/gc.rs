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

use common_catalog::table::Table;
use common_catalog::table_context::TableContext;
use common_exception::ErrorCode;
use common_exception::Result;
use storages_common_cache::CacheAccessor;
use storages_common_cache::LoadParams;
use storages_common_cache_manager::CachedObject;
use storages_common_index::BloomIndexMeta;
use storages_common_table_meta::meta::Location;
use storages_common_table_meta::meta::SegmentInfo;
use storages_common_table_meta::meta::TableSnapshot;
use storages_common_table_meta::meta::TableSnapshotStatistics;
use tracing::info;
use tracing::warn;

use crate::io::Files;
use crate::io::MetaReaders;
use crate::io::SegmentsIO;
use crate::io::SnapshotsIO;
use crate::io::TableMetaLocationGenerator;
use crate::FuseTable;

impl FuseTable {
    #[async_backtrace::framed]
    pub async fn do_purge(
        &self,
        ctx: &Arc<dyn TableContext>,
        keep_last_snapshot: bool,
    ) -> Result<()> {
        let root_snapshot_location_op = self.snapshot_loc().await?;
        if root_snapshot_location_op.is_none() {
            return Ok(());
        }

        let root_snapshot_location = root_snapshot_location_op.unwrap();
        let reader = MetaReaders::table_snapshot_reader(self.get_operator());
        let ver = TableMetaLocationGenerator::snapshot_version(root_snapshot_location.as_str());
        let params = LoadParams {
            location: root_snapshot_location.clone(),
            len_hint: None,
            ver,
            put_cache: true,
        };
        let root_snapshot = match reader.read(&params).await {
            Err(e) if e.code() == ErrorCode::STORAGE_NOT_FOUND => {
                // concurrent gc: someone else has already collected this snapshot, ignore it
                warn!(
                    "concurrent gc: snapshot {:?} already collected. table: {}, ident {}",
                    root_snapshot_location, self.table_info.desc, self.table_info.ident,
                );
                return Ok(());
            }
            Err(e) => return Err(e),
            Ok(v) => v,
        };
        let root_snapshot_ts = root_snapshot.timestamp;
        let root_ts_location_opt = root_snapshot.table_statistics_location.clone();
        let locations_referenced_by_root = self
            .get_block_locations(ctx.clone(), &root_snapshot.segments, true)
            .await?;
        let segments_referenced_by_root = HashSet::from_iter(root_snapshot.segments.clone());
        let segments_excluded = Arc::new(segments_referenced_by_root);
        drop(root_snapshot);

        let snapshots_io = SnapshotsIO::create(
            ctx.clone(),
            self.operator.clone(),
            self.snapshot_format_version().await?,
        );

        // List all the snapshot file paths
        // note that snapshot file paths of ongoing txs might be included
        let mut snapshot_files = vec![];
        if let Some(prefix) = SnapshotsIO::get_s3_prefix_from_file(&root_snapshot_location) {
            snapshot_files = snapshots_io.list_files(&prefix, None).await?;
        }

        let chunk_size = ctx.get_settings().get_max_storage_io_requests()? as usize;
        let location_gen = self.meta_location_generator();
        let mut count = 0;
        let mut remain_snapshots = Vec::new();
        let mut counter = PurgeCounter::new();
        for chunk in snapshot_files.chunks(chunk_size).rev() {
            let results = snapshots_io
                .read_snapshot_lite_extends(
                    chunk,
                    root_snapshot_ts,
                    segments_excluded.clone(),
                    root_ts_location_opt.clone(),
                )
                .await?;
            let mut snapshots: Vec<_> = results.into_iter().filter_map(|r| r.ok()).collect();
            if snapshots.is_empty() {
                continue;
            }
            snapshots.extend(std::mem::take(&mut remain_snapshots));
            snapshots.sort_by(|a, b| b.timestamp.cmp(&a.timestamp));

            let base_segments = snapshots[0].segments.clone();
            let base_timestamp = snapshots[0].timestamp;
            let base_ts_location_opt = snapshots[0].table_statistics_location.clone();

            let mut snapshots_to_be_purged = HashSet::new();
            let mut segments_to_be_purged = HashSet::new();
            let mut ts_to_be_purged = HashSet::new();
            for s in snapshots.into_iter() {
                if s.timestamp >= base_timestamp {
                    remain_snapshots.push(s);
                    continue;
                }

                let diff: HashSet<_> = s.segments.difference(&base_segments).cloned().collect();
                segments_to_be_purged.extend(diff);
                if let Ok(loc) =
                    location_gen.snapshot_location_from_uuid(&s.snapshot_id, s.format_version)
                {
                    snapshots_to_be_purged.insert(loc);
                }

                if s.table_statistics_location.is_some()
                    && s.table_statistics_location != base_ts_location_opt
                {
                    ts_to_be_purged.insert(s.table_statistics_location.unwrap());
                }
            }

            // Refresh status.
            {
                count += chunk.len();
                let status = format!(
                    "gc: read snapshot files:{}/{}, cost:{} sec",
                    count,
                    snapshot_files.len(),
                    counter.start.elapsed().as_secs()
                );
                info!(status);
                ctx.set_status_info(&status);
            }

            self.partial_purge(
                ctx,
                &mut counter,
                segments_to_be_purged,
                HashSet::new(),
                &locations_referenced_by_root,
                ts_to_be_purged,
                snapshots_to_be_purged,
            )
            .await?;
        }

        let mut remain_snapshots_to_be_purged = HashSet::new();
        let mut remain_segments_to_be_purged = HashSet::new();
        let mut remain_ts_to_be_purged = HashSet::new();
        let mut root_segments_to_be_purged = HashSet::new();
        for s in remain_snapshots.into_iter() {
            if let Ok(loc) =
                location_gen.snapshot_location_from_uuid(&s.snapshot_id, s.format_version)
            {
                remain_snapshots_to_be_purged.insert(loc);
            }
            remain_segments_to_be_purged.extend(s.segments);
            if let Some(ts) = s.table_statistics_location {
                remain_ts_to_be_purged.insert(ts);
            }
        }

        if !keep_last_snapshot {
            remain_snapshots_to_be_purged.insert(root_snapshot_location);
            if let Some(ts) = root_ts_location_opt {
                remain_ts_to_be_purged.insert(ts);
            }
            root_segments_to_be_purged = segments_excluded
                .as_ref()
                .iter()
                .map(|loc| loc.0.clone())
                .collect();
        }

        if !remain_snapshots_to_be_purged.is_empty() {
            self.partial_purge(
                ctx,
                &mut counter,
                remain_segments_to_be_purged,
                root_segments_to_be_purged,
                &locations_referenced_by_root,
                remain_ts_to_be_purged,
                remain_snapshots_to_be_purged,
            )
            .await?;
        }

        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
    async fn partial_purge(
        &self,
        ctx: &Arc<dyn TableContext>,
        counter: &mut PurgeCounter,
        segments_to_be_purged: HashSet<Location>,
        root_segments_to_be_purged: HashSet<String>,
        locations_referenced_by_root: &LocationTuple,
        ts_to_be_purged: HashSet<String>,
        snapshots_to_be_purged: HashSet<String>,
    ) -> Result<()> {
        let chunk_size = ctx.get_settings().get_max_storage_io_requests()? as usize;
        let keep_last_snapshot = root_segments_to_be_purged.is_empty();

        // 1. Purge segments&blocks by chunk size
        {
            let segment_locations = Vec::from_iter(segments_to_be_purged);
            for chunk in segment_locations.chunks(chunk_size) {
                let locations = self.get_block_locations(ctx.clone(), chunk, false).await?;

                let mut blocks_to_be_purged = HashSet::new();
                for loc in &locations.block_location {
                    if keep_last_snapshot
                        && locations_referenced_by_root.block_location.contains(loc)
                    {
                        continue;
                    }
                    blocks_to_be_purged.insert(loc.to_string());
                }

                let mut blooms_to_be_purged = HashSet::new();
                for loc in &locations.bloom_location {
                    if keep_last_snapshot
                        && locations_referenced_by_root.bloom_location.contains(loc)
                    {
                        continue;
                    }
                    blooms_to_be_purged.insert(loc.to_string());
                }

                let segment_locations_to_be_purged = HashSet::from_iter(
                    chunk
                        .iter()
                        .map(|loc| loc.0.clone())
                        .collect::<Vec<String>>(),
                );

                self.purge_block_segments(
                    ctx,
                    counter,
                    blocks_to_be_purged,
                    blooms_to_be_purged,
                    segment_locations_to_be_purged,
                )
                .await?;
            }
        }

        // 2. Purge root segments&blocks.
        if !keep_last_snapshot {
            self.purge_block_segments(
                ctx,
                counter,
                locations_referenced_by_root.block_location.clone(),
                locations_referenced_by_root.bloom_location.clone(),
                root_segments_to_be_purged,
            )
            .await?;
        }

        // 3. Purge table statistic files
        if !ts_to_be_purged.is_empty() {
            counter.table_statistics += ts_to_be_purged.len();
            self.try_purge_location_files_and_cache::<TableSnapshotStatistics>(
                ctx.clone(),
                ts_to_be_purged,
            )
            .await?;
        }

        // 4. Purge snapshots.
        if !snapshots_to_be_purged.is_empty() {
            counter.snapshots += snapshots_to_be_purged.len();
            self.try_purge_location_files_and_cache::<TableSnapshot>(
                ctx.clone(),
                snapshots_to_be_purged,
            )
            .await?;
        }

        // 5. Refresh status.
        {
            let status = format!(
                "gc: block files purged:{}, bloom files purged:{}, segment files purged:{}, table statistic files purged:{}, snapshots purged:{}, take:{} sec",
                counter.blocks,
                counter.blooms,
                counter.segments,
                counter.table_statistics,
                counter.snapshots,
                counter.start.elapsed().as_secs()
            );
            ctx.set_status_info(&status);
            info!(status);
        }
        Ok(())
    }

    async fn purge_block_segments(
        &self,
        ctx: &Arc<dyn TableContext>,
        counter: &mut PurgeCounter,
        blocks_to_be_purged: HashSet<String>,
        blooms_to_be_purged: HashSet<String>,
        segments_to_be_purged: HashSet<String>,
    ) -> Result<()> {
        // 1. Try to purge block file chunks.
        counter.blocks += blocks_to_be_purged.len();
        self.try_purge_location_files(ctx.clone(), blocks_to_be_purged)
            .await?;

        // 2. Try to purge bloom index file chunks.
        counter.blooms += blooms_to_be_purged.len();
        self.try_purge_location_files_and_cache::<BloomIndexMeta>(ctx.clone(), blooms_to_be_purged)
            .await?;

        // 3. Try to purge segment file chunks.
        counter.segments += segments_to_be_purged.len();
        self.try_purge_location_files_and_cache::<SegmentInfo>(ctx.clone(), segments_to_be_purged)
            .await
    }

    // Purge file by location chunks.
    #[async_backtrace::framed]
    async fn try_purge_location_files(
        &self,
        ctx: Arc<dyn TableContext>,
        locations_to_be_purged: HashSet<String>,
    ) -> Result<()> {
        let fuse_file = Files::create(ctx.clone(), self.operator.clone());
        let locations = Vec::from_iter(locations_to_be_purged);
        fuse_file.remove_file_in_batch(&locations).await
    }

    // Purge file by location chunks.
    #[async_backtrace::framed]
    async fn try_purge_location_files_and_cache<T>(
        &self,
        ctx: Arc<dyn TableContext>,
        locations_to_be_purged: HashSet<String>,
    ) -> Result<()>
    where
        T: CachedObject<T>,
    {
        if let Some(cache) = T::cache() {
            for loc in locations_to_be_purged.iter() {
                cache.evict(loc);
            }
        }
        self.try_purge_location_files(ctx, locations_to_be_purged)
            .await
    }

    #[async_backtrace::framed]
    async fn get_block_locations(
        &self,
        ctx: Arc<dyn TableContext>,
        segment_locations: &[Location],
        put_cache: bool,
    ) -> Result<LocationTuple> {
        let mut blocks = HashSet::new();
        let mut blooms = HashSet::new();

        let fuse_segments = SegmentsIO::create(ctx.clone(), self.operator.clone(), self.schema());
        let results = fuse_segments
            .read_segments_into::<LocationTuple>(segment_locations, put_cache)
            .await?;
        for (idx, location_tuple) in results.into_iter().enumerate() {
            let location_tuple = match location_tuple {
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
            blocks.extend(location_tuple.block_location.into_iter());
            blooms.extend(location_tuple.bloom_location.into_iter());
        }

        Ok(LocationTuple {
            block_location: blocks,
            bloom_location: blooms,
        })
    }
}

#[derive(Default)]
struct LocationTuple {
    block_location: HashSet<String>,
    bloom_location: HashSet<String>,
}

impl From<Arc<SegmentInfo>> for LocationTuple {
    fn from(value: Arc<SegmentInfo>) -> Self {
        let mut block_location = HashSet::new();
        let mut bloom_location = HashSet::new();
        for block_meta in &value.blocks {
            block_location.insert(block_meta.location.0.clone());
            if let Some(bloom_loc) = &block_meta.bloom_filter_index_location {
                bloom_location.insert(bloom_loc.0.clone());
            }
        }
        Self {
            block_location,
            bloom_location,
        }
    }
}

struct PurgeCounter {
    start: Instant,
    blocks: usize,
    blooms: usize,
    segments: usize,
    table_statistics: usize,
    snapshots: usize,
}

impl PurgeCounter {
    fn new() -> Self {
        Self {
            start: Instant::now(),
            blocks: 0,
            blooms: 0,
            segments: 0,
            table_statistics: 0,
            snapshots: 0,
        }
    }
}
