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
use std::collections::HashSet;
use std::sync::Arc;
use std::time::Instant;

use databend_common_catalog::table::Table;
use databend_common_catalog::table_context::TableContext;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::ScalarRef;
use databend_common_meta_app::schema::ListIndexesByIdReq;
use databend_common_meta_app::schema::TableIndex;
use databend_storages_common_cache::CacheAccessor;
use databend_storages_common_cache::CachedObject;
use databend_storages_common_cache::LoadParams;
use databend_storages_common_index::BloomIndexMeta;
use databend_storages_common_index::InvertedIndexFile;
use databend_storages_common_index::InvertedIndexMeta;
use databend_storages_common_io::Files;
use databend_storages_common_table_meta::meta::column_oriented_segment::ColumnOrientedSegment;
use databend_storages_common_table_meta::meta::column_oriented_segment::BLOOM_FILTER_INDEX_LOCATION;
use databend_storages_common_table_meta::meta::column_oriented_segment::LOCATION;
use databend_storages_common_table_meta::meta::CompactSegmentInfo;
use databend_storages_common_table_meta::meta::Location;
use databend_storages_common_table_meta::meta::SegmentInfo;
use databend_storages_common_table_meta::meta::TableSnapshot;
use databend_storages_common_table_meta::meta::TableSnapshotStatistics;
use log::error;
use log::info;
use log::warn;

use crate::io::read::ColumnOrientedSegmentReader;
use crate::io::read::RowOrientedSegmentReader;
use crate::io::InvertedIndexReader;
use crate::io::MetaReaders;
use crate::io::SegmentsIO;
use crate::io::SnapshotLiteExtended;
use crate::io::SnapshotsIO;
use crate::io::TableMetaLocationGenerator;
use crate::FuseTable;
use crate::FUSE_TBL_SNAPSHOT_PREFIX;

impl FuseTable {
    pub async fn do_purge(
        &self,
        ctx: &Arc<dyn TableContext>,
        snapshot_files: Vec<String>,
        num_snapshot_limit: Option<usize>,
        keep_last_snapshot: bool,
        dry_run: bool,
    ) -> Result<Option<Vec<String>>> {
        let mut counter = PurgeCounter::new();
        let res = self
            .execute_purge(
                ctx,
                snapshot_files,
                num_snapshot_limit,
                keep_last_snapshot,
                &mut counter,
                dry_run,
            )
            .await;
        info!("purge counter {:?}", counter);
        res
    }

    #[async_backtrace::framed]
    async fn execute_purge(
        &self,
        ctx: &Arc<dyn TableContext>,
        snapshot_files: Vec<String>,
        num_snapshot_limit: Option<usize>,
        keep_last_snapshot: bool,
        counter: &mut PurgeCounter,
        dry_run: bool,
    ) -> Result<Option<Vec<String>>> {
        // 1. Read the root snapshot.
        let root_snapshot_info_opt = self.read_root_snapshot(ctx, keep_last_snapshot).await?;
        if root_snapshot_info_opt.is_none() {
            if dry_run {
                return Ok(Some(vec![]));
            } else {
                return Ok(None);
            }
        }

        let root_snapshot_info = root_snapshot_info_opt.unwrap();

        if root_snapshot_info.snapshot_lite.timestamp.is_none() {
            return Err(ErrorCode::StorageOther(format!(
                "gc: snapshot timestamp is none, snapshot location: {}",
                root_snapshot_info.snapshot_location
            )));
        }

        let snapshots_io = SnapshotsIO::create(ctx.clone(), self.operator.clone());
        let location_gen = self.meta_location_generator();
        let purged_snapshot_limit = num_snapshot_limit.unwrap_or(snapshot_files.len());

        let mut read_snapshot_count = 0;
        let mut remain_snapshots = Vec::<SnapshotLiteExtended>::new();
        let mut dry_run_purge_files = vec![];
        let mut purged_snapshot_count = 0;

        let catalog = ctx.get_catalog(&ctx.get_current_catalog()).await?;
        let table_agg_index_ids = catalog
            .list_index_ids_by_table_id(ListIndexesByIdReq::new(ctx.get_tenant(), self.get_id()))
            .await?;

        let inverted_indexes = &self.table_info.meta.indexes;

        // 2. Read snapshot fields by chunk size.
        let chunk_size = ctx.get_settings().get_max_threads()? as usize * 4;
        for chunk in snapshot_files.chunks(chunk_size).rev() {
            if let Err(err) = ctx.check_aborting() {
                error!(
                    "gc: aborted query, because the server is shutting down or the query was killed. table: {}, ident {}",
                    self.table_info.desc, self.table_info.ident,
                );
                return Err(err.with_context("failed to read snapshot"));
            }

            let results = snapshots_io
                .read_snapshot_lite_extends(chunk, root_snapshot_info.snapshot_lite.clone(), false)
                .await?;
            let mut snapshots: Vec<_> = results.into_iter().flatten().collect();
            if snapshots.is_empty() {
                break;
            }
            // Gather the remain snapshots.
            snapshots.extend(std::mem::take(&mut remain_snapshots));
            // Sort snapshot by timestamp.
            snapshots.sort_by(|a, b| a.timestamp.cmp(&b.timestamp));

            // Set the last snapshot as base snapshot, extend the base snapshot.
            let base_snapshot = snapshots.pop().unwrap();
            let base_segments = base_snapshot.segments.clone();
            let base_timestamp = base_snapshot.timestamp;
            let base_ts_location_opt = base_snapshot.table_statistics_location.clone();
            remain_snapshots.push(base_snapshot);

            let mut snapshots_to_be_purged = HashSet::new();
            let mut segments_to_be_purged = HashSet::new();
            let mut ts_to_be_purged = HashSet::new();
            for s in snapshots.into_iter() {
                if s.timestamp.is_some() && s.timestamp >= base_timestamp {
                    remain_snapshots.push(s);
                    continue;
                }

                if let Ok(loc) =
                    location_gen.snapshot_location_from_uuid(&s.snapshot_id, s.format_version)
                {
                    if purged_snapshot_count >= purged_snapshot_limit {
                        break;
                    }
                    snapshots_to_be_purged.insert(loc);
                    purged_snapshot_count += 1;
                }

                let diff: HashSet<_> = s.segments.difference(&base_segments).cloned().collect();
                segments_to_be_purged.extend(diff);

                if s.table_statistics_location.is_some()
                    && s.table_statistics_location != base_ts_location_opt
                {
                    ts_to_be_purged.insert(s.table_statistics_location.unwrap());
                }
            }

            // Refresh status.
            {
                read_snapshot_count += chunk.len();
                let status = format!(
                    "gc: read snapshot files:{}/{}, cost:{:?}",
                    read_snapshot_count,
                    snapshot_files.len(),
                    counter.start.elapsed()
                );
                ctx.set_status_info(&status);
            }

            if !snapshots_to_be_purged.is_empty() {
                if dry_run {
                    debug_assert!(num_snapshot_limit.is_some());
                    self.dry_run_purge(
                        ctx,
                        &mut dry_run_purge_files,
                        &root_snapshot_info.referenced_locations,
                        segments_to_be_purged,
                        ts_to_be_purged,
                        snapshots_to_be_purged,
                        &table_agg_index_ids,
                    )
                    .await?;

                    if dry_run_purge_files.len() >= num_snapshot_limit.unwrap() {
                        return Ok(Some(dry_run_purge_files));
                    }
                } else {
                    self.partial_purge(
                        ctx,
                        counter,
                        &root_snapshot_info.referenced_locations,
                        segments_to_be_purged,
                        ts_to_be_purged,
                        snapshots_to_be_purged,
                        &table_agg_index_ids,
                        inverted_indexes,
                    )
                    .await?;

                    if purged_snapshot_count >= purged_snapshot_limit {
                        return Ok(None);
                    }
                }
            }
        }

        if !remain_snapshots.is_empty() {
            let mut snapshots_to_be_purged = HashSet::new();
            let mut segments_to_be_purged = HashSet::new();
            let mut ts_to_be_purged = HashSet::new();
            for s in remain_snapshots {
                if let Ok(loc) =
                    location_gen.snapshot_location_from_uuid(&s.snapshot_id, s.format_version)
                {
                    if purged_snapshot_count >= purged_snapshot_limit {
                        break;
                    }
                    snapshots_to_be_purged.insert(loc);
                    purged_snapshot_count += 1;
                }

                segments_to_be_purged.extend(s.segments);

                if s.table_statistics_location.is_some() {
                    ts_to_be_purged.insert(s.table_statistics_location.unwrap());
                }
            }
            if dry_run {
                self.dry_run_purge(
                    ctx,
                    &mut dry_run_purge_files,
                    &root_snapshot_info.referenced_locations,
                    segments_to_be_purged,
                    ts_to_be_purged,
                    snapshots_to_be_purged,
                    &table_agg_index_ids,
                )
                .await?;
            } else {
                self.partial_purge(
                    ctx,
                    counter,
                    &root_snapshot_info.referenced_locations,
                    segments_to_be_purged,
                    ts_to_be_purged,
                    snapshots_to_be_purged,
                    &table_agg_index_ids,
                    inverted_indexes,
                )
                .await?;
            }
        }

        if dry_run {
            return Ok(Some(dry_run_purge_files));
        }

        // 3. purge root snapshots
        if !keep_last_snapshot {
            self.purge_root_snapshot(
                ctx,
                counter,
                root_snapshot_info.snapshot_lite,
                root_snapshot_info.referenced_locations,
                root_snapshot_info.snapshot_location,
                &table_agg_index_ids,
                inverted_indexes,
            )
            .await?;
        }

        Ok(None)
    }

    async fn read_root_snapshot(
        &self,
        ctx: &Arc<dyn TableContext>,
        put_cache: bool,
    ) -> Result<Option<RootSnapshotInfo>> {
        let root_snapshot_location_op = self.snapshot_loc();
        if root_snapshot_location_op.is_none() {
            return Ok(None);
        }

        let snapshot_location = root_snapshot_location_op.unwrap();
        let reader = MetaReaders::table_snapshot_reader(self.get_operator());
        let ver = TableMetaLocationGenerator::snapshot_version(snapshot_location.as_str());
        let params = LoadParams {
            location: snapshot_location.clone(),
            len_hint: None,
            ver,
            put_cache,
        };
        let root_snapshot = match reader.read(&params).await {
            Err(e) if e.code() == ErrorCode::STORAGE_NOT_FOUND => {
                // concurrent gc: someone else has already collected this snapshot, ignore it
                warn!(
                    "concurrent gc: snapshot {:?} already collected. table: {}, ident {}",
                    snapshot_location, self.table_info.desc, self.table_info.ident,
                );
                return Ok(None);
            }
            Err(e) => return Err(e),
            Ok(v) => v,
        };

        // root snapshot cannot ignore storage not find error.
        let referenced_locations = self
            .get_block_locations(ctx.clone(), &root_snapshot.segments, put_cache, false)
            .await?;
        let snapshot_lite = Arc::new(SnapshotLiteExtended {
            format_version: ver,
            snapshot_id: root_snapshot.snapshot_id,
            timestamp: root_snapshot.timestamp,
            segments: HashSet::from_iter(root_snapshot.segments.clone()),
            table_statistics_location: root_snapshot.table_statistics_location.clone(),
        });
        Ok(Some(RootSnapshotInfo {
            snapshot_location,
            referenced_locations,
            snapshot_lite,
        }))
    }

    #[allow(clippy::too_many_arguments)]
    async fn dry_run_purge(
        &self,
        ctx: &Arc<dyn TableContext>,
        purge_files: &mut Vec<String>,
        locations_referenced_by_root: &LocationTuple,
        segments_to_be_purged: HashSet<Location>,
        ts_to_be_purged: HashSet<String>,
        snapshots_to_be_purged: HashSet<String>,
        table_agg_index_ids: &[u64],
    ) -> Result<()> {
        let chunk_size = ctx.get_settings().get_max_threads()? as usize * 4;
        // Purge segments&blocks by chunk size
        let segment_locations = Vec::from_iter(segments_to_be_purged);
        for chunk in segment_locations.chunks(chunk_size) {
            // since we are purging files, the ErrorCode::STORAGE_NOT_FOUND error can be safely ignored.
            let locations = self
                .get_block_locations(ctx.clone(), chunk, false, true)
                .await?;

            for loc in &locations.block_location {
                if locations_referenced_by_root.block_location.contains(loc) {
                    continue;
                }
                purge_files.push(loc.to_string());
                for index_id in table_agg_index_ids {
                    purge_files.push(
                        TableMetaLocationGenerator::gen_agg_index_location_from_block_location(
                            loc, *index_id,
                        ),
                    )
                }
            }

            for loc in &locations.bloom_location {
                if locations_referenced_by_root.bloom_location.contains(loc) {
                    continue;
                }
                purge_files.push(loc.to_string())
            }

            purge_files.extend(chunk.iter().map(|loc| loc.0.clone()));
        }
        purge_files.extend(ts_to_be_purged.iter().map(|loc| loc.to_string()));
        purge_files.extend(snapshots_to_be_purged.iter().map(|loc| loc.to_string()));

        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
    async fn partial_purge(
        &self,
        ctx: &Arc<dyn TableContext>,
        counter: &mut PurgeCounter,
        locations_referenced_by_root: &LocationTuple,
        segments_to_be_purged: HashSet<Location>,
        ts_to_be_purged: HashSet<String>,
        snapshots_to_be_purged: HashSet<String>,
        table_agg_index_ids: &[u64],
        inverted_indexes: &BTreeMap<String, TableIndex>,
    ) -> Result<()> {
        let chunk_size = ctx.get_settings().get_max_threads()? as usize * 4;
        // Purge segments&blocks by chunk size
        let mut count = 0;
        let segment_locations = Vec::from_iter(segments_to_be_purged);
        for chunk in segment_locations.chunks(chunk_size) {
            // since we are purging files, the ErrorCode::STORAGE_NOT_FOUND error can be safely ignored.
            let locations = self
                .get_block_locations(ctx.clone(), chunk, false, true)
                .await?;

            let mut blocks_to_be_purged = HashSet::new();
            let mut agg_indexes_to_be_purged = HashSet::new();
            let mut inverted_indexes_to_be_purged = HashSet::new();
            for loc in &locations.block_location {
                if locations_referenced_by_root.block_location.contains(loc) {
                    continue;
                }
                blocks_to_be_purged.insert(loc.to_string());
                for index_id in table_agg_index_ids {
                    agg_indexes_to_be_purged.insert(
                        TableMetaLocationGenerator::gen_agg_index_location_from_block_location(
                            loc, *index_id,
                        ),
                    );
                }

                for idx in inverted_indexes.values() {
                    inverted_indexes_to_be_purged.insert(
                        TableMetaLocationGenerator::gen_inverted_index_location_from_block_location(
                            loc,
                            idx.name.as_str(),
                            idx.version.as_str(),
                        ),
                    );
                }
            }

            let mut blooms_to_be_purged = HashSet::new();
            for loc in &locations.bloom_location {
                if locations_referenced_by_root.bloom_location.contains(loc) {
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

            // Refresh status.
            {
                count += chunk.len();
                let status = format!(
                    "gc: read purged segment files:{}/{}, cost:{:?}",
                    count,
                    segment_locations.len(),
                    counter.start.elapsed()
                );
                ctx.set_status_info(&status);
            }

            self.purge_block_segments(
                ctx,
                counter,
                blocks_to_be_purged,
                agg_indexes_to_be_purged,
                inverted_indexes_to_be_purged,
                blooms_to_be_purged,
                segment_locations_to_be_purged,
            )
            .await?;
        }

        self.purge_ts_snapshots(ctx, counter, ts_to_be_purged, snapshots_to_be_purged)
            .await
    }

    async fn purge_root_snapshot(
        &self,
        ctx: &Arc<dyn TableContext>,
        counter: &mut PurgeCounter,
        root_snapshot: Arc<SnapshotLiteExtended>,
        root_location_tuple: LocationTuple,
        root_snapshot_location: String,
        table_agg_index_ids: &[u64],
        inverted_indexes: &BTreeMap<String, TableIndex>,
    ) -> Result<()> {
        let segment_locations_to_be_purged = HashSet::from_iter(
            root_snapshot
                .segments
                .iter()
                .map(|loc| loc.0.clone())
                .collect::<Vec<_>>(),
        );

        let mut agg_indexes_to_be_purged = HashSet::new();
        let mut inverted_indexes_to_be_purged = HashSet::new();
        for index_id in table_agg_index_ids {
            agg_indexes_to_be_purged.extend(root_location_tuple.block_location.iter().map(|loc| {
                TableMetaLocationGenerator::gen_agg_index_location_from_block_location(
                    loc, *index_id,
                )
            }));
        }

        // Collect the inverted index files accompanying blocks
        // NOTE:  For a block, and one index of it, there might be multiple inverted index files,
        // such as, different versions of same (in the sense of name) inverted index.
        // we do not handle this one block multiple inverted indexes case now.
        for idx in inverted_indexes.values() {
            inverted_indexes_to_be_purged.extend(root_location_tuple.block_location.iter().map(
                |loc| {
                    TableMetaLocationGenerator::gen_inverted_index_location_from_block_location(
                        loc,
                        idx.name.as_str(),
                        idx.version.as_str(),
                    )
                },
            ));
        }

        self.purge_block_segments(
            ctx,
            counter,
            root_location_tuple.block_location,
            agg_indexes_to_be_purged,
            inverted_indexes_to_be_purged,
            root_location_tuple.bloom_location,
            segment_locations_to_be_purged,
        )
        .await?;

        let mut ts_to_be_purged = HashSet::new();
        if let Some(ts) = root_snapshot.table_statistics_location.clone() {
            ts_to_be_purged.insert(ts);
        }
        self.purge_ts_snapshots(
            ctx,
            counter,
            ts_to_be_purged,
            HashSet::from([root_snapshot_location]),
        )
        .await
    }

    async fn purge_block_segments(
        &self,
        ctx: &Arc<dyn TableContext>,
        counter: &mut PurgeCounter,
        blocks_to_be_purged: HashSet<String>,
        agg_indexes_to_be_purged: HashSet<String>,
        inverted_indexes_to_be_purged: HashSet<String>,
        blooms_to_be_purged: HashSet<String>,
        segments_to_be_purged: HashSet<String>,
    ) -> Result<()> {
        // 1. Try to purge block file chunks.
        let blocks_count = blocks_to_be_purged.len();
        if blocks_count > 0 {
            counter.blocks += blocks_count;
            self.try_purge_location_files(ctx.clone(), blocks_to_be_purged)
                .await?;
        }

        let agg_index_count = agg_indexes_to_be_purged.len();
        if agg_index_count > 0 {
            counter.agg_indexes += agg_index_count;
            self.try_purge_location_files(ctx.clone(), agg_indexes_to_be_purged)
                .await?;
        }

        let inverted_index_count = inverted_indexes_to_be_purged.len();
        if inverted_index_count > 0 {
            counter.inverted_indexes += inverted_index_count;

            // if there is inverted index file cache, evict the cached items
            if let Some(inverted_index_cache) = InvertedIndexFile::cache() {
                for index_path in &inverted_indexes_to_be_purged {
                    InvertedIndexReader::cache_key_of_index_columns(index_path)
                        .iter()
                        .for_each(|cache_key| {
                            inverted_index_cache.evict(cache_key);
                        })
                }
            }

            self.try_purge_location_files_and_cache::<InvertedIndexMeta, _>(
                ctx.clone(),
                inverted_indexes_to_be_purged,
            )
            .await?;
        }

        // 2. Try to purge bloom index file chunks.
        let blooms_count = blooms_to_be_purged.len();
        if blooms_count > 0 {
            counter.blooms += blooms_count;
            self.try_purge_location_files_and_cache::<BloomIndexMeta, _>(
                ctx.clone(),
                blooms_to_be_purged,
            )
            .await?;
        }

        // 3. Try to purge segment file chunks.
        let segments_count = segments_to_be_purged.len();
        if segments_count > 0 {
            counter.segments += segments_count;
            self.try_purge_location_files_and_cache::<SegmentInfo, _>(
                ctx.clone(),
                segments_to_be_purged,
            )
            .await?;
        }
        Ok(())
    }

    async fn purge_ts_snapshots(
        &self,
        ctx: &Arc<dyn TableContext>,
        counter: &mut PurgeCounter,
        ts_to_be_purged: HashSet<String>,
        snapshots_to_be_purged: HashSet<String>,
    ) -> Result<()> {
        // 3. Purge table statistic files
        let ts_count = ts_to_be_purged.len();
        if ts_count > 0 {
            counter.table_statistics += ts_count;
            self.try_purge_location_files_and_cache::<TableSnapshotStatistics, _>(
                ctx.clone(),
                ts_to_be_purged,
            )
            .await?;
        }

        // 4. Purge snapshots.
        let snapshots_count = snapshots_to_be_purged.len();
        if snapshots_count > 0 {
            counter.snapshots += snapshots_count;
            self.try_purge_location_files_and_cache::<TableSnapshot, _>(
                ctx.clone(),
                snapshots_to_be_purged,
            )
            .await?;
        }

        // 5. Refresh status.
        {
            let status = format!(
                "gc: block files purged:{}, bloom files purged:{}, segment files purged:{}, table statistic files purged:{}, snapshots purged:{}, take:{:?}",
                counter.blocks,
                counter.blooms,
                counter.segments,
                counter.table_statistics,
                counter.snapshots,
                counter.start.elapsed()
            );
            ctx.set_status_info(&status);
        }
        Ok(())
    }

    // Purge file by location chunks.
    #[async_backtrace::framed]
    pub async fn try_purge_location_files(
        &self,
        ctx: Arc<dyn TableContext>,
        locations_to_be_purged: HashSet<String>,
    ) -> Result<()> {
        let fuse_file = Files::create(ctx.clone(), self.operator.clone());
        fuse_file.remove_file_in_batch(locations_to_be_purged).await
    }

    // Purge file by location chunks.
    #[async_backtrace::framed]
    pub async fn try_purge_location_files_and_cache<T, C>(
        &self,
        ctx: Arc<dyn TableContext>,
        locations_to_be_purged: HashSet<String>,
    ) -> Result<()>
    where
        T: CachedObject<C>,
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
    pub async fn get_block_locations(
        &self,
        ctx: Arc<dyn TableContext>,
        segment_locations: &[Location],
        put_cache: bool,
        ignore_err: bool,
    ) -> Result<LocationTuple> {
        let mut blocks = HashSet::new();
        let mut blooms = HashSet::new();

        let fuse_segments = SegmentsIO::create(ctx.clone(), self.operator.clone(), self.schema());
        let chunk_size = ctx.get_settings().get_max_threads()? as usize * 4;
        let mut projection = HashSet::new();
        projection.insert(LOCATION.to_string());
        projection.insert(BLOOM_FILTER_INDEX_LOCATION.to_string());
        for chunk in segment_locations.chunks(chunk_size) {
            let results = match self.is_column_oriented() {
                true => {
                    let segments = fuse_segments
                        .generic_read_compact_segments::<ColumnOrientedSegmentReader>(
                            chunk,
                            put_cache,
                            &projection,
                        )
                        .await?;
                    let mut results = Vec::new();
                    for segment in segments {
                        match segment {
                            Ok(segment) => match LocationTuple::try_from(segment) {
                                Ok(location_tuple) => results.push(Ok(location_tuple)),
                                Err(e) => results.push(Err(e)),
                            },
                            Err(e) => results.push(Err(e)),
                        }
                    }
                    results
                }
                false => {
                    let segments = fuse_segments
                        .generic_read_compact_segments::<RowOrientedSegmentReader>(
                            chunk,
                            put_cache,
                            &projection,
                        )
                        .await?;
                    let mut results = Vec::new();
                    for segment in segments {
                        match segment {
                            Ok(segment) => match LocationTuple::try_from(segment) {
                                Ok(location_tuple) => results.push(Ok(location_tuple)),
                                Err(e) => results.push(Err(e)),
                            },
                            Err(e) => results.push(Err(e)),
                        }
                    }
                    results
                }
            };
            for (idx, location_tuple) in results.into_iter().enumerate() {
                let location_tuple = match location_tuple {
                    Err(e) if e.code() == ErrorCode::STORAGE_NOT_FOUND && ignore_err => {
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
        }

        Ok(LocationTuple {
            block_location: blocks,
            bloom_location: blooms,
        })
    }

    pub async fn list_snapshot_files(&self) -> Result<Vec<String>> {
        let prefix = format!(
            "{}/{}/",
            self.meta_location_generator().prefix(),
            FUSE_TBL_SNAPSHOT_PREFIX,
        );
        SnapshotsIO::list_files(self.get_operator(), &prefix, None).await
    }
}

struct RootSnapshotInfo {
    snapshot_location: String,
    referenced_locations: LocationTuple,
    snapshot_lite: Arc<SnapshotLiteExtended>,
}

#[derive(Default)]
pub struct LocationTuple {
    pub block_location: HashSet<String>,
    pub bloom_location: HashSet<String>,
}

impl TryFrom<Arc<CompactSegmentInfo>> for LocationTuple {
    type Error = ErrorCode;
    fn try_from(value: Arc<CompactSegmentInfo>) -> Result<Self> {
        let mut block_location = HashSet::new();
        let mut bloom_location = HashSet::new();
        let block_metas = value.block_metas()?;
        for block_meta in block_metas.into_iter() {
            block_location.insert(block_meta.location.0.clone());
            if let Some(bloom_loc) = &block_meta.bloom_filter_index_location {
                bloom_location.insert(bloom_loc.0.clone());
            }
        }
        Ok(Self {
            block_location,
            bloom_location,
        })
    }
}

impl TryFrom<Arc<ColumnOrientedSegment>> for LocationTuple {
    type Error = ErrorCode;
    fn try_from(value: Arc<ColumnOrientedSegment>) -> Result<Self> {
        let mut block_location = HashSet::new();
        let mut bloom_location = HashSet::new();

        let location_path = value.location_path_col();
        for path in location_path.iter() {
            block_location.insert(path.to_string());
        }

        let (index, _) = value
            .segment_schema
            .column_with_name(BLOOM_FILTER_INDEX_LOCATION)
            .unwrap();
        let column = value
            .block_metas
            .get_by_offset(index)
            .to_column(value.block_metas.num_rows());
        for value in column.iter() {
            match value {
                ScalarRef::Null => {}
                ScalarRef::Tuple(values) => {
                    let path = values[0].as_string().unwrap();
                    bloom_location.insert(path.to_string());
                }
                _ => unreachable!(),
            }
        }

        Ok(Self {
            block_location,
            bloom_location,
        })
    }
}

#[derive(Debug)]
struct PurgeCounter {
    start: Instant,
    blocks: usize,
    agg_indexes: usize,
    inverted_indexes: usize,
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
            agg_indexes: 0,
            inverted_indexes: 0,
            blooms: 0,
            segments: 0,
            table_statistics: 0,
            snapshots: 0,
        }
    }
}
