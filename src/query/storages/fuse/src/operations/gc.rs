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

use chrono::DateTime;
use chrono::Utc;
use databend_common_catalog::table::Table;
use databend_common_catalog::table_context::TableContext;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::ScalarRef;
use databend_common_meta_app::schema::ListIndexesByIdReq;
use databend_common_meta_app::schema::SnapshotRef;
use databend_common_meta_app::schema::SnapshotRefType;
use databend_common_meta_app::schema::TableIndex;
use databend_storages_common_cache::CacheAccessor;
use databend_storages_common_cache::CachedObject;
use databend_storages_common_index::BloomIndexMeta;
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
use futures::TryStreamExt;
use log::error;
use log::info;
use log::warn;
use opendal::Entry;

use crate::index::InvertedIndexFile;
use crate::io::read::ColumnOrientedSegmentReader;
use crate::io::read::RowOrientedSegmentReader;
use crate::io::read::SnapshotHistoryReader;
use crate::io::InvertedIndexReader;
use crate::io::MetaReaders;
use crate::io::SegmentsIO;
use crate::io::SnapshotLiteExtended;
use crate::io::SnapshotsIO;
use crate::io::TableMetaLocationGenerator;
use crate::FuseTable;
use crate::RetentionPolicy;
use crate::FUSE_TBL_SNAPSHOT_PREFIX;

const DEFAULT_REF_NUM_SNAPSHOT_LIMIT: usize = 100;

impl FuseTable {
    pub async fn do_purge(
        &self,
        ctx: &Arc<dyn TableContext>,
        snapshot_files: Vec<String>,
        num_snapshot_limit: Option<usize>,
        dry_run: bool,
    ) -> Result<Option<Vec<String>>> {
        let mut counter = PurgeCounter::new();

        // Step 1: Process snapshot refs (branches and tags) before main purge
        let ref_protected_segments = self
            .process_refs_for_purge(ctx, &mut counter, dry_run)
            .await?;

        let res = self
            .execute_purge(
                ctx,
                snapshot_files,
                num_snapshot_limit,
                &mut counter,
                dry_run,
                ref_protected_segments,
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
        counter: &mut PurgeCounter,
        dry_run: bool,
        ref_protected_segments: HashSet<Location>,
    ) -> Result<Option<Vec<String>>> {
        // 1. Read the root snapshot.
        let root_snapshot_info_opt = self.read_root_snapshot(ctx, ref_protected_segments).await?;
        if root_snapshot_info_opt.is_none() {
            return if dry_run { Ok(Some(vec![])) } else { Ok(None) };
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

        Ok(None)
    }

    async fn read_root_snapshot(
        &self,
        ctx: &Arc<dyn TableContext>,
        ref_protected_segments: HashSet<Location>,
    ) -> Result<Option<RootSnapshotInfo>> {
        let root_snapshot_location_op = self.snapshot_loc();
        if root_snapshot_location_op.is_none() {
            return Ok(None);
        }

        let snapshot_location = root_snapshot_location_op.unwrap();
        let Some(root_snapshot) =
            SnapshotsIO::read_snapshot_for_vacuum(self.get_operator(), &snapshot_location).await?
        else {
            return Ok(None);
        };

        // root snapshot cannot ignore storage not find error.
        let mut segments =
            HashSet::with_capacity(ref_protected_segments.len() + root_snapshot.segments.len());
        segments.extend(ref_protected_segments);
        segments.extend(root_snapshot.segments.clone());
        let segment_refs: Vec<&Location> = segments.iter().collect();
        let referenced_locations = self
            .get_block_locations(ctx.clone(), &segment_refs, true, false)
            .await?;
        let snapshot_lite = Arc::new(SnapshotLiteExtended {
            format_version: root_snapshot.format_version,
            snapshot_id: root_snapshot.snapshot_id,
            timestamp: root_snapshot.timestamp,
            segments,
            table_statistics_location: root_snapshot.table_statistics_location(),
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
            let chunk_refs: Vec<&Location> = chunk.iter().collect();
            let locations = self
                .get_block_locations(ctx.clone(), &chunk_refs, false, true)
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
            let chunk_refs: Vec<&Location> = chunk.iter().collect();
            let locations = self
                .get_block_locations(ctx.clone(), &chunk_refs, false, true)
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

            let mut stats_to_be_purged = HashSet::new();
            for loc in &locations.hll_location {
                if locations_referenced_by_root.hll_location.contains(loc) {
                    continue;
                }
                stats_to_be_purged.insert(loc.to_string());
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
                stats_to_be_purged,
                segment_locations_to_be_purged,
            )
            .await?;
        }

        self.purge_ts_snapshots(ctx, counter, ts_to_be_purged, snapshots_to_be_purged)
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
        stats_to_be_purged: HashSet<String>,
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

        // 3. Try to purge segment statistic file chunks.
        let stats_count = stats_to_be_purged.len();
        if stats_count > 0 {
            counter.hlls += stats_count;
            self.try_purge_location_files(ctx.clone(), stats_to_be_purged)
                .await?;
        }

        // 4. Try to purge segment file chunks.
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
                "gc: block files purged:{}, bloom files purged:{}, segment stats files purged:{}, segment files purged:{}, table statistic files purged:{}, snapshots purged:{}, take:{:?}",
                counter.blocks,
                counter.blooms,
                counter.hlls,
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
        segment_locations: &[&Location],
        put_cache: bool,
        ignore_err: bool,
    ) -> Result<LocationTuple> {
        let mut blocks = HashSet::new();
        let mut blooms = HashSet::new();
        let mut hlls = HashSet::new();

        let fuse_segments = SegmentsIO::create(ctx.clone(), self.operator.clone(), self.schema());
        let chunk_size = ctx.get_settings().get_max_threads()? as usize * 4;
        let projection = HashSet::from([
            LOCATION.to_string(),
            BLOOM_FILTER_INDEX_LOCATION.to_string(),
        ]);
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
                        let location = chunk[idx];
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
                hlls.extend(location_tuple.hll_location.into_iter());
            }
        }

        Ok(LocationTuple {
            block_location: blocks,
            bloom_location: blooms,
            hll_location: hlls,
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

    /// Design note:
    /// Branches are vacuumed using a timestamp-based retention policy to simplify data lifecycle management.
    /// For inactive branches, the snapshot root timestamp may remain very old.
    /// If we apply snapshot-count based cleanup, this old timestamp could unnecessarily retain data
    pub fn get_refs_retention_policy(
        &self,
        ctx: &dyn TableContext,
        now: DateTime<Utc>,
    ) -> Result<(DateTime<Utc>, usize)> {
        // referenced by the main branch and delay garbage collection.
        let retention_policy = self.get_data_retention_policy(ctx)?;
        let (delta_duration, num_snapshots_to_keep) = match retention_policy {
            RetentionPolicy::ByNumOfSnapshotsToKeep(n) => {
                let duration = self.get_data_retention_period(ctx)?;
                (duration, n)
            }
            RetentionPolicy::ByTimePeriod(delta_duration) => {
                (delta_duration, DEFAULT_REF_NUM_SNAPSHOT_LIMIT)
            }
        };
        let retention_time = now - delta_duration;
        Ok((retention_time, num_snapshots_to_keep))
    }

    /// List snapshots for branch with fallback strategy:
    /// 1. First try to list by timestamp (retention_time)
    /// 2. If empty, fallback to list all and truncate by num_snapshots_to_keep
    #[async_backtrace::framed]
    pub async fn list_branch_snapshots_with_fallback(
        &self,
        branch_id: u64,
        head: &str,
        retention_time: DateTime<Utc>,
        num_snapshots_to_keep: usize,
    ) -> Result<Vec<Entry>> {
        let ref_snapshot_location_prefix = self
            .meta_location_generator()
            .ref_snapshot_location_prefix();
        let ref_prefix = format!("{}{}/", ref_snapshot_location_prefix, branch_id);
        // First attempt: list by timestamp
        let mut snapshots = self
            .list_files_until_timestamp(&ref_prefix, retention_time, true, None)
            .await?;

        // If no snapshots found by timestamp, fallback to count-based strategy
        let len = snapshots.len();
        if len == 0 {
            snapshots = self
                .list_files_until_prefix(&ref_prefix, head, true, None)
                .await?;
            if len > num_snapshots_to_keep {
                let num_candidates = len - num_snapshots_to_keep + 2;
                snapshots.truncate(num_candidates);
            } else {
                snapshots.clear();
            }
        }
        Ok(snapshots)
    }

    /// Find the earliest snapshot via snapshot history traversal
    #[async_backtrace::framed]
    pub async fn find_earliest_snapshot_via_history(
        &self,
        ref_name: &str,
        snapshot_ref: &SnapshotRef,
    ) -> Result<Arc<TableSnapshot>> {
        let head_location = &snapshot_ref.loc;
        let snapshot_version = TableMetaLocationGenerator::snapshot_version(head_location);
        let reader = MetaReaders::table_snapshot_reader(self.get_operator());
        let mut snapshot_stream = reader.snapshot_history(
            head_location.to_string(),
            snapshot_version,
            self.meta_location_generator().clone(),
            Some(snapshot_ref.id),
        );

        let mut last_snapshot = None;
        while let Some((snapshot, _version)) = snapshot_stream.try_next().await? {
            last_snapshot = Some(snapshot);
        }

        last_snapshot.ok_or_else(|| {
            ErrorCode::Internal(format!(
                "Failed to find any snapshot in history for branch {}",
                ref_name
            ))
        })
    }

    /// Process gc_root from last snapshot and collect snapshots to purge
    ///
    /// Returns gc_root snapshot if found
    async fn select_branch_gc_root(
        &self,
        branch_id: u64,
        snapshots_before_retention: &[Entry],
        ref_snapshots_to_purge: &mut Vec<String>,
    ) -> Result<Option<Arc<TableSnapshot>>> {
        if snapshots_before_retention.len() < 2 {
            return Ok(None);
        }

        let last_snapshot_path = snapshots_before_retention.last().unwrap().path();
        let op = self.get_operator();
        let (last_snapshot, _) =
            SnapshotsIO::read_snapshot(last_snapshot_path.to_string(), op.clone(), false).await?;

        // Get its prev_snapshot_id as gc_root
        let Some((gc_root_id, gc_root_ver)) = last_snapshot.prev_snapshot_id else {
            return Ok(None);
        };
        let gc_root_path = self
            .meta_location_generator()
            .ref_snapshot_location_from_uuid(branch_id, &gc_root_id, gc_root_ver)?;
        // Try to read gc_root snapshot
        match SnapshotsIO::read_snapshot(gc_root_path.clone(), op.clone(), false).await {
            Ok((gc_root_snap, _)) => {
                // Collect snapshots_to_purge
                let mut gc_candidates = Vec::with_capacity(snapshots_before_retention.len());
                for snapshot in snapshots_before_retention.iter() {
                    gc_candidates.push(snapshot.path().to_owned());
                }

                // Find gc_root position in candidates
                let gc_root_idx = gc_candidates.binary_search(&gc_root_path).map_err(|_| {
                    ErrorCode::Internal(format!(
                        "gc root path {} should be one of the candidates, candidates: {:?}",
                        gc_root_path, gc_candidates
                    ))
                })?;
                ref_snapshots_to_purge.extend_from_slice(&gc_candidates[..gc_root_idx]);

                Ok(Some(gc_root_snap))
            }
            Err(e) => {
                // Log the error but continue processing
                warn!(
                    "Failed to read gc_root snapshot at {}: {}, using anchor instead",
                    gc_root_path, e
                );
                Ok(None)
            }
        }
    }

    /// Process snapshot refs (branches and tags) for purge.
    /// Return the protected segments from ref gc roots (tags and branches).
    #[async_backtrace::framed]
    async fn process_refs_for_purge(
        &self,
        ctx: &Arc<dyn TableContext>,
        counter: &mut PurgeCounter,
        dry_run: bool,
    ) -> Result<HashSet<Location>> {
        let now = Utc::now();
        let table_info = self.get_table_info();
        let op = self.get_operator();
        let (retention_time, num_snapshots_to_keep) =
            self.get_refs_retention_policy(ctx.as_ref(), now)?;

        let mut ref_protected_segments = HashSet::new();
        let mut ref_snapshots_to_purge = Vec::new();
        let mut expired_refs = HashSet::new();

        // First pass: process refs, identify expired refs, and collect anchor updates
        for (ref_name, snapshot_ref) in table_info.meta.refs.iter() {
            // Check if ref is expired
            if snapshot_ref.expire_at.is_some_and(|v| v < now) {
                expired_refs.insert(ref_name);
                continue;
            }

            match &snapshot_ref.typ {
                SnapshotRefType::Tag => {
                    // Tag: read head snapshot as gc root to protect its segments
                    let (tag_snapshot, _) =
                        SnapshotsIO::read_snapshot(snapshot_ref.loc.clone(), op.clone(), true)
                            .await?;

                    // Collect segments from tag
                    for seg_loc in &tag_snapshot.segments {
                        ref_protected_segments.insert(seg_loc.clone());
                    }
                }
                SnapshotRefType::Branch => {
                    let branch_id = snapshot_ref.id;
                    let snapshots_before_lvt = self
                        .list_branch_snapshots_with_fallback(
                            branch_id,
                            &snapshot_ref.loc,
                            retention_time,
                            num_snapshots_to_keep,
                        )
                        .await?;

                    let gc_root_snap = if let Some(gc_root_snap) = self
                        .select_branch_gc_root(
                            branch_id,
                            &snapshots_before_lvt,
                            &mut ref_snapshots_to_purge,
                        )
                        .await?
                    {
                        gc_root_snap
                    } else {
                        self.find_earliest_snapshot_via_history(ref_name, snapshot_ref)
                            .await?
                    };
                    // Collect segments from gc_root
                    for seg_loc in &gc_root_snap.segments {
                        ref_protected_segments.insert(seg_loc.clone());
                    }
                }
            }
        }

        if dry_run {
            return Ok(ref_protected_segments);
        }

        // Cleanup expired ref directories
        if !expired_refs.is_empty() {
            let _ = self.update_table_refs_meta(ctx, &expired_refs).await?;
        }

        // Purge ref snapshots if not dry_run
        if !ref_snapshots_to_purge.is_empty() {
            counter.snapshots += ref_snapshots_to_purge.len();
            let fuse_file = Files::create(ctx.clone(), op);
            fuse_file
                .remove_file_in_batch(ref_snapshots_to_purge)
                .await?;
        }

        Ok(ref_protected_segments)
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
    pub hll_location: HashSet<String>,
}

impl TryFrom<Arc<CompactSegmentInfo>> for LocationTuple {
    type Error = ErrorCode;
    fn try_from(value: Arc<CompactSegmentInfo>) -> Result<Self> {
        let mut block_location = HashSet::new();
        let mut bloom_location = HashSet::new();
        let mut hll_location = HashSet::new();
        let block_metas = value.block_metas()?;
        for block_meta in block_metas.into_iter() {
            block_location.insert(block_meta.location.0.clone());
            if let Some(bloom_loc) = &block_meta.bloom_filter_index_location {
                bloom_location.insert(bloom_loc.0.clone());
            }
        }
        if let Some(loc) = value.as_ref().summary.additional_stats_loc() {
            hll_location.insert(loc.0);
        }
        Ok(Self {
            block_location,
            bloom_location,
            hll_location,
        })
    }
}

impl TryFrom<Arc<ColumnOrientedSegment>> for LocationTuple {
    type Error = ErrorCode;
    fn try_from(value: Arc<ColumnOrientedSegment>) -> Result<Self> {
        let mut block_location = HashSet::new();
        let mut bloom_location = HashSet::new();
        let mut hll_location = HashSet::new();

        let location_path = value.location_path_col();
        for path in location_path.iter() {
            block_location.insert(path.to_string());
        }

        let (index, _) = value
            .segment_schema
            .column_with_name(BLOOM_FILTER_INDEX_LOCATION)
            .unwrap();
        let column = value.block_metas.get_by_offset(index).to_column();
        for value in column.iter() {
            if let ScalarRef::Tuple(values) = value {
                let path = values[0].as_string().unwrap();
                bloom_location.insert(path.to_string());
            }
        }

        if let Some(loc) = value.as_ref().summary.additional_stats_loc() {
            hll_location.insert(loc.0);
        }
        Ok(Self {
            block_location,
            bloom_location,
            hll_location,
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
    hlls: usize,
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
            hlls: 0,
            segments: 0,
            table_statistics: 0,
            snapshots: 0,
        }
    }
}
