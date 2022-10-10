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
use std::sync::atomic::Ordering;
use std::sync::Arc;

use common_cache::Cache;
use common_catalog::table_context::TableContext;
use common_exception::ErrorCode;
use common_exception::Result;
use common_fuse_meta::caches::CacheManager;
use common_fuse_meta::meta::Location;
use common_fuse_meta::meta::SnapshotId;
use common_fuse_meta::meta::TableSnapshot;
use futures_util::StreamExt;
use opendal::Operator;
use tracing::warn;

use crate::fuse_segment::read_segments;
use crate::fuse_snapshot::read_snapshots;
use crate::FuseTable;

impl FuseTable {
    pub async fn do_gc(&self, ctx: &Arc<dyn TableContext>, keep_last_snapshot: bool) -> Result<()> {
        let r = self.read_table_snapshot(ctx.clone()).await;
        let snapshot_opt = match r {
            Err(e) if e.code() == ErrorCode::storage_not_found_code() => {
                // concurrent gc: someone else has already collected this snapshot, ignore it
                warn!(
                    "concurrent gc: snapshot {:?} already collected. table: {}, ident {}",
                    self.snapshot_loc(),
                    self.table_info.desc,
                    self.table_info.ident,
                );
                return Ok(());
            }
            Err(e) => return Err(e),
            Ok(v) => v,
        };

        let last_snapshot = if let Some(s) = snapshot_opt {
            s
        } else {
            // empty table, have nothing to do here
            return Ok(());
        };

        let (prev_id, prev_ver) = if let Some((id, ver)) = last_snapshot.prev_snapshot_id {
            (id, ver)
        } else {
            return if keep_last_snapshot {
                // short cut:
                // we do not have previous snapshot, and should keep the last snapshot, nothing to do
                Ok(())
            } else {
                // short cut:
                // - no previous snapshot
                // - no need to keep the last snapshot
                // just drop the whole snapshot,
                self.purge_blocks(ctx.clone(), &last_snapshot.segments, &HashSet::new())
                    .await?;

                let snapshots = vec![(last_snapshot.snapshot_id, self.snapshot_format_version())];
                self.purge_snapshots_and_segments(ctx.as_ref(), &last_snapshot.segments, &snapshots)
                    .await
            };
        };

        let prev_loc = self
            .meta_location_generator
            .snapshot_location_from_uuid(&prev_id, prev_ver)?;

        let mut snapshots_to_be_deleted: Vec<_> = Vec::new();
        if !keep_last_snapshot {
            snapshots_to_be_deleted
                .push((last_snapshot.snapshot_id, self.snapshot_format_version()));
        }

        let segments_referenced_by_gc_root: HashSet<Location> = if !keep_last_snapshot {
            //  segment gc root references nothing;
            HashSet::new()
        } else {
            // segment gc root contains all the segments referenced by pivot snapshot;
            HashSet::from_iter(last_snapshot.segments.clone())
        };

        // segments which no longer need to be kept
        let mut segments_to_be_deleted: HashSet<_> = HashSet::new();
        {
            if !keep_last_snapshot {
                segments_to_be_deleted.extend(last_snapshot.segments.clone())
            }

            // collects
            // - all the previous snapshots
            // - segments referenced by previous snapshots, but not by gc_root
            let mut snapshots = read_snapshots(
                ctx.clone(),
                prev_loc,
                prev_ver,
                self.meta_location_generator.clone(),
            )
            .await?;

            while let ss = snapshots
                .recv()
                .await
                .map_err(|e| ErrorCode::StorageOther(format!("read snapshots failure, {}", e)))?
            {
                snapshots_to_be_deleted.push((ss.snapshot_id, ss.format_version()));
                for seg in &ss.segments {
                    if !segments_referenced_by_gc_root.contains(seg) {
                        segments_to_be_deleted.insert(seg.clone());
                    }
                }
            }
        }

        let ref_by_gc_segment_locations = Vec::from_iter(segments_referenced_by_gc_root);
        let blocks_referenced_by_gc_root: HashSet<String> = self
            .get_block_locations(ctx.clone(), &ref_by_gc_segment_locations)
            .await?;

        // 1. purge un-referenced blocks
        let delete_segment_locations = Vec::from_iter(segments_to_be_deleted);
        self.purge_blocks(
            ctx.clone(),
            &delete_segment_locations,
            &blocks_referenced_by_gc_root,
        )
        .await?;

        // 2. purge ss and sg files
        self.purge_snapshots_and_segments(
            ctx.as_ref(),
            &delete_segment_locations,
            &snapshots_to_be_deleted,
        )
        .await
    }

    async fn get_block_locations(
        &self,
        ctx: Arc<dyn TableContext>,
        segment_locations: &[Location],
    ) -> Result<HashSet<String>> {
        let mut result = HashSet::new();

        let segments = read_segments(ctx, segment_locations).await?;
        for (idx, segment) in segments.iter().enumerate() {
            let segment = segment.clone();
            let segment_info = match segment {
                Err(e) if e.code() == ErrorCode::storage_not_found_code() => {
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
                result.insert(block_meta.location.0.clone());
            }
        }

        Ok(result)
    }

    // collect in the sense of GC
    async fn purge_snapshots_and_segments(
        &self,
        ctx: &dyn TableContext,
        segments_to_be_deleted: &[Location],
        snapshots_to_be_deleted: &[(SnapshotId, u64)],
    ) -> Result<()> {
        // order matters, should always remove the blocks first, segment 2nd, snapshot last,
        // so that if something goes wrong, e.g. process crashed, gc task can be "picked up" and continued

        let aborting = ctx.get_aborting();

        // 1. remove the segments
        for (loc, _v) in segments_to_be_deleted {
            if aborting.load(Ordering::Relaxed) {
                return Err(ErrorCode::AbortedQuery(
                    "Aborted query, because the server is shutting down or the query was killed.",
                ));
            }

            if let Some(c) = CacheManager::instance().get_table_segment_cache() {
                let cache = &mut *c.write();
                cache.pop(loc.as_str());
            }

            self.remove_file_by_location(&self.operator, loc.as_str())
                .await?;
        }

        let locs = self.meta_location_generator();
        // 2. remove the snapshots
        for (id, ver) in snapshots_to_be_deleted.iter().rev() {
            if aborting.load(Ordering::Relaxed) {
                return Err(ErrorCode::AbortedQuery(
                    "Aborted query, because the server is shutting down or the query was killed.",
                ));
            }

            let loc = locs.snapshot_location_from_uuid(id, *ver)?;
            if let Some(c) = CacheManager::instance().get_table_snapshot_cache() {
                let cache = &mut *c.write();
                cache.pop(loc.as_str());
            }

            self.remove_file_by_location(&self.operator, loc.as_str())
                .await?;
        }

        Ok(())
    }

    /// rm all the blocks, which are
    /// - referenced by any one of `segments`
    /// - but NOT referenced by `root`
    async fn purge_blocks(
        &self,
        ctx: Arc<dyn TableContext>,
        segment_locations: &[Location],
        root: &HashSet<String>,
    ) -> Result<()> {
        let accessor = &self.operator;
        let aborting = ctx.get_aborting();
        let segments = read_segments(ctx, segment_locations).await?;

        for segment in segments {
            let segment = segment?;

            if aborting.load(Ordering::Relaxed) {
                return Err(ErrorCode::AbortedQuery(
                    "Aborted query, because the server is shutting down or the query was killed.",
                ));
            }
            if aborting.load(Ordering::Relaxed) {
                return Err(ErrorCode::AbortedQuery(
                    "Aborted query, because the server is shutting down or the query was killed.",
                ));
            }

            for block_meta in &segment.blocks {
                if !root.contains(block_meta.location.0.as_str()) {
                    if aborting.load(Ordering::Relaxed) {
                        return Err(ErrorCode::AbortedQuery(
                            "Aborted query, because the server is shutting down or the query was killed.",
                        ));
                    }

                    if let Some(bloom_index_location) = &block_meta.bloom_filter_index_location {
                        let path = &bloom_index_location.0;
                        if let Some(c) = CacheManager::instance().get_bloom_index_meta_cache() {
                            let cache = &mut *c.write();
                            cache.pop(path);
                        }

                        self.remove_file_by_location(accessor, bloom_index_location.0.as_str())
                            .await?;
                    }

                    if aborting.load(Ordering::Relaxed) {
                        return Err(ErrorCode::AbortedQuery(
                            "Aborted query, because the server is shutting down or the query was killed.",
                        ));
                    }

                    self.remove_file_by_location(accessor, block_meta.location.0.as_str())
                        .await?;
                }
            }
        }

        Ok(())
    }

    async fn remove_file_by_location(
        &self,
        data_accessor: &Operator,
        block_location: impl AsRef<str>,
    ) -> Result<()> {
        match self
            .do_remove_file_by_location(data_accessor, block_location.as_ref())
            .await
        {
            Err(e) if e.code() == ErrorCode::storage_not_found_code() => {
                warn!(
                    "concurrent gc: block of location {} already collected. table: {}, ident {}",
                    block_location.as_ref(),
                    self.table_info.desc,
                    self.table_info.ident,
                );
                Ok(())
            }
            Err(e) => Err(e),
            Ok(_) => Ok(()),
        }
    }

    // make type checker happy
    #[inline]
    async fn do_remove_file_by_location(
        &self,
        data_accessor: &Operator,
        location: &str,
    ) -> Result<()> {
        Ok(data_accessor.object(location.as_ref()).delete().await?)
    }
}
