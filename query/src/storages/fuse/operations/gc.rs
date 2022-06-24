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
//

use std::collections::HashSet;
use std::sync::Arc;

use common_cache::Cache;
use common_exception::ErrorCode;
use common_exception::Result;
use common_tracing::tracing::warn;
use futures::TryStreamExt;
use opendal::Operator;

use crate::sessions::QueryContext;
use crate::storages::fuse::io::MetaReaders;
use crate::storages::fuse::meta::Location;
use crate::storages::fuse::meta::SnapshotId;
use crate::storages::fuse::FuseTable;

impl FuseTable {
    pub async fn do_gc(&self, ctx: &Arc<QueryContext>, keep_last_snapshot: bool) -> Result<()> {
        let r = self.read_table_snapshot(ctx.as_ref()).await;
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

        let reader = MetaReaders::table_snapshot_reader(ctx.as_ref());

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
                let snapshots = vec![(last_snapshot.snapshot_id, self.snapshot_format_version())];
                let segments = HashSet::from_iter(last_snapshot.segments.clone());
                self.purge_blocks(ctx, segments.iter(), &HashSet::new())
                    .await?;
                self.collect(ctx.as_ref(), segments, snapshots).await
            };
        };

        let prev_loc = self
            .meta_location_generator
            .snapshot_location_from_uuid(&prev_id, prev_ver)?;

        let mut snapshot_history =
            reader.snapshot_history(prev_loc, prev_ver, self.meta_location_generator.clone());

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
            while let Some(s) = snapshot_history.try_next().await? {
                snapshots_to_be_deleted.push((s.snapshot_id, s.format_version()));
                for seg in &s.segments {
                    if !segments_referenced_by_gc_root.contains(seg) {
                        segments_to_be_deleted.insert(seg.clone());
                    }
                }
            }
        }

        let blocks_referenced_by_gc_root: HashSet<String> = self
            .blocks_of(ctx, segments_referenced_by_gc_root.iter())
            .await?;

        // removed un-referenced blocks
        self.purge_blocks(
            ctx,
            segments_to_be_deleted.iter(),
            &blocks_referenced_by_gc_root,
        )
        .await?;

        self.collect(
            ctx.as_ref(),
            segments_to_be_deleted,
            snapshots_to_be_deleted,
        )
        .await
    }

    async fn blocks_of(
        &self,
        ctx: &QueryContext,
        segments: impl Iterator<Item = &Location>,
    ) -> Result<HashSet<String>> {
        let mut result = HashSet::new();
        let reader = MetaReaders::segment_info_reader(ctx);
        for l in segments {
            let (segment_location, ver) = l;
            let r = reader.read(segment_location, None, *ver).await;
            let segment_info = match r {
                Err(e) if e.code() == ErrorCode::storage_not_found_code() => {
                    // concurrent gc: someone else has already collected this segment, ignore it
                    warn!(
                        "concurrent gc: segment of location {} already collected. table: {}, ident {}",
                        segment_location,
                        self.table_info.desc,
                        self.table_info.ident,
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

    /// rm all the blocks, which are
    /// - referenced by any one of `segments`
    /// - but NOT referenced by `root`
    async fn purge_blocks(
        &self,
        ctx: &QueryContext,
        segments: impl Iterator<Item = &Location>,
        root: &HashSet<String>,
    ) -> Result<()> {
        let reader = MetaReaders::segment_info_reader(ctx);
        let accessor = ctx.get_storage_operator()?;
        for l in segments {
            let (x, ver) = l;
            let res = reader.read(x, None, *ver).await?;
            for block_meta in &res.blocks {
                if !root.contains(block_meta.location.0.as_str()) {
                    self.remove_location(&accessor, block_meta.location.0.as_str())
                        .await?;
                }
            }
        }
        Ok(())
    }

    async fn remove_location(
        &self,
        data_accessor: &Operator,
        location: impl AsRef<str>,
    ) -> Result<()> {
        match self
            .do_remove_location(data_accessor, location.as_ref())
            .await
        {
            Err(e) if e.code() == ErrorCode::storage_not_found_code() => {
                warn!(
                    "concurrent gc: block of location {} already collected. table: {}, ident {}",
                    location.as_ref(),
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
    async fn do_remove_location(&self, data_accessor: &Operator, location: &str) -> Result<()> {
        Ok(data_accessor.object(location.as_ref()).delete().await?)
    }

    // collect in the sense of GC
    async fn collect(
        &self,
        ctx: &QueryContext,
        segments_to_be_deleted: HashSet<Location>,
        snapshots_to_be_deleted: Vec<(SnapshotId, u64)>,
    ) -> Result<()> {
        let accessor = ctx.get_storage_operator()?;

        // order matters, should always remove the blocks first, segment 2nd, snapshot last,
        // so that if something goes wrong, e.g. process crashed, gc task can be "picked up" and continued

        // 1. remove the segments
        for (x, _v) in segments_to_be_deleted {
            self.remove_location(&accessor, x.as_str()).await?;
            if let Some(c) = ctx.get_storage_cache_manager().get_table_segment_cache() {
                let cache = &mut *c.write().await;
                cache.pop(x.as_str());
            }
        }

        let locs = self.meta_location_generator();
        // 2. remove the snapshots
        for (id, ver) in snapshots_to_be_deleted.iter().rev() {
            let loc = locs.snapshot_location_from_uuid(id, *ver)?;
            self.remove_location(&accessor, loc.as_str()).await?;
            if let Some(c) = ctx.get_storage_cache_manager().get_table_snapshot_cache() {
                let cache = &mut *c.write().await;
                cache.pop(loc.as_str());
            }
        }

        Ok(())
    }
}
