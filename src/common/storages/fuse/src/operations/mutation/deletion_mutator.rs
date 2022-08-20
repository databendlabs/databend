//  Copyright 2022 Datafuse Labs.
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

use std::sync::Arc;

use common_catalog::table_context::TableContext;
use common_datablocks::DataBlock;
use common_exception::Result;
use common_fuse_meta::caches::CacheManager;
use common_fuse_meta::meta::BlockMeta;
use common_fuse_meta::meta::ClusterStatistics;
use common_fuse_meta::meta::Location;
use common_fuse_meta::meta::TableSnapshot;

use crate::io::BlockWriter;
use crate::io::TableMetaLocationGenerator;
use crate::operations::mutation::BaseMutator;
use crate::statistics::ClusterStatsGenerator;

pub enum Deletion {
    NothingDeleted,
    Remains(DataBlock),
}

pub struct DeletionMutator {
    base_mutator: BaseMutator,
    cluster_stats_gen: ClusterStatsGenerator,
}

impl DeletionMutator {
    pub fn try_create(
        ctx: Arc<dyn TableContext>,
        location_generator: TableMetaLocationGenerator,
        base_snapshot: Arc<TableSnapshot>,
        cluster_stats_gen: ClusterStatsGenerator,
    ) -> Result<Self> {
        let base_mutator = BaseMutator::try_create(ctx, location_generator, base_snapshot)?;
        Ok(Self {
            base_mutator,
            cluster_stats_gen,
        })
    }

    pub async fn into_new_snapshot(self) -> Result<(TableSnapshot, String)> {
        let (segments, summary) = self.base_mutator.generate_segments().await?;
        self.base_mutator.into_new_snapshot(segments, summary).await
    }

    /// Records the replacements:  
    ///  the block located at `block_location` of segment indexed by `seg_idx` with a new block
    pub async fn replace_with(
        &mut self,
        seg_idx: usize,
        location_of_block_to_be_replaced: Location,
        origin_stats: Option<ClusterStatistics>,
        replace_with: DataBlock,
    ) -> Result<()> {
        // write new block, and keep the mutations
        let new_block_meta = if replace_with.num_rows() == 0 {
            None
        } else {
            let block_writer = BlockWriter::new(
                &self.base_mutator.ctx,
                &self.base_mutator.data_accessor,
                &self.base_mutator.location_generator,
            );
            let cluster_stats = self
                .cluster_stats_gen
                .gen_with_origin_stats(&replace_with, origin_stats)?;
            Some(block_writer.write(replace_with, cluster_stats).await?)
        };
        let original_block_loc = location_of_block_to_be_replaced;
        self.base_mutator
            .add_mutation(seg_idx, original_block_loc, new_block_meta);
        Ok(())
    }
}
