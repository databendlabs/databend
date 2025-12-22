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

use std::sync::Arc;

use databend_common_catalog::plan::Partitions;
use databend_common_catalog::table::CompactionLimits;
use databend_common_exception::Result;
use databend_common_expression::ComputedExpr;
use databend_common_expression::FieldIndex;
use databend_common_io::constants::DEFAULT_BLOCK_PER_SEGMENT;
use databend_storages_common_table_meta::meta::TableSnapshot;

use crate::FUSE_OPT_KEY_BLOCK_PER_SEGMENT;
use crate::FuseTable;
use crate::Table;
use crate::TableContext;
use crate::operations::mutation::BlockCompactMutator;
use crate::operations::mutation::SegmentCompactMutator;

#[derive(Clone)]
pub struct CompactOptions {
    // the snapshot that compactor working on, it never changed during phases compaction.
    pub base_snapshot: Arc<TableSnapshot>,
    pub block_per_seg: usize,
    pub num_segment_limit: Option<usize>,
    pub num_block_limit: Option<usize>,
}

impl FuseTable {
    #[async_backtrace::framed]
    pub(crate) async fn do_compact_segments(
        &self,
        ctx: Arc<dyn TableContext>,
        num_segment_limit: Option<usize>,
    ) -> Result<()> {
        let compact_options = if let Some(v) = self
            .compact_options_with_segment_limit(num_segment_limit)
            .await?
        {
            v
        } else {
            return Ok(());
        };

        let table_meta_timestamps =
            ctx.get_table_meta_timestamps(self, Some(compact_options.base_snapshot.clone()))?;

        let mut segment_compactor = SegmentCompactMutator::try_create(
            ctx.clone(),
            compact_options,
            self.meta_location_generator().clone(),
            self.operator.clone(),
            self.cluster_key_id(),
            table_meta_timestamps,
        )?;

        if !segment_compactor.target_select().await? {
            return Ok(());
        }

        segment_compactor.try_commit(self).await
    }

    #[async_backtrace::framed]
    pub(crate) async fn do_compact_blocks(
        &self,
        ctx: Arc<dyn TableContext>,
        limits: CompactionLimits,
    ) -> Result<Option<(Partitions, Arc<TableSnapshot>)>> {
        let compact_options = if let Some(v) = self
            .compact_options(limits.segment_limit, limits.block_limit)
            .await?
        {
            v
        } else {
            return Ok(None);
        };

        let thresholds = self.get_block_thresholds();
        let mut mutator = BlockCompactMutator::new(
            ctx.clone(),
            thresholds,
            compact_options,
            self.operator.clone(),
            self.cluster_key_id(),
        );

        let partitions = mutator.target_select().await?;
        if partitions.is_empty() {
            return Ok(None);
        }

        Ok(Some((
            partitions,
            mutator.compact_params.base_snapshot.clone(),
        )))
    }

    async fn compact_options_with_segment_limit(
        &self,
        num_segment_limit: Option<usize>,
    ) -> Result<Option<CompactOptions>> {
        self.compact_options(num_segment_limit, None).await
    }

    #[async_backtrace::framed]
    async fn compact_options(
        &self,
        num_segment_limit: Option<usize>,
        num_block_limit: Option<usize>,
    ) -> Result<Option<CompactOptions>> {
        let snapshot_opt = self.read_table_snapshot().await?;
        let base_snapshot = if let Some(val) = snapshot_opt {
            val
        } else {
            // no snapshot, no compaction.
            return Ok(None);
        };

        if base_snapshot.summary.block_count <= 1 {
            return Ok(None);
        }

        let block_per_seg =
            self.get_option(FUSE_OPT_KEY_BLOCK_PER_SEGMENT, DEFAULT_BLOCK_PER_SEGMENT);

        Ok(Some(CompactOptions {
            base_snapshot,
            block_per_seg,
            num_segment_limit,
            num_block_limit,
        }))
    }

    pub fn all_column_indices(&self) -> Vec<FieldIndex> {
        self.schema_with_stream()
            .fields()
            .iter()
            .enumerate()
            .filter(|(_, f)| !matches!(f.computed_expr(), Some(ComputedExpr::Virtual(_))))
            .map(|(i, _)| i)
            .collect::<Vec<FieldIndex>>()
    }
}
