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

use std::collections::HashSet;
use std::sync::Arc;

use common_base::runtime::Runtime;
use common_catalog::plan::Partitions;
use common_catalog::plan::PartitionsShuffleKind;
use common_catalog::plan::Projection;
use common_exception::ErrorCode;
use common_exception::Result;
use common_expression::ColumnId;
use common_pipeline_core::processors::processor::ProcessorPtr;
use common_pipeline_transforms::processors::transforms::AsyncAccumulatingTransformer;
use common_sql::executor::MutationKind;
use storages_common_table_meta::meta::TableSnapshot;

use crate::operations::common::TableMutationAggregator;
use crate::operations::common::TransformSerializeBlock;
use crate::operations::mutation::BlockCompactMutator;
use crate::operations::mutation::CompactLazyPartInfo;
use crate::operations::mutation::CompactSource;
use crate::operations::mutation::SegmentCompactMutator;
use crate::pipelines::Pipeline;
use crate::FuseTable;
use crate::Table;
use crate::TableContext;
use crate::DEFAULT_BLOCK_PER_SEGMENT;
use crate::FUSE_OPT_KEY_BLOCK_PER_SEGMENT;

#[derive(Clone)]
pub struct CompactOptions {
    // the snapshot that compactor working on, it never changed during phases compaction.
    pub base_snapshot: Arc<TableSnapshot>,
    pub block_per_seg: usize,
    pub limit: Option<usize>,
}

impl FuseTable {
    #[async_backtrace::framed]
    pub(crate) async fn do_compact_segments(
        &self,
        ctx: Arc<dyn TableContext>,
        limit: Option<usize>,
    ) -> Result<()> {
        let compact_options = if let Some(v) = self.compact_options(limit).await? {
            v
        } else {
            return Ok(());
        };

        let mut segment_mutator = SegmentCompactMutator::try_create(
            ctx.clone(),
            compact_options,
            self.meta_location_generator().clone(),
            self.operator.clone(),
            self.cluster_key_id(),
        )?;

        if !segment_mutator.target_select().await? {
            return Ok(());
        }

        segment_mutator.try_commit(Arc::new(self.clone())).await
    }

    #[async_backtrace::framed]
    pub(crate) async fn do_compact_blocks(
        &self,
        ctx: Arc<dyn TableContext>,
        limit: Option<usize>,
    ) -> Result<Option<(Partitions, Arc<TableSnapshot>)>> {
        let compact_options = if let Some(v) = self.compact_options(limit).await? {
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

    pub fn build_compact_source(
        &self,
        ctx: Arc<dyn TableContext>,
        parts: Partitions,
        column_ids: HashSet<ColumnId>,
        pipeline: &mut Pipeline,
    ) -> Result<()> {
        let is_lazy = parts.is_lazy;
        let thresholds = self.get_block_thresholds();
        let cluster_key_id = self.cluster_key_id();
        let mut max_threads = ctx.get_settings().get_max_threads()? as usize;
        if is_lazy {
            let query_ctx = ctx.clone();

            let lazy_parts = parts
                .partitions
                .into_iter()
                .map(|v| {
                    v.as_any()
                        .downcast_ref::<CompactLazyPartInfo>()
                        .unwrap()
                        .clone()
                })
                .collect::<Vec<_>>();

            pipeline.set_on_init(move || {
                let ctx = query_ctx.clone();
                let column_ids = column_ids.clone();
                let partitions = Runtime::with_worker_threads(2, None)?.block_on(async move {
                    let partitions = BlockCompactMutator::build_compact_tasks(
                        ctx.clone(),
                        column_ids,
                        cluster_key_id,
                        thresholds,
                        lazy_parts,
                    )
                    .await?;

                    Result::<_, ErrorCode>::Ok(partitions)
                })?;

                let partitions = Partitions::create_nolazy(PartitionsShuffleKind::Mod, partitions);
                query_ctx.set_partitions(partitions)?;
                Ok(())
            });
        } else {
            max_threads = max_threads.min(parts.len()).max(1);
            ctx.set_partitions(parts)?;
        }

        let all_column_indices = self.all_column_indices();
        let projection = Projection::Columns(all_column_indices);
        let block_reader = self.create_block_reader(projection, false, ctx.clone())?;
        // Add source pipe.
        pipeline.add_source(
            |output| {
                CompactSource::try_create(
                    ctx.clone(),
                    self.storage_format,
                    block_reader.clone(),
                    output,
                )
            },
            max_threads,
        )?;

        // sort
        let cluster_stats_gen =
            self.cluster_gen_for_append(ctx.clone(), pipeline, thresholds, None)?;
        pipeline.add_transform(
            |input: Arc<common_pipeline_core::processors::port::InputPort>, output| {
                let proc = TransformSerializeBlock::try_create(
                    ctx.clone(),
                    input,
                    output,
                    self,
                    cluster_stats_gen.clone(),
                )?;
                proc.into_processor()
            },
        )?;

        if is_lazy {
            pipeline.try_resize(1)?;
            pipeline.add_transform(|input, output| {
                let mutation_aggregator =
                    TableMutationAggregator::new(self, ctx.clone(), vec![], MutationKind::Compact);
                Ok(ProcessorPtr::create(AsyncAccumulatingTransformer::create(
                    input,
                    output,
                    mutation_aggregator,
                )))
            })?;
        }
        Ok(())
    }

    #[async_backtrace::framed]
    async fn compact_options(&self, limit: Option<usize>) -> Result<Option<CompactOptions>> {
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
            limit,
        }))
    }
}
