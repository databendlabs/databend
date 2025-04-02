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

use databend_common_base::runtime::Runtime;
use databend_common_catalog::plan::PartInfoType;
use databend_common_catalog::plan::Partitions;
use databend_common_catalog::plan::PartitionsShuffleKind;
use databend_common_catalog::plan::Projection;
use databend_common_catalog::table::Table;
use databend_common_catalog::table_context::TableContext;
use databend_common_exception::Result;
use databend_common_pipeline_sources::EmptySource;
use databend_common_pipeline_sources::PrefetchAsyncSourcer;
use databend_common_pipeline_transforms::processors::TransformPipelineHelper;
use databend_common_sql::executor::physical_plans::CompactSource as PhysicalCompactSource;
use databend_common_sql::executor::physical_plans::MutationKind;
use databend_common_sql::StreamContext;
use databend_common_storages_fuse::operations::BlockCompactMutator;
use databend_common_storages_fuse::operations::CompactLazyPartInfo;
use databend_common_storages_fuse::operations::CompactSource;
use databend_common_storages_fuse::operations::CompactTransform;
use databend_common_storages_fuse::operations::TableMutationAggregator;
use databend_common_storages_fuse::operations::TransformSerializeBlock;
use databend_common_storages_fuse::FuseTable;

use crate::pipelines::PipelineBuilder;

impl PipelineBuilder {
    pub(crate) fn build_compact_source(
        &mut self,
        compact_block: &PhysicalCompactSource,
    ) -> Result<()> {
        let table = self
            .ctx
            .build_table_by_table_info(&compact_block.table_info, None)?;
        let table = FuseTable::try_from_table(table.as_ref())?;

        if compact_block.parts.is_empty() {
            return self.main_pipeline.add_source(EmptySource::create, 1);
        }

        let is_lazy = compact_block.parts.partitions_type() == PartInfoType::LazyLevel;
        let thresholds = table.get_block_thresholds();
        let cluster_key_id = table.cluster_key_id();
        let mut max_threads = self.ctx.get_settings().get_max_threads()? as usize;

        if is_lazy {
            let query_ctx = self.ctx.clone();

            let lazy_parts = compact_block
                .parts
                .partitions
                .iter()
                .map(|v| {
                    v.as_any()
                        .downcast_ref::<CompactLazyPartInfo>()
                        .unwrap()
                        .clone()
                })
                .collect::<Vec<_>>();

            let column_ids = compact_block.column_ids.clone();
            self.main_pipeline.set_on_init(move || {
                let ctx = query_ctx.clone();
                let partitions =
                    Runtime::with_worker_threads(2, Some("build_compact_tasks".to_string()))?
                        .block_on(async move {
                            let partitions = BlockCompactMutator::build_compact_tasks(
                                ctx.clone(),
                                column_ids.clone(),
                                cluster_key_id,
                                thresholds,
                                lazy_parts,
                            )
                            .await?;

                            Result::<_>::Ok(partitions)
                        })?;

                let partitions = Partitions::create(PartitionsShuffleKind::Mod, partitions);
                query_ctx.set_partitions(partitions)?;
                Ok(())
            });
        } else {
            max_threads = max_threads.min(compact_block.parts.len()).max(1);
            self.ctx.set_partitions(compact_block.parts.clone())?;
        }

        let block_reader = table.create_block_reader(
            self.ctx.clone(),
            Projection::Columns(table.all_column_indices()),
            false,
            table.change_tracking_enabled(),
            false,
        )?;
        let stream_ctx = if table.change_tracking_enabled() {
            Some(StreamContext::try_create(
                self.ctx.get_function_context()?,
                table.schema_with_stream(),
                table.get_table_info().ident.seq,
                false,
                false,
            )?)
        } else {
            None
        };
        // Add source pipe.
        self.main_pipeline.add_source(
            |output| {
                let source = CompactSource::create(self.ctx.clone(), block_reader.clone(), 1);
                PrefetchAsyncSourcer::create(self.ctx.clone(), output, source)
            },
            max_threads,
        )?;
        let storage_format = table.get_storage_format();
        self.main_pipeline.add_block_meta_transformer(|| {
            CompactTransform::create(
                self.ctx.clone(),
                block_reader.clone(),
                storage_format,
                stream_ctx.clone(),
            )
        });

        // sort
        let cluster_stats_gen = table.cluster_gen_for_append(
            self.ctx.clone(),
            &mut self.main_pipeline,
            thresholds,
            None,
        )?;
        self.main_pipeline.add_transform(|input, output| {
            let proc = TransformSerializeBlock::try_create(
                self.ctx.clone(),
                input,
                output,
                table,
                cluster_stats_gen.clone(),
                MutationKind::Compact,
                compact_block.table_meta_timestamps,
            )?;
            proc.into_processor()
        })?;

        if is_lazy {
            self.main_pipeline.try_resize(1)?;
            self.main_pipeline.add_async_accumulating_transformer(|| {
                TableMutationAggregator::create(
                    table,
                    self.ctx.clone(),
                    vec![],
                    vec![],
                    vec![],
                    Default::default(),
                    MutationKind::Compact,
                    compact_block.table_meta_timestamps,
                )
            });
        }
        Ok(())
    }
}
