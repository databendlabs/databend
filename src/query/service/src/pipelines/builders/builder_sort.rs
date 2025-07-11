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

use std::assert_matches::debug_assert_matches;
use std::sync::Arc;

use databend_common_exception::Result;
use databend_common_expression::DataSchemaRef;
use databend_common_expression::LimitType;
use databend_common_expression::SortColumnDescription;
use databend_common_pipeline_core::processors::InputPort;
use databend_common_pipeline_core::processors::OutputPort;
use databend_common_pipeline_core::processors::ProcessorPtr;
use databend_common_pipeline_core::Pipe;
use databend_common_pipeline_core::PipeItem;
use databend_common_pipeline_core::Pipeline;
use databend_common_pipeline_transforms::processors::add_k_way_merge_sort;
use databend_common_pipeline_transforms::processors::sort::utils::add_order_field;
use databend_common_pipeline_transforms::processors::try_add_multi_sort_merge;
use databend_common_pipeline_transforms::processors::TransformPipelineHelper;
use databend_common_pipeline_transforms::processors::TransformSortPartial;
use databend_common_pipeline_transforms::MemorySettings;
use databend_common_sql::evaluator::BlockOperator;
use databend_common_sql::evaluator::CompoundBlockOperator;
use databend_common_sql::executor::physical_plans::Sort;
use databend_common_sql::executor::physical_plans::SortStep;
use databend_common_sql::executor::PhysicalPlan;
use databend_common_storage::DataOperator;
use databend_common_storages_fuse::TableContext;
use databend_storages_common_cache::TempDirManager;

use crate::pipelines::memory_settings::MemorySettingsExt;
use crate::pipelines::processors::transforms::TransformSortBuilder;
use crate::pipelines::PipelineBuilder;
use crate::sessions::QueryContext;
use crate::spillers::Spiller;
use crate::spillers::SpillerConfig;
use crate::spillers::SpillerDiskConfig;
use crate::spillers::SpillerType;

impl PipelineBuilder {
    pub(crate) fn build_sort(&mut self, sort: &Sort) -> Result<()> {
        let output_schema = sort.output_schema()?;
        let sort_desc = sort
            .order_by
            .iter()
            .map(|desc| {
                let offset = output_schema.index_of(&desc.order_by.to_string())?;
                Ok(SortColumnDescription {
                    offset,
                    asc: desc.asc,
                    nulls_first: desc.nulls_first,
                })
            })
            .collect::<Result<Vec<_>>>()?;
        let sort_desc = sort_desc.into();

        if sort.step != SortStep::Shuffled {
            self.build_pipeline(&sort.input)?;
        }

        if let Some(proj) = &sort.pre_projection {
            debug_assert_matches!(
                sort.step,
                SortStep::Single | SortStep::Partial | SortStep::Sample
            );

            let input_schema = sort.input.output_schema()?;
            // Do projection to reduce useless data copying during sorting.
            let projection = proj
                .iter()
                .map(|i| input_schema.index_of(&i.to_string()).unwrap())
                .collect::<Vec<_>>();

            self.main_pipeline.add_transformer(|| {
                CompoundBlockOperator::new(
                    vec![BlockOperator::Project {
                        projection: projection.clone(),
                    }],
                    self.func_ctx.clone(),
                    input_schema.num_fields(),
                )
            });
        }

        let max_threads = self.settings.get_max_threads()? as usize;

        // TODO(Winter): the query will hang in MultiSortMergeProcessor when max_threads == 1 and output_len != 1
        if self.main_pipeline.output_len() == 1 || max_threads == 1 {
            self.main_pipeline.try_resize(max_threads)?;
        }

        let builder = SortPipelineBuilder::create(
            self.ctx.clone(),
            output_schema,
            sort_desc,
            sort.broadcast_id,
        )?
        .with_limit(sort.limit);

        match sort.step {
            SortStep::Single => {
                // Build for single node mode.
                // We build the full sort pipeline for it.
                builder
                    .remove_order_col_at_last()
                    .build_full_sort_pipeline(&mut self.main_pipeline)
            }

            SortStep::Partial => {
                // Build for each cluster node.
                // We build the full sort pipeline for it.
                // Don't remove the order column at last.
                builder.build_full_sort_pipeline(&mut self.main_pipeline)
            }
            SortStep::Final => {
                // Build for the coordinator node.
                // We only build a `MultiSortMergeTransform`,
                // as the data is already sorted in each cluster node.
                // The input number of the transform is equal to the number of cluster nodes.
                if self.main_pipeline.output_len() > 1 {
                    builder
                        .remove_order_col_at_last()
                        .build_multi_merge(&mut self.main_pipeline)
                } else {
                    builder
                        .remove_order_col_at_last()
                        .build_merge_sort_pipeline(&mut self.main_pipeline, true)
                }
            }

            SortStep::Sample => {
                builder.build_sample(&mut self.main_pipeline)?;
                self.exchange_injector = TransformSortBuilder::exchange_injector();
                Ok(())
            }
            SortStep::Shuffled => {
                if matches!(*sort.input, PhysicalPlan::ExchangeSource(_)) {
                    let exchange = TransformSortBuilder::exchange_injector();
                    let old_inject = std::mem::replace(&mut self.exchange_injector, exchange);
                    self.build_pipeline(&sort.input)?;
                    self.exchange_injector = old_inject;
                } else {
                    self.build_pipeline(&sort.input)?;
                }

                builder
                    .remove_order_col_at_last()
                    .build_bounded_merge_sort(&mut self.main_pipeline)
            }
            SortStep::Route => TransformSortBuilder::add_route(&mut self.main_pipeline),
        }
    }
}

pub struct SortPipelineBuilder {
    ctx: Arc<QueryContext>,
    output_schema: DataSchemaRef,
    sort_desc: Arc<[SortColumnDescription]>,
    limit: Option<usize>,
    block_size: usize,
    remove_order_col_at_last: bool,
    enable_loser_tree: bool,
    broadcast_id: Option<u32>,
}

impl SortPipelineBuilder {
    pub fn create(
        ctx: Arc<QueryContext>,
        output_schema: DataSchemaRef,
        sort_desc: Arc<[SortColumnDescription]>,
        broadcast_id: Option<u32>,
    ) -> Result<Self> {
        let settings = ctx.get_settings();
        let block_size = settings.get_max_block_size()? as usize;
        let enable_loser_tree = settings.get_enable_loser_tree_merge_sort()?;
        Ok(Self {
            ctx,
            output_schema,
            sort_desc,
            limit: None,
            block_size,
            remove_order_col_at_last: false,
            enable_loser_tree,
            broadcast_id,
        })
    }

    pub fn with_limit(mut self, limit: Option<usize>) -> Self {
        self.limit = limit;
        self
    }

    // The expected output block size, the actual output block size will be equal to or less than the given value.
    pub fn with_block_size_hit(mut self, block_size: usize) -> Self {
        self.block_size = self.block_size.min(block_size);
        self
    }

    pub fn remove_order_col_at_last(mut self) -> Self {
        self.remove_order_col_at_last = true;
        self
    }

    pub fn build_full_sort_pipeline(self, pipeline: &mut Pipeline) -> Result<()> {
        // Partial sort
        pipeline.add_transformer(|| {
            TransformSortPartial::new(
                LimitType::from_limit_rows(self.limit),
                self.sort_desc.clone(),
            )
        });

        self.build_merge_sort_pipeline(pipeline, false)
    }

    fn build_merge_sort(&self, pipeline: &mut Pipeline, order_col_generated: bool) -> Result<()> {
        // Merge sort
        let need_multi_merge = pipeline.output_len() > 1;
        let output_order_col = need_multi_merge || !self.remove_order_col_at_last;
        debug_assert!(
            // If `order_col_generated`, it means this transform is the last processor in the distributed sort pipeline.
            !order_col_generated || !output_order_col
        );

        let memory_settings = MemorySettings::from_sort_settings(&self.ctx)?;
        let sort_merge_output_schema = match output_order_col {
            true => add_order_field(self.output_schema.clone(), &self.sort_desc),
            false => self.output_schema.clone(),
        };

        let settings = self.ctx.get_settings();
        let enable_loser_tree = settings.get_enable_loser_tree_merge_sort()?;

        let spiller = {
            let temp_dir_manager = TempDirManager::instance();
            let disk_bytes_limit = settings.get_sort_spilling_to_disk_bytes_limit()?;
            let enable_dio = settings.get_enable_dio()?;
            let disk_spill = temp_dir_manager
                .get_disk_spill_dir(disk_bytes_limit, &self.ctx.get_id())
                .map(|temp_dir| SpillerDiskConfig::new(temp_dir, enable_dio))
                .transpose()?;

            let location_prefix = self.ctx.query_id_spill_prefix();
            let config = SpillerConfig {
                spiller_type: SpillerType::OrderBy,
                location_prefix,
                disk_spill,
                use_parquet: settings.get_spilling_file_format()?.is_parquet(),
            };
            let op = DataOperator::instance().spill_operator();
            Arc::new(Spiller::create(self.ctx.clone(), op, config)?)
        };

        pipeline.add_transform(|input, output| {
            let builder = TransformSortBuilder::new(
                sort_merge_output_schema.clone(),
                self.sort_desc.clone(),
                self.block_size,
            )
            .with_spiller(spiller.clone())
            .with_limit(self.limit)
            .with_order_column(order_col_generated, output_order_col)
            .with_memory_settings(memory_settings.clone())
            .with_enable_loser_tree(enable_loser_tree);

            Ok(ProcessorPtr::create(builder.build(input, output)?))
        })
    }

    fn build_merge_sort_pipeline(
        self,
        pipeline: &mut Pipeline,
        order_col_generated: bool,
    ) -> Result<()> {
        let need_multi_merge = pipeline.output_len() > 1;
        self.build_merge_sort(pipeline, order_col_generated)?;

        if !need_multi_merge {
            return Ok(());
        }

        self.build_multi_merge(pipeline)
    }

    pub fn build_multi_merge(self, pipeline: &mut Pipeline) -> Result<()> {
        // Multi-pipelines merge sort
        let settings = self.ctx.get_settings();
        if settings.get_enable_parallel_multi_merge_sort()? {
            let max_threads = settings.get_max_threads()? as usize;
            add_k_way_merge_sort(
                pipeline,
                self.output_schema.clone(),
                max_threads,
                self.block_size,
                self.limit,
                self.sort_desc,
                self.remove_order_col_at_last,
                self.enable_loser_tree,
            )
        } else {
            try_add_multi_sort_merge(
                pipeline,
                self.output_schema.clone(),
                self.block_size,
                self.limit,
                self.sort_desc,
                self.remove_order_col_at_last,
                self.enable_loser_tree,
            )
        }
    }

    fn build_sample(self, pipeline: &mut Pipeline) -> Result<()> {
        let settings = self.ctx.get_settings();
        let max_block_size = settings.get_max_block_size()? as usize;

        // Partial sort
        pipeline.add_transformer(|| {
            TransformSortPartial::new(
                LimitType::from_limit_rows(self.limit),
                self.sort_desc.clone(),
            )
        });

        let spiller = {
            let location_prefix = self.ctx.query_id_spill_prefix();
            let config = SpillerConfig {
                spiller_type: SpillerType::OrderBy,
                location_prefix,
                disk_spill: None,
                use_parquet: settings.get_spilling_file_format()?.is_parquet(),
            };
            let op = DataOperator::instance().spill_operator();
            Arc::new(Spiller::create(self.ctx.clone(), op, config)?)
        };

        let memory_settings = MemorySettings::from_sort_settings(&self.ctx)?;
        let enable_loser_tree = settings.get_enable_loser_tree_merge_sort()?;

        let builder = TransformSortBuilder::new(
            self.output_schema.clone(),
            self.sort_desc.clone(),
            max_block_size,
        )
        .with_spiller(spiller)
        .with_limit(self.limit)
        .with_order_column(false, true)
        .with_memory_settings(memory_settings)
        .with_enable_loser_tree(enable_loser_tree);

        pipeline.add_transform(|input, output| {
            Ok(ProcessorPtr::create(builder.build_collect(input, output)?))
        })?;

        builder.add_bound_broadcast(
            pipeline,
            max_block_size,
            self.ctx.clone(),
            self.broadcast_id.unwrap(),
        )?;

        pipeline.add_transform(|input, output| {
            Ok(ProcessorPtr::create(builder.build_restore(input, output)?))
        })?;

        pipeline.add_transform(|input, output| {
            Ok(ProcessorPtr::create(
                builder.build_bound_edge(input, output)?,
            ))
        })?;

        Ok(())
    }

    fn build_bounded_merge_sort(self, pipeline: &mut Pipeline) -> Result<()> {
        let builder = TransformSortBuilder::new(
            self.output_schema.clone(),
            self.sort_desc.clone(),
            self.block_size,
        )
        .with_limit(self.limit)
        .with_order_column(true, !self.remove_order_col_at_last)
        .with_enable_loser_tree(self.enable_loser_tree);

        let inputs_port: Vec<_> = (0..pipeline.output_len())
            .map(|_| InputPort::create())
            .collect();
        let output_port = OutputPort::create();

        let processor = ProcessorPtr::create(
            builder.build_bounded_merge_sort(inputs_port.clone(), output_port.clone())?,
        );

        pipeline.add_pipe(Pipe::create(inputs_port.len(), 1, vec![PipeItem::create(
            processor,
            inputs_port,
            vec![output_port],
        )]));
        Ok(())
    }
}
