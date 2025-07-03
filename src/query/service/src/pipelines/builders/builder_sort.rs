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
use databend_common_pipeline_core::processors::ProcessorPtr;
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
use crate::pipelines::processors::transforms::add_range_shuffle_route;
use crate::pipelines::processors::transforms::SortInjector;
use crate::pipelines::processors::transforms::SortRangeExchange;
use crate::pipelines::processors::transforms::TransformLimit;
use crate::pipelines::processors::transforms::TransformSortBuilder;
use crate::pipelines::PipelineBuilder;
use crate::servers::flight::v1::exchange::ExchangeInjector;
use crate::sessions::QueryContext;
use crate::spillers::Spiller;
use crate::spillers::SpillerConfig;
use crate::spillers::SpillerDiskConfig;
use crate::spillers::SpillerType;

impl PipelineBuilder {
    pub(crate) fn build_sort(&mut self, sort: &Sort) -> Result<()> {
        let plan_schema = sort.output_schema()?;
        let sort_desc = sort
            .order_by
            .iter()
            .map(|desc| {
                let offset = plan_schema.index_of(&desc.order_by.to_string())?;
                Ok(SortColumnDescription {
                    offset,
                    asc: desc.asc,
                    nulls_first: desc.nulls_first,
                })
            })
            .collect::<Result<Vec<_>>>()?;
        let sort_desc = sort_desc.into();

        if sort.step != SortStep::RangeSort {
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

        let builder = SortPipelineBuilder::create(self.ctx.clone(), plan_schema, sort_desc, None)?
            .with_limit(sort.limit);

        match sort.step {
            SortStep::FinalMerge => {
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
            SortStep::Partial => {
                // Build for each cluster node.
                // We build the full sort pipeline for it.
                // Don't remove the order column at last.
                builder.build_full_sort_pipeline(&mut self.main_pipeline)
            }
            SortStep::Single => {
                // Build for single node mode.
                // We build the full sort pipeline for it.
                builder
                    .remove_order_col_at_last()
                    .build_full_sort_pipeline(&mut self.main_pipeline)
            }
            SortStep::Sample => builder.build_sort_part(&mut self.main_pipeline),
            SortStep::RangeSort => {
                if matches!(*sort.input, PhysicalPlan::ExchangeSource(_)) {
                    let exchange = builder.exchange_injector();
                    let old_inject = std::mem::replace(&mut self.exchange_injector, exchange);
                    self.build_pipeline(&sort.input)?;
                    self.exchange_injector = old_inject;
                } else {
                    self.build_pipeline(&sort.input)?;
                }
                todo!()
            }
            SortStep::Route => todo!(),
        }
    }
}

pub struct SortPipelineBuilder {
    ctx: Arc<QueryContext>,
    schema: DataSchemaRef,
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
        schema: DataSchemaRef,
        sort_desc: Arc<[SortColumnDescription]>,
        broadcast_id: Option<u32>,
    ) -> Result<Self> {
        let settings = ctx.get_settings();
        let block_size = settings.get_max_block_size()? as usize;
        let enable_loser_tree = settings.get_enable_loser_tree_merge_sort()?;
        Ok(Self {
            ctx,
            schema,
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

    fn build_range_shuffle_sort_pipeline(self, pipeline: &mut Pipeline) -> Result<()> {
        let inputs = pipeline.output_len();
        let settings = self.ctx.get_settings();
        let num_exec = inputs;
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

        let builder = TransformSortBuilder::create(
            self.schema.clone(),
            self.sort_desc.clone(),
            max_block_size,
            spiller,
        )
        .with_limit(self.limit)
        .with_order_col_generated(false)
        .with_output_order_col(false)
        .with_memory_settings(memory_settings)
        .with_enable_loser_tree(enable_loser_tree);

        pipeline.add_transform(|input, output| {
            Ok(ProcessorPtr::create(builder.build_collect(input, output)?))
        })?;

        builder.add_bound_broadcast(
            pipeline,
            builder.inner_schema(),
            max_block_size,
            self.ctx.clone(),
            self.broadcast_id.unwrap(),
        )?;

        pipeline.exchange(num_exec, Arc::new(SortRangeExchange));

        pipeline.add_transform(|input, output| {
            Ok(ProcessorPtr::create(builder.build_combine(input, output)?))
        })?;

        pipeline.add_transform(|input, output| {
            Ok(ProcessorPtr::create(builder.build_exec(input, output)?))
        })?;

        add_range_shuffle_route(pipeline)?;

        if self.limit.is_none() {
            return Ok(());
        }

        pipeline.add_transform(|input, output| {
            TransformLimit::try_create(self.limit, 0, input, output).map(ProcessorPtr::create)
        })
    }

    fn build_sort_part(self, pipeline: &mut Pipeline) -> Result<()> {
        let inputs = pipeline.output_len();
        let settings = self.ctx.get_settings();
        let num_exec = inputs;
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

        let builder = TransformSortBuilder::create(
            self.schema.clone(),
            self.sort_desc.clone(),
            max_block_size,
            spiller,
        )
        .with_limit(self.limit)
        .with_order_col_generated(false)
        .with_output_order_col(false)
        .with_memory_settings(memory_settings)
        .with_enable_loser_tree(enable_loser_tree);

        pipeline.add_transform(|input, output| {
            Ok(ProcessorPtr::create(builder.build_collect(input, output)?))
        })?;

        builder.add_bound_broadcast(
            pipeline,
            builder.inner_schema(),
            max_block_size,
            self.ctx.clone(),
            self.broadcast_id.unwrap(),
        )?;

        Ok(())
    }

    fn build_merge_sort(&self, pipeline: &mut Pipeline, order_col_generated: bool) -> Result<()> {
        // Merge sort
        let need_multi_merge = pipeline.output_len() > 1;
        let output_order_col = need_multi_merge || !self.remove_order_col_at_last;
        debug_assert!(if order_col_generated {
            // If `order_col_generated`, it means this transform is the last processor in the distributed sort pipeline.
            !output_order_col
        } else {
            true
        });

        let memory_settings = MemorySettings::from_sort_settings(&self.ctx)?;
        let sort_merge_output_schema = match output_order_col {
            true => add_order_field(self.schema.clone(), &self.sort_desc),
            false => self.schema.clone(),
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
            let builder = TransformSortBuilder::create(
                sort_merge_output_schema.clone(),
                self.sort_desc.clone(),
                self.block_size,
                spiller.clone(),
            )
            .with_limit(self.limit)
            .with_order_col_generated(order_col_generated)
            .with_output_order_col(output_order_col)
            .with_memory_settings(memory_settings.clone())
            .with_enable_loser_tree(enable_loser_tree);

            Ok(ProcessorPtr::create(builder.build(input, output)?))
        })
    }

    pub fn build_merge_sort_pipeline(
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
                self.schema.clone(),
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
                self.schema.clone(),
                self.block_size,
                self.limit,
                self.sort_desc,
                self.remove_order_col_at_last,
                self.enable_loser_tree,
            )
        }
    }

    pub fn exchange_injector(self) -> Arc<dyn ExchangeInjector> {
        Arc::new(SortInjector {})
    }
}
