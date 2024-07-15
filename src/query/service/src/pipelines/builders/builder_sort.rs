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

use databend_common_exception::Result;
use databend_common_expression::DataSchemaRef;
use databend_common_expression::SortColumnDescription;
use databend_common_pipeline_core::processors::ProcessorPtr;
use databend_common_pipeline_core::query_spill_prefix;
use databend_common_pipeline_core::Pipeline;
use databend_common_pipeline_transforms::processors::sort::utils::add_order_field;
use databend_common_pipeline_transforms::processors::try_add_multi_sort_merge;
use databend_common_pipeline_transforms::processors::TransformPipelineHelper;
use databend_common_pipeline_transforms::processors::TransformSortMergeBuilder;
use databend_common_pipeline_transforms::processors::TransformSortPartial;
use databend_common_sql::evaluator::BlockOperator;
use databend_common_sql::evaluator::CompoundBlockOperator;
use databend_common_sql::executor::physical_plans::Sort;
use databend_common_storage::DataOperator;
use databend_common_storages_fuse::TableContext;

use crate::pipelines::processors::transforms::create_transform_sort_spill;
use crate::pipelines::PipelineBuilder;
use crate::sessions::QueryContext;
use crate::spillers::Spiller;
use crate::spillers::SpillerConfig;
use crate::spillers::SpillerType;

impl PipelineBuilder {
    // The pipeline graph of distributed sort can be found in https://github.com/datafuselabs/databend/pull/13881
    pub(crate) fn build_sort(&mut self, sort: &Sort) -> Result<()> {
        self.build_pipeline(&sort.input)?;

        let input_schema = sort.input.output_schema()?;

        if !matches!(sort.after_exchange, Some(true)) {
            // If the Sort plan is after exchange, we don't need to do a projection,
            // because the data is already projected in each cluster node.
            if let Some(proj) = &sort.pre_projection {
                // Do projection to reduce useless data copying during sorting.
                let projection = proj
                    .iter()
                    .filter_map(|i| input_schema.index_of(&i.to_string()).ok())
                    .collect::<Vec<_>>();

                if projection.len() < input_schema.fields().len() {
                    // Only if the projection is not a full projection, we need to add a projection transform.
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
            }
        }

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
                    is_nullable: plan_schema.field(offset).is_nullable(),  // This information is not needed here.
                })
            })
            .collect::<Result<Vec<_>>>()?;

        if sort.window_partition.is_empty() {
            self.build_sort_pipeline(plan_schema, sort_desc, sort.limit, sort.after_exchange)
        } else {
            self.build_window_sort_pipeline(plan_schema, sort_desc, sort.limit, sort.after_exchange)
        }
    }

    pub(crate) fn build_sort_pipeline(
        &mut self,
        plan_schema: DataSchemaRef,
        sort_desc: Vec<SortColumnDescription>,
        limit: Option<usize>,
        after_exchange: Option<bool>,
    ) -> Result<()> {
        let block_size = self.settings.get_max_block_size()? as usize;
        let max_threads = self.settings.get_max_threads()? as usize;
        let sort_desc = Arc::new(sort_desc);

        // TODO(Winter): the query will hang in MultiSortMergeProcessor when max_threads == 1 and output_len != 1
        if self.main_pipeline.output_len() != 1 && max_threads == 1 {
            self.main_pipeline.try_resize(max_threads)?;
        }

        let mut builder =
            SortPipelineBuilder::create(self.ctx.clone(), plan_schema.clone(), sort_desc.clone())
                .with_partial_block_size(block_size)
                .with_final_block_size(block_size)
                .with_limit(limit);

        match after_exchange {
            Some(true) => {
                // Build for the coordinator node.
                // We only build a `MultiSortMergeTransform`,
                // as the data is already sorted in each cluster node.
                // The input number of the transform is equal to the number of cluster nodes.
                if self.main_pipeline.output_len() > 1 {
                    try_add_multi_sort_merge(
                        &mut self.main_pipeline,
                        plan_schema,
                        block_size,
                        limit,
                        sort_desc,
                        true,
                        self.ctx.get_settings().get_enable_loser_tree_merge_sort()?,
                    )
                } else {
                    builder = builder.remove_order_col_at_last();
                    builder.build_merge_sort_pipeline(&mut self.main_pipeline, true, false)
                }
            }
            Some(false) => {
                // Build for each cluster node.
                // We build the full sort pipeline for it.
                // Don't remove the order column at last.
                builder.build_full_sort_pipeline(&mut self.main_pipeline, false)
            }
            None => {
                // Build for single node mode.
                // We build the full sort pipeline for it.
                builder = builder.remove_order_col_at_last();
                builder.build_full_sort_pipeline(&mut self.main_pipeline, false)
            }
        }
    }

    pub(crate) fn build_window_sort_pipeline(
        &mut self,
        plan_schema: DataSchemaRef,
        sort_desc: Vec<SortColumnDescription>,
        limit: Option<usize>,
        after_exchange: Option<bool>,
    ) -> Result<()> {
        let block_size = self.settings.get_max_block_size()? as usize;
        let max_threads = self.settings.get_max_threads()? as usize;
        let sort_desc = Arc::new(sort_desc);

        // TODO(Winter): the query will hang in MultiSortMergeProcessor when max_threads == 1 and output_len != 1
        if self.main_pipeline.output_len() == 1 || max_threads == 1 {
            self.main_pipeline.try_resize(max_threads)?;
        }

        let mut builder =
            SortPipelineBuilder::create(self.ctx.clone(), plan_schema.clone(), sort_desc.clone())
                .with_partial_block_size(block_size)
                .with_final_block_size(block_size)
                .with_limit(limit);

        // Build for single node mode cause it's window shuffle
        // We build the full sort pipeline for it
        match after_exchange {
            Some(true) => {
                // Build for the coordinator node.
                // We only build a `MultiSortMergeTransform`,
                // as the data is already sorted in each cluster node.
                // The input number of the transform is equal to the number of cluster nodes.
                if self.main_pipeline.output_len() > 1 {
                    try_add_multi_sort_merge(
                        &mut self.main_pipeline,
                        plan_schema,
                        block_size,
                        limit,
                        sort_desc,
                        true,
                        self.ctx.get_settings().get_enable_loser_tree_merge_sort()?,
                    )
                } else {
                    builder = builder.remove_order_col_at_last();
                    builder.build_merge_sort_pipeline(&mut self.main_pipeline, true, true)
                }
            }
            _ => {
                // Build for each single node mode.
                // We build the full sort pipeline for it.
                builder = builder.remove_order_col_at_last();
                builder.build_full_sort_pipeline(&mut self.main_pipeline, true)
            }
        }
    }
}

pub struct SortPipelineBuilder {
    ctx: Arc<QueryContext>,
    schema: DataSchemaRef,
    sort_desc: Arc<Vec<SortColumnDescription>>,
    limit: Option<usize>,
    partial_block_size: usize,
    final_block_size: usize,
    remove_order_col_at_last: bool,
}

impl SortPipelineBuilder {
    pub fn create(
        ctx: Arc<QueryContext>,
        schema: DataSchemaRef,
        sort_desc: Arc<Vec<SortColumnDescription>>,
    ) -> Self {
        Self {
            ctx,
            schema,
            sort_desc,
            limit: None,
            partial_block_size: 0,
            final_block_size: 0,
            remove_order_col_at_last: false,
        }
    }

    pub fn with_limit(mut self, limit: Option<usize>) -> Self {
        self.limit = limit;
        self
    }

    pub fn with_partial_block_size(mut self, partial_block_size: usize) -> Self {
        self.partial_block_size = partial_block_size;
        self
    }

    pub fn with_final_block_size(mut self, final_block_size: usize) -> Self {
        self.final_block_size = final_block_size;
        self
    }

    pub fn remove_order_col_at_last(mut self) -> Self {
        self.remove_order_col_at_last = true;
        self
    }

    pub fn build_full_sort_pipeline(
        self,
        pipeline: &mut Pipeline,
        is_window_sort: bool,
    ) -> Result<()> {
        // Partial sort
        pipeline.add_transformer(|| TransformSortPartial::new(self.limit, self.sort_desc.clone()));

        self.build_merge_sort_pipeline(pipeline, false, is_window_sort)
    }

    fn get_memory_settings(&self, num_threads: usize) -> Result<(usize, usize)> {
        let enable_sort_spill = self.ctx.get_enable_sort_spill();
        if !enable_sort_spill {
            return Ok((0, 0));
        }

        let settings = self.ctx.get_settings();
        let memory_ratio = settings.get_sort_spilling_memory_ratio()?;
        let bytes_limit_per_proc = settings.get_sort_spilling_bytes_threshold_per_proc()?;
        if memory_ratio == 0 && bytes_limit_per_proc == 0 {
            // If these two settings are not set, do not enable sort spill.
            // TODO(spill): enable sort spill by default like aggregate.
            return Ok((0, 0));
        }
        let memory_ratio = (memory_ratio as f64 / 100_f64).min(1_f64);
        let max_memory_usage = match settings.get_max_memory_usage()? {
            0 => usize::MAX,
            max_memory_usage => {
                if memory_ratio == 0_f64 {
                    usize::MAX
                } else {
                    (max_memory_usage as f64 * memory_ratio) as usize
                }
            }
        };
        let spill_threshold_per_core =
            match settings.get_sort_spilling_bytes_threshold_per_proc()? {
                0 => max_memory_usage / num_threads,
                bytes => bytes,
            };

        Ok((max_memory_usage, spill_threshold_per_core))
    }

    pub fn build_merge_sort_pipeline(
        self,
        pipeline: &mut Pipeline,
        order_col_generated: bool,
        is_window_sort: bool,
    ) -> Result<()> {
        // Merge sort
        let need_multi_merge = pipeline.output_len() > 1;
        let output_order_col = if is_window_sort {
            false
        } else {
            need_multi_merge || !self.remove_order_col_at_last
        };
        debug_assert!(if order_col_generated {
            // If `order_col_generated`, it means this transform is the last processor in the distributed sort pipeline.
            !output_order_col
        } else {
            true
        });

        let (max_memory_usage, bytes_limit_per_proc) =
            self.get_memory_settings(pipeline.output_len())?;

        let may_spill = max_memory_usage != 0 && bytes_limit_per_proc != 0;

        let sort_merge_output_schema = if output_order_col || may_spill {
            add_order_field(self.schema.clone(), &self.sort_desc)
        } else {
            self.schema.clone()
        };

        let enable_loser_tree = self.ctx.get_settings().get_enable_loser_tree_merge_sort()?;
        let spilling_batch_bytes = self.ctx.get_settings().get_sort_spilling_batch_bytes()?;
        pipeline.add_transform(|input, output| {
            let builder = TransformSortMergeBuilder::create(
                input,
                output,
                sort_merge_output_schema.clone(),
                self.sort_desc.clone(),
                self.partial_block_size,
            )
            .with_limit(self.limit)
            .with_order_col_generated(order_col_generated)
            .with_output_order_col(output_order_col || may_spill)
            .with_max_memory_usage(max_memory_usage)
            .with_spilling_bytes_threshold_per_core(bytes_limit_per_proc)
            .with_spilling_batch_bytes(spilling_batch_bytes)
            .with_enable_loser_tree(enable_loser_tree);

            Ok(ProcessorPtr::create(builder.build()?))
        })?;

        if may_spill {
            let schema = add_order_field(sort_merge_output_schema.clone(), &self.sort_desc);
            let config = SpillerConfig::create(query_spill_prefix(
                self.ctx.get_tenant().tenant_name(),
                &self.ctx.get_id(),
            ));
            pipeline.add_transform(|input, output| {
                let op = DataOperator::instance().operator();
                let spiller =
                    Spiller::create(self.ctx.clone(), op, config.clone(), SpillerType::OrderBy)?;
                Ok(ProcessorPtr::create(create_transform_sort_spill(
                    input,
                    output,
                    schema.clone(),
                    self.sort_desc.clone(),
                    self.limit,
                    spiller,
                    output_order_col,
                )))
            })?;
        }

        if need_multi_merge && !is_window_sort {
            // if need_multi_merge {
            // Multi-pipelines merge sort
            try_add_multi_sort_merge(
                pipeline,
                self.schema.clone(),
                self.final_block_size,
                self.limit,
                self.sort_desc,
                self.remove_order_col_at_last,
                self.ctx.get_settings().get_enable_loser_tree_merge_sort()?,
            )?;
        }

        Ok(())
    }
}
