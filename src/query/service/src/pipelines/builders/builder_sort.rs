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
use databend_common_expression::LimitType;
use databend_common_expression::SortColumnDescription;
use databend_common_pipeline_core::processors::ProcessorPtr;
use databend_common_pipeline_core::Pipeline;
use databend_common_pipeline_transforms::processors::add_k_way_merge_sort;
use databend_common_pipeline_transforms::processors::sort::utils::add_order_field;
use databend_common_pipeline_transforms::processors::try_add_multi_sort_merge;
use databend_common_pipeline_transforms::processors::TransformPipelineHelper;
use databend_common_pipeline_transforms::processors::TransformSortMergeBuilder;
use databend_common_pipeline_transforms::processors::TransformSortPartial;
use databend_common_pipeline_transforms::MemorySettings;
use databend_common_sql::evaluator::BlockOperator;
use databend_common_sql::evaluator::CompoundBlockOperator;
use databend_common_sql::executor::physical_plans::Sort;
use databend_common_storage::DataOperator;
use databend_common_storages_fuse::TableContext;

use crate::pipelines::memory_settings::MemorySettingsExt;
use crate::pipelines::processors::transforms::create_transform_stream_sort_spill;
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
                })
            })
            .collect::<Result<Vec<_>>>()?;

        self.build_sort_pipeline(plan_schema, sort_desc, sort.limit, sort.after_exchange)
    }

    pub(crate) fn build_sort_pipeline(
        &mut self,
        plan_schema: DataSchemaRef,
        sort_desc: Vec<SortColumnDescription>,
        limit: Option<usize>,
        after_exchange: Option<bool>,
    ) -> Result<()> {
        let max_threads = self.settings.get_max_threads()? as usize;
        let sort_desc = Arc::new(sort_desc);

        // TODO(Winter): the query will hang in MultiSortMergeProcessor when max_threads == 1 and output_len != 1
        if self.main_pipeline.output_len() == 1 || max_threads == 1 {
            self.main_pipeline.try_resize(max_threads)?;
        }

        let builder = SortPipelineBuilder::create(self.ctx.clone(), plan_schema, sort_desc)?
            .with_limit(limit);

        match after_exchange {
            Some(true) => {
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
            Some(false) => {
                // Build for each cluster node.
                // We build the full sort pipeline for it.
                // Don't remove the order column at last.
                builder.build_full_sort_pipeline(&mut self.main_pipeline)
            }
            None => {
                // Build for single node mode.
                // We build the full sort pipeline for it.
                builder
                    .remove_order_col_at_last()
                    .build_full_sort_pipeline(&mut self.main_pipeline)
            }
        }
    }
}

pub struct SortPipelineBuilder {
    ctx: Arc<QueryContext>,
    schema: DataSchemaRef,
    sort_desc: Arc<Vec<SortColumnDescription>>,
    limit: Option<usize>,
    block_size: usize,
    remove_order_col_at_last: bool,
}

impl SortPipelineBuilder {
    pub fn create(
        ctx: Arc<QueryContext>,
        schema: DataSchemaRef,
        sort_desc: Arc<Vec<SortColumnDescription>>,
    ) -> Result<Self> {
        let block_size = ctx.get_settings().get_max_block_size()? as usize;
        Ok(Self {
            ctx,
            schema,
            sort_desc,
            limit: None,
            block_size,
            remove_order_col_at_last: false,
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

    pub fn build_merge_sort_pipeline(
        self,
        pipeline: &mut Pipeline,
        order_col_generated: bool,
    ) -> Result<()> {
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
        let enable_spill =
            memory_settings.enable_query_level_spill || memory_settings.enable_global_level_spill;

        let sort_merge_output_schema = match output_order_col || enable_spill {
            true => add_order_field(self.schema.clone(), &self.sort_desc),
            false => self.schema.clone(),
        };

        let settings = self.ctx.get_settings();
        let enable_loser_tree = settings.get_enable_loser_tree_merge_sort()?;

        pipeline.add_transform(|input, output| {
            let builder = TransformSortMergeBuilder::create(
                input,
                output,
                sort_merge_output_schema.clone(),
                self.sort_desc.clone(),
                self.block_size,
            )
            .with_limit(self.limit)
            .with_order_col_generated(order_col_generated)
            .with_output_order_col(output_order_col || enable_spill)
            .with_memory_settings(memory_settings.clone())
            .with_enable_loser_tree(enable_loser_tree);

            Ok(ProcessorPtr::create(builder.build()?))
        })?;

        if enable_spill {
            let schema = add_order_field(sort_merge_output_schema.clone(), &self.sort_desc);
            let location_prefix = self.ctx.query_id_spill_prefix();

            let config = SpillerConfig {
                spiller_type: SpillerType::OrderBy,
                location_prefix,
                disk_spill: None,
                use_parquet: settings.get_spilling_file_format()?.is_parquet(),
            };
            pipeline.add_transform(|input, output| {
                let op = DataOperator::instance().spill_operator();
                let spiller = Spiller::create(self.ctx.clone(), op, config.clone())?;
                Ok(ProcessorPtr::create(create_transform_stream_sort_spill(
                    input,
                    output,
                    schema.clone(),
                    self.sort_desc.clone(),
                    self.limit,
                    spiller,
                    output_order_col,
                    enable_loser_tree,
                )))
            })?;
        }

        if !need_multi_merge {
            return Ok(());
        }

        self.build_multi_merge(pipeline)
    }

    pub fn build_multi_merge(self, pipeline: &mut Pipeline) -> Result<()> {
        // Multi-pipelines merge sort
        let settings = self.ctx.get_settings();
        let enable_loser_tree = settings.get_enable_loser_tree_merge_sort()?;
        let max_threads = settings.get_max_threads()? as usize;
        if settings.get_enable_parallel_multi_merge_sort()? {
            add_k_way_merge_sort(
                pipeline,
                self.schema.clone(),
                max_threads,
                self.block_size,
                self.limit,
                self.sort_desc,
                self.remove_order_col_at_last,
                enable_loser_tree,
            )
        } else {
            try_add_multi_sort_merge(
                pipeline,
                self.schema.clone(),
                self.block_size,
                self.limit,
                self.sort_desc,
                self.remove_order_col_at_last,
                enable_loser_tree,
            )
        }
    }
}
