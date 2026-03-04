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

use std::collections::HashMap;
use std::sync::Arc;

use databend_common_catalog::table::Table;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::BlockThresholds;
use databend_common_expression::DataSchema;
use databend_common_expression::DataSchemaRef;
use databend_common_pipeline::basic::create_resize_item;
use databend_common_pipeline::core::Pipe;
use databend_common_pipeline::core::ProcessorPtr;
use databend_common_pipeline_transforms::blocks::CastSchemaBranch;
use databend_common_pipeline_transforms::blocks::TransformBranchedCastSchema;
use databend_common_pipeline_transforms::blocks::build_cast_exprs;
use databend_common_pipeline_transforms::columns::TransformAddComputedColumns;
use databend_common_pipeline_transforms::processors::AccumulatingTransformer;
use databend_common_pipeline_transforms::processors::BlockCompactBuilder;
use databend_common_pipeline_transforms::processors::BlockMetaTransformer;
use databend_common_pipeline_transforms::processors::TransformCompactBlock;
use databend_common_pipeline_transforms::processors::TransformPipelineHelper;
use databend_common_pipeline_transforms::processors::create_dummy_item;
use databend_common_sql::DefaultExprBinder;
use databend_common_storages_fuse::FuseTable;
use databend_common_storages_fuse::TableContext;
use databend_common_storages_fuse::operations::UnMatchedExprs;

use crate::pipelines::PipelineBuilder;
use crate::pipelines::processors::transforms::AsyncFunctionBranch;
use crate::pipelines::processors::transforms::ReadFileContext;
use crate::pipelines::processors::transforms::TransformAsyncFunction;
use crate::pipelines::processors::transforms::TransformBranchedAsyncFunction;
use crate::pipelines::processors::transforms::TransformResortAddOnWithoutSourceSchema;
use crate::pipelines::processors::transforms::build_expression_transform;

impl PipelineBuilder {
    pub fn build_fill_columns_in_merge_into(
        &mut self,
        tbl: Arc<dyn Table>,
        transform_len: usize,
        need_match: bool,
        unmatched: UnMatchedExprs,
    ) -> Result<()> {
        let table = FuseTable::try_from_table(tbl.as_ref())?;

        // fill default columns
        let table_default_schema = &table.schema_with_stream().remove_computed_fields();
        let default_schema: DataSchemaRef = Arc::new(table_default_schema.into());

        let mut expression_transforms = Vec::with_capacity(unmatched.len());
        let mut data_schemas = HashMap::with_capacity(unmatched.len());
        let mut trigger_non_null_errors = Vec::with_capacity(unmatched.len());
        let mut async_function_branches = HashMap::with_capacity(unmatched.len());
        let mut cast_schema_branches = HashMap::with_capacity(unmatched.len());
        for (idx, item) in unmatched.iter().enumerate() {
            let mut input_schema = item.0.clone();
            let mut default_expr_binder = DefaultExprBinder::try_new(self.ctx.clone())?;
            if let Some((async_funcs, new_default_schema, new_default_schema_no_cast)) =
                default_expr_binder
                    .split_async_default_exprs(input_schema.clone(), default_schema.clone())?
            {
                let sequence_counters =
                    TransformAsyncFunction::create_sequence_counters(async_funcs.len());
                async_function_branches.insert(idx, AsyncFunctionBranch {
                    async_func_descs: async_funcs,
                    sequence_counters,
                });

                if new_default_schema != new_default_schema_no_cast {
                    cast_schema_branches.insert(idx, CastSchemaBranch {
                        to_schema: new_default_schema.clone(),
                        from_schema: new_default_schema_no_cast.clone(),
                        exprs: build_cast_exprs(
                            new_default_schema_no_cast.clone(),
                            new_default_schema.clone(),
                        )?,
                    });
                }
                // update input_schema, which is used in `TransformResortAddOnWithoutSourceSchema`
                input_schema = new_default_schema;
            }

            data_schemas.insert(idx, input_schema.clone());
            match build_expression_transform(
                input_schema,
                default_schema.clone(),
                tbl.clone(),
                self.ctx.clone(),
            ) {
                Ok(expression_transform) => {
                    expression_transforms.push(Some(expression_transform));
                    trigger_non_null_errors.push(None);
                }
                Err(err) => {
                    if err.code() != ErrorCode::BAD_ARGUMENTS {
                        return Err(err);
                    }

                    expression_transforms.push(None);
                    trigger_non_null_errors.push(Some(err));
                }
            };
        }

        if !async_function_branches.is_empty() {
            let branches = Arc::new(async_function_branches);

            let mut builder = self
                .main_pipeline
                .try_create_async_transform_pipeline_builder_with_len(
                    || {
                        Ok(TransformBranchedAsyncFunction {
                            ctx: self.ctx.clone(),
                            branches: branches.clone(),
                            read_file_ctx: ReadFileContext::try_new(&self.ctx)?,
                        })
                    },
                    transform_len,
                )?;
            if need_match {
                builder.add_items_prepend(vec![create_dummy_item()]);
            }
            self.main_pipeline.add_pipe(builder.finalize());
        }

        if !cast_schema_branches.is_empty() {
            let branches = Arc::new(cast_schema_branches);
            let mut builder = self
                .main_pipeline
                .try_create_transform_pipeline_builder_with_len(
                    || {
                        Ok(TransformBranchedCastSchema {
                            func_ctx: self.ctx.get_function_context()?,
                            branches: branches.clone(),
                        })
                    },
                    transform_len,
                )?;
            if need_match {
                builder.add_items_prepend(vec![create_dummy_item()]);
            }
            self.main_pipeline.add_pipe(builder.finalize());
        }

        let mut builder = self
            .main_pipeline
            .try_create_transform_pipeline_builder_with_len(
                || {
                    TransformResortAddOnWithoutSourceSchema::try_new(
                        self.ctx.clone(),
                        Arc::new(DataSchema::from(table_default_schema)),
                        tbl.clone(),
                        Arc::new(DataSchema::from(table.schema_with_stream())),
                        data_schemas.clone(),
                        expression_transforms.clone(),
                        trigger_non_null_errors.clone(),
                    )
                },
                transform_len,
            )?;
        if need_match {
            builder.add_items_prepend(vec![create_dummy_item()]);
        }
        self.main_pipeline.add_pipe(builder.finalize());

        // fill computed columns
        let table_computed_schema = &table.schema_with_stream().remove_virtual_computed_fields();
        let default_schema: DataSchemaRef = Arc::new(table_default_schema.into());
        let computed_schema: DataSchemaRef = Arc::new(table_computed_schema.into());
        if default_schema != computed_schema {
            builder = self
                .main_pipeline
                .try_create_transform_pipeline_builder_with_len(
                    || {
                        TransformAddComputedColumns::try_new(
                            self.ctx.clone(),
                            default_schema.clone(),
                            computed_schema.clone(),
                        )
                    },
                    transform_len,
                )?;
            if need_match {
                builder.add_items_prepend(vec![create_dummy_item()]);
            }
            self.main_pipeline.add_pipe(builder.finalize());
        }
        Ok(())
    }

    pub fn build_compact_and_cluster_sort_in_merge_into(
        &mut self,
        table: &FuseTable,
        need_match: bool,
        transform_len: usize,
        block_thresholds: BlockThresholds,
    ) -> Result<()> {
        // we should avoid too much little block write, because for s3 write, there are too many
        // little blocks, it will cause high latency.
        let mut origin_len = transform_len;
        let mut resize_len = 1;
        let mut pipe_items = Vec::with_capacity(2);
        if need_match {
            origin_len += 1;
            resize_len += 1;
            pipe_items.push(create_dummy_item());
        }
        pipe_items.push(create_resize_item(transform_len, 1));
        self.main_pipeline
            .add_pipe(Pipe::create(origin_len, resize_len, pipe_items));

        let mut builder = self.main_pipeline.add_transform_with_specified_len(
            |transform_input_port, transform_output_port| {
                Ok(ProcessorPtr::create(AccumulatingTransformer::create(
                    transform_input_port,
                    transform_output_port,
                    BlockCompactBuilder::new(block_thresholds),
                )))
            },
            1,
        )?;
        if need_match {
            builder.add_items_prepend(vec![create_dummy_item()]);
        }
        self.main_pipeline.add_pipe(builder.finalize());

        let mut pipe_items = Vec::with_capacity(2);
        if need_match {
            pipe_items.push(create_dummy_item());
        }
        pipe_items.push(create_resize_item(1, transform_len));
        self.main_pipeline
            .add_pipe(Pipe::create(resize_len, origin_len, pipe_items));

        let mut builder = self.main_pipeline.add_transform_with_specified_len(
            |transform_input_port, transform_output_port| {
                Ok(ProcessorPtr::create(BlockMetaTransformer::create(
                    transform_input_port,
                    transform_output_port,
                    TransformCompactBlock::default(),
                )))
            },
            transform_len,
        )?;
        if need_match {
            builder.add_items_prepend(vec![create_dummy_item()]);
        }
        self.main_pipeline.add_pipe(builder.finalize());

        // cluster sort
        table.cluster_gen_for_append_with_specified_len(
            self.ctx.clone(),
            &mut self.main_pipeline,
            block_thresholds,
            transform_len,
            need_match,
        )?;
        Ok(())
    }
}
