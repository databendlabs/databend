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

use databend_common_catalog::table::Table;
use databend_common_exception::Result;
use databend_common_expression::DataField;
use databend_common_expression::DataSchema;
use databend_common_expression::DataSchemaRef;
use databend_common_expression::DataSchemaRefExt;
use databend_common_expression::SortColumnDescription;
use databend_common_functions::BUILTIN_FUNCTIONS;
use databend_common_meta_app::schema::UpdateStreamMetaReq;
use databend_common_meta_app::schema::UpsertTableCopiedFileReq;
use databend_common_pipeline::core::Pipeline;
use databend_common_pipeline_transforms::TransformPipelineHelper;
use databend_common_pipeline_transforms::blocks::CompoundBlockOperator;
use databend_common_sql::evaluator::BlockOperator;
use databend_common_storages_fuse::FuseTable;
use databend_storages_common_table_meta::meta::TableMetaTimestamps;

use crate::pipelines::PipelineBuilder;
use crate::pipelines::builders::SortPipelineBuilder;
use crate::sessions::QueryContext;
use crate::sessions::TableContextSettings;

/// This file implements append to table pipeline builder.
impl PipelineBuilder {
    /// Coalesce hash-partitioned writes on the current node. Distributed inputs
    /// must route the same partition keys to one node before reaching this stage.
    pub fn build_table_write_layout(
        ctx: Arc<QueryContext>,
        pipeline: &mut Pipeline,
        table: Arc<dyn Table>,
    ) -> Result<()> {
        if table.engine() != "FUSE" {
            return Ok(());
        }
        let fuse_table = FuseTable::try_from_table(table.as_ref())?;
        if !fuse_table.use_hash_write_distribution() {
            return Ok(());
        }

        let Some(partition_info) = fuse_table.partition_pruning_info(ctx.clone()) else {
            return Ok(());
        };
        let table_schema = table.schema().remove_virtual_computed_fields();
        let input_schema = DataSchema::from(&table_schema);
        let num_input_columns = input_schema.num_fields();
        let partition_exprs = partition_info
            .partition_keys
            .into_iter()
            .map(|key| {
                key.as_expr(&BUILTIN_FUNCTIONS)
                    .project_column_ref(|name| input_schema.index_of(name))
            })
            .collect::<Result<Vec<_>>>()?;

        let mut fields = input_schema.fields().clone();
        let mut sort_desc = Vec::with_capacity(partition_exprs.len());
        for (index, expr) in partition_exprs.iter().enumerate() {
            fields.push(DataField::new(
                &format!("partition_key_{index}"),
                expr.data_type().clone(),
            ));
            sort_desc.push(SortColumnDescription {
                offset: num_input_columns + index,
                asc: true,
                nulls_first: false,
            });
        }

        let projections = (0..fields.len()).collect();
        let op = BlockOperator::Map {
            exprs: partition_exprs,
            projections: Some(projections),
        };
        let func_ctx = ctx.get_function_context()?;
        pipeline.add_transformer(|| {
            CompoundBlockOperator::new(vec![op.clone()], func_ctx.clone(), num_input_columns)
        });

        let num_sort_columns = fields.len();
        let sort_schema = DataSchemaRefExt::create(fields);
        let enable_fixed_rows = ctx.get_settings().get_enable_fixed_rows_sort()?;
        let sort_builder = SortPipelineBuilder::create(
            ctx.clone(),
            sort_schema,
            sort_desc.into(),
            None,
            enable_fixed_rows,
        )?
        .with_block_size_hit(table.get_block_thresholds().max_rows_per_block);
        let max_threads = ctx.get_settings().get_max_threads()? as usize;
        if pipeline.output_len() == 1 || max_threads == 1 {
            pipeline.try_resize(max_threads)?;
        }
        sort_builder.build_full_sort_pipeline(pipeline, false)?;

        let projection = (0..num_input_columns).collect::<Vec<_>>();
        pipeline.add_transformer(|| {
            CompoundBlockOperator::new(
                vec![BlockOperator::Project {
                    projection: projection.clone(),
                }],
                func_ctx.clone(),
                num_sort_columns,
            )
        });
        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
    pub fn build_append2table_with_commit_pipeline(
        ctx: Arc<QueryContext>,
        main_pipeline: &mut Pipeline,
        table: Arc<dyn Table>,
        source_schema: DataSchemaRef,
        copied_files: Option<UpsertTableCopiedFileReq>,
        update_stream_meta: Vec<UpdateStreamMetaReq>,
        overwrite: bool,
        deduplicated_label: Option<String>,
        table_meta_timestamps: TableMetaTimestamps,
    ) -> Result<()> {
        Self::fill_and_reorder_columns(ctx.clone(), main_pipeline, table.clone(), source_schema)?;
        Self::build_table_write_layout(ctx.clone(), main_pipeline, table.clone())?;

        table.append_data(ctx.clone(), main_pipeline, table_meta_timestamps)?;
        table.commit_insertion(
            ctx,
            main_pipeline,
            copied_files,
            update_stream_meta,
            overwrite,
            None,
            deduplicated_label,
            table_meta_timestamps,
        )?;

        Ok(())
    }

    pub fn build_append2table_without_commit_pipeline(
        ctx: Arc<QueryContext>,
        main_pipeline: &mut Pipeline,
        table: Arc<dyn Table>,
        source_schema: DataSchemaRef,
        table_meta_timestamps: TableMetaTimestamps,
    ) -> Result<()> {
        Self::fill_and_reorder_columns(ctx.clone(), main_pipeline, table.clone(), source_schema)?;
        Self::build_table_write_layout(ctx.clone(), main_pipeline, table.clone())?;

        table.append_data(ctx, main_pipeline, table_meta_timestamps)?;

        Ok(())
    }
}
