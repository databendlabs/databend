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

use common_base::runtime::GlobalIORuntime;
use common_catalog::table::AppendMode;
use common_catalog::table::Table;
use common_catalog::table_context::TableContext;
use common_exception::Result;
use common_expression::ComputedExpr;
use common_expression::DataField;
use common_expression::DataSchemaRef;
use common_expression::DataSchemaRefExt;
use common_pipeline_core::Pipeline;

use crate::pipelines::processors::transforms::TransformAddComputedColumns;
use crate::pipelines::processors::TransformResortAddOn;
use crate::pipelines::PipelineBuildResult;
use crate::sessions::QueryContext;

pub fn fill_missing_columns(
    ctx: Arc<QueryContext>,
    source_schema: &DataSchemaRef,
    table: Arc<dyn Table>,
    pipeline: &mut Pipeline,
) -> Result<()> {
    let with_default_fields = table
        .schema()
        .fields()
        .iter()
        .filter(|f| f.computed_expr().is_none())
        .map(DataField::from)
        .collect::<Vec<_>>();

    let with_computed_fields = table
        .schema()
        .fields()
        .iter()
        .filter(|f| !matches!(f.computed_expr(), Some(ComputedExpr::Virtual(_))))
        .map(DataField::from)
        .collect::<Vec<_>>();

    // Fill missing default columns.
    if source_schema.fields().len() < with_default_fields.len() {
        let output_schema = DataSchemaRefExt::create(with_default_fields.clone());
        pipeline.add_transform(|transform_input_port, transform_output_port| {
            TransformResortAddOn::try_create(
                ctx.clone(),
                transform_input_port,
                transform_output_port,
                source_schema.clone(),
                output_schema.clone(),
                table.clone(),
            )
        })?;
    }

    // Fill computed columns.
    if with_default_fields.len() < with_computed_fields.len() {
        let input_schema = DataSchemaRefExt::create(with_default_fields);
        let output_schema = DataSchemaRefExt::create(with_computed_fields);
        pipeline.add_transform(|transform_input_port, transform_output_port| {
            TransformAddComputedColumns::try_create(
                ctx.clone(),
                transform_input_port,
                transform_output_port,
                input_schema.clone(),
                output_schema.clone(),
                table.clone(),
            )
        })?;
    }

    Ok(())
}

pub fn append2table(
    ctx: Arc<QueryContext>,
    table: Arc<dyn Table>,
    source_schema: DataSchemaRef,
    build_res: &mut PipelineBuildResult,
    overwrite: bool,
    need_commit: bool,
    append_mode: AppendMode,
) -> Result<()> {
    fill_missing_columns(
        ctx.clone(),
        &source_schema,
        table.clone(),
        &mut build_res.main_pipeline,
    )?;

    table.append_data(
        ctx.clone(),
        &mut build_res.main_pipeline,
        append_mode,
        false,
    )?;

    if need_commit {
        build_res.main_pipeline.set_on_finished(move |may_error| {
            // capture out variable
            let overwrite = overwrite;
            let ctx = ctx.clone();
            let table = table.clone();

            if may_error.is_none() {
                let append_entries = ctx.consume_precommit_blocks();
                // We must put the commit operation to global runtime, which will avoid the "dispatch dropped without returning error" in tower
                return GlobalIORuntime::instance().block_on(async move {
                    table
                        .commit_insertion(ctx, append_entries, None, overwrite)
                        .await
                });
            }

            Err(may_error.as_ref().unwrap().clone())
        });
    }

    Ok(())
}
