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

use common_catalog::table::AppendMode;
use common_catalog::table::Table;
use common_exception::ErrorCode;
use common_exception::Result;
use common_expression::DataField;
use common_expression::DataSchemaRef;
use common_expression::DataSchemaRefExt;
use common_meta_app::schema::UpsertTableCopiedFileReq;
use common_pipeline_core::pipe::Pipe;
use common_pipeline_core::Pipeline;
use common_sql::executor::PhysicalPlanBuilder;
use common_sql::plans::Plan;
use common_sql::plans::RelOperator;
use common_sql::Planner;

use crate::pipelines::processors::transforms::TransformAddComputedColumns;
use crate::pipelines::processors::TransformResortAddOn;
use crate::pipelines::PipelineBuildResult;
use crate::schedulers::build_query_pipeline_without_render_result_set;
use crate::sessions::QueryContext;

pub fn build_fill_missing_columns_pipeline(
    ctx: Arc<QueryContext>,
    pipeline: &mut Pipeline,
    table: Arc<dyn Table>,
    source_schema: DataSchemaRef,
) -> Result<()> {
    let table_default_schema = &table.schema().remove_computed_fields();
    let table_computed_schema = &table.schema().remove_virtual_computed_fields();
    let default_schema: DataSchemaRef = Arc::new(table_default_schema.into());
    let computed_schema: DataSchemaRef = Arc::new(table_computed_schema.into());

    // Fill missing default columns and resort the columns.
    if source_schema != default_schema {
        pipeline.add_transform(|transform_input_port, transform_output_port| {
            TransformResortAddOn::try_create(
                ctx.clone(),
                transform_input_port,
                transform_output_port,
                source_schema.clone(),
                default_schema.clone(),
                table.clone(),
            )
        })?;
    }

    // Fill computed columns.
    if default_schema != computed_schema {
        pipeline.add_transform(|transform_input_port, transform_output_port| {
            TransformAddComputedColumns::try_create(
                ctx.clone(),
                transform_input_port,
                transform_output_port,
                default_schema.clone(),
                computed_schema.clone(),
            )
        })?;
    }

    Ok(())
}

pub async fn build_append2table_with_commit_pipeline(
    ctx: Arc<QueryContext>,
    main_pipeline: &mut Pipeline,
    table: Arc<dyn Table>,
    source_schema: DataSchemaRef,
    copied_files: Option<UpsertTableCopiedFileReq>,
    overwrite: bool,
    append_mode: AppendMode,
) -> Result<()> {
    build_fill_missing_columns_pipeline(ctx.clone(), main_pipeline, table.clone(), source_schema)?;

    table.append_data(ctx.clone(), main_pipeline, append_mode)?;

    let select_pipeline = get_agg_index_pipeline(ctx.clone()).await?;

    table.refresh_aggregating_indexes(ctx.clone(), main_pipeline)?;

    table.commit_insertion(ctx, main_pipeline, copied_files, overwrite, None)?;

    dbg!(&select_pipeline.main_pipeline);

    for pipe in select_pipeline.main_pipeline.pipes.clone() {
        if pipe.items[0]
            .name()
            .eq_ignore_ascii_case("SyncReadParquetDataSource")
        {
            continue;
        }
        main_pipeline.add_pipe(Pipe::create(1, 1, vec![pipe.items[0].clone()]));
    }

    dbg!(main_pipeline);

    Ok(())
}

async fn get_agg_index_pipeline(ctx: Arc<QueryContext>) -> Result<PipelineBuildResult> {
    let sql = "select sum_state(a) from t2";
    let mut planner = Planner::new(ctx.clone());
    let (plan, _) = planner.plan_sql(sql).await?;

    let (mut query_plan, output_schema, select_columns) = match plan {
        Plan::Query {
            s_expr,
            metadata,
            bind_context,
            ..
        } => {
            let schema = if let RelOperator::EvalScalar(eval) = s_expr.plan() {
                let fields = eval
                    .items
                    .iter()
                    .map(|item| {
                        let ty = item.scalar.data_type()?;
                        Ok(DataField::new(&item.index.to_string(), ty))
                    })
                    .collect::<Result<Vec<_>>>()?;
                DataSchemaRefExt::create(fields)
            } else {
                return Err(ErrorCode::SemanticError(
                    "The last operator of the plan of aggregate index query should be EvalScalar",
                ));
            };

            let mut builder = PhysicalPlanBuilder::new(metadata.clone(), ctx.clone(), false);
            (
                builder.build(s_expr.as_ref()).await?,
                schema,
                bind_context.columns.clone(),
            )
        }
        _ => {
            return Err(ErrorCode::SemanticError(
                "Refresh aggregating index encounter Non-Query Plan",
            ));
        }
    };

    let build_res =
        build_query_pipeline_without_render_result_set(&ctx, &query_plan, false).await?;

    Ok(build_res)
}

pub fn build_append2table_without_commit_pipeline(
    ctx: Arc<QueryContext>,
    main_pipeline: &mut Pipeline,
    table: Arc<dyn Table>,
    source_schema: DataSchemaRef,
    append_mode: AppendMode,
) -> Result<()> {
    build_fill_missing_columns_pipeline(ctx.clone(), main_pipeline, table.clone(), source_schema)?;

    table.append_data(ctx, main_pipeline, append_mode)?;

    Ok(())
}
