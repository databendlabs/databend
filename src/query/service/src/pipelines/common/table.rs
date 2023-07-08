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
use common_catalog::table_context::TableContext;
use common_exception::ErrorCode;
use common_exception::Result;
use common_expression::ComputedExpr;
use common_expression::DataSchemaRef;
use common_expression::DataSchemaRefExt;
use common_expression::TableSchemaRef;
use common_meta_app::schema::UpsertTableCopiedFileReq;
use common_pipeline_core::Pipeline;
use common_sql::parse_computed_expr;

use crate::pipelines::processors::transforms::TransformAddComputedColumns;
use crate::pipelines::processors::TransformResortAddOn;
use crate::sessions::QueryContext;

pub fn fill_missing_columns(
    ctx: Arc<QueryContext>,
    table: Arc<dyn Table>,
    source_schema: DataSchemaRef,
    pipeline: &mut Pipeline,
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

pub fn append2table(
    ctx: Arc<QueryContext>,
    table: Arc<dyn Table>,
    source_schema: DataSchemaRef,
    main_pipeline: &mut Pipeline,
    copied_files: Option<UpsertTableCopiedFileReq>,
    overwrite: bool,
    append_mode: AppendMode,
) -> Result<()> {
    fill_missing_columns(ctx.clone(), table.clone(), source_schema, main_pipeline)?;

    table.append_data(ctx.clone(), main_pipeline, append_mode)?;

    table.commit_insertion(ctx, main_pipeline, copied_files, overwrite)?;

    Ok(())
}

pub fn append2table_without_commit(
    ctx: Arc<QueryContext>,
    table: Arc<dyn Table>,
    source_schema: DataSchemaRef,
    main_pipeline: &mut Pipeline,
    append_mode: AppendMode,
) -> Result<()> {
    fill_missing_columns(ctx.clone(), table.clone(), source_schema, main_pipeline)?;

    table.append_data(ctx, main_pipeline, append_mode)?;

    Ok(())
}

pub fn check_referenced_computed_columns(
    ctx: Arc<dyn TableContext>,
    table_schema: TableSchemaRef,
    column: &str,
) -> Result<()> {
    let fields = table_schema
        .fields()
        .iter()
        .filter(|f| f.name != column)
        .map(|f| f.into())
        .collect::<Vec<_>>();
    let schema = DataSchemaRefExt::create(fields);

    for f in schema.fields() {
        if let Some(computed_expr) = f.computed_expr() {
            let expr = match computed_expr {
                ComputedExpr::Stored(expr) => expr.clone(),
                ComputedExpr::Virtual(expr) => expr.clone(),
            };
            if parse_computed_expr(ctx.clone(), schema.clone(), &expr).is_err() {
                return Err(ErrorCode::ColumnReferencedByComputedColumn(format!(
                    "column `{}` is referenced by computed column `{}`",
                    column,
                    &f.name()
                )));
            }
        }
    }
    Ok(())
}
