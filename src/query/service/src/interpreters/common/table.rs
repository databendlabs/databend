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
use common_exception::Result;
use common_expression::DataSchema;
use common_expression::DataSchemaRef;
use common_meta_app::schema::UpsertTableCopiedFileReq;
use common_pipeline_core::Pipeline;

use crate::pipelines::processors::transforms::TransformAddComputedColumns;
use crate::pipelines::processors::TransformResortAddOn;
use crate::pipelines::PipelineBuildResult;
use crate::sessions::QueryContext;

pub fn fill_missing_columns(
    ctx: Arc<QueryContext>,
    table: Arc<dyn Table>,
    source_schema: DataSchemaRef,
    pipeline: &mut Pipeline,
) -> Result<()> {
    let table_default_schema = &table.schema().remove_computed_fields();
    let table_computed_schema = &table.schema().remove_virtual_computed_fields();
    let default_schema = Arc::new(DataSchema::from(table_default_schema));
    let computed_schema = Arc::new(DataSchema::from(table_computed_schema));

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
    build_res: &mut PipelineBuildResult,
    copied_files: Option<UpsertTableCopiedFileReq>,
    overwrite: bool,
    append_mode: AppendMode,
) -> Result<()> {
    fill_missing_columns(
        ctx.clone(),
        table.clone(),
        source_schema,
        &mut build_res.main_pipeline,
    )?;

    table.append_data(ctx.clone(), &mut build_res.main_pipeline, append_mode)?;

    table.commit_insertion(ctx, &mut build_res.main_pipeline, copied_files, overwrite)?;

    Ok(())
}
