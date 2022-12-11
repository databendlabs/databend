// Copyright 2022 Datafuse Labs.
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
use common_datavalues::DataSchemaRef;
use common_exception::Result;
use common_pipeline_core::Pipeline;

use crate::pipelines::processors::TransformAddOn;
use crate::pipelines::PipelineBuildResult;
use crate::sessions::QueryContext;

fn fill_missing_columns(
    ctx: Arc<QueryContext>,
    source_schema: &DataSchemaRef,
    target_schema: &DataSchemaRef,
    pipeline: &mut Pipeline,
) -> Result<()> {
    let need_fill_missing_columns = target_schema != source_schema;
    if need_fill_missing_columns {
        pipeline.add_transform(|transform_input_port, transform_output_port| {
            TransformAddOn::try_create(
                transform_input_port,
                transform_output_port,
                source_schema.clone(),
                target_schema.clone(),
                ctx.clone(),
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
        &table.schema(),
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
                    table.commit_insertion(ctx, append_entries, overwrite).await
                });
            }

            Err(may_error.as_ref().unwrap().clone())
        });
    }

    Ok(())
}
