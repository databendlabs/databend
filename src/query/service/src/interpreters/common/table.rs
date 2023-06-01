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
use common_expression::DataSchemaRef;
use common_pipeline_core::Pipeline;

use crate::pipelines::processors::TransformResortAddOn;
use crate::pipelines::PipelineBuildResult;
use crate::sessions::QueryContext;

fn fill_missing_columns(
    ctx: Arc<QueryContext>,
    source_schema: &DataSchemaRef,
    table: Arc<dyn Table>,
    pipeline: &mut Pipeline,
) -> Result<()> {
    pipeline.add_transform(|transform_input_port, transform_output_port| {
        TransformResortAddOn::try_create(
            ctx.clone(),
            transform_input_port,
            transform_output_port,
            source_schema.clone(),
            table.clone(),
        )
    })?;
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

    table.append_data(ctx.clone(), &mut build_res.main_pipeline, append_mode)?;

    if need_commit {
        table.commit_insertion(ctx, &mut build_res.main_pipeline, None, overwrite)?;
    }

    Ok(())
}
