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

use crate::pipelines::PipelineBuildResult;
use crate::pipelines::PipelineBuilder;
use crate::schedulers::Fragmenter;
use crate::schedulers::QueryFragmentsActions;
use crate::sessions::QueryContext;
use crate::sessions::TableContext;
use crate::sql::executor::PhysicalPlan;
use crate::sql::ColumnBinding;

/// Build query pipeline from physical plan.
/// If plan is distributed plan it will build_distributed_pipeline
/// else build_local_pipeline.
#[async_backtrace::framed]
pub async fn build_query_pipeline(
    ctx: &Arc<QueryContext>,
    result_columns: &[ColumnBinding],
    plan: &PhysicalPlan,
    ignore_result: bool,
) -> Result<PipelineBuildResult> {
    let mut build_res = build_query_pipeline_without_render_result_set(ctx, plan).await?;
    if matches!(plan, PhysicalPlan::UnionAll { .. }) {
        // Union doesn't need to add extra processor to project the result.
        // It will be handled in union processor.
        return Ok(build_res);
    }
    let input_schema = plan.output_schema()?;
    PipelineBuilder::build_result_projection(
        &ctx.get_function_context()?,
        input_schema,
        result_columns,
        &mut build_res.main_pipeline,
        ignore_result,
    )?;
    Ok(build_res)
}

#[async_backtrace::framed]
pub async fn build_query_pipeline_without_render_result_set(
    ctx: &Arc<QueryContext>,
    plan: &PhysicalPlan,
) -> Result<PipelineBuildResult> {
    let build_res = if !plan.is_distributed_plan() {
        build_local_pipeline(ctx, plan).await
    } else {
        build_distributed_pipeline(ctx, plan).await
    }?;
    Ok(build_res)
}

/// Build local pipeline.
#[async_backtrace::framed]
pub async fn build_local_pipeline(
    ctx: &Arc<QueryContext>,
    plan: &PhysicalPlan,
) -> Result<PipelineBuildResult> {
    let pipeline = PipelineBuilder::create(
        ctx.get_function_context()?,
        ctx.get_settings(),
        ctx.clone(),
        vec![],
    );
    let mut build_res = pipeline.finalize(plan)?;

    let settings = ctx.get_settings();
    build_res.set_max_threads(settings.get_max_threads()? as usize);
    Ok(build_res)
}

/// Build distributed pipeline via fragment and actions.
#[async_backtrace::framed]
pub async fn build_distributed_pipeline(
    ctx: &Arc<QueryContext>,
    plan: &PhysicalPlan,
) -> Result<PipelineBuildResult> {
    let fragmenter = Fragmenter::try_create(ctx.clone())?;

    let root_fragment = fragmenter.build_fragment(plan)?;
    let mut fragments_actions = QueryFragmentsActions::create(ctx.clone());
    root_fragment.get_actions(ctx.clone(), &mut fragments_actions)?;

    let exchange_manager = ctx.get_exchange_manager();

    let mut build_res = exchange_manager
        .commit_actions(ctx.clone(), fragments_actions)
        .await?;

    let settings = ctx.get_settings();
    build_res.set_max_threads(settings.get_max_threads()? as usize);
    Ok(build_res)
}
