//  Copyright 2021 Datafuse Labs.
//
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.
//

use std::sync::Arc;

use common_exception::Result;
use common_planners::PlanNode;
use common_tracing::tracing::debug;

use crate::interpreters::fragments::QueryFragmentsActions;
use crate::interpreters::fragments::QueryFragmentsBuilder;
use crate::interpreters::fragments::RootQueryFragment;
use crate::pipelines::PipelineBuildResult;
use crate::pipelines::QueryPipelineBuilder;
use crate::sessions::QueryContext;
use crate::sessions::TableContext;

async fn schedule_query_impl(
    ctx: Arc<QueryContext>,
    plan: &PlanNode,
) -> Result<PipelineBuildResult> {
    let query_fragments = QueryFragmentsBuilder::build(ctx.clone(), plan)?;
    let root_query_fragments = RootQueryFragment::create(query_fragments, ctx.clone(), plan)?;

    if !root_query_fragments.distribute_query()? {
        return QueryPipelineBuilder::create(ctx.clone()).finalize(plan);
    }

    let exchange_manager = ctx.get_exchange_manager();
    let mut fragments_actions = QueryFragmentsActions::create(ctx.clone());
    root_query_fragments.finalize(&mut fragments_actions)?;

    debug!("QueryFragments actions: {:?}", fragments_actions);

    // TODO: move commit into pipeline processor(e.g CommitActionProcessor). It can help us make
    // Interpreter::execute as sync method
    exchange_manager
        .commit_actions(ctx, fragments_actions)
        .await
}

pub async fn schedule_query_new(
    ctx: Arc<QueryContext>,
    plan: &PlanNode,
) -> Result<PipelineBuildResult> {
    let settings = ctx.get_settings();
    let mut pipeline = schedule_query_impl(ctx, plan).await?;
    pipeline.set_max_threads(settings.get_max_threads()? as usize);
    Ok(pipeline)
}
