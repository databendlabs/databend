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
use common_streams::SendableDataBlockStream;
use common_tracing::tracing;
use common_tracing::tracing::debug;

use crate::interpreters::fragments::QueryFragmentsActions;
use crate::interpreters::fragments::QueryFragmentsBuilder;
use crate::interpreters::fragments::RootQueryFragment;
use crate::interpreters::plan_schedulers;
use crate::interpreters::plan_schedulers::Scheduled;
use crate::interpreters::plan_schedulers::ScheduledStream;
use crate::interpreters::PlanScheduler;
use crate::pipelines::new::NewPipeline;
use crate::pipelines::new::QueryPipelineBuilder;
use crate::pipelines::processors::PipelineBuilder;
use crate::sessions::QueryContext;

#[tracing::instrument(level = "debug", skip(ctx), fields(ctx.id = ctx.get_id().as_str()))]
pub async fn schedule_query(
    ctx: &Arc<QueryContext>,
    plan: &PlanNode,
) -> Result<SendableDataBlockStream> {
    let scheduler = PlanScheduler::try_create(ctx.clone())?;
    let scheduled_tasks = scheduler.reschedule(plan)?;
    let remote_stage_actions = scheduled_tasks.get_tasks()?;

    let config = ctx.get_config();
    let cluster = ctx.get_cluster();
    let timeout = ctx.get_settings().get_flight_client_timeout()?;
    let mut scheduled = Scheduled::new();
    for (node, action) in remote_stage_actions {
        println!("node {} execute action {:?}", node.id, action);
        let mut flight_client = cluster.create_node_conn(&node.id, &config).await?;
        let executing_action = flight_client.execute_action(action.clone(), timeout);

        executing_action.await?;
        scheduled.insert(node.id.clone(), node.clone());
    }

    let pipeline_builder = PipelineBuilder::create(ctx.clone());
    let mut in_local_pipeline = pipeline_builder.build(&scheduled_tasks.get_local_task())?;
    tracing::log::debug!("local_pipeline:\n{:?}", in_local_pipeline);

    match in_local_pipeline.execute().await {
        Ok(stream) => Ok(ScheduledStream::create(ctx.clone(), scheduled, stream)),
        Err(error) => {
            plan_schedulers::handle_error(ctx, scheduled, timeout).await;
            Err(error)
        }
    }
}

async fn schedule_query_impl(ctx: Arc<QueryContext>, plan: &PlanNode) -> Result<NewPipeline> {
    let query_fragments = QueryFragmentsBuilder::build(ctx.clone(), plan)?;
    let root_query_fragments = RootQueryFragment::create(query_fragments, ctx.clone(), plan)?;

    if !root_query_fragments.distribute_query()? {
        return QueryPipelineBuilder::create(ctx.clone()).finalize(plan);
    }

    let exchange_manager = ctx.get_exchange_manager();
    let mut fragments_actions = QueryFragmentsActions::create(ctx.clone());
    root_query_fragments.finalize(&mut fragments_actions)?;

    println!("QueryFragments actions: {:?}", fragments_actions);
    debug!("QueryFragments actions: {:?}", fragments_actions);

    exchange_manager
        .commit_actions(ctx, fragments_actions)
        .await
}

pub async fn schedule_query_new(ctx: Arc<QueryContext>, plan: &PlanNode) -> Result<NewPipeline> {
    let settings = ctx.get_settings();
    let mut pipeline = schedule_query_impl(ctx, plan).await?;
    pipeline.set_max_threads(settings.get_max_threads()? as usize);
    Ok(pipeline)
}
