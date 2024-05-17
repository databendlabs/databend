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

use databend_common_base::base::tokio;
use databend_common_exception::Result;
use databend_common_expression::SendableDataBlockStream;
use databend_common_sql::executor::physical_plans::HashJoin;
use databend_common_sql::executor::PhysicalPlan;
use databend_common_sql::executor::PhysicalPlanBuilder;
use databend_common_sql::plans::Plan;
use databend_common_sql::Planner;
use databend_query::interpreters::InterpreterFactory;
use databend_query::pipelines::processors::HashJoinBuildState;
use databend_query::pipelines::processors::HashJoinDesc;
use databend_query::pipelines::processors::HashJoinState;
use databend_query::sessions::QueryContext;
use databend_query::sessions::TableContext;
use databend_query::test_kits::TestFixture;

async fn plan_sql(ctx: Arc<QueryContext>, sql: &str) -> Result<Plan> {
    let mut planner = Planner::new(ctx.clone());
    let (plan, _) = planner.plan_sql(sql).await?;
    Ok(plan)
}

async fn execute_sql(ctx: Arc<QueryContext>, sql: &str) -> Result<SendableDataBlockStream> {
    let plan = plan_sql(ctx.clone(), sql).await?;
    let it = InterpreterFactory::get(ctx.clone(), &plan).await?;
    it.execute(ctx).await
}

async fn physical_plan(ctx: Arc<QueryContext>, sql: &str) -> Result<PhysicalPlan> {
    let plan = plan_sql(ctx.clone(), sql).await?;
    match plan {
        Plan::Query {
            s_expr,
            metadata,
            bind_context,
            ..
        } => {
            let mut builder = PhysicalPlanBuilder::new(metadata.clone(), ctx, false);
            builder.build(&s_expr, bind_context.column_set()).await
        }
        _ => unreachable!("Query plan expected"),
    }
}

// The method is used to find the join in the physical plan.
// The physical plan should be a simple tree which only contains one binary operator and the binary operator is join.
fn find_join(plan: &PhysicalPlan) -> Result<HashJoin> {
    match plan {
        PhysicalPlan::HashJoin(join) => Ok(join.clone()),
        PhysicalPlan::Filter(plan) => find_join(plan.input.as_ref()),
        PhysicalPlan::Project(plan) => find_join(plan.input.as_ref()),
        PhysicalPlan::EvalScalar(plan) => find_join(plan.input.as_ref()),
        PhysicalPlan::ProjectSet(plan) => find_join(plan.input.as_ref()),
        PhysicalPlan::AggregateExpand(plan) => find_join(plan.input.as_ref()),
        PhysicalPlan::AggregatePartial(plan) => find_join(plan.input.as_ref()),
        PhysicalPlan::AggregateFinal(plan) => find_join(plan.input.as_ref()),
        PhysicalPlan::Window(plan) => find_join(plan.input.as_ref()),
        PhysicalPlan::Sort(plan) => find_join(plan.input.as_ref()),
        PhysicalPlan::Limit(plan) => find_join(plan.input.as_ref()),
        PhysicalPlan::RowFetch(plan) => find_join(plan.input.as_ref()),
        _ => unreachable!("unexpected plan: {:?}", plan.name()),
    }
}

async fn join_build_state(
    ctx: &Arc<QueryContext>,
    join: &HashJoin,
) -> Result<Arc<HashJoinBuildState>> {
    let func_ctx = ctx.get_function_context()?;

    let join_state = HashJoinState::try_create(
        ctx.clone(),
        join.build.output_schema()?,
        &join.build_projections,
        HashJoinDesc::create(join)?,
        &join.probe_to_build,
        false,
        true,
        None,
    )?;
    let build_state = HashJoinBuildState::try_create(
        ctx.clone(),
        func_ctx,
        &join.build_keys,
        &join.build_projections,
        join_state.clone(),
        1,
    )?;
    Ok(build_state)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_generate_runtime_filter() -> Result<()> {
    let fixture = TestFixture::setup().await?;
    // Create table
    let _ = execute_sql(
        fixture.new_query_ctx().await?,
        "CREATE TABLE aa (number int) as select number from numbers(10000000)",
    )
    .await?;

    let _ = execute_sql(
        fixture.new_query_ctx().await?,
        "CREATE TABLE bb (number int) as select number from numbers(10)",
    )
    .await?;

    let plan = physical_plan(
        fixture.new_query_ctx().await?,
        "SELECT * FROM aa JOIN bb ON aa.number = bb.number",
    )
    .await?;
    let join = find_join(&plan)?;
    assert!(join.enable_bloom_runtime_filter);
    let join_build_state = join_build_state(&fixture.new_query_ctx().await?, &join).await?;
    assert!(join_build_state.get_enable_bloom_runtime_filter());
    assert!(join_build_state.get_enable_min_max_runtime_filter());
    Ok(())
}
