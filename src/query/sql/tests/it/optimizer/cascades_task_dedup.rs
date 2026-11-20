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

use databend_common_catalog::table_context::TableContextSettings;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_sql::optimizer::OptimizerContext;
use databend_common_sql::optimizer::ir::Distribution;
use databend_common_sql::optimizer::ir::RequiredProperty;
use databend_common_sql::optimizer::optimizers::CascadesOptimizer;
use databend_common_sql::optimizer::optimizers::DEFAULT_TASK_LIMIT;
use databend_common_sql::optimizer::optimizers::SchedulerStat;
use databend_common_sql::plans::Plan;

use crate::framework::LiteTableContext;

/// `SELECT 'k0', 'v0' UNION ALL SELECT 'k1', 'v1' UNION ALL ...`, which binds to a
/// left-deep chain of `branches - 1` `UnionAll` operators.
fn union_all_chain(branches: usize) -> String {
    (0..branches)
        .map(|i| format!("SELECT 'k{i}' AS k, 'v{i}' AS v"))
        .collect::<Vec<_>>()
        .join(" UNION ALL ")
}

/// Run the Cascades search alone on the bound plan in distributed mode and return
/// its scheduler statistics.
async fn cascades_stat(sql: &str) -> Result<SchedulerStat> {
    let ctx = LiteTableContext::create().await?;
    ctx.configure_for_optimizer_case(false)?;
    ctx.set_cluster_node_num(3);

    let Plan::Query {
        s_expr, metadata, ..
    } = ctx.bind_sql(sql).await?
    else {
        return Err(ErrorCode::Internal("expected a query plan"));
    };

    let opt_ctx = OptimizerContext::new(ctx.clone(), metadata, ctx.get_function_context()?)
        .with_settings(&ctx.get_settings())?;
    opt_ctx.set_enable_distributed_optimization(true);

    let mut cascades = CascadesOptimizer::new(opt_ctx)?;
    cascades.optimize_sync((*s_expr).into_logical()?)?;

    // `optimize_sync` silently falls back to the heuristic plan when the search
    // fails or runs out of budget, so check that the search itself found a plan.
    let root = cascades
        .memo()
        .root()
        .ok_or_else(|| ErrorCode::Internal("memo has no root group"))?;
    assert!(
        root.best_prop(&RequiredProperty {
            distribution: Distribution::Serial,
        })
        .is_some(),
        "Cascades fell back to the heuristic plan for: {sql}"
    );

    cascades
        .scheduler_stat()
        .cloned()
        .ok_or_else(|| ErrorCode::Internal("Cascades did not run the task scheduler"))
}

/// Every `UnionAll` enumerates two required distributions for its children, so each
/// level of the chain requests the optimization of the same child group twice. Before
/// requests to an in-flight group task were deduplicated, that doubled the task count at
/// every level and a chain of a few dozen branches exhausted `DEFAULT_TASK_LIMIT`,
/// spending seconds in the optimizer before falling back to the heuristic plan.
#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_deep_union_all_cascades_tasks_grow_linearly() -> Result<()> {
    let small = cascades_stat(&union_all_chain(10)).await?;
    let large = cascades_stat(&union_all_chain(40)).await?;

    assert!(
        large.scheduled_task_count <= DEFAULT_TASK_LIMIT,
        "Cascades exhausted its task budget: {large:?}"
    );
    assert!(
        large.optimize_group_wait_count > 0,
        "duplicate group requests should attach to the in-flight task: {large:?}"
    );
    // 4x the branches must not cost more than ~4x the tasks (with slack for the
    // constant part of the plan); an exponential blow-up fails this by orders of
    // magnitude.
    assert!(
        large.scheduled_task_count <= small.scheduled_task_count * 6,
        "Cascades task count is not linear in the union depth: small={small:?}, large={large:?}"
    );

    Ok(())
}
