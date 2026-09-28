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
use databend_common_exception::Result;
use databend_common_sql::optimizer::Optimizer;
use databend_common_sql::optimizer::OptimizerContext;
use databend_common_sql::optimizer::ir::Distribution;
use databend_common_sql::optimizer::ir::RequiredProperty;
use databend_common_sql::optimizer::ir::StatContext;
use databend_common_sql::optimizer::optimizers::CascadesOptimizer;
use databend_common_sql::plans::Plan;

use crate::framework::LiteTableContext;
use crate::framework::golden::SqlTestCase;
use crate::framework::golden::open_golden_file;
use crate::framework::golden::write_case_header;

async fn write_optimized_case(
    file: &mut impl std::io::Write,
    case: &SqlTestCase,
    grouping_sets_to_union: bool,
) -> Result<()> {
    let ctx = LiteTableContext::create().await?;
    ctx.set_table_warehouse_distribution(true);
    ctx.set_cluster_node_num(3);
    for setup_sql in case.setup_sqls {
        ctx.register_setup_sql(setup_sql).await?;
    }
    if grouping_sets_to_union {
        ctx.get_settings()
            .set_setting("grouping_sets_to_union".to_string(), "1".to_string())?;
    }

    let raw_plan = ctx.bind_sql(case.sql).await?;
    let optimized_plan = ctx.optimize_plan(raw_plan.clone()).await?;

    write_case_header(file, case)?;
    writeln!(file, "raw_plan:")?;
    writeln!(
        file,
        "{}",
        raw_plan.format_indent(Default::default(), &StatContext::default())?
    )?;
    writeln!(file, "optimized_plan:")?;
    writeln!(
        file,
        "{}",
        optimized_plan.format_indent(Default::default(), &StatContext::default())?
    )?;
    writeln!(file)?;

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_materialized_cte_distribution_optimizer_outcomes() -> Result<()> {
    let mut file = open_golden_file("optimizer", "materialized_cte_distribution.txt")?;

    let merge_backed = SqlTestCase {
        name: "merge_backed_scalar_producer_is_redistributed",
        description: "A scalar grouping-set MCTE backed by Merge should be redistributed after the final aggregate.",
        setup_sqls: &[MCTE_INPUT_TABLE],
        sql: "SELECT a, b, sum(v)
FROM mcte_input
GROUP BY ROLLUP(a, b)",
    };
    write_optimized_case(&mut file, &merge_backed, true).await?;

    let dummy_scan = SqlTestCase {
        name: "dummy_scan_serial_producer_is_not_redistributed",
        description: "Seriality from DummyTableScan is not evidence of a Merge-backed MCTE producer.",
        setup_sqls: &[],
        sql: "WITH c AS (SELECT 1 AS x)
SELECT * FROM c
UNION ALL
SELECT * FROM c",
    };
    write_optimized_case(&mut file, &dummy_scan, false).await?;

    Ok(())
}

// Check the memo's root cost as well as the returned plan: optimize_sync falls
// back to the input on a Cascades error, so a successful return alone is not enough.
#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_materialized_cte_cascades_cost() -> Result<()> {
    let ctx = LiteTableContext::create().await?;
    ctx.register_setup_sql(MCTE_INPUT_TABLE).await?;
    let sql = "WITH c AS (SELECT a, v FROM mcte_input),
               d AS (SELECT a FROM c WHERE v > 0)
               SELECT a FROM d UNION ALL SELECT a FROM d";
    let raw_plan = ctx.bind_sql(sql).await?;
    let raw = raw_plan.format_indent(Default::default(), &StatContext::default())?;
    let Plan::Query {
        s_expr, metadata, ..
    } = raw_plan
    else {
        unreachable!("expected a query plan")
    };

    for operator in ["Sequence", "MaterializedCTE", "MaterializedCTERef"] {
        assert!(raw.contains(operator), "raw plan must contain {operator}");
    }

    for distributed in [false, true] {
        let opt_ctx =
            OptimizerContext::new(ctx.clone(), metadata.clone(), ctx.get_function_context()?);
        opt_ctx.set_enable_distributed_optimization(distributed);
        let mut optimizer = CascadesOptimizer::new(opt_ctx)?;
        optimizer.optimize_sync(*s_expr.clone())?;
        let required = if distributed {
            RequiredProperty {
                distribution: Distribution::Serial,
            }
        } else {
            RequiredProperty::default()
        };
        let memo = optimizer.memo().unwrap();
        assert!(
            memo.root().unwrap().best_prop(&required).is_some(),
            "Cascades fell back on materialized CTE (distributed={distributed})"
        );
    }
    Ok(())
}

const MCTE_INPUT_TABLE: &str = "CREATE TABLE mcte_input
(
    a INTEGER,
    b INTEGER,
    v INTEGER
)";
