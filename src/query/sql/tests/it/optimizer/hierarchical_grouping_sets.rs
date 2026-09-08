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
use databend_common_sql::optimizer::ir::Distribution;
use databend_common_sql::optimizer::ir::RelExpr;
use databend_common_sql::optimizer::ir::SExpr;
use databend_common_sql::optimizer::ir::SExprVisitor;
use databend_common_sql::optimizer::ir::VisitAction;
use databend_common_sql::plans::Operator;
use databend_common_sql::plans::Plan;
use databend_common_sql::plans::RelOp;

use crate::framework::LiteTableContext;
use crate::framework::golden::SqlTestCase;
use crate::framework::golden::open_golden_file;
use crate::framework::golden::write_case_header;

async fn write_optimized_case(
    file: &mut impl std::io::Write,
    case: &SqlTestCase,
    enable_cascading: bool,
) -> Result<()> {
    let ctx = LiteTableContext::create().await?;
    ctx.set_cluster_node_num(2);
    ctx.set_table_warehouse_distribution(true);
    for setup_sql in case.setup_sqls {
        ctx.register_setup_sql(setup_sql).await?;
    }
    ctx.get_settings()
        .set_setting("grouping_sets_to_union".to_string(), "1".to_string())?;
    ctx.get_settings().set_setting(
        "enable_cascading_grouping_sets".to_string(),
        u8::from(enable_cascading).to_string(),
    )?;

    let raw_plan = ctx.bind_sql(case.sql).await?;
    let optimized_plan = ctx.optimize_plan(raw_plan.clone()).await?;
    assert_no_serial_sequence_producer(&optimized_plan)?;

    write_case_header(file, case)?;
    writeln!(file, "raw_plan:")?;
    writeln!(file, "{}", raw_plan.format_indent(Default::default())?)?;
    writeln!(file, "optimized_plan:")?;
    writeln!(
        file,
        "{}",
        optimized_plan.format_indent(Default::default())?
    )?;
    writeln!(file)?;

    Ok(())
}

fn assert_no_serial_sequence_producer(plan: &Plan) -> Result<()> {
    let Plan::Query { s_expr, .. } = plan else {
        unreachable!("the ROLLUP test query should bind to Plan::Query")
    };

    struct SequenceDistributionChecker;

    impl SExprVisitor for SequenceDistributionChecker {
        fn visit(&mut self, expr: &SExpr) -> Result<VisitAction> {
            if expr.plan().rel_op() == RelOp::Sequence {
                let left_prop = RelExpr::with_s_expr(expr.left_child()).derive_physical_prop()?;
                assert_ne!(
                    left_prop.distribution,
                    Distribution::Serial,
                    "a Serial Sequence producer removes every Exchange from the query"
                );
            }
            Ok(VisitAction::Continue)
        }
    }

    s_expr.accept(&mut SequenceDistributionChecker)?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_hierarchical_grouping_sets_optimizer_outcomes() -> Result<()> {
    let mut file = open_golden_file("optimizer", "hierarchical_grouping_sets.txt")?;

    let shared_base = SqlTestCase {
        name: "rollup_shared_base",
        description: "The default hierarchy derives every lower ROLLUP level from the most detailed grouping.",
        setup_sqls: &[ROLLUP_TABLE],
        sql: ROLLUP_QUERY,
    };
    write_optimized_case(&mut file, &shared_base, false).await?;

    let cascading = SqlTestCase {
        name: "rollup_nearest_parent_cascade",
        description: "The optional cascade derives every lower ROLLUP level from its closest strict superset.",
        setup_sqls: &[ROLLUP_TABLE],
        sql: ROLLUP_QUERY,
    };
    write_optimized_case(&mut file, &cascading, true).await?;

    Ok(())
}

const ROLLUP_TABLE: &str = "CREATE TABLE t
(
    a UInt64,
    b UInt64,
    c UInt64,
    v UInt64
)";

const ROLLUP_QUERY: &str = "SELECT a, b, c, sum(v), count(v), min(v), max(v)
FROM t
GROUP BY ROLLUP(a, b, c)";
