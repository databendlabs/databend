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

use std::collections::HashMap;
use std::io::Write;
use std::sync::Arc;

use databend_common_ast::ast::ExplainKind;
use databend_common_catalog::table_context::TableContextSettings;
use databend_common_exception::Result;
use databend_common_sql::optimizer::OptimizerContext;
use databend_common_sql::optimizer::optimize;
use databend_common_sql::plans::Plan;

use super::table_statistics;
use crate::framework::LiteTableContext;
use crate::framework::golden::SqlTestCase;
use crate::framework::golden::open_golden_file;
use crate::framework::golden::write_case_header;

const TABLE_SQL: &str = "CREATE TABLE t(k Int64 NOT NULL, v Int64 NOT NULL)";
const SETUP_SQLS: &[&str] = &[TABLE_SQL];

async fn distributed_context() -> Result<Arc<LiteTableContext>> {
    let ctx = LiteTableContext::create().await?;
    ctx.set_cluster_node_num(2);
    ctx.set_table_warehouse_distribution(true);
    // Without row counts every distribution costs zero and ties are broken by
    // search order, so give the table a realistic size.
    ctx.register_table_sql_with_stats(
        TABLE_SQL,
        Some(table_statistics(100_000_000)),
        HashMap::new(),
        HashMap::new(),
    )
    .await?;
    Ok(ctx)
}

/// Whether Cascades found a plan for the root group instead of falling back
/// to the heuristic distributed optimizer, which it does silently on error.
async fn cascades_found_plan(ctx: &Arc<LiteTableContext>, sql: &str) -> Result<bool> {
    let plan = ctx.bind_sql(&format!("EXPLAIN MEMO {sql}")).await?;
    let Plan::Explain { plan: query, .. } = &plan else {
        unreachable!("EXPLAIN MEMO binds to an explain plan")
    };
    let Plan::Query { metadata, .. } = query.as_ref() else {
        unreachable!("EXPLAIN MEMO explains a query")
    };
    let opt_ctx = OptimizerContext::new(ctx.clone(), metadata.clone(), ctx.get_function_context()?)
        .with_settings(&ctx.get_settings())?;
    opt_ctx.set_enable_distributed_optimization(true);
    let Plan::Explain {
        kind: ExplainKind::Memo(memo),
        ..
    } = optimize(opt_ctx, plan).await?
    else {
        unreachable!("EXPLAIN MEMO keeps its kind")
    };

    let root = memo
        .lines()
        .find_map(|line| {
            line.trim_start_matches(['├', '└', '─', ' '])
                .strip_prefix("root group: ")
        })
        .expect("memo display names the root group")
        .to_string();
    // The best properties of the root group, which the fallback leaves empty.
    let header = format!("Group {root}");
    Ok(memo
        .lines()
        .skip_while(|line| !line.ends_with(&header))
        .skip(1)
        .take_while(|line| !line.contains("Group #"))
        .any(|line| line.contains("{ dist: Serial }:")))
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_distributed_window_cascades() -> Result<()> {
    let mut file = open_golden_file("optimizer", "distributed_window.txt")?;
    let cases = [
        SqlTestCase {
            name: "partition_without_order",
            description: "An aggregate window partitioned by a column shuffles its input by the partition key.",
            setup_sqls: SETUP_SQLS,
            sql: "SELECT k, sum(v) OVER (PARTITION BY k) FROM t",
        },
        SqlTestCase {
            name: "partition_with_order",
            description: "A ranking window with ORDER BY shuffles by the partition key and sorts within partitions.",
            setup_sqls: SETUP_SQLS,
            sql: "SELECT k, row_number() OVER (PARTITION BY k ORDER BY v) FROM t",
        },
        SqlTestCase {
            name: "partition_by_expression",
            description: "An expression partition key is evaluated before the shuffle.",
            setup_sqls: SETUP_SQLS,
            sql: "SELECT k, sum(v) OVER (PARTITION BY k + 1) FROM t",
        },
        SqlTestCase {
            name: "window_group_same_partition",
            description: "Several windows sharing the partition key are grouped and shuffle once.",
            setup_sqls: SETUP_SQLS,
            sql: "SELECT k, sum(v) OVER (PARTITION BY k), max(v) OVER (PARTITION BY k) FROM t",
        },
        SqlTestCase {
            name: "window_group_different_partitions",
            description: "Windows with different partition keys shuffle once per partition key.",
            setup_sqls: SETUP_SQLS,
            sql: "SELECT k, sum(v) OVER (PARTITION BY k), sum(v) OVER (PARTITION BY v) FROM t",
        },
        SqlTestCase {
            name: "serial_input",
            description: "A Serial input is not shuffled again for a partitioned window.",
            setup_sqls: SETUP_SQLS,
            sql: "SELECT k, sum(v) OVER (PARTITION BY k) FROM (SELECT * FROM t LIMIT 10) AS s",
        },
        SqlTestCase {
            name: "partition_over_aggregate",
            description: "A partitioned window over a distributed aggregate keeps the plan distributed.",
            setup_sqls: SETUP_SQLS,
            sql: "SELECT k, row_number() OVER (PARTITION BY k ORDER BY v) FROM (SELECT k, max(v) AS v FROM t GROUP BY k) AS s",
        },
        SqlTestCase {
            name: "partition_then_global_window",
            description: "A partitioned window runs distributed before a window without PARTITION BY merges its output.",
            setup_sqls: SETUP_SQLS,
            sql: "SELECT k, avg(v) OVER (PARTITION BY k), avg(v) OVER () FROM t",
        },
        SqlTestCase {
            name: "partition_over_serial_window",
            description: "A partitioned window over a window without PARTITION BY stays on the merged input.",
            setup_sqls: SETUP_SQLS,
            sql: "SELECT k, sum(v) OVER (PARTITION BY k) FROM (SELECT k, v, row_number() OVER (ORDER BY v) AS rn FROM t) AS s",
        },
        SqlTestCase {
            name: "order_without_partition",
            description: "A window without PARTITION BY needs a Serial input.",
            setup_sqls: SETUP_SQLS,
            sql: "SELECT k, row_number() OVER (ORDER BY v) FROM t",
        },
    ];

    for case in cases {
        let ctx = distributed_context().await?;
        assert!(
            cascades_found_plan(&ctx, case.sql).await?,
            "Cascades fell back for {}",
            case.name
        );
        let plan = ctx.optimize_plan(ctx.bind_sql(case.sql).await?).await?;
        write_case_header(&mut file, &case)?;
        writeln!(
            file,
            "{}",
            plan.format_indent(Default::default(), &ctx.stat_context()?)?
        )?;
        writeln!(file)?;
    }
    Ok(())
}
