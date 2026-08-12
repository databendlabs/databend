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

use databend_common_catalog::BasicColumnStatistics;
use databend_common_exception::Result;
use databend_common_sql::Metadata;
use databend_common_sql::optimizer::CollectStatisticsOptimizer;
use databend_common_sql::optimizer::Optimizer;
use databend_common_sql::optimizer::OptimizerContext;
use databend_common_sql::optimizer::ir::RelExpr;
use databend_common_sql::optimizer::ir::SExpr;
use databend_common_sql::plans::Plan;
use databend_common_sql::plans::RelOperator;

use super::column_stat;
use super::table_statistics;
use crate::framework::LiteTableContext;
use crate::framework::golden::open_golden_file;
use crate::framework::golden::write_case_title;

const QUERY: &str = "SELECT count()
FROM fact_events AS e
JOIN customers AS c ON e.customer_id = c.customer_id
JOIN segments AS s ON c.segment_id = s.segment_id
WHERE e.event_time >= subtract_hours(now(), 24)";

fn stats(entries: &[(&str, &str)]) -> Result<HashMap<String, BasicColumnStatistics>> {
    entries
        .iter()
        .map(|(name, value)| Ok(((*name).to_string(), column_stat(value)?)))
        .collect()
}

fn write_estimated_rows(
    file: &mut impl Write,
    metadata: &Metadata,
    expr: &SExpr,
    indent: usize,
) -> Result<()> {
    let name = match expr.plan() {
        RelOperator::Scan(scan) => format!("Scan({})", metadata.table(scan.table_index).name()),
        RelOperator::Join(_) => "Join".to_string(),
        RelOperator::Filter(_) => "Filter".to_string(),
        RelOperator::Aggregate(_) => "Aggregate".to_string(),
        RelOperator::EvalScalar(_) => "EvalScalar".to_string(),
        _ => return Ok(()),
    };
    let cardinality = RelExpr::with_s_expr(expr).derive_cardinality()?.cardinality;
    writeln!(file, "{}{name}: {cardinality:.2}", "  ".repeat(indent))?;
    for child in expr.children() {
        write_estimated_rows(file, metadata, child, indent + 1)?;
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_current_time_selectivity_guides_join_order() -> Result<()> {
    let ctx = LiteTableContext::create().await?;
    ctx.configure_for_optimizer_case(true)?;
    ctx.set_table_warehouse_distribution(true);
    ctx.set_cluster_node_num(1);

    ctx.register_table_sql_with_stats(
        "CREATE TABLE fact_events(
            event_id UInt64,
            customer_id UInt64,
            event_time Timestamp
        )",
        Some(table_statistics(1_000_000_000)),
        stats(&[
            (
                "customer_id",
                r#"{"min":1,"max":100000000,"ndv":100000000,"null_count":0}"#,
            ),
            (
                "event_time",
                // 2024-01-01 through 2025-01-01, encoded as timestamp microseconds.
                r#"{"min":1704067200000000,"max":1735689600000000,"ndv":31622400,"null_count":0}"#,
            ),
        ])?,
        HashMap::new(),
    )
    .await?;
    ctx.register_table_sql_with_stats(
        "CREATE TABLE customers(customer_id UInt64, segment_id UInt64)",
        Some(table_statistics(100_000_000)),
        stats(&[
            (
                "customer_id",
                r#"{"min":1,"max":100000000,"ndv":100000000,"null_count":0}"#,
            ),
            (
                "segment_id",
                r#"{"min":1,"max":5000000,"ndv":5000000,"null_count":0}"#,
            ),
        ])?,
        HashMap::new(),
    )
    .await?;
    ctx.register_table_sql_with_stats(
        "CREATE TABLE segments(segment_id UInt64)",
        Some(table_statistics(5_000_000)),
        stats(&[(
            "segment_id",
            r#"{"min":1,"max":5000000,"ndv":5000000,"null_count":0}"#,
        )])?,
        HashMap::new(),
    )
    .await?;

    let raw_plan = ctx.bind_sql(QUERY).await?;
    let Plan::Query {
        s_expr: raw_expr,
        metadata,
        ..
    } = &raw_plan
    else {
        unreachable!("SELECT should produce a query plan")
    };
    let mut collector =
        CollectStatisticsOptimizer::new(OptimizerContext::new(ctx.clone(), metadata.clone()));
    let collected = collector.optimize(raw_expr).await?;
    let optimized_plan = ctx.optimize_plan(raw_plan.clone()).await?;

    let mut file = open_golden_file("optimizer", "current_time_selectivity.txt")?;
    write_case_title(
        &mut file,
        "recent_fact_filter_changes_join_order",
        "A recent-time predicate should not estimate all historical fact rows as selected.",
    )?;
    writeln!(file, "sql: {QUERY}")?;
    writeln!(file, "raw_plan:")?;
    writeln!(file, "{}", raw_plan.format_indent(Default::default())?)?;
    writeln!(file, "estimated_rows_before_join_reorder:")?;
    write_estimated_rows(&mut file, &metadata.read(), &collected, 0)?;
    writeln!(file, "optimized_plan:")?;
    writeln!(
        file,
        "{}",
        optimized_plan.format_indent(Default::default())?
    )?;

    Ok(())
}
