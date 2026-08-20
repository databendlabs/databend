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
use databend_common_catalog::TableStatistics;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_sql::ColumnEntry;
use databend_common_sql::Metadata;
use databend_common_sql::Symbol;
use databend_common_sql::optimizer::CollectStatisticsOptimizer;
use databend_common_sql::optimizer::Optimizer;
use databend_common_sql::optimizer::OptimizerContext;
use databend_common_sql::optimizer::ir::ColumnStat;
use databend_common_sql::optimizer::ir::RelExpr;
use databend_common_sql::optimizer::ir::SExpr;
use databend_common_sql::optimizer::ir::StatInfo;
use databend_common_sql::optimizer::optimizers::recursive::RecursiveRuleOptimizer;
use databend_common_sql::optimizer::optimizers::rule::RuleID;
use databend_common_sql::plans::Operator;
use databend_common_sql::plans::Plan;
use databend_common_sql::plans::RelOp;
use databend_common_statistics::Datum;

use crate::framework::LiteTableContext;
use crate::framework::golden::open_golden_file;
use crate::framework::golden::write_case_title;

struct StatsCase {
    name: &'static str,
    description: &'static str,
    sql: &'static str,
    operator: RelOp,
}

fn table_statistics(rows: u64) -> TableStatistics {
    TableStatistics {
        num_rows: Some(rows),
        data_size: Some(rows.saturating_mul(16)),
        data_size_compressed: None,
        index_size: None,
        bloom_index_size: None,
        ngram_index_size: None,
        inverted_index_size: None,
        vector_index_size: None,
        virtual_column_size: None,
        number_of_blocks: Some(1),
        number_of_segments: Some(1),
    }
}

fn column_statistics() -> HashMap<String, BasicColumnStatistics> {
    HashMap::from([
        ("k".to_string(), BasicColumnStatistics {
            min: Some(Datum::Int(1)),
            max: Some(Datum::Int(100)),
            ndv: Some(100),
            null_count: 10,
            in_memory_size: 800,
        }),
        ("p".to_string(), BasicColumnStatistics {
            min: Some(Datum::Int(1)),
            max: Some(Datum::Int(5)),
            ndv: Some(5),
            null_count: 0,
            in_memory_size: 800,
        }),
    ])
}

fn find_operator(expr: &SExpr, operator: RelOp) -> Option<&SExpr> {
    if expr.plan().rel_op() == operator {
        return Some(expr);
    }
    expr.children()
        .find_map(|child| find_operator(child, operator.clone()))
}

fn column_label(metadata: &Metadata, column: Symbol) -> String {
    let id = column.as_usize();
    match metadata.column(column) {
        ColumnEntry::BaseTableColumn(column) => {
            let table = metadata.table(column.table_index);
            format!("{}.{} (#{id})", table.name(), column.column_name)
        }
        entry => format!("{} (#{id})", entry.name()),
    }
}

fn write_stats(file: &mut impl Write, metadata: &Metadata, stats: &StatInfo) -> Result<()> {
    writeln!(file, "cardinality: {:.3}", stats.cardinality)?;
    writeln!(
        file,
        "precise_cardinality: {:?}",
        stats.statistics.precise_cardinality
    )?;

    let mut column_stats = stats.statistics.column_stats.iter().collect::<Vec<_>>();
    column_stats.sort_by_key(|(column, _)| **column);
    for (column, stat) in column_stats {
        let Some(bounds) = stat.bounds() else {
            writeln!(
                file,
                "column_stat: {} all-null, null_count={:?}",
                column_label(metadata, *column),
                stat.null_count(),
            )?;
            continue;
        };
        let (min, max) = bounds.display_parts();
        let has_histogram = match stat {
            ColumnStat::Int { histogram, .. } => histogram.is_some(),
            ColumnStat::UInt { histogram, .. } => histogram.is_some(),
            ColumnStat::Float { histogram, .. } => histogram.is_some(),
            ColumnStat::Bytes { histogram, .. } => histogram.is_some(),
            ColumnStat::Boolean { .. } | ColumnStat::AllNull { .. } => false,
        };
        writeln!(
            file,
            "column_stat: {} min={}, max={}, ndv={:?}, null_count={:?}, histogram={}",
            column_label(metadata, *column),
            min,
            max,
            stat.ndv(),
            stat.null_count(),
            if has_histogram { "some" } else { "none" }
        )?;
    }
    Ok(())
}

async fn write_case(file: &mut impl Write, case: &StatsCase) -> Result<()> {
    let ctx = LiteTableContext::create().await?;
    ctx.register_table_sql_with_stats(
        "CREATE TABLE t(k BIGINT NULL, p BIGINT NOT NULL)",
        Some(table_statistics(100)),
        column_statistics(),
        HashMap::new(),
    )
    .await?;

    let raw_plan = ctx.bind_sql(case.sql).await?;
    let (s_expr, metadata) = if case.operator == RelOp::Sort {
        let Plan::Query {
            s_expr, metadata, ..
        } = raw_plan
        else {
            return Err(ErrorCode::Internal("expected query plan"));
        };
        let opt_ctx = OptimizerContext::new(ctx, metadata.clone());
        let mut collector = CollectStatisticsOptimizer::new(opt_ctx.clone());
        let s_expr = collector.optimize(&s_expr).await?;
        let s_expr = RecursiveRuleOptimizer::new(opt_ctx, &[RuleID::PushDownLimitSort])
            .optimize_sync(&s_expr)?;
        (s_expr, metadata)
    } else {
        let Plan::Query {
            s_expr, metadata, ..
        } = ctx.optimize_plan(raw_plan).await?
        else {
            return Err(ErrorCode::Internal("expected optimized query plan"));
        };
        (*s_expr, metadata)
    };
    let target = find_operator(&s_expr, case.operator.clone()).ok_or_else(|| {
        ErrorCode::Internal(format!("cannot find {:?} in optimized plan", case.operator))
    })?;
    let stats = RelExpr::with_s_expr(target).derive_cardinality()?;

    write_case_title(file, case.name, case.description)?;
    writeln!(file, "sql: {}", case.sql)?;
    writeln!(file, "operator: {:?}", case.operator)?;
    write_stats(file, &metadata.read(), &stats)?;
    writeln!(file)?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_sort_and_window_statistics_golden() -> Result<()> {
    let mut file = open_golden_file("optimizer", "stat_derivation.txt")?;
    let cases = [
        StatsCase {
            name: "sort_top_n_caps_statistics",
            description: "Top-N sort caps row-count statistics without discarding value bounds.",
            sql: "SELECT k FROM t ORDER BY k LIMIT 10",
            operator: RelOp::Sort,
        },
        StatsCase {
            name: "row_number_statistics",
            description: "ROW_NUMBER derives a non-null positive integer range.",
            sql: "SELECT ROW_NUMBER() OVER (PARTITION BY p ORDER BY k) AS w FROM t",
            operator: RelOp::Window,
        },
        StatsCase {
            name: "rank_statistics",
            description: "RANK derives a non-null range bounded by the input cardinality.",
            sql: "SELECT RANK() OVER (PARTITION BY p ORDER BY k) AS w FROM t",
            operator: RelOp::Window,
        },
        StatsCase {
            name: "dense_rank_statistics",
            description: "DENSE_RANK derives a non-null range bounded by the input cardinality.",
            sql: "SELECT DENSE_RANK() OVER (PARTITION BY p ORDER BY k) AS w FROM t",
            operator: RelOp::Window,
        },
        StatsCase {
            name: "ntile_statistics",
            description: "NTILE derives a non-null range bounded by its bucket count.",
            sql: "SELECT NTILE(4) OVER (PARTITION BY p ORDER BY k) AS w FROM t",
            operator: RelOp::Window,
        },
        StatsCase {
            name: "lag_statistics",
            description: "LAG inherits its argument bounds and accounts for boundary NULLs.",
            sql: "SELECT LAG(k) OVER (PARTITION BY p ORDER BY k) AS w FROM t",
            operator: RelOp::Window,
        },
        StatsCase {
            name: "lead_default_statistics",
            description: "LEAD merges a derived default value into its argument bounds.",
            sql: "SELECT LEAD(k, 1, 200) OVER (PARTITION BY p ORDER BY k) AS w FROM t",
            operator: RelOp::Window,
        },
        StatsCase {
            name: "window_group_statistics",
            description: "WindowGroup derives statistics for every supported output column.",
            sql: "SELECT ROW_NUMBER() OVER (PARTITION BY p ORDER BY k), NTILE(4) OVER (PARTITION BY p ORDER BY k), LAG(k) OVER (PARTITION BY p ORDER BY k) FROM t",
            operator: RelOp::WindowGroup,
        },
    ];

    for case in &cases {
        write_case(&mut file, case).await?;
    }
    Ok(())
}
