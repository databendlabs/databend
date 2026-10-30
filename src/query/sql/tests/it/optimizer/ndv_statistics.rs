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
use databend_common_catalog::table_context::TableContextSettings;
use databend_common_exception::Result;
use databend_common_sql::optimizer::CollectStatisticsOptimizer;
use databend_common_sql::optimizer::ColumnStatisticsTrace;
use databend_common_sql::optimizer::Optimizer;
use databend_common_sql::optimizer::OptimizerContext;
use databend_common_sql::optimizer::StatisticsTraceCollector;
use databend_common_sql::optimizer::ir::ColumnStat;
use databend_common_sql::optimizer::ir::PExpr;
use databend_common_sql::optimizer::ir::RelExpr;
use databend_common_sql::plans::PhysicalRelOperator;
use databend_common_sql::plans::Plan;
use databend_common_sql_test_support::optimizer::ReplayInput;
use databend_common_statistics::Datum;
use databend_common_statistics::HistogramBuilder;
use databend_common_statistics::NdvEstimate;

use super::table_statistics;
use crate::framework::LiteTableContext;
use crate::framework::golden::open_golden_file;
use crate::framework::golden::write_case_title;

fn column(ndv: Option<NdvEstimate>) -> BasicColumnStatistics {
    BasicColumnStatistics {
        min: Some(Datum::Int(0)),
        max: Some(Datum::Int(10_000)),
        ndv,
        null_count: 20,
        in_memory_size: 24_000,
    }
}

#[test]
fn test_ndv_trace_round_trip() -> Result<()> {
    for ndv in [
        None,
        Some(NdvEstimate::exact(12.0)),
        Some(NdvEstimate::new(17.0, 2980.0)),
        Some(NdvEstimate::upper_bound(2980.0)),
    ] {
        let stats = column(ndv);
        let trace = serde_json::to_value(ColumnStatisticsTrace::from(&stats))?;
        if let Some(ndv) = ndv {
            assert_eq!(
                trace["ndv"]["expected"],
                serde_json::to_value(ndv.expected)?
            );
            assert_eq!(trace["ndv"]["upper"], serde_json::to_value(ndv.upper)?);
        } else {
            assert!(trace["ndv"].is_null());
        }
        let trace: ColumnStatisticsTrace = serde_json::from_value(trace)?;
        let replay = BasicColumnStatistics::try_from(&trace)?;
        assert_eq!(replay.ndv, ndv);
        assert_eq!(replay.min, stats.min);
        assert_eq!(replay.max, stats.max);
        assert_eq!(replay.null_count, stats.null_count);
        assert_eq!(replay.in_memory_size, stats.in_memory_size);
    }
    Ok(())
}

#[test]
fn test_ndv_trace_rejects_invalid_bounds() -> Result<()> {
    for (expected, upper) in [(Some(20.0), 10.0), (Some(-1.0), 10.0), (None, -1.0)] {
        let mut trace = serde_json::to_value(ColumnStatisticsTrace::from(&column(None)))?;
        trace["ndv"] = serde_json::json!({"expected": expected, "upper": upper});
        let trace: ColumnStatisticsTrace = serde_json::from_value(trace)?;
        assert!(BasicColumnStatistics::try_from(&trace).is_err());
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_collected_trace_preserves_ndv_estimate() -> Result<()> {
    let ctx = LiteTableContext::create().await?;
    let ndv = NdvEstimate::new(17.0, 2980.0);
    ctx.register_table_sql_with_stats(
        "CREATE TABLE t(k BIGINT NULL)",
        Some(table_statistics(3000)),
        HashMap::from([("k".to_string(), column(Some(ndv)))]),
        HashMap::new(),
    )
    .await?;
    let Plan::Query {
        s_expr, metadata, ..
    } = ctx.bind_sql("SELECT k FROM t").await?
    else {
        unreachable!()
    };
    let trace = StatisticsTraceCollector::default();
    let opt_ctx = OptimizerContext::new(ctx.clone(), metadata, ctx.get_function_context()?);
    let mut collector =
        CollectStatisticsOptimizer::new(opt_ctx).with_trace_collector(trace.clone());
    collector.optimize((*s_expr).into_logical()?).await?;
    let replay: ReplayInput = serde_json::from_value(trace.take().unwrap())?;
    let parts = replay.into_parts()?;
    assert_eq!(parts.tables[0].column_stats["k"].ndv, Some(ndv));
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_scan_ndv_statistics_golden() -> Result<()> {
    let mut file = open_golden_file("optimizer", "ndv_statistics.txt")?;
    let cases = [
        (
            "exact_ndv",
            "Exact fixture NDV may synthesize a histogram.",
            Some(NdvEstimate::exact(12.0)),
            3000,
        ),
        (
            "extrapolated_ndv",
            "Sample extrapolation preserves an upper bound distinct from its point estimate.",
            Some(NdvEstimate::exact(12.0)),
            100,
        ),
        (
            "approximate_full_coverage",
            "Full statistics coverage does not make an approximate NDV exact.",
            Some(NdvEstimate::new(12.0, 15.0)),
            3000,
        ),
        (
            "supplied_histogram",
            "A separately supplied histogram is retained without replacing the column NDV upper bound.",
            Some(NdvEstimate::new(12.0, 3000.0)),
            3000,
        ),
        (
            "missing_ndv",
            "Missing NDV stays upper-only and must not synthesize a histogram.",
            None,
            3000,
        ),
        (
            "missing_sample",
            "An NDV without sample coverage must not invent an expected value.",
            Some(NdvEstimate::exact(12.0)),
            0,
        ),
    ];
    for (name, description, input_ndv, stats_rows) in cases {
        let ctx = LiteTableContext::create().await?;
        let basic = column(input_ndv).get_useful_stat(3000, stats_rows).unwrap();
        let expected_ndv = basic.ndv.unwrap();
        let histograms = if name == "supplied_histogram" {
            HashMap::from([(
                "k".to_string(),
                HistogramBuilder::from_ndv(
                    12,
                    2980,
                    Some((Datum::Int(0), Datum::Int(10_000))),
                    32,
                    1.0,
                )
                .unwrap(),
            )])
        } else {
            HashMap::new()
        };
        ctx.register_table_sql_with_stats(
            "CREATE TABLE t(k BIGINT NULL)",
            Some(table_statistics(3000)),
            HashMap::from([("k".to_string(), basic)]),
            histograms,
        )
        .await?;
        let raw = ctx.bind_sql("SELECT k FROM t").await?;
        write_case_title(&mut file, name, description)?;
        writeln!(
            file,
            "raw_plan:\n{}",
            raw.format_indent(Default::default(), &ctx.stat_context()?)?
        )?;
        let optimized = ctx.optimize_plan(raw).await?;
        writeln!(
            file,
            "optimized_plan:\n{}",
            optimized.format_indent(Default::default(), &ctx.stat_context()?)?
        )?;
        let Plan::Query { s_expr, .. } = optimized else {
            unreachable!()
        };
        let scan = s_expr.planned()?.expr();
        // This query needs no projection operator: the optimized root is Scan.
        assert!(matches!(scan.plan(), PhysicalRelOperator::Scan(_)));
        let stats = RelExpr::with_p_expr(scan).derive_cardinality(&ctx.stat_context()?)?;
        let stat = stats.statistics.column_stats.values().next().unwrap();
        assert_eq!(stat.ndv(), expected_ndv);
        let has_histogram = matches!(
            stat,
            ColumnStat::Int {
                histogram: Some(_),
                ..
            } | ColumnStat::UInt {
                histogram: Some(_),
                ..
            } | ColumnStat::Float {
                histogram: Some(_),
                ..
            } | ColumnStat::Bytes {
                histogram: Some(_),
                ..
            }
        );
        // Any point NDV, exact or not, may synthesize; upper-only NDV may not.
        assert_eq!(
            has_histogram,
            name == "supplied_histogram" || expected_ndv.expected.is_some()
        );
        writeln!(file, "scan_ndv: {:?}", stat.ndv())?;
        writeln!(file, "histogram: {has_histogram}\n")?;
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_constant_inputs_use_shared_statistics() -> Result<()> {
    fn find_constant(expr: &PExpr) -> Option<&PExpr> {
        if matches!(expr.plan(), PhysicalRelOperator::ConstantTableScan(_)) {
            return Some(expr);
        }
        expr.children().find_map(find_constant)
    }

    let cases = [
        ("nullable_duplicates", "(1), (1), (2), (NULL::BIGINT)".to_string(), 4, 2, 1),
        ("all_null", "(NULL::BIGINT), (NULL::BIGINT)".to_string(), 2, 0, 2),
        ("fractional", "(1.00::DECIMAL(10, 2)), (1.01::DECIMAL(10, 2)), (1.01::DECIMAL(10, 2)), (NULL::DECIMAL(10, 2))".to_string(), 4, 2, 1),
        // The large sparse domain prevents a domain cap from hiding an
        // approximate aggregate incorrectly presented as an exact measurement.
        ("many_distinct", (0..1000).map(|value| format!("({})", value * 100)).collect::<Vec<_>>().join(", "), 1000, 1000, 0),
    ];
    let mut file = open_golden_file("optimizer", "constant_ndv_statistics.txt")?;
    for (name, values, rows, ndv, nulls) in cases {
        let ctx = LiteTableContext::create().await?;
        let raw = ctx
            .bind_sql(&format!("SELECT k FROM (VALUES {values}) AS t(k)"))
            .await?;
        write_case_title(
            &mut file,
            name,
            "Constant inputs use the same base-statistics rules as Scan.",
        )?;
        writeln!(
            file,
            "raw_plan:\n{}",
            raw.format_indent(Default::default(), &ctx.stat_context()?)?
        )?;
        let optimized = ctx.optimize_plan(raw).await?;
        writeln!(
            file,
            "optimized_plan:\n{}",
            optimized.format_indent(Default::default(), &ctx.stat_context()?)?
        )?;
        let Plan::Query { s_expr, .. } = optimized else {
            unreachable!()
        };
        let scan = find_constant(s_expr.planned()?.expr())
            .expect("VALUES must exercise ConstantTableScan");
        let stats = RelExpr::with_p_expr(scan).derive_cardinality(&ctx.stat_context()?)?;
        assert_eq!(stats.cardinality, rows as f64);
        let stat = stats.statistics.column_stats.values().next().unwrap();
        assert_eq!(stat.ndv(), NdvEstimate::exact(ndv as f64));
        assert_eq!(
            stat.null_count(),
            databend_common_statistics::StatCount::exact(nulls)
        );
        let histogram_rows = stat.histogram().map(|histogram| histogram.num_values());
        if ndv > 0 {
            assert!((histogram_rows.unwrap() - (rows - nulls) as f64).abs() < 1e-9);
        } else {
            assert!(matches!(stat, ColumnStat::AllNull { .. }));
            assert_eq!(histogram_rows, None);
        }
        let histogram_rows = histogram_rows.map(f64::round);
        writeln!(
            file,
            "row_count: {rows}\nndv: {:?}\nnull_count: {:?}\nhistogram_rows: {histogram_rows:?}\n",
            stat.ndv(),
            stat.null_count()
        )?;
    }
    Ok(())
}
