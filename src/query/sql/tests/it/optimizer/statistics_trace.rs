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

//! The statistics trace wire format: collection, replay, and older formats.

use std::collections::HashMap;

use databend_common_catalog::BasicColumnStatistics;
use databend_common_catalog::table_context::TableContextSettings;
use databend_common_exception::Result;
use databend_common_sql::optimizer::CollectStatisticsOptimizer;
use databend_common_sql::optimizer::ColumnStatisticsTrace;
use databend_common_sql::optimizer::Optimizer;
use databend_common_sql::optimizer::OptimizerContext;
use databend_common_sql::optimizer::StatisticsTraceCollector;
use databend_common_sql::plans::Plan;
use databend_common_sql_test_support::optimizer::ReplayInput;
use databend_common_statistics::Datum;
use databend_common_statistics::NdvEstimate;

use super::table_statistics;
use crate::framework::LiteTableContext;

const ROWS: u64 = 3000;

/// 3000 rows, 20 NULL: the NDV upper may not exceed 2980.
fn column(ndv: NdvEstimate) -> BasicColumnStatistics {
    BasicColumnStatistics::new(
        Some(Datum::Int(0)),
        Some(Datum::Int(10_000)),
        20,
        24_000,
        ndv,
        Some(ROWS),
    )
    .unwrap()
}

fn trace_json(ndv: serde_json::Value) -> serde_json::Value {
    serde_json::json!({
        "min": {"Int": 0},
        "max": {"Int": 10000},
        "ndv": ndv,
        "null_count": 20,
        "in_memory_size": 24000,
    })
}

#[test]
fn test_ndv_trace_round_trip() -> Result<()> {
    for ndv in [
        NdvEstimate::exact(12.0),
        NdvEstimate::new(17.0, 2980.0),
        NdvEstimate::upper_bound(2980.0),
    ] {
        let stats = column(ndv);
        let trace = serde_json::to_value(ColumnStatisticsTrace::from(&stats))?;
        assert_eq!(
            trace["ndv"],
            serde_json::json!({"expected": ndv.expected, "upper": ndv.upper})
        );
        let trace: ColumnStatisticsTrace = serde_json::from_value(trace)?;
        assert_eq!(trace.to_basic_column_statistics(Some(ROWS))?, stats);
    }
    Ok(())
}

#[test]
fn test_ndv_trace_accepts_legacy_scalar() -> Result<()> {
    // Older traces stored a scalar the planner used as exact, with zero
    // meaning unknown, or no NDV at all. Replay derives them as providers of
    // that time did and rewrites them in the current form.
    for (legacy, expected) in [
        (serde_json::json!(25), NdvEstimate::exact(25.0)),
        (serde_json::json!(5000), NdvEstimate::exact(2980.0)),
        (serde_json::json!(0), NdvEstimate::upper_bound(2980.0)),
        (serde_json::Value::Null, NdvEstimate::upper_bound(2980.0)),
    ] {
        let trace: ColumnStatisticsTrace = serde_json::from_value(trace_json(legacy))?;
        let replay = trace.to_basic_column_statistics(Some(ROWS))?;
        assert_eq!(replay, column(expected));
        assert_eq!(
            serde_json::to_value(ColumnStatisticsTrace::from(&replay))?["ndv"],
            serde_json::json!({"expected": expected.expected, "upper": expected.upper})
        );
    }
    for invalid in [serde_json::json!(-1), serde_json::json!(1.5)] {
        assert!(serde_json::from_value::<ColumnStatisticsTrace>(trace_json(invalid)).is_err());
    }
    Ok(())
}

#[test]
fn test_ndv_trace_rejects_invalid_bounds() -> Result<()> {
    // Current traces are replayed unchanged: an upper above the 2980 non-NULL
    // rows is rejected, not capped.
    for (expected, upper) in [
        (Some(20.0), 10.0),
        (Some(-1.0), 10.0),
        (None, -1.0),
        (Some(12.0), 5000.0),
    ] {
        let trace: ColumnStatisticsTrace = serde_json::from_value(trace_json(
            serde_json::json!({"expected": expected, "upper": upper}),
        ))?;
        assert!(trace.to_basic_column_statistics(Some(ROWS)).is_err());
    }
    Ok(())
}

#[test]
fn test_ndv_trace_without_row_count() -> Result<()> {
    // An unknown row count imposes no row bound: a current trace keeps an
    // upper above the rows, and a legacy unknown NDV keeps the domain bound.
    let trace: ColumnStatisticsTrace = serde_json::from_value(trace_json(
        serde_json::json!({"expected": 12.0, "upper": 5000.0}),
    ))?;
    assert_eq!(
        trace.to_basic_column_statistics(None)?.ndv(),
        NdvEstimate::new(12.0, 5000.0)
    );
    let trace: ColumnStatisticsTrace = serde_json::from_value(trace_json(serde_json::Value::Null))?;
    assert_eq!(
        trace.to_basic_column_statistics(None)?.ndv(),
        NdvEstimate::upper_bound(10_001.0)
    );
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_collected_trace_preserves_ndv_estimate() -> Result<()> {
    let ctx = LiteTableContext::create().await?;
    let ndv = NdvEstimate::new(17.0, 2980.0);
    ctx.register_table_sql_with_stats(
        "CREATE TABLE t(k BIGINT NULL)",
        Some(table_statistics(ROWS)),
        HashMap::from([("k".to_string(), column(ndv))]),
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
    assert_eq!(parts.tables[0].column_stats["k"].ndv(), ndv);
    Ok(())
}
