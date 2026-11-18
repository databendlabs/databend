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

//! An upper-only provider NDV is a cap, not an estimate. The planner must not
//! divide by it: with the row count as the upper, `1 / ndv` would estimate an
//! equality filter to select one row and a join to produce one row. Instead
//! the planner falls back to its default equality selectivity, and uses the
//! upper only as a cap: on group counts, and on the join divisor.

use std::collections::HashMap;

use databend_common_catalog::BasicColumnStatistics;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_sql::optimizer::ir::RelExpr;
use databend_common_sql::plans::Plan;
use databend_common_statistics::Datum;
use databend_common_statistics::NdvEstimate;

use super::table_statistics;
use crate::framework::LiteTableContext;

const ROWS: u64 = 100_000;

fn column(min: i64, max: i64, ndv: NdvEstimate) -> BasicColumnStatistics {
    BasicColumnStatistics::new(
        Some(Datum::Int(min)),
        Some(Datum::Int(max)),
        0,
        ROWS * 8,
        ndv,
        Some(ROWS),
    )
    .unwrap()
}

async fn estimated_rows(ctx: &std::sync::Arc<LiteTableContext>, sql: &str) -> Result<f64> {
    let raw = ctx.bind_sql(sql).await?;
    let Plan::Query { s_expr, .. } = ctx.optimize_plan(raw).await? else {
        return Err(ErrorCode::Internal("expected query plan"));
    };
    let stats =
        RelExpr::with_p_expr(s_expr.planned()?.expr()).derive_cardinality(&ctx.stat_context()?)?;
    Ok(stats.cardinality)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_upper_only_ndv_is_a_cap_not_an_estimate() -> Result<()> {
    let ctx = LiteTableContext::create().await?;
    ctx.configure_for_optimizer_case(true)?;
    // `k` spans [0, 999], `g` spans [0, 9]. `point` carries exact NDVs,
    // `rows_upper` only the non-NULL row count as the upper (a provider with
    // no NDV source), `nostat` has no column statistics at all.
    ctx.register_table_sql_with_stats(
        "CREATE TABLE point(k INT NOT NULL, g INT NOT NULL)",
        Some(table_statistics(ROWS)),
        HashMap::from([
            ("k".to_string(), column(0, 999, NdvEstimate::exact(1000.0))),
            ("g".to_string(), column(0, 9, NdvEstimate::exact(10.0))),
        ]),
        HashMap::new(),
    )
    .await?;
    ctx.register_table_sql_with_stats(
        "CREATE TABLE rows_upper(k INT NOT NULL, g INT NOT NULL)",
        Some(table_statistics(ROWS)),
        HashMap::from([
            (
                "k".to_string(),
                column(0, 999, NdvEstimate::upper_bound(1000.0)),
            ),
            (
                "g".to_string(),
                column(0, 9, NdvEstimate::upper_bound(10.0)),
            ),
        ]),
        HashMap::new(),
    )
    .await?;

    let rows = |table: &str, sql: &str| {
        let ctx = ctx.clone();
        let sql = sql.replace("{t}", table);
        async move { estimated_rows(&ctx, &sql).await }
    };
    // (sql, point estimate, upper-only estimate)
    let cases: [(&str, f64, f64); 8] = [
        // Equality: `1 / upper` would give 100 (k) and 10000 (g) as if the
        // upper were the NDV; the upper-only column takes the default
        // selectivity instead, like a column without statistics.
        ("SELECT * FROM {t} WHERE k = 5", 100.0, ROWS as f64 / 5.0),
        ("SELECT * FROM {t} WHERE g = 5", 10_000.0, ROWS as f64 / 5.0),
        // Range: the bounds decide, independent of the NDV.
        ("SELECT * FROM {t} WHERE k < 100", 10_000.0, 10_000.0),
        // Join: no point on either side takes the default equality
        // selectivity (0.2), neither one output row nor a cross product.
        (
            "SELECT * FROM {t} a JOIN {t} b ON a.k = b.k",
            1e7,
            (ROWS * ROWS) as f64 / 5.0,
        ),
        // The uppers cap the divisor: `g` holds at most 10 values, which
        // happens to exceed the default 5, so the default applies.
        (
            "SELECT * FROM {t} a JOIN {t} b ON a.g = b.g",
            1e9,
            (ROWS * ROWS) as f64 / 5.0,
        ),
        // Group by: the default ratio, capped by the product of the uppers.
        ("SELECT g, count() FROM {t} GROUP BY g", 10.0, 10.0),
        ("SELECT k, count() FROM {t} GROUP BY k", 1000.0, 1000.0),
        (
            "SELECT k, g, count() FROM {t} GROUP BY k, g",
            10_000.0,
            10_000.0,
        ),
    ];
    for (sql, point, upper_only) in cases {
        let actual_point = rows("point", sql).await?;
        let actual_upper_only = rows("rows_upper", sql).await?;
        assert!(
            (actual_point - point).abs() < 1e-6,
            "{sql}: point NDV estimated {actual_point}, expected {point}"
        );
        assert!(
            (actual_upper_only - upper_only).abs() < 1e-6,
            "{sql}: upper-only NDV estimated {actual_upper_only}, expected {upper_only}"
        );
    }
    // A boolean-like key caps the divisor below the default: at most 2
    // distinct values, so at least half of the pairs match.
    ctx.register_table_sql_with_stats(
        "CREATE TABLE flag_upper(b INT NOT NULL)",
        Some(table_statistics(ROWS)),
        HashMap::from([("b".to_string(), column(0, 1, NdvEstimate::upper_bound(2.0)))]),
        HashMap::new(),
    )
    .await?;
    let joined = estimated_rows(
        &ctx,
        "SELECT * FROM flag_upper a JOIN flag_upper b ON a.b = b.b",
    )
    .await?;
    assert!(
        (joined - (ROWS * ROWS) as f64 / 2.0).abs() < 1e-6,
        "boolean-like join estimated {joined}"
    );
    Ok(())
}
