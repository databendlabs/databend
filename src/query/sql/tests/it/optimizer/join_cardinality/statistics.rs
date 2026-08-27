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
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_sql::plans::JoinType;
use databend_common_sql::plans::Plan;
use databend_common_statistics::Histogram;

use super::super::column_stat;
use super::super::histogram_stat;
use super::super::table_statistics;
use super::common::collect_join_cardinalities;
use crate::framework::LiteTableContext;
use crate::framework::golden::open_golden_file;
use crate::framework::golden::write_case_title;

struct SqlJoinTable {
    ddl: &'static str,
    rows: u64,
    column_stats: HashMap<String, BasicColumnStatistics>,
    histograms: HashMap<String, Histogram>,
}

struct SqlJoinStatisticsCase {
    name: &'static str,
    description: &'static str,
    sql: &'static str,
    expected_join_type: JoinType,
    left: SqlJoinTable,
    right: SqlJoinTable,
}

fn sql_join_table(
    ddl: &'static str,
    rows: u64,
    columns: impl IntoIterator<Item = (&'static str, &'static str)>,
) -> Result<SqlJoinTable> {
    Ok(SqlJoinTable {
        ddl,
        rows,
        column_stats: columns
            .into_iter()
            .map(|(name, json)| Ok((name.to_string(), column_stat(json)?)))
            .collect::<Result<_>>()?,
        histograms: HashMap::new(),
    })
}

fn sql_join_table_with_histograms(
    ddl: &'static str,
    rows: u64,
    columns: impl IntoIterator<Item = (&'static str, &'static str)>,
    histograms: impl IntoIterator<Item = (&'static str, &'static str)>,
) -> Result<SqlJoinTable> {
    let mut table = sql_join_table(ddl, rows, columns)?;
    table.histograms = histograms
        .into_iter()
        .map(|(name, json)| Ok((name.to_string(), histogram_stat(json)?)))
        .collect::<Result<_>>()?;
    Ok(table)
}

fn write_sql_join_table_stats(
    file: &mut impl Write,
    side: &str,
    table: &SqlJoinTable,
) -> Result<()> {
    writeln!(file, "{side} ddl      : {}", table.ddl)?;
    writeln!(file, "{side} rows     : {}", table.rows)?;
    let mut column_stats = table.column_stats.iter().collect::<Vec<_>>();
    column_stats.sort_by_key(|(name, _)| *name);
    for (name, stat) in column_stats {
        let bounds = match (&stat.min, &stat.max) {
            (Some(min), Some(max)) => format!("min={min}, max={max}"),
            (None, None) => "all_null=true".to_string(),
            (min, max) => format!("min={min:?}, max={max:?}"),
        };
        writeln!(
            file,
            "input stat    : {side}.{name} {bounds}, ndv={:?}, null={}",
            stat.ndv, stat.null_count
        )?;
    }
    let mut histogram_columns = table.histograms.keys().collect::<Vec<_>>();
    histogram_columns.sort();
    for name in histogram_columns {
        writeln!(file, "input hist    : {side}.{name} present")?;
    }
    Ok(())
}

async fn write_sql_join_statistics_case(
    file: &mut impl Write,
    case: &SqlJoinStatisticsCase,
) -> Result<()> {
    write_case_title(file, case.name, case.description)?;
    writeln!(file, "sql           : {}", case.sql)?;
    write_sql_join_table_stats(file, "left", &case.left)?;
    write_sql_join_table_stats(file, "right", &case.right)?;

    let ctx = LiteTableContext::create().await?;
    ctx.register_table_sql_with_stats(
        case.left.ddl,
        Some(table_statistics(case.left.rows)),
        case.left.column_stats.clone(),
        case.left.histograms.clone(),
    )
    .await?;
    ctx.register_table_sql_with_stats(
        case.right.ddl,
        Some(table_statistics(case.right.rows)),
        case.right.column_stats.clone(),
        case.right.histograms.clone(),
    )
    .await?;

    let plan = ctx.optimize_plan(ctx.bind_sql(case.sql).await?).await?;
    let Plan::Query {
        s_expr, metadata, ..
    } = plan
    else {
        return Err(ErrorCode::Internal("SELECT should bind to a query plan"));
    };
    let metadata = metadata.read();
    let joins =
        collect_join_cardinalities(file, &metadata, &s_expr, case.expected_join_type, case.name)?;
    assert_eq!(joins, 1);
    writeln!(file)?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_join_statistics_boundaries_golden() -> Result<()> {
    let mut file = open_golden_file("optimizer", "join_cardinality/statistics.txt")?;
    for case in sql_join_statistics_cases()? {
        write_sql_join_statistics_case(&mut file, &case).await?;
    }
    Ok(())
}

fn sql_join_statistics_cases() -> Result<Vec<SqlJoinStatisticsCase>> {
    Ok(vec![
        SqlJoinStatisticsCase {
            name: "inner_nullable_equality",
            description: "Regular equality rejects NULL join keys and exposes the final cardinality and both output column statistics.",
            sql: "SELECT * FROM l INNER JOIN r ON l.k = r.k",
            expected_join_type: JoinType::Inner,
            left: sql_join_table("CREATE TABLE l(k INT NULL)", 4, [(
                "k",
                r#"{"min": 1, "max": 3, "ndv": 3, "null_count": 1}"#,
            )])?,
            right: sql_join_table("CREATE TABLE r(k INT NULL)", 3, [(
                "k",
                r#"{"min": 1, "max": 2, "ndv": 2, "null_count": 1}"#,
            )])?,
        },
        SqlJoinStatisticsCase {
            name: "inner_canonical_integer_to_string_key",
            description: "An integer compared with a string cast of another integer uses the equality-preserving integer source for join statistics.",
            sql: "SELECT * FROM l INNER JOIN r ON l.k = CAST(r.k AS STRING)",
            expected_join_type: JoinType::Inner,
            left: sql_join_table("CREATE TABLE l(k INT64 NOT NULL)", 4, [(
                "k",
                r#"{"min": 1, "max": 4, "ndv": 4, "null_count": 0}"#,
            )])?,
            right: sql_join_table("CREATE TABLE r(k INT32 NOT NULL)", 3, [(
                "k",
                r#"{"min": 0, "max": 1, "ndv": 2, "null_count": 0}"#,
            )])?,
        },
        SqlJoinStatisticsCase {
            name: "correlated_exists_projection_nullable_key",
            description: "A nullable correlated EXISTS projection is decorrelated into RIGHT MARK with a null-safe correlation equality key.",
            sql: "SELECT EXISTS (SELECT 1 FROM r WHERE r.k = l.k) FROM l",
            expected_join_type: JoinType::RightMark,
            left: sql_join_table("CREATE TABLE l(k INT NULL)", 4, [(
                "k",
                r#"{"min": 1, "max": 3, "ndv": 3, "null_count": 1}"#,
            )])?,
            right: sql_join_table("CREATE TABLE r(k INT NULL)", 3, [(
                "k",
                r#"{"min": 1, "max": 2, "ndv": 2, "null_count": 1}"#,
            )])?,
        },
        SqlJoinStatisticsCase {
            name: "intersect_all_null_key_uses_null_safe_equality",
            description: "INTERSECT supplies a real null-safe equality key, so an all-NULL left input is matched by NULL rows on the right.",
            sql: "SELECT k FROM l INTERSECT SELECT k FROM r",
            expected_join_type: JoinType::RightSemi,
            left: sql_join_table("CREATE TABLE l(k INT NULL)", 4, [(
                "k",
                r#"{"min": null, "max": null, "ndv": 0, "null_count": 4}"#,
            )])?,
            right: sql_join_table("CREATE TABLE r(k INT NULL)", 10, [(
                "k",
                r#"{"min": 1, "max": 3, "ndv": 3, "null_count": 2}"#,
            )])?,
        },
        SqlJoinStatisticsCase {
            name: "intersect_nullable_key_combines_value_and_null_matches",
            description: "A null-safe equality from INTERSECT combines ordinary value matches with NULL-to-NULL matches when estimating matched rows.",
            sql: "SELECT k FROM l INTERSECT SELECT k FROM r",
            expected_join_type: JoinType::RightSemi,
            left: sql_join_table("CREATE TABLE l(k INT NULL)", 3, [(
                "k",
                r#"{"min": 1, "max": 1, "ndv": 1, "null_count": 2}"#,
            )])?,
            right: sql_join_table("CREATE TABLE r(k INT NULL)", 4, [(
                "k",
                r#"{"min": 1, "max": 1, "ndv": 1, "null_count": 3}"#,
            )])?,
        },
        SqlJoinStatisticsCase {
            name: "intersect_null_only_match_drops_value_histogram",
            description: "When INTERSECT matches only NULL, the preserved key becomes AllNull and does not retain its input value histogram.",
            sql: "SELECT k FROM l INTERSECT SELECT k FROM r",
            expected_join_type: JoinType::LeftSemi,
            left: sql_join_table_with_histograms(
                "CREATE TABLE l(k INT NULL)",
                2,
                [("k", r#"{"min": 1, "max": 1, "ndv": 1, "null_count": 1}"#)],
                [(
                    "k",
                    r#"{
                    "accuracy": true,
                    "buckets": [
                        {"lower_bound": {"Int": 1}, "upper_bound": {"Int": 1}, "num_values": 1.0, "num_distinct": 1.0}
                    ]
                }"#,
                )],
            )?,
            right: sql_join_table("CREATE TABLE r(k INT NULL)", 2, [(
                "k",
                r#"{"min": 2, "max": 2, "ndv": 1, "null_count": 1}"#,
            )])?,
        },
        SqlJoinStatisticsCase {
            name: "intersect_missing_peer_statistics_uses_null_safe_fallback",
            description: "An incomplete null-safe equality keeps the preserved-side INTERSECT cardinality without inventing a matched value distribution.",
            sql: "SELECT k FROM l INTERSECT SELECT k FROM r",
            expected_join_type: JoinType::RightSemi,
            left: sql_join_table("CREATE TABLE l(k INT NULL)", 4, [(
                "k",
                r#"{"min": 1, "max": 3, "ndv": 3, "null_count": 1}"#,
            )])?,
            right: sql_join_table("CREATE TABLE r(k INT NULL)", 20, [])?,
        },
        SqlJoinStatisticsCase {
            name: "inner_derived_boolean_equality",
            description: "Boolean join-key statistics are derived by EvalScalar from numeric input statistics rather than injected at the scan boundary.",
            sql: "SELECT * FROM (SELECT k > 1 AS b FROM l) dl INNER JOIN (SELECT k > 1 AS b FROM r) dr ON dl.b = dr.b",
            expected_join_type: JoinType::Inner,
            left: sql_join_table("CREATE TABLE l(k INT NULL)", 4, [(
                "k",
                r#"{"min": 1, "max": 3, "ndv": 3, "null_count": 1}"#,
            )])?,
            right: sql_join_table("CREATE TABLE r(k INT NULL)", 3, [(
                "k",
                r#"{"min": 1, "max": 2, "ndv": 2, "null_count": 1}"#,
            )])?,
        },
        SqlJoinStatisticsCase {
            name: "inner_derived_boolean_lossless_cast",
            description: "A lossless Boolean-to-UInt8 join cast consumes Boolean statistics derived upstream instead of relying on serialized Boolean column statistics.",
            sql: "SELECT * FROM (SELECT k > 1 AS b FROM l) dl INNER JOIN (SELECT k > 1 AS b FROM r) dr ON CAST(dl.b AS UInt8) = CAST(dr.b AS UInt8)",
            expected_join_type: JoinType::Inner,
            left: sql_join_table("CREATE TABLE l(k INT NULL)", 4, [(
                "k",
                r#"{"min": 1, "max": 3, "ndv": 3, "null_count": 1}"#,
            )])?,
            right: sql_join_table("CREATE TABLE r(k INT NULL)", 3, [(
                "k",
                r#"{"min": 1, "max": 2, "ndv": 2, "null_count": 1}"#,
            )])?,
        },
        SqlJoinStatisticsCase {
            name: "inner_disjoint_bounds",
            description: "Disjoint value bounds drive regular equality to zero matches without a histogram.",
            sql: "SELECT * FROM l INNER JOIN r ON l.k = r.k",
            expected_join_type: JoinType::Inner,
            left: sql_join_table("CREATE TABLE l(k INT NOT NULL)", 4, [(
                "k",
                r#"{"min": 1, "max": 3, "ndv": 3, "null_count": 0}"#,
            )])?,
            right: sql_join_table("CREATE TABLE r(k INT NOT NULL)", 5, [(
                "k",
                r#"{"min": 10, "max": 14, "ndv": 5, "null_count": 0}"#,
            )])?,
        },
        SqlJoinStatisticsCase {
            name: "left_outer_disjoint_bounds",
            description: "A disjoint LEFT join preserves its outer input and NULL-extends every column on the nullable side, including non-key payloads.",
            sql: "SELECT * FROM l LEFT JOIN r ON l.k = r.k",
            expected_join_type: JoinType::Right,
            left: sql_join_table("CREATE TABLE l(k INT NULL)", 4, [(
                "k",
                r#"{"min": 1, "max": 3, "ndv": 3, "null_count": 1}"#,
            )])?,
            right: sql_join_table("CREATE TABLE r(k INT NULL, payload INT NOT NULL)", 5, [
                ("k", r#"{"min": 10, "max": 14, "ndv": 5, "null_count": 1}"#),
                (
                    "payload",
                    r#"{"min": 20, "max": 24, "ndv": 5, "null_count": 0}"#,
                ),
            ])?,
        },
        SqlJoinStatisticsCase {
            name: "left_outer_partial_overlap",
            description: "A partially matched LEFT join adds one unmatched outer row to the nullable-side join key's NULL count.",
            sql: "SELECT * FROM l LEFT JOIN r ON l.k = r.k",
            expected_join_type: JoinType::Left,
            left: sql_join_table("CREATE TABLE l(k INT NOT NULL)", 2, [(
                "k",
                r#"{"min": 1, "max": 2, "ndv": 2, "null_count": 0}"#,
            )])?,
            right: sql_join_table("CREATE TABLE r(k INT NOT NULL)", 1, [(
                "k",
                r#"{"min": 1, "max": 1, "ndv": 1, "null_count": 0}"#,
            )])?,
        },
        SqlJoinStatisticsCase {
            name: "left_semi_non_key_histogram_finish",
            description: "A selective SEMI join keeps the preserved join-key histogram while dropping the now-inaccurate synthesized histogram on its payload column.",
            sql: "SELECT * FROM l LEFT SEMI JOIN r ON l.k = r.k",
            expected_join_type: JoinType::LeftSemi,
            left: sql_join_table(
                "CREATE TABLE l(k INT NOT NULL, payload INT NOT NULL)",
                10,
                [
                    ("k", r#"{"min": 1, "max": 10, "ndv": 10, "null_count": 0}"#),
                    (
                        "payload",
                        r#"{"min": 1, "max": 10, "ndv": 10, "null_count": 0}"#,
                    ),
                ],
            )?,
            right: sql_join_table("CREATE TABLE r(k INT NOT NULL)", 5, [(
                "k",
                r#"{"min": 1, "max": 5, "ndv": 5, "null_count": 0}"#,
            )])?,
        },
        SqlJoinStatisticsCase {
            name: "inner_non_key_contraction",
            description: "A selective join reduces non-key NDV and NULL rows using matched input rows before join fanout, then drops the non-key histogram.",
            sql: "SELECT * FROM l INNER JOIN r ON l.k = r.k",
            expected_join_type: JoinType::Inner,
            left: sql_join_table("CREATE TABLE l(k INT NOT NULL, payload INT NULL)", 1000, [
                (
                    "k",
                    r#"{"min": 1, "max": 100, "ndv": 100, "null_count": 0}"#,
                ),
                (
                    "payload",
                    r#"{"min": 1, "max": 10, "ndv": 10, "null_count": 200}"#,
                ),
            ])?,
            right: sql_join_table("CREATE TABLE r(k INT NOT NULL)", 10, [(
                "k",
                r#"{"min": 1, "max": 10, "ndv": 10, "null_count": 0}"#,
            )])?,
        },
        SqlJoinStatisticsCase {
            name: "inner_non_key_expansion",
            description: "Join fanout expands non-key NULL row counts while keeping the surviving NDV capped by its input range.",
            sql: "SELECT * FROM l INNER JOIN r ON l.k = r.k",
            expected_join_type: JoinType::Inner,
            left: sql_join_table("CREATE TABLE l(k INT NOT NULL, payload INT NULL)", 100, [
                ("k", r#"{"min": 1, "max": 10, "ndv": 10, "null_count": 0}"#),
                (
                    "payload",
                    r#"{"min": 1, "max": 10, "ndv": 10, "null_count": 20}"#,
                ),
            ])?,
            right: sql_join_table("CREATE TABLE r(k INT NOT NULL)", 100, [(
                "k",
                r#"{"min": 1, "max": 10, "ndv": 10, "null_count": 0}"#,
            )])?,
        },
        SqlJoinStatisticsCase {
            name: "inner_derived_boolean_non_key",
            description: "A Boolean non-key statistic derived upstream is contracted by the INNER join without injecting Boolean statistics at Scan.",
            sql: "SELECT * FROM (SELECT k, k > 50 AS flag FROM l) dl INNER JOIN r ON dl.k = r.k",
            expected_join_type: JoinType::Inner,
            left: sql_join_table("CREATE TABLE l(k INT NOT NULL)", 100, [(
                "k",
                r#"{"min": 1, "max": 100, "ndv": 100, "null_count": 0}"#,
            )])?,
            right: sql_join_table("CREATE TABLE r(k INT NOT NULL)", 10, [(
                "k",
                r#"{"min": 1, "max": 10, "ndv": 10, "null_count": 0}"#,
            )])?,
        },
        SqlJoinStatisticsCase {
            name: "inner_derived_all_null_non_key",
            description: "An all-NULL non-key statistic derived upstream scales its NULL row count with the INNER join output.",
            sql: "SELECT * FROM (SELECT k, NULL AS payload FROM l) dl INNER JOIN r ON dl.k = r.k",
            expected_join_type: JoinType::Inner,
            left: sql_join_table("CREATE TABLE l(k INT NOT NULL)", 100, [(
                "k",
                r#"{"min": 1, "max": 100, "ndv": 100, "null_count": 0}"#,
            )])?,
            right: sql_join_table("CREATE TABLE r(k INT NOT NULL)", 10, [(
                "k",
                r#"{"min": 1, "max": 10, "ndv": 10, "null_count": 0}"#,
            )])?,
        },
        SqlJoinStatisticsCase {
            name: "inner_two_key_equality",
            description: "Multiple equality keys are applied cumulatively; a disjoint second key eliminates matches after the first key has narrowed statistics.",
            sql: "SELECT * FROM l INNER JOIN r ON l.k1 = r.k1 AND l.k2 = r.k2",
            expected_join_type: JoinType::Inner,
            left: sql_join_table("CREATE TABLE l(k1 INT NOT NULL, k2 INT NOT NULL)", 100, [
                ("k1", r#"{"min": 1, "max": 10, "ndv": 10, "null_count": 0}"#),
                ("k2", r#"{"min": 1, "max": 2, "ndv": 2, "null_count": 0}"#),
            ])?,
            right: sql_join_table("CREATE TABLE r(k1 INT NOT NULL, k2 INT NOT NULL)", 100, [
                ("k1", r#"{"min": 1, "max": 10, "ndv": 10, "null_count": 0}"#),
                ("k2", r#"{"min": 20, "max": 21, "ndv": 2, "null_count": 0}"#),
            ])?,
        },
        SqlJoinStatisticsCase {
            name: "inner_three_key_combined_decay",
            description: "Three non-zero equality estimates use exponential backoff, then retain and scale every estimated equality-key histogram to the combined cardinality.",
            sql: "SELECT * FROM l INNER JOIN r ON l.k1 = r.k1 AND l.k2 = r.k2 AND l.k3 = r.k3",
            expected_join_type: JoinType::Inner,
            left: sql_join_table(
                "CREATE TABLE l(k1 INT NOT NULL, k2 INT NOT NULL, k3 INT NOT NULL)",
                10,
                [
                    ("k1", r#"{"min": 1, "max": 4, "ndv": 4, "null_count": 0}"#),
                    ("k2", r#"{"min": 1, "max": 10, "ndv": 10, "null_count": 0}"#),
                    ("k3", r#"{"min": 1, "max": 2, "ndv": 2, "null_count": 0}"#),
                ],
            )?,
            right: sql_join_table(
                "CREATE TABLE r(k1 INT NOT NULL, k2 INT NOT NULL, k3 INT NOT NULL)",
                10,
                [
                    ("k1", r#"{"min": 1, "max": 4, "ndv": 4, "null_count": 0}"#),
                    ("k2", r#"{"min": 1, "max": 10, "ndv": 10, "null_count": 0}"#),
                    ("k3", r#"{"min": 1, "max": 2, "ndv": 2, "null_count": 0}"#),
                ],
            )?,
        },
        SqlJoinStatisticsCase {
            name: "inner_three_key_combined_decay_reordered",
            description: "Reordering the same three equality conditions leaves the combined cardinality and propagated column distributions unchanged.",
            sql: "SELECT * FROM l INNER JOIN r ON l.k3 = r.k3 AND l.k1 = r.k1 AND l.k2 = r.k2",
            expected_join_type: JoinType::Inner,
            left: sql_join_table(
                "CREATE TABLE l(k1 INT NOT NULL, k2 INT NOT NULL, k3 INT NOT NULL)",
                10,
                [
                    ("k1", r#"{"min": 1, "max": 4, "ndv": 4, "null_count": 0}"#),
                    ("k2", r#"{"min": 1, "max": 10, "ndv": 10, "null_count": 0}"#),
                    ("k3", r#"{"min": 1, "max": 2, "ndv": 2, "null_count": 0}"#),
                ],
            )?,
            right: sql_join_table(
                "CREATE TABLE r(k1 INT NOT NULL, k2 INT NOT NULL, k3 INT NOT NULL)",
                10,
                [
                    ("k1", r#"{"min": 1, "max": 4, "ndv": 4, "null_count": 0}"#),
                    ("k2", r#"{"min": 1, "max": 10, "ndv": 10, "null_count": 0}"#),
                    ("k3", r#"{"min": 1, "max": 2, "ndv": 2, "null_count": 0}"#),
                ],
            )?,
        },
        SqlJoinStatisticsCase {
            name: "inner_repeated_column_uses_most_selective_histogram",
            description: "When one column participates in multiple equality expressions, its most selective local expression supplies the histogram and the combined selectivity scales every propagated equality-key histogram.",
            sql: "SELECT * FROM l INNER JOIN r ON l.k = r.k1 AND CAST(l.k AS INT) = r.k2",
            expected_join_type: JoinType::Inner,
            left: sql_join_table("CREATE TABLE l(k SMALLINT NULL)", 100, [(
                "k",
                r#"{"min": 1, "max": 10, "ndv": 10, "null_count": 0}"#,
            )])?,
            right: sql_join_table(
                "CREATE TABLE r(k1 SMALLINT NOT NULL, k2 INT NOT NULL)",
                1000,
                [
                    (
                        "k1",
                        r#"{"min": 5, "max": 104, "ndv": 100, "null_count": 0}"#,
                    ),
                    ("k2", r#"{"min": 1, "max": 10, "ndv": 10, "null_count": 0}"#),
                ],
            )?,
        },
        SqlJoinStatisticsCase {
            name: "inner_repeated_identity_column_combines_distributions",
            description: "When one identity column participates in multiple direct equalities, all matched distributions narrow its bounds while the most selective equality supplies its histogram shape.",
            sql: "SELECT * FROM l INNER JOIN r ON l.k = r.k1 AND l.k = r.k2",
            expected_join_type: JoinType::Inner,
            left: sql_join_table("CREATE TABLE l(k INT NOT NULL)", 100, [(
                "k",
                r#"{"min": 1, "max": 100, "ndv": 100, "null_count": 0}"#,
            )])?,
            right: sql_join_table("CREATE TABLE r(k1 INT NOT NULL, k2 INT NOT NULL)", 1000, [
                (
                    "k1",
                    r#"{"min": 10, "max": 80, "ndv": 71, "null_count": 0}"#,
                ),
                (
                    "k2",
                    r#"{"min": 30, "max": 1000, "ndv": 971, "null_count": 0}"#,
                ),
            ])?,
        },
        SqlJoinStatisticsCase {
            name: "inner_repeated_column_complex_winner_blocks_source_histogram",
            description: "When a complex equality expression is more selective than a direct equality using the same source column, it affects cardinality but cannot supply that source column's histogram.",
            sql: "SELECT * FROM l INNER JOIN r ON l.k = r.k1 AND CAST(l.k AS INT) = r.k2",
            expected_join_type: JoinType::Inner,
            left: sql_join_table("CREATE TABLE l(k SMALLINT NOT NULL)", 100, [(
                "k",
                r#"{"min": 1, "max": 100, "ndv": 100, "null_count": 0}"#,
            )])?,
            right: sql_join_table(
                "CREATE TABLE r(k1 SMALLINT NOT NULL, k2 INT NOT NULL)",
                1000,
                [
                    ("k1", r#"{"min": 1, "max": 10, "ndv": 10, "null_count": 0}"#),
                    (
                        "k2",
                        r#"{"min": 1, "max": 1000, "ndv": 1000, "null_count": 0}"#,
                    ),
                ],
            )?,
        },
        SqlJoinStatisticsCase {
            name: "inner_unknown_non_equi_uses_default_decay",
            description: "A residual comparison between varying columns uses the default 0.5 selectivity in exponential-backoff cardinality and NDV scaling while leaving equality histogram propagation intact.",
            sql: "SELECT * FROM l INNER JOIN r ON l.k1 = r.k1 AND l.k2 = r.k2 AND l.k1 > r.k2",
            expected_join_type: JoinType::Inner,
            left: sql_join_table("CREATE TABLE l(k1 INT NOT NULL, k2 INT NOT NULL)", 100, [
                (
                    "k1",
                    r#"{"min": 1, "max": 100, "ndv": 100, "null_count": 0}"#,
                ),
                ("k2", r#"{"min": 1, "max": 10, "ndv": 10, "null_count": 0}"#),
            ])?,
            right: sql_join_table("CREATE TABLE r(k1 INT NOT NULL, k2 INT NOT NULL)", 100, [
                ("k1", r#"{"min": 1, "max": 10, "ndv": 10, "null_count": 0}"#),
                ("k2", r#"{"min": 1, "max": 10, "ndv": 10, "null_count": 0}"#),
            ])?,
        },
        SqlJoinStatisticsCase {
            name: "inner_modeled_non_equi_joins_combined_decay",
            description: "A cross-side residual comparison with a singleton operand has numeric selectivity, so it joins the equality estimate in exponential-backoff cardinality and NDV scaling while leaving equality histogram propagation intact.",
            sql: "SELECT * FROM l INNER JOIN r ON l.id = r.id AND l.k > r.c",
            expected_join_type: JoinType::Inner,
            left: sql_join_table("CREATE TABLE l(id INT NOT NULL, k INT NOT NULL)", 100, [
                (
                    "id",
                    r#"{"min": 1, "max": 100, "ndv": 100, "null_count": 0}"#,
                ),
                (
                    "k",
                    r#"{"min": 1, "max": 100, "ndv": 100, "null_count": 0}"#,
                ),
            ])?,
            right: sql_join_table("CREATE TABLE r(id INT NOT NULL, c INT NOT NULL)", 100, [
                (
                    "id",
                    r#"{"min": 1, "max": 100, "ndv": 100, "null_count": 0}"#,
                ),
                ("c", r#"{"min": 5, "max": 5, "ndv": 1, "null_count": 0}"#),
            ])?,
        },
        SqlJoinStatisticsCase {
            name: "inner_only_modeled_non_equi",
            description: "A Join containing only a cross-side comparison with a singleton operand uses the shared selectivity estimator through the production optimizer path.",
            sql: "SELECT * FROM l INNER JOIN r ON l.k > r.c",
            expected_join_type: JoinType::Inner,
            left: sql_join_table("CREATE TABLE l(k INT NOT NULL)", 100, [(
                "k",
                r#"{"min": 1, "max": 100, "ndv": 100, "null_count": 0}"#,
            )])?,
            right: sql_join_table("CREATE TABLE r(c INT NOT NULL)", 10, [(
                "c",
                r#"{"min": 5, "max": 5, "ndv": 1, "null_count": 0}"#,
            )])?,
        },
        SqlJoinStatisticsCase {
            name: "inner_only_unknown_non_equi",
            description: "A Join containing only a comparison between two varying columns uses the conservative unknown-selectivity fallback.",
            sql: "SELECT * FROM l INNER JOIN r ON l.k > r.c",
            expected_join_type: JoinType::Inner,
            left: sql_join_table("CREATE TABLE l(k INT NOT NULL)", 100, [(
                "k",
                r#"{"min": 1, "max": 100, "ndv": 100, "null_count": 0}"#,
            )])?,
            right: sql_join_table("CREATE TABLE r(c INT NOT NULL)", 100, [(
                "c",
                r#"{"min": 1, "max": 100, "ndv": 100, "null_count": 0}"#,
            )])?,
        },
        SqlJoinStatisticsCase {
            name: "inner_non_equi_missing_column_statistics",
            description: "A cross-side comparison with missing statistics reaches the selectivity estimator fallback without bypassing Join evaluation.",
            sql: "SELECT * FROM l INNER JOIN r ON l.k > r.c",
            expected_join_type: JoinType::Inner,
            left: sql_join_table("CREATE TABLE l(k INT NOT NULL)", 100, [(
                "k",
                r#"{"min": 1, "max": 100, "ndv": 100, "null_count": 0}"#,
            )])?,
            right: sql_join_table("CREATE TABLE r(c INT NOT NULL)", 100, [])?,
        },
        SqlJoinStatisticsCase {
            name: "left_outer_only_modeled_non_equi",
            description: "A non-equi LEFT join estimates rows matching at least one peer and never falls below the preserved left input.",
            sql: "SELECT * FROM l LEFT JOIN r ON l.k > r.c",
            expected_join_type: JoinType::Left,
            left: sql_join_table("CREATE TABLE l(k INT NOT NULL)", 100, [(
                "k",
                r#"{"min": 1, "max": 100, "ndv": 100, "null_count": 0}"#,
            )])?,
            right: sql_join_table("CREATE TABLE r(c INT NOT NULL)", 10, [(
                "c",
                r#"{"min": 5, "max": 5, "ndv": 1, "null_count": 0}"#,
            )])?,
        },
        SqlJoinStatisticsCase {
            name: "right_outer_only_modeled_non_equi",
            description: "With one row on the non-preserved side, a non-equi RIGHT join returns exactly the fifteen preserved matched-or-unmatched rows.",
            sql: "SELECT * FROM l RIGHT JOIN r ON l.k > r.c",
            expected_join_type: JoinType::Left,
            left: sql_join_table("CREATE TABLE l(k INT NOT NULL)", 1, [(
                "k",
                r#"{"min": 10, "max": 10, "ndv": 1, "null_count": 0}"#,
            )])?,
            right: sql_join_table("CREATE TABLE r(c INT NOT NULL)", 15, [(
                "c",
                r#"{"min": 1, "max": 15, "ndv": 15, "null_count": 0}"#,
            )])?,
        },
        SqlJoinStatisticsCase {
            name: "full_outer_only_modeled_non_equi",
            description: "A non-equi FULL join includes unmatched rows from both inputs and never falls below either input cardinality.",
            sql: "SELECT * FROM l FULL JOIN r ON l.k < r.c",
            expected_join_type: JoinType::Full,
            left: sql_join_table("CREATE TABLE l(k INT NOT NULL)", 100, [(
                "k",
                r#"{"min": 1, "max": 100, "ndv": 100, "null_count": 0}"#,
            )])?,
            right: sql_join_table("CREATE TABLE r(c INT NOT NULL)", 10, [(
                "c",
                r#"{"min": 5, "max": 5, "ndv": 1, "null_count": 0}"#,
            )])?,
        },
        SqlJoinStatisticsCase {
            name: "left_outer_mixed_equi_and_non_equi",
            description: "A mixed-condition LEFT join caps matched left rows by the final matched-pair estimate before adding unmatched rows.",
            sql: "SELECT * FROM l LEFT JOIN r ON l.id = r.id AND l.k > r.c",
            expected_join_type: JoinType::Left,
            left: sql_join_table("CREATE TABLE l(id INT NOT NULL, k INT NOT NULL)", 100, [
                (
                    "id",
                    r#"{"min": 1, "max": 100, "ndv": 100, "null_count": 0}"#,
                ),
                (
                    "k",
                    r#"{"min": 1, "max": 100, "ndv": 100, "null_count": 0}"#,
                ),
            ])?,
            right: sql_join_table("CREATE TABLE r(id INT NOT NULL, c INT NOT NULL)", 100, [
                (
                    "id",
                    r#"{"min": 1, "max": 100, "ndv": 100, "null_count": 0}"#,
                ),
                ("c", r#"{"min": 5, "max": 5, "ndv": 1, "null_count": 0}"#),
            ])?,
        },
        SqlJoinStatisticsCase {
            name: "left_semi_only_modeled_non_equi",
            description: "A non-equi LEFT SEMI join estimates left rows matching at least one right peer instead of preserving the full left input.",
            sql: "SELECT * FROM l LEFT SEMI JOIN r ON l.k > r.c",
            expected_join_type: JoinType::LeftSemi,
            left: sql_join_table("CREATE TABLE l(k INT NOT NULL)", 100, [(
                "k",
                r#"{"min": 1, "max": 100, "ndv": 100, "null_count": 0}"#,
            )])?,
            right: sql_join_table("CREATE TABLE r(c INT NOT NULL)", 10, [(
                "c",
                r#"{"min": 5, "max": 5, "ndv": 1, "null_count": 0}"#,
            )])?,
        },
        SqlJoinStatisticsCase {
            name: "right_semi_only_modeled_non_equi",
            description: "A non-equi RIGHT SEMI join estimates right rows matching at least one left peer instead of preserving the full right input.",
            sql: "SELECT * FROM l RIGHT SEMI JOIN r ON l.k > r.c",
            expected_join_type: JoinType::RightSemi,
            left: sql_join_table("CREATE TABLE l(k INT NOT NULL)", 100, [(
                "k",
                r#"{"min": 1, "max": 100, "ndv": 100, "null_count": 0}"#,
            )])?,
            right: sql_join_table("CREATE TABLE r(c INT NOT NULL)", 10, [(
                "c",
                r#"{"min": 5, "max": 5, "ndv": 1, "null_count": 0}"#,
            )])?,
        },
        SqlJoinStatisticsCase {
            name: "left_anti_only_modeled_non_equi",
            description: "A non-equi LEFT ANTI join subtracts modeled left matches while retaining the conservative reserve for uncertain overlap.",
            sql: "SELECT * FROM l LEFT ANTI JOIN r ON l.k > r.c",
            expected_join_type: JoinType::LeftAnti,
            left: sql_join_table("CREATE TABLE l(k INT NOT NULL)", 100, [(
                "k",
                r#"{"min": 1, "max": 100, "ndv": 100, "null_count": 0}"#,
            )])?,
            right: sql_join_table("CREATE TABLE r(c INT NOT NULL)", 10, [(
                "c",
                r#"{"min": 5, "max": 5, "ndv": 1, "null_count": 0}"#,
            )])?,
        },
        SqlJoinStatisticsCase {
            name: "right_anti_only_modeled_non_equi",
            description: "A non-equi RIGHT ANTI join subtracts modeled right matches while retaining the conservative reserve for uncertain overlap.",
            sql: "SELECT * FROM l RIGHT ANTI JOIN r ON l.k > r.c",
            expected_join_type: JoinType::RightAnti,
            left: sql_join_table("CREATE TABLE l(k INT NOT NULL)", 100, [(
                "k",
                r#"{"min": 1, "max": 100, "ndv": 100, "null_count": 0}"#,
            )])?,
            right: sql_join_table("CREATE TABLE r(c INT NOT NULL)", 10, [(
                "c",
                r#"{"min": 5, "max": 5, "ndv": 1, "null_count": 0}"#,
            )])?,
        },
        SqlJoinStatisticsCase {
            name: "asof_residual_uses_matched_pair_upper_bound",
            description: "ASOF avoids an independent peer-match model and only caps equality-derived side matches by the combined matched-pair estimate.",
            sql: "SELECT * FROM l ASOF JOIN r ON l.id = r.id AND l.t >= r.t",
            expected_join_type: JoinType::Asof,
            left: sql_join_table("CREATE TABLE l(id INT NOT NULL, t INT NOT NULL)", 100, [
                (
                    "id",
                    r#"{"min": 1, "max": 100, "ndv": 100, "null_count": 0}"#,
                ),
                ("t", r#"{"min": 1, "max": 10, "ndv": 10, "null_count": 0}"#),
            ])?,
            right: sql_join_table("CREATE TABLE r(id INT NOT NULL, t INT NOT NULL)", 100, [
                (
                    "id",
                    r#"{"min": 1, "max": 100, "ndv": 100, "null_count": 0}"#,
                ),
                (
                    "t",
                    r#"{"min": 100, "max": 200, "ndv": 100, "null_count": 0}"#,
                ),
            ])?,
        },
        SqlJoinStatisticsCase {
            name: "inner_unsupported_multicolumn_expression_uses_partial_stats",
            description: "When a two-column arithmetic key has no derived distribution, the direct peer's available statistics still participate in NULL-aware fallback and output propagation.",
            sql: "SELECT * FROM l INNER JOIN r ON l.a + l.b = r.k",
            expected_join_type: JoinType::Inner,
            left: sql_join_table("CREATE TABLE l(a INT NULL, b INT NULL)", 100, [
                ("a", r#"{"min": 1, "max": 50, "ndv": 50, "null_count": 2}"#),
                ("b", r#"{"min": 1, "max": 30, "ndv": 30, "null_count": 3}"#),
            ])?,
            right: sql_join_table("CREATE TABLE r(k INT NULL)", 100, [(
                "k",
                r#"{"min": 1, "max": 80, "ndv": 80, "null_count": 10}"#,
            )])?,
        },
        SqlJoinStatisticsCase {
            name: "inner_derived_expression_with_missing_peer_stats",
            description: "A derivable arithmetic key remains local when its direct peer lacks statistics, exercising the one-sided equality-result path without source value writeback.",
            sql: "SELECT * FROM l INNER JOIN r ON l.k + 1 = r.k",
            expected_join_type: JoinType::Inner,
            left: sql_join_table("CREATE TABLE l(k INT NULL)", 100, [(
                "k",
                r#"{"min": 1, "max": 50, "ndv": 50, "null_count": 5}"#,
            )])?,
            right: sql_join_table("CREATE TABLE r(k INT NULL)", 100, [])?,
        },
        SqlJoinStatisticsCase {
            name: "inner_missing_second_key_stats_preserves_first_key_histogram",
            description: "An equality condition with incomplete statistics contributes its conservative fallback without erasing the propagated histogram from another modeled key.",
            sql: "SELECT * FROM l INNER JOIN r ON l.k1 = r.k1 AND l.k2 = r.k2",
            expected_join_type: JoinType::Inner,
            left: sql_join_table_with_histograms(
                "CREATE TABLE l(k1 INT NOT NULL, k2 INT NOT NULL)",
                10,
                [
                    ("k1", r#"{"min": 1, "max": 10, "ndv": 10, "null_count": 0}"#),
                    ("k2", r#"{"min": 1, "max": 10, "ndv": 10, "null_count": 0}"#),
                ],
                [(
                    "k1",
                    r#"{
                    "accuracy": true,
                    "buckets": [
                        {"lower_bound": {"Int": 1}, "upper_bound": {"Int": 10}, "num_values": 10.0, "num_distinct": 10.0}
                    ]
                }"#,
                )],
            )?,
            right: sql_join_table_with_histograms(
                "CREATE TABLE r(k1 INT NOT NULL, k2 INT NOT NULL)",
                10,
                [("k1", r#"{"min": 1, "max": 5, "ndv": 5, "null_count": 0}"#)],
                [(
                    "k1",
                    r#"{
                    "accuracy": true,
                    "buckets": [
                        {"lower_bound": {"Int": 1}, "upper_bound": {"Int": 5}, "num_values": 10.0, "num_distinct": 5.0}
                    ]
                }"#,
                )],
            )?,
        },
        SqlJoinStatisticsCase {
            name: "left_semi_two_key_combined_decay",
            description: "Multiple equality keys apply exponential backoff to preserved-side matched rows, then retain and scale every estimated equality-key histogram on that side.",
            sql: "SELECT * FROM l LEFT SEMI JOIN r ON l.k1 = r.k1 AND l.k2 = r.k2",
            expected_join_type: JoinType::LeftSemi,
            left: sql_join_table("CREATE TABLE l(k1 INT NOT NULL, k2 INT NOT NULL)", 10, [
                ("k1", r#"{"min": 1, "max": 10, "ndv": 10, "null_count": 0}"#),
                ("k2", r#"{"min": 1, "max": 10, "ndv": 10, "null_count": 0}"#),
            ])?,
            right: sql_join_table("CREATE TABLE r(k1 INT NOT NULL, k2 INT NOT NULL)", 10, [
                ("k1", r#"{"min": 1, "max": 4, "ndv": 4, "null_count": 0}"#),
                ("k2", r#"{"min": 1, "max": 2, "ndv": 2, "null_count": 0}"#),
            ])?,
        },
        SqlJoinStatisticsCase {
            name: "left_anti_two_key_combines_histogram_uncertainty",
            description: "ANTI combines matched rows and histogram uncertainty from every equality key before applying its conservative overlap reserve.",
            sql: "SELECT * FROM l LEFT ANTI JOIN r ON l.k1 = r.k1 AND l.k2 = r.k2",
            expected_join_type: JoinType::LeftAnti,
            left: sql_join_table_with_histograms(
                "CREATE TABLE l(k1 VARCHAR NOT NULL, k2 INT NOT NULL)",
                100,
                [
                    (
                        "k1",
                        r#"{"min": "a", "max": "z", "ndv": 100, "null_count": 0}"#,
                    ),
                    ("k2", r#"{"min": 1, "max": 10, "ndv": 10, "null_count": 0}"#),
                ],
                [(
                    "k1",
                    r#"{
                    "accuracy": false,
                    "buckets": [
                        {"lower_bound": {"Bytes": [97]}, "upper_bound": {"Bytes": [122]}, "num_values": 100.0, "num_distinct": 100.0}
                    ]
                }"#,
                )],
            )?,
            right: sql_join_table_with_histograms(
                "CREATE TABLE r(k1 VARCHAR NOT NULL, k2 INT NOT NULL)",
                100,
                [
                    (
                        "k1",
                        r#"{"min": "a", "max": "z", "ndv": 95, "null_count": 0}"#,
                    ),
                    ("k2", r#"{"min": 1, "max": 10, "ndv": 10, "null_count": 0}"#),
                ],
                [(
                    "k1",
                    r#"{
                    "accuracy": false,
                    "buckets": [
                        {"lower_bound": {"Bytes": [97]}, "upper_bound": {"Bytes": [122]}, "num_values": 100.0, "num_distinct": 95.0}
                    ]
                }"#,
                )],
            )?,
        },
        SqlJoinStatisticsCase {
            name: "left_outer_nullable_key",
            description: "A left outer join excludes key NULLs from its inner estimate but preserves the outer-side NULL statistics.",
            sql: "SELECT * FROM l LEFT JOIN r ON l.k = r.k",
            expected_join_type: JoinType::Left,
            left: sql_join_table("CREATE TABLE l(k INT NULL)", 4, [(
                "k",
                r#"{"min": 42, "max": 44, "ndv": 2, "null_count": 1}"#,
            )])?,
            right: sql_join_table("CREATE TABLE r(k INT NULL)", 4, [(
                "k",
                r#"{"min": 42, "max": 45, "ndv": 3, "null_count": 1}"#,
            )])?,
        },
        SqlJoinStatisticsCase {
            name: "inner_lossless_cast_key",
            description: "A lossless injective cast participates in join estimation and rejects NULLs from its source column.",
            sql: "SELECT * FROM l INNER JOIN r ON CAST(l.k AS BIGINT) = r.k",
            expected_join_type: JoinType::Inner,
            left: sql_join_table("CREATE TABLE l(k INT NULL)", 4, [(
                "k",
                r#"{"min": 1, "max": 3, "ndv": 3, "null_count": 1}"#,
            )])?,
            right: sql_join_table("CREATE TABLE r(k BIGINT NULL)", 3, [(
                "k",
                r#"{"min": 1, "max": 2, "ndv": 2, "null_count": 1}"#,
            )])?,
        },
        SqlJoinStatisticsCase {
            name: "inner_mixed_numeric_equality",
            description: "Different integer SQL types reach the numeric comparison boundary through the normal binder and optimizer path.",
            sql: "SELECT * FROM l INNER JOIN r ON l.k = r.k",
            expected_join_type: JoinType::Inner,
            left: sql_join_table("CREATE TABLE l(k INT NOT NULL)", 4, [(
                "k",
                r#"{"min": 1, "max": 3, "ndv": 3, "null_count": 0}"#,
            )])?,
            right: sql_join_table("CREATE TABLE r(k BIGINT NOT NULL)", 3, [(
                "k",
                r#"{"min": 1, "max": 2, "ndv": 2, "null_count": 0}"#,
            )])?,
        },
        SqlJoinStatisticsCase {
            name: "inner_mixed_integer_large_histogram_boundaries",
            description: "Mixed signed and unsigned integer histograms keep adjacent values above 2^53 distinct through SQL comparison typing.",
            sql: "SELECT * FROM l INNER JOIN r ON l.k = r.k",
            expected_join_type: JoinType::Inner,
            left: sql_join_table_with_histograms(
                "CREATE TABLE l(k BIGINT NOT NULL)",
                3,
                [(
                    "k",
                    r#"{"min": 9007199254740992, "max": 9007199254741002, "ndv": 1, "null_count": 0}"#,
                )],
                [(
                    "k",
                    r#"{
                    "accuracy": true,
                    "buckets": [
                        {"lower_bound": {"Int": 9007199254740992}, "upper_bound": {"Int": 9007199254740992}, "num_values": 3.0, "num_distinct": 1.0}
                    ]
                }"#,
                )],
            )?,
            right: sql_join_table_with_histograms(
                "CREATE TABLE r(k UINT64 NOT NULL)",
                2,
                [(
                    "k",
                    r#"{"min": {"UInt": 9007199254740992}, "max": {"UInt": 9007199254741002}, "ndv": 1, "null_count": 0}"#,
                )],
                [(
                    "k",
                    r#"{
                    "accuracy": true,
                    "buckets": [
                        {"lower_bound": {"UInt": 9007199254740993}, "upper_bound": {"UInt": 9007199254740993}, "num_values": 2.0, "num_distinct": 1.0}
                    ]
                }"#,
                )],
            )?,
        },
        SqlJoinStatisticsCase {
            name: "inner_mixed_integer_upper_only_extremes",
            description: "Upper-only UInt64 and Int64 statistics at opposite numeric extremes produce no overlap without overflowing conversion.",
            sql: "SELECT * FROM l INNER JOIN r ON l.k = r.k",
            expected_join_type: JoinType::Inner,
            left: sql_join_table("CREATE TABLE l(k UINT64 NOT NULL)", 1, [(
                "k",
                r#"{"min": {"UInt": 18446744073709551615}, "max": {"UInt": 18446744073709551615}, "ndv": null, "null_count": 0}"#,
            )])?,
            right: sql_join_table("CREATE TABLE r(k BIGINT NOT NULL)", 1, [(
                "k",
                r#"{"min": 0, "max": 0, "ndv": null, "null_count": 0}"#,
            )])?,
        },
        SqlJoinStatisticsCase {
            name: "inner_float_cast_adjacent_large_integers",
            description: "Explicit Float64 comparison casts make adjacent integers above 2^53 collide, and the SQL estimate reflects that comparison domain.",
            sql: "SELECT * FROM l INNER JOIN r ON CAST(l.k AS DOUBLE) = CAST(r.k AS DOUBLE)",
            expected_join_type: JoinType::Inner,
            left: sql_join_table_with_histograms(
                "CREATE TABLE l(k BIGINT NOT NULL)",
                3,
                [(
                    "k",
                    r#"{"min": 9007199254740992, "max": 9007199254740992, "ndv": 1, "null_count": 0}"#,
                )],
                [(
                    "k",
                    r#"{
                    "accuracy": true,
                    "buckets": [
                        {"lower_bound": {"Int": 9007199254740992}, "upper_bound": {"Int": 9007199254740992}, "num_values": 3.0, "num_distinct": 1.0}
                    ]
                }"#,
                )],
            )?,
            right: sql_join_table_with_histograms(
                "CREATE TABLE r(k BIGINT NOT NULL)",
                2,
                [(
                    "k",
                    r#"{"min": 9007199254740993, "max": 9007199254740993, "ndv": 1, "null_count": 0}"#,
                )],
                [(
                    "k",
                    r#"{
                    "accuracy": true,
                    "buckets": [
                        {"lower_bound": {"Int": 9007199254740993}, "upper_bound": {"Int": 9007199254740993}, "num_values": 2.0, "num_distinct": 1.0}
                    ]
                }"#,
                )],
            )?,
        },
        SqlJoinStatisticsCase {
            name: "inner_mixed_numeric_histogram_not_propagated",
            description: "Mixed signed and unsigned histograms contribute to cardinality, while incompatible typed histograms are not propagated to output columns.",
            sql: "SELECT * FROM l INNER JOIN r ON l.k = r.k",
            expected_join_type: JoinType::Inner,
            left: sql_join_table_with_histograms(
                "CREATE TABLE l(k INT NOT NULL)",
                3,
                [("k", r#"{"min": -5, "max": 10, "ndv": 1, "null_count": 0}"#)],
                [(
                    "k",
                    r#"{
                    "accuracy": true,
                    "buckets": [
                        {"lower_bound": {"Int": 1}, "upper_bound": {"Int": 1}, "num_values": 3.0, "num_distinct": 1.0}
                    ]
                }"#,
                )],
            )?,
            right: sql_join_table_with_histograms(
                "CREATE TABLE r(k UINT8 NOT NULL)",
                2,
                [(
                    "k",
                    r#"{"min": {"UInt": 0}, "max": {"UInt": 15}, "ndv": 1, "null_count": 0}"#,
                )],
                [(
                    "k",
                    r#"{
                    "accuracy": true,
                    "buckets": [
                        {"lower_bound": {"UInt": 1}, "upper_bound": {"UInt": 1}, "num_values": 2.0, "num_distinct": 1.0}
                    ]
                }"#,
                )],
            )?,
        },
        SqlJoinStatisticsCase {
            name: "inner_try_cast_key",
            description: "TRY_CAST stays outside the NULL-rejection whitelist, so its source statistics are not rewritten.",
            sql: "SELECT * FROM l INNER JOIN r ON TRY_CAST(l.k AS BIGINT) = r.k",
            expected_join_type: JoinType::Inner,
            left: sql_join_table("CREATE TABLE l(k INT NULL)", 4, [(
                "k",
                r#"{"min": 1, "max": 3, "ndv": 3, "null_count": 1}"#,
            )])?,
            right: sql_join_table("CREATE TABLE r(k BIGINT NULL)", 3, [(
                "k",
                r#"{"min": 1, "max": 2, "ndv": 2, "null_count": 1}"#,
            )])?,
        },
        SqlJoinStatisticsCase {
            name: "inner_lossy_cast_key",
            description: "A narrowing cast is outside the reverse-propagation whitelist, so the golden captures its fallback cardinality and per-side output statistics.",
            sql: "SELECT * FROM l INNER JOIN r ON CAST(l.k AS TINYINT) = r.k",
            expected_join_type: JoinType::Inner,
            left: sql_join_table("CREATE TABLE l(k BIGINT NULL)", 4, [(
                "k",
                r#"{"min": 1, "max": 3, "ndv": 3, "null_count": 1}"#,
            )])?,
            right: sql_join_table("CREATE TABLE r(k TINYINT NULL)", 3, [(
                "k",
                r#"{"min": 1, "max": 2, "ndv": 2, "null_count": 1}"#,
            )])?,
        },
        SqlJoinStatisticsCase {
            name: "inner_function_join_key",
            description: "A function join key is not reverse-propagated to its source, while the complete plan still shows any statistics derivable for the expression.",
            sql: "SELECT * FROM l INNER JOIN r ON COALESCE(l.k, 0) = r.k",
            expected_join_type: JoinType::Inner,
            left: sql_join_table("CREATE TABLE l(k INT NULL)", 4, [(
                "k",
                r#"{"min": 1, "max": 3, "ndv": 3, "null_count": 1}"#,
            )])?,
            right: sql_join_table("CREATE TABLE r(k INT NULL)", 3, [(
                "k",
                r#"{"min": 1, "max": 2, "ndv": 2, "null_count": 1}"#,
            )])?,
        },
        SqlJoinStatisticsCase {
            name: "inner_string_equality",
            description: "String bounds pass through the byte-statistics path and narrow both join keys to their shared range.",
            sql: "SELECT * FROM l INNER JOIN r ON l.k = r.k",
            expected_join_type: JoinType::Inner,
            left: sql_join_table("CREATE TABLE l(k VARCHAR NOT NULL)", 4, [(
                "k",
                r#"{"min": "a", "max": "d", "ndv": 4, "null_count": 0}"#,
            )])?,
            right: sql_join_table("CREATE TABLE r(k VARCHAR NOT NULL)", 3, [(
                "k",
                r#"{"min": "b", "max": "c", "ndv": 2, "null_count": 0}"#,
            )])?,
        },
        SqlJoinStatisticsCase {
            name: "inner_inaccurate_bytes_histogram_fallback",
            description: "Inaccurate byte histograms are ignored for equality cardinality, so the complete SQL join falls back to NDV and propagates no histogram.",
            sql: "SELECT * FROM l INNER JOIN r ON l.k = r.k",
            expected_join_type: JoinType::Inner,
            left: sql_join_table_with_histograms(
                "CREATE TABLE l(k VARCHAR NOT NULL)",
                100,
                [(
                    "k",
                    r#"{"min": "a", "max": "z", "ndv": 10, "null_count": 0}"#,
                )],
                [(
                    "k",
                    r#"{
                    "accuracy": false,
                    "buckets": [
                        {"lower_bound": {"Bytes": [97]}, "upper_bound": {"Bytes": [97]}, "num_values": 100.0, "num_distinct": 1.0}
                    ]
                }"#,
                )],
            )?,
            right: sql_join_table_with_histograms(
                "CREATE TABLE r(k VARCHAR NOT NULL)",
                200,
                [(
                    "k",
                    r#"{"min": "a", "max": "z", "ndv": 20, "null_count": 0}"#,
                )],
                [(
                    "k",
                    r#"{
                    "accuracy": false,
                    "buckets": [
                        {"lower_bound": {"Bytes": [97]}, "upper_bound": {"Bytes": [97]}, "num_values": 200.0, "num_distinct": 1.0}
                    ]
                }"#,
                )],
            )?,
        },
        SqlJoinStatisticsCase {
            name: "inner_date_equality",
            description: "DATE statistics retain their semantic type while using integer-backed bounds and histograms end to end.",
            sql: "SELECT * FROM l INNER JOIN r ON l.k = r.k",
            expected_join_type: JoinType::Inner,
            left: sql_join_table("CREATE TABLE l(k DATE NOT NULL)", 4, [(
                "k",
                r#"{"min": 1, "max": 3, "ndv": 3, "null_count": 0}"#,
            )])?,
            right: sql_join_table("CREATE TABLE r(k DATE NOT NULL)", 3, [(
                "k",
                r#"{"min": 1, "max": 2, "ndv": 2, "null_count": 0}"#,
            )])?,
        },
        SqlJoinStatisticsCase {
            name: "inner_timestamp_equality",
            description: "TIMESTAMP statistics retain their semantic type while using integer-backed bounds and histograms end to end.",
            sql: "SELECT * FROM l INNER JOIN r ON l.k = r.k",
            expected_join_type: JoinType::Inner,
            left: sql_join_table("CREATE TABLE l(k TIMESTAMP NOT NULL)", 4, [(
                "k",
                r#"{"min": 1, "max": 3, "ndv": 3, "null_count": 0}"#,
            )])?,
            right: sql_join_table("CREATE TABLE r(k TIMESTAMP NOT NULL)", 3, [(
                "k",
                r#"{"min": 1, "max": 2, "ndv": 2, "null_count": 0}"#,
            )])?,
        },
        SqlJoinStatisticsCase {
            name: "inner_date_timestamp_equality",
            description: "DATE and TIMESTAMP keys use comparison cardinality without treating their shared integer-backed statistics representation as the same semantic distribution.",
            sql: "SELECT * FROM l INNER JOIN r ON l.k = r.k",
            expected_join_type: JoinType::Inner,
            left: sql_join_table("CREATE TABLE l(k DATE NOT NULL)", 10, [(
                "k",
                r#"{"min": 0, "max": 10, "ndv": 10, "null_count": 0}"#,
            )])?,
            right: sql_join_table("CREATE TABLE r(k TIMESTAMP NOT NULL)", 10, [(
                "k",
                r#"{"min": 0, "max": 10, "ndv": 10, "null_count": 0}"#,
            )])?,
        },
        SqlJoinStatisticsCase {
            name: "inner_empty_left_input",
            description: "An exact-empty input produces no join pairs and does not fall back to treating either side as fully matched.",
            sql: "SELECT * FROM l INNER JOIN r ON l.k = r.k",
            expected_join_type: JoinType::Inner,
            left: sql_join_table("CREATE TABLE l(k INT NOT NULL)", 0, [])?,
            right: sql_join_table("CREATE TABLE r(k INT NOT NULL)", 10, [(
                "k",
                r#"{"min": 1, "max": 10, "ndv": 10, "null_count": 0}"#,
            )])?,
        },
        SqlJoinStatisticsCase {
            name: "inner_missing_right_column_statistics",
            description: "Missing statistics on one join input exercise the cardinality fallback without manufacturing bounds for that column.",
            sql: "SELECT * FROM l INNER JOIN r ON l.k = r.k",
            expected_join_type: JoinType::Inner,
            left: sql_join_table("CREATE TABLE l(k INT NULL)", 4, [(
                "k",
                r#"{"min": 1, "max": 3, "ndv": 3, "null_count": 1}"#,
            )])?,
            right: sql_join_table("CREATE TABLE r(k INT NULL)", 20, [])?,
        },
        SqlJoinStatisticsCase {
            name: "inner_known_and_upper_only_ndv",
            description: "One missing NDV input yields an upper-only Scan estimate and the join fallback uses the other side's known NDV.",
            sql: "SELECT * FROM l INNER JOIN r ON l.k = r.k",
            expected_join_type: JoinType::Inner,
            left: sql_join_table("CREATE TABLE l(k INT NOT NULL)", 100, [(
                "k",
                r#"{"min": 1, "max": 1000, "ndv": 10, "null_count": 0}"#,
            )])?,
            right: sql_join_table("CREATE TABLE r(k INT NOT NULL)", 200, [(
                "k",
                r#"{"min": 1, "max": 1000, "ndv": null, "null_count": 0}"#,
            )])?,
        },
        SqlJoinStatisticsCase {
            name: "inner_both_upper_only_ndv",
            description: "Two missing NDV inputs reach the conservative upper-only join fallback without test-only optimizer intervention.",
            sql: "SELECT * FROM l INNER JOIN r ON l.k = r.k",
            expected_join_type: JoinType::Inner,
            left: sql_join_table("CREATE TABLE l(k INT NOT NULL)", 100, [(
                "k",
                r#"{"min": 1, "max": 1000, "ndv": null, "null_count": 0}"#,
            )])?,
            right: sql_join_table("CREATE TABLE r(k INT NOT NULL)", 200, [(
                "k",
                r#"{"min": 1, "max": 1000, "ndv": null, "null_count": 0}"#,
            )])?,
        },
        SqlJoinStatisticsCase {
            name: "inner_all_null_equality",
            description: "Regular equality with an all-NULL key produces no matches through the scan-to-join statistics path.",
            sql: "SELECT * FROM l INNER JOIN r ON l.k = r.k",
            expected_join_type: JoinType::Inner,
            left: sql_join_table("CREATE TABLE l(k INT NULL)", 4, [(
                "k",
                r#"{"min": null, "max": null, "ndv": 0, "null_count": 4}"#,
            )])?,
            right: sql_join_table("CREATE TABLE r(k INT NULL)", 10, [(
                "k",
                r#"{"min": 1, "max": 3, "ndv": 3, "null_count": 2}"#,
            )])?,
        },
    ])
}
