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
            // With the derived is_not_null(r.k) filter, r's estimate drops to
            // match l's (both 4 rows), so the join keeps its original
            // orientation instead of flipping sides.
            expected_join_type: JoinType::Left,
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
            name: "inner_three_key_strictest_nonzero",
            description: "Three non-zero equality estimates select the strictest key while a later looser key does not replace the winner histogram.",
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
