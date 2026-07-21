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

use databend_common_catalog::BasicColumnStatistics;
use databend_common_catalog::TableStatistics;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::stat_distribution::NdvEstimate;
use databend_common_expression::stat_distribution::StatCount;
use databend_common_expression::types::DataType;
use databend_common_expression::types::NumberDataType;
use databend_common_sql::ColumnBindingBuilder;
use databend_common_sql::ColumnEntry;
use databend_common_sql::Metadata;
use databend_common_sql::Symbol;
use databend_common_sql::Visibility;
use databend_common_sql::optimizer::ir::ColumnStat;
use databend_common_sql::optimizer::ir::RelExpr;
use databend_common_sql::optimizer::ir::SExpr;
use databend_common_sql::optimizer::ir::StatInfo;
use databend_common_sql::optimizer::ir::Statistics;
use databend_common_sql::plans::BoundColumnRef;
use databend_common_sql::plans::Join;
use databend_common_sql::plans::JoinEquiCondition;
use databend_common_sql::plans::JoinType;
use databend_common_sql::plans::Plan;
use databend_common_sql::plans::RelOperator;
use databend_common_sql::plans::ScalarExpr;
use databend_common_statistics::Datum;
use databend_common_statistics::Histogram;
use databend_storages_common_table_meta::meta::TableSnapshotStatistics;

use crate::framework::LiteTableContext;
use crate::framework::golden::open_golden_file;
use crate::framework::golden::write_case_title;

#[derive(Clone, Copy)]
struct TableStats {
    rows: u64,
    min: i64,
    max: i64,
    ndv: u64,
    histogram_json: &'static str,
}

#[derive(Clone, Copy)]
struct JoinQueryCase {
    name: &'static str,
    sql: &'static str,
}

#[derive(Clone, Copy)]
enum JoinInput {
    Sql(JoinQueryCase),
    InternalRightSingle,
}

#[derive(Clone, Copy)]
struct JoinTestCase {
    name: &'static str,
    description: &'static str,
    expected_join_type: JoinType,
    input: JoinInput,
    left: TableStats,
    right: TableStats,
}

struct JoinBehaviorGroup {
    name: &'static str,
    description: &'static str,
    cases: Vec<JoinTestCase>,
}

fn table_statistics(rows: u64) -> TableStatistics {
    TableStatistics {
        num_rows: Some(rows),
        data_size: Some(rows.saturating_mul(8)),
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

fn column_statistics(stats: TableStats) -> HashMap<String, BasicColumnStatistics> {
    HashMap::from([("k".to_string(), BasicColumnStatistics {
        min: Some(Datum::Int(stats.min)),
        max: Some(Datum::Int(stats.max)),
        ndv: Some(stats.ndv),
        null_count: 0,
        in_memory_size: stats.rows.saturating_mul(8),
    })])
}

fn histogram_from_json(json: &str) -> Result<Histogram> {
    let statistics_json = format!(
        r#"{{
            "format_version": 4,
            "snapshot_id": "00000000-0000-0000-0000-000000000000",
            "row_count": 0,
            "hll": {{}},
            "histograms": {{"0": {json}}}
        }}"#
    );
    let mut statistics: TableSnapshotStatistics = serde_json::from_str(&statistics_json)
        .map_err(|err| ErrorCode::Internal(format!("invalid histogram json: {err}")))?;
    statistics
        .histograms
        .remove(&0)
        .ok_or_else(|| ErrorCode::Internal("invalid histogram".to_string()))
}

fn histogram_statistics(stats: TableStats) -> Result<HashMap<String, Histogram>> {
    Ok(HashMap::from([(
        "k".to_string(),
        histogram_from_json(stats.histogram_json)?,
    )]))
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

fn histogram_summary(histogram: &Histogram) -> String {
    let buckets = histogram
        .bucket_iter()
        .map(|bucket| {
            format!(
                "{}..{}:{:.3}/{:.3}",
                bucket.lower_bound(),
                bucket.upper_bound(),
                bucket.num_values(),
                bucket.num_distinct()
            )
        })
        .collect::<Vec<_>>()
        .join(", ");
    format!(
        "rows={:.3}, ndv={:.3}, buckets=[{}]",
        histogram.num_values(),
        histogram.ndv().expected.unwrap_or(histogram.ndv().upper),
        buckets
    )
}

fn write_join_stat_info(
    file: &mut impl Write,
    metadata: &Metadata,
    stat_info: &StatInfo,
) -> Result<()> {
    let mut column_stats = stat_info.statistics.column_stats.iter().collect::<Vec<_>>();
    column_stats.sort_by_key(|(column, _)| **column);

    for (column, stat) in column_stats {
        let histogram = stat
            .histogram
            .as_ref()
            .map(histogram_summary)
            .unwrap_or_else(|| "none".to_string());
        writeln!(
            file,
            "stat          : {} min={}, max={}, ndv={:.3}, null={:.3}, histogram={}",
            column_label(metadata, *column),
            stat.min,
            stat.max,
            stat.ndv.expected.unwrap_or(stat.ndv.upper),
            stat.null_count.expected(),
            histogram
        )?;
    }

    Ok(())
}

fn write_direct_join_stat_info(file: &mut impl Write, stat_info: &StatInfo) -> Result<()> {
    let mut column_stats = stat_info.statistics.column_stats.iter().collect::<Vec<_>>();
    column_stats.sort_by_key(|(column, _)| **column);

    for (column, stat) in column_stats {
        let histogram = stat
            .histogram
            .as_ref()
            .map(histogram_summary)
            .unwrap_or_else(|| "none".to_string());
        let label = match column.as_usize() {
            0 => "left.k (#0)".to_string(),
            1 => "right.k (#1)".to_string(),
            id => format!("column #{id}"),
        };
        writeln!(
            file,
            "stat          : {} min={}, max={}, ndv={:.3}, null={:.3}, histogram={}",
            label,
            stat.min,
            stat.max,
            stat.ndv.expected.unwrap_or(stat.ndv.upper),
            stat.null_count.expected(),
            histogram
        )?;
    }

    Ok(())
}

fn collect_join_cardinalities(
    file: &mut impl Write,
    metadata: &Metadata,
    expr: &SExpr,
    expected_join_type: JoinType,
) -> Result<usize> {
    let mut joins = 0;
    if let RelOperator::Join(join) = expr.plan() {
        assert_eq!(join.join_type, expected_join_type);
        let stat_info = RelExpr::with_s_expr(expr).derive_cardinality()?;
        writeln!(
            file,
            "join          : {:<11} cardinality={:.3}",
            join.join_type, stat_info.cardinality
        )?;
        write_join_stat_info(file, metadata, &stat_info)?;
        joins += 1;
    }

    for child in expr.children() {
        joins += collect_join_cardinalities(file, metadata, child, expected_join_type)?;
    }

    Ok(joins)
}

fn direct_column(column: usize, table: &str) -> ScalarExpr {
    BoundColumnRef {
        span: None,
        column: ColumnBindingBuilder::new(
            "k".to_string(),
            Symbol::new(column),
            Box::new(DataType::Number(NumberDataType::Int64)),
            Visibility::Visible,
        )
        .table_name(Some(table.to_string()))
        .build(),
    }
    .into()
}

fn direct_stat_info(column: usize, stats: TableStats) -> Result<Arc<StatInfo>> {
    Ok(Arc::new(StatInfo {
        cardinality: stats.rows as f64,
        statistics: Statistics {
            precise_cardinality: None,
            column_stats: HashMap::from([(Symbol::new(column), ColumnStat {
                min: Datum::Int(stats.min),
                max: Datum::Int(stats.max),
                ndv: NdvEstimate::exact(stats.ndv as f64),
                null_count: StatCount::exact(0),
                histogram: Some(histogram_from_json(stats.histogram_json)?),
            })]),
            top_n: Default::default(),
            count_min_sketch: Default::default(),
        },
    }))
}

fn write_internal_right_single_case(file: &mut impl Write, case: &JoinTestCase) -> Result<()> {
    // RightSingle has no stable SQL spelling; the optimizer synthesizes it as
    // the opposite of LeftSingle.
    let join_type = JoinType::RightSingle;
    let join = Join {
        equi_conditions: vec![JoinEquiCondition::new(
            direct_column(0, "left"),
            direct_column(1, "right"),
            false,
        )],
        join_type,
        ..Default::default()
    };
    let stat_info = join.derive_join_stats(
        direct_stat_info(0, case.left)?,
        direct_stat_info(1, case.right)?,
    )?;

    writeln!(file, "query         : internal_right_single")?;
    writeln!(
        file,
        "input         : optimizer-internal RightSingle branch"
    )?;
    writeln!(
        file,
        "join          : {:<11} cardinality={:.3}",
        join_type, stat_info.cardinality
    )?;
    write_direct_join_stat_info(file, &stat_info)
}

async fn write_sql_join_input(
    file: &mut impl Write,
    case: &JoinTestCase,
    query: JoinQueryCase,
    expected_join_type: JoinType,
) -> Result<()> {
    let ctx = LiteTableContext::create().await?;
    ctx.register_table_sql_with_stats(
        "CREATE TABLE l(k BIGINT, t BIGINT)",
        Some(table_statistics(case.left.rows)),
        column_statistics(case.left),
        histogram_statistics(case.left)?,
    )
    .await?;
    ctx.register_table_sql_with_stats(
        "CREATE TABLE r(k BIGINT, t BIGINT)",
        Some(table_statistics(case.right.rows)),
        column_statistics(case.right),
        histogram_statistics(case.right)?,
    )
    .await?;

    let plan = ctx.optimize_plan(ctx.bind_sql(query.sql).await?).await?;
    let Plan::Query {
        s_expr, metadata, ..
    } = plan
    else {
        unreachable!("SELECT should bind to a query plan");
    };
    let metadata = metadata.read();

    writeln!(file, "query         : {}", query.name)?;
    writeln!(file, "sql           : {}", query.sql)?;
    let joins = collect_join_cardinalities(file, &metadata, &s_expr, expected_join_type)?;
    assert_eq!(joins, 1);
    Ok(())
}

fn write_stats_case_header(file: &mut impl Write, case: &JoinTestCase) -> Result<()> {
    writeln!(file, "case          : {}", case.name)?;
    writeln!(file, "description   : {}", case.description)?;
    writeln!(
        file,
        "left stats    : rows={}, min={}, max={}, ndv={}",
        case.left.rows, case.left.min, case.left.max, case.left.ndv
    )?;
    writeln!(
        file,
        "right stats   : rows={}, min={}, max={}, ndv={}",
        case.right.rows, case.right.min, case.right.max, case.right.ndv
    )?;
    Ok(())
}

async fn write_join_behavior_group(file: &mut impl Write, group: &JoinBehaviorGroup) -> Result<()> {
    write_case_title(file, group.name, group.description)?;
    writeln!(file)?;

    for case in &group.cases {
        write_stats_case_header(file, case)?;
        match case.input {
            JoinInput::Sql(query) => {
                write_sql_join_input(file, case, query, case.expected_join_type).await?
            }
            JoinInput::InternalRightSingle => {
                assert_eq!(case.expected_join_type, JoinType::RightSingle);
                write_internal_right_single_case(file, case)?;
            }
        }
        writeln!(file)?;
    }
    writeln!(file)?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_join_cardinality_estimation_golden() -> Result<()> {
    let mut file = open_golden_file("optimizer", "join_cardinality.txt")?;

    for group in join_behavior_groups() {
        write_join_behavior_group(&mut file, &group).await?;
    }

    Ok(())
}

fn overlap_left_stats() -> TableStats {
    TableStats {
        rows: 9,
        min: 1,
        max: 5,
        ndv: 3,
        histogram_json: r#"{
            "accuracy": true,
            "buckets": [
                {"lower_bound": {"Int": 1}, "upper_bound": {"Int": 1}, "num_values": 2.0, "num_distinct": 1.0},
                {"lower_bound": {"Int": 3}, "upper_bound": {"Int": 3}, "num_values": 4.0, "num_distinct": 1.0},
                {"lower_bound": {"Int": 5}, "upper_bound": {"Int": 5}, "num_values": 3.0, "num_distinct": 1.0}
            ]
        }"#,
    }
}

fn overlap_right_stats() -> TableStats {
    TableStats {
        rows: 26,
        min: 1,
        max: 5,
        ndv: 4,
        histogram_json: r#"{
            "accuracy": true,
            "buckets": [
                {"lower_bound": {"Int": 1}, "upper_bound": {"Int": 1}, "num_values": 5.0, "num_distinct": 1.0},
                {"lower_bound": {"Int": 2}, "upper_bound": {"Int": 2}, "num_values": 7.0, "num_distinct": 1.0},
                {"lower_bound": {"Int": 3}, "upper_bound": {"Int": 3}, "num_values": 6.0, "num_distinct": 1.0},
                {"lower_bound": {"Int": 5}, "upper_bound": {"Int": 5}, "num_values": 8.0, "num_distinct": 1.0}
            ]
        }"#,
    }
}

fn no_overlap_right_stats() -> TableStats {
    TableStats {
        rows: 26,
        min: 20,
        max: 23,
        ndv: 4,
        histogram_json: r#"{
            "accuracy": true,
            "buckets": [
                {"lower_bound": {"Int": 20}, "upper_bound": {"Int": 20}, "num_values": 5.0, "num_distinct": 1.0},
                {"lower_bound": {"Int": 21}, "upper_bound": {"Int": 21}, "num_values": 7.0, "num_distinct": 1.0},
                {"lower_bound": {"Int": 22}, "upper_bound": {"Int": 22}, "num_values": 6.0, "num_distinct": 1.0},
                {"lower_bound": {"Int": 23}, "upper_bound": {"Int": 23}, "num_values": 8.0, "num_distinct": 1.0}
            ]
        }"#,
    }
}

fn partial_overlap_right_stats() -> TableStats {
    TableStats {
        rows: 30,
        min: 3,
        max: 9,
        ndv: 4,
        histogram_json: r#"{
            "accuracy": true,
            "buckets": [
                {"lower_bound": {"Int": 3}, "upper_bound": {"Int": 3}, "num_values": 6.0, "num_distinct": 1.0},
                {"lower_bound": {"Int": 5}, "upper_bound": {"Int": 5}, "num_values": 8.0, "num_distinct": 1.0},
                {"lower_bound": {"Int": 8}, "upper_bound": {"Int": 8}, "num_values": 7.0, "num_distinct": 1.0},
                {"lower_bound": {"Int": 9}, "upper_bound": {"Int": 9}, "num_values": 9.0, "num_distinct": 1.0}
            ]
        }"#,
    }
}

fn sql_input(name: &'static str, sql: &'static str) -> JoinInput {
    JoinInput::Sql(JoinQueryCase { name, sql })
}

fn overlap_case(
    name: &'static str,
    expected_join_type: JoinType,
    input: JoinInput,
) -> JoinTestCase {
    JoinTestCase {
        name,
        description: "Join-key histograms overlap, so the inner estimate has non-zero matches when this join type uses it.",
        expected_join_type,
        input,
        left: overlap_left_stats(),
        right: overlap_right_stats(),
    }
}

fn no_overlap_case(
    name: &'static str,
    expected_join_type: JoinType,
    input: JoinInput,
) -> JoinTestCase {
    JoinTestCase {
        name,
        description: "Join-key histograms do not overlap, so the inner estimate is zero before join-type rules apply.",
        expected_join_type,
        input,
        left: overlap_left_stats(),
        right: no_overlap_right_stats(),
    }
}

fn partial_overlap_case(
    name: &'static str,
    expected_join_type: JoinType,
    input: JoinInput,
) -> JoinTestCase {
    JoinTestCase {
        name,
        description: "Join-key histograms partially overlap, so estimated join-key stats should narrow bounds when this join type keeps them.",
        expected_join_type,
        input,
        left: overlap_left_stats(),
        right: partial_overlap_right_stats(),
    }
}

fn join_behavior_groups() -> Vec<JoinBehaviorGroup> {
    vec![
        JoinBehaviorGroup {
            name: "inner_like_cardinality_and_join_key_stats",
            description: "INNER-like joins use the inner join estimate as final cardinality. INNER rebuilds join-key histograms, while INNER ANY and ASOF keep estimated min/max/NDV but drop histograms.",
            cases: vec![
                no_overlap_case(
                    "cross_join_no_overlap",
                    JoinType::Cross,
                    sql_input("cross_join", "SELECT * FROM l CROSS JOIN r"),
                ),
                overlap_case(
                    "inner_join_overlap",
                    JoinType::Inner,
                    sql_input("inner_join", "SELECT * FROM l INNER JOIN r ON l.k = r.k"),
                ),
                no_overlap_case(
                    "inner_join_no_overlap",
                    JoinType::Inner,
                    sql_input("inner_join", "SELECT * FROM l INNER JOIN r ON l.k = r.k"),
                ),
                overlap_case(
                    "inner_any_join_overlap",
                    JoinType::InnerAny,
                    sql_input(
                        "inner_any_join",
                        "SELECT * FROM l INNER ANY JOIN r ON l.k = r.k",
                    ),
                ),
                overlap_case(
                    "asof_join_overlap",
                    JoinType::Asof,
                    sql_input(
                        "asof_join",
                        "SELECT * FROM l ASOF JOIN r ON l.k = r.k AND l.t >= r.t",
                    ),
                ),
            ],
        },
        JoinBehaviorGroup {
            name: "left_preserving_cardinality_and_nullable_stats",
            description: "LEFT-family joins use max(internal left rows, inner estimate). They estimate nullable-side join-key min/max/NDV from the inner match and drop that side's histogram.",
            cases: vec![
                no_overlap_case(
                    "left_join_no_overlap",
                    JoinType::Left,
                    sql_input("right_join", "SELECT * FROM l RIGHT JOIN r ON l.k = r.k"),
                ),
                overlap_case(
                    "left_any_join_overlap",
                    JoinType::LeftAny,
                    sql_input(
                        "left_any_join",
                        "SELECT * FROM l LEFT ANY JOIN r ON l.k = r.k",
                    ),
                ),
                partial_overlap_case(
                    "left_asof_join_partial_overlap",
                    JoinType::LeftAsof,
                    sql_input(
                        "asof_left_join",
                        "SELECT * FROM l ASOF LEFT JOIN r ON l.k = r.k AND l.t >= r.t",
                    ),
                ),
                no_overlap_case(
                    "left_asof_join_no_overlap",
                    JoinType::LeftAsof,
                    sql_input(
                        "asof_left_join",
                        "SELECT * FROM l ASOF LEFT JOIN r ON l.k = r.k AND l.t >= r.t",
                    ),
                ),
            ],
        },
        JoinBehaviorGroup {
            name: "right_preserving_cardinality_and_nullable_stats",
            description: "RIGHT-family joins use max(internal right rows, inner estimate). They estimate nullable-side join-key min/max/NDV from the inner match and drop that side's histogram.",
            cases: vec![
                no_overlap_case(
                    "right_join_no_overlap",
                    JoinType::Right,
                    sql_input("left_join", "SELECT * FROM l LEFT JOIN r ON l.k = r.k"),
                ),
                overlap_case(
                    "right_any_join_overlap",
                    JoinType::RightAny,
                    sql_input(
                        "right_any_join",
                        "SELECT * FROM l RIGHT ANY JOIN r ON l.k = r.k",
                    ),
                ),
                partial_overlap_case(
                    "right_asof_join_partial_overlap",
                    JoinType::RightAsof,
                    sql_input(
                        "asof_right_join",
                        "SELECT * FROM l ASOF RIGHT JOIN r ON l.k = r.k AND l.t >= r.t",
                    ),
                ),
                no_overlap_case(
                    "right_asof_join_no_overlap",
                    JoinType::RightAsof,
                    sql_input(
                        "asof_right_join",
                        "SELECT * FROM l ASOF RIGHT JOIN r ON l.k = r.k AND l.t >= r.t",
                    ),
                ),
            ],
        },
        JoinBehaviorGroup {
            name: "full_preserving_cardinality_without_nullable_rewrite",
            description: "FULL-family joins combine both preserved sides with the inner estimate. They do not keep estimated join-key stats, so histograms are only dropped when the inner estimator touched the join keys.",
            cases: vec![
                overlap_case(
                    "full_join_overlap",
                    JoinType::Full,
                    sql_input("full_join", "SELECT * FROM l FULL JOIN r ON l.k = r.k"),
                ),
                no_overlap_case(
                    "full_join_no_overlap",
                    JoinType::Full,
                    sql_input("full_join", "SELECT * FROM l FULL JOIN r ON l.k = r.k"),
                ),
                overlap_case(
                    "full_asof_join_overlap",
                    JoinType::FullAsof,
                    sql_input(
                        "asof_full_join",
                        "SELECT * FROM l ASOF FULL JOIN r ON l.k = r.k AND l.t >= r.t",
                    ),
                ),
                no_overlap_case(
                    "full_asof_join_no_overlap",
                    JoinType::FullAsof,
                    sql_input(
                        "asof_full_join",
                        "SELECT * FROM l ASOF FULL JOIN r ON l.k = r.k AND l.t >= r.t",
                    ),
                ),
            ],
        },
        JoinBehaviorGroup {
            name: "semi_cardinality_and_histogram_finish",
            description: "SEMI joins use estimated join-key stats, cap final cardinality with the preserved side, keep that side's semi histogram, and drop the other side's histogram.",
            cases: vec![
                overlap_case(
                    "left_semi_join_overlap",
                    JoinType::LeftSemi,
                    sql_input(
                        "right_semi_join",
                        "SELECT * FROM l RIGHT SEMI JOIN r ON l.k = r.k",
                    ),
                ),
                no_overlap_case(
                    "left_semi_join_no_overlap",
                    JoinType::LeftSemi,
                    sql_input(
                        "right_semi_join",
                        "SELECT * FROM l RIGHT SEMI JOIN r ON l.k = r.k",
                    ),
                ),
                overlap_case(
                    "right_semi_join_overlap",
                    JoinType::RightSemi,
                    sql_input(
                        "left_semi_join",
                        "SELECT * FROM l LEFT SEMI JOIN r ON l.k = r.k",
                    ),
                ),
                no_overlap_case(
                    "exists_no_overlap",
                    JoinType::RightSemi,
                    sql_input(
                        "exists",
                        "SELECT * FROM l WHERE EXISTS (SELECT 1 FROM r WHERE l.k = r.k)",
                    ),
                ),
            ],
        },
        JoinBehaviorGroup {
            name: "fixed_left_cardinality",
            description: "LEFT-fixed joins return the internal left input cardinality regardless of the inner estimate. LeftSingle still rewrites the nullable-side join-key stats from the inner estimate.",
            cases: vec![
                overlap_case(
                    "left_single_from_scalar_overlap",
                    JoinType::LeftSingle,
                    sql_input(
                        "left_single_from_scalar_projection",
                        "SELECT (SELECT l.k FROM l WHERE l.k = r.k) FROM r",
                    ),
                ),
                overlap_case(
                    "right_mark_from_any_overlap",
                    JoinType::RightMark,
                    sql_input(
                        "right_mark_from_any_projection",
                        "SELECT r.k = ANY (SELECT l.k FROM l) FROM r",
                    ),
                ),
                overlap_case(
                    "left_anti_join_overlap",
                    JoinType::LeftAnti,
                    sql_input(
                        "right_anti_join",
                        "SELECT * FROM l RIGHT ANTI JOIN r ON l.k = r.k",
                    ),
                ),
            ],
        },
        JoinBehaviorGroup {
            name: "fixed_right_cardinality",
            description: "RIGHT-fixed joins return the internal right input cardinality regardless of the inner estimate. RightSingle still rewrites the nullable-side join-key stats from the inner estimate.",
            cases: vec![
                partial_overlap_case(
                    "right_single_internal_partial_overlap",
                    JoinType::RightSingle,
                    JoinInput::InternalRightSingle,
                ),
                overlap_case(
                    "left_mark_from_any_overlap",
                    JoinType::LeftMark,
                    sql_input(
                        "left_mark_from_any_projection",
                        "SELECT l.k = ANY (SELECT r.k FROM r) FROM l",
                    ),
                ),
                overlap_case(
                    "right_anti_join_overlap",
                    JoinType::RightAnti,
                    sql_input(
                        "left_anti_join",
                        "SELECT * FROM l LEFT ANTI JOIN r ON l.k = r.k",
                    ),
                ),
            ],
        },
    ]
}
