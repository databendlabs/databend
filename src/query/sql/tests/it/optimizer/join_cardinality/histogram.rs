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

mod anti;
mod asof;
mod decimal;
mod inner;
mod mark;
mod outer;
mod semi;
mod single;

#[derive(Clone, Copy)]
pub(super) struct TableStats {
    pub(super) rows: u64,
    pub(super) column_json: &'static str,
    pub(super) histogram_json: Option<&'static str>,
}

impl TableStats {
    fn column_stat(self) -> Result<BasicColumnStatistics> {
        column_stat(self.column_json)
    }

    fn histogram(self) -> Result<Option<Histogram>> {
        self.histogram_json.map(histogram_stat).transpose()
    }

    pub(super) fn without_histogram_input(self) -> Self {
        Self {
            histogram_json: None,
            ..self
        }
    }
}

#[derive(Clone, Copy)]
pub(super) struct JoinQueryCase {
    pub(super) name: &'static str,
    pub(super) sql: &'static str,
}

#[derive(Clone, Copy)]
pub(super) struct JoinTestCase {
    pub(super) name: &'static str,
    pub(super) description: &'static str,
    pub(super) expected_join_type: JoinType,
    pub(super) input: JoinQueryCase,
    pub(super) left: TableStats,
    pub(super) right: TableStats,
}

fn column_statistics(stats: TableStats) -> Result<HashMap<String, BasicColumnStatistics>> {
    Ok(HashMap::from([("k".to_string(), stats.column_stat()?)]))
}

fn histogram_statistics(stats: TableStats) -> Result<HashMap<String, Histogram>> {
    Ok(stats
        .histogram()?
        .map(|histogram| HashMap::from([("k".to_string(), histogram)]))
        .unwrap_or_default())
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
        column_statistics(case.left)?,
        histogram_statistics(case.left)?,
    )
    .await?;
    ctx.register_table_sql_with_stats(
        "CREATE TABLE r(k BIGINT, t BIGINT)",
        Some(table_statistics(case.right.rows)),
        column_statistics(case.right)?,
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
    let joins =
        collect_join_cardinalities(file, &metadata, &s_expr, expected_join_type, case.name)?;
    assert_eq!(joins, 1);
    Ok(())
}

fn write_stats_case_header(file: &mut impl Write, case: &JoinTestCase) -> Result<()> {
    writeln!(file, "case          : {}", case.name)?;
    writeln!(file, "description   : {}", case.description)?;
    write_input_stats(file, "left", case.left)?;
    write_input_stats(file, "right", case.right)?;
    Ok(())
}

fn write_input_stats(file: &mut impl Write, side: &str, stats: TableStats) -> Result<()> {
    let column_stat = stats.column_stat()?;
    let min = column_stat
        .min
        .ok_or_else(|| ErrorCode::Internal(format!("{side} column statistics require min")))?;
    let max = column_stat
        .max
        .ok_or_else(|| ErrorCode::Internal(format!("{side} column statistics require max")))?;
    let ndv = column_stat
        .ndv
        .ok_or_else(|| ErrorCode::Internal(format!("{side} column statistics require ndv")))?;
    let label = format!("{side} stats");
    writeln!(
        file,
        "{label:<14}: rows={}, min={}, max={}, ndv={}, histogram_input={}",
        stats.rows,
        min,
        max,
        ndv,
        if stats.histogram_json.is_some() {
            "present"
        } else {
            "missing"
        }
    )?;
    Ok(())
}

pub(super) async fn run_join_cases(
    file_name: &str,
    group_name: &str,
    group_description: &str,
    cases: Vec<JoinTestCase>,
) -> Result<()> {
    let mut file = open_golden_file(
        "optimizer",
        &format!("join_cardinality/histogram/{file_name}"),
    )?;
    write_case_title(&mut file, group_name, group_description)?;
    writeln!(file)?;

    for case in &cases {
        write_stats_case_header(&mut file, case)?;
        write_sql_join_input(&mut file, case, case.input, case.expected_join_type).await?;
        writeln!(&mut file)?;
    }
    writeln!(&mut file)?;
    Ok(())
}

pub(super) fn decimal_overlap_left_stats() -> TableStats {
    TableStats {
        rows: 9,
        column_json: r#"{"min": 1.25, "max": 4.75, "ndv": 3, "null_count": 0}"#,
        histogram_json: Some(
            r#"{
            "accuracy": true,
            "buckets": [
                {"lower_bound": {"Float": 1.25}, "upper_bound": {"Float": 1.25}, "num_values": 2.0, "num_distinct": 1.0},
                {"lower_bound": {"Float": 2.5}, "upper_bound": {"Float": 2.5}, "num_values": 4.0, "num_distinct": 1.0},
                {"lower_bound": {"Float": 4.75}, "upper_bound": {"Float": 4.75}, "num_values": 3.0, "num_distinct": 1.0}
            ]
        }"#,
        ),
    }
}

pub(super) fn decimal_overlap_right_stats() -> TableStats {
    TableStats {
        rows: 26,
        column_json: r#"{"min": 1.25, "max": 4.75, "ndv": 4, "null_count": 0}"#,
        histogram_json: Some(
            r#"{
            "accuracy": true,
            "buckets": [
                {"lower_bound": {"Float": 1.25}, "upper_bound": {"Float": 1.25}, "num_values": 5.0, "num_distinct": 1.0},
                {"lower_bound": {"Float": 2.0}, "upper_bound": {"Float": 2.0}, "num_values": 7.0, "num_distinct": 1.0},
                {"lower_bound": {"Float": 2.5}, "upper_bound": {"Float": 2.5}, "num_values": 6.0, "num_distinct": 1.0},
                {"lower_bound": {"Float": 4.75}, "upper_bound": {"Float": 4.75}, "num_values": 8.0, "num_distinct": 1.0}
            ]
        }"#,
        ),
    }
}

pub(super) fn overlap_left_stats() -> TableStats {
    TableStats {
        rows: 9,
        column_json: r#"{"min": 1, "max": 5, "ndv": 3, "null_count": 0}"#,
        histogram_json: Some(
            r#"{
            "accuracy": true,
            "buckets": [
                {"lower_bound": {"Int": 1}, "upper_bound": {"Int": 1}, "num_values": 2.0, "num_distinct": 1.0},
                {"lower_bound": {"Int": 3}, "upper_bound": {"Int": 3}, "num_values": 4.0, "num_distinct": 1.0},
                {"lower_bound": {"Int": 5}, "upper_bound": {"Int": 5}, "num_values": 3.0, "num_distinct": 1.0}
            ]
        }"#,
        ),
    }
}

pub(super) fn overlap_right_stats() -> TableStats {
    TableStats {
        rows: 26,
        column_json: r#"{"min": 1, "max": 5, "ndv": 4, "null_count": 0}"#,
        histogram_json: Some(
            r#"{
            "accuracy": true,
            "buckets": [
                {"lower_bound": {"Int": 1}, "upper_bound": {"Int": 1}, "num_values": 5.0, "num_distinct": 1.0},
                {"lower_bound": {"Int": 2}, "upper_bound": {"Int": 2}, "num_values": 7.0, "num_distinct": 1.0},
                {"lower_bound": {"Int": 3}, "upper_bound": {"Int": 3}, "num_values": 6.0, "num_distinct": 1.0},
                {"lower_bound": {"Int": 5}, "upper_bound": {"Int": 5}, "num_values": 8.0, "num_distinct": 1.0}
            ]
        }"#,
        ),
    }
}

pub(super) fn no_overlap_right_stats() -> TableStats {
    TableStats {
        rows: 26,
        column_json: r#"{"min": 20, "max": 23, "ndv": 4, "null_count": 0}"#,
        histogram_json: Some(
            r#"{
            "accuracy": true,
            "buckets": [
                {"lower_bound": {"Int": 20}, "upper_bound": {"Int": 20}, "num_values": 5.0, "num_distinct": 1.0},
                {"lower_bound": {"Int": 21}, "upper_bound": {"Int": 21}, "num_values": 7.0, "num_distinct": 1.0},
                {"lower_bound": {"Int": 22}, "upper_bound": {"Int": 22}, "num_values": 6.0, "num_distinct": 1.0},
                {"lower_bound": {"Int": 23}, "upper_bound": {"Int": 23}, "num_values": 8.0, "num_distinct": 1.0}
            ]
        }"#,
        ),
    }
}

pub(super) fn partial_overlap_right_stats() -> TableStats {
    TableStats {
        rows: 30,
        column_json: r#"{"min": 3, "max": 9, "ndv": 4, "null_count": 0}"#,
        histogram_json: Some(
            r#"{
            "accuracy": true,
            "buckets": [
                {"lower_bound": {"Int": 3}, "upper_bound": {"Int": 3}, "num_values": 6.0, "num_distinct": 1.0},
                {"lower_bound": {"Int": 5}, "upper_bound": {"Int": 5}, "num_values": 8.0, "num_distinct": 1.0},
                {"lower_bound": {"Int": 8}, "upper_bound": {"Int": 8}, "num_values": 7.0, "num_distinct": 1.0},
                {"lower_bound": {"Int": 9}, "upper_bound": {"Int": 9}, "num_values": 9.0, "num_distinct": 1.0}
            ]
        }"#,
        ),
    }
}

pub(super) fn large_dense_partial_overlap_left_stats() -> TableStats {
    TableStats {
        rows: 1_000_000,
        column_json: r#"{"min": 0, "max": 9999, "ndv": 10000, "null_count": 0}"#,
        histogram_json: None,
    }
}

pub(super) fn large_dense_partial_overlap_right_stats() -> TableStats {
    TableStats {
        rows: 1_000_000,
        column_json: r#"{"min": 5000, "max": 14999, "ndv": 10000, "null_count": 0}"#,
        histogram_json: None,
    }
}

pub(super) fn large_dense_partial_overlap_case(
    name: &'static str,
    description: &'static str,
    expected_join_type: JoinType,
    input: JoinQueryCase,
) -> JoinTestCase {
    JoinTestCase {
        name,
        description,
        expected_join_type,
        input,
        left: large_dense_partial_overlap_left_stats(),
        right: large_dense_partial_overlap_right_stats(),
    }
}

pub(super) fn full_mixed_right_stats() -> TableStats {
    TableStats {
        rows: 26,
        column_json: r#"{"min": 1, "max": 5, "ndv": 4, "null_count": 0}"#,
        histogram_json: Some(
            r#"{
            "accuracy": true,
            "buckets": [
                {"lower_bound": {"Int": 1}, "upper_bound": {"Int": 1}, "num_values": 1.0, "num_distinct": 1.0},
                {"lower_bound": {"Int": 2}, "upper_bound": {"Int": 2}, "num_values": 22.0, "num_distinct": 1.0},
                {"lower_bound": {"Int": 3}, "upper_bound": {"Int": 3}, "num_values": 1.0, "num_distinct": 1.0},
                {"lower_bound": {"Int": 5}, "upper_bound": {"Int": 5}, "num_values": 2.0, "num_distinct": 1.0}
            ]
        }"#,
        ),
    }
}

pub(super) fn selective_semi_right_stats() -> TableStats {
    TableStats {
        rows: 1614,
        column_json: r#"{"min": 3, "max": 9, "ndv": 4, "null_count": 0}"#,
        histogram_json: Some(
            r#"{
            "accuracy": true,
            "buckets": [
                {"lower_bound": {"Int": 3}, "upper_bound": {"Int": 3}, "num_values": 6.0, "num_distinct": 1.0},
                {"lower_bound": {"Int": 5}, "upper_bound": {"Int": 5}, "num_values": 8.0, "num_distinct": 1.0},
                {"lower_bound": {"Int": 8}, "upper_bound": {"Int": 8}, "num_values": 700.0, "num_distinct": 1.0},
                {"lower_bound": {"Int": 9}, "upper_bound": {"Int": 9}, "num_values": 900.0, "num_distinct": 1.0}
            ]
        }"#,
        ),
    }
}

pub(super) fn sql_input(name: &'static str, sql: &'static str) -> JoinQueryCase {
    JoinQueryCase { name, sql }
}

pub(super) fn overlap_case(
    name: &'static str,
    expected_join_type: JoinType,
    input: JoinQueryCase,
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

pub(super) fn no_overlap_case(
    name: &'static str,
    expected_join_type: JoinType,
    input: JoinQueryCase,
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

pub(super) fn partial_overlap_case(
    name: &'static str,
    expected_join_type: JoinType,
    input: JoinQueryCase,
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
