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

use std::io::Write;

use databend_common_exception::Result;
use databend_common_sql::plans::JoinType;
use databend_common_sql::plans::Plan;

use super::super::super::table_statistics;
use super::super::common::collect_join_cardinalities;
use super::TableStats;
use super::column_statistics;
use super::decimal_overlap_left_stats;
use super::decimal_overlap_right_stats;
use super::histogram_statistics;
use super::write_input_stats;
use crate::framework::LiteTableContext;
use crate::framework::golden::open_golden_file;
use crate::framework::golden::write_case_title;

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_decimal_join_cardinality_golden() -> Result<()> {
    let mut file = open_golden_file("optimizer", "join_cardinality/histogram/decimal.txt")?;
    let cases = [
        (
            "decimal_same_scale_overlap",
            "Equal-scale Decimal keys use their Float-backed histograms for the complete SQL join estimate.",
            "CREATE TABLE r(k DECIMAL(10, 2), t BIGINT)",
            decimal_overlap_right_stats(),
        ),
        (
            "decimal_different_scale_overlap",
            "Different-scale Decimal keys reach the same comparison domain and retain the histogram estimate.",
            "CREATE TABLE r(k DECIMAL(12, 3), t BIGINT)",
            decimal_overlap_right_stats(),
        ),
        (
            "decimal_disjoint_bounds",
            "Disjoint Decimal bounds produce no matches through the SQL planner and typed histogram estimator.",
            "CREATE TABLE r(k DECIMAL(10, 2), t BIGINT)",
            decimal_disjoint_right_stats(),
        ),
    ];

    for (name, description, right_ddl, right) in cases {
        write_decimal_case(&mut file, name, description, right_ddl, right).await?;
    }
    Ok(())
}

async fn write_decimal_case(
    file: &mut impl Write,
    name: &str,
    description: &str,
    right_ddl: &str,
    right: TableStats,
) -> Result<()> {
    let left = decimal_overlap_left_stats();
    write_case_title(file, name, description)?;
    write_input_stats(file, "left", left)?;
    write_input_stats(file, "right", right)?;

    let ctx = LiteTableContext::create().await?;
    ctx.register_table_sql_with_stats(
        "CREATE TABLE l(k DECIMAL(10, 2), t BIGINT)",
        Some(table_statistics(left.rows)),
        column_statistics(left)?,
        histogram_statistics(left)?,
    )
    .await?;
    ctx.register_table_sql_with_stats(
        right_ddl,
        Some(table_statistics(right.rows)),
        column_statistics(right)?,
        histogram_statistics(right)?,
    )
    .await?;

    let sql = "SELECT * FROM l INNER JOIN r ON l.k = r.k";
    let plan = ctx.optimize_plan(ctx.bind_sql(sql).await?).await?;
    let Plan::Query {
        s_expr, metadata, ..
    } = plan
    else {
        unreachable!("SELECT should bind to a query plan");
    };
    let metadata = metadata.read();

    writeln!(file, "sql           : {sql}")?;
    let joins = collect_join_cardinalities(file, &metadata, &s_expr, JoinType::Inner, name)?;
    assert_eq!(joins, 1);
    writeln!(file)?;
    Ok(())
}

fn decimal_disjoint_right_stats() -> TableStats {
    TableStats {
        rows: 13,
        column_json: r#"{"min": 20.0, "max": 23.0, "ndv": 2, "null_count": 0}"#,
        histogram_json: Some(
            r#"{
            "accuracy": true,
            "buckets": [
                {"lower_bound": {"Float": 20.0}, "upper_bound": {"Float": 20.0}, "num_values": 5.0, "num_distinct": 1.0},
                {"lower_bound": {"Float": 23.0}, "upper_bound": {"Float": 23.0}, "num_values": 8.0, "num_distinct": 1.0}
            ]
        }"#,
        ),
    }
}
