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

use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_sql::optimizer::ir::RelExpr;
use databend_common_sql::optimizer::ir::SExpr;
use databend_common_sql::optimizer::optimizers::rule::Rule;
use databend_common_sql::optimizer::optimizers::rule::RuleCommuteJoin;
use databend_common_sql::optimizer::optimizers::rule::TransformResult;
use databend_common_sql::plans::JoinType;
use databend_common_sql::plans::Plan;
use databend_common_sql::plans::RelOperator;

use super::super::super::table_statistics;
use super::super::common::collect_join_cardinalities;
use super::JoinTestCase;
use super::TableStats;
use super::column_statistics;
use super::histogram_statistics;
use super::overlap_left_stats;
use super::overlap_right_stats;
use super::sql_input;
use super::write_sql_join_input;
use super::write_stats_case_header;
use crate::framework::LiteTableContext;
use crate::framework::golden::open_golden_file;
use crate::framework::golden::write_case_title;

fn large_overlap_stats() -> TableStats {
    TableStats {
        rows: 1000,
        column_json: r#"{"min": 1, "max": 5, "ndv": 3, "null_count": 0}"#,
        histogram_json: Some(
            r#"{
            "accuracy": true,
            "buckets": [
                {"lower_bound": {"Int": 1}, "upper_bound": {"Int": 1}, "num_values": 200.0, "num_distinct": 1.0},
                {"lower_bound": {"Int": 3}, "upper_bound": {"Int": 3}, "num_values": 400.0, "num_distinct": 1.0},
                {"lower_bound": {"Int": 5}, "upper_bound": {"Int": 5}, "num_values": 400.0, "num_distinct": 1.0}
            ]
        }"#,
        ),
    }
}

fn find_join(expr: &SExpr, join_type: JoinType) -> Option<&SExpr> {
    if matches!(expr.plan(), RelOperator::Join(join) if join.join_type == join_type) {
        return Some(expr);
    }
    expr.children()
        .find_map(|child| find_join(child, join_type))
}

async fn write_optimizer_commuted_right_single(
    file: &mut impl Write,
    case: &JoinTestCase,
) -> Result<()> {
    write_stats_case_header(file, case)?;

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

    let Plan::Query {
        s_expr, metadata, ..
    } = ctx
        .optimize_plan(ctx.bind_sql(case.input.sql).await?)
        .await?
    else {
        return Err(ErrorCode::Internal("SELECT should bind to a query plan"));
    };
    let mut state = TransformResult::new();
    let (right_single, optimizer) = match find_join(&s_expr, JoinType::RightSingle) {
        Some(right_single) => (right_single, "full optimizer"),
        None => {
            let left_single = find_join(&s_expr, JoinType::LeftSingle)
                .ok_or_else(|| ErrorCode::Internal("optimizer did not derive SINGLE from SQL"))?;
            RuleCommuteJoin::new().apply(left_single, &mut state)?;
            let left_cardinality = RelExpr::with_s_expr(left_single.child(0)?)
                .derive_cardinality()?
                .cardinality;
            let right_cardinality = RelExpr::with_s_expr(left_single.child(1)?)
                .derive_cardinality()?
                .cardinality;
            let right_single = state
                .results()
                .iter()
                .find(|expr| {
                    matches!(expr.plan(), RelOperator::Join(join) if join.join_type == JoinType::RightSingle)
                })
                .ok_or_else(|| ErrorCode::Internal(format!(
                    "join commute rule did not derive RIGHT SINGLE: left={left_cardinality}, right={right_cardinality}"
                )))?;
            (right_single, "CommuteJoin")
        }
    };

    writeln!(file, "query         : {}", case.input.name)?;
    writeln!(file, "sql           : {}", case.input.sql)?;
    writeln!(file, "optimizer     : {optimizer}")?;
    let joins = collect_join_cardinalities(
        file,
        &metadata.read(),
        right_single,
        JoinType::RightSingle,
        case.name,
    )?;
    assert_eq!(joins, 1);
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_single_join_cardinality_golden() -> Result<()> {
    let mut file = open_golden_file("optimizer", "join_cardinality/histogram/single.txt")?;
    write_case_title(
        &mut file,
        "single_join_cardinality",
        "SINGLE joins are derived from scalar subqueries; input cardinalities exercise both the original and optimizer-commuted orientations.",
    )?;
    writeln!(&mut file)?;

    let left_single = JoinTestCase {
        name: "left_single_from_scalar_subquery",
        description: "The optimized SQL plan keeps the scalar-subquery join in its LEFT SINGLE orientation.",
        expected_join_type: JoinType::LeftSingle,
        input: sql_input(
            "left_single_from_scalar_projection",
            "SELECT (SELECT l.k FROM l WHERE l.k = r.k) FROM r",
        ),
        left: overlap_left_stats(),
        right: overlap_right_stats(),
    };
    write_stats_case_header(&mut file, &left_single)?;
    write_sql_join_input(
        &mut file,
        &left_single,
        left_single.input,
        JoinType::LeftSingle,
    )
    .await?;
    writeln!(&mut file)?;

    let right_single = JoinTestCase {
        name: "right_single_from_optimizer_commute",
        description: "The optimizer derives RIGHT SINGLE from scalar-subquery SQL; the explicit CommuteJoin fallback covers the same optimizer rewrite when needed.",
        expected_join_type: JoinType::RightSingle,
        input: sql_input(
            "right_single_from_scalar_projection",
            "SELECT (SELECT l.k FROM l WHERE l.k = r.k) FROM r",
        ),
        left: large_overlap_stats(),
        right: overlap_left_stats(),
    };
    write_optimizer_commuted_right_single(&mut file, &right_single).await?;
    writeln!(&mut file)?;
    Ok(())
}
