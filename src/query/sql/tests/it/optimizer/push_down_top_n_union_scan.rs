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

use databend_common_exception::Result;

use crate::framework::golden::SqlTestCase;
use crate::framework::golden::open_golden_file;
use crate::framework::golden::setup_context;
use crate::framework::golden::write_case_header;

async fn write_optimized_case(file: &mut impl std::io::Write, case: &SqlTestCase) -> Result<()> {
    let ctx = setup_context(case).await?;
    let raw_plan = ctx.bind_sql(case.sql).await?;
    let optimized_plan = ctx.optimize_plan(raw_plan).await?;

    write_case_header(file, case)?;
    writeln!(file, "optimized_plan:")?;
    writeln!(
        file,
        "{}",
        optimized_plan.format_indent(Default::default(), &ctx.stat_context()?)?
    )?;
    writeln!(file)?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_push_down_top_n_union_scan_outcomes() -> Result<()> {
    let mut file = open_golden_file("optimizer", "push_down_top_n_union_scan.txt")?;

    let cases = [
        SqlTestCase {
            name: "union_branches_get_branch_column_order",
            description: "A TopN over UNION ALL should order every branch scan by the column that branch outputs.",
            setup_sqls: &[T1_TABLE, T2_TABLE],
            sql: "SELECT a FROM (SELECT a FROM t1 UNION ALL SELECT b FROM t2) u ORDER BY a LIMIT 3",
        },
        SqlTestCase {
            name: "filtered_branch_pushes_through_eval_scalar",
            description: "A filtered branch keeps an EvalScalar above its scan, which the push-down crosses.",
            setup_sqls: &[T1_TABLE, T2_TABLE],
            sql: "SELECT a FROM (SELECT a FROM t1 WHERE x > 0 UNION ALL SELECT b FROM t2) u ORDER BY a DESC LIMIT 3",
        },
        SqlTestCase {
            name: "derived_branch_column_does_not_push",
            description: "A branch whose column is computed above its scan keeps no scan order; the other branch still gets it.",
            setup_sqls: &[BIGINT_TABLE, T2_TABLE],
            sql: "SELECT d FROM (SELECT d FROM t4 UNION ALL SELECT b + 1 FROM t2) u ORDER BY d LIMIT 3",
        },
        SqlTestCase {
            name: "nested_unions_push_to_all_scans",
            description: "Nested UNION ALL branches should all receive the scan order.",
            setup_sqls: &[T1_TABLE, T2_TABLE],
            sql: "SELECT a FROM (SELECT a FROM t1 UNION ALL SELECT b FROM t2 UNION ALL SELECT x FROM t1) u ORDER BY a LIMIT 3",
        },
        SqlTestCase {
            name: "nullable_coercion_keeps_branch_order",
            description: "A branch cast only to the nullable type keeps its values, so it still receives the scan order.",
            setup_sqls: &[T1_TABLE, NOT_NULL_TABLE],
            sql: "SELECT a FROM (SELECT a FROM t1 UNION ALL SELECT c FROM t3) u ORDER BY a LIMIT 3",
        },
        SqlTestCase {
            name: "widening_coercion_skips_branch",
            description: "A branch cast to a wider type changes its values, so only the uncast branch receives the scan order.",
            setup_sqls: &[T1_TABLE, BIGINT_TABLE],
            sql: "SELECT a FROM (SELECT a FROM t1 UNION ALL SELECT d FROM t4) u ORDER BY a LIMIT 3",
        },
        SqlTestCase {
            name: "multiple_sort_keys_push_all_keys",
            description: "A multi-key TopN pushes every key, translated per branch; the runtime filter prunes by the first key.",
            setup_sqls: &[T1_TABLE, T2_TABLE],
            sql: "SELECT a, x FROM (SELECT a, x FROM t1 UNION ALL SELECT b, b FROM t2) u ORDER BY a, x LIMIT 3",
        },
        SqlTestCase {
            name: "limited_branch_does_not_push",
            description: "A Limit inside a branch stops the push-down for that branch only.",
            setup_sqls: &[T1_TABLE, T2_TABLE],
            sql: "SELECT a FROM ((SELECT a FROM t1 LIMIT 10) UNION ALL SELECT b FROM t2) u ORDER BY a LIMIT 3",
        },
        SqlTestCase {
            name: "union_distinct_does_not_push",
            description: "UNION DISTINCT aggregates above the union, so the TopN does not see the union directly.",
            setup_sqls: &[T1_TABLE, T2_TABLE],
            sql: "SELECT a FROM (SELECT a FROM t1 UNION SELECT b FROM t2) u ORDER BY a LIMIT 3",
        },
    ];

    for case in &cases {
        write_optimized_case(&mut file, case).await?;
    }

    Ok(())
}

const T1_TABLE: &str = "CREATE TABLE t1(a INT NULL, x INT NULL)";
const T2_TABLE: &str = "CREATE TABLE t2(b INT NULL)";
const NOT_NULL_TABLE: &str = "CREATE TABLE t3(c INT NOT NULL)";
const BIGINT_TABLE: &str = "CREATE TABLE t4(d BIGINT NULL)";
