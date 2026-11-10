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

//! Rules that depend only on the value of a constant also apply to a
//! `TypedConstantExpr`, which bind-time folding produces for an explicit cast
//! whose type a literal cannot have, e.g. `CAST(1 AS INT NULL)`.

use databend_common_exception::Result;

use crate::framework::golden::SqlTestCase;
use crate::framework::golden::open_golden_file;
use crate::framework::golden::setup_context;
use crate::framework::golden::write_case_header;

async fn write_optimized_case(file: &mut impl std::io::Write, case: &SqlTestCase) -> Result<()> {
    let ctx = setup_context(case).await?;
    let raw_plan = ctx.bind_sql(case.sql).await?;
    let optimized_plan = ctx.optimize_plan(raw_plan.clone()).await?;

    write_case_header(file, case)?;
    writeln!(file, "raw_plan:")?;
    writeln!(
        file,
        "{}",
        raw_plan.format_indent(Default::default(), &ctx.stat_context()?)?
    )?;
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
async fn test_typed_constant_optimizer_rules() -> Result<()> {
    let mut file = open_golden_file("optimizer", "typed_constant.txt")?;

    let cases = [
        SqlTestCase {
            name: "typed_true_filter_is_removed",
            description: "A filter that is a typed TRUE constant is dropped.",
            setup_sqls: &[LEFT_TABLE],
            sql: "SELECT a FROM typed_constant_left WHERE CAST(true AS BOOLEAN NULL)",
        },
        SqlTestCase {
            name: "typed_null_filter_is_empty",
            description: "A filter that is a typed NULL constant yields no rows.",
            setup_sqls: &[LEFT_TABLE],
            sql: "SELECT a FROM typed_constant_left WHERE CAST(NULL AS BOOLEAN)",
        },
        SqlTestCase {
            name: "range_absorbs_typed_constant",
            description: "Filter inference merges a range bounded by a typed constant with another range on the same column.",
            setup_sqls: &[LEFT_TABLE],
            sql: "SELECT a FROM typed_constant_left WHERE a > 5 AND a > CAST(3 AS INT NULL)",
        },
        SqlTestCase {
            name: "comparison_with_typed_null_is_false",
            description: "Filter inference turns a comparison with a typed NULL into a false filter.",
            setup_sqls: &[LEFT_TABLE],
            sql: "SELECT a FROM typed_constant_left WHERE a = CAST(NULL AS INT) AND b > 1",
        },
        SqlTestCase {
            name: "trivial_cast_removed_against_typed_constant",
            description: "A lossless cast of a column compared with a typed constant is removed; the constant is converted to the column type.",
            setup_sqls: &[LEFT_TABLE],
            sql: "SELECT a FROM typed_constant_left WHERE CAST(s AS BIGINT) > CAST(1 AS BIGINT NULL)",
        },
        SqlTestCase {
            name: "null_rejecting_typed_constant_makes_inner_join",
            description: "A filter on the right side that is NULL when the right columns are NULL turns a LEFT join into an INNER join; a typed constant in it does not block the check.",
            setup_sqls: &[LEFT_TABLE, RIGHT_TABLE],
            sql: "SELECT l.a FROM typed_constant_left AS l LEFT JOIN typed_constant_right AS r ON l.a = r.a WHERE r.b + CAST(1 AS INT NULL) > 0",
        },
        SqlTestCase {
            name: "typed_constant_in_subquery",
            description: "A typed constant on the left of IN (subquery) is decorrelated like a literal.",
            setup_sqls: &[LEFT_TABLE, RIGHT_TABLE],
            sql: "SELECT a FROM typed_constant_left WHERE CAST(1 AS INT NULL) IN (SELECT a FROM typed_constant_right)",
        },
        SqlTestCase {
            name: "in_unnest_of_typed_array",
            description: "IN over the unnest of a typed constant array becomes a comparison with its elements.",
            setup_sqls: &[LEFT_TABLE],
            sql: "SELECT a FROM typed_constant_left WHERE a IN (SELECT * FROM UNNEST(CAST([1, 2] AS ARRAY(INT) NULL)) AS t)",
        },
    ];

    for case in &cases {
        write_optimized_case(&mut file, case).await?;
    }

    Ok(())
}

const LEFT_TABLE: &str = "CREATE TABLE typed_constant_left
(
    a INT,
    b INT NULL,
    s TINYINT UNSIGNED
)";

const RIGHT_TABLE: &str = "CREATE TABLE typed_constant_right
(
    a INT,
    b INT NULL
)";
