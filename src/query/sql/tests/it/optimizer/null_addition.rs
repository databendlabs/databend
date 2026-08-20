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
    let optimized_plan = ctx.optimize_plan(raw_plan.clone()).await?;

    write_case_header(file, case)?;
    writeln!(file, "raw_plan:")?;
    writeln!(file, "{}", raw_plan.format_indent(Default::default())?)?;
    writeln!(file, "optimized_plan:")?;
    writeln!(
        file,
        "{}",
        optimized_plan.format_indent(Default::default())?
    )?;
    writeln!(file)?;

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_null_addition_optimizer_outcomes() -> Result<()> {
    let mut file = open_golden_file("optimizer", "null_addition.txt")?;

    let cases = [
        SqlTestCase {
            name: "inner_join_derives_is_not_null_on_both_sides",
            description: "Null-rejecting equi keys on an inner join should derive is_not_null filters on both branches.",
            setup_sqls: &[LEFT_TABLE, RIGHT_TABLE],
            sql: "SELECT l.k, r.w
FROM null_addition_left AS l
JOIN null_addition_right AS r ON l.k = r.k",
        },
        SqlTestCase {
            name: "left_join_derives_only_on_null_supplying_side",
            description: "A left outer join preserves NULL-key rows on the left, so only the right branch may derive is_not_null.",
            setup_sqls: &[LEFT_TABLE, RIGHT_TABLE],
            sql: "SELECT l.k, r.w
FROM null_addition_left AS l
LEFT JOIN null_addition_right AS r ON l.k = r.k",
        },
        SqlTestCase {
            name: "right_join_derives_only_on_null_supplying_side",
            description: "A right outer join preserves NULL-key rows on the right, so only the left branch may derive is_not_null.",
            setup_sqls: &[LEFT_TABLE, RIGHT_TABLE],
            sql: "SELECT l.k, r.w
FROM null_addition_left AS l
RIGHT JOIN null_addition_right AS r ON l.k = r.k",
        },
        SqlTestCase {
            name: "semi_join_derives_is_not_null_on_both_sides",
            description: "A semi join never preserves unmatched probe rows and NULL keys never match, so both branches may derive is_not_null.",
            setup_sqls: &[LEFT_TABLE, RIGHT_TABLE],
            sql: "SELECT l.k
FROM null_addition_left AS l
WHERE l.k IN (SELECT r.k FROM null_addition_right AS r)",
        },
        SqlTestCase {
            name: "derived_filter_enables_outer_to_inner_in_subquery",
            description: "The is_not_null derived on the inner join key should push into the subquery and turn its left outer join into an inner join.",
            setup_sqls: &[A_TABLE, B_TABLE, C_TABLE],
            sql: "SELECT *
FROM null_addition_a AS a
JOIN (SELECT c.id, b.name
      FROM null_addition_b AS b
      LEFT JOIN null_addition_c AS c ON b.id = c.id) AS v
ON a.id = v.id",
        },
        SqlTestCase {
            name: "full_outer_join_derives_nothing",
            description: "A full outer join preserves NULL-key rows on both sides, so no is_not_null may be derived.",
            setup_sqls: &[LEFT_TABLE, RIGHT_TABLE],
            sql: "SELECT l.k, r.w
FROM null_addition_left AS l
FULL OUTER JOIN null_addition_right AS r ON l.k = r.k",
        },
        SqlTestCase {
            name: "non_nullable_keys_derive_nothing",
            description: "Join keys that are already NOT NULL need no derived is_not_null filter.",
            setup_sqls: &[NN_LEFT_TABLE, NN_RIGHT_TABLE],
            sql: "SELECT l.k, r.w
FROM null_addition_nn_left AS l
JOIN null_addition_nn_right AS r ON l.k = r.k",
        },
        SqlTestCase {
            name: "null_safe_equals_derives_nothing",
            description: "IS NOT DISTINCT FROM matches NULL keys, so it is not null-rejecting and must not derive is_not_null.",
            setup_sqls: &[LEFT_TABLE, RIGHT_TABLE],
            sql: "SELECT l.k, r.w
FROM null_addition_left AS l
JOIN null_addition_right AS r ON l.k IS NOT DISTINCT FROM r.k",
        },
        SqlTestCase {
            name: "deterministic_computed_key_wraps_inlined_predicate",
            description: "A derived is_not_null on a deterministic computed key is inlined through the EvalScalar and wrapped in assume_true_on_error, so a throwing key expression (e.g. a failing cast) keeps the row and defers the error to the real key evaluation. The plain-column side still derives an unwrapped predicate.",
            setup_sqls: &[A_TABLE, S_TABLE],
            sql: "SELECT *
FROM null_addition_a AS a
JOIN (SELECT id, CAST(s AS INT) AS k FROM null_addition_s) AS v
ON a.id = v.k",
        },
        SqlTestCase {
            name: "nondeterministic_key_derives_nothing",
            description: "A join key computed by a nondeterministic expression must not get a derived is_not_null: the materialized value is computed once by the EvalScalar, and later re-evaluation (e.g. inlining) could produce a different value. The plain-column side still derives.",
            setup_sqls: &[A_TABLE, B_TABLE],
            sql: "SELECT *
FROM null_addition_a AS a
JOIN (SELECT id, if(rand() > 0.5, NULL, id) AS k FROM null_addition_b) AS v
ON a.id = v.k",
        },
    ];

    for case in &cases {
        write_optimized_case(&mut file, case).await?;
    }

    Ok(())
}

const LEFT_TABLE: &str = "CREATE TABLE null_addition_left
(
    k INTEGER,
    v INTEGER
)";

const RIGHT_TABLE: &str = "CREATE TABLE null_addition_right
(
    k INTEGER,
    w INTEGER
)";

const NN_LEFT_TABLE: &str = "CREATE TABLE null_addition_nn_left
(
    k INTEGER NOT NULL,
    v INTEGER
)";

const NN_RIGHT_TABLE: &str = "CREATE TABLE null_addition_nn_right
(
    k INTEGER NOT NULL,
    w INTEGER
)";

const A_TABLE: &str = "CREATE TABLE null_addition_a
(
    id INTEGER
)";

const B_TABLE: &str = "CREATE TABLE null_addition_b
(
    id INTEGER,
    name STRING
)";

const C_TABLE: &str = "CREATE TABLE null_addition_c
(
    id INTEGER
)";

const S_TABLE: &str = "CREATE TABLE null_addition_s
(
    id INTEGER,
    s STRING
)";
