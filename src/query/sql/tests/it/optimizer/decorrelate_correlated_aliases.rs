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

const TEST_UDAF_SQL: &str = r#"
CREATE OR REPLACE FUNCTION weighted_avg (a INT, b INT) STATE { sum INT, weight INT } RETURNS FLOAT
LANGUAGE javascript AS $$
export function create_state() {
    return {sum: 0, weight: 0};
}
export function accumulate(state, value, weight) {
    state.sum += value * weight;
    state.weight += weight;
    return state;
}
export function retract(state, value, weight) {
    state.sum -= value * weight;
    state.weight -= weight;
    return state;
}
export function merge(state1, state2) {
    state1.sum += state2.sum;
    state1.weight += state2.weight;
    return state1;
}
export function finish(state) {
    return state.sum / state.weight;
}
$$
"#;

async fn write_optimized_case(file: &mut impl std::io::Write, case: &SqlTestCase) -> Result<()> {
    let ctx = setup_context(case).await?;
    ctx.set_cluster_node_num(1);

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
async fn test_decorrelate_correlated_alias_regressions() -> Result<()> {
    let mut file = open_golden_file("optimizer", "decorrelate_correlated_aliases.txt")?;

    let cases = [
        SqlTestCase {
            name: "nested_filter_alias_reaches_limit_rewrite",
            description: "Filter-derived correlated aliases must remain visible while rewriting a deeper correlated LIMIT subtree.",
            setup_sqls: &[],
            sql: r#"
        SELECT *
        FROM (VALUES (1, 1)) AS t1(a, b)
        WHERE EXISTS (
            SELECT 1
            FROM (
                SELECT t2.a
                FROM (VALUES (1, 1)) AS t2(a, b)
                WHERE t2.b = t1.b
                LIMIT 1
            ) AS s
            WHERE s.a = t1.a
        )
    "#,
        },
        SqlTestCase {
            name: "nested_filter_alias_survives_deeper_join_rewrite",
            description: "A deeper join remap must override a stale filter-local alias instead of collapsing the correlated predicate into a self-equality.",
            setup_sqls: &[],
            sql: r#"
        SELECT *
        FROM (VALUES (1, 1)) AS t1(a, b)
        WHERE EXISTS (
            SELECT 1
            FROM (
                SELECT t2.a
                FROM (VALUES (1, 1)) AS t2(a, b)
                JOIN (VALUES (1)) AS t3(c)
                  ON t2.a = t1.a
                WHERE t2.b = t1.b
            ) AS s
            WHERE s.a = t1.a
        )
    "#,
        },
        SqlTestCase {
            name: "lambda_argument_survives_correlated_exists_decorrelation",
            description: "Lambda-function arguments must be flattened when a correlated EXISTS projection is decorrelated.",
            setup_sqls: &[],
            sql: r#"
        SELECT ref0
        FROM (
            SELECT t1.c0array AS ref0,
                   EXISTS (
                       SELECT 1
                       FROM (VALUES (['baz', 'ab', 'baz'])) AS t66(c0array)
                       WHERE t66.c0array = t1.c0array
                         AND array_any(array_filter(t66.c0array, x -> x = 'baz')) = 'baz'
                   ) AS ref1
            FROM (VALUES (['other'])) AS t1(c0array)
        ) AS s
        WHERE ref1
    "#,
        },
        SqlTestCase {
            name: "correlated_lambda_argument_uses_derived_column",
            description: "A correlated column used as a lambda-function argument must be remapped to its derived column during decorrelation.",
            setup_sqls: &[],
            sql: r#"
        SELECT ref0
        FROM (
            SELECT t1.c0array AS ref0,
                   EXISTS (
                       SELECT 1
                       FROM (VALUES (1)) AS t66(c)
                       WHERE t66.c = 1
                         AND array_any(array_filter(t1.c0array, x -> x = 'baz')) = 'baz'
                   ) AS ref1
            FROM (VALUES (['baz', 'ab'])) AS t1(c0array)
        ) AS s
        WHERE ref1
    "#,
        },
        SqlTestCase {
            name: "typed_constant_survives_correlated_exists_decorrelation",
            description: "A folded scalar subquery constant must remain valid while its containing correlated EXISTS is decorrelated.",
            setup_sqls: &[
                "CREATE TABLE typed_constant_outer(a INT)",
                "CREATE TABLE typed_constant_inner(b INT)",
            ],
            sql: r#"
        SELECT ref0
        FROM (
            SELECT o.a AS ref0,
                   EXISTS (
                       SELECT 1
                       FROM typed_constant_inner AS i
                       WHERE i.b = o.a AND (SELECT true)
                   ) AS ref1
            FROM typed_constant_outer AS o
        ) AS s
        WHERE ref1
    "#,
        },
        SqlTestCase {
            name: "lambda_udf_survives_correlated_exists_decorrelation",
            description: "An expanded SQL lambda UDF must remain valid inside a decorrelated EXISTS predicate.",
            setup_sqls: &[
                "CREATE TABLE lambda_udf_outer(a INT)",
                "CREATE TABLE lambda_udf_inner(b INT)",
                "CREATE FUNCTION is_positive AS (x) -> x > 0",
            ],
            sql: r#"
        SELECT ref0
        FROM (
            SELECT o.a AS ref0,
                   EXISTS (
                       SELECT 1
                       FROM lambda_udf_inner AS i
                       WHERE i.b = o.a AND is_positive(i.b)
                   ) AS ref1
            FROM lambda_udf_outer AS o
        ) AS s
        WHERE ref1
    "#,
        },
        SqlTestCase {
            name: "udaf_survives_correlated_scalar_subquery_decorrelation",
            description: "A UDAF and its arguments must remain valid when a correlated scalar subquery is decorrelated.",
            setup_sqls: &[
                "CREATE TABLE udaf_outer(a INT)",
                "CREATE TABLE udaf_inner(b INT)",
                TEST_UDAF_SQL,
            ],
            sql: r#"
        SELECT ref0
        FROM (
            SELECT o.a AS ref0,
                   (
                       SELECT weighted_avg(i.b, 1)
                       FROM udaf_inner AS i
                       WHERE i.b = o.a
                   ) AS ref1
            FROM udaf_outer AS o
        ) AS s
        WHERE ref1 > 0
    "#,
        },
    ];

    for case in &cases {
        write_optimized_case(&mut file, case).await?;
    }

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_scalar_subquery_comparison_refreshes_return_type() -> Result<()> {
    let case = SqlTestCase {
        name: "scalar_subquery_comparison_refreshes_return_type",
        description: "",
        setup_sqls: &[],
        sql: "SELECT 1 WHERE (SELECT a FROM (VALUES (0), (1)) t(a)) >= 0",
    };
    let ctx = setup_context(&case).await?;
    let plan = ctx.bind_sql(case.sql).await?;
    ctx.optimize_plan(plan).await?;
    Ok(())
}
