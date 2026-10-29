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

use std::collections::HashSet;

use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_sql::plans::PhysicalRelOperator;
use databend_common_sql::plans::Plan;

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

    assert_unique_eval_scalar_indexes(&optimized_plan, case.name);

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

/// An `EvalScalar` defines exactly one value per output index and may only reference columns
/// produced by its input. Check both invariants directly on the optimized plan tree, so the test
/// does not depend on the golden text alone.
fn assert_unique_eval_scalar_indexes(plan: &Plan, case_name: &str) {
    let Plan::Query { s_expr, .. } = plan else {
        panic!("case {case_name} should optimize into a query plan");
    };

    let mut stack = vec![
        s_expr
            .planned()
            .expect("optimized query should be physically planned")
            .expr(),
    ];
    let mut eval_scalars_seen = 0usize;
    while let Some(expr) = stack.pop() {
        if let PhysicalRelOperator::EvalScalar(eval_scalar) = expr.plan() {
            eval_scalars_seen += 1;
            let input_columns = expr
                .unary_child()
                .derive_relational_prop()
                .unwrap()
                .output_columns
                .clone();
            let mut indexes = HashSet::with_capacity(eval_scalar.items.len());
            for item in &eval_scalar.items {
                assert!(
                    item.scalar.used_columns().is_subset(&input_columns),
                    "case {case_name}: EvalScalar references columns outside its input: {:?}",
                    item
                );
                assert!(
                    indexes.insert(item.index),
                    "case {}: EvalScalar defines output index {} more than once, in items {:?}",
                    case_name,
                    item.index,
                    eval_scalar
                        .items
                        .iter()
                        .map(|item| item.index)
                        .collect::<Vec<_>>(),
                );
            }
        }
        stack.extend(expr.children());
    }

    // Guard against the walk silently passing because it never reached an EvalScalar.
    assert!(
        eval_scalars_seen > 0,
        "case {case_name} should contain at least one EvalScalar"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_computed_correlated_alias_variants() -> Result<()> {
    let statements = [
        (
            "arithmetic_alias_in_nested_join",
            "SELECT * FROM (VALUES (1,1),(2,2)) t1(a,b) WHERE EXISTS (SELECT 1 FROM (SELECT t2.a+1 AS a FROM (VALUES (1,1),(2,2)) t2(a,b) JOIN (VALUES (1)) t3(c) ON t2.a=t1.a WHERE t2.b=t1.b) s WHERE s.a=t1.a)",
        ),
        (
            "computed_second_alias",
            "SELECT * FROM (VALUES (1,1),(2,2)) t1(a,b) WHERE EXISTS (SELECT 1 FROM (SELECT t2.a AS a, t2.b+1 AS b FROM (VALUES (1,1),(2,2)) t2(a,b) JOIN (VALUES (1)) t3(c) ON t2.a=t1.a WHERE t2.b=t1.b) s WHERE s.a=t1.a AND s.b=t1.b)",
        ),
        (
            "two_correlations_share_one_inner_column",
            "SELECT * FROM (VALUES (1,1),(2,2)) t1(a,b) WHERE EXISTS (SELECT 1 FROM (SELECT t2.a AS a FROM (VALUES (1,1),(2,2)) t2(a,b) WHERE t2.a=t1.a AND t2.a=t1.b) s WHERE s.a=t1.a)",
        ),
        (
            "computed_alias_under_limit",
            "SELECT * FROM (VALUES (1,1),(2,2)) t1(a,b) WHERE EXISTS (SELECT 1 FROM (SELECT t2.a+1 AS a FROM (VALUES (1,1),(2,2)) t2(a,b) WHERE t2.b=t1.b LIMIT 1) s WHERE s.a=t1.a)",
        ),
        (
            "correlation_in_outer_join_on",
            "SELECT * FROM (VALUES (1,1),(2,2)) t1(a,b) WHERE NOT EXISTS (SELECT 1 FROM (SELECT t2.a AS a FROM (VALUES (1,1),(2,2)) t2(a,b) LEFT JOIN (VALUES (1)) t3(c) ON t2.a=t1.a WHERE t2.b=t1.b) s WHERE s.a=t1.a)",
        ),
    ];
    let mut file = open_golden_file("optimizer", "decorrelate_computed_aliases.txt")?;
    for (name, sql) in statements {
        let case = SqlTestCase {
            name,
            description: "Preserve definitions while resolving correlated aliases.",
            setup_sqls: &[],
            sql,
        };
        write_optimized_case(&mut file, &case).await?;
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_filter_alias_scope_variants() -> Result<()> {
    let statements = [
        (
            "identity_alias_above_ordered_limit",
            "SELECT * FROM (VALUES (1,1),(2,1)) t1(a,b) WHERE EXISTS (SELECT 1 FROM (SELECT t2.a AS a FROM (VALUES (1,1),(2,1)) t2(a,b) WHERE t2.b=t1.b ORDER BY t2.a DESC LIMIT 1) s WHERE s.a=t1.a)",
        ),
        (
            "computed_alias_above_ordered_limit",
            "SELECT * FROM (VALUES (1,1),(2,1)) t1(a,b) WHERE EXISTS (SELECT 1 FROM (SELECT t2.a+1 AS a FROM (VALUES (0,1),(1,1)) t2(a,b) WHERE t2.b=t1.b ORDER BY t2.a DESC LIMIT 1) s WHERE s.a=t1.a)",
        ),
        (
            "aggregate_output_alias",
            "SELECT * FROM (VALUES (1,1),(2,1)) t1(a,b) WHERE EXISTS (SELECT 1 FROM (SELECT max(t2.a) AS a FROM (VALUES (1,1),(2,1)) t2(a,b) WHERE t2.b=t1.b) s WHERE s.a=t1.a)",
        ),
        (
            "union_output_alias",
            "SELECT * FROM (VALUES (1,1),(2,1)) t1(a,b) WHERE EXISTS (SELECT 1 FROM (SELECT t2.a AS a FROM (VALUES (1,1),(2,1)) t2(a,b) WHERE t2.b=t1.b UNION ALL SELECT t3.a FROM (VALUES (5,1)) t3(a,b) WHERE t3.b=t1.b) s WHERE s.a=t1.a)",
        ),
        (
            "computed_alias_above_join_without_outer_reference",
            "SELECT * FROM (VALUES (1,1),(2,1)) t1(a,b) WHERE EXISTS (SELECT 1 FROM (SELECT x.a+1 AS a FROM (SELECT * FROM (VALUES (1,1),(2,1)) t2(a,b) WHERE t2.b=t1.b) x JOIN (VALUES (1)) t3(c) ON true) s WHERE s.a=t1.a)",
        ),
    ];
    let mut file = open_golden_file("optimizer", "decorrelate_filter_alias_scope.txt")?;
    for (name, sql) in statements {
        let case = SqlTestCase {
            name,
            description: "A filter alias for an outer column must not be used below its definition or below a LIMIT.",
            setup_sqls: &[],
            sql,
        };
        write_optimized_case(&mut file, &case).await?;
    }
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
        SqlTestCase {
            name: "identity_projection_keeps_table_binding",
            description: "When a correlated column resolves to an index the subquery already projects as an identity, keep that item so the remaining filter still carries a table binding and is absorbed into the scan instead of staying as a redundant Filter.",
            setup_sqls: &[
                "CREATE TABLE a(id INT, c1 INT NULL)",
                "CREATE TABLE b(id INT, c1 INT NULL)",
            ],
            sql: "SELECT * FROM a WHERE a.id = (SELECT id FROM b WHERE a.id = b.id)",
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

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_count_alias_without_group_by_is_rejected() -> Result<()> {
    // These aggregates return a row for an empty input (`count(*)` is 0), but materializing
    // the outer input would drop that row. Reject the query instead of returning wrong results.
    let statements = [
        (
            "count_alias",
            "SELECT * FROM (VALUES (0,2),(1,1)) t1(a,b) WHERE EXISTS (SELECT 1 FROM (SELECT count(*) AS n FROM (VALUES (1,1)) t2(a,b) WHERE t2.b=t1.b) s WHERE s.n=t1.a)",
        ),
        (
            "count_alias_under_window",
            "SELECT * FROM (VALUES (0,2),(1,1)) t1(a,b) WHERE EXISTS (SELECT 1 FROM (SELECT count(*) AS n, row_number() OVER () AS r FROM (VALUES (1,1)) t2(a,b) WHERE t2.b=t1.b) s WHERE s.n=t1.a)",
        ),
        (
            "count_alias_with_empty_grouping_set",
            "SELECT * FROM (VALUES (0,2),(1,1)) t1(a,b) WHERE EXISTS (SELECT 1 FROM (SELECT count(*) AS n FROM (VALUES (1,1)) t2(a,b) WHERE t2.b=t1.b GROUP BY GROUPING SETS ((), (t2.a))) s WHERE s.n=t1.a)",
        ),
    ];
    for (name, sql) in statements {
        let case = SqlTestCase {
            name,
            description: "",
            setup_sqls: &[],
            sql,
        };
        let ctx = setup_context(&case).await?;
        let plan = ctx.bind_sql(case.sql).await?;
        let Err(err) = ctx.optimize_plan(plan).await else {
            panic!("case {name} should be rejected");
        };
        assert_eq!(
            err.code(),
            ErrorCode::UNIMPLEMENTED,
            "case {name}: unexpected error: {err:?}"
        );
    }
    Ok(())
}
