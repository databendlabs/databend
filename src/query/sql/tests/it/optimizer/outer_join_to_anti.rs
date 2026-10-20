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
use databend_common_sql::optimizer::optimizers::rule::outer_join_to_anti_join;
use databend_common_sql::plans::Plan;
use databend_common_sql::plans::ScalarExpr;

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
async fn test_outer_join_to_anti_optimizer_outcomes() -> Result<()> {
    let mut file = open_golden_file("optimizer", "outer_join_to_anti.txt")?;

    let cases = [
        SqlTestCase {
            name: "regular_right_key_becomes_left_anti",
            description: "A NULL filter on a regular right equi-key should become a left anti join.",
            setup_sqls: &[LEFT_TABLE, RIGHT_TABLE],
            sql: "SELECT l.k
FROM outer_to_anti_left AS l
LEFT JOIN outer_to_anti_right AS r ON l.k = r.k
WHERE r.k IS NULL",
        },
        SqlTestCase {
            name: "right_outputs_are_reconstructed_as_nulls",
            description: "Right outputs observed above the rewrite should remain available as typed NULL expressions.",
            setup_sqls: &[LEFT_TABLE, RIGHT_TABLE],
            sql: "SELECT l.k, r.k, r.payload
FROM outer_to_anti_left AS l
LEFT JOIN outer_to_anti_right AS r ON l.k = r.k
WHERE r.k IS NULL",
        },
        SqlTestCase {
            name: "remaining_filter_and_limit_optimize_through_reconstruction",
            description: "Multiple equi-conditions, a remaining left filter, and LIMIT should continue optimizing around the NULL reconstruction.",
            setup_sqls: &[LEFT_TABLE, RIGHT_TABLE],
            sql: "SELECT l.k, r.payload
FROM outer_to_anti_left AS l
LEFT JOIN outer_to_anti_right AS r ON l.k = r.k AND l.item = r.item
WHERE r.k IS NULL AND l.keep > 0
LIMIT 10",
        },
        SqlTestCase {
            name: "nullable_payload_test_keeps_left_outer",
            description: "A nullable non-key payload can be NULL on matched rows, so it must not become an anti join.",
            setup_sqls: &[LEFT_TABLE, RIGHT_TABLE],
            sql: "SELECT l.k, r.payload
FROM outer_to_anti_left AS l
LEFT JOIN outer_to_anti_right AS r ON l.k = r.k
WHERE r.payload IS NULL",
        },
        SqlTestCase {
            name: "right_outer_exclusion_becomes_anti",
            description: "A NULL filter on a regular left equi-key should become an anti join; later join commutation may canonicalize it to a left anti join with swapped inputs.",
            setup_sqls: &[LEFT_TABLE, RIGHT_TABLE],
            sql: "SELECT r.k
FROM outer_to_anti_left AS l
RIGHT JOIN outer_to_anti_right AS r ON l.k = r.k
WHERE l.k IS NULL",
        },
        SqlTestCase {
            name: "left_outputs_are_reconstructed_as_nulls",
            description: "Left outputs observed above a right anti rewrite should remain available as typed NULL expressions.",
            setup_sqls: &[LEFT_TABLE, RIGHT_TABLE],
            sql: "SELECT l.k, l.keep, r.k
FROM outer_to_anti_left AS l
RIGHT JOIN outer_to_anti_right AS r ON l.k = r.k
WHERE l.k IS NULL",
        },
        SqlTestCase {
            name: "right_filter_and_limit_optimize_through_reconstruction",
            description: "A remaining right filter and LIMIT should continue optimizing around left-side NULL reconstruction.",
            setup_sqls: &[LEFT_TABLE, RIGHT_TABLE],
            sql: "SELECT l.keep, r.k
FROM outer_to_anti_left AS l
RIGHT JOIN outer_to_anti_right AS r ON l.k = r.k AND l.item = r.item
WHERE l.k IS NULL AND r.payload > 0
LIMIT 10",
        },
        SqlTestCase {
            name: "nullable_left_payload_test_keeps_outer_join",
            description: "A nullable non-key left payload can be NULL on matched rows, so the right outer join must remain an outer join even if later canonicalized with swapped inputs.",
            setup_sqls: &[LEFT_TABLE, RIGHT_TABLE],
            sql: "SELECT l.keep, r.k
FROM outer_to_anti_left AS l
RIGHT JOIN outer_to_anti_right AS r ON l.k = r.k
WHERE l.keep IS NULL",
        },
        SqlTestCase {
            name: "null_safe_condition_keeps_left_outer",
            description: "A null-safe condition is not a regular equi-key in the raw plan and must remain a left outer join.",
            setup_sqls: &[LEFT_TABLE, RIGHT_TABLE],
            sql: "SELECT l.k, r.k
FROM outer_to_anti_left AS l
LEFT JOIN outer_to_anti_right AS r ON l.k IS NOT DISTINCT FROM r.k
WHERE r.k IS NULL",
        },
    ];

    for case in cases.iter().chain(EXPRESSION_CASES).chain(NON_KEY_CASES) {
        write_optimized_case(&mut file, case).await?;
    }

    Ok(())
}

// Exercise the eligibility check on SQL-bound plans as well as the full optimizer
// golden above. This makes the regression independent of later folding/rule
// ordering, without constructing a synthetic SExpr tree.
#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_outer_join_to_anti_expression_key_guard() -> Result<()> {
    for case in EXPRESSION_CASES {
        let ctx = setup_context(case).await?;
        let raw_plan = ctx.bind_sql(case.sql).await?;
        let Plan::Query {
            s_expr, metadata, ..
        } = &raw_plan
        else {
            unreachable!("test query should bind to Plan::Query")
        };
        let filter = s_expr.logical()?.unary_child();
        assert!(filter.plan().as_filter().is_some(), "{}", case.name);
        let join = filter.unary_child().plan().as_join().unwrap();
        assert_eq!(join.equi_conditions.len(), 1, "{}", case.name);
        let key = match join.join_type {
            databend_common_sql::plans::JoinType::Left => &join.equi_conditions[0].right,
            databend_common_sql::plans::JoinType::Right => &join.equi_conditions[0].left,
            _ => unreachable!("expected a left or right outer join"),
        };
        assert!(matches!(key, ScalarExpr::FunctionCall(_)), "{}", case.name);
        assert!(!join.equi_conditions[0].is_null_equal, "{}", case.name);
        let predicates = &filter.plan().as_filter().unwrap().predicates;
        let [ScalarExpr::FunctionCall(not)] = predicates.as_slice() else {
            panic!("expected a single NULL test: {}", case.name)
        };
        assert_eq!(not.func_name, "not", "{}", case.name);
        let [ScalarExpr::FunctionCall(is_not_null)] = not.arguments.as_slice() else {
            panic!("expected IS NOT NULL under NOT: {}", case.name)
        };
        assert_eq!(is_not_null.func_name, "is_not_null", "{}", case.name);
        assert_eq!(is_not_null.arguments.as_slice(), std::slice::from_ref(key));
        assert!(
            outer_join_to_anti_join(filter, metadata.clone())?.is_none(),
            "a null-masking expression is not a direct column: {}",
            case.name
        );
    }
    Ok(())
}

const EXPRESSION_CASES: &[SqlTestCase] = &[
    SqlTestCase {
        name: "null_masking_right_expression_does_not_become_anti_join",
        description: "The nullable payload prevents trivial IS NULL folding, but the equi-expression maps null extension to zero and must not identify unmatched rows.",
        setup_sqls: &[LEFT_TABLE, RIGHT_TABLE],
        sql: "SELECT l.k, r.k, r.payload
FROM outer_to_anti_left AS l
LEFT JOIN outer_to_anti_right AS r ON l.k = IF(r.k IS NULL, 0, r.payload)
WHERE IF(r.k IS NULL, 0, r.payload) IS NULL",
    },
    SqlTestCase {
        name: "null_masking_left_expression_does_not_become_anti_join",
        description: "The right-outer mirror must also reject a null-masking expression key, even though the expression may be NULL on source rows.",
        setup_sqls: &[LEFT_TABLE, RIGHT_TABLE],
        sql: "SELECT l.k, l.keep, r.k
FROM outer_to_anti_left AS l
RIGHT JOIN outer_to_anti_right AS r ON IF(l.k IS NULL, 0, l.keep) = r.k
WHERE IF(l.k IS NULL, 0, l.keep) IS NULL",
    },
];

// These cases protect the existing non-key rejection. This change does not
// introduce new nonnull-inference paths.
const NON_KEY_CASES: &[SqlTestCase] = &[
    SqlTestCase {
        name: "grouping_sets_output_keeps_outer_join",
        description: "GROUPING SETS can synthesize NULL from a NOT NULL grouping key on a matched row; a non-key NULL test is not an exclusion signal.",
        setup_sqls: &[LEFT_TABLE, NON_NULL_TABLE],
        sql: "SELECT l.k, r.marker
FROM outer_to_anti_left AS l
LEFT JOIN (
    SELECT marker, count(*) AS n
    FROM outer_to_anti_non_null
    GROUP BY GROUPING SETS ((marker), ())
) AS r ON l.k = r.n
WHERE r.marker IS NULL",
    },
    SqlTestCase {
        name: "rollup_output_keeps_right_outer_join",
        description: "The right-outer mirror must not use a ROLLUP key as a nonnull payload witness.",
        setup_sqls: &[LEFT_TABLE, NON_NULL_TABLE],
        sql: "SELECT l.k, r.marker
FROM (
    SELECT marker, count(*) AS n
    FROM outer_to_anti_non_null
    GROUP BY ROLLUP(marker)
) AS r RIGHT JOIN outer_to_anti_left AS l ON l.k = r.n
WHERE r.marker IS NULL",
    },
    SqlTestCase {
        name: "nested_outer_join_output_keeps_outer_join",
        description: "A nested outer join may null-extend a NOT NULL source column on a matched outer row.",
        setup_sqls: &[LEFT_TABLE, RIGHT_TABLE, NON_NULL_TABLE],
        sql: "SELECT l.k, d.marker
FROM outer_to_anti_left AS l
LEFT JOIN (
    SELECT r.k, n.marker
    FROM outer_to_anti_right AS r
    LEFT JOIN outer_to_anti_non_null AS n ON r.k = n.k
) AS d ON l.k = d.k
WHERE d.marker IS NULL",
    },
    SqlTestCase {
        name: "nullable_union_payload_keeps_outer_join",
        description: "A nullable non-key payload remains nullable through UNION ALL and cannot identify unmatched rows.",
        setup_sqls: &[LEFT_TABLE, RIGHT_TABLE],
        sql: "SELECT l.k, r.marker
FROM outer_to_anti_left AS l
LEFT JOIN (
    SELECT k, payload AS marker FROM outer_to_anti_right
    UNION ALL
    SELECT k, 1 AS marker FROM outer_to_anti_right
) AS r ON l.k = r.k
WHERE r.marker IS NULL",
    },
];

const NON_NULL_TABLE: &str = "CREATE TABLE outer_to_anti_non_null
(
    k INTEGER,
    marker INTEGER NOT NULL
)";

const LEFT_TABLE: &str = "CREATE TABLE outer_to_anti_left
(
    k INTEGER,
    item INTEGER,
    keep INTEGER
)";

const RIGHT_TABLE: &str = "CREATE TABLE outer_to_anti_right
(
    k INTEGER,
    item INTEGER,
    payload INTEGER
)";
