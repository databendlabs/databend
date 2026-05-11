use super::*;

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_type_check_scalar_rewrites() -> Result<()> {
    let cases = [
        SqlTestCase {
            name: "not_between_rewrites_to_or_comparisons",
            description: "A NOT BETWEEN predicate from range-join tests should type check as the explicit disjunction.",
            setup_sqls: &[],
            sql: "number NOT BETWEEN 1 AND 5",
        },
        SqlTestCase {
            name: "is_distinct_from_rewrites_null_safe_comparison",
            description: "IS DISTINCT FROM appears in join tests and should lower through the null-aware comparison rewrite.",
            setup_sqls: &[],
            sql: "number IS DISTINCT FROM delta",
        },
        SqlTestCase {
            name: "is_not_distinct_from_rewrites_null_safe_comparison",
            description: "IS NOT DISTINCT FROM should share the same null-aware comparison path with the inverted result.",
            setup_sqls: &[],
            sql: "number IS NOT DISTINCT FROM delta",
        },
        SqlTestCase {
            name: "searched_case_from_sqllogictest_binds",
            description: "A searched CASE expression from sqllogictest patterns should rewrite into the IF function shape.",
            setup_sqls: &[],
            sql: "CASE WHEN number > 1 THEN text WHEN number < 0 THEN pattern ELSE 'fallback' END",
        },
        SqlTestCase {
            name: "simple_case_compares_operand_to_each_condition",
            description: "A simple CASE expression should preserve the operand-comparison rewrite.",
            setup_sqls: &[],
            sql: "CASE number WHEN 1 THEN text WHEN 2 THEN pattern ELSE 'fallback' END",
        },
        SqlTestCase {
            name: "small_in_list_rewrites_to_balanced_or",
            description: "A small IN list should stay in scalar type checking rather than using the subquery conversion path.",
            setup_sqls: &[],
            sql: "number IN (1, 2, delta)",
        },
        SqlTestCase {
            name: "not_in_list_wraps_rewritten_predicate",
            description: "NOT IN should wrap the scalar IN-list rewrite with a NOT function.",
            setup_sqls: &[],
            sql: "number NOT IN (1, 2, delta)",
        },
    ];

    run_type_check_cases("scalar_rewrites.txt", &cases).await
}
