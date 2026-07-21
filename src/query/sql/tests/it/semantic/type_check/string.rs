use super::*;

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_type_check_string_rewrites() -> Result<()> {
    let cases = [
        SqlTestCase {
            name: "substring_from_for_from_tpc_h_binds",
            description: "The TPCH substring(expr FROM start FOR len) form should route to the substring function.",
            setup_sqls: &[],
            sql: "substring(text FROM 1 FOR 2)",
        },
        SqlTestCase {
            name: "position_in_from_function_tests_binds",
            description: "The POSITION(substr IN str) syntax should type check as locate(substr, str).",
            setup_sqls: &[],
            sql: "position(pattern IN text)",
        },
        SqlTestCase {
            name: "trim_leading_from_function_tests_binds",
            description: "The SQL TRIM LEADING form should type check through trim_leading.",
            setup_sqls: &[],
            sql: "trim(leading 'a' from text)",
        },
        SqlTestCase {
            name: "trim_both_dynamic_pattern_binds",
            description: "The SQL TRIM BOTH form should keep dynamic trim patterns supported.",
            setup_sqls: &[],
            sql: "trim(both pattern from text)",
        },
        SqlTestCase {
            name: "like_prefix_rewrites_to_range",
            description: "A LIKE prefix pattern from comparison tests should type check as a range predicate.",
            setup_sqls: &[],
            sql: "text LIKE 'ab%'",
        },
        SqlTestCase {
            name: "non_ascii_like_prefix_rewrites_to_range",
            description: "A non-ASCII LIKE prefix should use the Unicode scalar range rewrite.",
            setup_sqls: &[],
            sql: "text LIKE '测试%'",
        },
        SqlTestCase {
            name: "latin1_like_prefix_rewrites_without_byte_overflow",
            description: "A LIKE prefix ending at U+00FF should rewrite using the next Unicode scalar.",
            setup_sqls: &[],
            sql: "text LIKE 'ÿ%'",
        },
        SqlTestCase {
            name: "like_percent_rewrites_to_is_not_null",
            description: "A LIKE pattern made only of percent wildcards should type check as an IS NOT NULL predicate.",
            setup_sqls: &[],
            sql: "text LIKE '%%'",
        },
        SqlTestCase {
            name: "like_escape_from_sqllogictest_binds",
            description: "A LIKE ESCAPE expression from sqllogictests should preserve the escape-aware function path.",
            setup_sqls: &[],
            sql: "text LIKE 'a!_%' ESCAPE '!'",
        },
        SqlTestCase {
            name: "dynamic_like_pattern_keeps_like_function",
            description: "A dynamic LIKE pattern should not be rewritten as a constant-prefix optimization.",
            setup_sqls: &[],
            sql: "text LIKE pattern",
        },
        SqlTestCase {
            name: "ilike_lowers_both_sides",
            description: "ILIKE should preserve case-insensitive semantics instead of using plain LIKE.",
            setup_sqls: &[],
            sql: "text ILIKE pattern",
        },
        SqlTestCase {
            name: "not_ilike_lowers_both_sides",
            description: "NOT ILIKE should negate the case-insensitive LIKE expression.",
            setup_sqls: &[],
            sql: "text NOT ILIKE pattern",
        },
        SqlTestCase {
            name: "ilike_any_lowers_tuple_patterns",
            description: "ILIKE ANY should bind to the case-insensitive like_any function.",
            setup_sqls: &[],
            sql: "text ILIKE ANY ('A%', 'B%')",
        },
        SqlTestCase {
            name: "ilike_escape_lowers_escape",
            description: "ILIKE ESCAPE should preserve the escape marker for runtime pattern conversion.",
            setup_sqls: &[],
            sql: "text ILIKE 'A_%' ESCAPE 'A'",
        },
        SqlTestCase {
            name: "regexp_operator_from_function_tests_binds",
            description: "The regexp operator form from scalar comparison tests should type check as regexp_like.",
            setup_sqls: &[],
            sql: "text REGEXP pattern",
        },
        SqlTestCase {
            name: "rlike_operator_from_function_tests_binds",
            description: "The rlike alias should share the regexp_like type-check path.",
            setup_sqls: &[],
            sql: "text RLIKE pattern",
        },
    ];

    run_type_check_cases("string.txt", &cases).await
}
