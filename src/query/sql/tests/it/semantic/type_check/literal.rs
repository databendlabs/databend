use super::*;

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_type_check_literal() -> Result<()> {
    let cases = [
        SqlTestCase {
            name: "float_literal_from_parser_tests_binds",
            description: "A float literal shape from parser tests should resolve as a typed constant.",
            setup_sqls: &[],
            sql: "1.925e-3",
        },
        SqlTestCase {
            name: "binary_literal_from_parser_tests_binds",
            description: "A binary literal shape from parser tests should resolve as a typed constant.",
            setup_sqls: &[],
            sql: "x'deedbeef'",
        },
        SqlTestCase {
            name: "null_literal_binds",
            description: "A NULL literal should stay on the literal type-check path.",
            setup_sqls: &[],
            sql: "NULL",
        },
        SqlTestCase {
            name: "constant_array_from_comparison_tests_binds",
            description: "A constant array from scalar comparison tests should use the constant-array fast path.",
            setup_sqls: &[],
            sql: "[1, 2, 3]",
        },
        SqlTestCase {
            name: "mixed_array_with_column_binds",
            description: "An array with a column element should fall back to the scalar array function path.",
            setup_sqls: &[],
            sql: "[1, delta, NULL]",
        },
        SqlTestCase {
            name: "map_literal_from_sqllogictest_binds",
            description: "A map literal shape from map sqllogictests should resolve through map construction.",
            setup_sqls: &[],
            sql: "{'k1': 1, 'k2': delta}",
        },
        SqlTestCase {
            name: "tuple_literal_from_parser_tests_binds",
            description: "A tuple expression from parser tests should resolve through tuple construction.",
            setup_sqls: &[],
            sql: "(number, text, true)",
        },
    ];

    run_type_check_cases("literal.txt", &cases).await
}
