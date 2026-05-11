use super::*;

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_type_check_rule_errors() -> Result<()> {
    let cases = [
        SqlTestCase {
            name: "window_function_inside_lambda_errors",
            description: "Window functions should remain rejected while resolving lambda bodies.",
            setup_sqls: &[],
            sql: "array_transform([number], x -> row_number() OVER ())",
        },
        SqlTestCase {
            name: "unknown_function_keeps_suggestion",
            description: "Unknown scalar functions should keep the suggestion-oriented error.",
            setup_sqls: &[],
            sql: "abss(number)",
        },
        SqlTestCase {
            name: "scalar_parameter_must_be_constant",
            description: "Parameterized scalar functions should reject non-constant parameters before scalar resolution.",
            setup_sqls: &[],
            sql: "to_decimal(number, number)",
        },
        SqlTestCase {
            name: "read_file_rejects_invalid_arity",
            description: "read_file should validate its one-or-two argument shape during async-function lowering.",
            setup_sqls: &[],
            sql: "read_file()",
        },
        SqlTestCase {
            name: "read_file_requires_stage_path",
            description: "A single-argument read_file call should require an @stage path when the location is constant.",
            setup_sqls: &[],
            sql: "read_file('not_stage_path')",
        },
        SqlTestCase {
            name: "async_functions_cannot_be_nested",
            description: "Async functions should remain rejected while resolving another async function argument.",
            setup_sqls: &[],
            sql: "read_file(read_file('@s/file.txt'))",
        },
        SqlTestCase {
            name: "nextval_requires_identifier_argument",
            description: "nextval should reject non-identifier arguments before sequence resolution.",
            setup_sqls: &[],
            sql: "nextval(1)",
        },
        SqlTestCase {
            name: "nextval_rejects_multipart_identifier",
            description: "nextval should accept only a one-part sequence identifier.",
            setup_sqls: &[],
            sql: "nextval(db.seq)",
        },
        SqlTestCase {
            name: "dict_get_requires_dictionary_identifier",
            description: "dict_get should reject a non-identifier dictionary argument before catalog resolution.",
            setup_sqls: &[],
            sql: "dict_get('dict', 'field', number)",
        },
        SqlTestCase {
            name: "dict_get_rejects_three_part_identifier",
            description: "dict_get should accept only one-or-two-part dictionary identifiers.",
            setup_sqls: &[],
            sql: "dict_get(catalog.db.dict, 'field', number)",
        },
        SqlTestCase {
            name: "score_rejects_arguments",
            description: "score should keep its zero-argument search-function contract.",
            setup_sqls: &[],
            sql: "score(1)",
        },
        SqlTestCase {
            name: "match_requires_where_context",
            description: "match search should remain restricted to WHERE-clause resolution.",
            setup_sqls: &[],
            sql: "match(text, 'needle')",
        },
        SqlTestCase {
            name: "query_requires_where_context",
            description: "query search should remain restricted to WHERE-clause resolution.",
            setup_sqls: &[],
            sql: "query('text:needle')",
        },
        SqlTestCase {
            name: "non_window_function_rejects_over_clause",
            description: "A scalar function should not be accepted with window syntax.",
            setup_sqls: &[],
            sql: "abs(number) OVER ()",
        },
        SqlTestCase {
            name: "non_aggregate_function_rejects_within_group",
            description: "WITHIN GROUP syntax should remain limited to aggregate functions.",
            setup_sqls: &[],
            sql: "abs(number) WITHIN GROUP (ORDER BY number)",
        },
        SqlTestCase {
            name: "window_function_rejects_ignore_nulls_when_unsupported",
            description: "Non-value window functions should reject IGNORE/RESPECT NULLS options.",
            setup_sqls: &[],
            sql: "row_number() IGNORE NULLS OVER ()",
        },
        SqlTestCase {
            name: "non_lambda_function_rejects_lambda_syntax",
            description: "Only lambda functions should accept lambda syntax.",
            setup_sqls: &[],
            sql: "abs(number, x -> x)",
        },
        SqlTestCase {
            name: "lambda_function_requires_lambda_expression",
            description: "Lambda functions should reject calls without a lambda expression.",
            setup_sqls: &[],
            sql: "array_transform([number])",
        },
        SqlTestCase {
            name: "lambda_functions_cannot_be_nested",
            description: "Lambda functions should remain rejected while resolving another lambda body.",
            setup_sqls: &[],
            sql: "array_transform([number], x -> array_transform([x], y -> y))",
        },
        SqlTestCase {
            name: "lambda_function_requires_collection_argument",
            description: "Lambda functions should require an array or map input argument.",
            setup_sqls: &[],
            sql: "array_transform(number, x -> x)",
        },
        SqlTestCase {
            name: "lambda_filter_requires_boolean_result",
            description: "Filter-style lambda functions should require a boolean lambda result.",
            setup_sqls: &[],
            sql: "array_filter([number], x -> x)",
        },
        SqlTestCase {
            name: "lambda_function_checks_parameter_count",
            description: "Lambda functions should validate the number of lambda parameters for the input collection.",
            setup_sqls: &[],
            sql: "array_transform([number], (x, y) -> x)",
        },
        SqlTestCase {
            name: "aggregate_parameter_must_be_constant",
            description: "Parameterized aggregate arguments should be constant before aggregate resolution.",
            setup_sqls: &[],
            sql: "quantile_cont(number)(number)",
        },
        SqlTestCase {
            name: "map_accessor_requires_literal_path",
            description: "Map and variant accessors should reject unsupported bracket expressions during lowering.",
            setup_sqls: &[],
            sql: "to_variant({'k1': 1})[true]",
        },
    ];

    run_type_check_cases("rules.txt", &cases).await
}
