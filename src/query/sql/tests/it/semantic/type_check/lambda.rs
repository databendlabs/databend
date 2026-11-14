use super::*;

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_type_check_lambda_functions() -> Result<()> {
    let cases = [
        SqlTestCase {
            name: "non_lambda_function_keeps_ambiguous_arrow_as_json_expression",
            description: "Non-lambda functions should interpret an ambiguous trailing arrow as a JSON expression.",
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
            name: "lambda_functions_can_be_nested",
            description: "Lambda functions should be allowed inside another lambda body.",
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
            name: "lambda_parameter_names_are_normalized_like_references",
            description: "Parameter names are normalized as the names in the body are: an unquoted name matches it in any case, a quoted one as written.",
            setup_sqls: &[],
            sql: "(array_transform([number], X -> x + 1), array_transform([number], \"Y\" -> \"Y\" + 1))",
        },
    ];

    run_type_check_cases("lambda.txt", &cases).await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_type_check_lambda_parameters_case_sensitive() -> Result<()> {
    init_testing_globals();
    let settings = Settings::create(Tenant::new_literal("default"));
    settings.set_setting("unquoted_ident_case_sensitive".to_string(), "1".to_string())?;
    let cases = [
        SqlTestCase {
            name: "case_sensitive_lambda_parameter_matches_same_case",
            description: "With unquoted names case sensitive, a parameter is referred to by its name as written.",
            setup_sqls: &[],
            sql: "array_transform([number], X -> X + 1)",
        },
        SqlTestCase {
            name: "case_sensitive_lambda_parameter_differs_from_other_case",
            description: "With unquoted names case sensitive, a name in another case is not the parameter.",
            setup_sqls: &[],
            sql: "array_transform([number], X -> x + 1)",
        },
    ];

    run_type_check_cases_with_settings(
        "lambda_case_sensitive.txt",
        &cases,
        settings,
        ExprContext::Unknown,
    )
    .await
}
