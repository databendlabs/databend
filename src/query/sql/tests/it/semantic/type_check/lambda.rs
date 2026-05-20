use super::*;

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_type_check_lambda_functions() -> Result<()> {
    let cases = [
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
    ];

    run_type_check_cases("lambda.txt", &cases).await
}
