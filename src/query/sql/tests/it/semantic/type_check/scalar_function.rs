use super::*;

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_type_check_scalar_function_rules() -> Result<()> {
    let cases = [
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
    ];

    run_type_check_cases("scalar_function.txt", &cases).await
}
