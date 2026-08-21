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
        SqlTestCase {
            name: "identity_cast_preserves_complete_argument",
            description: "An eliminated identity cast must not remap an inner function argument onto the original call.",
            setup_sqls: &[],
            sql: "to_int64(100000000 + ((854435761::UInt64 * number + 123456789) % 900000000))",
        },
    ];

    run_type_check_cases("scalar_function.txt", &cases).await
}
