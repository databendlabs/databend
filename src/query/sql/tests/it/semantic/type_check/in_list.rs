use super::*;

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_type_check_in_list_rewrites() -> Result<()> {
    let cases = [
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

    run_type_check_cases("in_list.txt", &cases).await
}
