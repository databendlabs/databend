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
        SqlTestCase {
            name: "constant_in_list_uses_contains_after_or_threshold",
            description: "A constant IN list above max_inlist_to_or should preserve the old contains(array_distinct(...)) rewrite.",
            setup_sqls: &[],
            sql: "number IN (1, 2, 3, 4)",
        },
        SqlTestCase {
            name: "constant_not_in_list_wraps_contains_rewrite",
            description: "NOT IN should wrap the contains rewrite when the constant list exceeds max_inlist_to_or.",
            setup_sqls: &[],
            sql: "number NOT IN (1, 2, 3, 4)",
        },
    ];

    run_type_check_cases("in_list.txt", &cases).await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_type_check_in_list_subquery_conversion() -> Result<()> {
    init_testing_globals();
    let settings = Settings::create(Tenant::new_literal("default"));
    settings.set_setting("inlist_to_join_threshold".to_string(), "4".to_string())?;
    let cases = [
        SqlTestCase {
            name: "in_list_at_join_threshold_converts_to_subquery",
            description: "An IN list at inlist_to_join_threshold should preserve the old constant-scan subquery conversion.",
            setup_sqls: &[],
            sql: "number IN (1, 2, 3, 4)",
        },
        SqlTestCase {
            name: "not_in_list_at_join_threshold_wraps_subquery_conversion",
            description: "NOT IN at inlist_to_join_threshold should wrap the converted subquery with NOT.",
            setup_sqls: &[],
            sql: "number NOT IN (1, 2, 3, 4)",
        },
    ];

    run_type_check_cases_with_settings(
        "in_list_subquery.txt",
        &cases,
        settings,
        ExprContext::Unknown,
    )
    .await
}
