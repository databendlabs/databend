use super::*;

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_type_check_rewrite_functions() -> Result<()> {
    let cases = [
        SqlTestCase {
            name: "nullif_rewrites_to_if",
            description: "nullif(x, y) should rewrite to the IF expression shape.",
            setup_sqls: &[],
            sql: "nullif(number, delta)",
        },
        SqlTestCase {
            name: "equal_null_rewrites_to_null_safe_if",
            description: "equal_null(x, y) should use the explicit null-safe rewrite.",
            setup_sqls: &[],
            sql: "equal_null(number, delta)",
        },
        SqlTestCase {
            name: "iff_alias_rewrites_to_if",
            description: "iff(cond, x, y) should type check through the IF function.",
            setup_sqls: &[],
            sql: "iff(flag, text, pattern)",
        },
        SqlTestCase {
            name: "ifnull_rewrites_to_if",
            description: "ifnull(x, y) should rewrite through IS NULL and IF.",
            setup_sqls: &[],
            sql: "ifnull(text, pattern)",
        },
        SqlTestCase {
            name: "nvl_alias_rewrites_to_if",
            description: "nvl(x, y) should share the ifnull rewrite.",
            setup_sqls: &[],
            sql: "nvl(text, pattern)",
        },
        SqlTestCase {
            name: "nvl2_rewrites_to_if",
            description: "nvl2(x, y, z) should rewrite through IS NOT NULL and IF.",
            setup_sqls: &[],
            sql: "nvl2(text, pattern, 'fallback')",
        },
    ];

    run_type_check_cases("rewrite_functions.txt", &cases).await
}
