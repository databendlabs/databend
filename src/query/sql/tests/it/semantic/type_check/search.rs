use super::*;

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_type_check_search_rule_errors() -> Result<()> {
    let cases = [
        SqlTestCase {
            name: "match_rejects_non_constant_query_text",
            description: "match should require constant query text after reaching WHERE-clause search resolution.",
            setup_sqls: &[],
            sql: "match(text, pattern)",
        },
        SqlTestCase {
            name: "match_rejects_non_constant_option",
            description: "match should require constant option text after reaching WHERE-clause search resolution.",
            setup_sqls: &[],
            sql: "match(text, 'needle', pattern)",
        },
        SqlTestCase {
            name: "match_option_requires_key_value",
            description: "Search function options should keep the key=value contract.",
            setup_sqls: &[],
            sql: "match(text, 'needle', 'operator')",
        },
        SqlTestCase {
            name: "match_rejects_unsupported_option",
            description: "Search function options should reject unsupported keys or values.",
            setup_sqls: &[],
            sql: "match(text, 'needle', 'operator=x')",
        },
        SqlTestCase {
            name: "match_rejects_invalid_boost",
            description: "match should validate boost values when the field list is supplied as a constant string.",
            setup_sqls: &[],
            sql: "match('text^bad', 'needle')",
        },
        SqlTestCase {
            name: "query_rejects_non_constant_query_text",
            description: "query should require constant query text after reaching WHERE-clause search resolution.",
            setup_sqls: &[],
            sql: "query(pattern)",
        },
        SqlTestCase {
            name: "query_option_requires_key_value",
            description: "query should share the search option parser with match.",
            setup_sqls: &[],
            sql: "query('text:needle', 'lenient')",
        },
    ];

    run_type_check_cases_in_context("search.txt", &cases, ExprContext::WhereClause).await
}
