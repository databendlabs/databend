use super::*;

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_type_check_set_returning_functions() -> Result<()> {
    let cases = [
        SqlTestCase {
            name: "unnest_extracts_single_tuple_field",
            description: "A single-output SRF should preserve the old get(unnest(...), 1) top-level extraction.",
            setup_sqls: &[],
            sql: "unnest([1, 2, 3])",
        },
        SqlTestCase {
            name: "nested_set_returning_function_errors",
            description: "SRF arguments should keep the old nested set-returning function rejection.",
            setup_sqls: &[],
            sql: "unnest(unnest([1, 2, 3]))",
        },
        SqlTestCase {
            name: "set_returning_function_rejects_window_spec",
            description: "SRFs should remain rejected while resolving aggregate window arguments.",
            setup_sqls: &[],
            sql: "sum(unnest([1, 2, 3])) OVER ()",
        },
        SqlTestCase {
            name: "set_returning_function_rejects_filter",
            description: "FILTER syntax should be rejected before SRF-specific lowering can ignore it.",
            setup_sqls: &[],
            sql: "unnest([1]) FILTER (WHERE false)",
        },
    ];

    run_type_check_cases("set_returning.txt", &cases).await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_type_check_set_returning_clause_rules() -> Result<()> {
    let cases = [
        (
            "set_returning_where.txt",
            ExprContext::WhereClause,
            SqlTestCase {
                name: "set_returning_function_rejects_where_clause",
                description: "SRFs should keep the old WHERE-clause rejection.",
                setup_sqls: &[],
                sql: "unnest([1, 2, 3])",
            },
        ),
        (
            "set_returning_having.txt",
            ExprContext::HavingClause,
            SqlTestCase {
                name: "set_returning_function_rejects_having_clause",
                description: "SRFs should keep the old HAVING-clause rejection.",
                setup_sqls: &[],
                sql: "unnest([1, 2, 3])",
            },
        ),
        (
            "set_returning_qualify.txt",
            ExprContext::QualifyClause,
            SqlTestCase {
                name: "set_returning_function_rejects_qualify_clause",
                description: "SRFs should keep the old QUALIFY-clause rejection.",
                setup_sqls: &[],
                sql: "unnest([1, 2, 3])",
            },
        ),
    ];

    for (file_name, expr_context, case) in cases {
        run_type_check_cases_in_context(file_name, &[case], expr_context).await?;
    }

    Ok(())
}
