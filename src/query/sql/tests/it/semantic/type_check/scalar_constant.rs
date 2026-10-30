use super::*;

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_type_check_constant_parameters() -> Result<()> {
    let cases = [
        SqlTestCase {
            name: "decimal_scale_from_folded_expression",
            description: "A constant argument read as a parameter accepts expressions folded at bind time.",
            setup_sqls: &[],
            sql: "round(CAST(number AS DECIMAL(10,3)), 1 + 1)",
        },
        SqlTestCase {
            name: "decimal_scale_from_explicit_cast",
            description: "An explicit cast of a literal folds at bind time, so it is read as a parameter.",
            setup_sqls: &[],
            sql: "round(CAST(number AS DECIMAL(10,3)), CAST(2 AS INT))",
        },
        SqlTestCase {
            name: "decimal_scale_rejects_column",
            description: "A column cannot be read as a constant parameter.",
            setup_sqls: &[],
            sql: "round(CAST(number AS DECIMAL(10,3)), number)",
        },
        SqlTestCase {
            name: "decimal_precision_from_explicit_cast",
            description: "to_decimal precision accepts a cast literal.",
            setup_sqls: &[],
            sql: "to_decimal(text, CAST(10 AS INT), 2)",
        },
        SqlTestCase {
            name: "histogram_buckets_must_fit_target_type",
            description: "A constant that does not convert to the parameter type reports the failed cast.",
            setup_sqls: &[],
            sql: "histogram(number, -1)",
        },
        SqlTestCase {
            name: "range_offset_from_explicit_cast",
            description: "A window RANGE offset accepts a cast literal.",
            setup_sqls: &[],
            sql: "sum(number) OVER (ORDER BY delta RANGE BETWEEN CAST(2 AS INT) PRECEDING AND CURRENT ROW)",
        },
        SqlTestCase {
            name: "range_offset_rejects_column",
            description: "A window RANGE offset must be constant.",
            setup_sqls: &[],
            sql: "sum(number) OVER (ORDER BY delta RANGE BETWEEN number PRECEDING AND CURRENT ROW)",
        },
        SqlTestCase {
            name: "lag_offset_accepts_folded_cast",
            description: "An explicit cast of a literal folds before LAG reads its offset.",
            setup_sqls: &[],
            sql: "lag(number, CAST(2 AS INT)) OVER (ORDER BY delta)",
        },
    ];

    run_type_check_cases("scalar_constant.txt", &cases).await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_scalar_expr_constant_value() -> Result<()> {
    let func_ctx = FunctionContext::default();
    for (sql, expected) in [
        ("1 + 2", Some("3 UInt8")),
        // An explicit cast of a literal folds, keeping the type it names.
        ("CAST(2 AS INT)", Some("2 Int32")),
        // A NULL cast folds to a constant that keeps the type it names.
        ("CAST(NULL AS INT)", Some("NULL Int32 NULL")),
        ("number", None),
        ("number + 1 > number", None),
        ("rand()", None),
        ("count(number)", None),
    ] {
        init_testing_globals();
        let adapter = TestTypeCheckAdapter::new(Settings::create(Tenant::new_literal("default")));
        let mut bind_context = test_bind_context(ExprContext::Unknown);
        let (scalar, _) = resolve_type_check_sql(sql, adapter, &mut bind_context)?;
        let value = scalar
            .constant_value(&func_ctx)
            .map(|(value, data_type)| format!("{value} {data_type}"));
        assert_eq!(value.as_deref(), expected, "{sql}");
    }
    Ok(())
}
