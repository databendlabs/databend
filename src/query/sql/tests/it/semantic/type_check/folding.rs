//! `defer_column_folding` keeps column-dependent expressions unfolded, as when
//! binding the pre-aggregate forms that GROUPING SETS keys are matched against.

use super::*;

fn non_null_context(defer: bool) -> BindContext {
    let mut context = test_bind_context(ExprContext::Unknown);
    *context.columns[0].data_type = DataType::Number(NumberDataType::Int64);
    context.defer_column_folding = defer;
    context
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_deferred_column_folding_preserves_column_predicates() -> Result<()> {
    init_testing_globals();
    let settings = Settings::create(Tenant::new_literal("default"));
    for name in ["is_null", "is_not_null"] {
        let sql = format!("{name}(number)");
        let (ordinary, _) = resolve_type_check_sql(
            &sql,
            TestTypeCheckAdapter::new(settings.clone()),
            &mut non_null_context(false),
        )?;
        assert!(matches!(ordinary, ScalarExpr::ConstantExpr(_)));

        let (deferred, _) = resolve_type_check_sql(
            &sql,
            TestTypeCheckAdapter::new(settings.clone()),
            &mut non_null_context(true),
        )?;
        assert!(matches!(deferred, ScalarExpr::FunctionCall(_)));
        assert!(deferred.used_columns().contains(&Symbol::new(0)));
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_deferred_column_folding_preserves_udf_condition() -> Result<()> {
    init_testing_globals();
    let settings = Settings::create(Tenant::new_literal("default"));
    let adapter =
        TestTypeCheckAdapter::new(settings).with_udf_adapter(TestUdfAdapter::with_definitions([
            UserDefinedFunction::create_lambda_udf(
                "with_default",
                vec!["x".to_string()],
                "coalesce(x, 100)",
                "",
            ),
        ]));
    let (scalar, _) =
        resolve_type_check_sql("with_default(number)", adapter, &mut non_null_context(true))?;
    let ScalarExpr::UDFLambdaCall(udf) = scalar else {
        panic!("expected an expanded lambda UDF")
    };
    let ScalarExpr::FunctionCall(function) = *udf.scalar else {
        panic!("the UDF must retain its conditional expression")
    };
    assert_eq!(function.func_name, "if");
    let ScalarExpr::FunctionCall(condition) = &function.arguments[0] else {
        panic!("the UDF parameter's non-null test must not become a constant")
    };
    assert_eq!(condition.func_name, "is_not_null");
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_deferred_column_folding_keeps_pure_constant_evaluation() -> Result<()> {
    init_testing_globals();
    let settings = Settings::create(Tenant::new_literal("default"));
    let (scalar, _) = resolve_type_check_sql(
        "coalesce(NULL, 40 + 2)",
        TestTypeCheckAdapter::new(settings),
        &mut non_null_context(true),
    )?;
    let ScalarExpr::ConstantExpr(constant) = scalar else {
        panic!("literal-only expressions should still be evaluated during binding")
    };
    assert_eq!(constant.value.get_i64(), Some(42));
    Ok(())
}
