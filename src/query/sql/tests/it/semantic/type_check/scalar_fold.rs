use databend_common_expression::TableDataType;

use super::*;

const fn case(name: &'static str, description: &'static str, sql: &'static str) -> SqlTestCase {
    SqlTestCase {
        name,
        description,
        setup_sqls: &[],
        sql,
    }
}

/// `test_bind_context` columns are nullable; folding by type also needs
/// non-nullable ones.
fn folding_bind_context() -> BindContext {
    let mut bind_context = test_bind_context(ExprContext::Unknown);
    for (index, name, data_type) in [
        (7, "small", DataType::Number(NumberDataType::UInt8)),
        (8, "nn", DataType::Number(NumberDataType::Int64)),
        (9, "nb", DataType::Boolean),
    ] {
        bind_context.add_column_binding(
            ColumnBindingBuilder::new(
                name.to_string(),
                Symbol::new(index),
                Box::new(data_type),
                Visibility::Visible,
            )
            .build(),
        );
    }
    bind_context
}

fn script_udf() -> UserDefinedFunction {
    UserDefinedFunction::create_udf_script(
        "script_udf",
        "return arguments[0];",
        "handler",
        "javascript",
        vec![TableDataType::String],
        TableDataType::String,
        "",
        "",
        Some(false),
    )
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_type_check_bind_time_folding() -> Result<()> {
    let cases = [
        // Calls over constants are evaluated; the result is typed as a literal.
        case(
            "constant_arithmetic",
            "Calls over constants are evaluated.",
            "1 + 2 * 3",
        ),
        case(
            "decimal_product_with_reduced_scale",
            "A product rounded to a smaller scale is evaluated, not taken from raw-value domains.",
            "CAST(2.5 AS DECIMAL(38, 14)) * CAST(2 AS DECIMAL(38, 14))",
        ),
        case(
            "column_free_lambda_is_evaluated",
            "A lambda over constants is evaluated by its parent.",
            "length(array_transform([1, 2], x -> x + 1))",
        ),
        // An explicit cast keeps the type it names; a call that folds over it does not.
        case(
            "explicit_cast_keeps_its_type",
            "A folded explicit cast keeps its type, so the parent is checked on Int64.",
            "CAST(1 AS BIGINT) + CAST(number AS TINYINT UNSIGNED)",
        ),
        case(
            "explicit_decimal_cast_keeps_size",
            "A folded decimal cast keeps its size, so the parent stays decimal arithmetic.",
            "CAST(1 AS DECIMAL(38, 0)) / CAST(3 AS DECIMAL(38, 0))",
        ),
        case(
            "explicit_nullable_cast_is_typed_constant",
            "A literal cannot have a nullable type, so the folded cast keeps it as a typed constant.",
            "CAST(1 AS INT NULL)",
        ),
        case(
            "call_over_explicit_cast_is_literal",
            "The type of an explicit cast does not carry through a call that folds over it.",
            "CAST(1 AS BIGINT) + 1",
        ),
        // A constant the call casts implicitly is replaced only if its literal has the checked type.
        case(
            "implicit_cast_to_timestamp_is_replaced",
            "The literal cast to a timestamp is replaced by its value.",
            "ts > '2001-01-01'",
        ),
        case(
            "implicit_cast_to_decimal_keeps_literal",
            "The value shrinks to an integer, not the checked decimal, so the literal stays.",
            "CAST(number AS DECIMAL(10, 3)) * 2",
        ),
        case(
            "string_compared_with_number_is_not_cast",
            "eq has a (number, string) overload, so the literal is not cast and stays a string.",
            "number = '5'",
        ),
        // Types and nullability imply single values.
        case(
            "type_range_decides_comparison",
            "The range of UInt8 decides the comparison.",
            "small < 300",
        ),
        case(
            "type_range_through_explicit_cast",
            "Type ranges follow casts.",
            "CAST(small AS BIGINT) < 1000",
        ),
        case(
            "non_nullable_argument_is_not_null",
            "Nullability of a non-nullable expression decides IS NOT NULL.",
            "is_not_null(small + 1)",
        ),
        case(
            "nullable_argument_stays",
            "A nullable column can be NULL.",
            "is_not_null(number)",
        ),
        case(
            "argument_that_may_throw_stays",
            "An argument that may fail cannot be dropped, so IS NOT NULL is kept.",
            "is_not_null(to_int64(text))",
        ),
        case(
            "multiply_non_nullable_by_zero",
            "A non-nullable value times zero is zero.",
            "nn * 0",
        ),
        case(
            "multiply_nullable_by_zero_stays",
            "A nullable value times zero can be NULL.",
            "number * 0",
        ),
        case(
            "aggregate_reference_can_be_dropped",
            "Aggregates are computed by their operator; their non-null result decides IS NOT NULL.",
            "is_not_null(count(number))",
        ),
        // Constant conditions select or drop if branches; CASE and COALESCE lower to if.
        case(
            "if_selects_first_branch",
            "A true first condition selects its branch.",
            "if(true, number, delta)",
        ),
        case(
            "if_selects_else_branch_with_cast",
            "The selected branch keeps the type of the call through the implicit cast.",
            "if(false, number, delta)",
        ),
        case(
            "if_selects_constant_branch_as_literal",
            "A selected constant branch is typed as a literal, not as the type that if unifies its branches to.",
            "if(true, 1, number)",
        ),
        case(
            "if_selects_explicitly_cast_constant_as_literal",
            "The type of an explicit cast does not carry through the if that selects it.",
            "if(true, 1::INT64, number)",
        ),
        case(
            "if_null_condition_is_false",
            "A NULL condition never selects its branch.",
            "if(NULL, number, delta)",
        ),
        case(
            "if_drops_false_branch",
            "Branches behind false conditions are removed.",
            "if(flag, number, false, delta, number + 1)",
        ),
        case(
            "if_drops_unevaluated_failing_branch",
            "A branch that is never evaluated may fail; it is dropped.",
            "if(false, to_int64('bad'), number)",
        ),
        case(
            "case_with_constant_condition",
            "CASE lowers to IF and drops branches behind false conditions.",
            "CASE WHEN 1 = 2 THEN 'a' WHEN flag THEN 'b' ELSE 'c' END",
        ),
        case(
            "coalesce_of_non_nullable",
            "COALESCE of a non-nullable first argument yields it.",
            "coalesce(nn, number)",
        ),
        // AND and OR fold by absorption and contradictory ranges.
        case(
            "boolean_absorption",
            "false absorbs AND of a non-nullable boolean.",
            "nb AND false",
        ),
        case(
            "contradictory_ranges_on_non_nullable",
            "Contradictory comparisons on a non-nullable column are false.",
            "nn > 10 AND nn < 5",
        ),
        case(
            "contradictory_ranges_on_nullable_stay",
            "With a nullable column, AND yields NULL or false, so it is kept.",
            "number > 10 AND number < 5",
        ),
        // Non-deterministic calls and UDFs are kept.
        case(
            "non_deterministic_call_stays",
            "Non-deterministic calls are not evaluated.",
            "rand() < 2",
        ),
        case(
            "udf_is_not_dropped_by_nullability",
            "A UDF may fail or have effects, so IS NOT NULL keeps it.",
            "is_not_null(script_udf(text))",
        ),
        case(
            "udf_is_not_dropped_by_multiplication",
            "A UDF is kept even when the result is known.",
            "length(script_udf(text)) * 0",
        ),
        case(
            "if_selects_udf_branch",
            "Selecting a branch keeps it even when it is a UDF.",
            "if(true, script_udf(text), 'x')",
        ),
    ];

    let mut file = open_golden_file("semantic/type_check", "scalar_fold.txt")?;
    for (index, case) in cases.iter().enumerate() {
        if index > 0 {
            writeln!(file)?;
        }
        write_case_header(&mut file, case)?;
        init_testing_globals();
        let adapter = TestTypeCheckAdapter::new(Settings::create(Tenant::new_literal("default")))
            .with_udf_adapter(TestUdfAdapter::with_definitions([script_udf()]));
        let mut bind_context = folding_bind_context();
        let result = resolve_type_check_sql(case.sql, adapter, &mut bind_context);
        let outcome = type_check_outcome(case, result);
        write_case_outcome_body(&mut file, &outcome)?;
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_non_deterministic_call_marks_result_uncacheable() -> Result<()> {
    init_testing_globals();
    let settings = Settings::create(Tenant::new_literal("default"));
    for (sql, uncacheable) in [("rand() < 2", true), ("number + 1", false)] {
        let adapter = TestTypeCheckAdapter::new(settings.clone());
        let mut bind_context = folding_bind_context();
        resolve_type_check_sql(sql, adapter.clone(), &mut bind_context)?;
        assert_eq!(adapter.result_cache_uncacheable(), uncacheable, "{sql}");
    }
    Ok(())
}
