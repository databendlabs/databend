use super::*;

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_type_check_column_refs() -> Result<()> {
    let cases = [
        SqlTestCase {
            name: "column_name_resolves_binding",
            description: "A named column reference should resolve through BindContext name lookup.",
            setup_sqls: &[],
            sql: "number",
        },
        SqlTestCase {
            name: "column_position_resolves_binding",
            description: "A positional column reference should preserve the old BindContext position lookup.",
            setup_sqls: &[],
            sql: "$1",
        },
    ];

    run_type_check_cases("column.txt", &cases).await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_type_check_virtual_column_ref() -> Result<()> {
    init_testing_globals();
    let settings = Settings::create(Tenant::new_literal("default"));
    let adapter = TestTypeCheckAdapter::new(settings);
    let mut bind_context = test_bind_context(ExprContext::Unknown);

    bind_context.add_column_binding(
        ColumnBindingBuilder::new(
            "computed".to_string(),
            Symbol::new(7),
            Box::new(DataType::Number(NumberDataType::Int64).wrap_nullable()),
            Visibility::Visible,
        )
        .column_position(Some(8))
        .virtual_expr(Some("number + delta".to_string()))
        .build(),
    );

    let case = SqlTestCase {
        name: "virtual_column_resolves_expression",
        description: "A column binding with a virtual expression should preserve the old recursive expression resolution.",
        setup_sqls: &[],
        sql: "computed",
    };

    let outcome = match resolve_type_check_sql(case.sql, adapter, &mut bind_context) {
        Ok((scalar, data_type)) => SqlTestOutcome::Plan(format!(
            "scalar: {}\ntype: {}",
            format_scalar(&scalar),
            data_type
        )),
        Err(err) => SqlTestOutcome::Error {
            code: err.code(),
            message: err.message(),
        },
    };

    let mut file = open_golden_file("semantic/type_check", "column_virtual.txt")?;
    write_case_header(&mut file, &case)?;
    write_case_outcome_body(&mut file, &outcome)?;

    Ok(())
}
