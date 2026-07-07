use chrono::Utc;
use databend_common_expression::TableDataType as DataType;
use databend_common_expression::TableField as DataField;
use databend_common_expression::types::NumberDataType;
use databend_common_meta_app::principal::ScalarUDF;
use databend_common_meta_app::principal::UDAFScript;
use databend_common_meta_app::principal::UDFDefinition;
use databend_common_sql::plans::BoundColumnRef;

use super::*;

fn test_settings() -> Arc<Settings> {
    init_testing_globals();
    Settings::create(Tenant::new_literal("default"))
}

fn adapter_with_udfs(
    udfs: impl IntoIterator<Item = UserDefinedFunction>,
) -> (TestTypeCheckAdapter, TestUdfAdapter) {
    let udf_adapter = TestUdfAdapter::with_definitions(udfs);
    let adapter = TestTypeCheckAdapter::new(test_settings()).with_udf_adapter(udf_adapter.clone());
    (adapter, udf_adapter)
}

fn scalar_udf(
    name: &str,
    arg_types: Vec<(String, DataType)>,
    return_type: DataType,
    definition: &str,
) -> UserDefinedFunction {
    let now = Utc::now();
    UserDefinedFunction {
        name: name.to_string(),
        description: String::new(),
        definition: UDFDefinition::ScalarUDF(ScalarUDF {
            arg_types,
            return_type,
            definition: definition.to_string(),
        }),
        created_on: now,
        update_on: now,
    }
}

fn cached_udf() -> UserDefinedFunction {
    UserDefinedFunction::create_lambda_udf("cached_udf", vec!["x".to_string()], "x + 1", "")
}

fn server_udf() -> UserDefinedFunction {
    UserDefinedFunction::create_udf_server(
        "server_udf",
        "http://127.0.0.1:8815",
        "handler",
        &Default::default(),
        "python",
        Vec::new(),
        vec![DataType::Number(NumberDataType::Int64).wrap_nullable()],
        DataType::String,
        "",
        Some(false),
    )
}

fn script_udf() -> UserDefinedFunction {
    UserDefinedFunction::create_udf_script(
        "script_udf",
        "return arguments[0];",
        "handler",
        "javascript",
        vec![DataType::String],
        DataType::String,
        "",
        "",
        Some(false),
    )
}

fn cloud_udf() -> UserDefinedFunction {
    UserDefinedFunction::create_udf_script(
        "cloud_udf",
        "def handler(x): return x",
        "handler",
        "python",
        vec![DataType::Number(NumberDataType::Int64).wrap_nullable()],
        DataType::String,
        "",
        "",
        Some(false),
    )
}

fn udaf_script(name: &str) -> UserDefinedFunction {
    let now = Utc::now();
    UserDefinedFunction {
        name: name.to_string(),
        description: String::new(),
        definition: UDFDefinition::UDAFScript(UDAFScript {
            code: "return state;".to_string(),
            imports: Vec::new(),
            packages: Vec::new(),
            language: "javascript".to_string(),
            arg_types: vec![DataType::String],
            state_fields: vec![DataField::new("state", DataType::String)],
            return_type: DataType::String,
            runtime_version: String::new(),
        }),
        created_on: now,
        update_on: now,
    }
}

struct UdfGoldenCase {
    case: SqlTestCase,
    adapter: TestTypeCheckAdapter,
    cached_udf: Option<UserDefinedFunction>,
    aliases: &'static [(&'static str, &'static str)],
}

async fn run_udf_golden_cases(cases: &[UdfGoldenCase]) -> Result<()> {
    let mut file = open_golden_file("semantic/type_check", "udf.txt")?;

    for (index, golden_case) in cases.iter().enumerate() {
        if index > 0 {
            writeln!(file)?;
        }
        write_case_header(&mut file, &golden_case.case)?;
        let mut bind_context = test_bind_context(ExprContext::Unknown);
        if let Some(udf) = &golden_case.cached_udf {
            bind_context
                .udf_cache
                .write()
                .insert(udf.name.clone(), Some(udf.clone()));
        }
        let aliases = aliases_from_columns(&bind_context, golden_case.aliases);
        let outcome = match resolve_type_check_sql_with_aliases(
            golden_case.case.sql,
            golden_case.adapter.clone(),
            &mut bind_context,
            &aliases,
        ) {
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
        write_case_outcome_body(&mut file, &outcome)?;
    }

    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn test_type_check_udf_behaviors() -> Result<()> {
    init_testing_globals();
    let (unknown_adapter, _) = adapter_with_udfs([]);

    let blocked_udf =
        UserDefinedFunction::create_lambda_udf("blocked_udf", vec!["x".to_string()], "x + 1", "");
    let (blocked_adapter, _) = adapter_with_udfs([blocked_udf]);
    let blocked_adapter = blocked_adapter.with_forbid_udf(true);

    let lambda_one =
        UserDefinedFunction::create_lambda_udf("lambda_one", vec!["x".to_string()], "x + 1", "");
    let lambda_two = UserDefinedFunction::create_lambda_udf(
        "lambda_two",
        vec!["x".to_string(), "y".to_string()],
        "x + y",
        "",
    );
    let scalar_to_string = scalar_udf(
        "scalar_to_string",
        vec![(
            "x".to_string(),
            DataType::Number(NumberDataType::Int32).wrap_nullable(),
        )],
        DataType::String,
        "x",
    );
    let scope_probe = UserDefinedFunction::create_lambda_udf(
        "scope_probe",
        vec!["x".to_string(), "arr".to_string()],
        "array_transform(arr, y -> x + y)",
        "",
    );
    let variant_probe = UserDefinedFunction::create_lambda_udf(
        "variant_probe",
        vec!["x".to_string()],
        "x::variant",
        "",
    );

    let (main_adapter, _) = adapter_with_udfs([
        lambda_one,
        lambda_two,
        scalar_to_string,
        scope_probe,
        variant_probe,
        server_udf(),
        script_udf(),
        udaf_script("udaf_script"),
    ]);
    let cache_adapter =
        TestTypeCheckAdapter::new(test_settings()).with_udf_adapter(TestUdfAdapter::default());
    let cloud_adapter = TestTypeCheckAdapter::new(test_settings()).with_udf_adapter(
        TestUdfAdapter::with_definitions([cloud_udf()]).with_enable_udf_sandbox(),
    );

    let cases = [
        UdfGoldenCase {
            case: SqlTestCase {
                name: "unknown_udf_does_not_resolve_arguments",
                description: "Unknown UDF should report unknown function before resolving arguments.",
                setup_sqls: &[],
                sql: "missing_udf(missing_column)",
            },
            adapter: unknown_adapter,
            cached_udf: None,
            aliases: &[],
        },
        UdfGoldenCase {
            case: SqlTestCase {
                name: "forbid_udf_reports_unknown_function",
                description: "When UDFs are forbidden, a matching UDF definition should still be reported as unknown.",
                setup_sqls: &[],
                sql: "blocked_udf(number)",
            },
            adapter: blocked_adapter,
            cached_udf: None,
            aliases: &[],
        },
        UdfGoldenCase {
            case: SqlTestCase {
                name: "cached_udf_resolves_definition",
                description: "Cached UDF definition should resolve without loading it through the adapter.",
                setup_sqls: &[],
                sql: "cached_udf(number)",
            },
            adapter: cache_adapter,
            cached_udf: Some(cached_udf()),
            aliases: &[],
        },
        UdfGoldenCase {
            case: SqlTestCase {
                name: "lambda_udf_resolves_definition",
                description: "Lambda UDF should resolve its SQL definition with the provided argument.",
                setup_sqls: &[],
                sql: "lambda_one(number)",
            },
            adapter: main_adapter.clone(),
            cached_udf: None,
            aliases: &[],
        },
        UdfGoldenCase {
            case: SqlTestCase {
                name: "lambda_udf_checks_argument_count",
                description: "Lambda UDF should validate parameter count before resolving the body.",
                setup_sqls: &[],
                sql: "lambda_two(number)",
            },
            adapter: main_adapter.clone(),
            cached_udf: None,
            aliases: &[],
        },
        UdfGoldenCase {
            case: SqlTestCase {
                name: "scalar_udf_resolves_definition_and_return_cast",
                description: "Scalar UDF should resolve its SQL definition and cast to declared return type.",
                setup_sqls: &[],
                sql: "scalar_to_string(number)",
            },
            adapter: main_adapter.clone(),
            cached_udf: None,
            aliases: &[],
        },
        UdfGoldenCase {
            case: SqlTestCase {
                name: "lambda_udf_definition_ignores_alias_scope",
                description: "Lambda UDF definition should resolve against its own parameters even when call-site aliases use the same names.",
                setup_sqls: &[],
                sql: "scope_probe(delta, [number])",
            },
            adapter: main_adapter.clone(),
            cached_udf: None,
            aliases: &[("x", "number"), ("arr", "text"), ("y", "delta")],
        },
        UdfGoldenCase {
            case: SqlTestCase {
                name: "lambda_udf_definition_registers_parameter_metadata",
                description: "Lambda UDF parameters should be present in the isolated metadata used while resolving the definition.",
                setup_sqls: &[],
                sql: "variant_probe((number, delta))",
            },
            adapter: main_adapter.clone(),
            cached_udf: None,
            aliases: &[],
        },
        UdfGoldenCase {
            case: SqlTestCase {
                name: "server_udf_requires_config_enablement",
                description: "Server UDF should remain rejected when UDF server support is disabled.",
                setup_sqls: &[],
                sql: "server_udf(number)",
            },
            adapter: main_adapter.clone(),
            cached_udf: None,
            aliases: &[],
        },
        UdfGoldenCase {
            case: SqlTestCase {
                name: "script_udf_loads_code",
                description: "Script UDF should build a runtime UDF call from script metadata.",
                setup_sqls: &[],
                sql: "script_udf(text)",
            },
            adapter: main_adapter.clone(),
            cached_udf: None,
            aliases: &[],
        },
        UdfGoldenCase {
            case: SqlTestCase {
                name: "python_script_udf_cloud_path_uses_adapter_config",
                description: "Python sandbox UDF should resolve through the adapter-provided cloud enablement.",
                setup_sqls: &[],
                sql: "cloud_udf(number)",
            },
            adapter: cloud_adapter,
            cached_udf: None,
            aliases: &[],
        },
        UdfGoldenCase {
            case: SqlTestCase {
                name: "udaf_script_loads_code",
                description: "UDAF script should build a runtime aggregate UDF call from script metadata.",
                setup_sqls: &[],
                sql: "udaf_script(text)",
            },
            adapter: main_adapter.clone(),
            cached_udf: None,
            aliases: &[],
        },
        UdfGoldenCase {
            case: SqlTestCase {
                name: "scalar_udf_filter_rejected_after_resolution",
                description: "FILTER on a scalar UDF should still report the aggregate-only error, but only after UDF resolution.",
                setup_sqls: &[],
                sql: "scalar_to_string(number) FILTER (WHERE flag)",
            },
            adapter: main_adapter.clone(),
            cached_udf: None,
            aliases: &[],
        },
        UdfGoldenCase {
            case: SqlTestCase {
                name: "udaf_script_filter_reports_unimplemented",
                description: "FILTER on a UDAF should be recognized as an aggregate call and reported as not yet supported rather than aggregate-only.",
                setup_sqls: &[],
                sql: "udaf_script(text) FILTER (WHERE flag)",
            },
            adapter: main_adapter,
            cached_udf: None,
            aliases: &[],
        },
    ];

    run_udf_golden_cases(&cases).await
}

fn aliases_from_columns(
    bind_context: &BindContext,
    aliases: &[(&str, &str)],
) -> Vec<(String, ScalarExpr)> {
    aliases
        .iter()
        .map(|(alias, column_name)| {
            let column = bind_context
                .all_column_bindings()
                .iter()
                .find(|column| column.column_name == *column_name)
                .unwrap_or_else(|| panic!("missing test column {column_name}"))
                .clone();
            (
                (*alias).to_string(),
                BoundColumnRef { span: None, column }.into(),
            )
        })
        .collect()
}

#[tokio::test(flavor = "current_thread")]
async fn test_udf_definition_cache_is_used_before_adapter_lookup() -> Result<()> {
    let udf_adapter = TestUdfAdapter::default();
    let adapter = TestTypeCheckAdapter::new(test_settings()).with_udf_adapter(udf_adapter.clone());
    let mut bind_context = test_bind_context(ExprContext::Unknown);
    bind_context
        .udf_cache
        .write()
        .insert("cached_udf".to_string(), Some(cached_udf()));

    resolve_type_check_sql("cached_udf(number)", adapter, &mut bind_context)?;

    assert_eq!(udf_adapter.definition_load_count(), 0);
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn test_forbid_udf_skips_adapter_lookup() -> Result<()> {
    let udf =
        UserDefinedFunction::create_lambda_udf("blocked_udf", vec!["x".to_string()], "x + 1", "");
    let (adapter, udf_adapter) = adapter_with_udfs([udf]);
    let adapter = adapter.with_forbid_udf(true);
    let mut bind_context = test_bind_context(ExprContext::Unknown);

    resolve_type_check_sql("blocked_udf(number)", adapter, &mut bind_context)
        .expect_err("forbidden UDF should be reported as unknown");

    assert_eq!(udf_adapter.definition_load_count(), 0);
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn test_unknown_udf_does_not_resolve_arguments() -> Result<()> {
    let (adapter, udf_adapter) = adapter_with_udfs([]);
    let mut bind_context = test_bind_context(ExprContext::Unknown);

    resolve_type_check_sql("missing_udf(missing_column)", adapter, &mut bind_context)
        .expect_err("unknown UDF should fail before resolving arguments");

    assert_eq!(udf_adapter.definition_load_count(), 1);
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn test_server_udf_requires_config_enablement() -> Result<()> {
    let (adapter, _) = adapter_with_udfs([server_udf()]);
    let result_cache = adapter.clone();
    let mut bind_context = test_bind_context(ExprContext::Unknown);

    resolve_type_check_sql("server_udf(number)", adapter, &mut bind_context)
        .expect_err("server UDF should be rejected when enable_udf_server is false");

    assert!(!bind_context.have_udf_server);
    assert!(!result_cache.result_cache_uncacheable());
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn test_script_udf_loads_code_and_marks_runtime_state() -> Result<()> {
    let (adapter, udf_adapter) = adapter_with_udfs([script_udf()]);
    let result_cache = adapter.clone();
    let mut bind_context = test_bind_context(ExprContext::Unknown);

    resolve_type_check_sql("script_udf(text)", adapter, &mut bind_context)?;

    assert_eq!(udf_adapter.code_load_count(), 1);
    assert_eq!(udf_adapter.stage_load_count(), 1);
    assert!(bind_context.have_udf_script);
    assert!(result_cache.result_cache_uncacheable());
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn test_python_script_udf_cloud_path_uses_adapter_config() -> Result<()> {
    let udf_adapter = TestUdfAdapter::with_definitions([cloud_udf()]).with_enable_udf_sandbox();
    let adapter = TestTypeCheckAdapter::new(test_settings()).with_udf_adapter(udf_adapter.clone());
    let result_cache = adapter.clone();
    let mut bind_context = test_bind_context(ExprContext::Unknown);

    resolve_type_check_sql("cloud_udf(number)", adapter, &mut bind_context)?;

    assert_eq!(udf_adapter.cloud_script_count(), 1);
    assert!(bind_context.have_udf_server);
    assert!(!bind_context.have_udf_script);
    assert!(result_cache.result_cache_uncacheable());
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn test_udaf_script_loads_code_and_marks_runtime_state() -> Result<()> {
    let (adapter, udf_adapter) = adapter_with_udfs([udaf_script("udaf_script")]);
    let result_cache = adapter.clone();
    let mut bind_context = test_bind_context(ExprContext::Unknown);

    resolve_type_check_sql("udaf_script(text)", adapter, &mut bind_context)?;

    assert_eq!(udf_adapter.code_load_count(), 1);
    assert_eq!(udf_adapter.stage_load_count(), 1);
    assert!(bind_context.have_udf_script);
    assert!(result_cache.result_cache_uncacheable());
    Ok(())
}
