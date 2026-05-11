// Copyright 2021 Datafuse Labs
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::io::Write;
use std::sync::Arc;

use databend_common_ast::Span;
use databend_common_ast::parser::parse_expr;
use databend_common_ast::parser::tokenize_sql;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::FunctionContext;
use databend_common_expression::Scalar;
use databend_common_expression::types::DataType;
use databend_common_expression::types::NumberDataType;
use databend_common_functions::aggregates::AggregateFunctionFactory;
use databend_common_meta_app::principal::StageInfo;
use databend_common_meta_app::principal::UserDefinedFunction;
use databend_common_meta_app::principal::UserInfo;
use databend_common_meta_app::tenant::Tenant;
use databend_common_settings::Settings;
use databend_common_sql::AuthFunction;
use databend_common_sql::BindContext;
use databend_common_sql::ColumnBindingBuilder;
use databend_common_sql::Metadata;
use databend_common_sql::NameResolutionContext;
use databend_common_sql::NamespaceFunction;
use databend_common_sql::SessionFunction;
use databend_common_sql::Symbol;
use databend_common_sql::TypeCheckAdapter;
use databend_common_sql::TypeCheckDictionary;
use databend_common_sql::TypeChecker;
use databend_common_sql::Visibility;
use databend_common_sql::binder::ExprContext;
use databend_common_sql::format_scalar;
use databend_common_sql::plans::DictGetFunctionArgument;
use databend_common_sql::plans::DictionarySource;
use databend_common_sql::plans::RedisSource;
use parking_lot::RwLock;

use crate::framework::golden::SqlTestCase;
use crate::framework::golden::SqlTestOutcome;
use crate::framework::golden::open_golden_file;
use crate::framework::golden::write_case_header;
use crate::framework::golden::write_case_outcome_body;
use crate::framework::init_testing_globals;

#[derive(Clone)]
struct TestTypeCheckAdapter {
    settings: Arc<Settings>,
    func_ctx: FunctionContext,
}

impl TestTypeCheckAdapter {
    fn new(settings: Arc<Settings>) -> Self {
        Self {
            settings,
            func_ctx: FunctionContext::default(),
        }
    }
}

impl TypeCheckAdapter for TestTypeCheckAdapter {
    fn function_context(&self) -> Result<FunctionContext> {
        Ok(self.func_ctx.clone())
    }

    fn settings(&self) -> Arc<Settings> {
        self.settings.clone()
    }

    fn aggregate_function_factory(&self) -> &'static AggregateFunctionFactory {
        AggregateFunctionFactory::instance()
    }

    fn validate_sequence(&self, sequence_name: &str) -> Result<()> {
        if sequence_name == "seq" {
            Ok(())
        } else {
            Err(ErrorCode::SemanticError(format!(
                "unknown mock sequence {sequence_name}"
            )))
        }
    }

    fn resolve_dictionary(
        &self,
        db_name: Option<&str>,
        dict_name: &str,
        attr_name: &str,
    ) -> Result<TypeCheckDictionary> {
        if dict_name != "dict" {
            return Err(ErrorCode::SemanticError(format!(
                "unknown mock dictionary {dict_name}"
            )));
        }
        if attr_name != "field" {
            return Err(ErrorCode::SemanticError(format!(
                "unknown mock dictionary attribute {attr_name}"
            )));
        }
        Ok(TypeCheckDictionary {
            db_name: db_name.unwrap_or("default").to_string(),
            attr_type: DataType::String,
            primary_type: DataType::Number(NumberDataType::Int64),
            func_arg: DictGetFunctionArgument {
                dict_source: DictionarySource::Redis(RedisSource {
                    host: "127.0.0.1".to_string(),
                    port: 6379,
                    username: None,
                    password: None,
                    db_index: None,
                }),
                default_value: Scalar::String(String::new()),
            },
        })
    }

    fn resolve_read_file_stage_info(&self, _span: Span, stage_name: &str) -> Result<StageInfo> {
        Ok(StageInfo::new_user_stage(stage_name))
    }

    fn resolve_udf(&self, _udf_name: &str) -> Result<Option<UserDefinedFunction>> {
        Ok(None)
    }

    fn resolve_namespace_function(&self, function: NamespaceFunction) -> Result<Scalar> {
        match function {
            NamespaceFunction::CurrentCatalog | NamespaceFunction::CurrentDatabase => {
                Ok(Scalar::String("default".to_string()))
            }
        }
    }

    fn resolve_session_function(&self, function: SessionFunction<'_>) -> Result<Scalar> {
        match function {
            SessionFunction::Version => Ok(Scalar::String(String::new())),
            SessionFunction::ConnectionId => Ok(Scalar::String("lite-conn".to_string())),
            SessionFunction::ClientSessionId => Ok(Scalar::String(String::new())),
            SessionFunction::LastQueryId(_) | SessionFunction::Variable(_) => Ok(Scalar::Null),
        }
    }

    fn resolve_authorization_function(&self, function: AuthFunction) -> Result<Scalar> {
        match function {
            AuthFunction::CurrentUser => Ok(Scalar::String(
                UserInfo::new_no_auth("root", "%")
                    .identity()
                    .display()
                    .to_string(),
            )),
            AuthFunction::CurrentRole => Ok(Scalar::String(String::new())),
            AuthFunction::CurrentSecondaryRoles => Ok(Scalar::String(
                serde_json::json!({ "roles": "", "value": "ALL" }).to_string(),
            )),
            AuthFunction::CurrentAvailableRoles => Ok(Scalar::String("[]".to_string())),
        }
    }

    fn set_result_cache_uncacheable(&self) {}
}

fn add_test_column(bind_context: &mut BindContext, index: usize, name: &str, data_type: DataType) {
    bind_context.add_column_binding(
        ColumnBindingBuilder::new(
            name.to_string(),
            Symbol::new(index),
            Box::new(data_type.wrap_nullable()),
            Visibility::Visible,
        )
        .build(),
    );
}

async fn type_check_case_in_context(
    case: &SqlTestCase,
    expr_context: ExprContext,
) -> Result<SqlTestOutcome> {
    init_testing_globals();
    assert!(
        case.setup_sqls.is_empty(),
        "type_check tests use a dependency-only adapter and do not run setup SQL"
    );
    let settings = Settings::create(Tenant::new_literal("default"));
    let adapter = TestTypeCheckAdapter::new(settings);
    let tokens = tokenize_sql(case.sql)?;
    let dialect = adapter.settings().get_sql_dialect()?;
    let expr = parse_expr(&tokens, dialect)?;

    let name_resolution_ctx = NameResolutionContext::try_from(adapter.settings().as_ref())?;
    let mut bind_context = BindContext::new();
    add_test_column(
        &mut bind_context,
        0,
        "number",
        DataType::Number(NumberDataType::Int64),
    );
    add_test_column(&mut bind_context, 1, "text", DataType::String);
    add_test_column(&mut bind_context, 2, "pattern", DataType::String);
    add_test_column(
        &mut bind_context,
        3,
        "delta",
        DataType::Number(NumberDataType::Int32),
    );
    add_test_column(&mut bind_context, 4, "flag", DataType::Boolean);
    add_test_column(&mut bind_context, 5, "ts", DataType::Timestamp);
    add_test_column(&mut bind_context, 6, "date", DataType::Date);
    bind_context.expr_context = expr_context;
    let metadata = Arc::new(RwLock::new(Metadata::default()));
    let mut type_checker = TypeChecker::try_create_with_adapter(
        &mut bind_context,
        adapter,
        &name_resolution_ctx,
        metadata,
        &[],
    )?;

    let outcome = match type_checker.resolve(&expr).map(|resolved| *resolved) {
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
    Ok(outcome)
}

async fn run_type_check_cases_in_context(
    file_name: &str,
    cases: &[SqlTestCase],
    expr_context: ExprContext,
) -> Result<()> {
    let mut file = open_golden_file("semantic/type_check", file_name)?;

    for (index, case) in cases.iter().enumerate() {
        if index > 0 {
            writeln!(file)?;
        }
        write_case_header(&mut file, case)?;
        let outcome = type_check_case_in_context(case, expr_context).await?;
        write_case_outcome_body(&mut file, &outcome)?;
    }

    Ok(())
}

async fn run_type_check_cases(file_name: &str, cases: &[SqlTestCase]) -> Result<()> {
    run_type_check_cases_in_context(file_name, cases, ExprContext::Unknown).await
}

mod adapter;
mod aggregate_window;
mod core_lowering;
mod datetime;
mod literals_collections;
mod rewrite_functions;
mod rules;
mod scalar_rewrites;
mod search;
mod special_functions;
mod string_like;
