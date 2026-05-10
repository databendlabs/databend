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

use databend_common_ast::Span;
use databend_common_ast::ast::ColumnID;
use databend_common_ast::ast::ColumnRef;
use databend_common_ast::ast::Expr;
use databend_common_catalog::table_context::TableContext;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::types::DataType;
use databend_common_expression::types::NumberDataType;
use databend_common_functions::ASYNC_FUNCTIONS;
use databend_common_meta_app::principal::StageInfo;
use databend_common_meta_app::principal::StageType;
use databend_common_meta_app::schema::DictionaryIdentity;
use databend_common_meta_app::schema::GetSequenceReq;
use databend_common_meta_app::schema::SequenceIdent;
use databend_common_meta_app::schema::dictionary_name_ident::DictionaryNameIdent;
use databend_common_users::Object;
use unicase::Ascii;

use super::TypeChecker;
use super::core_expr::CoreExpr;
use super::core_expr::CoreExprArena;
use super::core_expr::CoreExprId;
use super::core_expr::CoreSearchFunctionArgs;
use crate::DefaultExprBinder;
use crate::binder::ExprContext;
use crate::binder::parse_stage_name;
use crate::binder::resolve_stage_location;
use crate::binder::wrap_cast;
use crate::planner::semantic::normalize_identifier;
use crate::plans::AsyncFunctionArgument;
use crate::plans::AsyncFunctionCall;
use crate::plans::ConstantExpr;
use crate::plans::DictGetFunctionArgument;
use crate::plans::DictionarySource;
use crate::plans::ReadFileFunctionArgument;
use crate::plans::RedisSource;
use crate::plans::ScalarExpr;
use crate::plans::SqlSource;

pub(super) enum CoreAsyncFunction<'a> {
    NextVal {
        sequence: &'a ColumnRef,
    },
    DictGet {
        dictionary: &'a ColumnRef,
        field: CoreExprId,
        field_display: String,
        key: CoreExprId,
        key_display: String,
    },
    Args {
        args: CoreSearchFunctionArgs,
    },
}

impl<'a> CoreExprArena<'a> {
    pub(super) fn async_function(
        &mut self,
        span: Span,
        func_name: &'static str,
        args: &'a [Expr],
    ) -> Result<CoreExprId> {
        let function = match func_name {
            "nextval" => {
                let [Expr::ColumnRef { column, .. }] = args else {
                    let args = self.lower_display_expr_args(args)?;
                    return Ok(self.alloc(CoreExpr::AsyncFunction {
                        span,
                        func_name,
                        function: CoreAsyncFunction::Args { args },
                    }));
                };
                CoreAsyncFunction::NextVal { sequence: column }
            }
            "dict_get" => {
                let [Expr::ColumnRef { column, .. }, field, key] = args else {
                    let args = self.lower_display_expr_args(args)?;
                    return Ok(self.alloc(CoreExpr::AsyncFunction {
                        span,
                        func_name,
                        function: CoreAsyncFunction::Args { args },
                    }));
                };
                CoreAsyncFunction::DictGet {
                    dictionary: column,
                    field: self.lower_ast_expr(field)?,
                    field_display: format!("{:#}", field),
                    key: self.lower_ast_expr(key)?,
                    key_display: format!("{:#}", key),
                }
            }
            "read_file" => CoreAsyncFunction::Args {
                args: self.lower_display_expr_args(args)?,
            },
            _ => {
                return Err(ErrorCode::Internal(format!(
                    "async function {func_name} should have been classified before lowering",
                )));
            }
        };
        Ok(self.alloc(CoreExpr::AsyncFunction {
            span,
            func_name,
            function,
        }))
    }
}

pub(super) fn async_function_name(func_name: &str) -> Option<&'static str> {
    let func_name = Ascii::new(func_name);
    ASYNC_FUNCTIONS
        .iter()
        .cloned()
        .find(|name| *name == func_name)
        .map(Ascii::into_inner)
}

impl<'a, A> TypeChecker<'a, A>
where A: super::TypeCheckAdapter
{
    pub(super) fn resolve_async_function(
        &mut self,
        arena: &CoreExprArena<'_>,
        span: Span,
        func_name: &str,
        function: &CoreAsyncFunction<'_>,
    ) -> Result<Box<(ScalarExpr, DataType)>> {
        if matches!(self.bind_context.expr_context, ExprContext::InAsyncFunction) {
            return Err(
                ErrorCode::SemanticError("async functions cannot be nested".to_string())
                    .set_span(span),
            );
        }
        let original_context = self
            .bind_context
            .replace_expr_context(ExprContext::InAsyncFunction);
        let result = match func_name {
            "nextval" => self.resolve_nextval_async_function(span, func_name, function)?,
            "dict_get" => self.resolve_dict_get_async_function(arena, span, func_name, function)?,
            "read_file" => {
                self.resolve_read_file_async_function(arena, span, func_name, function)?
            }
            _ => {
                return Err(ErrorCode::SemanticError(format!(
                    "cannot find async function {}",
                    func_name
                ))
                .set_span(span));
            }
        };
        // Restore the original context
        self.bind_context.expr_context = original_context;
        self.bind_context.have_async_func = true;
        Ok(result)
    }

    fn resolve_nextval_async_function(
        &mut self,
        span: Span,
        func_name: &str,
        function: &CoreAsyncFunction<'_>,
    ) -> Result<Box<(ScalarExpr, DataType)>> {
        let CoreAsyncFunction::NextVal { sequence } = function else {
            return Err(ErrorCode::SemanticError(format!(
                "nextval function argument don't support expr"
            ))
            .set_span(span));
        };
        let (sequence_name, display_name) = {
            if sequence.database.is_some() || sequence.table.is_some() {
                return Err(ErrorCode::SemanticError(
                    "nextval function argument identifier should only contain one part".to_string(),
                )
                .set_span(span));
            }
            match &sequence.column {
                ColumnID::Name(ident) => {
                    let ident = normalize_identifier(ident, self.name_resolution_ctx);
                    (ident.name.to_string(), format!("{}({})", func_name, ident))
                }
                ColumnID::Position(pos) => {
                    return Err(ErrorCode::SemanticError(format!(
                        "nextval function argument don't support identifier {}",
                        pos
                    ))
                    .set_span(span));
                }
            }
        };

        if !self.adapter.skip_sequence_check() {
            let catalog = self.adapter.table_context().get_default_catalog()?;
            let req = GetSequenceReq {
                ident: SequenceIdent::new(
                    self.adapter.table_context().get_tenant(),
                    sequence_name.clone(),
                ),
            };

            let visibility_checker = if self
                .adapter
                .table_context()
                .get_settings()
                .get_enable_experimental_sequence_privilege_check()?
            {
                let ctx = self.adapter.table_context().clone();
                Some(self.block_on(async move {
                    ctx.get_visibility_checker(false, Object::Sequence).await
                })??)
            } else {
                None
            };
            self.block_on(catalog.get_sequence(req, &visibility_checker))??;
        }

        let return_type = DataType::Number(NumberDataType::UInt64);
        let func_arg = AsyncFunctionArgument::SequenceFunction(sequence_name);

        let async_func = AsyncFunctionCall {
            span,
            func_name: func_name.to_string(),
            display_name,
            return_type: Box::new(return_type.clone()),
            arguments: vec![],
            func_arg,
        };

        Ok(Box::new((async_func.into(), return_type)))
    }

    fn resolve_dict_get_async_function(
        &mut self,
        arena: &CoreExprArena<'_>,
        span: Span,
        func_name: &str,
        function: &CoreAsyncFunction<'_>,
    ) -> Result<Box<(ScalarExpr, DataType)>> {
        let CoreAsyncFunction::DictGet {
            dictionary,
            field,
            field_display,
            key,
            key_display,
        } = function
        else {
            return Err(ErrorCode::SemanticError(
                "async function can only used as column".to_string(),
            )
            .set_span(span));
        };
        let tenant = self.adapter.table_context().get_tenant();
        let catalog = self.adapter.table_context().get_default_catalog()?;

        // Get dict_name and dict_meta.
        let (db_name, dict_name) = {
            if dictionary.database.is_some() {
                return Err(ErrorCode::SemanticError(
                    "dict_get function argument identifier should contain one or two parts"
                        .to_string(),
                )
                .set_span(span));
            }
            let db_name = match &dictionary.table {
                Some(ident) => normalize_identifier(ident, self.name_resolution_ctx).name,
                None => self.adapter.table_context().get_current_database(),
            };
            let dict_name = match &dictionary.column {
                ColumnID::Name(ident) => normalize_identifier(ident, self.name_resolution_ctx).name,
                ColumnID::Position(pos) => {
                    return Err(ErrorCode::SemanticError(format!(
                        "dict_get function argument don't support identifier {}",
                        pos
                    ))
                    .set_span(span));
                }
            };
            (db_name, dict_name)
        };
        let db = self.block_on(catalog.get_database(&tenant, db_name.as_str()))??;
        let db_id = db.get_db_info().database_id.db_id;
        let req = DictionaryNameIdent::new(
            tenant.clone(),
            DictionaryIdentity::new(db_id, dict_name.clone()),
        );
        let reply = self.block_on(catalog.get_dictionary(req))??;
        let dictionary = if let Some(r) = reply {
            r.dictionary_meta
        } else {
            return Err(ErrorCode::UnknownDictionary(format!(
                "Unknown dictionary {}",
                dict_name,
            )));
        };

        // Get attr_name, attr_type and return_type.
        let box (field_scalar, _field_data_type) = self.resolve_core(arena, *field)?;
        let Ok(field_expr) = ConstantExpr::try_from(field_scalar.clone()) else {
            return Err(ErrorCode::SemanticError(format!(
                "invalid arguments for dict_get function, attr_name must be a constant string, but got {}",
                field_display
            ))
            .set_span(field_scalar.span()));
        };
        let Some(attr_name) = field_expr.value.as_string() else {
            return Err(ErrorCode::SemanticError(format!(
                "invalid arguments for dict_get function, attr_name must be a constant string, but got {}",
                field_display
            ))
            .set_span(field_scalar.span()));
        };
        let attr_field = dictionary.schema.field_with_name(attr_name)?;
        let attr_type: DataType = (&attr_field.data_type).into();
        let default_value = DefaultExprBinder::try_new(self.adapter.table_context().clone())?
            .get_scalar(attr_field)?;

        // Get primary_key_value and check type.
        let primary_column_id = dictionary.primary_column_ids[0];
        let primary_field = dictionary.schema.field_of_column_id(primary_column_id)?;
        let primary_type: DataType = (&primary_field.data_type).into();

        let mut args = Vec::with_capacity(1);
        let box (key_scalar, key_type) = self.resolve_core(arena, *key)?;

        if primary_type != key_type.remove_nullable() {
            args.push(wrap_cast(&key_scalar, &primary_type));
        } else {
            args.push(key_scalar);
        }
        let dict_source = match dictionary.source.as_str() {
            "mysql" => {
                let connection_url = dictionary.build_sql_connection_url()?;
                let table = dictionary
                    .options
                    .get("table")
                    .ok_or_else(|| ErrorCode::BadArguments("Miss option `table`"))?;
                DictionarySource::Mysql(SqlSource {
                    connection_url,
                    table: table.to_string(),
                    key_field: primary_field.name.clone(),
                    value_field: attr_field.name.clone(),
                })
            }
            "redis" => {
                let host = dictionary
                    .options
                    .get("host")
                    .ok_or_else(|| ErrorCode::BadArguments("Miss option `host`"))?;
                let port_str = dictionary
                    .options
                    .get("port")
                    .ok_or_else(|| ErrorCode::BadArguments("Miss option `port`"))?;
                let port = port_str
                    .parse()
                    .expect("Failed to parse String port to u16");
                let username = dictionary.options.get("username").cloned();
                let password = dictionary.options.get("password").cloned();
                let db_index = dictionary
                    .options
                    .get("db_index")
                    .map(|i| i.parse::<i64>().unwrap());
                DictionarySource::Redis(RedisSource {
                    host: host.to_string(),
                    port,
                    username,
                    password,
                    db_index,
                })
            }
            _ => {
                return Err(ErrorCode::Unimplemented(format!(
                    "Unsupported source {}",
                    dictionary.source
                )));
            }
        };

        let dict_get_func_arg = DictGetFunctionArgument {
            dict_source,
            default_value,
        };
        let display_name = format!(
            "{}({}.{}, {}, {})",
            func_name, db_name, dict_name, field_display, key_display,
        );
        Ok(Box::new((
            ScalarExpr::AsyncFunctionCall(AsyncFunctionCall {
                span,
                func_name: func_name.to_string(),
                display_name,
                return_type: Box::new(attr_type.clone()),
                arguments: args,
                func_arg: AsyncFunctionArgument::DictGetFunction(dict_get_func_arg),
            }),
            attr_type,
        )))
    }

    fn resolve_read_file_async_function(
        &mut self,
        arena: &CoreExprArena<'_>,
        span: Span,
        func_name: &str,
        function: &CoreAsyncFunction<'_>,
    ) -> Result<Box<(ScalarExpr, DataType)>> {
        let CoreAsyncFunction::Args { args } = function else {
            return Err(ErrorCode::SemanticError(format!(
                "read_file function need one or two arguments but got 0"
            ))
            .set_span(span));
        };
        if args.len() != 1 && args.len() != 2 {
            return Err(ErrorCode::SemanticError(format!(
                "read_file function need one or two arguments but got {}",
                args.len()
            ))
            .set_span(span));
        }

        let mut resolved_args = Vec::with_capacity(args.len());
        let mut arg_types = Vec::with_capacity(args.len());
        for (_, arg) in args {
            let box (arg_scalar, arg_type) = self.resolve_core(arena, *arg)?;
            let arg_scalar = if arg_type.remove_nullable() != DataType::String {
                wrap_cast(&arg_scalar, &DataType::String)
            } else {
                arg_scalar
            };
            resolved_args.push(arg_scalar);
            arg_types.push(arg_type);
        }

        let mut read_file_arg = ReadFileFunctionArgument {
            stage_name: None,
            stage_info: None,
        };

        if args.len() == 1 {
            if let ScalarExpr::ConstantExpr(constant) = &resolved_args[0] {
                if let Some(location) = constant.value.as_string() {
                    if !location.starts_with('@') {
                        return Err(ErrorCode::SemanticError(format!(
                            "stage path must start with @, but got {}",
                            location
                        ))
                        .set_span(span));
                    }
                }
            }
        } else if let ScalarExpr::ConstantExpr(constant) = &resolved_args[0] {
            if let Some(stage) = constant.value.as_string() {
                let stage_name = parse_stage_name(stage).map_err(|err| {
                    ErrorCode::SemanticError(err.message().to_string()).set_span(span)
                })?;
                let stage_info = self.resolve_stage_info_for_read_file(
                    self.adapter.table_context().as_ref(),
                    span,
                    &stage_name,
                )?;
                read_file_arg.stage_name = Some(stage_name);
                read_file_arg.stage_info = Some(Box::new(stage_info));
            }
        }

        let return_type = if arg_types
            .iter()
            .any(|arg_type| arg_type.is_nullable_or_null())
        {
            DataType::Nullable(Box::new(DataType::Binary))
        } else {
            DataType::Binary
        };

        let display_name = if args.len() == 1 {
            format!("{}({})", func_name, args[0].0)
        } else {
            format!("{}({}, {})", func_name, args[0].0, args[1].0)
        };
        Ok(Box::new((
            ScalarExpr::AsyncFunctionCall(AsyncFunctionCall {
                span,
                func_name: func_name.to_string(),
                display_name,
                return_type: Box::new(return_type.clone()),
                arguments: resolved_args,
                func_arg: AsyncFunctionArgument::ReadFile(read_file_arg),
            }),
            return_type,
        )))
    }

    fn resolve_stage_info_for_read_file(
        &self,
        ctx: &dyn TableContext,
        span: Span,
        stage_name: &str,
    ) -> Result<StageInfo> {
        self.block_on(async move {
            let (stage_info, _) = resolve_stage_location(ctx, stage_name).await?;
            if ctx.get_settings().get_enable_experimental_rbac_check()? {
                let visibility_checker = ctx.get_visibility_checker(false, Object::Stage).await?;
                if !(stage_info.is_temporary
                    || visibility_checker.check_stage_read_visibility(&stage_info.stage_name)
                    || stage_info.stage_type == StageType::User
                        && stage_info.stage_name == ctx.get_current_user()?.name)
                {
                    return Err(ErrorCode::PermissionDenied(format!(
                        "Permission denied: privilege READ is required on stage {} for user {}",
                        stage_info.stage_name.clone(),
                        &ctx.get_current_user()?.identity().display(),
                    ))
                    .set_span(span));
                }
            }
            Ok(stage_info)
        })?
    }
}
