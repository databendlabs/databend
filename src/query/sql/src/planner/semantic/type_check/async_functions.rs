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
use databend_common_ast::ast::Expr;
use databend_common_catalog::table_context::TableContext;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::types::DataType;
use databend_common_expression::types::NumberDataType;
use databend_common_meta_app::principal::StageInfo;
use databend_common_meta_app::principal::StageType;
use databend_common_meta_app::schema::DictionaryIdentity;
use databend_common_meta_app::schema::GetSequenceReq;
use databend_common_meta_app::schema::SequenceIdent;
use databend_common_meta_app::schema::dictionary_name_ident::DictionaryNameIdent;
use databend_common_users::Object;

use super::TypeChecker;
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

impl<'a> TypeChecker<'a> {
    pub(super) fn resolve_async_function(
        &mut self,
        span: Span,
        func_name: &str,
        arguments: &[&Expr],
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
            "nextval" => self.resolve_nextval_async_function(span, func_name, arguments)?,
            "dict_get" => self.resolve_dict_get_async_function(span, func_name, arguments)?,
            "read_file" => self.resolve_read_file_async_function(span, func_name, arguments)?,
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
        arguments: &[&Expr],
    ) -> Result<Box<(ScalarExpr, DataType)>> {
        if arguments.len() != 1 {
            return Err(ErrorCode::SemanticError(format!(
                "nextval function need one argument but got {}",
                arguments.len()
            ))
            .set_span(span));
        }
        let (sequence_name, display_name) = if let Expr::ColumnRef { column, .. } = arguments[0] {
            if column.database.is_some() || column.table.is_some() {
                return Err(ErrorCode::SemanticError(
                    "nextval function argument identifier should only contain one part".to_string(),
                )
                .set_span(span));
            }
            match &column.column {
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
        } else {
            return Err(ErrorCode::SemanticError(format!(
                "nextval function argument don't support expr {}",
                arguments[0]
            ))
            .set_span(span));
        };

        if !self.skip_sequence_check {
            let catalog = self.ctx.get_default_catalog()?;
            let req = GetSequenceReq {
                ident: SequenceIdent::new(self.ctx.get_tenant(), sequence_name.clone()),
            };

            let visibility_checker = if self
                .ctx
                .get_settings()
                .get_enable_experimental_sequence_privilege_check()?
            {
                Some(databend_common_base::runtime::block_on(async move {
                    self.ctx
                        .get_visibility_checker(false, Object::Sequence)
                        .await
                })?)
            } else {
                None
            };
            databend_common_base::runtime::block_on(
                catalog.get_sequence(req, &visibility_checker),
            )?;
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
        span: Span,
        func_name: &str,
        args: &[&Expr],
    ) -> Result<Box<(ScalarExpr, DataType)>> {
        if args.len() != 3 {
            return Err(ErrorCode::SemanticError(format!(
                "dict_get function need three arguments but got {}",
                args.len()
            ))
            .set_span(span));
        }
        let tenant = self.ctx.get_tenant();
        let catalog = self.ctx.get_default_catalog()?;

        let dict_name_arg = args[0];
        let field_arg = args[1];
        let key_arg = args[2];

        // Get dict_name and dict_meta.
        let (db_name, dict_name) = if let Expr::ColumnRef { column, .. } = dict_name_arg {
            if column.database.is_some() {
                return Err(ErrorCode::SemanticError(
                    "dict_get function argument identifier should contain one or two parts"
                        .to_string(),
                )
                .set_span(dict_name_arg.span()));
            }
            let db_name = match &column.table {
                Some(ident) => normalize_identifier(ident, self.name_resolution_ctx).name,
                None => self.ctx.get_current_database(),
            };
            let dict_name = match &column.column {
                ColumnID::Name(ident) => normalize_identifier(ident, self.name_resolution_ctx).name,
                ColumnID::Position(pos) => {
                    return Err(ErrorCode::SemanticError(format!(
                        "dict_get function argument don't support identifier {}",
                        pos
                    ))
                    .set_span(dict_name_arg.span()));
                }
            };
            (db_name, dict_name)
        } else {
            return Err(ErrorCode::SemanticError(
                "async function can only used as column".to_string(),
            )
            .set_span(dict_name_arg.span()));
        };
        let db = databend_common_base::runtime::block_on(
            catalog.get_database(&tenant, db_name.as_str()),
        )?;
        let db_id = db.get_db_info().database_id.db_id;
        let req = DictionaryNameIdent::new(
            tenant.clone(),
            DictionaryIdentity::new(db_id, dict_name.clone()),
        );
        let reply = databend_common_base::runtime::block_on(catalog.get_dictionary(req))?;
        let dictionary = if let Some(r) = reply {
            r.dictionary_meta
        } else {
            return Err(ErrorCode::UnknownDictionary(format!(
                "Unknown dictionary {}",
                dict_name,
            )));
        };

        // Get attr_name, attr_type and return_type.
        let box (field_scalar, _field_data_type) = self.resolve(field_arg)?;
        let Ok(field_expr) = ConstantExpr::try_from(field_scalar.clone()) else {
            return Err(ErrorCode::SemanticError(format!(
                "invalid arguments for dict_get function, attr_name must be a constant string, but got {}",
                field_arg
            ))
            .set_span(field_scalar.span()));
        };
        let Some(attr_name) = field_expr.value.as_string() else {
            return Err(ErrorCode::SemanticError(format!(
                "invalid arguments for dict_get function, attr_name must be a constant string, but got {}",
                field_arg
            ))
            .set_span(field_scalar.span()));
        };
        let attr_field = dictionary.schema.field_with_name(attr_name)?;
        let attr_type: DataType = (&attr_field.data_type).into();
        let default_value = DefaultExprBinder::try_new(self.ctx.clone())?.get_scalar(attr_field)?;

        // Get primary_key_value and check type.
        let primary_column_id = dictionary.primary_column_ids[0];
        let primary_field = dictionary.schema.field_of_column_id(primary_column_id)?;
        let primary_type: DataType = (&primary_field.data_type).into();

        let mut args = Vec::with_capacity(1);
        let box (key_scalar, key_type) = self.resolve(key_arg)?;

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
            func_name, db_name, dict_name, field_arg, key_arg,
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
        span: Span,
        func_name: &str,
        args: &[&Expr],
    ) -> Result<Box<(ScalarExpr, DataType)>> {
        if args.len() != 1 && args.len() != 2 {
            return Err(ErrorCode::SemanticError(format!(
                "read_file function need one or two arguments but got {}",
                args.len()
            ))
            .set_span(span));
        }

        let mut resolved_args = Vec::with_capacity(args.len());
        let mut arg_types = Vec::with_capacity(args.len());
        for arg in args {
            let box (arg_scalar, arg_type) = self.resolve(arg)?;
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
                let stage_info =
                    Self::resolve_stage_info_for_read_file(self.ctx.as_ref(), span, &stage_name)?;
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
            format!("{}({})", func_name, args[0])
        } else {
            format!("{}({}, {})", func_name, args[0], args[1])
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
        ctx: &dyn TableContext,
        span: Span,
        stage_name: &str,
    ) -> Result<StageInfo> {
        databend_common_base::runtime::block_on(async move {
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
        })
    }
}
