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
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::types::DataType;
use databend_common_expression::types::NumberDataType;
use databend_common_functions::ASYNC_FUNCTIONS;
use unicase::Ascii;

use super::TypeChecker;
use super::core_expr::CoreExpr;
use super::core_expr::CoreExprArena;
use super::core_expr::CoreExprId;
use super::core_expr::CoreSearchFunctionArgs;
use crate::binder::ExprContext;
use crate::binder::parse_stage_name;
use crate::binder::wrap_cast;
use crate::planner::semantic::normalize_identifier;
use crate::plans::AsyncFunctionArgument;
use crate::plans::AsyncFunctionCall;
use crate::plans::ConstantExpr;
use crate::plans::ReadFileFunctionArgument;
use crate::plans::ScalarExpr;

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

        self.adapter.validate_sequence(&sequence_name)?;

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
        // Get dict_name and dict_meta.
        let (db_name, dict_name) = {
            if dictionary.database.is_some() {
                return Err(ErrorCode::SemanticError(
                    "dict_get function argument identifier should contain one or two parts"
                        .to_string(),
                )
                .set_span(span));
            }
            let db_name = dictionary
                .table
                .as_ref()
                .map(|ident| normalize_identifier(ident, self.name_resolution_ctx).name);
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
        let dictionary =
            self.adapter
                .resolve_dictionary(db_name.as_deref(), &dict_name, attr_name)?;

        let mut args = Vec::with_capacity(1);
        let box (key_scalar, key_type) = self.resolve_core(arena, *key)?;

        if dictionary.primary_type != key_type.remove_nullable() {
            args.push(wrap_cast(&key_scalar, &dictionary.primary_type));
        } else {
            args.push(key_scalar);
        }
        let display_name = format!(
            "{}({}.{}, {}, {})",
            func_name, dictionary.db_name, dict_name, field_display, key_display,
        );
        Ok(Box::new((
            ScalarExpr::AsyncFunctionCall(AsyncFunctionCall {
                span,
                func_name: func_name.to_string(),
                display_name,
                return_type: Box::new(dictionary.attr_type.clone()),
                arguments: args,
                func_arg: AsyncFunctionArgument::DictGetFunction(dictionary.func_arg),
            }),
            dictionary.attr_type,
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
                let stage_info = self
                    .adapter
                    .resolve_read_file_stage_info(span, &stage_name)?;
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
}
