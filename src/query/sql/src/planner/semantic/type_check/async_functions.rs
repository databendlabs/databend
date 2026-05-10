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
use crate::binder::ExprContext;
use crate::binder::parse_stage_name;
use crate::binder::wrap_cast;
use crate::planner::semantic::normalize_identifier;
use crate::plans::AsyncFunctionArgument;
use crate::plans::AsyncFunctionCall;
use crate::plans::ConstantExpr;
use crate::plans::ReadFileFunctionArgument;
use crate::plans::ScalarExpr;

pub(super) struct CoreAsyncFunctionArg {
    expr: CoreExprId,
    display: String,
}

pub(super) struct CoreDictGetFunction<'a> {
    dictionary: &'a ColumnRef,
    field: CoreAsyncFunctionArg,
    key: CoreAsyncFunctionArg,
}

pub(super) struct CoreReadFileFunction {
    stage: Option<CoreAsyncFunctionArg>,
    location: CoreAsyncFunctionArg,
}

pub(super) enum CoreAsyncFunction<'a> {
    NextVal { sequence: &'a ColumnRef },
    DictGet(CoreDictGetFunction<'a>),
    ReadFile(CoreReadFileFunction),
}

impl<'a> CoreExprArena<'a> {
    fn lower_async_function_arg(&mut self, expr: &'a Expr) -> Result<CoreAsyncFunctionArg> {
        Ok(CoreAsyncFunctionArg {
            expr: self.lower_ast_expr(expr)?,
            display: format!("{:#}", expr),
        })
    }

    pub(super) fn async_function(
        &mut self,
        span: Span,
        func_name: &'static str,
        args: &'a [Expr],
    ) -> Result<CoreExprId> {
        let function = match func_name {
            "nextval" => {
                let [Expr::ColumnRef { column, .. }] = args else {
                    return Err(ErrorCode::SemanticError(
                        "nextval function argument don't support expr".to_string(),
                    )
                    .set_span(span));
                };
                CoreAsyncFunction::NextVal { sequence: column }
            }
            "dict_get" => {
                let [Expr::ColumnRef { column, .. }, field, key] = args else {
                    return Err(ErrorCode::SemanticError(
                        "async function can only used as column".to_string(),
                    )
                    .set_span(span));
                };
                CoreAsyncFunction::DictGet(CoreDictGetFunction {
                    dictionary: column,
                    field: self.lower_async_function_arg(field)?,
                    key: self.lower_async_function_arg(key)?,
                })
            }
            "read_file" => match args {
                [location] => CoreAsyncFunction::ReadFile(CoreReadFileFunction {
                    stage: None,
                    location: self.lower_async_function_arg(location)?,
                }),
                [stage, location] => CoreAsyncFunction::ReadFile(CoreReadFileFunction {
                    stage: Some(self.lower_async_function_arg(stage)?),
                    location: self.lower_async_function_arg(location)?,
                }),
                _ => {
                    return Err(ErrorCode::SemanticError(format!(
                        "read_file function need one or two arguments but got {}",
                        args.len()
                    ))
                    .set_span(span));
                }
            },
            _ => {
                return Err(ErrorCode::Internal(format!(
                    "async function {func_name} should have been classified before lowering",
                )));
            }
        };
        Ok(self.alloc(CoreExpr::AsyncFunction { span, function }))
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
        let result = match function {
            CoreAsyncFunction::NextVal { sequence } => {
                self.resolve_nextval_async_function(span, sequence)?
            }
            CoreAsyncFunction::DictGet(function) => {
                self.resolve_dict_get_async_function(arena, span, function)?
            }
            CoreAsyncFunction::ReadFile(function) => {
                self.resolve_read_file_async_function(arena, span, function)?
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
        sequence: &ColumnRef,
    ) -> Result<Box<(ScalarExpr, DataType)>> {
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
                    (ident.name.to_string(), format!("nextval({})", ident))
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
            func_name: "nextval".to_string(),
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
        function: &CoreDictGetFunction<'_>,
    ) -> Result<Box<(ScalarExpr, DataType)>> {
        // Get dict_name and dict_meta.
        let (db_name, dict_name) = {
            if function.dictionary.database.is_some() {
                return Err(ErrorCode::SemanticError(
                    "dict_get function argument identifier should contain one or two parts"
                        .to_string(),
                )
                .set_span(span));
            }
            let db_name = function
                .dictionary
                .table
                .as_ref()
                .map(|ident| normalize_identifier(ident, self.name_resolution_ctx).name);
            let dict_name = match &function.dictionary.column {
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
        let box (field_scalar, _field_data_type) = self.resolve_core(arena, function.field.expr)?;
        let Ok(field_expr) = ConstantExpr::try_from(field_scalar.clone()) else {
            return Err(ErrorCode::SemanticError(format!(
                "invalid arguments for dict_get function, attr_name must be a constant string, but got {}",
                function.field.display
            ))
            .set_span(field_scalar.span()));
        };
        let Some(attr_name) = field_expr.value.as_string() else {
            return Err(ErrorCode::SemanticError(format!(
                "invalid arguments for dict_get function, attr_name must be a constant string, but got {}",
                function.field.display
            ))
            .set_span(field_scalar.span()));
        };
        let dictionary =
            self.adapter
                .resolve_dictionary(db_name.as_deref(), &dict_name, attr_name)?;

        let mut args = Vec::with_capacity(1);
        let box (key_scalar, key_type) = self.resolve_core(arena, function.key.expr)?;

        if dictionary.primary_type != key_type.remove_nullable() {
            args.push(wrap_cast(&key_scalar, &dictionary.primary_type));
        } else {
            args.push(key_scalar);
        }
        let display_name = format!(
            "{}({}.{}, {}, {})",
            "dict_get", dictionary.db_name, dict_name, function.field.display, function.key.display,
        );
        Ok(Box::new((
            ScalarExpr::AsyncFunctionCall(AsyncFunctionCall {
                span,
                func_name: "dict_get".to_string(),
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
        function: &CoreReadFileFunction,
    ) -> Result<Box<(ScalarExpr, DataType)>> {
        let capacity = if function.stage.is_some() { 2 } else { 1 };
        let mut resolved_args = Vec::with_capacity(capacity);
        let mut has_nullable_arg = false;

        if let Some(stage) = function.stage.as_ref() {
            let box (stage_scalar, stage_type) = self.resolve_core(arena, stage.expr)?;
            has_nullable_arg |= stage_type.is_nullable_or_null();
            let stage_scalar = if stage_type.remove_nullable() != DataType::String {
                wrap_cast(&stage_scalar, &DataType::String)
            } else {
                stage_scalar
            };
            resolved_args.push(stage_scalar);
        }

        let box (location_scalar, location_type) =
            self.resolve_core(arena, function.location.expr)?;
        has_nullable_arg |= location_type.is_nullable_or_null();
        let location_scalar = if location_type.remove_nullable() != DataType::String {
            wrap_cast(&location_scalar, &DataType::String)
        } else {
            location_scalar
        };
        resolved_args.push(location_scalar);

        let mut read_file_arg = ReadFileFunctionArgument {
            stage_name: None,
            stage_info: None,
        };

        if function.stage.is_none() {
            if let ScalarExpr::ConstantExpr(constant) = &resolved_args[0]
                && let Some(location) = constant.value.as_string()
                && !location.starts_with('@')
            {
                return Err(ErrorCode::SemanticError(format!(
                    "stage path must start with @, but got {}",
                    location
                ))
                .set_span(span));
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

        let return_type = if has_nullable_arg {
            DataType::Nullable(Box::new(DataType::Binary))
        } else {
            DataType::Binary
        };

        let display_name = if let Some(stage) = function.stage.as_ref() {
            format!(
                "read_file({}, {})",
                stage.display, function.location.display
            )
        } else {
            format!("read_file({})", function.location.display)
        };
        Ok(Box::new((
            ScalarExpr::AsyncFunctionCall(AsyncFunctionCall {
                span,
                func_name: "read_file".to_string(),
                display_name,
                return_type: Box::new(return_type.clone()),
                arguments: resolved_args,
                func_arg: AsyncFunctionArgument::ReadFile(read_file_arg),
            }),
            return_type,
        )))
    }
}
