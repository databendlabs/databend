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

use std::collections::HashSet;
use std::mem;

use databend_common_ast::Span;
use databend_common_ast::ast::Expr;
use databend_common_ast::ast::FunctionCall as ASTFunctionCall;
use databend_common_ast::ast::Identifier;
use databend_common_ast::ast::Lambda;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::ConstantFolder;
use databend_common_expression::DataField;
use databend_common_expression::DataSchema;
use databend_common_expression::Scalar;
use databend_common_expression::Symbol;
use databend_common_expression::types::DataType;
use databend_common_expression::types::Decimal;
use databend_common_expression::types::NumberDataType;
use databend_common_expression::types::decimal::DecimalSize;
use databend_common_expression::types::i256;
use databend_common_functions::BUILTIN_FUNCTIONS;
use databend_common_functions::GENERAL_LAMBDA_FUNCTIONS;
use itertools::Itertools;
use unicase::Ascii;

use super::CoreExpr;
use super::CoreExprArena;
use super::CoreExprArgs;
use super::CoreExprId;
use super::TypeChecker;
use crate::BindContext;
use crate::ColumnBindingBuilder;
use crate::Visibility;
use crate::binder::ExprContext;
use crate::planner::semantic::lowering::TypeCheck;
use crate::plans::BoundColumnRef;
use crate::plans::CastExpr;
use crate::plans::ConstantExpr;
use crate::plans::LambdaFunc;
use crate::plans::ScalarExpr;
use crate::plans::Visitor;

impl<'a> CoreExprArena<'a> {
    pub(super) fn try_lower_lambda(
        &mut self,
        span: Span,
        func_name: &str,
        func: &'a ASTFunctionCall,
    ) -> Result<Option<CoreExprId>> {
        let uni_case_func_name = Ascii::new(func_name);
        if func.lambda.is_some() && !GENERAL_LAMBDA_FUNCTIONS.contains(&uni_case_func_name) {
            return Err(
                ErrorCode::SemanticError("only lambda functions allowed in lambda syntax")
                    .set_span(span),
            );
        }

        let Some(func_name) = GENERAL_LAMBDA_FUNCTIONS
            .iter()
            .cloned()
            .find(|name| *name == uni_case_func_name)
            .map(Ascii::into_inner)
        else {
            return Ok(None);
        };
        let Some(lambda) = func.lambda.as_ref() else {
            return Err(ErrorCode::SemanticError(format!(
                "function {func_name} must have a lambda expression",
            ))
            .set_span(span));
        };

        Ok(Some(
            self.lambda_function(span, func_name, &func.args, lambda)?,
        ))
    }

    pub(super) fn lambda_function(
        &mut self,
        span: Span,
        func_name: &'static str,
        args: &'a [Expr],
        lambda: &'a Lambda,
    ) -> Result<CoreExprId> {
        let args = self.lower_expr_args(args)?;
        let previous_in_lambda_function = self.in_lambda_function;
        self.in_lambda_function = true;
        let lambda_expr = self.lower_ast_expr(&lambda.expr);
        self.in_lambda_function = previous_in_lambda_function;
        let lambda_expr = lambda_expr?;
        Ok(self.alloc(CoreExpr::LambdaFunction {
            span,
            func_name,
            args,
            lambda_params: &lambda.params,
            lambda_expr,
        }))
    }
}

impl<'a, A> TypeChecker<'a, A>
where A: super::TypeCheckAdapter
{
    fn resolve_core_lambda_expr(
        &mut self,
        arena: &CoreExprArena<'_>,
        lambda_context: &mut BindContext,
        lambda_columns: &[(String, DataType)],
        lambda_expr: CoreExprId,
    ) -> Result<Box<(ScalarExpr, DataType)>> {
        lambda_context.expr_context = ExprContext::InLambdaFunction;

        for (lambda_column, lambda_column_type) in lambda_columns.iter() {
            let column_index = lambda_context.next_column_index();
            lambda_context.add_column_binding(
                ColumnBindingBuilder::new(
                    lambda_column.clone(),
                    column_index,
                    Box::new(lambda_column_type.clone()),
                    Visibility::Visible,
                )
                .build(),
            );
        }

        let mut type_checker = TypeChecker::try_create_with_adapter(
            lambda_context,
            self.adapter.clone(),
            self.name_resolution_ctx,
            self.metadata.clone(),
            &[],
        )?;
        type_checker.resolve_core(arena, lambda_expr)
    }

    fn transform_to_max_type(&self, ty: &DataType) -> Result<DataType> {
        let max_ty = match ty.remove_nullable() {
            DataType::Number(s) => {
                if s.is_float() {
                    DataType::Number(NumberDataType::Float64)
                } else {
                    DataType::Number(NumberDataType::Int64)
                }
            }
            DataType::Decimal(s) if s.can_carried_by_128() => {
                let decimal_size = DecimalSize::new_unchecked(i128::MAX_PRECISION, s.scale());
                DataType::Decimal(decimal_size)
            }
            DataType::Decimal(s) => {
                let decimal_size = DecimalSize::new_unchecked(i256::MAX_PRECISION, s.scale());
                DataType::Decimal(decimal_size)
            }
            DataType::Null => DataType::Null,
            DataType::Binary => DataType::Binary,
            DataType::String => DataType::String,
            DataType::Variant => DataType::Variant,
            _ => {
                return Err(ErrorCode::BadDataValueType(format!(
                    "array_reduce does not support type '{:?}'",
                    ty
                )));
            }
        };

        if ty.is_nullable() {
            Ok(max_ty.wrap_nullable())
        } else {
            Ok(max_ty)
        }
    }

    pub(super) fn resolve_core_lambda_function(
        &mut self,
        arena: &CoreExprArena<'_>,
        span: Span,
        func_name: &str,
        args: &CoreExprArgs,
        lambda_params: &[Identifier],
        lambda_expr: CoreExprId,
    ) -> Result<Box<(ScalarExpr, DataType)>> {
        if matches!(
            self.bind_context.expr_context,
            ExprContext::InLambdaFunction
        ) {
            return Err(ErrorCode::SemanticError(
                "lambda functions can not be used in lambda function".to_string(),
            )
            .set_span(span));
        }

        if args.len() != 1 {
            return Err(ErrorCode::SemanticError(format!(
                "invalid arguments for lambda function, {} expects 1 argument, but got {}",
                func_name,
                args.len()
            ))
            .set_span(span));
        }
        let box (arg, arg_type) = self.resolve_core(arena, args[0])?;
        self.resolve_lambda_function_arg(
            arena,
            span,
            func_name,
            arg,
            arg_type,
            lambda_params,
            lambda_expr,
        )
    }

    fn resolve_lambda_function_arg(
        &mut self,
        arena: &CoreExprArena<'_>,
        span: Span,
        func_name: &str,
        mut arg: ScalarExpr,
        mut arg_type: DataType,
        lambda_params: &[Identifier],
        lambda_expr: CoreExprId,
    ) -> Result<Box<(ScalarExpr, DataType)>> {
        let mut func_name = func_name;
        let mut is_cast_variant = false;
        if arg_type.remove_nullable() == DataType::Variant {
            if func_name.starts_with("json_") {
                func_name = &func_name[5..];
            }
            // Try auto cast the Variant type to Array(Variant) or Map(String, Variant),
            // so that the lambda functions support variant type as argument.
            let mut target_type = if func_name.starts_with("array") {
                DataType::Array(Box::new(DataType::Nullable(Box::new(DataType::Variant))))
            } else {
                DataType::Map(Box::new(DataType::Tuple(vec![
                    DataType::String,
                    DataType::Nullable(Box::new(DataType::Variant)),
                ])))
            };
            if arg_type.is_nullable() {
                target_type = target_type.wrap_nullable();
            }

            arg = ScalarExpr::CastExpr(CastExpr {
                span: None,
                is_try: false,
                argument: Box::new(arg.clone()),
                target_type: Box::new(target_type.clone()),
            });
            arg_type = target_type;

            is_cast_variant = true;
        }

        let params = lambda_params
            .iter()
            .map(|param| param.name.to_lowercase())
            .collect::<Vec<_>>();

        self.check_lambda_param_count(func_name, params.len(), span)?;

        let inner_ty = match arg_type.remove_nullable() {
            DataType::Array(box inner_ty) => inner_ty.clone(),
            DataType::Map(box inner_ty) => inner_ty.clone(),
            DataType::Null | DataType::EmptyArray | DataType::EmptyMap => DataType::Null,
            _ => {
                return Err(ErrorCode::SemanticError(
                    "invalid arguments for lambda function, argument data type must be an array or map"
                        .to_string(),
                )
                .set_span(span));
            }
        };

        let inner_tys = if func_name == "array_reduce" {
            let max_ty = self.transform_to_max_type(&inner_ty)?;
            vec![max_ty.clone(), max_ty.clone()]
        } else if func_name == "map_filter"
            || func_name == "map_transform_keys"
            || func_name == "map_transform_values"
        {
            match &inner_ty {
                DataType::Null => {
                    vec![DataType::Null, DataType::Null]
                }
                DataType::Tuple(t) => t.clone(),
                _ => unreachable!(),
            }
        } else {
            vec![inner_ty.clone()]
        };

        let lambda_columns = params
            .iter()
            .zip(inner_tys.iter())
            .map(|(col, ty)| (col.clone(), ty.clone()))
            .collect::<Vec<_>>();

        let mut lambda_context = self.bind_context.clone();
        let box (lambda_expr, lambda_type) = self.resolve_core_lambda_expr(
            arena,
            &mut lambda_context,
            &lambda_columns,
            lambda_expr,
        )?;

        let return_type = if func_name == "array_filter" || func_name == "map_filter" {
            if lambda_type.remove_nullable() == DataType::Boolean {
                arg_type.clone()
            } else {
                return Err(ErrorCode::SemanticError(
                    format!("invalid lambda function for `{}`, the result data type of lambda function must be boolean", func_name)
                )
                .set_span(span));
            }
        } else if func_name == "array_reduce" {
            // transform arg type
            let max_ty = inner_tys[0].clone();
            let target_type = if arg_type.is_nullable() {
                Box::new(DataType::Nullable(Box::new(DataType::Array(Box::new(
                    max_ty.clone(),
                )))))
            } else {
                Box::new(DataType::Array(Box::new(max_ty.clone())))
            };
            // we should convert arg to max_ty to avoid overflow in 'ADD'/'SUB',
            // so if arg_type(origin_type) != target_type(max_type), cast arg
            // for example, if arg = [1INT8, 2INT8, 3INT8], after cast it be [1INT64, 2INT64, 3INT64]
            if arg_type != *target_type {
                arg = ScalarExpr::CastExpr(CastExpr {
                    span: arg.span(),
                    is_try: false,
                    argument: Box::new(arg),
                    target_type,
                });
            }
            max_ty.wrap_nullable()
        } else if func_name == "map_transform_keys" {
            if arg_type.is_nullable() {
                DataType::Nullable(Box::new(DataType::Map(Box::new(DataType::Tuple(vec![
                    lambda_type.clone(),
                    inner_tys[1].clone(),
                ])))))
            } else {
                DataType::Map(Box::new(DataType::Tuple(vec![
                    lambda_type.clone(),
                    inner_tys[1].clone(),
                ])))
            }
        } else if func_name == "map_transform_values" {
            if arg_type.is_nullable() {
                DataType::Nullable(Box::new(DataType::Map(Box::new(DataType::Tuple(vec![
                    inner_tys[0].clone(),
                    lambda_type.clone(),
                ])))))
            } else {
                DataType::Map(Box::new(DataType::Tuple(vec![
                    inner_tys[0].clone(),
                    lambda_type.clone(),
                ])))
            }
        } else if arg_type.is_nullable() {
            DataType::Nullable(Box::new(DataType::Array(Box::new(lambda_type.clone()))))
        } else {
            DataType::Array(Box::new(lambda_type.clone()))
        };

        let (lambda_func, data_type) = match arg_type.remove_nullable() {
            // Null and Empty array can convert to ConstantExpr
            DataType::Null => (
                ConstantExpr {
                    span,
                    value: Scalar::Null,
                }
                .into(),
                DataType::Null,
            ),
            DataType::EmptyArray => (
                ConstantExpr {
                    span,
                    value: Scalar::EmptyArray,
                }
                .into(),
                DataType::EmptyArray,
            ),
            DataType::EmptyMap => (
                ConstantExpr {
                    span,
                    value: Scalar::EmptyMap,
                }
                .into(),
                DataType::EmptyMap,
            ),
            _ => {
                struct LambdaVisitor<'a> {
                    bind_context: &'a BindContext,
                    arg_index: HashSet<Symbol>,
                    args: Vec<ScalarExpr>,
                    fields: Vec<DataField>,
                }

                impl<'a> Visitor<'a> for LambdaVisitor<'a> {
                    fn visit_bound_column_ref(&mut self, col: &'a BoundColumnRef) -> Result<()> {
                        if self.arg_index.contains(&col.column.index) {
                            return Ok(());
                        }
                        self.arg_index.insert(col.column.index);
                        let is_outer_column = self
                            .bind_context
                            .all_column_bindings()
                            .iter()
                            .map(|c| c.index)
                            .contains(&col.column.index);
                        if is_outer_column {
                            let arg = ScalarExpr::BoundColumnRef(col.clone());
                            self.args.push(arg);
                            let field = DataField::new(
                                &format!("{}", col.column.index),
                                *col.column.data_type.clone(),
                            );
                            self.fields.push(field);
                        }
                        Ok(())
                    }
                }

                // Collect outer scope columns as arguments first.
                let mut lambda_visitor = LambdaVisitor {
                    bind_context: self.bind_context,
                    arg_index: HashSet::new(),
                    args: Vec::new(),
                    fields: Vec::new(),
                };
                lambda_visitor.visit(&lambda_expr)?;

                let mut lambda_args = mem::take(&mut lambda_visitor.args);
                lambda_args.push(arg);
                let mut lambda_fields = mem::take(&mut lambda_visitor.fields);
                // Add lambda columns as arguments at end.
                for (lambda_column_name, lambda_column_type) in lambda_columns.into_iter() {
                    for column in lambda_context.all_column_bindings().iter().rev() {
                        if column.column_name == lambda_column_name {
                            let lambda_field =
                                DataField::new(&format!("{}", column.index), lambda_column_type);
                            lambda_fields.push(lambda_field);
                            break;
                        }
                    }
                }
                let lambda_schema = DataSchema::new(lambda_fields);
                let expr = lambda_expr
                    .type_check(&lambda_schema)?
                    .project_column_ref(|index| lambda_schema.index_of(&index.to_string()))?;
                let (expr, _) = ConstantFolder::fold(&expr, &self.func_ctx, &BUILTIN_FUNCTIONS);
                let remote_lambda_expr = expr.as_remote_expr();
                let lambda_display = format!("{:?} -> {}", params, expr.sql_display());

                (
                    LambdaFunc {
                        span,
                        func_name: func_name.to_string(),
                        args: lambda_args,
                        lambda_expr: Box::new(remote_lambda_expr),
                        lambda_display,
                        return_type: Box::new(return_type.clone()),
                    }
                    .into(),
                    return_type,
                )
            }
        };

        if is_cast_variant {
            let result_target_type = if data_type.is_nullable() {
                DataType::Nullable(Box::new(DataType::Variant))
            } else {
                DataType::Variant
            };
            let result_target_scalar = ScalarExpr::CastExpr(CastExpr {
                span: None,
                is_try: false,
                argument: Box::new(lambda_func),
                target_type: Box::new(result_target_type.clone()),
            });
            Ok(Box::new((result_target_scalar, result_target_type)))
        } else {
            Ok(Box::new((lambda_func, data_type)))
        }
    }

    fn check_lambda_param_count(
        &mut self,
        func_name: &str,
        param_count: usize,
        span: Span,
    ) -> Result<()> {
        // json lambda functions are cast to array or map, ignored here.
        let expected_count = if func_name == "array_reduce" {
            2
        } else if func_name.starts_with("array") {
            1
        } else if func_name.starts_with("map") {
            2
        } else {
            unreachable!()
        };

        if param_count != expected_count {
            return Err(ErrorCode::SemanticError(format!(
                "incorrect number of parameters in lambda function, {} expects {} parameter(s), but got {}",
                func_name, expected_count, param_count
            ))
            .set_span(span));
        }
        Ok(())
    }
}
