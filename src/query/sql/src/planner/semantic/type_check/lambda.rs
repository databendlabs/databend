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

use std::borrow::Cow;
use std::collections::HashMap;
use std::collections::HashSet;
use std::convert::Infallible;

use databend_common_ast::Span;
use databend_common_ast::ast::Expr;
use databend_common_ast::ast::FunctionCall as ASTFunctionCall;
use databend_common_ast::ast::Lambda;
use databend_common_ast::ast::LambdaArgument;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::ColumnRef as ExprColumnRef;
use databend_common_expression::ConstantFolder;
use databend_common_expression::DataField;
use databend_common_expression::DataSchema;
use databend_common_expression::Expr as EExpr;
use databend_common_expression::ExprVisitor;
use databend_common_expression::RemoteExpr;
use databend_common_expression::Scalar;
use databend_common_expression::Symbol;
use databend_common_expression::types::DataType;
use databend_common_expression::types::Decimal;
use databend_common_expression::types::NumberDataType;
use databend_common_expression::types::decimal::DecimalSize;
use databend_common_expression::types::i256;
use databend_common_expression::visit_expr;
use databend_common_functions::BUILTIN_FUNCTIONS;
use databend_common_functions::GENERAL_LAMBDA_FUNCTIONS;
use unicase::Ascii;

use super::CoreExpr;
use super::CoreExprArena;
use super::CoreExprArgs;
use super::CoreExprId;
use super::TypeChecker;
use crate::BindContext;
use crate::ColumnBinding;
use crate::ColumnBindingBuilder;
use crate::Visibility;
use crate::binder::AliasLookup;
use crate::binder::SelectItems;
use crate::planner::semantic::lowering::TypeCheck;
use crate::plans::BoundColumnRef;
use crate::plans::CastExpr;
use crate::plans::ConstantExpr;
use crate::plans::LambdaFunc;
use crate::plans::ScalarExpr;
use crate::plans::UDAFCall;
use crate::plans::UDFCall;
use crate::plans::Visitor;

impl<'a> CoreExprArena<'a> {
    pub(super) fn try_lower_lambda(
        &mut self,
        span: Span,
        func_name: &str,
        func: &'a ASTFunctionCall,
    ) -> Result<Option<CoreExprId>> {
        let uni_case_func_name = Ascii::new(func_name);
        let Some(func_name) = GENERAL_LAMBDA_FUNCTIONS
            .iter()
            .cloned()
            .find(|name| *name == uni_case_func_name)
            .map(Ascii::into_inner)
        else {
            if func.has_explicit_lambda() {
                return Err(ErrorCode::SemanticError(
                    "only lambda functions allowed in lambda syntax",
                )
                .set_span(span));
            }
            return Ok(None);
        };
        let Some(lambda_arg) = func.lambda.as_ref() else {
            return Err(ErrorCode::SemanticError(format!(
                "function {func_name} must have a lambda expression",
            ))
            .set_span(span));
        };

        let (args, lambda) = match lambda_arg {
            LambdaArgument::Lambda(lambda) => (func.args.as_slice(), lambda),
            LambdaArgument::Ambiguous(lambda) => {
                let Some((_, args)) = func.args.split_last() else {
                    return Err(ErrorCode::SemanticError(format!(
                        "function {func_name} must have a lambda expression",
                    ))
                    .set_span(span));
                };
                (args, lambda)
            }
        };

        Ok(Some(self.lambda_function(span, func_name, args, lambda)?))
    }

    /// Reject the nodes lowered from `first` on that a lambda body cannot
    /// hold.
    ///
    /// A body must compile into the `RemoteExpr` that the expression
    /// evaluator runs for each element: scalar functions, casts and other
    /// lambdas over the parameters, the captured columns and constants.
    /// Subqueries and aggregate, window, set-returning, async and search
    /// functions are evaluated by operators of their own, so they are rejected
    /// here, where they are lowered, with their spans. Script and server UDFs
    /// are the one exception: only binding tells them from SQL UDFs, see
    /// `reject_runtime_udf_in_lambda_body`.
    pub(super) fn reject_lambda_body_nodes(&self, first: usize) -> Result<()> {
        for node in &self.nodes[first..] {
            let (span, kind) = match node {
                CoreExpr::Subquery { span, .. } => (span, "subqueries"),
                CoreExpr::AggregateFunction { span, .. } => (span, "aggregate functions"),
                CoreExpr::AggregateWindowFunction { span, .. }
                | CoreExpr::CountAllWindowFunction { span, .. }
                | CoreExpr::GeneralWindowFunction { span, .. }
                | CoreExpr::UdfCall {
                    span,
                    has_window: true,
                    ..
                } => (span, "window functions"),
                CoreExpr::SetReturningFunction { span, .. } => (span, "set-returning functions"),
                CoreExpr::AsyncFunction { span, .. } => (span, "async functions"),
                CoreExpr::SearchFunction { span, .. } => (span, "search functions"),
                _ => continue,
            };
            return Err(ErrorCode::SemanticError(format!(
                "{kind} can not be used in lambda function"
            ))
            .set_span(*span));
        }
        Ok(())
    }

    pub(super) fn lambda_function(
        &mut self,
        span: Span,
        func_name: &'static str,
        args: &'a [Expr],
        lambda: &'a Lambda,
    ) -> Result<CoreExprId> {
        let args = self.lower_expr_args(args)?;
        // A parameter is named by a plain identifier, without quotes or
        // IDENTIFIER().
        if let Some(param) = lambda
            .params
            .iter()
            .find(|param| param.is_quoted() || param.is_hole() || param.is_variable())
        {
            return Err(ErrorCode::SemanticError(format!(
                "lambda parameter {param} must be a plain identifier, without quotes or IDENTIFIER()"
            ))
            .set_span(param.span));
        }
        let first = self.nodes.len();
        let lambda_expr = self.lower_ast_expr(&lambda.expr)?;
        self.reject_lambda_body_nodes(first)?;
        Ok(self.alloc(CoreExpr::LambdaFunction {
            span,
            func_name,
            args,
            lambda_params: self
                .name_resolution_ctx
                .normalize_lambda_params(&lambda.params),
            lambda_expr,
        }))
    }
}

impl<'a, A> TypeChecker<'a, A>
where A: super::TypeCheckAdapter
{
    /// The parameter named `name` of the innermost lambda whose body is being
    /// bound that has one. An unqualified name is a parameter before it is a
    /// column, so a parameter shadows columns, and those of enclosing lambdas.
    pub(super) fn lambda_param(&self, name: &str) -> Option<&ColumnBinding> {
        self.lambda_params
            .iter()
            .rev()
            .find_map(|params| params.iter().find(|param| param.column_name == name))
    }

    /// Bind the body of a lambda with `lambda_columns` as its parameters.
    ///
    /// The body is bound by this type checker, with the parameters in scope:
    /// a name of a parameter is the parameter, and every other name resolves
    /// as in the expression around the lambda, also to the output of a
    /// grouping key, but never to a SELECT alias, see [`LambdaBodyState`] for
    /// the state it is bound with. The parameters are numbered
    /// in a range of their own, past those of the enclosing lambdas, and are
    /// not registered in metadata: the compiled body refers to them by
    /// position. Returns the bound body, its type and the parameters.
    fn resolve_core_lambda_expr(
        &mut self,
        arena: &CoreExprArena<'_>,
        func_name: &str,
        span: Span,
        lambda_columns: &[(String, DataType)],
        lambda_expr: CoreExprId,
    ) -> Result<(ScalarExpr, DataType, Vec<ColumnBinding>)> {
        let base = self
            .lambda_params
            .iter()
            .flatten()
            .map(|param| param.index.as_usize() + 1)
            .fold(Symbol::MIN_LAMBDA_PARAMETER.as_usize(), usize::max);
        let params = lambda_columns
            .iter()
            .enumerate()
            .map(|(offset, (name, data_type))| {
                ColumnBindingBuilder::new(
                    name.clone(),
                    Symbol::new(base + offset),
                    Box::new(data_type.clone()),
                    Visibility::Visible,
                )
                .build()
            })
            .collect::<Vec<_>>();

        let state = LambdaBodyState::enter(self);
        self.lambda_params.push(params);
        let result = self.resolve_core(arena, lambda_expr);
        let params = self.lambda_params.pop().unwrap();
        state.leave(self);

        let deref!((body, data_type)) = result?;
        Self::reject_runtime_udf_in_lambda_body(func_name, span, &body)?;
        Ok((body, data_type, params))
    }

    /// Compile the bound `body` of a lambda with `params` into the
    /// `RemoteExpr` the evaluator runs for each element, over a schema of
    /// `capture_fields` and then the parameters. Returns it with its display.
    ///
    /// Parameters, also those of enclosing lambdas that the body captures,
    /// are displayed by name: their symbols differ each time a lambda is
    /// bound, so with them, two bindings of the same lambda would differ.
    fn compile_lambda_body(
        &self,
        body: &ScalarExpr,
        params: &[ColumnBinding],
        mut capture_fields: Vec<DataField>,
    ) -> Result<(RemoteExpr, String)> {
        capture_fields.extend(
            params
                .iter()
                .map(|param| DataField::new(&param.index.to_string(), (*param.data_type).clone())),
        );
        let schema = DataSchema::new(capture_fields);
        let param_names = self
            .lambda_params
            .iter()
            .flatten()
            .chain(params)
            .filter_map(|param| {
                let position = schema.index_of(&param.index.to_string()).ok()?;
                Some((position, param.column_name.clone()))
            })
            .collect::<HashMap<_, _>>();
        let expr = body
            .type_check(&schema)?
            .project_column_ref(|index| schema.index_of(&index.to_string()))?;
        let expr = match visit_expr(&expr, &mut ParamNames(&param_names)) {
            Ok(renamed) => renamed.unwrap_or(expr),
            Err(never) => match never {},
        };
        let (expr, _) = ConstantFolder::fold(Cow::Owned(expr), &self.func_ctx, &BUILTIN_FUNCTIONS);
        let names = params
            .iter()
            .map(|param| param.column_name.clone())
            .collect::<Vec<_>>();
        let display = format!("{:?} -> {}", names, expr.sql_display());
        Ok((expr.as_remote_expr(), display))
    }

    /// A lambda body is compiled into a `RemoteExpr` and evaluated per element by
    /// the expression evaluator, so it cannot host a script or server UDF: those
    /// are executed by a separate `Udf` plan node that `UdfRewriter` lifts out of
    /// the enclosing `EvalScalar`/`Filter`. Without this check the unrewritten
    /// call is lowered into a dummy column reference and later fails with a
    /// confusing "Unable to get field named <internal column id>" error.
    fn reject_runtime_udf_in_lambda_body(
        func_name: &str,
        span: Span,
        lambda_body: &ScalarExpr,
    ) -> Result<()> {
        struct RuntimeUdfVisitor {
            found: Option<(Span, String)>,
        }

        impl<'b> Visitor<'b> for RuntimeUdfVisitor {
            fn visit_udf_call(&mut self, udf: &'b UDFCall) -> Result<()> {
                if self.found.is_none() {
                    self.found = Some((udf.span, udf.name.clone()));
                }
                Ok(())
            }

            fn visit_udaf_call(&mut self, udaf: &'b UDAFCall) -> Result<()> {
                if self.found.is_none() {
                    self.found = Some((udaf.span, udaf.name.clone()));
                }
                Ok(())
            }
        }

        let mut visitor = RuntimeUdfVisitor { found: None };
        visitor.visit(lambda_body)?;

        match visitor.found {
            None => Ok(()),
            Some((udf_span, udf_name)) => Err(ErrorCode::SemanticError(format!(
                "UDF {udf_name:?} is not supported in the lambda body of {func_name}, \
                 because script and server UDFs are evaluated by a separate plan node"
            ))
            .set_span(udf_span.or(span))),
        }
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
        lambda_params: &[String],
        lambda_expr: CoreExprId,
    ) -> Result<Box<(ScalarExpr, DataType)>> {
        if func_name == "json_path_transform" {
            if args.len() != 2 {
                return Err(ErrorCode::SemanticError(format!(
                    "invalid arguments for lambda function, {} expects 2 arguments (json, path), but got {}",
                    func_name,
                    args.len()
                ))
                .set_span(span));
            }
            return self.resolve_json_path_transform(arena, span, args, lambda_params, lambda_expr);
        }
        if args.len() != 1 {
            return Err(ErrorCode::SemanticError(format!(
                "invalid arguments for lambda function, {} expects 1 argument, but got {}",
                func_name,
                args.len()
            ))
            .set_span(span));
        }
        let deref!((arg, arg_type)) = self.resolve_core(arena, args[0])?;
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
        lambda_params: &[String],
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

        let params = lambda_params.to_vec();

        self.check_lambda_param_count(func_name, params.len(), span)?;

        let inner_ty = match arg_type.remove_nullable() {
            DataType::Array(deref!(inner_ty)) => inner_ty.clone(),
            DataType::Map(deref!(inner_ty)) => inner_ty.clone(),
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

        let (lambda_expr, lambda_type, lambda_params) =
            self.resolve_core_lambda_expr(arena, func_name, span, &lambda_columns, lambda_expr)?;

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
                // The captured columns are the leading arguments, the parameters
                // the trailing fields of the schema.
                let (mut lambda_args, lambda_fields) =
                    self.collect_lambda_capture_args(&lambda_expr, &lambda_params)?;
                lambda_args.push(arg);
                let (remote_lambda_expr, lambda_display) =
                    self.compile_lambda_body(&lambda_expr, &lambda_params, lambda_fields)?;

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

    /// Collects the columns that a lambda body with `params` captures; they
    /// become the leading arguments of the lambda function and fields of its
    /// schema. A column of the body is a parameter of the lambda, or of a
    /// lambda nested in it, which the nested one compiled away; otherwise it
    /// is captured: a symbol that the context of the lambda owns, see
    /// `BindContext::owns_symbol`, or a parameter of an enclosing lambda,
    /// whose body binds the captures. The columns are collected after the body
    /// is bound, so that those a masking policy expands to are found too.
    fn collect_lambda_capture_args(
        &self,
        lambda_expr: &ScalarExpr,
        params: &[ColumnBinding],
    ) -> Result<(Vec<ScalarExpr>, Vec<DataField>)> {
        struct LambdaVisitor<'a> {
            bind_context: &'a BindContext,
            enclosing_params: &'a [Vec<ColumnBinding>],
            params: &'a [ColumnBinding],
            arg_index: HashSet<Symbol>,
            args: Vec<ScalarExpr>,
            fields: Vec<DataField>,
        }

        impl<'a> Visitor<'a> for LambdaVisitor<'a> {
            fn visit_bound_column_ref(&mut self, col: &'a BoundColumnRef) -> Result<()> {
                let index = col.column.index;
                if !self.arg_index.insert(index) {
                    return Ok(());
                }
                if self.params.iter().any(|param| param.index == index) {
                    return Ok(());
                }
                if self.bind_context.owns_symbol(index)
                    || self
                        .enclosing_params
                        .iter()
                        .flatten()
                        .any(|param| param.index == index)
                {
                    self.args.push(ScalarExpr::BoundColumnRef(col.clone()));
                    self.fields.push(DataField::new(
                        &format!("{index}"),
                        *col.column.data_type.clone(),
                    ));
                    return Ok(());
                }
                // TODO: capture columns of outer queries. They would become
                // correlated arguments of the lambda function, which
                // decorrelation does not support yet.
                Err(ErrorCode::SemanticError(format!(
                    "lambda functions in a subquery can't reference column \"{}\" of an outer query",
                    col.column.column_name
                ))
                .set_span(col.span))
            }
        }

        let mut lambda_visitor = LambdaVisitor {
            bind_context: self.bind_context,
            enclosing_params: &self.lambda_params,
            params,
            arg_index: HashSet::new(),
            args: Vec::new(),
            fields: Vec::new(),
        };
        lambda_visitor.visit(lambda_expr)?;
        Ok((lambda_visitor.args, lambda_visitor.fields))
    }

    /// Resolves `json_path_transform(<json>, <path>, <param> -> <expr>)`.
    ///
    /// The variant argument is not cast to an array or map: the path locates
    /// the values to transform and the lambda only runs on them. The lambda
    /// body is cast to `Nullable(Variant)`; a NULL result writes a JSON null.
    fn resolve_json_path_transform(
        &mut self,
        arena: &CoreExprArena<'_>,
        span: Span,
        args: &CoreExprArgs,
        lambda_params: &[String],
        lambda_expr: CoreExprId,
    ) -> Result<Box<(ScalarExpr, DataType)>> {
        let func_name = "json_path_transform";
        let deref!((json_arg, json_type)) = self.resolve_core(arena, args[0])?;
        let deref!((path_arg, path_type)) = self.resolve_core(arena, args[1])?;

        if lambda_params.len() != 1 {
            return Err(ErrorCode::SemanticError(format!(
                "incorrect number of parameters in lambda function, {} expects 1 parameter, but got {}",
                func_name,
                lambda_params.len()
            ))
            .set_span(span));
        }
        if json_type != DataType::Null && json_type.remove_nullable() != DataType::Variant {
            return Err(ErrorCode::SemanticError(format!(
                "invalid arguments for {}, the json argument must be a variant, but got {}",
                func_name, json_type
            ))
            .set_span(span));
        }
        if path_type != DataType::Null && path_type.remove_nullable() != DataType::String {
            return Err(ErrorCode::SemanticError(format!(
                "invalid arguments for {}, the path argument must be a string, but got {}",
                func_name, path_type
            ))
            .set_span(span));
        }

        let fold_to_null = json_type == DataType::Null || path_type == DataType::Null;

        let params = lambda_params.to_vec();
        let param_type = DataType::Nullable(Box::new(DataType::Variant));
        let lambda_columns = vec![(params[0].clone(), param_type.clone())];

        let (lambda_scalar, lambda_type, lambda_params) =
            self.resolve_core_lambda_expr(arena, func_name, span, &lambda_columns, lambda_expr)?;

        let lambda_scalar = if lambda_type != param_type {
            ScalarExpr::CastExpr(CastExpr {
                span: lambda_scalar.span(),
                is_try: false,
                argument: Box::new(lambda_scalar),
                target_type: Box::new(param_type.clone()),
            })
        } else {
            lambda_scalar
        };

        // Argument layout is `[json, path, captures...]` so that EXPLAIN and
        // profile output keep the signature order; the evaluator relies on it.
        let (capture_args, lambda_fields) =
            self.collect_lambda_capture_args(&lambda_scalar, &lambda_params)?;
        let mut lambda_args = vec![json_arg, path_arg];
        lambda_args.extend(capture_args);
        let (remote_lambda_expr, lambda_display) =
            self.compile_lambda_body(&lambda_scalar, &lambda_params, lambda_fields)?;

        // Fold NULL constants only after the lambda body passed normal
        // semantic and type validation.
        if fold_to_null {
            return Ok(Box::new((
                ConstantExpr {
                    span,
                    value: Scalar::Null,
                }
                .into(),
                DataType::Null,
            )));
        }

        let return_type = if json_type.is_nullable() || path_type.is_nullable() {
            DataType::Nullable(Box::new(DataType::Variant))
        } else {
            DataType::Variant
        };

        Ok(Box::new((
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
        )))
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

/// The state of a type checker that binding a lambda body sets, and restores
/// after it.
///
/// The body is bound without SELECT aliases, nor the aliases of a WHERE that
/// uses unmasked values, and not inside an aggregate function or a window
/// function. Every other field is inherited from the expression around the
/// lambda: the expression context of its clause, the grouping-key matching
/// state and its memo, the folding policy, whether masking policies apply and
/// whether virtual computed columns are forbidden, which are the mode of the
/// whole binding, and the parameters of enclosing lambdas, whose presence
/// tells that a body is bound. Whether a masking policy is being bound is
/// inherited too: a body in a policy is a part of it, and a column it refers
/// to must not have the policy applied again, which would bind the policy
/// without end.
struct LambdaBodyState<'a> {
    aliases: AliasLookup<'a>,
    select_items: Option<SelectItems<'a>>,
    in_aggregate_function: bool,
    in_window_function: bool,
    masking_where_aliases: Option<(&'a CoreExprArena<'a>, &'a [(String, CoreExprId)])>,
    resolving_masking_where_aliases: Vec<String>,
}

impl<'a> LambdaBodyState<'a> {
    fn enter<A: super::TypeCheckAdapter>(checker: &mut TypeChecker<'a, A>) -> Self {
        Self {
            aliases: std::mem::replace(&mut checker.aliases, AliasLookup::all(&[])),
            select_items: checker.select_items.take(),
            in_aggregate_function: std::mem::take(&mut checker.in_aggregate_function),
            in_window_function: std::mem::take(&mut checker.in_window_function),
            masking_where_aliases: checker.masking_where_aliases.take(),
            resolving_masking_where_aliases: std::mem::take(
                &mut checker.resolving_masking_where_aliases,
            ),
        }
    }

    fn leave<A: super::TypeCheckAdapter>(self, checker: &mut TypeChecker<'a, A>) {
        checker.aliases = self.aliases;
        checker.select_items = self.select_items;
        checker.in_aggregate_function = self.in_aggregate_function;
        checker.in_window_function = self.in_window_function;
        checker.masking_where_aliases = self.masking_where_aliases;
        checker.resolving_masking_where_aliases = self.resolving_masking_where_aliases;
    }
}

/// Displays the column references at the positions of lambda parameters by
/// the names of the parameters.
struct ParamNames<'a>(&'a HashMap<usize, String>);

impl ExprVisitor<usize> for ParamNames<'_> {
    type Error = Infallible;

    fn enter_column_ref(
        &mut self,
        column: &ExprColumnRef<usize>,
    ) -> std::result::Result<Option<EExpr<usize>>, Infallible> {
        Ok(self.0.get(&column.id).map(|name| {
            EExpr::ColumnRef(ExprColumnRef {
                display_name: name.clone(),
                ..column.clone()
            })
        }))
    }
}
