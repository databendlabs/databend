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

use std::collections::HashMap;

use databend_common_ast::Span;
use log::error;

use crate::ColumnBuilder;
use crate::ColumnIndex;
use crate::EvalContext;
use crate::FunctionContext;
use crate::FunctionDomain;
use crate::FunctionEval;
use crate::FunctionRegistry;
use crate::Scalar;
use crate::Value;
use crate::block::DataBlock;
use crate::evaluator::Evaluator;
use crate::expression::Cast;
use crate::expression::ColumnRef;
use crate::expression::Constant;
use crate::expression::Expr;
use crate::expression::FunctionCall;
use crate::expression::LambdaFunctionCall;
use crate::property::Domain;
use crate::type_check::check_function;
use crate::type_check::get_simple_cast_function;
use crate::types::DataType;
use crate::types::boolean::BooleanDomain;
use crate::types::nullable::NullableDomain;
use crate::types::number::NumberScalar;

const MAX_FUNCTION_ARGS_TO_FOLD: usize = 4096;

pub struct ConstantFolder<'a, Index: ColumnIndex> {
    input_domains: &'a HashMap<Index, Domain>,
    func_ctx: &'a FunctionContext,
    fn_registry: &'a FunctionRegistry,
}

impl<'a, Index: ColumnIndex> ConstantFolder<'a, Index> {
    /// Fold a single expression, returning the new expression and the domain of the new expression.
    pub fn fold(
        expr: &Expr<Index>,
        func_ctx: &'a FunctionContext,
        fn_registry: &'a FunctionRegistry,
    ) -> (Expr<Index>, Option<Domain>) {
        let input_domains = Self::full_input_domains(expr);

        let folder = ConstantFolder {
            input_domains: &input_domains,
            func_ctx,
            fn_registry,
        };

        folder.fold_to_stable(expr)
    }

    /// Fold a single expression with columns' domain, and then return the new expression and the
    /// domain of the new expression.
    pub fn fold_with_domain(
        expr: &Expr<Index>,
        input_domains: &'a HashMap<Index, Domain>,
        func_ctx: &'a FunctionContext,
        fn_registry: &'a FunctionRegistry,
    ) -> (Expr<Index>, Option<Domain>) {
        let folder = ConstantFolder {
            input_domains,
            func_ctx,
            fn_registry,
        };

        folder.fold_to_stable(expr)
    }

    pub fn full_input_domains(expr: &Expr<Index>) -> HashMap<Index, Domain> {
        expr.column_refs()
            .into_iter()
            .map(|(id, ty)| {
                let domain = Domain::full(&ty);
                (id, domain)
            })
            .collect()
    }

    /// Running `fold_once()` for only one time may not reach the simplest form of expression,
    /// therefore we need to call it repeatedly until the expression becomes stable.
    fn fold_to_stable(&self, expr: &Expr<Index>) -> (Expr<Index>, Option<Domain>) {
        const MAX_ITERATIONS: usize = 1024;

        let mut old_expr = expr.clone();
        let mut old_domain = None;
        for _ in 0..MAX_ITERATIONS {
            let (new_expr, new_domain) = self.fold_once(&old_expr);

            if new_expr == old_expr {
                return (new_expr, new_domain);
            }
            old_expr = new_expr;
            old_domain = new_domain;
        }

        error!("maximum iterations reached while folding expression");

        (old_expr, old_domain)
    }

    /// Fold expression by one step, specifically, by reducing expression by domain calculation and then
    /// folding the function calls whose all arguments are constants.
    #[recursive::recursive]
    fn fold_once(&self, expr: &Expr<Index>) -> (Expr<Index>, Option<Domain>) {
        let (new_expr, domain) = match expr {
            Expr::Constant(Constant {
                scalar, data_type, ..
            }) => (expr.clone(), Some(scalar.as_ref().domain(data_type))),
            Expr::ColumnRef(ColumnRef {
                span,
                id,
                data_type,
                ..
            }) => {
                let domain = &self.input_domains[id];
                let expr = domain
                    .as_singleton()
                    .map(|scalar| {
                        Expr::Constant(Constant {
                            span: *span,
                            scalar,
                            data_type: data_type.clone(),
                        })
                    })
                    .unwrap_or_else(|| expr.clone());
                (expr, Some(domain.clone()))
            }
            Expr::Cast(Cast {
                span,
                is_try,
                expr,
                dest_type,
            }) => {
                let (inner_expr, inner_domain) = self.fold_once(expr);

                let new_domain = if *is_try {
                    inner_domain.and_then(|inner_domain| {
                        self.calculate_try_cast(*span, expr.data_type(), dest_type, &inner_domain)
                    })
                } else {
                    inner_domain.and_then(|inner_domain| {
                        self.calculate_cast(*span, expr.data_type(), dest_type, &inner_domain)
                    })
                };

                let cast_expr = Expr::Cast(Cast {
                    span: *span,
                    is_try: *is_try,
                    expr: Box::new(inner_expr.clone()),
                    dest_type: dest_type.clone(),
                });

                if inner_expr.as_constant().is_some() {
                    let block = DataBlock::empty_with_rows(1);
                    let evaluator = Evaluator::new(&block, self.func_ctx, self.fn_registry);
                    // Since we know the expression is constant, it'll be safe to change its column index type.
                    let cast_expr = cast_expr.project_column_ref(|_| unreachable!()).unwrap();
                    if let Ok(Value::Scalar(scalar)) = evaluator.run(&cast_expr) {
                        return (
                            Expr::Constant(Constant {
                                span: *span,
                                scalar,
                                data_type: dest_type.clone(),
                            }),
                            None,
                        );
                    }
                }

                (
                    new_domain
                        .as_ref()
                        .and_then(Domain::as_singleton)
                        .map(|scalar| {
                            Expr::Constant(Constant {
                                span: *span,
                                scalar,
                                data_type: dest_type.clone(),
                            })
                        })
                        .unwrap_or(cast_expr),
                    new_domain,
                )
            }
            Expr::FunctionCall(FunctionCall {
                span,
                id,
                function,
                generics,
                args,
                return_type,
            }) if matches!(
                function.signature.name.as_str(),
                "and_filters" | "or_filters"
            ) =>
            {
                let is_or = function.signature.name.starts_with("or_");

                if args.len() > MAX_FUNCTION_ARGS_TO_FOLD {
                    return (expr.clone(), None);
                }

                let mut args_expr = Vec::new();
                let mut result_domain = Some(BooleanDomain {
                    has_true: true,
                    has_false: true,
                });

                for arg in args {
                    let (expr, domain) = self.fold_once(arg);
                    // A temporary hack to make `and_filters` shortcut on false.
                    // TODO(andylokandy): make it a rule in the optimizer.
                    if let Expr::Constant(Constant {
                        scalar: Scalar::Boolean(result),
                        ..
                    }) = &expr
                    {
                        if is_or == *result {
                            return (
                                Expr::Constant(Constant {
                                    span: *span,
                                    scalar: Scalar::Boolean(is_or),
                                    data_type: DataType::Boolean,
                                }),
                                None,
                            );
                        }
                    }
                    args_expr.push(expr);

                    result_domain = result_domain.zip(domain).map(|(func_domain, domain)| {
                        let (domain_has_true, domain_has_false) = match &domain {
                            Domain::Boolean(boolean_domain) => {
                                (boolean_domain.has_true, boolean_domain.has_false)
                            }
                            Domain::Nullable(nullable_domain) => match &nullable_domain.value {
                                Some(inner_domain) => {
                                    let boolean_domain = inner_domain.as_boolean().unwrap();
                                    (
                                        boolean_domain.has_true,
                                        nullable_domain.has_null || boolean_domain.has_false,
                                    )
                                }
                                None => (false, true),
                            },
                            _ => unreachable!(),
                        };
                        let (has_true, has_false) = if is_or {
                            (
                                func_domain.has_true || domain_has_true,
                                func_domain.has_false && domain_has_false,
                            )
                        } else {
                            (
                                func_domain.has_true && domain_has_true,
                                func_domain.has_false || domain_has_false,
                            )
                        };
                        BooleanDomain {
                            has_true,
                            has_false,
                        }
                    });

                    if let Some(Scalar::Boolean(result)) = result_domain
                        .as_ref()
                        .and_then(|domain| Domain::Boolean(*domain).as_singleton())
                    {
                        if is_or == result {
                            return (
                                Expr::Constant(Constant {
                                    span: *span,
                                    scalar: Scalar::Boolean(result),
                                    data_type: DataType::Boolean,
                                }),
                                None,
                            );
                        }
                    }
                }

                // Check for mutually exclusive ranges in AND filters
                if !is_or && args_expr.len() >= 2 {
                    if let Some(is_mutually_exclusive) =
                        self.check_mutually_exclusive_ranges(&args_expr)
                    {
                        if is_mutually_exclusive {
                            return (
                                Expr::Constant(Constant {
                                    span: *span,
                                    scalar: Scalar::Boolean(false),
                                    data_type: DataType::Boolean,
                                }),
                                None,
                            );
                        }
                    }
                }

                if let Some(scalar) = result_domain
                    .as_ref()
                    .and_then(|domain| Domain::Boolean(*domain).as_singleton())
                {
                    return (
                        Expr::Constant(Constant {
                            span: *span,
                            scalar,
                            data_type: DataType::Boolean,
                        }),
                        None,
                    );
                }

                let all_args_is_scalar = args_expr.iter().all(|arg| arg.as_constant().is_some());

                let func_expr = Expr::FunctionCall(FunctionCall {
                    span: *span,
                    id: id.clone(),
                    function: function.clone(),
                    generics: generics.clone(),
                    args: args_expr,
                    return_type: return_type.clone(),
                });

                if all_args_is_scalar {
                    let block = DataBlock::empty_with_rows(1);
                    let evaluator = Evaluator::new(&block, self.func_ctx, self.fn_registry);
                    // Since we know the expression is constant, it'll be safe to change its column index type.
                    let func_expr = func_expr.project_column_ref(|_| unreachable!()).unwrap();
                    if let Ok(Value::Scalar(scalar)) = evaluator.run(&func_expr) {
                        return (
                            Expr::Constant(Constant {
                                span: *span,
                                scalar,
                                data_type: return_type.clone(),
                            }),
                            None,
                        );
                    }
                }

                (func_expr, result_domain.map(Domain::Boolean))
            }
            Expr::FunctionCall(FunctionCall {
                span,
                id,
                function,
                generics,
                args,
                return_type,
            }) => {
                if args.len() > MAX_FUNCTION_ARGS_TO_FOLD {
                    return (expr.clone(), None);
                }

                let (mut args_expr, mut args_domain) = (
                    Vec::with_capacity(args.len()),
                    Some(Vec::with_capacity(args.len())),
                );
                for arg in args {
                    let (expr, domain) = self.fold_once(arg);
                    args_expr.push(expr);
                    args_domain = args_domain.zip(domain).map(|(mut domains, domain)| {
                        domains.push(domain);
                        domains
                    });
                }
                let all_args_is_scalar = args_expr.iter().all(|arg| arg.as_constant().is_some());
                let is_monotonicity = self
                    .fn_registry
                    .properties
                    .get(&function.signature.name)
                    .map(|p| {
                        args_expr.len() == 1
                            && (p.monotonicity
                                || p.monotonicity_by_type.contains(args_expr[0].data_type()))
                    })
                    .unwrap_or_default();

                // Check for mutually exclusive ranges in AND function
                if function.signature.name == "and"
                    && args_expr.len() >= 2
                    && args_expr
                        .iter()
                        .all(|arg| !arg.data_type().is_nullable_or_null())
                {
                    if let Some(is_mutually_exclusive) =
                        self.check_mutually_exclusive_ranges(&args_expr)
                    {
                        if is_mutually_exclusive {
                            return (
                                Expr::Constant(Constant {
                                    span: *span,
                                    scalar: Scalar::Boolean(false),
                                    data_type: DataType::Boolean,
                                }),
                                None,
                            );
                        }
                    }
                }

                let func_expr = Expr::FunctionCall(FunctionCall {
                    span: *span,
                    id: id.clone(),
                    function: function.clone(),
                    generics: generics.clone(),
                    args: args_expr,
                    return_type: return_type.clone(),
                });

                let (calc_domain, eval) = match &function.eval {
                    FunctionEval::Scalar {
                        calc_domain, eval, ..
                    } => (calc_domain, eval),
                    FunctionEval::SRF { .. } => {
                        return (func_expr, None);
                    }
                };

                let func_domain = args_domain.and_then(|domains: Vec<Domain>| {
                    let res = calc_domain.domain_eval(self.func_ctx, &domains);
                    match (res, is_monotonicity) {
                        (FunctionDomain::MayThrow | FunctionDomain::Full, true) => {
                            let (min, max) = domains.iter().map(Domain::to_minmax).next().unwrap();

                            if (min.is_null() || max.is_null())
                                && !args[0].data_type().is_nullable_or_null()
                            {
                                None
                            } else {
                                let mut ctx = EvalContext {
                                    generics,
                                    num_rows: 2,
                                    validity: None,
                                    errors: None,
                                    func_ctx: self.func_ctx,
                                    suppress_error: false,
                                    strict_eval: true,
                                };
                                let mut builder =
                                    ColumnBuilder::with_capacity(args[0].data_type(), 2);
                                builder.push(min.as_ref());
                                builder.push(max.as_ref());

                                let input = Value::Column(builder.build());
                                let result = eval.eval(&[input], &mut ctx);

                                if result.is_scalar() {
                                    None
                                } else {
                                    // if error happens, domain maybe incorrect
                                    // min, max: String("2024-09-02 00:00") String("2024-09-02 00:0�")
                                    // to_date(s) > to_date('2024-01-1')
                                    let col = result.as_column().unwrap();
                                    let d = if ctx.has_error(0) || ctx.has_error(1) {
                                        let (full_min, full_max) =
                                            Domain::full(return_type).to_minmax();
                                        if full_min.is_null() || full_max.is_null() {
                                            return None;
                                        }

                                        let mut builder =
                                            ColumnBuilder::with_capacity(return_type, 2);

                                        for (i, (v, f)) in
                                            col.iter().zip([full_min, full_max].iter()).enumerate()
                                        {
                                            if ctx.has_error(i) {
                                                builder.push(f.as_ref());
                                            } else {
                                                builder.push(v);
                                            }
                                        }
                                        builder.build().domain()
                                    } else {
                                        result.as_column().unwrap().domain()
                                    };
                                    let (min, max) = d.to_minmax();
                                    Some(Domain::from_min_max(min, max, return_type))
                                }
                            }
                        }
                        (FunctionDomain::MayThrow, _) => None,
                        (FunctionDomain::Full, _) => Some(Domain::full(return_type)),
                        (FunctionDomain::Domain(domain), _) => Some(domain),
                    }
                });

                if let Some(scalar) = func_domain.as_ref().and_then(Domain::as_singleton) {
                    return (
                        Expr::Constant(Constant {
                            span: *span,
                            scalar,
                            data_type: return_type.clone(),
                        }),
                        None,
                    );
                }

                if all_args_is_scalar {
                    let block = DataBlock::empty_with_rows(1);
                    let evaluator = Evaluator::new(&block, self.func_ctx, self.fn_registry);
                    // Since we know the expression is constant, it'll be safe to change its column index type.
                    let func_expr = func_expr.project_column_ref(|_| unreachable!()).unwrap();
                    if let Ok(Value::Scalar(scalar)) = evaluator.run(&func_expr) {
                        return (
                            Expr::Constant(Constant {
                                span: *span,
                                scalar,
                                data_type: return_type.clone(),
                            }),
                            None,
                        );
                    }
                }

                (func_expr, func_domain)
            }
            Expr::LambdaFunctionCall(LambdaFunctionCall {
                span,
                name,
                args,
                lambda_expr,
                lambda_display,
                return_type,
            }) => {
                if args.len() > MAX_FUNCTION_ARGS_TO_FOLD {
                    return (expr.clone(), None);
                }

                let mut args_expr = Vec::with_capacity(args.len());
                for arg in args {
                    let (expr, _) = self.fold_once(arg);
                    args_expr.push(expr);
                }
                let all_args_is_scalar = args_expr.iter().all(|arg| arg.as_constant().is_some());

                let func_expr = Expr::LambdaFunctionCall(LambdaFunctionCall {
                    span: *span,
                    name: name.clone(),
                    args: args_expr,
                    lambda_expr: lambda_expr.clone(),
                    lambda_display: lambda_display.clone(),
                    return_type: return_type.clone(),
                });

                if all_args_is_scalar {
                    let block = DataBlock::empty_with_rows(1);
                    let evaluator = Evaluator::new(&block, self.func_ctx, self.fn_registry);
                    // Since we know the expression is constant, it'll be safe to change its column index type.
                    let func_expr = func_expr.project_column_ref(|_| unreachable!()).unwrap();
                    if let Ok(Value::Scalar(scalar)) = evaluator.run(&func_expr) {
                        return (
                            Expr::Constant(Constant {
                                span: *span,
                                scalar,
                                data_type: return_type.clone(),
                            }),
                            None,
                        );
                    }
                }
                (func_expr, None)
            }
        };

        debug_assert_eq!(expr.data_type(), new_expr.data_type());

        (new_expr, domain)
    }

    fn calculate_cast(
        &self,
        span: Span,
        src_type: &DataType,
        dest_type: &DataType,
        domain: &Domain,
    ) -> Option<Domain> {
        if src_type == dest_type {
            return Some(domain.clone());
        }

        if let Some(cast_fn) = get_simple_cast_function(false, src_type, dest_type) {
            if let Some(new_domain) =
                self.calculate_simple_cast(span, src_type, dest_type, domain, &cast_fn)
            {
                return new_domain;
            }
        }

        match (src_type, dest_type) {
            (DataType::Null, DataType::Nullable(_)) => Some(domain.clone()),
            (DataType::Nullable(inner_src_ty), DataType::Nullable(inner_dest_ty)) => {
                let domain = domain.as_nullable().unwrap();
                let value = match &domain.value {
                    Some(value) => Some(Box::new(self.calculate_cast(
                        span,
                        inner_src_ty,
                        inner_dest_ty,
                        value,
                    )?)),
                    None => None,
                };
                Some(Domain::Nullable(NullableDomain {
                    has_null: domain.has_null,
                    value,
                }))
            }
            (_, DataType::Nullable(inner_dest_ty)) => Some(Domain::Nullable(NullableDomain {
                has_null: false,
                value: Some(Box::new(self.calculate_cast(
                    span,
                    src_type,
                    inner_dest_ty,
                    domain,
                )?)),
            })),

            (DataType::EmptyArray, DataType::Array(_)) => Some(domain.clone()),
            (DataType::Array(inner_src_ty), DataType::Array(inner_dest_ty)) => {
                let inner_domain = match domain.as_array().unwrap() {
                    Some(inner_domain) => Some(Box::new(self.calculate_cast(
                        span,
                        inner_src_ty,
                        inner_dest_ty,
                        inner_domain,
                    )?)),
                    None => None,
                };
                Some(Domain::Array(inner_domain))
            }

            (DataType::Tuple(fields_src_ty), DataType::Tuple(fields_dest_ty))
                if fields_src_ty.len() == fields_dest_ty.len() =>
            {
                Some(Domain::Tuple(
                    domain
                        .as_tuple()
                        .unwrap()
                        .iter()
                        .zip(fields_src_ty)
                        .zip(fields_dest_ty)
                        .map(|((field_domain, src_ty), dest_ty)| {
                            self.calculate_cast(span, src_ty, dest_ty, field_domain)
                        })
                        .collect::<Option<Vec<_>>>()?,
                ))
            }

            _ => None,
        }
    }

    fn calculate_try_cast(
        &self,
        span: Span,
        src_type: &DataType,
        dest_type: &DataType,
        domain: &Domain,
    ) -> Option<Domain> {
        if src_type == dest_type {
            return Some(domain.clone());
        }

        // The dest_type of `TRY_CAST` must be `Nullable`, which is guaranteed by the type checker.
        let inner_dest_type = &**dest_type.as_nullable().unwrap();

        if let Some(cast_fn) = get_simple_cast_function(true, src_type, inner_dest_type) {
            if let Some(new_domain) =
                self.calculate_simple_cast(span, src_type, dest_type, domain, &cast_fn)
            {
                return new_domain;
            }
        }

        match (src_type, inner_dest_type) {
            (DataType::Null, _) => Some(domain.clone()),
            (DataType::Nullable(inner_src_ty), _) => {
                let nullable_domain = domain.as_nullable().unwrap();
                match &nullable_domain.value {
                    Some(value) => {
                        let new_domain = self
                            .calculate_try_cast(span, inner_src_ty, dest_type, value)?
                            .into_nullable()
                            .unwrap();
                        Some(Domain::Nullable(NullableDomain {
                            has_null: nullable_domain.has_null || new_domain.has_null,
                            value: new_domain.value,
                        }))
                    }
                    None => Some(domain.clone()),
                }
            }
            (src_ty, inner_dest_ty) if src_ty == inner_dest_ty => {
                Some(Domain::Nullable(NullableDomain {
                    has_null: false,
                    value: Some(Box::new(domain.clone())),
                }))
            }

            (DataType::EmptyArray, DataType::Array(_)) => Some(Domain::Nullable(NullableDomain {
                has_null: false,
                value: Some(Box::new(domain.clone())),
            })),
            (DataType::Array(inner_src_ty), DataType::Array(inner_dest_ty)) => {
                let inner_domain = match domain.as_array().unwrap() {
                    Some(inner_domain) => Some(Box::new(self.calculate_try_cast(
                        span,
                        inner_src_ty,
                        inner_dest_ty,
                        inner_domain,
                    )?)),
                    None => None,
                };
                Some(Domain::Nullable(NullableDomain {
                    has_null: false,
                    value: Some(Box::new(Domain::Array(inner_domain))),
                }))
            }

            (DataType::Tuple(fields_src_ty), DataType::Tuple(fields_dest_ty))
                if fields_src_ty.len() == fields_dest_ty.len() =>
            {
                let fields_domain = domain.as_tuple().unwrap();
                let new_fields_domain = fields_domain
                    .iter()
                    .zip(fields_src_ty)
                    .zip(fields_dest_ty)
                    .map(|((domain, src_ty), dest_ty)| {
                        self.calculate_try_cast(span, src_ty, dest_ty, domain)
                    })
                    .collect::<Option<_>>()?;
                Some(Domain::Tuple(new_fields_domain))
            }

            _ => None,
        }
    }

    fn calculate_simple_cast(
        &self,
        span: Span,
        src_type: &DataType,
        dest_type: &DataType,
        domain: &Domain,
        cast_fn: &str,
    ) -> Option<Option<Domain>> {
        let expr = Expr::ColumnRef(ColumnRef {
            span,
            id: 0,
            data_type: src_type.clone(),
            display_name: String::new(),
        });

        let params = if let DataType::Decimal(ty) = dest_type {
            vec![
                Scalar::Number(NumberScalar::Int64(ty.precision() as _)),
                Scalar::Number(NumberScalar::Int64(ty.scale() as _)),
            ]
        } else {
            vec![]
        };
        let cast_expr = check_function(span, cast_fn, &params, &[expr], self.fn_registry).ok()?;

        if cast_expr.data_type() != dest_type {
            return None;
        }

        let (_, output_domain) = ConstantFolder::fold_with_domain(
            &cast_expr,
            &[(0, domain.clone())].into_iter().collect(),
            self.func_ctx,
            self.fn_registry,
        );

        Some(output_domain)
    }

    /// Check if AND expressions contain mutually exclusive range conditions
    /// Returns Some(true) if the expressions are mutually exclusive (should return false)
    /// Returns Some(false) if they are not mutually exclusive
    /// Returns None if analysis is inconclusive
    fn check_mutually_exclusive_ranges(&self, args: &[Expr<Index>]) -> Option<bool> {
        // Track constraints for each column
        let mut column_constraints: HashMap<Index, Vec<RangeConstraint<Index>>> = HashMap::new();

        // Extract constraints from each expression
        for arg in args {
            if let Some(constraint) = self.extract_range_constraint(arg) {
                column_constraints
                    .entry(constraint.column_id.clone())
                    .or_default()
                    .push(constraint);
            }
        }

        // Check for mutually exclusive constraints on each column
        for (_column_id, constraints) in column_constraints {
            if constraints.len() < 2 {
                continue;
            }

            // Check all pairs of constraints for mutual exclusion
            for i in 0..constraints.len() {
                for j in (i + 1)..constraints.len() {
                    if self.are_constraints_mutually_exclusive(&constraints[i], &constraints[j]) {
                        return Some(true); // Found mutually exclusive constraints
                    }
                }
            }
        }

        None // No conclusive mutual exclusion found
    }

    /// Extract range constraint from a comparison expression
    fn extract_range_constraint(&self, expr: &Expr<Index>) -> Option<RangeConstraint<Index>> {
        if let Expr::FunctionCall(FunctionCall { function, args, .. }) = expr {
            if args.len() != 2 {
                return None;
            }

            let op = function.signature.name.as_str();
            if !matches!(op, "gt" | "gte" | "lt" | "lte" | "eq" | "noteq") {
                return None;
            }

            // Try both orders: column op constant and constant op column
            if let (Some(column_ref), Some(constant)) =
                (args[0].as_column_ref(), args[1].as_constant())
            {
                return Some(RangeConstraint {
                    column_id: column_ref.id.clone(),
                    data_type: column_ref.data_type.clone(),
                    operator: op.to_string(),
                    constant: constant.scalar.clone(),
                    is_flipped: false,
                });
            } else if let (Some(constant), Some(column_ref)) =
                (args[0].as_constant(), args[1].as_column_ref())
            {
                // Flip the operator for constant op column
                let flipped_op = match op {
                    "gt" => "lt",
                    "gte" => "lte",
                    "lt" => "gt",
                    "lte" => "gte",
                    "eq" => "eq",
                    "noteq" => "noteq",
                    _ => return None,
                };
                return Some(RangeConstraint {
                    column_id: column_ref.id.clone(),
                    data_type: column_ref.data_type.clone(),
                    operator: flipped_op.to_string(),
                    constant: constant.scalar.clone(),
                    is_flipped: true,
                });
            }
        }
        None
    }

    /// Check if two range constraints are mutually exclusive
    pub fn are_constraints_mutually_exclusive(
        &self,
        c1: &RangeConstraint<Index>,
        c2: &RangeConstraint<Index>,
    ) -> bool {
        // Only check constraints on the same column with the same data type
        if c1.column_id != c2.column_id || c1.data_type != c2.data_type {
            return false;
        }

        // Check for patterns like: x > a AND x < b where a >= b
        // or x >= a AND x < b where a >= b
        // or x > a AND x <= b where a >= b
        // or x >= a AND x <= b where a > b
        match (c1.operator.as_str(), c2.operator.as_str()) {
            ("gt", "lt") => c1.constant >= c2.constant,
            ("lt", "gt") => c2.constant >= c1.constant,
            ("gt", "lte") => c1.constant >= c2.constant,
            ("lte", "gt") => c2.constant >= c1.constant,
            ("gte", "lt") => c1.constant >= c2.constant,
            ("lt", "gte") => c2.constant >= c1.constant,
            ("gte", "lte") => c1.constant > c2.constant,
            ("lte", "gte") => c2.constant > c1.constant,
            ("eq", "gt") => {
                // x = a AND x > b where a <= b
                c1.constant <= c2.constant
            }
            ("gt", "eq") => {
                // x > a AND x = b where b <= a
                c2.constant <= c1.constant
            }
            ("eq", "gte") => {
                // x = a AND x >= b where a < b
                c1.constant < c2.constant
            }
            ("gte", "eq") => {
                // x >= a AND x = b where b < a
                c2.constant < c1.constant
            }
            ("eq", "lt") => {
                // x = a AND x < b where a >= b
                c1.constant >= c2.constant
            }
            ("lt", "eq") => {
                // x < a AND x = b where b >= a
                c2.constant >= c1.constant
            }
            ("eq", "lte") => {
                // x = a AND x <= b where a > b
                c1.constant > c2.constant
            }
            ("lte", "eq") => {
                // x <= a AND x = b where b > a
                c2.constant > c1.constant
            }
            ("eq", "eq") => {
                // x = a AND x = b where a != b
                c1.constant != c2.constant
            }
            ("eq", "noteq") => {
                // x = a AND x != b where a == b
                c1.constant == c2.constant
            }
            ("noteq", "eq") => {
                // x != a AND x = b where a == b
                c1.constant == c2.constant
            }
            _ => false,
        }
    }

    #[cfg(test)]
    pub fn new_for_test(
        input_domains: &'a HashMap<Index, Domain>,
        func_ctx: &'a FunctionContext,
        fn_registry: &'a FunctionRegistry,
    ) -> Self {
        ConstantFolder {
            input_domains,
            func_ctx,
            fn_registry,
        }
    }
}

/// Represents a range constraint extracted from a comparison expression
#[derive(Debug, Clone)]
pub struct RangeConstraint<Index> {
    pub column_id: Index,
    pub data_type: DataType,
    pub operator: String, // "gt", "gte", "lt", "lte", "eq"
    pub constant: Scalar,
    pub is_flipped: bool, // true if original was constant op column
}
