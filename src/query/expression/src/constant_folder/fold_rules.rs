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

//! Node-local folding rules shared by expression representations.
//!
//! A rule consumes a resolved function, types and the facts of already folded
//! children, and returns a decision instead of a rebuilt tree, so a caller
//! with another tree type can apply the same decision to its own nodes. Only
//! [`FoldRules::eval_constant`] takes an expression, which must be free of columns.

use databend_common_ast::Span;

use super::monotonicity::calculate_monotonicity_domain;
use super::monotonicity::is_monotonic;
use crate::ColumnIndex;
use crate::Function;
use crate::FunctionContext;
use crate::FunctionDomain;
use crate::FunctionEval;
use crate::FunctionRegistry;
use crate::Scalar;
use crate::Value;
use crate::block::DataBlock;
use crate::evaluator::Evaluator;
use crate::expression::Cast;
use crate::expression::Constant;
use crate::expression::Expr;
use crate::expression::FunctionCall;
use crate::property::Domain;
use crate::type_check::resolve_cast_function;
use crate::types::DataType;
use crate::types::boolean::BooleanDomain;
use crate::types::nullable::NullableDomain;

/// The node-local folding rules. A unit type that only groups the rule
/// functions; it holds no state.
pub struct FoldRules;

impl FoldRules {
    /// Calls with more arguments are left unfolded to bound the folding cost.
    pub const MAX_FUNCTION_ARGS_TO_FOLD: usize = 4096;

    /// Domain of `CAST(expr AS dest_type)` (or `TRY_CAST`) from the domain of
    /// `expr`. Returns `None` when the cast may fail or its domain is unknown.
    /// For `TRY_CAST`, `dest_type` is the nullable result type, as produced by
    /// the type checker.
    pub fn cast_domain(
        func_ctx: &FunctionContext,
        fn_registry: &FunctionRegistry,
        span: Span,
        src_type: &DataType,
        dest_type: &DataType,
        is_try: bool,
        domain: &Domain,
    ) -> Option<Domain> {
        if is_try {
            Self::calculate_try_cast(func_ctx, fn_registry, span, src_type, dest_type, domain)
        } else {
            Self::calculate_cast(func_ctx, fn_registry, span, src_type, dest_type, domain)
        }
    }

    pub(super) fn calculate_cast(
        func_ctx: &FunctionContext,
        fn_registry: &FunctionRegistry,
        span: Span,
        src_type: &DataType,
        dest_type: &DataType,
        domain: &Domain,
    ) -> Option<Domain> {
        if src_type == dest_type {
            return Some(domain.clone());
        }

        if let Some(call) = resolve_cast_function(span, false, src_type, dest_type, fn_registry)
            && let Some(new_domain) =
                Self::calculate_simple_cast(func_ctx, fn_registry, call, domain)
        {
            return new_domain;
        }

        match (src_type, dest_type) {
            (DataType::Null, DataType::Nullable(_)) => Some(domain.clone()),
            (DataType::Nullable(inner_src_ty), DataType::Nullable(inner_dest_ty)) => {
                let domain = domain.as_nullable().unwrap();
                let value = match &domain.value {
                    Some(value) => Some(Box::new(Self::calculate_cast(
                        func_ctx,
                        fn_registry,
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
                value: Some(Box::new(Self::calculate_cast(
                    func_ctx,
                    fn_registry,
                    span,
                    src_type,
                    inner_dest_ty,
                    domain,
                )?)),
            })),

            (DataType::EmptyArray, DataType::Array(_)) => Some(domain.clone()),
            (DataType::Array(inner_src_ty), DataType::Array(inner_dest_ty)) => {
                let inner_domain = match domain.as_array().unwrap() {
                    Some(inner_domain) => Some(Box::new(Self::calculate_cast(
                        func_ctx,
                        fn_registry,
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
                            Self::calculate_cast(
                                func_ctx,
                                fn_registry,
                                span,
                                src_ty,
                                dest_ty,
                                field_domain,
                            )
                        })
                        .collect::<Option<Vec<_>>>()?,
                ))
            }

            _ => None,
        }
    }

    pub(super) fn calculate_try_cast(
        func_ctx: &FunctionContext,
        fn_registry: &FunctionRegistry,
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

        if let Some(call) = resolve_cast_function(span, true, src_type, dest_type, fn_registry)
            && let Some(new_domain) =
                Self::calculate_simple_cast(func_ctx, fn_registry, call, domain)
        {
            return new_domain;
        }

        match (src_type, inner_dest_type) {
            (DataType::Null, _) => Some(domain.clone()),
            (DataType::Nullable(inner_src_ty), _) => {
                let nullable_domain = domain.as_nullable().unwrap();
                match &nullable_domain.value {
                    Some(value) => {
                        let new_domain = Self::calculate_try_cast(
                            func_ctx,
                            fn_registry,
                            span,
                            inner_src_ty,
                            dest_type,
                            value,
                        )?
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
                    Some(inner_domain) => Some(Box::new(Self::calculate_try_cast(
                        func_ctx,
                        fn_registry,
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
                        Self::calculate_try_cast(
                            func_ctx,
                            fn_registry,
                            span,
                            src_ty,
                            dest_ty,
                            domain,
                        )
                    })
                    .collect::<Option<_>>()?;
                Some(Domain::Tuple(new_fields_domain))
            }

            _ => None,
        }
    }

    /// Domain of a call built by `resolve_cast_function`, which reads the
    /// cast input as column 0, directly or through an implicit cast. The
    /// result follows the domain rules of both; when the argument has a
    /// single value, the call is evaluated on it.
    fn calculate_simple_cast(
        func_ctx: &FunctionContext,
        fn_registry: &FunctionRegistry,
        call: FunctionCall,
        domain: &Domain,
    ) -> Option<Option<Domain>> {
        let [arg] = call.args.as_slice() else {
            return Some(None);
        };
        let (arg_domain, arg_value) = match arg {
            Expr::ColumnRef(_) => (Some(domain.clone()), domain.as_singleton()),
            Expr::Cast(cast) if matches!(cast.expr.as_ref(), Expr::ColumnRef(_)) => {
                let (src_type, dest_type) = (cast.expr.data_type(), &cast.dest_type);
                let arg_domain = if cast.is_try {
                    Self::calculate_try_cast(
                        func_ctx,
                        fn_registry,
                        cast.span,
                        src_type,
                        dest_type,
                        domain,
                    )
                } else {
                    Self::calculate_cast(
                        func_ctx,
                        fn_registry,
                        cast.span,
                        src_type,
                        dest_type,
                        domain,
                    )
                };
                let input_value = domain.as_singleton().and_then(|value| {
                    let cast = Expr::Cast(Cast {
                        expr: Box::new(Self::constant_of(&cast.expr, value)),
                        ..cast.clone()
                    });
                    Self::eval_constant(func_ctx, fn_registry, &cast)
                });
                let arg_value = input_value.or_else(|| arg_domain.as_ref()?.as_singleton());
                (arg_domain, arg_value)
            }
            _ => return Some(None),
        };

        let return_type = &call.return_type;
        let output_domain = arg_domain.and_then(|arg_domain| {
            Self::function_domain(
                func_ctx,
                fn_registry,
                &call.function,
                &call.generics,
                return_type,
                Some(arg.data_type()),
                &[arg_domain],
            )
        });
        if let Some(value) = output_domain.as_ref().and_then(Domain::as_singleton) {
            return Some(Some(value.as_ref().domain(return_type)));
        }
        let evaluated = arg_value.and_then(|value| {
            let call = Expr::FunctionCall(FunctionCall {
                args: vec![Self::constant_of(arg, value)],
                ..call.clone()
            });
            Self::eval_constant(func_ctx, fn_registry, &call)
        });
        match evaluated {
            Some(value) => Some(Some(value.as_ref().domain(return_type))),
            None => Some(output_domain),
        }
    }

    /// `value` in place of `expr`, with its type.
    fn constant_of<Index: ColumnIndex>(expr: &Expr<Index>, value: Scalar) -> Expr<Index> {
        Expr::Constant(Constant {
            span: expr.span(),
            scalar: value,
            data_type: expr.data_type().clone(),
        })
    }

    /// Evaluates an expression without column references. Returns `None` when it
    /// references a column, does not produce a scalar, or fails; a failing
    /// expression is left for runtime so that its error is reported there.
    pub fn eval_constant<Index: ColumnIndex>(
        func_ctx: &FunctionContext,
        fn_registry: &FunctionRegistry,
        expr: &Expr<Index>,
    ) -> Option<Scalar> {
        if expr.contains_column_ref() {
            return None;
        }
        let block = DataBlock::empty_with_rows(1);
        let evaluator = Evaluator::new(&block, func_ctx, fn_registry);
        // Without column references, the column index type can be changed freely.
        let projected_expr: Expr = expr.project_column_ref(|_| unreachable!()).ok()?;
        match evaluator.run(&projected_expr) {
            Ok(Value::Scalar(scalar)) => Some(scalar),
            _ => None,
        }
    }

    /// Output domain of a scalar function call from the domains of its arguments.
    ///
    /// `arg_domains` holds one domain per argument, each already describing the
    /// argument as the function receives it. Returns `None` when the call may
    /// throw or its domain is unknown, and for set-returning functions.
    pub fn function_domain(
        func_ctx: &FunctionContext,
        fn_registry: &FunctionRegistry,
        function: &Function,
        generics: &[DataType],
        return_type: &DataType,
        first_arg_type: Option<&DataType>,
        arg_domains: &[Domain],
    ) -> Option<Domain> {
        let FunctionEval::Scalar {
            calc_domain, eval, ..
        } = &function.eval
        else {
            return None;
        };

        let res = calc_domain.domain_eval(func_ctx, arg_domains);
        let monotonic = is_monotonic(
            func_ctx,
            fn_registry,
            &function.signature.name,
            arg_domains.len(),
            first_arg_type,
            arg_domains,
        );
        match (res, monotonic) {
            // `is_monotonic` only holds for exactly one argument.
            (FunctionDomain::MayThrow | FunctionDomain::Full, true) => {
                calculate_monotonicity_domain(
                    func_ctx,
                    return_type,
                    first_arg_type?,
                    arg_domains.first()?,
                    generics,
                    eval.as_ref(),
                )
            }
            (FunctionDomain::MayThrow, _) => None,
            (FunctionDomain::Full, _) => Some(Domain::full(return_type)),
            (FunctionDomain::Domain(domain), _) => Some(domain),
        }
    }

    /// Decides how constant conditions simplify an `if` call with `arg_count`
    /// arguments. `constant_arg` returns the constant value of an argument, if any.
    ///
    /// The caller must only pass a well-formed call: an odd number of at least
    /// three arguments.
    pub fn if_pruning<'a>(
        arg_count: usize,
        constant_arg: impl Fn(usize) -> Option<&'a Scalar>,
    ) -> IfPruning {
        debug_assert!(arg_count >= 3 && !arg_count.is_multiple_of(2));

        let mut kept = Vec::with_capacity(arg_count);
        let mut found_true_branch = false;
        for cond_idx in (0..arg_count - 1).step_by(2) {
            match constant_arg(cond_idx) {
                Some(Scalar::Boolean(true)) => {
                    if kept.is_empty() {
                        return IfPruning::Select(cond_idx + 1);
                    }
                    kept.push(cond_idx + 1);
                    found_true_branch = true;
                    break;
                }
                Some(Scalar::Boolean(false) | Scalar::Null) => {}
                _ => {
                    kept.push(cond_idx);
                    kept.push(cond_idx + 1);
                }
            }
        }

        if kept.is_empty() {
            return IfPruning::Select(arg_count - 1);
        }
        if !found_true_branch {
            kept.push(arg_count - 1);
        }
        if kept.len() == arg_count {
            IfPruning::Unchanged
        } else {
            IfPruning::Keep(kept)
        }
    }
}

/// Result of `and_filters` (`is_or == false`) or `or_filters` (`is_or == true`),
/// accumulated one argument at a time.
///
/// Filters treat NULL as false, and evaluation stops at the first argument
/// that decides the result, so later arguments are irrelevant once it does.
#[derive(Clone, Copy, Debug)]
pub struct FilterDomain {
    is_or: bool,
    domain: Option<BooleanDomain>,
}

impl FilterDomain {
    pub fn new(is_or: bool) -> Self {
        // Start with the operator identity: false for OR, true for AND.
        // Starting with both values possible would pin OR's has_true,
        // even when every argument is known to be false or NULL.
        Self {
            is_or,
            domain: Some(BooleanDomain {
                has_true: !is_or,
                has_false: is_or,
            }),
        }
    }

    /// The result every absorbing argument forces: true for OR, false for AND.
    pub fn absorbing_value(&self) -> bool {
        self.is_or
    }

    /// Whether a constant argument decides the result.
    pub fn is_absorbing(&self, constant: &Scalar) -> bool {
        matches!(constant, Scalar::Boolean(value) if *value == self.is_or)
    }

    /// Adds the next argument. An unknown argument domain makes the
    /// accumulated domain unknown.
    pub fn add(&mut self, domain: Option<Domain>) {
        let is_or = self.is_or;
        self.domain = self.domain.zip(domain).map(|(func_domain, domain)| {
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
    }

    /// The only possible result of the arguments added so far.
    pub fn singleton(&self) -> Option<bool> {
        match self.domain.map(Domain::Boolean)?.as_singleton() {
            Some(Scalar::Boolean(value)) => Some(value),
            _ => None,
        }
    }

    pub fn domain(&self) -> Option<BooleanDomain> {
        self.domain
    }
}

/// Simplification of `if(cond0, then0, ..., else)` by constant conditions.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum IfPruning {
    /// No condition is constant enough to drop an argument.
    Unchanged,
    /// The call always yields the argument at this index.
    Select(usize),
    /// Only the arguments at these indices, in order, can affect the result;
    /// they form a well-formed `if` call.
    Keep(Vec<usize>),
}
