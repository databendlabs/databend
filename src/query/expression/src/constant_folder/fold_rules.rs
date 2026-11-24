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
use crate::cast_plan::CastCall;
use crate::cast_plan::CastPlan;
use crate::cast_plan::CastStep;
use crate::cast_plan::resolve_cast_plan;
use crate::evaluator::Evaluator;
use crate::expression::ColumnRef;
use crate::expression::Expr;
use crate::expression::ExprVisitor;
use crate::expression::FunctionCall;
use crate::expression::LambdaFunctionCall;
use crate::expression::visit_expr;
use crate::property::Domain;
use crate::types::AnyType;
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
        src_type: &DataType,
        dest_type: &DataType,
        is_try: bool,
        domain: &Domain,
    ) -> Option<Domain> {
        let plan = resolve_cast_plan(src_type, dest_type, fn_registry)?;
        let (domain, may_fail) =
            Self::cast_plan_domain(func_ctx, fn_registry, &plan, is_try, domain)?;
        // The destination of `TRY_CAST` is nullable, so a failed row is NULL by now.
        debug_assert!(!may_fail);
        Some(domain)
    }

    /// Domain of the output of a cast plan, and whether a row may fail, which is only possible
    /// for `TRY_CAST`. A failed row becomes NULL at the nearest nullable destination, as in
    /// `Evaluator::run_cast_plan`. Returns `None` when the domain is unknown, or when a row of
    /// `CAST` may fail.
    fn cast_plan_domain(
        func_ctx: &FunctionContext,
        fn_registry: &FunctionRegistry,
        plan: &CastPlan,
        is_try: bool,
        domain: &Domain,
    ) -> Option<(Domain, bool)> {
        let dest_type = &plan.dest_type;
        let nested = |plan: &CastPlan, domain: &Domain| {
            Self::cast_plan_domain(func_ctx, fn_registry, plan, is_try, domain)
        };

        let (output, may_fail) = match &plan.step {
            CastStep::Identity => (domain.clone(), false),
            CastStep::Null if dest_type.is_nullable_or_null() => (domain.clone(), false),
            CastStep::Null => return None,
            CastStep::Function { value_call, .. } => {
                let src_type = &plan.src_type;
                // A function step with a nullable source keeps its NULLs.
                let (input, input_has_null) = match domain {
                    Domain::Nullable(NullableDomain { has_null, value })
                        if src_type.is_nullable() =>
                    {
                        (value.as_deref(), *has_null)
                    }
                    domain => (Some(domain), false),
                };
                let (value, has_null, may_fail) = match input {
                    Some(input) => {
                        let (output, may_fail) = match input.as_singleton() {
                            Some(value) => Self::cast_value_domain(
                                func_ctx,
                                fn_registry,
                                value_call,
                                &dest_type.remove_nullable(),
                                value,
                            ),
                            None => Self::cast_function_domain(func_ctx, value_call, input)?,
                        };
                        (output.value, input_has_null || output.has_null, may_fail)
                    }
                    None => (None, true, false),
                };
                // A row that fails is an error for `CAST`.
                if may_fail && !is_try {
                    return None;
                }
                if dest_type.is_nullable() {
                    let output = Domain::Nullable(NullableDomain { has_null, value });
                    (output, may_fail)
                } else {
                    debug_assert!(plan.rejects_null() || !has_null);
                    // A NULL is an error for `CAST`.
                    if has_null && !is_try {
                        return None;
                    }
                    // Known issue: when no row converts, e.g. `TRY_CAST(300 AS UInt8)`, there is no
                    // value domain, and the domain is unknown instead of a NULL at the nearest
                    // nullable destination. Conservative, so kept as before.
                    (*value?, may_fail || has_null)
                }
            }
            CastStep::Chain(steps) => {
                let mut output = domain.clone();
                let mut may_fail = false;
                for step in steps {
                    let (step_output, step_may_fail) = nested(step, &output)?;
                    output = step_output;
                    may_fail |= step_may_fail;
                }
                (output, may_fail)
            }
            CastStep::Nullable(inner) => {
                let domain = domain.as_nullable()?;
                match &domain.value {
                    Some(value) => {
                        let (value, may_fail) = nested(inner, value)?;
                        let output = Domain::Nullable(NullableDomain {
                            has_null: domain.has_null,
                            value: Some(Box::new(value)),
                        });
                        (output, may_fail)
                    }
                    None => (Domain::Nullable(domain.clone()), false),
                }
            }
            CastStep::FromNullable(inner) => {
                let domain = domain.as_nullable()?;
                // A NULL is an error for `CAST`.
                if domain.has_null && !is_try {
                    return None;
                }
                let (output, may_fail) = nested(inner, domain.value.as_ref()?)?;
                (output, may_fail || domain.has_null)
            }
            CastStep::ToNullable(inner) => {
                let (value, may_fail) = nested(inner, domain)?;
                let output = Domain::Nullable(NullableDomain {
                    has_null: false,
                    value: Some(Box::new(value)),
                });
                (output, may_fail)
            }
            CastStep::Empty => (domain.clone(), false),
            // The key-value tuples of a map convert as the elements of an array do.
            CastStep::Elements(inner) => {
                let (values, may_fail) = match domain {
                    Domain::Array(values) | Domain::Map(values) => match values {
                        Some(values) => {
                            let (values, may_fail) = nested(inner, values)?;
                            (Some(Box::new(values)), may_fail)
                        }
                        None => (None, false),
                    },
                    _ => return None,
                };
                let output = match dest_type {
                    DataType::Map(_) => Domain::Map(values),
                    _ => Domain::Array(values),
                };
                (output, may_fail)
            }
            CastStep::Tuple(fields) => {
                let mut may_fail = false;
                let fields = domain
                    .as_tuple()?
                    .iter()
                    .zip(fields)
                    .map(|(domain, field)| {
                        let (output, field_may_fail) = nested(field, domain)?;
                        may_fail |= field_may_fail;
                        Some(output)
                    })
                    .collect::<Option<Vec<_>>>()?;
                (Domain::Tuple(fields), may_fail)
            }
            CastStep::VariantToArray(_) | CastStep::VariantToMap(_) | CastStep::ToVector => {
                return None;
            }
        };

        if !may_fail {
            return Some((output, false));
        }
        if dest_type.is_nullable_or_null() {
            let value = match output {
                Domain::Nullable(output) => output.value,
                output => Some(Box::new(output)),
            };
            return Some((
                Domain::Nullable(NullableDomain {
                    has_null: true,
                    value,
                }),
                false,
            ));
        }
        // A failed row is an error for `CAST`, so the domain is unknown.
        is_try.then_some((output, true))
    }

    /// The output of `call`, the function of a cast step on a non-NULL value, for input in
    /// `input`: the values and whether one is NULL, and whether a row may fail.
    fn cast_function_domain(
        func_ctx: &FunctionContext,
        call: &CastCall,
        input: &Domain,
    ) -> Option<(NullableDomain<AnyType>, bool)> {
        let FunctionEval::Scalar { calc_domain, .. } = &call.function.eval else {
            return None;
        };
        let (domain, may_fail) =
            match calc_domain.domain_eval(func_ctx, std::slice::from_ref(input)) {
                FunctionDomain::MayThrow => (Domain::full(&call.return_type), true),
                FunctionDomain::MayThrowDomain(domain) => (domain, true),
                FunctionDomain::Full => (Domain::full(&call.return_type), false),
                FunctionDomain::Domain(domain) => (domain, false),
            };
        let output = match domain {
            Domain::Nullable(domain) => domain,
            domain => NullableDomain {
                has_null: false,
                value: Some(Box::new(domain)),
            },
        };
        Some((output, may_fail))
    }

    /// The exact output of `call`, the function of a cast step to `dest_type`, for one non-NULL
    /// value, by running it: the value, a NULL, or a failure.
    fn cast_value_domain(
        func_ctx: &FunctionContext,
        fn_registry: &FunctionRegistry,
        call: &CastCall,
        dest_type: &DataType,
        value: Scalar,
    ) -> (NullableDomain<AnyType>, bool) {
        let block = DataBlock::empty_with_rows(1);
        let evaluator = Evaluator::new(&block, func_ctx, fn_registry);
        let (value, may_fail) = match evaluator.run_cast_call(call, value) {
            Some(Scalar::Null) => (None, false),
            Some(value) => (Some(value.as_ref().domain(dest_type)), false),
            None => (None, true),
        };
        let output = NullableDomain {
            has_null: !may_fail && value.is_none(),
            value: value.map(Box::new),
        };
        (output, may_fail)
    }

    /// Evaluates an expression without column references. Returns `None` when it
    /// references a column, does not produce a scalar, or fails; a failing
    /// expression is left for runtime so that its error is reported there.
    /// Set-returning functions and `grouping` are not evaluated: the
    /// evaluator runs scalar functions only, and `grouping` is a placeholder
    /// that the SQL binder replaces with a call over the grouping id.
    pub fn eval_constant<Index: ColumnIndex>(
        func_ctx: &FunctionContext,
        fn_registry: &FunctionRegistry,
        expr: &Expr<Index>,
    ) -> Option<Scalar> {
        if needs_input(expr) {
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
            //
            // Known issue: for a call that may throw, the domain comes from the two ends of the
            // argument domain, and a value between them may still fail. The domain is then taken
            // as one that does not fail, so a call whose ends convert to the same value folds to
            // a constant, e.g. `to_date(s)` of the strings `2022-01-10`, `2022-01-10 99` and
            // `2022-01-10T12:00:00`, whose second row fails at runtime. The base had it too, and the
            // cast changes keep it: `CAST` does not take this path, and the SQL binder binds
            // `to_date(x)` as a cast.
            (
                FunctionDomain::MayThrow | FunctionDomain::MayThrowDomain(_) | FunctionDomain::Full,
                true,
            ) => calculate_monotonicity_domain(
                func_ctx,
                return_type,
                first_arg_type?,
                arg_domains.first()?,
                generics,
                eval.as_ref(),
            ),
            (FunctionDomain::MayThrow | FunctionDomain::MayThrowDomain(_), _) => None,
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

/// Whether `expr` references a column, calls a set-returning function or
/// calls the `grouping` placeholder, checked in one traversal. Any of them
/// keeps it from being evaluated alone.
fn needs_input<Index: ColumnIndex>(expr: &Expr<Index>) -> bool {
    struct NeedsInput(bool);
    impl<I: ColumnIndex> ExprVisitor<I> for NeedsInput {
        fn enter_column_ref(&mut self, _: &ColumnRef<I>) -> Result<Option<Expr<I>>, Self::Error> {
            self.0 = true;
            Ok(None)
        }

        fn enter_function_call(
            &mut self,
            call: &FunctionCall<I>,
        ) -> Result<Option<Expr<I>>, Self::Error> {
            if self.0
                || matches!(call.function.eval, FunctionEval::SRF { .. })
                || call.function.signature.name == "grouping"
            {
                self.0 = true;
                Ok(None)
            } else {
                Self::visit_function_call(call, self)
            }
        }

        fn enter_lambda_function_call(
            &mut self,
            call: &LambdaFunctionCall<I>,
        ) -> Result<Option<Expr<I>>, Self::Error> {
            if self.0 {
                Ok(None)
            } else {
                Self::visit_lambda_function_call(call, self)
            }
        }
    }

    let mut visitor = NeedsInput(false);
    visit_expr(expr, &mut visitor).unwrap();
    visitor.0
}
