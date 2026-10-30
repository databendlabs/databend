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

//! Bind-time folding applied directly to `ScalarExpr`.
//!
//! Children fold before their parent is type checked, so the parent is checked
//! once, on its folded arguments. A bound node carries [`ScalarFacts`] about
//! itself, so its parent looks at only one level: each child is represented by
//! a placeholder of its type instead of its lowered subtree. Without
//! statistics, the effective rules are few: constant evaluation, single
//! values implied by types and nullability, filter absorption, contradictory
//! ranges and `if` branch selection. They come from
//! `databend_common_expression::FoldRules`.

use databend_common_ast::Span;
use databend_common_exception::Result;
use databend_common_expression::Constant;
use databend_common_expression::Domain;
use databend_common_expression::Expr as EExpr;
use databend_common_expression::FilterDomain;
use databend_common_expression::FoldRules;
use databend_common_expression::FunctionEval;
use databend_common_expression::IfPruning;
use databend_common_expression::RangeConstraint;
use databend_common_expression::RawExpr;
use databend_common_expression::Scalar;
use databend_common_expression::expr;
use databend_common_expression::shrink_scalar;
use databend_common_expression::type_check;
use databend_common_expression::types::DataType;
use databend_common_functions::BUILTIN_FUNCTIONS;

use super::TypeCheckAdapter;
use super::TypeChecker;
use crate::ColumnBinding;
use crate::Symbol;
use crate::plans::CastExpr;
use crate::plans::ConstantExpr;
use crate::plans::FunctionCall;
use crate::plans::ScalarExpr;

/// What bind-time folding knows about a bound expression.
#[derive(Clone, Debug)]
pub(super) struct ScalarFacts {
    /// Values the expression can take; `None` when unknown.
    pub domain: Option<Domain>,
    /// Dropping the expression changes neither errors nor side effects.
    pub removable: bool,
    /// Same as determinism of the lowered expression.
    pub deterministic: bool,
    /// The lowered expression references no column, including the dummy
    /// columns that stand for aggregates, subqueries and UDFs.
    pub column_free: bool,
    /// Same as `ScalarExpr::evaluable`.
    pub evaluable: bool,
}

impl ScalarFacts {
    fn constant(value: &Scalar, data_type: &DataType) -> Self {
        Self {
            domain: Some(value.as_ref().domain(data_type)),
            removable: true,
            deterministic: true,
            column_free: true,
            evaluable: true,
        }
    }

    /// An expression computed outside the scalar evaluator. Its value can be
    /// anything of its type; `removable` tells whether dropping it is safe.
    fn computed_elsewhere(data_type: &DataType, removable: bool) -> Self {
        Self {
            domain: full_domain(data_type),
            removable,
            deterministic: true,
            column_free: false,
            evaluable: false,
        }
    }
}

/// A bound expression with its declared type and folding facts.
///
/// `data_type` is the type the producer reports, which callers of the
/// `(ScalarExpr, DataType)` interface receive. It can differ from
/// `scalar.data_type()`: `CAST` of a nullable tuple column to `VARIANT`
/// reports `Variant NULL` for a `json_object_keep_null` call returning
/// `Variant`.
#[derive(Clone, Debug)]
pub(super) struct BoundScalar {
    pub scalar: ScalarExpr,
    pub data_type: DataType,
    pub facts: ScalarFacts,
}

impl BoundScalar {
    pub(super) fn constant(constant: ConstantExpr, data_type: DataType) -> Self {
        let facts = ScalarFacts::constant(&constant.value, &data_type);
        Self {
            scalar: constant.into(),
            data_type,
            facts,
        }
    }

    pub(super) fn into_pair(self) -> Box<(ScalarExpr, DataType)> {
        Box::new((self.scalar, self.data_type))
    }
}

/// Facts of a checked argument, i.e. a child after implicit casts.
#[derive(Clone, Debug)]
pub(super) struct ArgFacts {
    pub domain: Option<Domain>,
    /// The only value of the argument, when it is known and can replace it.
    pub constant: Option<Scalar>,
    pub removable: bool,
    pub deterministic: bool,
}

fn full_domain(data_type: &DataType) -> Option<Domain> {
    // `Domain::full` is undefined for generic and stage location types.
    (!data_type.has_generic() && data_type.remove_nullable() != DataType::StageLocation)
        .then(|| Domain::full(data_type))
}

impl<'a, A> TypeChecker<'a, A>
where A: TypeCheckAdapter
{
    /// Attaches facts to an expression bound without them.
    pub(super) fn bind_derived(&self, scalar: ScalarExpr, data_type: DataType) -> BoundScalar {
        let facts = self.derive_facts(&scalar);
        BoundScalar {
            scalar,
            data_type,
            facts,
        }
    }

    pub(super) fn bind_derived_pair(&self, pair: Box<(ScalarExpr, DataType)>) -> BoundScalar {
        let (scalar, data_type) = *pair;
        self.bind_derived(scalar, data_type)
    }

    /// Computes facts of an expression from its structure. Used when a
    /// producer did not carry facts; it visits the whole expression.
    #[recursive::recursive]
    pub(super) fn derive_facts(&self, scalar: &ScalarExpr) -> ScalarFacts {
        match scalar {
            ScalarExpr::ConstantExpr(constant) => {
                ScalarFacts::constant(&constant.value, &constant.value.as_ref().infer_data_type())
            }
            ScalarExpr::TypedConstantExpr(constant, data_type) => {
                ScalarFacts::constant(&constant.value, data_type)
            }
            ScalarExpr::BoundColumnRef(column) => ScalarFacts {
                domain: full_domain(&column.column.data_type),
                removable: true,
                deterministic: true,
                column_free: false,
                evaluable: true,
            },
            // Aggregates and window functions are computed by their operators;
            // referring to one has no effect of its own.
            ScalarExpr::AggregateFunction(_) | ScalarExpr::WindowFunction(_) => {
                ScalarFacts::computed_elsewhere(&scalar.data_type(), true)
            }
            ScalarExpr::SubqueryExpr(_)
            | ScalarExpr::UDFCall(_)
            | ScalarExpr::UDAFCall(_)
            | ScalarExpr::AsyncFunctionCall(_) => {
                ScalarFacts::computed_elsewhere(&scalar.data_type(), false)
            }
            ScalarExpr::UDFLambdaCall(udf) => ScalarFacts {
                removable: false,
                evaluable: false,
                ..self.derive_facts(&udf.scalar)
            },
            ScalarExpr::LambdaFunction(lambda) => {
                let args = lambda
                    .args
                    .iter()
                    .map(|arg| self.derive_facts(arg))
                    .collect::<Vec<_>>();
                ScalarFacts {
                    domain: None,
                    removable: false,
                    deterministic: args.iter().all(|arg| arg.deterministic),
                    column_free: args.iter().all(|arg| arg.column_free),
                    evaluable: args.iter().all(|arg| arg.evaluable),
                }
            }
            ScalarExpr::CastExpr(cast) => {
                let facts = self.derive_facts(&cast.argument);
                self.cast_facts(
                    cast.span,
                    cast.is_try,
                    &cast.argument,
                    facts,
                    &cast.target_type,
                )
            }
            ScalarExpr::FunctionCall(func) => {
                let facts = func
                    .arguments
                    .iter()
                    .map(|arg| self.derive_facts(arg))
                    .collect::<Vec<_>>();
                let args = func.arguments.iter().zip(&facts).collect::<Vec<_>>();
                self.call_facts(func.span, &func.func_name, &func.params, &args)
            }
        }
    }

    /// Facts of `CAST(child AS target_type)` as a `CastExpr` lowers.
    pub(super) fn cast_facts(
        &self,
        span: Span,
        is_try: bool,
        child: &ScalarExpr,
        facts: ScalarFacts,
        target_type: &DataType,
    ) -> ScalarFacts {
        let raw = RawExpr::Cast {
            span,
            is_try,
            expr: Box::new(Self::placeholder(0, child, &facts)),
            dest_type: target_type.clone(),
        };
        let Ok(checked) = type_check::check(&raw, &BUILTIN_FUNCTIONS) else {
            return ScalarFacts {
                domain: None,
                removable: false,
                ..facts
            };
        };
        let arg = self.arg_facts(&checked, child, &facts);
        ScalarFacts {
            domain: arg.domain,
            removable: arg.removable,
            ..facts
        }
    }

    /// Facts of a function call over children with known facts, without
    /// folding it.
    fn call_facts(
        &self,
        span: Span,
        func_name: &str,
        params: &[Scalar],
        args: &[(&ScalarExpr, &ScalarFacts)],
    ) -> ScalarFacts {
        let column_free = args.iter().all(|(_, facts)| facts.column_free);
        let evaluable = args.iter().all(|(_, facts)| facts.evaluable);
        let unknown = ScalarFacts {
            domain: None,
            removable: false,
            deterministic: args.iter().all(|(_, facts)| facts.deterministic)
                && !BUILTIN_FUNCTIONS
                    .get_property(func_name)
                    .is_some_and(|property| property.non_deterministic),
            column_free,
            evaluable,
        };
        let raw = RawExpr::FunctionCall {
            span,
            name: func_name.to_string(),
            params: params.to_vec(),
            args: args
                .iter()
                .enumerate()
                .map(|(index, (scalar, facts))| Self::placeholder(index, scalar, facts))
                .collect(),
        };
        let Ok(checked) = type_check::check(&raw, &BUILTIN_FUNCTIONS) else {
            return unknown;
        };
        match &checked {
            EExpr::FunctionCall(call) if Self::is_grouping(call) => {
                ScalarFacts::computed_elsewhere(&call.return_type, false)
            }
            EExpr::FunctionCall(call) => {
                let arg_facts = self.call_arg_facts(call, args.iter().copied());
                let deterministic = Self::is_call_deterministic(call, &arg_facts);
                let is_srf = matches!(call.function.eval, FunctionEval::SRF { .. });
                let domain = if is_srf {
                    None
                } else {
                    self.call_domain(call, &arg_facts)
                };
                ScalarFacts {
                    removable: domain.is_some() && arg_facts.iter().all(|arg| arg.removable),
                    domain,
                    deterministic,
                    column_free,
                    evaluable: evaluable && !is_srf,
                }
            }
            // Identity calls and simple casts relate to their only argument.
            _ if args.len() == 1 => {
                let arg = self.arg_facts(&checked, args[0].0, args[0].1);
                ScalarFacts {
                    domain: arg.domain,
                    removable: arg.removable,
                    deterministic: arg.deterministic,
                    column_free,
                    evaluable,
                }
            }
            _ => unknown,
        }
    }

    pub(super) fn call_arg_facts<'x>(
        &self,
        call: &expr::FunctionCall<usize>,
        args: impl IntoIterator<Item = (&'x ScalarExpr, &'x ScalarFacts)>,
    ) -> Vec<ArgFacts> {
        call.args
            .iter()
            .zip(args)
            .map(|(checked, (scalar, facts))| self.arg_facts(checked, scalar, facts))
            .collect()
    }

    /// `grouping` is a placeholder that the aggregate rewriter replaces with
    /// a call over the grouping id, so it is computed by the aggregate
    /// operator; evaluating the placeholder panics. It is not removable, so
    /// that the rewriter still sees and validates it, e.g. `GROUPING()`.
    fn is_grouping(call: &expr::FunctionCall<usize>) -> bool {
        call.function.signature.name == "grouping"
    }

    pub(super) fn is_call_deterministic(
        call: &expr::FunctionCall<usize>,
        arg_facts: &[ArgFacts],
    ) -> bool {
        !BUILTIN_FUNCTIONS
            .get_property(&call.function.signature.name)
            .is_some_and(|property| property.non_deterministic)
            && arg_facts.iter().all(|arg| arg.deterministic)
    }

    /// Domain of a scalar call from the domains of its checked arguments.
    pub(super) fn call_domain(
        &self,
        call: &expr::FunctionCall<usize>,
        arg_facts: &[ArgFacts],
    ) -> Option<Domain> {
        let domains = arg_facts
            .iter()
            .map(|arg| arg.domain.clone())
            .collect::<Option<Vec<_>>>()?;
        FoldRules::function_domain(
            &self.func_ctx,
            &BUILTIN_FUNCTIONS,
            &call.function,
            &call.generics,
            &call.return_type,
            call.args.first().map(EExpr::data_type),
            &domains,
        )
    }

    /// Facts of a checked argument: `checked` is the placeholder of `child`,
    /// possibly wrapped in implicit casts.
    pub(super) fn arg_facts(
        &self,
        checked: &EExpr<usize>,
        child: &ScalarExpr,
        facts: &ScalarFacts,
    ) -> ArgFacts {
        let mut arg = self.wrapped_facts(checked, &child.data_type(), facts);
        if arg.constant.is_none() && arg.deterministic {
            arg.constant = if facts.column_free && facts.evaluable {
                // A column-free child is lowered as a whole, so the checked
                // argument can be evaluated directly.
                FoldRules::eval_constant(&self.func_ctx, &BUILTIN_FUNCTIONS, checked)
            } else if arg.removable {
                arg.domain.as_ref().and_then(Domain::as_singleton)
            } else {
                None
            };
            if let Some(constant) = &arg.constant {
                arg.domain = Some(constant.as_ref().domain(checked.data_type()));
                arg.removable = true;
            }
        }
        arg
    }

    fn wrapped_facts(
        &self,
        checked: &EExpr<usize>,
        child_type: &DataType,
        facts: &ScalarFacts,
    ) -> ArgFacts {
        match checked {
            EExpr::Constant(constant) => ArgFacts {
                domain: Some(constant.scalar.as_ref().domain(&constant.data_type)),
                constant: Some(constant.scalar.clone()),
                removable: true,
                deterministic: true,
            },
            _ if checked.data_type() == child_type => ArgFacts {
                domain: facts.domain.clone(),
                constant: None,
                removable: facts.removable,
                deterministic: facts.deterministic,
            },
            EExpr::Cast(cast) => {
                let inner = self.wrapped_facts(&cast.expr, child_type, facts);
                let domain = inner.domain.as_ref().and_then(|domain| {
                    FoldRules::cast_domain(
                        &self.func_ctx,
                        &BUILTIN_FUNCTIONS,
                        cast.span,
                        cast.expr.data_type(),
                        &cast.dest_type,
                        cast.is_try,
                        domain,
                    )
                });
                ArgFacts {
                    removable: inner.removable && domain.is_some(),
                    domain,
                    constant: None,
                    deterministic: inner.deterministic,
                }
            }
            _ => ArgFacts {
                domain: None,
                constant: None,
                removable: false,
                deterministic: facts.deterministic,
            },
        }
    }

    /// Represents a child for a one-level type check. Constants and
    /// column-free children keep their lowered form, so that constant
    /// coercion and overload ranking see the same arguments as a full
    /// lowering; any other child becomes a column of its type, keeping an
    /// explicit cast on top because overload ranking looks at it.
    pub(super) fn placeholder(
        index: usize,
        child: &ScalarExpr,
        facts: &ScalarFacts,
    ) -> RawExpr<usize> {
        let column = |span: Span, data_type: DataType| RawExpr::ColumnRef {
            span,
            id: index,
            data_type,
            display_name: format!("#{index}"),
        };
        match child {
            ScalarExpr::ConstantExpr(constant) => RawExpr::Constant {
                span: constant.span,
                scalar: constant.value.clone(),
                data_type: None,
            },
            ScalarExpr::TypedConstantExpr(constant, data_type) => RawExpr::Constant {
                span: constant.span,
                scalar: constant.value.clone(),
                data_type: Some(data_type.clone()),
            },
            _ if facts.column_free => {
                child
                    .as_raw_expr()
                    .project_column_ref(|_: &ColumnBinding| -> usize {
                        unreachable!("a column-free expression has no column reference")
                    })
            }
            ScalarExpr::CastExpr(cast) => RawExpr::Cast {
                span: cast.span,
                is_try: cast.is_try,
                expr: Box::new(column(
                    cast.argument.span(),
                    cast.argument.data_type().into_owned(),
                )),
                dest_type: (*cast.target_type).clone(),
            },
            _ => column(child.span(), child.data_type().into_owned()),
        }
    }

    /// A folded constant, typed as literals are.
    pub(super) fn folded_constant(span: Span, value: Scalar) -> BoundScalar {
        let value = shrink_scalar(value);
        let data_type = value.as_ref().infer_data_type();
        BoundScalar::constant(ConstantExpr { span, value }, data_type)
    }

    /// A folded explicit cast, which keeps the type it names. It is a literal
    /// when a literal has that type, e.g. `CAST('2020-01-01' AS DATE)`. The
    /// type binds only the cast itself: a call that folds over it is a
    /// constant typed as literals are, e.g. `if(true, 1::INT64, number)`.
    pub(super) fn folded_constant_of_type(
        span: Span,
        value: Scalar,
        data_type: DataType,
    ) -> BoundScalar {
        let shrunk = shrink_scalar(value.clone());
        let value = if shrunk.as_ref().infer_data_type() == data_type {
            shrunk
        } else {
            value
        };
        let facts = ScalarFacts::constant(&value, &data_type);
        let constant = ConstantExpr { span, value };
        let scalar = if constant.value.as_ref().infer_data_type() == data_type {
            constant.into()
        } else {
            ScalarExpr::TypedConstantExpr(constant, data_type.clone())
        };
        BoundScalar {
            scalar,
            data_type,
            facts,
        }
    }

    /// Converts a bound expression to `data_type` with the cast that type
    /// checking inserted for it.
    pub(super) fn bound_cast(
        &self,
        span: Span,
        is_try: bool,
        child: BoundScalar,
        data_type: DataType,
    ) -> BoundScalar {
        if child.scalar.data_type().as_ref() == &data_type {
            return child;
        }
        let facts = self.cast_facts(span, is_try, &child.scalar, child.facts, &data_type);
        BoundScalar {
            scalar: CastExpr {
                span,
                is_try,
                argument: Box::new(child.scalar),
                target_type: Box::new(data_type.clone()),
            }
            .into(),
            data_type,
            facts,
        }
    }

    /// A `column <op> constant` comparison as a range constraint.
    pub(super) fn range_constraint(&self, child: &BoundScalar) -> Option<RangeConstraint<Symbol>> {
        let ScalarExpr::FunctionCall(func) = &child.scalar else {
            return None;
        };
        if func.arguments.len() != 2 {
            return None;
        }
        let raw_arg = |arg: &ScalarExpr| match arg {
            ScalarExpr::BoundColumnRef(column) => Some(RawExpr::ColumnRef {
                span: column.span,
                id: column.column.index,
                data_type: (*column.column.data_type).clone(),
                display_name: column.column.column_name.clone(),
            }),
            ScalarExpr::ConstantExpr(constant) => Some(RawExpr::Constant {
                span: constant.span,
                scalar: constant.value.clone(),
                data_type: None,
            }),
            ScalarExpr::TypedConstantExpr(constant, data_type) => Some(RawExpr::Constant {
                span: constant.span,
                scalar: constant.value.clone(),
                data_type: Some(data_type.clone()),
            }),
            _ => None,
        };
        let raw = RawExpr::FunctionCall {
            span: func.span,
            name: func.func_name.clone(),
            params: func.params.clone(),
            args: vec![raw_arg(&func.arguments[0])?, raw_arg(&func.arguments[1])?],
        };
        let EExpr::FunctionCall(mut call) = type_check::check(&raw, &BUILTIN_FUNCTIONS).ok()?
        else {
            return None;
        };
        for arg in &mut call.args {
            if !arg.contains_column_ref() {
                let scalar = FoldRules::eval_constant(&self.func_ctx, &BUILTIN_FUNCTIONS, arg)?;
                *arg = EExpr::Constant(expr::Constant {
                    span: arg.span(),
                    scalar,
                    data_type: arg.data_type().clone(),
                });
            }
        }
        RangeConstraint::try_from_function_call(&call)
    }

    /// A call that type checking turned into a cast of its only argument.
    pub(super) fn fold_bound_cast(
        &self,
        cast: expr::Cast<usize>,
        child: BoundScalar,
    ) -> BoundScalar {
        let cast = EExpr::Cast(cast);
        let arg = self.arg_facts(&cast, &child.scalar, &child.facts);
        if !arg.deterministic {
            self.adapter.set_result_cache_uncacheable();
        }
        if let Some(value) = arg.constant {
            return Self::folded_constant_of_type(cast.span(), value, cast.data_type().clone());
        }
        let EExpr::Cast(cast) = cast else {
            unreachable!()
        };
        BoundScalar {
            scalar: CastExpr {
                span: cast.span,
                is_try: cast.is_try,
                argument: Box::new(child.scalar),
                target_type: Box::new(cast.dest_type.clone()),
            }
            .into(),
            data_type: cast.dest_type,
            facts: ScalarFacts {
                domain: arg.domain,
                removable: arg.removable,
                deterministic: arg.deterministic,
                column_free: child.facts.column_free,
                evaluable: child.facts.evaluable,
            },
        }
    }

    /// Folds a type checked call with the facts of its arguments, applying
    /// the rules that hold without statistics.
    pub(super) fn fold_bound_call(
        &self,
        span: Span,
        func_name: &str,
        params: Vec<Scalar>,
        call: expr::FunctionCall<usize>,
        args: Vec<BoundScalar>,
    ) -> Result<BoundScalar> {
        let arg_facts =
            self.call_arg_facts(&call, args.iter().map(|arg| (&arg.scalar, &arg.facts)));
        let deterministic = Self::is_call_deterministic(&call, &arg_facts);
        if !deterministic {
            self.adapter.set_result_cache_uncacheable();
        }

        let name = call.function.signature.name.as_str();
        let is_srf = matches!(call.function.eval, FunctionEval::SRF { .. });
        let is_grouping = Self::is_grouping(&call);
        let all_removable = arg_facts.iter().all(|arg| arg.removable);
        let mut domain = None;
        if !is_srf && !is_grouping && call.args.len() <= FoldRules::MAX_FUNCTION_ARGS_TO_FOLD {
            if name == "if" && call.args.len() >= 3 && !call.args.len().is_multiple_of(2) {
                match FoldRules::if_pruning(call.args.len(), |index| {
                    arg_facts[index].constant.as_ref()
                }) {
                    IfPruning::Unchanged => {}
                    IfPruning::Select(index) => {
                        return Ok(self.select_branch(&call, args, &arg_facts, index));
                    }
                    IfPruning::Keep(indices) => {
                        // Check the shorter call first, so that the arguments
                        // are moved only when it binds.
                        let raw = RawExpr::FunctionCall {
                            span,
                            name: func_name.to_string(),
                            params: params.clone(),
                            args: indices
                                .iter()
                                .enumerate()
                                .map(|(index, &arg)| {
                                    Self::placeholder(index, &args[arg].scalar, &args[arg].facts)
                                })
                                .collect(),
                        };
                        if type_check::check(&raw, &BUILTIN_FUNCTIONS).is_ok() {
                            let mut args = args.into_iter().map(Some).collect::<Vec<_>>();
                            let kept = indices
                                .iter()
                                .map(|&index| args[index].take().unwrap())
                                .collect();
                            let result =
                                self.resolve_bound_function_call(span, func_name, params, kept)?;
                            return Ok(self.with_call_type(span, false, result, &call.return_type));
                        }
                    }
                }
            }

            let is_filter = matches!(name, "and_filters" | "or_filters");
            if is_filter {
                let mut filter = FilterDomain::new(name == "or_filters");
                // Filters evaluate arguments in order, so an argument decides
                // the result only if the ones before it can be dropped.
                let mut prefix_removable = true;
                for arg in &arg_facts {
                    if prefix_removable
                        && arg
                            .constant
                            .as_ref()
                            .is_some_and(|constant| filter.is_absorbing(constant))
                    {
                        return Ok(Self::folded_constant(
                            span,
                            Scalar::Boolean(filter.absorbing_value()),
                        ));
                    }
                    prefix_removable &= arg.removable;
                    filter.add(arg.domain.clone());
                    if prefix_removable && filter.singleton() == Some(filter.absorbing_value()) {
                        return Ok(Self::folded_constant(
                            span,
                            Scalar::Boolean(filter.absorbing_value()),
                        ));
                    }
                }
                domain = filter.domain().map(Domain::Boolean);
            }

            let is_conjunction = name == "and_filters"
                || (name == "and"
                    && call
                        .args
                        .iter()
                        .all(|arg| !arg.data_type().is_nullable_or_null()));
            if is_conjunction
                && args.len() >= 2
                && all_removable
                && FoldRules::ranges_mutually_exclusive(
                    args.iter().filter_map(|arg| self.range_constraint(arg)),
                )
            {
                return Ok(Self::folded_constant(span, Scalar::Boolean(false)));
            }

            if !is_filter {
                domain = self.call_domain(&call, &arg_facts);
            }
            if deterministic
                && all_removable
                && let Some(value) = domain.as_ref().and_then(Domain::as_singleton)
            {
                return Ok(Self::folded_constant(span, value));
            }

            if deterministic
                && let Some(values) = arg_facts
                    .iter()
                    .map(|arg| arg.constant.clone())
                    .collect::<Option<Vec<_>>>()
            {
                let constant_call = EExpr::FunctionCall(expr::FunctionCall {
                    args: call
                        .args
                        .iter()
                        .zip(values)
                        .map(|(arg, scalar)| {
                            EExpr::Constant(Constant {
                                span: arg.span(),
                                scalar,
                                data_type: arg.data_type().clone(),
                            })
                        })
                        .collect(),
                    ..call.clone()
                });
                if let Some(value) =
                    FoldRules::eval_constant(&self.func_ctx, &BUILTIN_FUNCTIONS, &constant_call)
                {
                    return Ok(Self::folded_constant(span, value));
                }
            }
        }

        // Arguments fold before the call is checked, so an argument that
        // folds only here is one the check cast implicitly, e.g. the literal in
        // `timestamp > '2001-01-01'` becomes `978307200000000`. It is replaced
        // only if the literal keeps the checked type, nullability aside, so
        // that the call binds as checked.
        let mut folded_args = call
            .args
            .iter()
            .zip(
                call.function
                    .signature
                    .args_type
                    .iter()
                    .map(DataType::is_generic),
            )
            .zip(args)
            .zip(&arg_facts)
            .map(
                |(((checked_arg, is_generic), arg), facts)| match &facts.constant {
                    Some(value) if arg.facts.evaluable && !is_generic => {
                        let folded = Self::folded_constant(checked_arg.span(), value.clone());
                        if folded.data_type == checked_arg.data_type().remove_nullable() {
                            folded
                        } else {
                            arg
                        }
                    }
                    _ => arg,
                },
            )
            .collect::<Vec<_>>();

        // reorder
        if func_name == "eq"
            && folded_args.len() == 2
            && matches!(folded_args[0].scalar, ScalarExpr::ConstantExpr(_))
            && !matches!(folded_args[1].scalar, ScalarExpr::ConstantExpr(_))
        {
            folded_args.swap(0, 1);
        }

        let facts = if is_grouping {
            ScalarFacts::computed_elsewhere(&call.return_type, false)
        } else {
            ScalarFacts {
                removable: all_removable && domain.is_some(),
                domain,
                deterministic,
                column_free: folded_args.iter().all(|arg| arg.facts.column_free),
                evaluable: !is_srf && folded_args.iter().all(|arg| arg.facts.evaluable),
            }
        };
        let return_type = call.return_type;
        Ok(BoundScalar {
            scalar: FunctionCall {
                span,
                params,
                arguments: folded_args.into_iter().map(|arg| arg.scalar).collect(),
                func_name: func_name.to_string(),
                return_type: Box::new(return_type.clone()),
            }
            .into(),
            data_type: return_type,
            facts,
        })
    }

    /// The branch an `if` call always yields. A constant branch is typed as
    /// literals are; any other branch is converted to the call's type.
    fn select_branch(
        &self,
        call: &expr::FunctionCall<usize>,
        args: Vec<BoundScalar>,
        arg_facts: &[ArgFacts],
        index: usize,
    ) -> BoundScalar {
        let checked = &call.args[index];
        if let Some(value) = &arg_facts[index].constant {
            return Self::folded_constant(checked.span(), value.clone());
        }
        let is_try = matches!(checked, EExpr::Cast(cast) if cast.is_try);
        let branch = args.into_iter().nth(index).unwrap();
        self.with_call_type(checked.span(), is_try, branch, &call.return_type)
    }

    /// Keeps the type of a call whose result was simplified to `result`.
    /// Folded constants keep their literal typing.
    fn with_call_type(
        &self,
        span: Span,
        is_try: bool,
        result: BoundScalar,
        return_type: &DataType,
    ) -> BoundScalar {
        if matches!(
            result.scalar,
            ScalarExpr::ConstantExpr(_) | ScalarExpr::TypedConstantExpr(..)
        ) {
            return result;
        }
        self.bound_cast(span, is_try, result, return_type.clone())
    }
}
