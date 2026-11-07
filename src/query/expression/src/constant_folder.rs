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

use databend_common_ast::Span;
use log::error;

use crate::ColumnIndex;
use crate::FunctionContext;
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
use crate::type_check::resolve_cast_function;
use crate::types::DataType;
use crate::types::nullable::NullableDomain;

mod fold_rules;
mod monotonicity;
mod range_constraint;

pub use fold_rules::FilterDomain;
pub use fold_rules::FoldRules;
pub use fold_rules::IfPruning;
pub use range_constraint::RangeConstraint;

pub struct ConstantFolder<'a, Index: ColumnIndex> {
    input_domains: &'a HashMap<Index, Domain>,
    func_ctx: &'a FunctionContext,
    fn_registry: &'a FunctionRegistry,
}

enum FoldResult<'e, Index: ColumnIndex> {
    Unchanged(Cow<'e, Expr<Index>>),
    Changed(Expr<Index>),
}

impl<'e, Index: ColumnIndex> FoldResult<'e, Index> {
    fn as_ref(&self) -> &Expr<Index> {
        match self {
            FoldResult::Unchanged(expr) => expr.as_ref(),
            FoldResult::Changed(expr) => expr,
        }
    }

    fn is_changed(&self) -> bool {
        matches!(self, FoldResult::Changed(_))
    }

    fn into_expr(self) -> Expr<Index> {
        match self {
            FoldResult::Unchanged(expr) => expr.into_owned(),
            FoldResult::Changed(expr) => expr,
        }
    }
}

enum ExprArgs<'e, Index: ColumnIndex> {
    Borrowed(std::slice::Iter<'e, Expr<Index>>),
    Owned(std::vec::IntoIter<Expr<Index>>),
}

impl<'e, Index: ColumnIndex> Iterator for ExprArgs<'e, Index> {
    type Item = Cow<'e, Expr<Index>>;

    fn next(&mut self) -> Option<Self::Item> {
        match self {
            ExprArgs::Borrowed(args) => args.next().map(Cow::Borrowed),
            ExprArgs::Owned(args) => args.next().map(Cow::Owned),
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        match self {
            ExprArgs::Borrowed(args) => args.size_hint(),
            ExprArgs::Owned(args) => args.size_hint(),
        }
    }
}

impl<Index: ColumnIndex> ExactSizeIterator for ExprArgs<'_, Index> {}

enum FunctionNode<'e, Index: ColumnIndex> {
    Borrowed {
        expr: &'e Expr<Index>,
        call: &'e FunctionCall<Index>,
    },
    Owned(FunctionCall<Index>),
}

impl<'e, Index: ColumnIndex> FunctionNode<'e, Index> {
    fn call(&self) -> &FunctionCall<Index> {
        match self {
            FunctionNode::Borrowed { call, .. } => call,
            FunctionNode::Owned(call) => call,
        }
    }

    fn args_len(&self) -> usize {
        self.call().args.len()
    }

    fn into_args(self) -> (Self, ExprArgs<'e, Index>) {
        match self {
            FunctionNode::Borrowed { expr, call } => (
                FunctionNode::Borrowed { expr, call },
                ExprArgs::Borrowed(call.args.iter()),
            ),
            FunctionNode::Owned(mut call) => {
                let args = ExprArgs::Owned(std::mem::take(&mut call.args).into_iter());
                (FunctionNode::Owned(call), args)
            }
        }
    }

    fn into_cow(self) -> Cow<'e, Expr<Index>> {
        match self {
            FunctionNode::Borrowed { expr, .. } => Cow::Borrowed(expr),
            FunctionNode::Owned(call) => Cow::Owned(Expr::FunctionCall(call)),
        }
    }

    fn finish(self, args: impl IntoIterator<Item = Expr<Index>>) -> Expr<Index> {
        let args = args.into_iter().collect();
        match self {
            FunctionNode::Borrowed { call, .. } => Expr::FunctionCall(FunctionCall {
                span: call.span,
                id: call.id.clone(),
                function: call.function.clone(),
                generics: call.generics.clone(),
                args,
                return_type: call.return_type.clone(),
            }),
            FunctionNode::Owned(mut call) => {
                call.args = args;
                Expr::FunctionCall(call)
            }
        }
    }

    fn borrowed_expr(&self) -> Option<&'e Expr<Index>> {
        match self {
            FunctionNode::Borrowed { expr, .. } => Some(expr),
            FunctionNode::Owned(_) => None,
        }
    }

    fn finish_fold(self, args: Vec<FoldResult<'e, Index>>, changed: bool) -> FoldResult<'e, Index> {
        match (changed, self) {
            (false, FunctionNode::Borrowed { expr, .. }) => {
                FoldResult::Unchanged(Cow::Borrowed(expr))
            }
            (changed, node) => {
                let expr = node.finish(args.into_iter().map(FoldResult::into_expr));
                if changed {
                    FoldResult::Changed(expr)
                } else {
                    FoldResult::Unchanged(Cow::Owned(expr))
                }
            }
        }
    }
}

enum LambdaNode<'e, Index: ColumnIndex> {
    Borrowed {
        expr: &'e Expr<Index>,
        call: &'e LambdaFunctionCall<Index>,
    },
    Owned(LambdaFunctionCall<Index>),
}

impl<'e, Index: ColumnIndex> LambdaNode<'e, Index> {
    fn args_len(&self) -> usize {
        match self {
            LambdaNode::Borrowed { call, .. } => call.args.len(),
            LambdaNode::Owned(call) => call.args.len(),
        }
    }

    fn into_args(self) -> (Self, ExprArgs<'e, Index>) {
        match self {
            LambdaNode::Borrowed { expr, call } => (
                LambdaNode::Borrowed { expr, call },
                ExprArgs::Borrowed(call.args.iter()),
            ),
            LambdaNode::Owned(mut call) => {
                let args = ExprArgs::Owned(std::mem::take(&mut call.args).into_iter());
                (LambdaNode::Owned(call), args)
            }
        }
    }

    fn into_cow(self) -> Cow<'e, Expr<Index>> {
        match self {
            LambdaNode::Borrowed { expr, .. } => Cow::Borrowed(expr),
            LambdaNode::Owned(call) => Cow::Owned(Expr::LambdaFunctionCall(call)),
        }
    }

    fn finish(self, args: impl IntoIterator<Item = Expr<Index>>) -> Expr<Index> {
        let args = args.into_iter().collect();
        match self {
            LambdaNode::Borrowed { call, .. } => Expr::LambdaFunctionCall(LambdaFunctionCall {
                span: call.span,
                name: call.name.clone(),
                args,
                lambda_expr: call.lambda_expr.clone(),
                lambda_display: call.lambda_display.clone(),
                return_type: call.return_type.clone(),
            }),
            LambdaNode::Owned(mut call) => {
                call.args = args;
                Expr::LambdaFunctionCall(call)
            }
        }
    }

    fn borrowed_expr(&self) -> Option<&'e Expr<Index>> {
        match self {
            LambdaNode::Borrowed { expr, .. } => Some(expr),
            LambdaNode::Owned(_) => None,
        }
    }

    fn finish_fold(self, args: Vec<FoldResult<'e, Index>>, changed: bool) -> FoldResult<'e, Index> {
        match (changed, self) {
            (false, LambdaNode::Borrowed { expr, .. }) => {
                FoldResult::Unchanged(Cow::Borrowed(expr))
            }
            (changed, node) => {
                let expr = node.finish(args.into_iter().map(FoldResult::into_expr));
                if changed {
                    FoldResult::Changed(expr)
                } else {
                    FoldResult::Unchanged(Cow::Owned(expr))
                }
            }
        }
    }
}

enum CastNode<'e, Index: ColumnIndex> {
    Borrowed(&'e Expr<Index>),
    Owned {
        span: Span,
        is_try: bool,
        dest_type: DataType,
    },
}

impl<'e, Index: ColumnIndex> CastNode<'e, Index> {
    fn parts(&self) -> (Span, bool, &DataType) {
        match self {
            CastNode::Borrowed(Expr::Cast(cast)) => (cast.span, cast.is_try, &cast.dest_type),
            CastNode::Owned {
                span,
                is_try,
                dest_type,
            } => (*span, *is_try, dest_type),
            CastNode::Borrowed(_) => unreachable!(),
        }
    }

    fn finish(self, expr: Expr<Index>) -> Expr<Index> {
        match self {
            CastNode::Borrowed(Expr::Cast(cast)) => Expr::Cast(Cast {
                span: cast.span,
                is_try: cast.is_try,
                expr: Box::new(expr),
                dest_type: cast.dest_type.clone(),
            }),
            CastNode::Owned {
                span,
                is_try,
                dest_type,
            } => Expr::Cast(Cast {
                span,
                is_try,
                expr: Box::new(expr),
                dest_type,
            }),
            CastNode::Borrowed(_) => unreachable!(),
        }
    }

    fn borrowed_expr(&self) -> Option<&'e Expr<Index>> {
        match self {
            CastNode::Borrowed(expr) => Some(expr),
            CastNode::Owned { .. } => None,
        }
    }

    fn finish_fold(self, expr: FoldResult<'e, Index>, changed: bool) -> FoldResult<'e, Index> {
        if changed {
            return FoldResult::Changed(self.finish(expr.into_expr()));
        }

        match self {
            CastNode::Borrowed(expr) => FoldResult::Unchanged(Cow::Borrowed(expr)),
            CastNode::Owned {
                span,
                is_try,
                dest_type,
            } => FoldResult::Unchanged(Cow::Owned(Expr::Cast(Cast {
                span,
                is_try,
                expr: Box::new(expr.into_expr()),
                dest_type,
            }))),
        }
    }
}

enum FoldNode<'e, Index: ColumnIndex> {
    Constant(Cow<'e, Expr<Index>>),
    ColumnRef(Cow<'e, Expr<Index>>),
    Cast(CastNode<'e, Index>, Cow<'e, Expr<Index>>),
    FunctionCall(FunctionNode<'e, Index>),
    LambdaFunctionCall(LambdaNode<'e, Index>),
}

impl<'e, Index: ColumnIndex> FoldNode<'e, Index> {
    fn from_cow(expr: Cow<'e, Expr<Index>>) -> Self {
        match expr {
            Cow::Borrowed(expr @ Expr::Constant(_)) => FoldNode::Constant(Cow::Borrowed(expr)),
            Cow::Owned(expr @ Expr::Constant(_)) => FoldNode::Constant(Cow::Owned(expr)),
            Cow::Borrowed(expr @ Expr::ColumnRef(_)) => FoldNode::ColumnRef(Cow::Borrowed(expr)),
            Cow::Owned(expr @ Expr::ColumnRef(_)) => FoldNode::ColumnRef(Cow::Owned(expr)),
            Cow::Borrowed(expr @ Expr::Cast(cast)) => {
                FoldNode::Cast(CastNode::Borrowed(expr), Cow::Borrowed(cast.expr.as_ref()))
            }
            Cow::Owned(Expr::Cast(cast)) => FoldNode::Cast(
                CastNode::Owned {
                    span: cast.span,
                    is_try: cast.is_try,
                    dest_type: cast.dest_type,
                },
                Cow::Owned(*cast.expr),
            ),
            Cow::Borrowed(expr @ Expr::FunctionCall(call)) => {
                FoldNode::FunctionCall(FunctionNode::Borrowed { expr, call })
            }
            Cow::Owned(Expr::FunctionCall(call)) => {
                FoldNode::FunctionCall(FunctionNode::Owned(call))
            }
            Cow::Borrowed(expr @ Expr::LambdaFunctionCall(call)) => {
                FoldNode::LambdaFunctionCall(LambdaNode::Borrowed { expr, call })
            }
            Cow::Owned(Expr::LambdaFunctionCall(call)) => {
                FoldNode::LambdaFunctionCall(LambdaNode::Owned(call))
            }
        }
    }
}

impl<'a, Index: ColumnIndex> ConstantFolder<'a, Index> {
    /// Fold a single expression, returning the new expression and its domain.
    ///
    /// A borrowed input remains borrowed when folding does not rewrite it. An owned input is
    /// consumed and rebuilt by moving its unchanged subtrees whenever possible.
    pub fn fold<'e>(
        expr: Cow<'e, Expr<Index>>,
        func_ctx: &'a FunctionContext,
        fn_registry: &'a FunctionRegistry,
    ) -> (Cow<'e, Expr<Index>>, Option<Domain>) {
        let input_domains = Self::full_input_domains(expr.as_ref());

        let folder = ConstantFolder {
            input_domains: &input_domains,
            func_ctx,
            fn_registry,
        };

        folder.fold_to_stable(expr)
    }

    /// Fold a single expression with columns' domains, returning the new expression and its domain.
    ///
    /// A borrowed input remains borrowed when folding does not rewrite it. An owned input is
    /// consumed and rebuilt by moving its unchanged subtrees whenever possible.
    ///
    /// `input_domains` must contain every referenced column and conservatively include every value
    /// each column can take in the executions covered by this analysis.
    pub fn fold_with_domain<'e>(
        expr: Cow<'e, Expr<Index>>,
        input_domains: &'a HashMap<Index, Domain>,
        func_ctx: &'a FunctionContext,
        fn_registry: &'a FunctionRegistry,
    ) -> (Cow<'e, Expr<Index>>, Option<Domain>) {
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
    fn fold_to_stable<'e>(
        &self,
        mut expr: Cow<'e, Expr<Index>>,
    ) -> (Cow<'e, Expr<Index>>, Option<Domain>) {
        const MAX_ITERATIONS: usize = 1024;

        let mut domain = None;
        for _ in 0..MAX_ITERATIONS {
            let (result, new_domain) = self.fold_once(expr);
            match result {
                FoldResult::Unchanged(current) => return (current, new_domain),
                FoldResult::Changed(replacement) => expr = Cow::Owned(replacement),
            }
            domain = new_domain;
        }

        error!("maximum iterations reached while folding expression");

        (expr, domain)
    }

    /// Fold expression by one step, specifically, by reducing expression by domain calculation and then
    /// folding the function calls whose all arguments are constants.
    #[recursive::recursive]
    fn fold_once<'e>(&self, expr: Cow<'e, Expr<Index>>) -> (FoldResult<'e, Index>, Option<Domain>) {
        let data_type = expr.data_type().clone();
        let (result, domain) = match FoldNode::from_cow(expr) {
            FoldNode::Constant(expr) => {
                let Expr::Constant(constant) = expr.as_ref() else {
                    unreachable!()
                };
                let domain = constant.scalar.as_ref().domain(&constant.data_type);
                (FoldResult::Unchanged(expr), Some(domain))
            }
            FoldNode::ColumnRef(expr) => {
                let Expr::ColumnRef(column_ref) = expr.as_ref() else {
                    unreachable!()
                };
                let domain = &self.input_domains[&column_ref.id];
                if let Some(scalar) = domain.as_singleton() {
                    (
                        FoldResult::Changed(Expr::Constant(Constant {
                            span: column_ref.span,
                            scalar,
                            data_type: column_ref.data_type.clone(),
                        })),
                        Some(domain.clone()),
                    )
                } else {
                    (FoldResult::Unchanged(expr), Some(domain.clone()))
                }
            }
            FoldNode::Cast(node, expr) => {
                let (span, is_try, dest_type) = node.parts();
                let src_type = expr.data_type().clone();
                let (inner_result, inner_domain) = self.fold_once(expr);
                let inner_changed = inner_result.is_changed();

                let new_domain = if is_try {
                    inner_domain.and_then(|inner_domain| {
                        FoldRules::calculate_try_cast(
                            self.func_ctx,
                            self.fn_registry,
                            span,
                            &src_type,
                            dest_type,
                            &inner_domain,
                        )
                    })
                } else {
                    inner_domain.and_then(|inner_domain| {
                        FoldRules::calculate_cast(
                            self.func_ctx,
                            self.fn_registry,
                            span,
                            &src_type,
                            dest_type,
                            &inner_domain,
                        )
                    })
                };

                let inner_is_constant = inner_result.as_ref().as_constant().is_some();
                if inner_is_constant {
                    if !inner_changed && let Some(cast_expr) = node.borrowed_expr() {
                        let (cast_expr, folded) =
                            self.try_fold_constant_expr(Cow::Borrowed(cast_expr));
                        if folded {
                            return (FoldResult::Changed(cast_expr.into_owned()), None);
                        }
                    } else {
                        let cast_expr = node.finish(inner_result.into_expr());
                        let (cast_expr, folded) =
                            self.try_fold_constant_expr(Cow::Owned(cast_expr));
                        if folded {
                            return (FoldResult::Changed(cast_expr.into_owned()), None);
                        }

                        if let Some(scalar) = new_domain.as_ref().and_then(Domain::as_singleton) {
                            let Expr::Cast(cast) = cast_expr.into_owned() else {
                                unreachable!()
                            };
                            return (
                                FoldResult::Changed(Expr::Constant(Constant {
                                    span: cast.span,
                                    scalar,
                                    data_type: cast.dest_type,
                                })),
                                new_domain,
                            );
                        }

                        let result = if inner_changed {
                            FoldResult::Changed(cast_expr.into_owned())
                        } else {
                            FoldResult::Unchanged(cast_expr)
                        };
                        return (result, new_domain);
                    }
                }

                if let Some(scalar) = new_domain.as_ref().and_then(Domain::as_singleton) {
                    (
                        FoldResult::Changed(Expr::Constant(Constant {
                            span,
                            scalar,
                            data_type: dest_type.clone(),
                        })),
                        new_domain,
                    )
                } else {
                    (node.finish_fold(inner_result, inner_changed), new_domain)
                }
            }
            FoldNode::FunctionCall(node)
                if matches!(
                    node.call().function.signature.name.as_str(),
                    "and_filters" | "or_filters"
                ) =>
            {
                let call = node.call();
                let is_or = call.function.signature.name.starts_with("or_");
                let span = call.span;

                if node.args_len() > FoldRules::MAX_FUNCTION_ARGS_TO_FOLD {
                    return (FoldResult::Unchanged(node.into_cow()), None);
                }
                let (node, args) = node.into_args();

                let mut changed = false;
                let mut args_expr = Vec::with_capacity(args.len());
                let mut filter = FilterDomain::new(is_or);

                for arg in args {
                    let (arg, domain) = self.fold_once(arg);
                    changed |= arg.is_changed();
                    // A temporary hack to make `and_filters` shortcut on false.
                    // TODO(andylokandy): make it a rule in the optimizer.
                    if let Expr::Constant(Constant { scalar, .. }) = arg.as_ref()
                        && filter.is_absorbing(scalar)
                    {
                        return (
                            FoldResult::Changed(Expr::Constant(Constant {
                                span,
                                scalar: Scalar::Boolean(filter.absorbing_value()),
                                data_type: DataType::Boolean,
                            })),
                            None,
                        );
                    }
                    args_expr.push(arg);

                    filter.add(domain);
                    if filter.singleton() == Some(filter.absorbing_value()) {
                        return (
                            FoldResult::Changed(Expr::Constant(Constant {
                                span,
                                scalar: Scalar::Boolean(filter.absorbing_value()),
                                data_type: DataType::Boolean,
                            })),
                            None,
                        );
                    }
                }

                // Check for mutually exclusive ranges in AND filters
                if !is_or
                    && args_expr.len() >= 2
                    && self.check_mutually_exclusive_ranges(&args_expr)
                {
                    return (
                        FoldResult::Changed(Expr::Constant(Constant {
                            span,
                            scalar: Scalar::Boolean(false),
                            data_type: DataType::Boolean,
                        })),
                        None,
                    );
                }

                if let Some(result) = filter.singleton() {
                    return (
                        FoldResult::Changed(Expr::Constant(Constant {
                            span,
                            scalar: Scalar::Boolean(result),
                            data_type: DataType::Boolean,
                        })),
                        None,
                    );
                }

                let all_args_is_scalar = args_expr
                    .iter()
                    .all(|arg| arg.as_ref().as_constant().is_some());

                if all_args_is_scalar {
                    if !changed && let Some(func_expr) = node.borrowed_expr() {
                        let (func_expr, folded) =
                            self.try_fold_constant_expr(Cow::Borrowed(func_expr));
                        if folded {
                            return (FoldResult::Changed(func_expr.into_owned()), None);
                        }
                    } else {
                        let func_expr =
                            node.finish(args_expr.into_iter().map(FoldResult::into_expr));
                        let (func_expr, folded) =
                            self.try_fold_constant_expr(Cow::Owned(func_expr));
                        let result = if folded || changed {
                            FoldResult::Changed(func_expr.into_owned())
                        } else {
                            FoldResult::Unchanged(func_expr)
                        };
                        return (
                            result,
                            if folded {
                                None
                            } else {
                                filter.domain().map(Domain::Boolean)
                            },
                        );
                    }
                }

                let result = node.finish_fold(args_expr, changed);
                (result, filter.domain().map(Domain::Boolean))
            }
            FoldNode::FunctionCall(node) => {
                if node.args_len() > FoldRules::MAX_FUNCTION_ARGS_TO_FOLD {
                    return (FoldResult::Unchanged(node.into_cow()), None);
                }
                let (node, args) = node.into_args();

                let mut changed = false;
                let mut args_expr = Vec::with_capacity(args.len());
                let mut args_domain = Some(Vec::with_capacity(args.len()));
                for arg in args {
                    let (arg, domain) = self.fold_once(arg);
                    changed |= arg.is_changed();
                    args_expr.push(arg);
                    args_domain = args_domain.zip(domain).map(|(mut domains, domain)| {
                        domains.push(domain);
                        domains
                    });
                }

                let call = node.call();

                if call.function.signature.name == "if" {
                    if args_expr.len() < 3 || args_expr.len().is_multiple_of(2) {
                        let result = node.finish_fold(args_expr, changed);
                        return (result, None);
                    }

                    let pruning = FoldRules::if_pruning(args_expr.len(), |index| {
                        args_expr[index].as_ref().as_constant().map(|c| &c.scalar)
                    });
                    match pruning {
                        IfPruning::Unchanged => {}
                        IfPruning::Select(index) => {
                            return (
                                FoldResult::Changed(args_expr.swap_remove(index).into_expr()),
                                None,
                            );
                        }
                        IfPruning::Keep(simplified_indices) => {
                            let mut original_args =
                                args_expr.into_iter().map(Some).collect::<Vec<_>>();
                            let simplified_args = simplified_indices
                                .iter()
                                .map(|&index| original_args[index].take().unwrap().into_expr())
                                .collect::<Vec<_>>();
                            match check_function(
                                call.span,
                                "if",
                                call.id.params(),
                                &simplified_args,
                                self.fn_registry,
                            ) {
                                Ok(Expr::FunctionCall(mut call)) => {
                                    call.args = simplified_args;
                                    return (FoldResult::Changed(Expr::FunctionCall(call)), None);
                                }
                                Ok(func_expr) => return (FoldResult::Changed(func_expr), None),
                                Err(_) => {
                                    for (index, arg) in
                                        simplified_indices.into_iter().zip(simplified_args)
                                    {
                                        original_args[index] =
                                            Some(FoldResult::Unchanged(Cow::Owned(arg)));
                                    }
                                    args_expr =
                                        original_args.into_iter().map(Option::unwrap).collect();
                                }
                            }
                        }
                    }
                }

                let all_args_is_scalar = args_expr
                    .iter()
                    .all(|arg| arg.as_ref().as_constant().is_some());
                // Check for mutually exclusive ranges in AND function
                if call.function.signature.name == "and"
                    && args_expr.len() >= 2
                    && args_expr
                        .iter()
                        .all(|arg| !arg.as_ref().data_type().is_nullable_or_null())
                    && self.check_mutually_exclusive_ranges(&args_expr)
                {
                    return (
                        FoldResult::Changed(Expr::Constant(Constant {
                            span: call.span,
                            scalar: Scalar::Boolean(false),
                            data_type: DataType::Boolean,
                        })),
                        None,
                    );
                }

                if matches!(call.function.eval, FunctionEval::SRF { .. }) {
                    let result = node.finish_fold(args_expr, changed);
                    return (result, None);
                }

                let func_domain = args_domain.and_then(|domains: Vec<Domain>| {
                    FoldRules::function_domain(
                        self.func_ctx,
                        self.fn_registry,
                        &call.function,
                        &call.generics,
                        &call.return_type,
                        args_expr.first().map(|arg| arg.as_ref().data_type()),
                        &domains,
                    )
                });

                if let Some(scalar) = func_domain.as_ref().and_then(Domain::as_singleton) {
                    return (
                        FoldResult::Changed(Expr::Constant(Constant {
                            span: call.span,
                            scalar,
                            data_type: call.return_type.clone(),
                        })),
                        None,
                    );
                }

                let is_grouping = call.function.signature.name == "grouping";

                // `grouping` is a placeholder before the aggregate rewriter rewrites it to
                // `grouping<...>(_grouping_id)`. Folding it here can reach the dummy
                // implementation and panic on invalid queries.
                if is_grouping {
                    let result = node.finish_fold(args_expr, changed);
                    return (result, func_domain);
                }

                if all_args_is_scalar {
                    if !changed && let Some(func_expr) = node.borrowed_expr() {
                        let (func_expr, folded) =
                            self.try_fold_constant_expr(Cow::Borrowed(func_expr));
                        if folded {
                            return (FoldResult::Changed(func_expr.into_owned()), None);
                        }
                    } else {
                        let func_expr =
                            node.finish(args_expr.into_iter().map(FoldResult::into_expr));
                        let (func_expr, folded) =
                            self.try_fold_constant_expr(Cow::Owned(func_expr));
                        let result = if folded || changed {
                            FoldResult::Changed(func_expr.into_owned())
                        } else {
                            FoldResult::Unchanged(func_expr)
                        };
                        return (result, if folded { None } else { func_domain });
                    }
                }

                let result = node.finish_fold(args_expr, changed);
                (result, func_domain)
            }
            FoldNode::LambdaFunctionCall(node) => {
                if node.args_len() > FoldRules::MAX_FUNCTION_ARGS_TO_FOLD {
                    return (FoldResult::Unchanged(node.into_cow()), None);
                }
                let (node, args) = node.into_args();

                let mut changed = false;
                let mut args_expr = Vec::with_capacity(args.len());
                for arg in args {
                    let (arg, _) = self.fold_once(arg);
                    changed |= arg.is_changed();
                    args_expr.push(arg);
                }
                let all_args_is_scalar = args_expr
                    .iter()
                    .all(|arg| arg.as_ref().as_constant().is_some());

                if all_args_is_scalar {
                    if !changed && let Some(func_expr) = node.borrowed_expr() {
                        let (func_expr, folded) =
                            self.try_fold_constant_expr(Cow::Borrowed(func_expr));
                        if folded {
                            return (FoldResult::Changed(func_expr.into_owned()), None);
                        }
                    } else {
                        let func_expr =
                            node.finish(args_expr.into_iter().map(FoldResult::into_expr));
                        let (func_expr, folded) =
                            self.try_fold_constant_expr(Cow::Owned(func_expr));
                        let result = if folded || changed {
                            FoldResult::Changed(func_expr.into_owned())
                        } else {
                            FoldResult::Unchanged(func_expr)
                        };
                        return (result, None);
                    }
                }
                let result = node.finish_fold(args_expr, changed);
                (result, None)
            }
        };

        let result_type = match &result {
            FoldResult::Unchanged(expr) => expr.data_type(),
            FoldResult::Changed(expr) => expr.data_type(),
        };
        debug_assert_eq!(&data_type, result_type);

        (result, domain)
    }

    fn try_fold_constant_expr<'e>(
        &self,
        expr: Cow<'e, Expr<Index>>,
    ) -> (Cow<'e, Expr<Index>>, bool) {
        let Some(scalar) = FoldRules::eval_constant(self.func_ctx, self.fn_registry, expr.as_ref())
        else {
            return (expr, false);
        };
        let span = expr.span();
        let data_type = match expr {
            Cow::Borrowed(expr) => expr.data_type().clone(),
            Cow::Owned(expr) => expr.into_data_type(),
        };
        (
            Cow::Owned(Expr::Constant(Constant {
                span,
                scalar,
                data_type,
            })),
            true,
        )
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
