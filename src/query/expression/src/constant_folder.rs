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
use crate::type_check::resolve_cast_function;
use crate::types::DataType;
use crate::types::boolean::BooleanDomain;
use crate::types::nullable::NullableDomain;

const MAX_FUNCTION_ARGS_TO_FOLD: usize = 4096;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum FoldMode {
    /// Fold using the `FunctionContext` supplied by the caller.
    Full,
    /// Do not evaluate non-deterministic or `FunctionContext`-dependent operations.
    ContextIndependent,
}

pub struct ConstantFolder<'a, Index: ColumnIndex> {
    input_domains: &'a HashMap<Index, Domain>,
    func_ctx: &'a FunctionContext,
    fn_registry: &'a FunctionRegistry,
    mode: FoldMode,
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
            mode: FoldMode::Full,
        };

        folder.fold_to_stable(expr)
    }

    /// Fold without evaluating non-deterministic or `FunctionContext`-dependent operations.
    pub fn fold_context_independent<'e>(
        expr: Cow<'e, Expr<Index>>,
        fn_registry: &FunctionRegistry,
    ) -> (Cow<'e, Expr<Index>>, Option<Domain>) {
        let input_domains = Self::full_input_domains(expr.as_ref());
        // Context-dependent overloads are rejected before domain or value evaluation, so this
        // placeholder is observed only by functions registered as context independent.
        let context_placeholder = FunctionContext::default();
        let folder = ConstantFolder {
            input_domains: &input_domains,
            func_ctx: &context_placeholder,
            fn_registry,
            mode: FoldMode::ContextIndependent,
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
            mode: FoldMode::Full,
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
                let can_evaluate = self.can_evaluate_cast(is_try, &src_type, dest_type);

                let new_domain = if !can_evaluate {
                    None
                } else if is_try {
                    inner_domain.and_then(|inner_domain| {
                        self.calculate_try_cast(span, &src_type, dest_type, &inner_domain)
                    })
                } else {
                    inner_domain.and_then(|inner_domain| {
                        self.calculate_cast(span, &src_type, dest_type, &inner_domain)
                    })
                };

                let inner_is_constant = inner_result.as_ref().as_constant().is_some();
                if can_evaluate && inner_is_constant {
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

                if node.args_len() > MAX_FUNCTION_ARGS_TO_FOLD {
                    return (FoldResult::Unchanged(node.into_cow()), None);
                }
                let (node, args) = node.into_args();

                let mut changed = false;
                let mut args_expr = Vec::with_capacity(args.len());
                let mut result_domain = Some(BooleanDomain {
                    has_true: true,
                    has_false: true,
                });

                for arg in args {
                    let (arg, domain) = self.fold_once(arg);
                    changed |= arg.is_changed();
                    // A temporary hack to make `and_filters` shortcut on false.
                    // TODO(andylokandy): make it a rule in the optimizer.
                    if let Expr::Constant(Constant {
                        scalar: Scalar::Boolean(result),
                        ..
                    }) = arg.as_ref()
                    {
                        if is_or == *result {
                            return (
                                FoldResult::Changed(Expr::Constant(Constant {
                                    span,
                                    scalar: Scalar::Boolean(is_or),
                                    data_type: DataType::Boolean,
                                })),
                                None,
                            );
                        }
                    }
                    args_expr.push(arg);

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
                                FoldResult::Changed(Expr::Constant(Constant {
                                    span,
                                    scalar: Scalar::Boolean(result),
                                    data_type: DataType::Boolean,
                                })),
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
                                FoldResult::Changed(Expr::Constant(Constant {
                                    span,
                                    scalar: Scalar::Boolean(false),
                                    data_type: DataType::Boolean,
                                })),
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
                        FoldResult::Changed(Expr::Constant(Constant {
                            span,
                            scalar,
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
                                result_domain.map(Domain::Boolean)
                            },
                        );
                    }
                }

                let result = node.finish_fold(args_expr, changed);
                (result, result_domain.map(Domain::Boolean))
            }
            FoldNode::FunctionCall(node) => {
                if node.args_len() > MAX_FUNCTION_ARGS_TO_FOLD {
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

                    let mut simplified_indices = Vec::with_capacity(args_expr.len());
                    let mut found_true_branch = false;
                    for cond_idx in (0..args_expr.len() - 1).step_by(2) {
                        match args_expr[cond_idx]
                            .as_ref()
                            .as_constant()
                            .map(|c| &c.scalar)
                        {
                            Some(Scalar::Boolean(true)) => {
                                if simplified_indices.is_empty() {
                                    return (
                                        FoldResult::Changed(
                                            args_expr.remove(cond_idx + 1).into_expr(),
                                        ),
                                        None,
                                    );
                                }
                                simplified_indices.push(cond_idx + 1);
                                found_true_branch = true;
                                break;
                            }
                            Some(Scalar::Boolean(false) | Scalar::Null) => {}
                            _ => {
                                simplified_indices.push(cond_idx);
                                simplified_indices.push(cond_idx + 1);
                            }
                        }
                    }

                    if simplified_indices.is_empty() {
                        return (
                            FoldResult::Changed(args_expr.pop().unwrap().into_expr()),
                            None,
                        );
                    }
                    if !found_true_branch {
                        simplified_indices.push(args_expr.len() - 1);
                    }
                    if simplified_indices.len() != args_expr.len() {
                        let mut original_args = args_expr.into_iter().map(Some).collect::<Vec<_>>();
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
                                args_expr = original_args.into_iter().map(Option::unwrap).collect();
                            }
                        }
                    }
                }

                let all_args_is_scalar = args_expr
                    .iter()
                    .all(|arg| arg.as_ref().as_constant().is_some());
                let is_monotonicity = self
                    .fn_registry
                    .properties
                    .get(&call.function.signature.name)
                    .map(|p| {
                        args_expr.len() == 1
                            && (p.monotonicity
                                || p.monotonicity_by_type
                                    .contains(args_expr[0].as_ref().data_type()))
                    })
                    .unwrap_or_default();
                let monotonicity_check = self
                    .fn_registry
                    .properties
                    .get(&call.function.signature.name)
                    .and_then(|p| p.monotonicity_check)
                    .filter(|_| args_expr.len() == 1);

                // Check for mutually exclusive ranges in AND function
                if call.function.signature.name == "and"
                    && args_expr.len() >= 2
                    && args_expr
                        .iter()
                        .all(|arg| !arg.as_ref().data_type().is_nullable_or_null())
                {
                    if let Some(is_mutually_exclusive) =
                        self.check_mutually_exclusive_ranges(&args_expr)
                    {
                        if is_mutually_exclusive {
                            return (
                                FoldResult::Changed(Expr::Constant(Constant {
                                    span: call.span,
                                    scalar: Scalar::Boolean(false),
                                    data_type: DataType::Boolean,
                                })),
                                None,
                            );
                        }
                    }
                }

                if !self.can_evaluate_function(&call.id) {
                    let result = node.finish_fold(args_expr, changed);
                    return (result, None);
                }

                let (calc_domain, eval) = match &call.function.eval {
                    FunctionEval::Scalar {
                        calc_domain, eval, ..
                    } => (calc_domain, eval),
                    FunctionEval::SRF { .. } => {
                        let result = node.finish_fold(args_expr, changed);
                        return (result, None);
                    }
                };

                let func_domain = args_domain.and_then(|domains: Vec<Domain>| {
                    let res = calc_domain.domain_eval(self.func_ctx, &domains);
                    // Range-sensitive checks complement the static flags: they may prove
                    // monotonicity for this specific argument range and context only.
                    let is_monotonic = is_monotonicity
                        || monotonicity_check
                            .is_some_and(|check| check(self.func_ctx, &domains) == Some(0));
                    match (res, is_monotonic) {
                        (FunctionDomain::MayThrow | FunctionDomain::Full, true) => {
                            let domain = domains.first().unwrap();
                            let (value_domain, has_null) = match domain {
                                Domain::Nullable(NullableDomain { has_null, value }) => {
                                    (value.as_deref(), *has_null)
                                }
                                domain => (Some(domain), false),
                            };

                            let mut boundaries = Vec::with_capacity(3);
                            if let Some(value_domain) = value_domain {
                                let (min, max) = value_domain.to_minmax();
                                if min.is_null() || max.is_null() {
                                    return None;
                                }
                                boundaries.extend([min, max]);
                            }
                            if has_null {
                                boundaries.push(Scalar::Null);
                            }
                            if boundaries.is_empty() {
                                return None;
                            }

                            {
                                let mut ctx = EvalContext {
                                    generics: &call.generics,
                                    num_rows: boundaries.len(),
                                    validity: None,
                                    errors: None,
                                    func_ctx: self.func_ctx,
                                    suppress_error: false,
                                    strict_eval: true,
                                };
                                let mut builder = ColumnBuilder::with_capacity(
                                    args_expr[0].as_ref().data_type(),
                                    boundaries.len(),
                                );
                                for boundary in &boundaries {
                                    builder.push(boundary.as_ref());
                                }

                                let input = Value::Column(builder.build());
                                let result = eval.eval(&[input], &mut ctx);

                                if result.is_scalar() {
                                    None
                                } else {
                                    // if error happens, domain maybe incorrect
                                    // min, max: String("2024-09-02 00:00") String("2024-09-02 00:0�")
                                    // to_date(s) > to_date('2024-01-1')
                                    let col = result.as_column().unwrap();
                                    let d = if boundaries
                                        .iter()
                                        .enumerate()
                                        .any(|(index, _)| ctx.has_error(index))
                                    {
                                        // NULL is not an ordered boundary. If evaluating it fails,
                                        // the function's domain is not known.
                                        if has_null && ctx.has_error(boundaries.len() - 1) {
                                            return None;
                                        }

                                        let full_domain = Domain::full(&call.return_type);
                                        let full_value_domain = match &full_domain {
                                            Domain::Nullable(NullableDomain { value, .. }) => {
                                                value.as_deref()?
                                            }
                                            domain => domain,
                                        };
                                        let (full_min, full_max) = full_value_domain.to_minmax();
                                        if full_min.is_null() || full_max.is_null() {
                                            return None;
                                        }

                                        let mut builder = ColumnBuilder::with_capacity(
                                            &call.return_type,
                                            boundaries.len(),
                                        );

                                        for (index, value) in col.iter().enumerate() {
                                            if ctx.has_error(index) {
                                                let fallback =
                                                    if index == 0 { &full_min } else { &full_max };
                                                builder.push(fallback.as_ref());
                                            } else {
                                                builder.push(value);
                                            }
                                        }
                                        builder.build().domain()
                                    } else {
                                        result.as_column().unwrap().domain()
                                    };
                                    Some(d)
                                }
                            }
                        }
                        (FunctionDomain::MayThrow, _) => None,
                        (FunctionDomain::Full, _) => Some(Domain::full(&call.return_type)),
                        (FunctionDomain::Domain(domain), _) => Some(domain),
                    }
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
                if node.args_len() > MAX_FUNCTION_ARGS_TO_FOLD {
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

                if self.mode == FoldMode::ContextIndependent {
                    let result = node.finish_fold(args_expr, changed);
                    return (result, None);
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
        let block = DataBlock::empty_with_rows(1);
        let evaluator = Evaluator::new(&block, self.func_ctx, self.fn_registry);
        // Since the expression is constant, it is safe to change its column index type.
        let projected_expr = expr
            .as_ref()
            .project_column_ref(|_| unreachable!())
            .unwrap();
        let Ok(Value::Scalar(scalar)) = evaluator.run(&projected_expr) else {
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

    fn can_evaluate_function(&self, id: &crate::FunctionID) -> bool {
        self.mode == FoldMode::Full
            || (self
                .fn_registry
                .get_property(id.name().as_ref())
                .is_some_and(|property| !property.non_deterministic)
                && !self.fn_registry.is_context_dependent(id))
    }

    fn can_evaluate_cast(&self, is_try: bool, src_type: &DataType, dest_type: &DataType) -> bool {
        self.mode == FoldMode::Full || self.context_independent_cast(is_try, src_type, dest_type)
    }

    fn context_independent_cast(
        &self,
        is_try: bool,
        src_type: &DataType,
        dest_type: &DataType,
    ) -> bool {
        if src_type == dest_type {
            return true;
        }

        let simple_dest_type = if is_try {
            let Some(dest_type) = dest_type.as_nullable() else {
                return false;
            };
            dest_type
        } else {
            dest_type
        };

        if matches!(src_type, DataType::Null) {
            return true;
        }

        // Composite casts are evaluated recursively. Check their inner casts before resolving a
        // nullable function overload so type-specific context-independent rules still apply.
        match (src_type, simple_dest_type) {
            (DataType::Nullable(inner_src), DataType::Nullable(inner_dest)) if !is_try => {
                return self.context_independent_cast(false, inner_src, inner_dest);
            }
            (DataType::Nullable(inner_src), inner_dest) if is_try => {
                return self.context_independent_cast(
                    true,
                    inner_src,
                    &inner_dest.clone().wrap_nullable(),
                );
            }
            (src, DataType::Nullable(inner_dest)) if !is_try => {
                return self.context_independent_cast(false, src, inner_dest);
            }
            (DataType::EmptyArray, DataType::Array(_)) => return true,
            (DataType::Array(inner_src), DataType::Array(inner_dest)) => {
                return self.context_independent_cast(false, inner_src, inner_dest);
            }
            (DataType::Tuple(src_fields), DataType::Tuple(dest_fields))
                if src_fields.len() == dest_fields.len() =>
            {
                return src_fields
                    .iter()
                    .zip(dest_fields)
                    .all(|(src, dest)| self.context_independent_cast(false, src, dest));
            }
            _ => {}
        }

        if let (DataType::Decimal(src_size), DataType::Decimal(dest_size)) =
            (src_type, simple_dest_type)
        {
            // Decimal casts only consult the session rounding mode when reducing scale.
            // Keeping exact widening casts foldable preserves useful constant bounds.
            return dest_size.scale() >= src_size.scale();
        }

        if let (DataType::Number(src_type), DataType::Decimal(_)) = (src_type, simple_dest_type)
            && src_type.is_integer()
        {
            // Integer-to-Decimal conversion does not round. Its value or overflow is independent
            // of the session rounding mode.
            return true;
        }

        if let Some(call) =
            resolve_cast_function(None, is_try, src_type, dest_type, self.fn_registry)
        {
            return self.can_evaluate_function(&call.id);
        }

        false
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

        if let Some(call) =
            resolve_cast_function(span, false, src_type, dest_type, self.fn_registry)
            && let Some(new_domain) = self.calculate_simple_cast(call, domain)
        {
            return new_domain;
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

        if let Some(call) = resolve_cast_function(span, true, src_type, dest_type, self.fn_registry)
            && let Some(new_domain) = self.calculate_simple_cast(call, domain)
        {
            return new_domain;
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

    fn calculate_simple_cast(&self, call: FunctionCall, domain: &Domain) -> Option<Option<Domain>> {
        let cast_expr = Expr::FunctionCall(call);

        let input_domains = [(0, domain.clone())].into_iter().collect();
        // The caller has already checked `can_evaluate_cast` for this source and destination type.
        // Some cast factories serve both context-independent and context-dependent type pairs, so
        // the resolved factory ID alone is too coarse to make this nested domain calculation.
        let (_, output_domain) = ConstantFolder::fold_with_domain(
            Cow::Owned(cast_expr),
            &input_domains,
            self.func_ctx,
            self.fn_registry,
        );

        Some(output_domain)
    }

    /// Check if AND expressions contain mutually exclusive range conditions
    /// Returns Some(true) if the expressions are mutually exclusive (should return false)
    /// Returns Some(false) if they are not mutually exclusive
    /// Returns None if analysis is inconclusive
    fn check_mutually_exclusive_ranges(&self, args: &[FoldResult<'_, Index>]) -> Option<bool> {
        // Track constraints for each column
        let mut column_constraints: HashMap<Index, Vec<RangeConstraint<Index>>> = HashMap::new();

        // Extract constraints from each expression
        for arg in args {
            if let Some(constraint) = RangeConstraint::try_from_expr(arg.as_ref()) {
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

            if self.are_combined_constraints_mutually_exclusive(&constraints) {
                return Some(true);
            }
        }

        None // No conclusive mutual exclusion found
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

    fn are_combined_constraints_mutually_exclusive(
        &self,
        constraints: &[RangeConstraint<Index>],
    ) -> bool {
        let mut lower = None;
        let mut upper = None;
        let mut not_eq_constants = Vec::new();

        for constraint in constraints {
            match constraint.operator.as_str() {
                "gt" => tighten_lower_bound(&mut lower, &constraint.constant, false),
                "gte" => tighten_lower_bound(&mut lower, &constraint.constant, true),
                "lt" => tighten_upper_bound(&mut upper, &constraint.constant, false),
                "lte" => tighten_upper_bound(&mut upper, &constraint.constant, true),
                "noteq" => not_eq_constants.push(&constraint.constant),
                _ => {}
            }
        }

        let (Some((lower, lower_inclusive)), Some((upper, upper_inclusive))) = (&lower, &upper)
        else {
            return false;
        };

        if lower > upper {
            return true;
        }
        if lower != upper {
            return false;
        }
        if !lower_inclusive || !upper_inclusive {
            return true;
        }

        not_eq_constants.contains(&lower)
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
            mode: FoldMode::Full,
        }
    }
}

fn tighten_lower_bound(bound: &mut Option<(Scalar, bool)>, constant: &Scalar, inclusive: bool) {
    let should_update = bound.as_ref().is_none_or(|(current, current_inclusive)| {
        constant > current || (constant == current && !inclusive && *current_inclusive)
    });
    if should_update {
        *bound = Some((constant.clone(), inclusive));
    }
}

fn tighten_upper_bound(bound: &mut Option<(Scalar, bool)>, constant: &Scalar, inclusive: bool) {
    let should_update = bound.as_ref().is_none_or(|(current, current_inclusive)| {
        constant < current || (constant == current && !inclusive && *current_inclusive)
    });
    if should_update {
        *bound = Some((constant.clone(), inclusive));
    }
}

fn constant_behind_nullable_cast<Index: ColumnIndex>(expr: &Expr<Index>) -> Option<&Constant> {
    if let Expr::Constant(constant) = expr {
        return Some(constant);
    }

    let Expr::Cast(Cast {
        is_try: false,
        expr,
        dest_type,
        ..
    }) = expr
    else {
        return None;
    };

    let Expr::Constant(constant) = expr.as_ref() else {
        return None;
    };

    (dest_type.is_nullable()
        && !constant.data_type.is_nullable()
        && dest_type.remove_nullable() == constant.data_type)
        .then_some(constant)
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

impl<Index: ColumnIndex> RangeConstraint<Index> {
    /// Extracts a normalized column-to-constant comparison. Comparisons with
    /// the constant on the left are flipped so the column is always the lhs.
    pub fn try_from_expr(expr: &Expr<Index>) -> Option<Self> {
        let Expr::FunctionCall(call) = expr else {
            return None;
        };
        Self::try_from_function_call(call)
    }

    pub fn try_from_function_call(call: &FunctionCall<Index>) -> Option<Self> {
        let FunctionCall { function, args, .. } = call;
        if args.len() != 2 {
            return None;
        }

        let op = function.signature.name.as_str();
        if !matches!(op, "gt" | "gte" | "lt" | "lte" | "eq" | "noteq") {
            return None;
        }

        if let (Some(column_ref), Some(constant)) = (
            args[0].as_column_ref(),
            constant_behind_nullable_cast(&args[1]),
        ) {
            return Some(Self {
                column_id: column_ref.id.clone(),
                data_type: column_ref.data_type.clone(),
                operator: op.to_string(),
                constant: constant.scalar.clone(),
                is_flipped: false,
            });
        }

        let (Some(constant), Some(column_ref)) = (
            constant_behind_nullable_cast(&args[0]),
            args[1].as_column_ref(),
        ) else {
            return None;
        };
        let operator = match op {
            "gt" => "lt",
            "gte" => "lte",
            "lt" => "gt",
            "lte" => "gte",
            "eq" => "eq",
            "noteq" => "noteq",
            _ => unreachable!(),
        };
        Some(Self {
            column_id: column_ref.id.clone(),
            data_type: column_ref.data_type.clone(),
            operator: operator.to_string(),
            constant: constant.scalar.clone(),
            is_flipped: true,
        })
    }
}
