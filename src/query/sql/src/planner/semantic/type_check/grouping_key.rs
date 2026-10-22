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

//! Binding post-aggregate expressions of a SELECT with GROUPING SETS.
//!
//! Each grouping key outputs a dedicated nullable symbol. While the
//! [`GroupingKeyScope`] of a SELECT is active, every node is checked top-down,
//! before its children are resolved. A node binds to the output of key `K` iff
//!
//! - it is structurally eligible: it references a column and contains no
//!   subquery, lambda, async, search, window or aggregate function, nor
//!   `grouping()`;
//! - its pre-aggregate form is deterministic, so in particular contains no
//!   UDF other than SQL UDFs, which expand to builtin functions in place. The
//!   pre-aggregate form is the
//!   node bound again with matching suspended and column-dependent folding
//!   off, resolving SELECT aliases to the pre-aggregate SELECT items;
//! - the pre-aggregate form equals the pre-aggregate expression of `K`,
//!   compared with `ScalarExpr::eq` without any normalization.
//!
//! The outermost matching node wins, so `a + 1` in
//! `GROUPING SETS ((a), (a + 1))` binds to the key `a + 1` rather than to the
//! key `a` plus one. A set-returning function matches a key over the output of
//! the same pre-aggregate set-returning function. Nested contexts, such as
//! subqueries and lambda bodies, match the keys of the active scopes of their
//! ancestors; a reference to an ungrouped column of such an ancestor is
//! rejected. Expressions that are not eligible can only refer to a key through
//! a SELECT alias or position.
//!
//! [`GroupingKeyScope`]: crate::binder::GroupingKeyScope

use databend_common_ast::Span;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::types::DataType;

use super::CoreExpr;
use super::CoreExprArena;
use super::CoreExprId;
use super::TypeCheckAdapter;
use super::TypeChecker;
use crate::BindContext;
use crate::ColumnBinding;
use crate::Symbol;
use crate::Visibility;
use crate::binder::ColumnBindingBuilder;
use crate::plans::BoundColumnRef;
use crate::plans::FunctionCall;
use crate::plans::ScalarExpr;

#[derive(Clone, Copy, Debug, Default)]
pub(super) struct GroupingKeyState {
    /// Set while binding the pre-aggregate form of a node for the scope at
    /// this position of the context chain (0 is the current context): the
    /// keys of that scope and of the scopes nested in it are not matched, and
    /// column-dependent expressions are not folded.
    binding_input_of: Option<usize>,
    /// Set while binding a pre-aggregate form if an alias in it has no known
    /// pre-aggregate form; the node is then not matched.
    input_unavailable: bool,
}

impl CoreExprArena<'_> {
    /// Whether the subtree is structurally eligible as a grouping key.
    fn is_grouping_key_candidate(&self, id: CoreExprId) -> bool {
        let mut has_column = false;
        self.is_grouping_key_eligible(id, &mut has_column) && has_column
    }

    fn are_grouping_key_eligible(
        &self,
        mut ids: impl Iterator<Item = CoreExprId>,
        has_column: &mut bool,
    ) -> bool {
        ids.all(|id| self.is_grouping_key_eligible(id, has_column))
    }

    fn is_grouping_key_eligible(&self, id: CoreExprId, has_column: &mut bool) -> bool {
        match self.get(id) {
            CoreExpr::ColumnRef { .. } => {
                *has_column = true;
                true
            }
            CoreExpr::Literal { .. } => true,
            CoreExpr::Array { exprs, .. } | CoreExpr::Tuple { exprs, .. } => {
                self.are_grouping_key_eligible(exprs.iter().copied(), has_column)
            }
            CoreExpr::Map { kvs, .. } => {
                self.are_grouping_key_eligible(kvs.iter().map(|(_, value)| *value), has_column)
            }
            CoreExpr::MapAccess { expr, .. } | CoreExpr::Cast { expr, .. } => {
                self.is_grouping_key_eligible(*expr, has_column)
            }
            CoreExpr::Call {
                func_name, args, ..
            } => {
                !func_name.eq_ignore_ascii_case("grouping")
                    && self.are_grouping_key_eligible(args.iter().copied(), has_column)
            }
            CoreExpr::ScalarFunction { params, args, .. } => self.are_grouping_key_eligible(
                params
                    .iter()
                    .map(|(_, param)| *param)
                    .chain(args.iter().copied()),
                has_column,
            ),
            CoreExpr::InList { expr, list, .. } => self.are_grouping_key_eligible(
                std::iter::once(*expr).chain(list.iter().copied()),
                has_column,
            ),
            CoreExpr::SpecialFunction { function, .. } => {
                self.are_grouping_key_eligible(function.args().into_iter(), has_column)
            }
            // A SQL UDF expands to builtin functions; other UDFs leave a
            // non-deterministic call in the pre-aggregate form.
            // TODO: script and external UDFs carry no determinism declaration,
            // so a key over one is only referenced by its alias or position.
            CoreExpr::UdfCall {
                args, filter: None, ..
            } => self.are_grouping_key_eligible(args.iter().map(|(_, arg)| *arg), has_column),
            CoreExpr::UdfCall { .. }
            | CoreExpr::LambdaFunction { .. }
            | CoreExpr::SearchFunction { .. }
            | CoreExpr::AsyncFunction { .. }
            | CoreExpr::SetReturningFunction { .. }
            | CoreExpr::Subquery { .. }
            | CoreExpr::AggregateFunction { .. }
            | CoreExpr::AggregateWindowFunction { .. }
            | CoreExpr::CountAllWindowFunction { .. }
            | CoreExpr::GeneralWindowFunction { .. }
            | CoreExpr::StageLocation { .. } => false,
        }
    }

    /// Whether the node is a set-returning function over eligible arguments.
    fn is_grouping_key_srf_candidate(&self, id: CoreExprId) -> bool {
        let CoreExpr::SetReturningFunction { args, .. } = self.get(id) else {
            return false;
        };
        let mut has_column = false;
        self.are_grouping_key_eligible(args.iter().copied(), &mut has_column) && has_column
    }
}

fn active_scope(bind_context: &BindContext) -> bool {
    bind_context
        .grouping_key_scope
        .as_ref()
        .is_some_and(|scope| scope.active)
}

/// The current context followed by its ancestors.
fn context_chain(bind_context: &BindContext) -> impl Iterator<Item = &BindContext> {
    std::iter::successors(Some(bind_context), |context| context.parent.as_deref())
}

impl<A> TypeChecker<'_, A>
where A: TypeCheckAdapter
{
    pub(super) fn defer_column_folding(&self) -> bool {
        self.grouping_key.binding_input_of.is_some() || self.bind_context.defer_column_folding
    }

    /// Stop matching the keys of this SELECT, e.g. inside the arguments of
    /// its aggregates, which are evaluated before the aggregate. Returns
    /// whether to resume.
    pub(super) fn suspend_grouping_key_scope(&mut self) -> bool {
        match &mut self.bind_context.grouping_key_scope {
            Some(scope) if scope.active => {
                scope.active = false;
                true
            }
            _ => false,
        }
    }

    pub(super) fn resume_grouping_key_scope(&mut self, suspended: bool) {
        if suspended && let Some(scope) = &mut self.bind_context.grouping_key_scope {
            scope.active = true;
        }
    }

    /// The positions in the context chain of the scopes whose keys are matched.
    fn matched_scopes(&self) -> Vec<usize> {
        let first = self
            .grouping_key
            .binding_input_of
            .map_or(0, |position| position + 1);
        context_chain(self.bind_context)
            .enumerate()
            .skip(first)
            .filter(|(_, context)| active_scope(context))
            .map(|(position, _)| position)
            .collect()
    }

    /// Resolve the node to the output of the grouping key it matches. The
    /// scopes are tried from the innermost.
    pub(super) fn try_resolve_grouping_key(
        &mut self,
        arena: &CoreExprArena<'_>,
        id: CoreExprId,
    ) -> Result<Option<Box<(ScalarExpr, DataType)>>> {
        let scopes = self.matched_scopes();
        if scopes.is_empty() {
            return Ok(None);
        }
        let is_srf = if arena.is_grouping_key_candidate(id) {
            false
        } else if arena.is_grouping_key_srf_candidate(id) {
            true
        } else {
            return Ok(None);
        };

        for position in scopes {
            // Set-returning functions are only keys of this SELECT.
            if is_srf && position != 0 {
                break;
            }
            let Some(input) = self.resolve_grouping_key_input(arena, id, position)? else {
                continue;
            };
            let context = context_chain(self.bind_context).nth(position).unwrap();
            let column = if is_srf {
                lookup_grouping_key_srf(context, &input)?
            } else if is_deterministic_input(&input) && !input.used_columns().is_empty() {
                context.aggregate_info.lookup_grouping_key_output(&input)
            } else {
                None
            };
            if let Some(column) = column {
                let data_type = (*column.data_type).clone();
                let span = input.span();
                return Ok(Some(Box::new((
                    BoundColumnRef { span, column }.into(),
                    data_type,
                ))));
            }
        }
        Ok(None)
    }

    /// Bind the pre-aggregate form of the node for the scope at `position`, or
    /// `None` if an alias in it has no known pre-aggregate form. Binding errors
    /// are returned as is.
    fn resolve_grouping_key_input(
        &mut self,
        arena: &CoreExprArena<'_>,
        id: CoreExprId,
        position: usize,
    ) -> Result<Option<ScalarExpr>> {
        let state = self.grouping_key;
        self.grouping_key = GroupingKeyState {
            binding_input_of: Some(position),
            input_unavailable: false,
        };
        let result = self.resolve_core(arena, id);
        let unavailable = self.grouping_key.input_unavailable;
        self.grouping_key = state;
        let deref!((input, _)) = result?;
        Ok((!unavailable).then_some(input))
    }

    /// While binding a pre-aggregate form, map an alias of the grouped SELECT
    /// to the pre-aggregate SELECT item.
    pub(super) fn grouping_key_alias_input(&mut self, scalar: ScalarExpr) -> ScalarExpr {
        if self.grouping_key.binding_input_of.is_none() || !active_scope(self.bind_context) {
            return scalar;
        }
        let scope = self.bind_context.grouping_key_scope.as_ref().unwrap();
        match scope.alias_input(&scalar) {
            Some(input) => input.clone(),
            None => {
                self.grouping_key.input_unavailable = true;
                scalar
            }
        }
    }

    /// Reject a reference from a nested context to an input column of an
    /// enclosing grouped SELECT that no grouping key covers.
    pub(super) fn check_grouping_key_outer_column(
        &self,
        span: Span,
        column: &ColumnBinding,
    ) -> Result<()> {
        if self.grouping_key.binding_input_of.is_some() {
            return Ok(());
        }
        let is_input = |context: &BindContext, index: Symbol| {
            context.columns.iter().any(|column| column.index == index)
        };
        if !is_input(self.bind_context, column.index)
            && context_chain(self.bind_context)
                .skip(1)
                .any(|context| active_scope(context) && is_input(context, column.index))
        {
            return Err(ErrorCode::SemanticError(format!(
                "column \"{}\" must appear in the GROUP BY clause or be used in an aggregate function",
                column.column_name
            ))
            .set_span(span));
        }
        Ok(())
    }
}

/// A set-returning function matches a key over the output of the same
/// pre-aggregate set-returning function of the SELECT.
fn lookup_grouping_key_srf(
    context: &BindContext,
    input: &ScalarExpr,
) -> Result<Option<ColumnBinding>> {
    // Keys refer to registered set-returning functions by their outputs.
    let mut input = input.clone();
    for srf in &context.srf_info.srfs {
        // Bound column references compare by symbol and table only.
        let column = BoundColumnRef {
            span: None,
            column: ColumnBindingBuilder::new(
                String::new(),
                srf.index,
                Box::new(srf.scalar.data_type().into_owned()),
                Visibility::Visible,
            )
            .build(),
        };
        input.replace_sub_scalar(srf.scalar.clone(), column.into())?;
    }
    Ok(context.aggregate_info.lookup_grouping_key_output(&input))
}

/// Whether the pre-aggregate form is deterministic. An expanded SQL UDF is as
/// deterministic as its body.
fn is_deterministic_input(scalar: &ScalarExpr) -> bool {
    match scalar {
        ScalarExpr::UDFLambdaCall(udf) => is_deterministic_input(&udf.scalar),
        ScalarExpr::CastExpr(cast) => is_deterministic_input(&cast.argument),
        ScalarExpr::FunctionCall(func) => {
            let call: ScalarExpr = FunctionCall {
                arguments: vec![],
                ..func.clone()
            }
            .into();
            call.is_deterministic() && func.arguments.iter().all(is_deterministic_input)
        }
        _ => scalar.is_deterministic(),
    }
}
