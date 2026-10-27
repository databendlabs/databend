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
//! - its pre-aggregate form equals the pre-aggregate expression of `K`,
//!   compared with `ScalarExpr::eq` without any normalization. The
//!   pre-aggregate form is the node bound again with matching suspended and
//!   column-dependent folding off, resolving SELECT aliases to the
//!   pre-aggregate forms of their items.
//!
//! As with GROUP BY, whether the key is deterministic does not matter, so a
//! key over `now()` or a script UDF is matched like any other.
//!
//! The outermost matching node wins, as with GROUP BY, so `a + 1` in
//! `GROUPING SETS ((a), (a + 1))` binds to the key `a + 1` rather than to the
//! key `a` plus one. As with GROUP BY, an alias stands for the expression of
//! its item, so with `SELECT a AS k`, `k + 1` is the key `a + 1`. A node that
//! refers to an alias whose item has no pre-aggregate form after the
//! aggregate, see `grouping_sets_select.rs`, is not matched. A set-returning
//! function matches a key over the output of the same pre-aggregate
//! set-returning function. Nested contexts, such as subqueries and lambda
//! bodies, match the keys of the active scopes of their ancestors; a reference
//! to an ungrouped column of such an ancestor is rejected. Expressions that
//! are not eligible can only refer to a key through a SELECT alias or
//! position.
//!
//! Trying every node would bind each node once per enclosing node, which is
//! quadratic in the depth of the expression. Instead, the pre-aggregate form
//! of a node for a scope is bound once: it is memoized in the arena while the
//! form of an enclosing node is bound, and reused when the node itself is
//! tried. The eligibility of subtrees is memoized as well. A pre-aggregate
//! form only depends on the node, the scope and the type checker, which owns
//! the binding context; type checkers that share an arena, such as the one of
//! a lambda body, keep separate forms.
//!
//! [`GroupingKeyScope`]: crate::binder::GroupingKeyScope

use std::collections::HashMap;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;

use databend_common_ast::Span;
use databend_common_ast::ast::ColumnRef;
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
use crate::plans::ScalarExpr;

#[derive(Clone, Copy, Debug)]
pub(super) struct GroupingKeyState {
    /// Identifies the type checker in the memoized pre-aggregate forms.
    checker: u64,
    /// Set while binding the pre-aggregate form of a node for the scope at
    /// this position of the context chain (0 is the current context): the
    /// keys of that scope and of the scopes nested in it are not matched, and
    /// column-dependent expressions are not folded.
    binding_input_of: Option<usize>,
    /// Set while binding a pre-aggregate form if it refers to an alias whose
    /// item has no pre-aggregate form; the node is then not matched.
    input_unavailable: bool,
}

impl GroupingKeyState {
    pub(super) fn new() -> Self {
        static NEXT_CHECKER: AtomicU64 = AtomicU64::new(0);
        Self {
            checker: NEXT_CHECKER.fetch_add(1, Ordering::Relaxed),
            binding_input_of: None,
            input_unavailable: false,
        }
    }
}

/// Grouping-key matching work memoized in a [`CoreExprArena`].
#[derive(Default)]
pub(super) struct GroupingKeyMemo {
    /// The eligibility of subtrees, by node.
    eligibility: HashMap<usize, Eligibility>,
    /// The pre-aggregate forms of candidate nodes, by type checker, node and
    /// position of the scope in the context chain.
    inputs: HashMap<(u64, usize, usize), PreAggregateForm>,
}

#[derive(Clone, Copy)]
struct Eligibility {
    eligible: bool,
    has_column: bool,
}

impl Eligibility {
    const INELIGIBLE: Self = Self {
        eligible: false,
        has_column: false,
    };
}

struct PreAggregateForm {
    scalar: ScalarExpr,
    data_type: DataType,
    input_unavailable: bool,
}

impl CoreExprArena<'_> {
    /// Whether the subtree is structurally eligible as a grouping key.
    fn is_grouping_key_candidate(&self, id: CoreExprId) -> bool {
        let eligibility = self.grouping_key_eligibility(id);
        eligibility.eligible && eligibility.has_column
    }

    /// Whether the subtree is structurally eligible as a part of a grouping
    /// key, which may reference no column.
    pub(super) fn is_grouping_key_eligible(&self, id: CoreExprId) -> bool {
        self.grouping_key_eligibility(id).eligible
    }

    /// The column references in the arena.
    pub(super) fn column_refs(&self) -> impl Iterator<Item = &ColumnRef> {
        self.nodes.iter().filter_map(|node| match node {
            CoreExpr::ColumnRef { column, .. } => Some(*column),
            _ => None,
        })
    }

    fn are_grouping_key_eligible(&self, ids: impl Iterator<Item = CoreExprId>) -> Eligibility {
        let mut has_column = false;
        for id in ids {
            let eligibility = self.grouping_key_eligibility(id);
            if !eligibility.eligible {
                return Eligibility::INELIGIBLE;
            }
            has_column |= eligibility.has_column;
        }
        Eligibility {
            eligible: true,
            has_column,
        }
    }

    #[recursive::recursive]
    fn grouping_key_eligibility(&self, id: CoreExprId) -> Eligibility {
        if let Some(eligibility) = self.grouping_key_memo.borrow().eligibility.get(&id.index) {
            return *eligibility;
        }
        let eligibility = match self.get(id) {
            CoreExpr::ColumnRef { .. } => Eligibility {
                eligible: true,
                has_column: true,
            },
            CoreExpr::Literal { .. } => Eligibility {
                eligible: true,
                has_column: false,
            },
            CoreExpr::Array { exprs, .. } | CoreExpr::Tuple { exprs, .. } => {
                self.are_grouping_key_eligible(exprs.iter().copied())
            }
            CoreExpr::Map { kvs, .. } => {
                self.are_grouping_key_eligible(kvs.iter().map(|(_, value)| *value))
            }
            CoreExpr::MapAccess { expr, .. } | CoreExpr::Cast { expr, .. } => {
                self.grouping_key_eligibility(*expr)
            }
            CoreExpr::Call {
                func_name, args, ..
            } => {
                if func_name.eq_ignore_ascii_case("grouping") {
                    Eligibility::INELIGIBLE
                } else {
                    self.are_grouping_key_eligible(args.iter().copied())
                }
            }
            CoreExpr::ScalarFunction { params, args, .. } => self.are_grouping_key_eligible(
                params
                    .iter()
                    .map(|(_, param)| *param)
                    .chain(args.iter().copied()),
            ),
            CoreExpr::InList { expr, list, .. } => {
                self.are_grouping_key_eligible(std::iter::once(*expr).chain(list.iter().copied()))
            }
            CoreExpr::SpecialFunction { function, .. } => {
                self.are_grouping_key_eligible(function.args().into_iter())
            }
            CoreExpr::UdfCall {
                args, filter: None, ..
            } => self.are_grouping_key_eligible(args.iter().map(|(_, arg)| *arg)),
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
            | CoreExpr::StageLocation { .. } => Eligibility::INELIGIBLE,
        };
        self.grouping_key_memo
            .borrow_mut()
            .eligibility
            .insert(id.index, eligibility);
        eligibility
    }

    /// Whether the node is a set-returning function over eligible arguments.
    fn is_grouping_key_srf_candidate(&self, id: CoreExprId) -> bool {
        let CoreExpr::SetReturningFunction { args, .. } = self.get(id) else {
            return false;
        };
        let eligibility = self.are_grouping_key_eligible(args.iter().copied());
        eligibility.eligible && eligibility.has_column
    }
}

fn active_scope(bind_context: &BindContext) -> bool {
    bind_context
        .grouping_key_scope
        .as_ref()
        .is_some_and(|scope| scope.active)
}

/// Whether the keys of this SELECT are not matched, as in the arguments of its
/// aggregate functions, which are evaluated before the aggregate.
fn suspended_scope(bind_context: &BindContext) -> bool {
    bind_context
        .grouping_key_scope
        .as_ref()
        .is_some_and(|scope| !scope.active)
}

/// The current context followed by its ancestors.
fn context_chain(bind_context: &BindContext) -> impl Iterator<Item = &BindContext> {
    std::iter::successors(Some(bind_context), |context| context.parent.as_deref())
}

impl<A> TypeChecker<'_, A>
where A: TypeCheckAdapter
{
    /// The position of the scope whose pre-aggregate form is being bound.
    pub(super) fn binding_grouping_key_input_of(&self) -> Option<usize> {
        self.grouping_key.binding_input_of
    }

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
            } else if !input.used_columns().is_empty() {
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
    /// `None` if it refers to an alias whose item has no pre-aggregate form.
    /// Binding errors are returned as is.
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
            ..state
        };
        let result = self.resolve_core(arena, id);
        let input_unavailable = self.grouping_key.input_unavailable;
        self.grouping_key = state;
        let deref!((input, _)) = result?;
        Ok((!input_unavailable).then_some(input))
    }

    /// Resolve a node while binding a pre-aggregate form for the scope at
    /// `position`. The form of a candidate node is memoized, so that binding
    /// the form of an enclosing node and trying the node itself bind it only
    /// once.
    pub(super) fn resolve_grouping_key_input_node(
        &mut self,
        arena: &CoreExprArena<'_>,
        id: CoreExprId,
        position: usize,
    ) -> Result<Box<(ScalarExpr, DataType)>> {
        let key = (self.grouping_key.checker, id.index, position);
        if let Some(form) = arena.grouping_key_memo.borrow().inputs.get(&key) {
            self.grouping_key.input_unavailable |= form.input_unavailable;
            return Ok(Box::new((form.scalar.clone(), form.data_type.clone())));
        }
        let enclosing_input_unavailable = std::mem::take(&mut self.grouping_key.input_unavailable);
        let result = self.resolve_core_node(arena, id);
        let input_unavailable = self.grouping_key.input_unavailable;
        self.grouping_key.input_unavailable = enclosing_input_unavailable || input_unavailable;
        let resolved = result?;
        if arena.is_grouping_key_candidate(id) {
            arena
                .grouping_key_memo
                .borrow_mut()
                .inputs
                .insert(key, PreAggregateForm {
                    scalar: resolved.0.clone(),
                    data_type: resolved.1.clone(),
                    input_unavailable,
                });
        }
        Ok(resolved)
    }

    /// Resolve an alias of the SELECT item at `position`, whose form after the
    /// aggregate is `scalar`. Where the expression around the alias is
    /// evaluated before the aggregate, while a node is matched to the keys and
    /// in the arguments of an aggregate function, the alias stands for the
    /// pre-aggregate form of the item instead. A node that refers to an item
    /// without such a form is not matched, and in aggregate arguments the
    /// alias keeps standing for `scalar`.
    pub(super) fn resolve_grouping_sets_alias(
        &mut self,
        position: usize,
        scalar: ScalarExpr,
    ) -> Result<ScalarExpr> {
        let matching = self.grouping_key.binding_input_of.is_some();
        if !matching && !suspended_scope(self.bind_context) {
            return Ok(scalar);
        }
        match self.post_aggregate_alias_form(position)? {
            Some(form) => Ok(form),
            None => {
                self.grouping_key.input_unavailable |= matching;
                Ok(scalar)
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
