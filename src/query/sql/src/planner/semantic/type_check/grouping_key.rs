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

//! Binding post-aggregate expressions of a grouped SELECT.
//!
//! Ordinary keys retain their input types; grouping-sets keys expose dedicated
//! nullable outputs. A global aggregate has an active scope with no keys. While the
//! [`GroupingKeyScope`] of a SELECT is active, every node is checked top-down,
//! before its children are resolved. A node binds to the output of key `K` iff
//!
//! - it is structurally eligible: it contains no subquery, lambda, search,
//!   window or aggregate function, nor `grouping()`, and it references a
//!   column or calls a function;
//! - its pre-aggregate form is not a deterministic expression without
//!   columns, unless the node references a column or `K` is a key of grouping
//!   sets of this SELECT;
//! - its pre-aggregate form equals the pre-aggregate expression of `K`,
//!   compared with `ScalarExpr::eq` without any normalization. The
//!   pre-aggregate form is the node bound again with matching suspended and
//!   the key's folding policy (normal for ordinary keys, structure-preserving
//!   for grouping sets), resolving SELECT aliases to the
//!   pre-aggregate forms of their items, see `pre_aggregate.rs`.
//!
//! A deterministic node without columns, such as `1 + 1`, is not matched to
//! an ordinary key: it evaluates to the same value after the aggregate, and
//! once folded, any expression that folds to the same constant would match
//! it. It is matched to a key of grouping sets, whose output is NULL in the
//! rows of the sets without the key, so `1 + 1` in `SELECT 1 + 1 FROM t GROUP
//! BY ROLLUP(1 + 1)` is NULL in the grand total. Forms of grouping-set keys
//! are not folded, so `coalesce(1 + 1, 100)` is not that key but an
//! expression over it, and `a + (1 + 1)` is not the key `a + 2`. Such a node
//! is not matched to a key of an enclosing SELECT, which it does not refer
//! to.
//!
//! A node that is not deterministic, such as `rand()`, `now()`, `nextval(s)`
//! or a script UDF, is matched like any other, since evaluating it again after
//! the aggregate gives another value than the key. Literals and constant
//! tuples call no function, so they are never bound to be tried: `2` in
//! `SELECT 2 FROM t GROUP BY ROLLUP(1 + 1)` is 2 in every row.
//!
//! The outermost matching node wins, as with GROUP BY, so `a + 1` in
//! `GROUPING SETS ((a), (a + 1))` binds to the key `a + 1` rather than to the
//! key `a` plus one. As with GROUP BY, an alias stands for the expression of
//! its item, so with `SELECT a AS k`, `k + 1` is the key `a + 1`. A node that
//! refers to an alias whose item has no pre-aggregate form after the
//! aggregate, see `grouped_select.rs`, is not matched. A set-returning
//! function matches a key over the output of the same pre-aggregate
//! set-returning function. Nested contexts, such as subqueries and lambda
//! bodies, match the keys of the active scopes of their ancestors; a reference
//! to an ungrouped column of such an ancestor is rejected. Expressions that
//! are not eligible can only refer to a key through a SELECT alias or
//! position.
//!
//! Trying every node would bind each node once per enclosing node, which is
//! quadratic in the depth of the expression. Instead, the pre-aggregate form
//! of a node for a scope is bound once: it is memoized while the form of an
//! enclosing node is bound, and reused when the node itself is tried. The
//! eligibility of subtrees is memoized as well. Each form retains its reported
//! type and folding facts, so memo hits do not derive facts by walking the
//! expression again. Cloning and hashing the owned scalar trees still traverse
//! subtrees; the memo bounds repeated binding, not all expression work.
//!
//! The memo, [`GroupingKeyMemo`], is owned by the type checker, apart from
//! the arena, which only holds the lowered expressions: the forms are binding
//! results, only valid for the arena being bound and for the binding context
//! of the type checker. So a type checker binds every arena with a memo of its
//! own, and so does the type checker of a lambda body, whose parameters are
//! bound anew each time. The pre-aggregate forms of the
//! SELECT items are kept apart, with the items, see `pre_aggregate.rs`.
//!
//! A form is a speculative binding: a reference to an ungrouped input column
//! of an enclosing scope is not rejected while binding it, since the node may
//! still match a key of that scope, but kept with the form, and raised where
//! the form is used as the binding of the node, e.g. as the pre-aggregate form
//! of a SELECT item.
//!
//! Binding a form must have no other effect that depends on the phase: it
//! registers no aggregate or set-returning function, binds no subquery and
//! allocates no symbol, which debug builds check. Structural eligibility
//! excludes the nodes whose binding has such effects, and an IN list that is
//! long enough to become a subquery stays a list in keys and their forms.
//!
//! [`GroupingKeyScope`]: crate::binder::GroupingKeyScope

use std::collections::HashMap;

use databend_common_ast::Span;
use databend_common_ast::ast::ColumnRef;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use smallvec::SmallVec;
use smallvec::smallvec;

use super::CoreExpr;
use super::CoreExprArena;
use super::CoreExprId;
use super::TypeCheckAdapter;
use super::TypeChecker;
use super::scalar_fold::BoundScalar;
use crate::BindContext;
use crate::ColumnBinding;
use crate::Visibility;
use crate::binder::AggregateRewriter;
use crate::binder::AliasLookup;
use crate::binder::ColumnBindingBuilder;
use crate::binder::ExprContext;
use crate::binder::GroupingScopeState;
use crate::binder::SetReturningAnalyzer;
use crate::plans::BoundColumnRef;
use crate::plans::ScalarExpr;
use crate::plans::VisitorMut;

/// Which side of a grouping boundary is being bound.
#[derive(Clone, Copy, Debug, Default)]
enum GroupingKeyPhase {
    #[default]
    Output,
    /// The pre-aggregate form for this context-chain position (0 is local).
    /// Keys of this scope and scopes nested inside it are not matched.
    Input { scope: usize },
}

impl GroupingKeyPhase {
    fn input_scope(self) -> Option<usize> {
        match self {
            Self::Output => None,
            Self::Input { scope } => Some(scope),
        }
    }
}

/// The grouping-key matching in progress in a type checker. Folding policy
/// is kept separately so phase changes do not implicitly change scalar facts.
#[derive(Clone, Debug, Default)]
pub(super) struct GroupingKeyState {
    phase: GroupingKeyPhase,
    /// Set while binding a pre-aggregate form if it refers to an alias whose
    /// item has no pre-aggregate form; the node is then not matched.
    input_unavailable: bool,
    /// The first error deferred while binding a pre-aggregate form.
    deferred_error: Option<ErrorCode>,
    /// Whether the arguments of `grouping()` are being bound, which must be
    /// keys of this SELECT.
    in_grouping_arguments: bool,
}

/// Grouping-key matching work memoized for the arena that a type checker
/// binds.
#[derive(Default)]
pub(super) struct GroupingKeyMemo {
    /// The eligibility of subtrees, by node.
    eligibility: HashMap<usize, Eligibility>,
    /// The pre-aggregate forms of candidate nodes, by node and position of the
    /// scope in the context chain.
    forms: HashMap<(usize, usize), PreAggregateForm>,
}

#[derive(Clone, Copy)]
struct Eligibility {
    eligible: bool,
    has_column: bool,
    /// Whether the subtree calls a function, which may not be deterministic.
    has_call: bool,
}

impl Eligibility {
    const INELIGIBLE: Self = Self {
        eligible: false,
        has_column: false,
        has_call: false,
    };

    /// Whether the subtree is a grouping key candidate. One without columns
    /// is only matched if its form is not deterministic, see the module
    /// documentation, so one that calls no function is not tried.
    fn is_candidate(self) -> bool {
        self.eligible && (self.has_column || self.has_call)
    }
}

/// Counts of what binding registers, which speculative binding must leave
/// unchanged.
#[cfg(debug_assertions)]
#[derive(Debug, PartialEq, Eq)]
pub(super) struct PhaseEffects {
    tables: usize,
    symbols: usize,
    aggregate_calls: usize,
    set_returning_functions: usize,
}

struct PreAggregateForm {
    bound: BoundScalar,
    input_unavailable: bool,
    deferred_error: Option<ErrorCode>,
}

impl CoreExprArena<'_> {
    /// The column references of a subtree that is eligible as a part of a
    /// grouping key.
    pub(super) fn grouping_key_column_refs(&self, id: CoreExprId) -> Vec<&ColumnRef> {
        let mut column_refs = Vec::new();
        let mut pending = vec![id];
        while let Some(id) = pending.pop() {
            if let CoreExpr::ColumnRef { column, .. } = self.get(id) {
                column_refs.push(*column);
            }
            pending.extend(grouping_key_children(self.get(id)).into_iter().flatten());
        }
        column_refs
    }
}

/// The children of a node that is structurally eligible as a part of a
/// grouping key if its children are, or `None` if it isn't.
fn grouping_key_children(expr: &CoreExpr<'_>) -> Option<SmallVec<[CoreExprId; 4]>> {
    Some(match expr {
        CoreExpr::ColumnRef { .. } | CoreExpr::Literal { .. } => smallvec![],
        CoreExpr::Array { exprs, .. } | CoreExpr::Tuple { exprs, .. } => exprs.clone(),
        CoreExpr::Map { kvs, .. } => kvs.iter().map(|(_, value)| *value).collect(),
        CoreExpr::MapAccess { expr, .. } | CoreExpr::Cast { expr, .. } => smallvec![*expr],
        CoreExpr::Call {
            func_name, args, ..
        } => {
            if func_name.eq_ignore_ascii_case("grouping") {
                return None;
            }
            args.clone()
        }
        CoreExpr::ScalarFunction { params, args, .. } => params
            .iter()
            .map(|(_, param)| *param)
            .chain(args.iter().copied())
            .collect(),
        CoreExpr::InList { expr, list, .. } => {
            std::iter::once(*expr).chain(list.iter().copied()).collect()
        }
        CoreExpr::SpecialFunction { function, .. } => function.args().into_iter().collect(),
        CoreExpr::AsyncFunction { function, .. } => function.args().into_iter().collect(),
        CoreExpr::UdfCall {
            args, filter: None, ..
        } => args.iter().map(|(_, arg)| *arg).collect(),
        CoreExpr::UdfCall { .. }
        | CoreExpr::LambdaFunction { .. }
        | CoreExpr::SearchFunction { .. }
        | CoreExpr::SetReturningFunction { .. }
        | CoreExpr::Subquery { .. }
        | CoreExpr::AggregateFunction { .. }
        | CoreExpr::AggregateWindowFunction { .. }
        | CoreExpr::CountAllWindowFunction { .. }
        | CoreExpr::GeneralWindowFunction { .. }
        | CoreExpr::StageLocation { .. } => return None,
    })
}

fn active_scope(bind_context: &BindContext) -> bool {
    matches!(
        bind_context.grouping_scope_state(),
        Some(GroupingScopeState::Active)
    )
}

/// The current context followed by its ancestors.
fn context_chain(bind_context: &BindContext) -> impl Iterator<Item = &BindContext> {
    std::iter::successors(Some(bind_context), |context| context.parent.as_deref())
}

impl<A> TypeChecker<'_, A>
where A: TypeCheckAdapter
{
    /// Run `f`, in which the type checker binds another arena, with a memo
    /// of its own.
    pub(super) fn with_own_grouping_key_memo<R>(&mut self, f: impl FnOnce(&mut Self) -> R) -> R {
        let memo = std::mem::take(&mut self.grouping_key_memo);
        let result = f(self);
        self.grouping_key_memo = memo;
        result
    }

    /// Whether the subtree is structurally eligible as a grouping key.
    fn is_grouping_key_candidate(&mut self, arena: &CoreExprArena<'_>, id: CoreExprId) -> bool {
        self.grouping_key_eligibility(arena, id).is_candidate()
    }

    /// Whether the subtree is structurally eligible as a part of a grouping
    /// key, which may reference no column.
    pub(super) fn is_grouping_key_eligible(
        &mut self,
        arena: &CoreExprArena<'_>,
        id: CoreExprId,
    ) -> bool {
        self.grouping_key_eligibility(arena, id).eligible
    }

    /// Whether the node is a set-returning function over eligible arguments.
    fn is_grouping_key_srf_candidate(&mut self, arena: &CoreExprArena<'_>, id: CoreExprId) -> bool {
        let CoreExpr::SetReturningFunction { args, .. } = arena.get(id) else {
            return false;
        };
        let eligibility = self.are_grouping_key_eligible(arena, args.iter().copied());
        eligibility.eligible && eligibility.has_column
    }

    fn are_grouping_key_eligible(
        &mut self,
        arena: &CoreExprArena<'_>,
        ids: impl Iterator<Item = CoreExprId>,
    ) -> Eligibility {
        let mut has_column = false;
        let mut has_call = false;
        for id in ids {
            let eligibility = self.grouping_key_eligibility(arena, id);
            if !eligibility.eligible {
                return Eligibility::INELIGIBLE;
            }
            has_column |= eligibility.has_column;
            has_call |= eligibility.has_call;
        }
        Eligibility {
            eligible: true,
            has_column,
            has_call,
        }
    }

    #[recursive::recursive]
    fn grouping_key_eligibility(
        &mut self,
        arena: &CoreExprArena<'_>,
        id: CoreExprId,
    ) -> Eligibility {
        if let Some(eligibility) = self.grouping_key_memo.eligibility.get(&id.index) {
            return *eligibility;
        }
        let expr = arena.get(id);
        let eligibility = match grouping_key_children(expr) {
            None => Eligibility::INELIGIBLE,
            Some(children) => {
                let eligibility = self.are_grouping_key_eligible(arena, children.into_iter());
                Eligibility {
                    has_column: eligibility.has_column
                        || (eligibility.eligible && matches!(expr, CoreExpr::ColumnRef { .. })),
                    has_call: eligibility.has_call
                        || (eligibility.eligible
                            && matches!(
                                expr,
                                CoreExpr::Call { .. }
                                    | CoreExpr::ScalarFunction { .. }
                                    | CoreExpr::SpecialFunction { .. }
                                    | CoreExpr::AsyncFunction { .. }
                                    | CoreExpr::UdfCall { .. }
                            )),
                    ..eligibility
                }
            }
        };
        self.grouping_key_memo
            .eligibility
            .insert(id.index, eligibility);
        eligibility
    }

    /// The position of the scope whose pre-aggregate form is being bound.
    pub(super) fn grouping_key_input_scope(&self) -> Option<usize> {
        self.grouping_key.phase.input_scope()
    }

    /// Bind an input form with an explicit policy, restoring both dimensions
    /// even on error. The returned state records availability/deferred errors.
    fn with_grouping_key_input<R>(
        &mut self,
        scope: usize,
        bind: impl FnOnce(&mut Self) -> Result<R>,
    ) -> (Result<R>, GroupingKeyState) {
        // The arguments of `grouping()` stay so: an alias in them names the
        // pre-aggregate form of an item, which a matched node binds too.
        let in_grouping_arguments = self.grouping_key.in_grouping_arguments;
        let state = std::mem::replace(&mut self.grouping_key, GroupingKeyState {
            phase: GroupingKeyPhase::Input { scope },
            in_grouping_arguments,
            ..Default::default()
        });
        // Without a scope yet, as for GROUP BY, the forms are bound with the
        // policy of this type checker.
        let input_policy = context_chain(self.bind_context)
            .nth(scope)
            .and_then(|context| context.grouping_key_scope.as_ref())
            .map_or(self.folding_policy, |scope| scope.input_folding_policy);
        let policy = std::mem::replace(&mut self.folding_policy, input_policy);
        let result = bind(self);
        self.folding_policy = policy;
        (result, std::mem::replace(&mut self.grouping_key, state))
    }

    /// Stop matching the keys of this SELECT, e.g. inside the arguments of
    /// its aggregates, which are evaluated before the aggregate. Returns
    /// whether to resume.
    pub(super) fn suspend_grouping_key_scope(&mut self) -> bool {
        match &mut self.bind_context.grouping_key_scope {
            Some(scope) if scope.state == GroupingScopeState::Active => {
                scope.state = GroupingScopeState::Suspended;
                true
            }
            _ => false,
        }
    }

    pub(super) fn resume_grouping_key_scope(&mut self, suspended: bool) {
        if suspended && let Some(scope) = &mut self.bind_context.grouping_key_scope {
            scope.state = GroupingScopeState::Active;
        }
    }

    /// The positions in the context chain of the scopes whose keys are matched.
    fn matched_scopes(&self) -> Vec<usize> {
        let first = self
            .grouping_key_input_scope()
            .map_or(0, |position| position + 1);
        context_chain(self.bind_context)
            .enumerate()
            .skip(first)
            .filter(|(_, context)| active_scope(context))
            .map(|(position, _)| position)
            .collect()
    }

    /// Rewrite an aggregate function or `grouping()` bound after the aggregate
    /// of this SELECT to the output of the aggregate, registering the
    /// aggregate function. It is rewritten as bound, before its parents can
    /// fold away the reference (a zero-key aggregate still emits a row), so
    /// that the aggregate is complete once the expressions after it are
    /// bound; the binder only checks that no aggregate function is left, see
    /// `AggregateRewriter::rewrite_existing_expr`.
    pub(super) fn rewrite_grouped_output(&mut self, scalar: &mut ScalarExpr) -> Result<()> {
        if self.grouping_key_input_scope().is_some() || !active_scope(self.bind_context) {
            return Ok(());
        }
        match scalar {
            ScalarExpr::FunctionCall(func) if func.func_name.eq_ignore_ascii_case("grouping") => {
                *func = self.bind_context.aggregate_info.replace_grouping(func)?;
            }
            ScalarExpr::AggregateFunction(_) | ScalarExpr::UDAFCall(_) => {
                // The set-returning functions in the arguments are evaluated
                // before the aggregate.
                let suspended = self.suspend_grouping_key_scope();
                let srfs = SetReturningAnalyzer::new(self.bind_context, self.metadata.clone())
                    .visit(scalar);
                self.resume_grouping_key_scope(suspended);
                srfs?;
                AggregateRewriter::rewrite_expr(
                    &mut self.bind_context.aggregate_info,
                    self.metadata.clone(),
                    scalar,
                )?;
            }
            _ => {}
        }
        Ok(())
    }

    /// Whether the arguments of a `grouping()` call are being bound.
    pub(super) fn in_grouping_arguments(&self) -> bool {
        self.grouping_key.in_grouping_arguments
    }

    /// Bind the arguments of a `grouping()` call. An ungrouped column of this
    /// SELECT in them is reported as an argument that is not a key.
    ///
    /// The arguments are keys, so they are bound as in GROUP BY, whichever
    /// clause the call is in: a name is an input column first, and otherwise
    /// the explicit alias of a SELECT item, which stands for its
    /// pre-aggregate form.
    pub(super) fn resolve_grouping_arguments(
        &mut self,
        arena: &CoreExprArena<'_>,
        args: &[CoreExprId],
    ) -> Result<Vec<BoundScalar>> {
        let in_grouping_arguments =
            std::mem::replace(&mut self.grouping_key.in_grouping_arguments, true);
        let expr_context = self
            .bind_context
            .replace_expr_context(ExprContext::GroupClaue);
        let group_by_aliases = match &self.select_items {
            Some(items) => items.exprs().group_by_aliases(),
            None => AliasLookup::all(&[]),
        };
        let aliases = std::mem::replace(&mut self.aliases, group_by_aliases);
        let result = args
            .iter()
            .map(|arg| self.resolve_core_bound(arena, *arg))
            .collect();
        self.aliases = aliases;
        self.bind_context.expr_context = expr_context;
        self.grouping_key.in_grouping_arguments = in_grouping_arguments;
        result
    }

    /// The effects that binding a node speculatively must not have, see the
    /// module documentation. Binding internal and virtual columns is
    /// idempotent, so the symbols they allocate are not counted.
    #[cfg(debug_assertions)]
    pub(super) fn phase_effects(&self) -> PhaseEffects {
        let metadata = self.metadata.read();
        PhaseEffects {
            tables: metadata.tables().len(),
            symbols: metadata.columns().len()
                - self.bind_context.bound_internal_columns.len()
                - self.bind_context.bound_virtual_columns.len(),
            aggregate_calls: self.bind_context.aggregate_info.aggregate_calls().count(),
            set_returning_functions: self.bind_context.srf_info.srfs.len(),
        }
    }

    /// Whether nodes are matched to the keys of some grouping scope.
    pub(super) fn matches_grouping_keys(&self) -> bool {
        !self.matched_scopes().is_empty()
    }

    /// Resolve the node to the output of the grouping key it matches. The
    /// scopes are tried from the innermost.
    pub(super) fn try_resolve_grouping_key(
        &mut self,
        arena: &CoreExprArena<'_>,
        id: CoreExprId,
    ) -> Result<Option<BoundScalar>> {
        let scopes = self.matched_scopes();
        if scopes.is_empty() {
            return Ok(None);
        }
        let eligibility = self.grouping_key_eligibility(arena, id);
        let is_srf = if eligibility.is_candidate() {
            false
        } else if self.is_grouping_key_srf_candidate(arena, id) {
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
            // A deterministic form without columns is only matched to a key
            // of grouping sets of this SELECT, see the module documentation.
            let constant = input.facts.column_free && input.facts.deterministic;
            if constant
                && !eligibility.has_column
                && (position != 0 || context.aggregate_info.grouping_sets().is_none())
            {
                return Ok(None);
            }
            let input = input.scalar;
            let column = if is_srf {
                lookup_grouping_key_srf(context, &input)?
            } else {
                context.aggregate_info.lookup_grouping_key_output(&input)
            };
            if let Some(column) = column {
                let data_type = (*column.data_type).clone();
                let span = input.span();
                return Ok(Some(
                    self.bind_derived(BoundColumnRef { span, column }.into(), data_type),
                ));
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
    ) -> Result<Option<BoundScalar>> {
        #[cfg(debug_assertions)]
        let effects = self.phase_effects();
        let (result, input) =
            self.with_grouping_key_input(position, |checker| checker.resolve_core_bound(arena, id));
        let bound = result?;
        #[cfg(debug_assertions)]
        debug_assert_eq!(
            effects,
            self.phase_effects(),
            "matching a node to grouping keys must not have phase-sensitive effects"
        );
        // A form with a deferred error refers to an ungrouped input column of
        // an enclosing scope, which no key of this scope does.
        Ok((!input.input_unavailable && input.deferred_error.is_none()).then_some(bound))
    }

    /// Bind the pre-aggregate form of the node for the current scope, as the
    /// binding of the node: an error deferred while binding it is returned,
    /// and `None` if it refers to an alias whose item has no pre-aggregate
    /// form.
    pub(super) fn resolve_pre_aggregate_node(
        &mut self,
        arena: &CoreExprArena<'_>,
        id: CoreExprId,
    ) -> Result<Option<ScalarExpr>> {
        let (result, input) =
            self.with_grouping_key_input(0, |checker| checker.resolve_checked_core(arena, id));
        let deref!((scalar, _)) = result?;
        if let Some(err) = input.deferred_error {
            return Err(err);
        }
        Ok((!input.input_unavailable).then_some(scalar))
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
    ) -> Result<BoundScalar> {
        let key = (id.index, position);
        if let Some(form) = self.grouping_key_memo.forms.get(&key) {
            self.grouping_key.input_unavailable |= form.input_unavailable;
            if self.grouping_key.deferred_error.is_none() {
                self.grouping_key.deferred_error = form.deferred_error.clone();
            }
            return Ok(form.bound.clone());
        }
        let (result, node) = self.with_grouping_key_input(position, |checker| {
            checker.resolve_core_bound_node(arena, id)
        });
        self.grouping_key.input_unavailable |= node.input_unavailable;
        if self.grouping_key.deferred_error.is_none() {
            self.grouping_key.deferred_error = node.deferred_error.clone();
        }
        let resolved = result?;
        if self.is_grouping_key_candidate(arena, id) {
            self.grouping_key_memo.forms.insert(key, PreAggregateForm {
                bound: resolved.clone(),
                input_unavailable: node.input_unavailable,
                deferred_error: node.deferred_error,
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
    /// alias cannot consume an aggregate output, which would nest aggregates.
    pub(super) fn resolve_grouped_alias(
        &mut self,
        position: usize,
        scalar: ScalarExpr,
    ) -> Result<ScalarExpr> {
        let matching = self.grouping_key_input_scope().is_some();
        // Outside matching, the pre-aggregate form is only needed in the
        // arguments of an aggregate function.
        let suspended = matches!(
            self.bind_context.grouping_scope_state(),
            Some(GroupingScopeState::Suspended)
        );
        if !matching && !suspended {
            return Ok(scalar);
        }
        match self.pre_aggregate_form(position)? {
            Some(form) => Ok(form),
            None => {
                self.grouping_key.input_unavailable |= matching;
                if !matching
                    && scalar.used_columns().iter().any(|index| {
                        self.bind_context
                            .aggregate_info
                            .has_aggregate_call_index(*index)
                    })
                {
                    return Err(ErrorCode::SemanticError(
                        "aggregate function calls cannot be nested",
                    ));
                }
                Ok(scalar)
            }
        }
    }

    /// Reject an input column owned by an active grouped scope. Key outputs
    /// are allowed, and candidate input forms skip their own grouping scope.
    ///
    /// While binding a pre-aggregate form for a scope, only the scopes
    /// enclosing it are checked, and the error is deferred, see the module
    /// documentation.
    pub(super) fn check_grouping_column(
        &mut self,
        span: Span,
        column: &ColumnBinding,
    ) -> Result<()> {
        let first = self
            .grouping_key_input_scope()
            .map_or(0, |position| position + 1);
        let Some((position, owner)) =
            context_chain(self.bind_context)
                .enumerate()
                .find(|(_, context)| {
                    context
                        .columns
                        .iter()
                        .any(|input| input.index == column.index)
                })
        else {
            return Ok(());
        };
        if position < first
            || !active_scope(owner)
            || owner
                .grouping_key_scope
                .as_ref()
                .is_some_and(|scope| !scope.input_columns.contains(&column.index))
            || owner.aggregate_info.is_grouping_key_output(column.index)
        {
            return Ok(());
        }
        let err = if position == 0 && self.grouping_key.in_grouping_arguments {
            owner.aggregate_info.grouping_argument_error()
        } else {
            ErrorCode::SemanticError(format!(
                "column \"{}\" must appear in the GROUP BY clause or be used in an aggregate function",
                column.column_name
            ))
        }
        .set_span(span);
        if self.grouping_key_input_scope().is_none() {
            return Err(err);
        }
        self.grouping_key.deferred_error.get_or_insert(err);
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
    for srf in context.srf_info.srfs.iter().map(|srf| &srf.item) {
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

#[cfg(test)]
mod tests {
    use databend_common_ast::parser::Dialect;
    use databend_common_ast::parser::parse_expr;
    use databend_common_ast::parser::tokenize_sql;
    use databend_common_expression::FunctionContext;
    use databend_common_expression::types::DataType;
    use databend_common_expression::types::NumberDataType;
    use databend_common_meta_app::tenant::Tenant;
    use databend_common_settings::Settings;
    use parking_lot::RwLock;

    use super::*;
    use crate::Metadata;
    use crate::NameResolutionContext;
    use crate::Symbol;
    use crate::binder::GroupingKeyScope;
    use crate::binder::GroupingScopeState;
    use crate::binder::ScalarFoldingPolicy;
    use crate::planner::semantic::type_check::persisted::PersistedTypeCheckAdapter;

    fn with_checker(
        check: impl FnOnce(&mut TypeChecker<'_, PersistedTypeCheckAdapter>) -> Result<()>,
    ) -> Result<()> {
        let settings = Settings::create(Tenant::new_literal("default"));
        let adapter = PersistedTypeCheckAdapter::new(&settings, FunctionContext::default())?;
        let mut context = BindContext::new();
        context.add_column_binding(
            ColumnBindingBuilder::new(
                "number".into(),
                Symbol::new(0),
                Box::new(DataType::Number(NumberDataType::Int64)),
                Visibility::Visible,
            )
            .build(),
        );
        context.grouping_key_scope = Some(GroupingKeyScope {
            state: GroupingScopeState::Suspended,
            input_folding_policy: ScalarFoldingPolicy::PreserveStructure,
            input_columns: context.column_set(),
        });
        let names = NameResolutionContext::default();
        let mut checker = TypeChecker::try_create_with_adapter(
            &mut context,
            adapter,
            &names,
            RwLock::new(Metadata::default()).into(),
            &[],
        )?;
        check(&mut checker)
    }

    // Integration tests cannot inspect the private input memo or its facts.
    #[test]
    fn input_memo_preserves_facts_and_reported_type() -> Result<()> {
        with_checker(|checker| {
            let tokens = tokenize_sql("is_null(number)")?;
            let expr = parse_expr(&tokens, Dialect::PostgreSQL)?;
            let mut arena = checker.core_expr_arena();
            let root = arena.lower_ast_expr(&expr)?;
            let (result, _) = checker
                .with_grouping_key_input(0, |checker| checker.resolve_core_bound(&arena, root));
            let first = result?;
            // Preserving structure does not compute the call's domain. Deriving
            // facts again would infer the singleton false from the input type.
            assert!(first.facts.domain.is_none());
            assert!(checker.derive_facts(&first.scalar).domain.is_some());
            assert!(!first.facts.column_free);

            // A producer's declared type need not equal scalar.data_type().
            let reported_type = DataType::Boolean.wrap_nullable();
            let form = checker
                .grouping_key_memo
                .forms
                .get_mut(&(root.index, 0))
                .unwrap();
            form.bound.data_type = reported_type.clone();
            form.input_unavailable = true;
            form.deferred_error = Some(ErrorCode::SemanticError("deferred input error"));
            let (result, input) = checker
                .with_grouping_key_input(0, |checker| checker.resolve_core_bound(&arena, root));
            let cached = result?;
            assert!(input.input_unavailable);
            assert_eq!(
                input.deferred_error.unwrap().message(),
                "deferred input error"
            );
            assert_eq!(cached.scalar, first.scalar);
            assert_eq!(cached.data_type, reported_type);
            assert!(cached.facts.domain.is_none());
            assert_eq!(cached.facts.column_free, first.facts.column_free);
            assert_eq!(cached.facts.removable, first.facts.removable);
            assert_eq!(cached.facts.deterministic, first.facts.deterministic);
            assert_eq!(cached.facts.evaluable, first.facts.evaluable);
            Ok(())
        })
    }

    #[test]
    fn pair_adapter_and_bound_entry_agree() -> Result<()> {
        with_checker(|checker| {
            for policy in [
                ScalarFoldingPolicy::Normal,
                ScalarFoldingPolicy::PreserveStructure,
            ] {
                checker.folding_policy = policy;
                for sql in [
                    "number + (40 + 2)",
                    "CAST(is_null(number) AS INT)",
                    "coalesce(number, 40 + 2)",
                    "(number, 40 + 2)",
                    "number IN (1, 2)",
                ] {
                    let tokens = tokenize_sql(sql)?;
                    let expr = parse_expr(&tokens, Dialect::PostgreSQL)?;
                    let mut arena = checker.core_expr_arena();
                    let root = arena.lower_ast_expr(&expr)?;
                    let bound = checker.with_own_grouping_key_memo(|checker| {
                        checker.resolve_core_bound(&arena, root)
                    })?;
                    let pair = checker
                        .with_own_grouping_key_memo(|checker| checker.resolve_core(&arena, root))?;
                    assert_eq!(bound.scalar, pair.0, "{policy:?}: {sql}");
                    assert_eq!(bound.data_type, pair.1, "{policy:?}: {sql}");
                }
            }
            Ok(())
        })
    }

    #[test]
    fn input_phase_and_folding_policy_restore_after_nested_errors() -> Result<()> {
        with_checker(|checker| {
            let ordinary = BindContext {
                grouping_key_scope: Some(GroupingKeyScope {
                    state: GroupingScopeState::Active,
                    input_folding_policy: ScalarFoldingPolicy::Normal,
                    input_columns: Default::default(),
                }),
                ..BindContext::new()
            };
            let grouping_sets = BindContext {
                grouping_key_scope: Some(GroupingKeyScope {
                    state: GroupingScopeState::Active,
                    input_folding_policy: ScalarFoldingPolicy::PreserveStructure,
                    input_columns: Default::default(),
                }),
                parent: Some(Box::new(ordinary)),
                ..BindContext::new()
            };
            checker.bind_context.parent = Some(Box::new(grouping_sets));
            let (result, _) = checker.with_grouping_key_input(1, |checker| {
                assert_eq!(checker.grouping_key_input_scope(), Some(1));
                assert_eq!(
                    checker.scalar_folding_policy(),
                    ScalarFoldingPolicy::PreserveStructure
                );
                let (nested, _) = checker.with_grouping_key_input(2, |checker| {
                    assert_eq!(checker.grouping_key_input_scope(), Some(2));
                    assert_eq!(checker.scalar_folding_policy(), ScalarFoldingPolicy::Normal);
                    Err::<(), _>(ErrorCode::SemanticError("nested binding failed"))
                });
                assert!(nested.is_err());
                assert_eq!(checker.grouping_key_input_scope(), Some(1));
                assert_eq!(
                    checker.scalar_folding_policy(),
                    ScalarFoldingPolicy::PreserveStructure
                );
                nested
            });
            assert!(result.is_err());
            assert_eq!(checker.grouping_key_input_scope(), None);
            assert_eq!(checker.scalar_folding_policy(), ScalarFoldingPolicy::Normal);
            Ok(())
        })
    }
}
