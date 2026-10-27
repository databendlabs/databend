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

//! The SELECT list of a query with GROUPING SETS, before the aggregate.
//!
//! The SELECT list of such a query is bound after its keys are known, see
//! `normalize_grouping_sets_select_list`. Before that, GROUP BY and WHERE
//! still need the pre-aggregate forms of the SELECT items they refer to by
//! position or alias. An item is bound in its pre-aggregate form when a
//! position or name first resolves to it, so the items bound before the
//! aggregate are exactly those referred to before it, and whatever binding
//! them registers, e.g. the set-returning functions in them, is needed before
//! the aggregate. Every other item, in particular every subquery, is bound
//! once, after the aggregate.
//!
//! After the aggregate, an alias stands for the pre-aggregate form of its item
//! where the expression around it is evaluated before the aggregate: while a
//! node is matched to the keys, and in the arguments of an aggregate function.
//! The form of an item is then bound only if that registers nothing, i.e. if
//! the item is eligible as a key and the items it refers to have such forms
//! too; other items have no form after the aggregate.
//!
//! The list is split by access. [`PreAggregateList`] holds the names and the
//! expressions of the items and is shared by name lookups, while the forms
//! are written through [`PreAggregateItemsMut`], which the binding in progress
//! holds exclusively and lends to the bindings it starts.

use databend_common_ast::ast::Expr;
use databend_common_ast::ast::GroupBy;
use databend_common_ast::ast::Indirection;
use databend_common_ast::ast::SelectStmt;
use databend_common_ast::ast::SelectTarget;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;

use super::project::select_item_name;
use super::select::ClauseAliasBindings;
use super::select::SelectList;
use crate::planner::binder::AliasLookup;
use crate::planner::binder::AliasTable;
use crate::planner::binder::BindContext;
use crate::planner::binder::Binder;
use crate::planner::binder::ExprContext;
use crate::plans::ScalarExpr;

/// The SELECT items of a GROUPING SETS query and their pre-aggregate forms.
pub(crate) struct PreAggregateItems<'a> {
    list: PreAggregateList<'a>,
    forms: Vec<PreAggregateForm>,
}

/// The names and expressions of the SELECT items.
pub(crate) struct PreAggregateList<'a> {
    entries: Vec<PreAggregateEntry<'a>>,
    /// Positions of the expression items, whose aliases later items may use.
    expr_positions: Vec<usize>,
    /// Positions of the items with an explicit alias, which GROUP BY may use.
    aliased_positions: Vec<usize>,
}

pub(crate) struct PreAggregateEntry<'a> {
    pub(crate) alias: String,
    /// `None` for a column expanded from `*`, whose form is bound when the
    /// star is expanded.
    expr: Option<&'a Expr>,
}

#[allow(clippy::large_enum_variant)]
enum PreAggregateForm {
    Unbound,
    /// Being bound. An item only refers to the items before it, so this is
    /// never reached again.
    Binding,
    Bound(ScalarExpr),
    /// Has no form that can be bound after the aggregate.
    Unavailable,
}

/// Exclusive access to the pre-aggregate forms.
pub(crate) struct PreAggregateItemsMut<'a> {
    list: &'a PreAggregateList<'a>,
    forms: &'a mut [PreAggregateForm],
    /// Whether the aggregate is bound, so that only forms that register
    /// nothing may be bound.
    after_aggregate: bool,
}

impl<'a> PreAggregateItems<'a> {
    /// Access for GROUP BY and WHERE, before the aggregate.
    pub(crate) fn as_mut(&mut self) -> PreAggregateItemsMut<'_> {
        PreAggregateItemsMut {
            list: &self.list,
            forms: &mut self.forms,
            after_aggregate: false,
        }
    }

    /// Access for the expressions after the aggregate, whose alias tables
    /// list the SELECT items by position.
    pub(crate) fn after_aggregate(&mut self) -> PreAggregateItemsMut<'_> {
        PreAggregateItemsMut {
            list: &self.list,
            forms: &mut self.forms,
            after_aggregate: true,
        }
    }

    pub(crate) fn len(&self) -> usize {
        self.forms.len()
    }

    /// The pre-aggregate form of the item at `position` if it is bound.
    pub(crate) fn form(&self, position: usize) -> Option<&ScalarExpr> {
        match self.forms.get(position) {
            Some(PreAggregateForm::Bound(scalar)) => Some(scalar),
            _ => None,
        }
    }
}

impl PreAggregateList<'_> {
    pub(crate) fn len(&self) -> usize {
        self.entries.len()
    }

    pub(crate) fn alias(&self, position: usize) -> &str {
        &self.entries[position].alias
    }

    /// Whether the item is a column reference, the only kind of item whose
    /// form is a column.
    pub(crate) fn is_column_ref(&self, position: usize) -> bool {
        matches!(self.entries[position].expr, Some(Expr::ColumnRef { .. }))
    }

    /// The aliases and expressions of the expression items.
    pub(crate) fn alias_exprs(&self) -> impl Iterator<Item = (String, Expr)> + '_ {
        self.entries
            .iter()
            .filter_map(|entry| Some((entry.alias.clone(), entry.expr?.clone())))
    }

    /// The aliases of all items, as WHERE sees them.
    pub(crate) fn lookup(&self) -> AliasLookup<'_> {
        AliasLookup::new(AliasTable::PreAggregate(&self.entries), None)
    }

    /// The aliases that the item at `position` sees: those of the expression
    /// items before it.
    pub(crate) fn select_item_lookup(&self, position: usize) -> AliasLookup<'_> {
        let end = self.expr_positions.partition_point(|p| *p < position);
        AliasLookup::new(
            AliasTable::PreAggregate(&self.entries),
            Some(&self.expr_positions[..end]),
        )
    }

    pub(crate) fn group_by_bindings(&self) -> ClauseAliasBindings<'_> {
        ClauseAliasBindings::new(
            AliasTable::PreAggregate(&self.entries),
            self.aliased_positions.clone(),
        )
    }
}

impl<'a> PreAggregateItemsMut<'a> {
    pub(crate) fn list(&self) -> &'a PreAggregateList<'a> {
        self.list
    }

    pub(crate) fn reborrow(&mut self) -> PreAggregateItemsMut<'_> {
        PreAggregateItemsMut {
            list: self.list,
            forms: &mut *self.forms,
            after_aggregate: self.after_aggregate,
        }
    }

    pub(crate) fn is_after_aggregate(&self) -> bool {
        self.after_aggregate
    }

    pub(crate) fn form(&self, position: usize) -> Option<&ScalarExpr> {
        match &self.forms[position] {
            PreAggregateForm::Bound(scalar) => Some(scalar),
            _ => None,
        }
    }

    /// Whether the item is known to have no form after the aggregate.
    pub(crate) fn is_unavailable(&self, position: usize) -> bool {
        matches!(self.forms[position], PreAggregateForm::Unavailable)
    }

    /// Start binding the form of the item at `position`, which is unbound;
    /// returns its expression.
    pub(crate) fn start_binding(&mut self, position: usize) -> Result<&'a Expr> {
        let expr = match (&self.forms[position], self.list.entries[position].expr) {
            (PreAggregateForm::Unbound, Some(expr)) => expr,
            _ => {
                return Err(ErrorCode::Internal(format!(
                    "SELECT item {} is not an unbound expression",
                    position + 1
                )));
            }
        };
        self.forms[position] = PreAggregateForm::Binding;
        Ok(expr)
    }

    /// Finish binding the form of the item at `position`, leaving it unbound
    /// if binding failed.
    pub(crate) fn finish_binding(&mut self, position: usize, result: &Result<Option<ScalarExpr>>) {
        self.forms[position] = match result {
            Ok(Some(scalar)) => PreAggregateForm::Bound(scalar.clone()),
            Ok(None) => PreAggregateForm::Unavailable,
            Err(_) => PreAggregateForm::Unbound,
        };
    }
}

impl Binder {
    /// Collect the SELECT items of a GROUPING SETS query. Stars are expanded,
    /// which binds the columns; the other items are bound on reference.
    pub(crate) fn bind_pre_aggregate_select_list<'a>(
        &mut self,
        input_context: &mut BindContext,
        select_list: &'a [SelectTarget],
    ) -> Result<PreAggregateItems<'a>> {
        input_context.expr_context = ExprContext::SelectClause;

        let mut list = PreAggregateList {
            entries: Vec::with_capacity(select_list.len()),
            expr_positions: Vec::new(),
            aliased_positions: Vec::new(),
        };
        let mut forms = Vec::with_capacity(select_list.len());
        for target in select_list {
            match target {
                SelectTarget::StarColumns {
                    qualified: names,
                    column_filter,
                } => {
                    if names.len() > 3 || names.is_empty() {
                        return Err(ErrorCode::SemanticError("Unsupported indirection type"));
                    }
                    let span = match names.last() {
                        Some(Indirection::Star(span)) => *span,
                        _ => None,
                    };
                    let mut star = SelectList::default();
                    self.resolve_star_columns(
                        span,
                        input_context,
                        target,
                        names.as_slice(),
                        column_filter,
                        &mut star,
                    )?;
                    for item in star.items {
                        list.entries.push(PreAggregateEntry {
                            alias: item.alias,
                            expr: None,
                        });
                        forms.push(PreAggregateForm::Bound(item.scalar));
                    }
                }
                SelectTarget::AliasedExpr { expr, alias } => {
                    let position = list.entries.len();
                    list.expr_positions.push(position);
                    if alias.is_some() {
                        list.aliased_positions.push(position);
                    }
                    list.entries.push(PreAggregateEntry {
                        alias: select_item_name(expr, alias, &self.name_resolution_ctx),
                        expr: Some(expr),
                    });
                    forms.push(PreAggregateForm::Unbound);
                }
            }
        }
        Ok(PreAggregateItems { list, forms })
    }
}

/// Whether the GROUP BY of `stmt` is GROUPING SETS, ROLLUP, CUBE or a
/// combination, which bind the SELECT list after the keys are known.
pub(crate) fn has_grouping_sets(stmt: &SelectStmt) -> bool {
    matches!(
        stmt.group_by,
        Some(
            GroupBy::GroupingSets(_) | GroupBy::Cube(_) | GroupBy::Rollup(_) | GroupBy::Combined(_)
        )
    )
}
