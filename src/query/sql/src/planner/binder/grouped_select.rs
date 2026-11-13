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

//! Shared SELECT item forms for ordinary GROUP BY and grouping sets.
//!
//! The SELECT list of such a query is bound after its keys are known, see
//! `normalize_grouped_select_list`, yet GROUP BY and WHERE, which are
//! evaluated before the aggregate, refer to the pre-aggregate forms of the
//! items by position or alias, and so do the expressions after the aggregate
//! wherever the expression around an alias is evaluated before the aggregate,
//! see `type_check/grouping_key.rs`. The item expressions are therefore
//! lowered once, into an arena that every clause of the query shares, see
//! [`SelectItemExprs`], and the pre-aggregate forms of the items are bound
//! once for every clause, into [`SelectItemForms`], see
//! `type_check/pre_aggregate.rs`. The expressions are shared, while the forms
//! are owned by the binding of the query and lent to one clause at a time,
//! together with the expressions, as [`SelectItems`].
//!
//! An item is bound in its pre-aggregate form when a position or name first
//! resolves to it. Before the aggregate, the items bound so are exactly those
//! that GROUP BY and WHERE refer to, and whatever binding them registers, e.g.
//! the set-returning functions in them, is needed before the aggregate. After
//! the aggregate, the form of an item is only bound if that registers
//! nothing, i.e. if the item is eligible as a key and the items it refers to
//! have such forms too; other items have no form after the aggregate. Every
//! item, in particular every subquery, is bound once after the aggregate.

use std::collections::HashMap;
use std::collections::HashSet;

use databend_common_ast::ast::ColumnID;
use databend_common_ast::ast::ColumnRef;
use databend_common_ast::ast::Expr;
use databend_common_ast::ast::Indirection;
use databend_common_ast::ast::Literal;
use databend_common_ast::ast::SelectTarget;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;

use super::project::select_item_name;
use super::select::ClauseAliasBindings;
use super::select::SelectList;
use crate::NameResolutionContext;
use crate::planner::binder::AliasLookup;
use crate::planner::binder::AliasTable;
use crate::planner::binder::BindContext;
use crate::planner::binder::Binder;
use crate::planner::binder::ExprContext;
use crate::planner::binder::ScalarFoldingPolicy;
use crate::planner::semantic::CoreExprArena;
use crate::planner::semantic::CoreExprId;
use crate::planner::semantic::FullTypeCheckAdapter;
use crate::planner::semantic::UdafNames;
use crate::planner::semantic::normalize_identifier;
use crate::plans::ScalarExpr;

/// The SELECT items of a grouped query, with their expressions lowered
/// into the arena shared by the clauses of the query.
pub(crate) struct SelectItemExprs<'a> {
    arena: CoreExprArena<'a>,
    items: Vec<SelectItemExpr<'a>>,
    /// Positions of the expression items, whose aliases later items may use.
    expr_positions: Vec<usize>,
    /// Positions of the items with an explicit alias, which GROUP BY may use.
    aliased_positions: Vec<usize>,
}

pub(crate) struct SelectItemExpr<'a> {
    pub(crate) alias: String,
    pub(crate) source: SelectItemSource<'a>,
    /// The position of the SELECT target the item is, or is expanded from.
    pub(crate) target: usize,
}

#[allow(clippy::large_enum_variant)]
pub(crate) enum SelectItemSource<'a> {
    /// A column expanded from `*`, bound when the star is expanded.
    Star(ScalarExpr),
    /// An expression, lowered to the node `root` of the arena.
    Expr { expr: &'a Expr, root: CoreExprId },
}

impl<'a> SelectItemExprs<'a> {
    /// The positions that GROUP BY ALL stands for: those of the items that
    /// contain no aggregate function and refer to no earlier item that does.
    /// A name refers to an item unless an input column has it.
    pub(crate) fn group_by_all(
        &self,
        names: &NameResolutionContext,
        udafs: &HashSet<String>,
        inputs: &BindContext,
    ) -> Vec<Expr> {
        let udafs = UdafNames {
            names: udafs,
            name_resolution_ctx: names,
        };
        let mut aliases = HashMap::<&str, bool>::new();
        let mut keys = Vec::new();
        for (position, item) in self.items.iter().enumerate() {
            let aggregate = match item.source {
                SelectItemSource::Star(_) => false,
                SelectItemSource::Expr { root, .. } => {
                    self.arena.contains_aggregate(root, &udafs)
                        || self.arena.column_refs(root).into_iter().any(|column| {
                            let ColumnRef {
                                database: None,
                                table: None,
                                column: ColumnID::Name(ident),
                            } = column
                            else {
                                return false;
                            };
                            let name = normalize_identifier(ident, names).name;
                            aliases.get(name.as_str()).copied().unwrap_or(false)
                                && !inputs
                                    .columns
                                    .iter()
                                    .any(|column| column.column_name.eq_ignore_ascii_case(&name))
                        })
                }
            };
            aliases.insert(&item.alias, aggregate);
            if !aggregate {
                keys.push(Expr::Literal {
                    span: None,
                    value: Literal::UInt64(position as u64 + 1),
                });
            }
        }
        keys
    }

    pub(crate) fn arena(&self) -> &CoreExprArena<'a> {
        &self.arena
    }

    pub(crate) fn len(&self) -> usize {
        self.items.len()
    }

    pub(crate) fn item(&self, position: usize) -> &SelectItemExpr<'a> {
        &self.items[position]
    }

    pub(crate) fn alias(&self, position: usize) -> &str {
        &self.items[position].alias
    }

    /// Whether the item is a column reference, the only kind of expression
    /// item whose form is a column.
    pub(crate) fn is_column_ref(&self, position: usize) -> bool {
        matches!(self.items[position].source, SelectItemSource::Expr {
            expr: Expr::ColumnRef { .. },
            ..
        })
    }

    /// The aliases and expressions of the expression items.
    pub(crate) fn alias_exprs(&self) -> impl Iterator<Item = (String, Expr)> + '_ {
        self.items.iter().filter_map(|item| match item.source {
            SelectItemSource::Expr { expr, .. } => Some((item.alias.clone(), expr.clone())),
            SelectItemSource::Star(_) => None,
        })
    }

    /// The aliases of all items, as WHERE sees them.
    pub(crate) fn lookup(&self) -> AliasLookup<'_> {
        AliasLookup::new(AliasTable::PreAggregate(&self.items), None)
    }

    /// The aliases that the item at `position` sees: those of the expression
    /// items before it.
    pub(crate) fn select_item_lookup(&self, position: usize) -> AliasLookup<'_> {
        let end = self.expr_positions.partition_point(|p| *p < position);
        AliasLookup::new(
            AliasTable::PreAggregate(&self.items),
            Some(&self.expr_positions[..end]),
        )
    }

    pub(crate) fn group_by_bindings(&self) -> ClauseAliasBindings<'_> {
        ClauseAliasBindings::new(
            AliasTable::PreAggregate(&self.items),
            self.aliased_positions.clone(),
        )
    }
}

/// The state of the pre-aggregate form of each SELECT item.
pub(crate) struct SelectItemForms {
    forms: Vec<ItemForm>,
    /// The folding policy of the grouping keys. Every pre-aggregate form is
    /// bound with it, whichever clause binds it first, so that a form equals
    /// the key it is written as.
    folding_policy: ScalarFoldingPolicy,
}

/// The state of the pre-aggregate form of a SELECT item.
#[allow(clippy::large_enum_variant)]
#[derive(Clone)]
pub(crate) enum ItemForm {
    Unbound,
    /// Being bound. An item only refers to the items before it, so this is
    /// never reached again.
    Binding,
    Bound(ScalarExpr),
    /// The item has no pre-aggregate form after the aggregate.
    Unavailable,
    /// Binding the form failed with this error, deferred while binding it.
    Failed(ErrorCode),
}

impl SelectItemForms {
    pub(crate) fn new(exprs: &SelectItemExprs<'_>, folding_policy: ScalarFoldingPolicy) -> Self {
        Self {
            forms: vec![ItemForm::Unbound; exprs.len()],
            folding_policy,
        }
    }
}

/// The SELECT items of a grouped query and their pre-aggregate forms,
/// owned by the binding of the query.
pub(crate) struct GroupedSelectItems<'a> {
    pub(crate) exprs: SelectItemExprs<'a>,
    pub(crate) forms: SelectItemForms,
}

impl<'a> GroupedSelectItems<'a> {
    pub(crate) fn new(exprs: SelectItemExprs<'a>, folding_policy: ScalarFoldingPolicy) -> Self {
        let forms = SelectItemForms::new(&exprs, folding_policy);
        Self { exprs, forms }
    }

    /// Lend the items to the binding of a clause.
    pub(crate) fn lend(&mut self) -> SelectItems<'_> {
        SelectItems::new(&self.exprs, &mut self.forms)
    }
}

/// The SELECT items with exclusive access to their pre-aggregate forms.
pub(crate) struct SelectItems<'s> {
    exprs: &'s SelectItemExprs<'s>,
    forms: &'s mut SelectItemForms,
}

impl<'s> SelectItems<'s> {
    pub(crate) fn new(exprs: &'s SelectItemExprs<'s>, forms: &'s mut SelectItemForms) -> Self {
        Self { exprs, forms }
    }

    pub(crate) fn reborrow(&mut self) -> SelectItems<'_> {
        SelectItems {
            exprs: self.exprs,
            forms: &mut *self.forms,
        }
    }

    pub(crate) fn exprs(&self) -> &'s SelectItemExprs<'s> {
        self.exprs
    }

    /// The folding policy of the grouping keys and the pre-aggregate forms.
    pub(crate) fn folding_policy(&self) -> ScalarFoldingPolicy {
        self.forms.folding_policy
    }

    pub(crate) fn form(&self, position: usize) -> &ItemForm {
        &self.forms.forms[position]
    }

    pub(crate) fn set_form(&mut self, position: usize, form: ItemForm) {
        self.forms.forms[position] = form;
    }

    /// The pre-aggregate form of the item at `position` if it is bound.
    pub(crate) fn bound_form(&self, position: usize) -> Option<ScalarExpr> {
        match (&self.exprs.items[position].source, self.form(position)) {
            (SelectItemSource::Star(scalar), _) => Some(scalar.clone()),
            (SelectItemSource::Expr { .. }, ItemForm::Bound(scalar)) => Some(scalar.clone()),
            (SelectItemSource::Expr { .. }, _) => None,
        }
    }
}

impl Binder {
    /// Collect the SELECT items of a grouped query. Stars are expanded,
    /// which binds the columns; the other items are lowered, and bound on
    /// reference.
    pub(crate) fn lower_grouped_select_list<'a>(
        &mut self,
        input_context: &mut BindContext,
        select_list: &'a [SelectTarget],
    ) -> Result<SelectItemExprs<'a>> {
        input_context.expr_context = ExprContext::SelectClause;

        let mut exprs = SelectItemExprs {
            arena: CoreExprArena::for_adapter(&FullTypeCheckAdapter::new(self.ctx.clone())?)?,
            items: Vec::with_capacity(select_list.len()),
            expr_positions: Vec::new(),
            aliased_positions: Vec::new(),
        };
        for (target_position, target) in select_list.iter().enumerate() {
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
                        exprs.items.push(SelectItemExpr {
                            alias: item.alias,
                            source: SelectItemSource::Star(item.scalar),
                            target: target_position,
                        });
                    }
                }
                SelectTarget::AliasedExpr { expr, alias } => {
                    let position = exprs.items.len();
                    exprs.expr_positions.push(position);
                    if alias.is_some() {
                        exprs.aliased_positions.push(position);
                    }
                    let root = exprs.arena.lower_ast_expr(expr)?;
                    exprs.items.push(SelectItemExpr {
                        alias: select_item_name(expr, alias, &self.name_resolution_ctx),
                        source: SelectItemSource::Expr { expr, root },
                        target: target_position,
                    });
                }
            }
        }
        Ok(exprs)
    }
}
