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

//! The pre-aggregate SELECT list of a query with GROUPING SETS.
//!
//! The SELECT list of such a query is bound after its keys are known, see
//! `normalize_grouping_sets_select_list`. Before that, GROUP BY and WHERE
//! still need the pre-aggregate forms of the SELECT items they refer to by
//! position or alias, and key matching needs the pre-aggregate forms of the
//! aliases that post-aggregate expressions refer to. Only those items are
//! bound before the aggregate, so that every other item, in particular every
//! subquery, is bound exactly once.

use std::collections::HashSet;

use databend_common_ast::ast::ColumnID;
use databend_common_ast::ast::ColumnRef;
use databend_common_ast::ast::Expr;
use databend_common_ast::ast::GroupBy;
use databend_common_ast::ast::Indirection;
use databend_common_ast::ast::Literal;
use databend_common_ast::ast::OrderByExpr;
use databend_common_ast::ast::Query;
use databend_common_ast::ast::SelectStmt;
use databend_common_ast::ast::SelectTarget;
use databend_common_ast::visit::VisitControl;
use databend_common_ast::visit::Visitor;
use databend_common_ast::visit::Walk;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::Scalar;

use super::aggregate_prepass::collect_function_names;
use super::project::select_item_name;
use super::select::SelectItem;
use super::select::SelectList;
use crate::planner::binder::BindContext;
use crate::planner::binder::Binder;
use crate::planner::binder::ExprContext;
use crate::planner::binder::scalar::ScalarBinder;
use crate::plans::ConstantExpr;

/// The SELECT list bound before the aggregate. Items that nothing refers to
/// before the aggregate are left unbound: they keep their names, so that the
/// positions and alias names of the list are those of the full list, but no
/// name lookup reaches them, see [`PreAggregateDemand`].
pub(crate) struct PreAggregateSelectList<'a> {
    pub(crate) list: SelectList<'a>,
    pub(crate) bound: Vec<bool>,
}

impl PreAggregateSelectList<'_> {
    /// The item at `position` if it is bound.
    pub(crate) fn item(&self, position: usize) -> Option<&SelectItem<'_>> {
        self.bound
            .get(position)
            .copied()
            .unwrap_or(false)
            .then(|| &self.list.items[position])
    }
}

/// Names and positions that a query may resolve to SELECT items. Collected
/// from the AST, so that a superset of the real references is found: every
/// unsigned literal counts as a position and every column name as an alias.
#[derive(Default)]
struct References {
    /// Lowercase column names, qualified or not.
    names: HashSet<String>,
    positions: HashSet<u64>,
    has_subquery: bool,
}

impl References {
    fn of(expr: &Expr) -> Self {
        let mut references = Self::default();
        let Ok(_) = expr.walk(&mut references);
        references
    }

    fn contains_name(&self, name: &str) -> bool {
        self.names.contains(&name.to_lowercase())
    }
}

impl Visitor for References {
    fn visit_expr(&mut self, expr: &Expr) -> std::result::Result<VisitControl, !> {
        match expr {
            Expr::ColumnRef {
                column:
                    ColumnRef {
                        column: ColumnID::Name(name),
                        ..
                    },
                ..
            } => {
                self.names.insert(name.name.to_lowercase());
            }
            Expr::Literal {
                value: Literal::UInt64(position),
                ..
            } => {
                self.positions.insert(*position);
            }
            _ => {}
        }
        Ok(VisitControl::Continue)
    }

    fn visit_query(&mut self, _query: &Query) -> std::result::Result<VisitControl, !> {
        self.has_subquery = true;
        Ok(VisitControl::Continue)
    }
}

/// Which SELECT items need a pre-aggregate form.
pub(crate) struct PreAggregateDemand {
    /// References from GROUP BY and WHERE, which are bound before the
    /// aggregate and resolve aliases to pre-aggregate forms.
    pre_aggregate: References,
    /// References from the SELECT list, HAVING, QUALIFY, WINDOW and ORDER BY.
    /// While matching keys, an alias in them stands for its pre-aggregate form.
    post_aggregate: References,
}

impl PreAggregateDemand {
    pub(crate) fn new(stmt: &SelectStmt, order_by: &[OrderByExpr]) -> Self {
        let mut pre_aggregate = References::default();
        if let Some(group_by) = &stmt.group_by {
            let Ok(_) = group_by.walk(&mut pre_aggregate);
        }
        if let Some(selection) = &stmt.selection {
            let Ok(_) = selection.walk(&mut pre_aggregate);
        }

        let mut post_aggregate = References::default();
        for target in &stmt.select_list {
            if let SelectTarget::AliasedExpr { expr, .. } = target {
                let Ok(_) = expr.walk(&mut post_aggregate);
            }
        }
        for expr in stmt.having.iter().chain(&stmt.qualify) {
            let Ok(_) = expr.walk(&mut post_aggregate);
        }
        for window in stmt.window_list.iter().flatten() {
            let Ok(_) = window.walk(&mut post_aggregate);
        }
        for item in order_by {
            let Ok(_) = item.walk(&mut post_aggregate);
        }

        Self {
            pre_aggregate,
            post_aggregate,
        }
    }

    /// Whether the item at 0-based `position` named `name` is needed before
    /// the aggregate on its own account.
    ///
    /// GROUP BY and WHERE need every item they may refer to. Key matching only
    /// needs the items whose pre-aggregate form may be deterministic: one
    /// with a subquery never is, so it is not bound before the aggregate and
    /// an alias of it matches no key.
    fn demands(&self, position: usize, name: &str, item: &References) -> bool {
        self.pre_aggregate
            .positions
            .contains(&(position as u64 + 1))
            || self.pre_aggregate.contains_name(name)
            || (!item.has_subquery && self.post_aggregate.contains_name(name))
    }
}

enum Slot<'a> {
    Star(Vec<SelectItem<'a>>),
    Expr {
        target: &'a SelectTarget,
        expr: &'a Expr,
        name: String,
        references: References,
        demanded: bool,
    },
}

impl Binder {
    /// Bind the pre-aggregate forms of the SELECT items that `demand` needs.
    ///
    /// Stars are expanded without binding expressions and are always kept. An
    /// item is also bound if a later bound item may refer to its alias, since
    /// SELECT items resolve the aliases of the items before them.
    pub(crate) fn normalize_pre_aggregate_select_list<'a>(
        &mut self,
        input_context: &mut BindContext,
        select_list: &'a [SelectTarget],
        demand: &PreAggregateDemand,
    ) -> Result<PreAggregateSelectList<'a>> {
        input_context.expr_context = ExprContext::SelectClause;

        let mut slots = Vec::with_capacity(select_list.len());
        let mut position = 0;
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
                    position += star.items.len();
                    slots.push(Slot::Star(star.items));
                }
                SelectTarget::AliasedExpr { expr, alias } => {
                    let name = select_item_name(expr, alias, &self.name_resolution_ctx);
                    let references = References::of(expr);
                    let demanded = demand.demands(position, &name, &references);
                    position += 1;
                    slots.push(Slot::Expr {
                        target,
                        expr,
                        name,
                        references,
                        demanded,
                    });
                }
            }
        }

        // Aliases only refer backwards, so one backward pass propagates the
        // demand of an item to the items whose aliases it may use.
        for i in (0..slots.len()).rev() {
            let (earlier, rest) = slots.split_at_mut(i);
            let Slot::Expr {
                demanded: true,
                references,
                ..
            } = &rest[0]
            else {
                continue;
            };
            for slot in earlier {
                if let Slot::Expr { name, demanded, .. } = slot
                    && references.contains_name(name)
                {
                    *demanded = true;
                }
            }
        }

        let mut list = SelectList::default();
        let mut bound = Vec::with_capacity(position);
        let mut prev_aliases = Vec::new();
        for slot in slots {
            match slot {
                Slot::Star(items) => {
                    bound.extend(std::iter::repeat_n(true, items.len()));
                    list.items.extend(items);
                }
                Slot::Expr {
                    target,
                    expr,
                    name,
                    demanded,
                    ..
                } => {
                    let scalar = if demanded {
                        let mut scalar_binder = ScalarBinder::new(
                            input_context,
                            self.ctx.clone(),
                            &self.name_resolution_ctx,
                            self.metadata.clone(),
                            &prev_aliases,
                        );
                        let scalar = scalar_binder.bind(expr)?.0;
                        prev_aliases.push((name.clone(), scalar.clone()));
                        scalar
                    } else {
                        // Never read: no reference reaches an unbound item.
                        ConstantExpr {
                            span: expr.span(),
                            value: Scalar::Null,
                        }
                        .into()
                    };
                    list.items.push(SelectItem {
                        select_target: target,
                        scalar,
                        alias: name,
                        used_functions: collect_function_names(&self.name_resolution_ctx, expr),
                    });
                    bound.push(demanded);
                }
            }
        }
        Ok(PreAggregateSelectList { list, bound })
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
