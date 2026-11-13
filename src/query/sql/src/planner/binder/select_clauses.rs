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

//! The clauses of a SELECT after FROM, as written and lowered.
//!
//! The clauses are lowered once, into one arena that every clause is bound
//! from, see [`SelectClauseExprs`]. The SELECT items are collected with them:
//! stars are expanded, and the alias and the target of every item are known
//! before any clause is bound. The aggregates that decisions about the SELECT
//! depend on are found in the lowered clauses before they are bound, see
//! [`Binder::analyze_select_aggregates`] and `type_check/aggregate_facts.rs`.

use std::collections::BTreeSet;
use std::collections::HashMap;
use std::collections::HashSet;

use databend_common_ast::ast::ColumnID;
use databend_common_ast::ast::ColumnRef;
use databend_common_ast::ast::Expr;
use databend_common_ast::ast::GroupBy;
use databend_common_ast::ast::Indirection;
use databend_common_ast::ast::OrderByExpr;
use databend_common_ast::ast::SelectTarget;
use databend_common_base::runtime::block_on;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_meta_app::principal::UserDefinedFunction;
use databend_common_users::UserApiProvider;

use super::aggregate::LoweredGroupBy;
use super::project::select_item_name;
use super::select::SelectList;
use crate::NameResolutionContext;
use crate::planner::binder::AliasLookup;
use crate::planner::binder::AliasTable;
use crate::planner::binder::BindContext;
use crate::planner::binder::Binder;
use crate::planner::semantic::CoreExprArena;
use crate::planner::semantic::CoreExprId;
use crate::planner::semantic::FullTypeCheckAdapter;
use crate::planner::semantic::UdafNames;
use crate::planner::semantic::normalize_identifier;
use crate::planner::semantic::sql_udf_aggregates;
use crate::planner::semantic::sql_udf_body;
use crate::plans::ScalarExpr;

/// The clauses of a SELECT after FROM, lowered into one arena, with the
/// SELECT items collected. Each expression is lowered once: the aggregates
/// are found in, and every clause is bound from, these nodes.
pub(crate) struct SelectClauseExprs<'a> {
    arena: CoreExprArena<'a>,
    select_list: &'a [SelectTarget],
    items: Vec<SelectItemExpr<'a>>,
    /// Positions of the expression items, whose aliases later items may use.
    expr_positions: Vec<usize>,
    /// Positions of the items with an explicit alias, which GROUP BY may use.
    aliased_positions: Vec<usize>,
    selection: Option<CoreExprId>,
    group_by: Option<LoweredGroupBy>,
    having: Option<CoreExprId>,
    qualify: Option<CoreExprId>,
    /// The node of each ORDER BY expression.
    order_by: Vec<CoreExprId>,
}

/// The clauses of a SELECT after FROM, as written.
#[derive(Clone, Copy, Default)]
pub(crate) struct SelectClauses<'a> {
    pub(crate) select_list: &'a [SelectTarget],
    pub(crate) selection: Option<&'a Expr>,
    pub(crate) group_by: Option<&'a GroupBy>,
    pub(crate) having: Option<&'a Expr>,
    pub(crate) qualify: Option<&'a Expr>,
    pub(crate) order_by: &'a [OrderByExpr],
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

impl<'a> SelectClauseExprs<'a> {
    /// The positions that GROUP BY ALL stands for: those of the items that
    /// contain no aggregate function and refer to no earlier item that does.
    /// A name refers to an item unless an input column has it.
    pub(crate) fn group_by_all(
        &self,
        names: &NameResolutionContext,
        udafs: &HashSet<String>,
        inputs: &BindContext,
    ) -> Vec<usize> {
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
                keys.push(position);
            }
        }
        keys
    }

    pub(crate) fn arena(&self) -> &CoreExprArena<'a> {
        &self.arena
    }

    /// The expression roots of the SELECT items.
    pub(crate) fn item_roots(&self) -> impl Iterator<Item = CoreExprId> + '_ {
        self.items.iter().filter_map(|item| match item.source {
            SelectItemSource::Expr { root, .. } => Some(root),
            SelectItemSource::Star(_) => None,
        })
    }

    /// The SELECT target that the item at `position` is, or is expanded from.
    pub(crate) fn target(&self, position: usize) -> &'a SelectTarget {
        &self.select_list[self.items[position].target]
    }

    /// The aliases of the expression items and their nodes.
    pub(crate) fn alias_roots(&self) -> impl Iterator<Item = (String, CoreExprId)> + '_ {
        self.items.iter().filter_map(|item| match item.source {
            SelectItemSource::Expr { root, .. } => Some((item.alias.clone(), root)),
            SelectItemSource::Star(_) => None,
        })
    }

    pub(crate) fn selection(&self) -> Option<CoreExprId> {
        self.selection
    }

    pub(crate) fn group_by(&self) -> Option<&LoweredGroupBy> {
        self.group_by.as_ref()
    }

    pub(crate) fn having(&self) -> Option<CoreExprId> {
        self.having
    }

    pub(crate) fn qualify(&self) -> Option<CoreExprId> {
        self.qualify
    }

    /// The node of the ORDER BY expression at `position`.
    pub(crate) fn order_by(&self, position: usize) -> CoreExprId {
        self.order_by[position]
    }

    /// The nodes of the ORDER BY expressions.
    pub(crate) fn order_by_roots(&self) -> &[CoreExprId] {
        &self.order_by
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

    /// The aliases that GROUP BY sees: those of the items with an explicit
    /// alias. A name in GROUP BY is an input column first, see
    /// `ExprContext::prefer_resolve_alias`.
    pub(crate) fn group_by_aliases(&self) -> AliasLookup<'_> {
        AliasLookup::new(
            AliasTable::PreAggregate(&self.items),
            Some(&self.aliased_positions),
        )
    }
}

impl Binder {
    /// Lower the clauses of a SELECT after FROM and collect its items. Stars
    /// are expanded, which binds the columns; nothing else is bound.
    pub(crate) fn lower_select_clauses<'a>(
        &self,
        input_context: &BindContext,
        written: SelectClauses<'a>,
    ) -> Result<SelectClauseExprs<'a>> {
        let SelectClauses {
            select_list,
            selection,
            group_by,
            having,
            qualify,
            order_by,
        } = written;
        let mut clauses = SelectClauseExprs {
            arena: CoreExprArena::for_adapter(&FullTypeCheckAdapter::new(self.ctx.clone())?)?,
            select_list,
            items: Vec::with_capacity(select_list.len()),
            expr_positions: Vec::new(),
            aliased_positions: Vec::new(),
            selection: None,
            group_by: None,
            having: None,
            qualify: None,
            order_by: Vec::with_capacity(order_by.len()),
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
                        clauses.items.push(SelectItemExpr {
                            alias: item.alias,
                            source: SelectItemSource::Star(item.scalar),
                            target: target_position,
                        });
                    }
                }
                SelectTarget::AliasedExpr { expr, alias } => {
                    let position = clauses.items.len();
                    clauses.expr_positions.push(position);
                    if alias.is_some() {
                        clauses.aliased_positions.push(position);
                    }
                    let root = clauses.arena.lower_ast_expr(expr)?;
                    clauses.items.push(SelectItemExpr {
                        alias: select_item_name(expr, alias, &self.name_resolution_ctx),
                        source: SelectItemSource::Expr { expr, root },
                        target: target_position,
                    });
                }
            }
        }
        clauses.selection = selection
            .map(|expr| clauses.arena.lower_ast_expr(expr))
            .transpose()?;
        clauses.group_by = group_by
            .map(|group_by| {
                LoweredGroupBy::lower(&mut clauses.arena, group_by, clauses.items.len())
            })
            .transpose()?;
        clauses.having = having
            .map(|expr| clauses.arena.lower_ast_expr(expr))
            .transpose()?;
        clauses.qualify = qualify
            .map(|expr| clauses.arena.lower_ast_expr(expr))
            .transpose()?;
        for order in order_by {
            let root = clauses.arena.lower_ast_expr(&order.expr)?;
            clauses.order_by.push(root);
        }
        Ok(clauses)
    }
}

/// The aggregates of the clauses of a SELECT after FROM.
pub(crate) struct SelectAggregates {
    /// The user-defined functions they call that aggregate: UDAFs, and SQL
    /// UDFs whose body calls an aggregate function.
    pub(crate) udaf_names: HashSet<String>,
    /// Whether the SELECT list, HAVING or ORDER BY calls an aggregate
    /// function, which makes the SELECT grouped even without GROUP BY.
    pub(crate) contains_aggregate: bool,
}

impl Binder {
    /// Load the user-defined aggregate functions that the clauses call and
    /// find whether they aggregate. Nothing is bound.
    pub(crate) fn analyze_select_aggregates(
        &self,
        bind_context: &BindContext,
        clauses: &SelectClauseExprs<'_>,
    ) -> Result<SelectAggregates> {
        let arena = clauses.arena();
        let roots = || {
            clauses
                .item_roots()
                .chain(clauses.having())
                .chain(clauses.qualify())
                .chain(clauses.order_by_roots().iter().copied())
        };
        let function_names = roots()
            .flat_map(|root| arena.udf_call_names(root))
            .map(|name| normalize_identifier(name, &self.name_resolution_ctx).name)
            .collect();
        let udaf_names = self.resolve_udaf_names(bind_context, function_names)?;
        let udafs = UdafNames {
            names: &udaf_names,
            name_resolution_ctx: &self.name_resolution_ctx,
        };

        if clauses
            .qualify()
            .is_some_and(|root| arena.contains_aggregate_outside_windows(root, &udafs))
        {
            return Err(ErrorCode::SemanticError(
                "Qualify clause must not contain aggregate functions",
            ));
        }
        let contains_aggregate = clauses
            .item_roots()
            .chain(clauses.having())
            .chain(clauses.order_by_roots().iter().copied())
            .any(|root| arena.contains_grouping_aggregate(root, &udafs));
        Ok(SelectAggregates {
            udaf_names,
            contains_aggregate,
        })
    }

    fn resolve_udaf_names(
        &self,
        bind_context: &BindContext,
        function_names: BTreeSet<String>,
    ) -> Result<HashSet<String>> {
        let tenant = self.ctx.get_tenant();
        let provider = UserApiProvider::instance();
        let mut load = |name: &str| -> Result<Option<UserDefinedFunction>> {
            if let Some(udf) = bind_context.udf_cache.read().get(name).cloned() {
                return Ok(udf);
            }
            let udf = block_on(provider.get_udf(&tenant, name))?;
            bind_context
                .udf_cache
                .write()
                .insert(name.to_string(), udf.clone());
            Ok(udf)
        };

        let mut udaf_names = HashSet::new();
        for name in function_names {
            let Some(udf) = load(&name)? else {
                continue;
            };
            let aggregates = if udf.definition.is_aggregate() {
                true
            } else if let Some(body) = sql_udf_body(&udf, self.dialect)? {
                sql_udf_aggregates(&body, &self.name_resolution_ctx, self.dialect, &mut load, 0)?
            } else {
                false
            };
            if aggregates {
                udaf_names.insert(name);
            }
        }

        Ok(udaf_names)
    }
}
