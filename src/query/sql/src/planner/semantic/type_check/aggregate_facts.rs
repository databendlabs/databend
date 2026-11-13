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

//! Facts about the aggregates of lowered expressions, known before they are
//! bound.
//!
//! Some decisions about a SELECT depend on all of its clauses, e.g. whether
//! it is grouped, while binding a clause has effects that belong to the phase
//! it is bound in, e.g. registering its aggregate functions. The decisions
//! read these facts instead, which the lowering classifies with the same
//! nodes the type checker binds, so no clause is bound early.

use std::collections::HashSet;

use databend_common_ast::ast::ColumnRef;
use databend_common_ast::ast::Identifier;
use smallvec::SmallVec;

use super::CoreExpr;
use super::CoreExprArena;
use super::CoreExprId;
use super::window::CoreWindow;
use super::window::CoreWindowDesc;
use crate::NameResolutionContext;
use crate::planner::semantic::normalize_identifier;

/// Where a node is, relative to the constructs that change what an aggregate
/// function in it means.
#[derive(Clone, Copy, Default)]
struct Region {
    /// In the arguments or the specification of a window function, which are
    /// evaluated after the aggregate.
    in_window: bool,
    /// In the specification of a window function, where an aggregate
    /// function must be one the SELECT already has.
    in_window_spec: bool,
    /// In the body of a lambda function.
    in_lambda_body: bool,
}

/// The user-defined functions that are aggregate functions, by normalized
/// name, and how to normalize the names of calls.
pub(crate) struct UdafNames<'a> {
    pub(crate) names: &'a HashSet<String>,
    pub(crate) name_resolution_ctx: &'a NameResolutionContext,
}

impl UdafNames<'_> {
    fn contains(&self, name: &Identifier) -> bool {
        self.names
            .contains(&normalize_identifier(name, self.name_resolution_ctx).name)
    }
}

impl<'a> CoreExprArena<'a> {
    /// Whether the expression makes its SELECT grouped: it calls an aggregate
    /// function outside subqueries, window specifications and lambda bodies,
    /// as `sum(a)` in `sum(sum(a)) OVER ()`. `grouping()` alone does not.
    pub(crate) fn contains_grouping_aggregate(
        &self,
        root: CoreExprId,
        udafs: &UdafNames<'_>,
    ) -> bool {
        let mut found = false;
        self.visit_outside_subqueries(root, &mut |expr, region| {
            found |= !region.in_window_spec && !region.in_lambda_body && is_aggregate(expr, udafs);
        });
        found
    }

    /// Whether the expression calls an aggregate function outside subqueries,
    /// window functions and lambda bodies.
    pub(crate) fn contains_aggregate_outside_windows(
        &self,
        root: CoreExprId,
        udafs: &UdafNames<'_>,
    ) -> bool {
        let mut found = false;
        self.visit_outside_subqueries(root, &mut |expr, region| {
            found |= !region.in_window && !region.in_lambda_body && is_aggregate(expr, udafs);
        });
        found
    }

    /// Whether the expression is evaluated after the aggregate of its SELECT:
    /// it calls an aggregate function or `grouping()` outside subqueries,
    /// also in the arguments of a window function.
    pub(crate) fn contains_aggregate(&self, root: CoreExprId, udafs: &UdafNames<'_>) -> bool {
        let mut found = false;
        self.visit_outside_subqueries(root, &mut |expr, _| {
            found |= is_aggregate(expr, udafs)
                || matches!(expr, CoreExpr::Call { func_name, .. } if func_name.eq_ignore_ascii_case("grouping"));
        });
        found
    }

    /// The column references outside subqueries.
    pub(crate) fn column_refs(&self, root: CoreExprId) -> Vec<&'a ColumnRef> {
        let mut column_refs = Vec::new();
        self.visit_outside_subqueries(root, &mut |expr, _| {
            if let CoreExpr::ColumnRef { column, .. } = expr {
                column_refs.push(*column);
            }
        });
        column_refs
    }

    /// The names of the calls of user-defined functions outside subqueries.
    pub(crate) fn udf_call_names(&self, root: CoreExprId) -> Vec<&'a Identifier> {
        let mut names = Vec::new();
        self.visit_outside_subqueries(root, &mut |expr, _| {
            if let CoreExpr::UdfCall { name, .. } = expr {
                names.push(*name);
            }
        });
        names
    }

    fn visit_outside_subqueries(
        &self,
        root: CoreExprId,
        visit: &mut impl FnMut(&CoreExpr<'a>, Region),
    ) {
        let mut pending = vec![(root, Region::default())];
        while let Some((id, region)) = pending.pop() {
            let expr = self.get(id);
            visit(expr, region);
            let window = Region {
                in_window: true,
                ..region
            };
            let window_spec = Region {
                in_window_spec: true,
                ..window
            };
            match expr {
                CoreExpr::LambdaFunction {
                    args, lambda_expr, ..
                } => {
                    pending.extend(args.iter().map(|arg| (*arg, region)));
                    pending.push((*lambda_expr, Region {
                        in_lambda_body: true,
                        ..region
                    }));
                }
                CoreExpr::AggregateWindowFunction {
                    params,
                    args,
                    order_by,
                    window: desc,
                    ..
                } => {
                    pending.extend(
                        params
                            .iter()
                            .map(|(_, param)| *param)
                            .chain(args.iter().copied())
                            .chain(order_by.iter().map(|order| order.expr))
                            .map(|id| (id, window)),
                    );
                    pending.extend(window_exprs(desc).map(|id| (id, window_spec)));
                }
                CoreExpr::GeneralWindowFunction {
                    args,
                    order_by,
                    window: desc,
                    ..
                } => {
                    pending.extend(
                        args.iter()
                            .copied()
                            .chain(order_by.iter().map(|order| order.expr))
                            .map(|id| (id, window)),
                    );
                    pending.extend(window_exprs(desc).map(|id| (id, window_spec)));
                }
                CoreExpr::CountAllWindowFunction { window: spec, .. } => {
                    pending.extend(window_spec_exprs(spec).map(|id| (id, window_spec)));
                }
                expr => pending.extend(children(expr).into_iter().map(|id| (id, region))),
            }
        }
    }
}

/// Whether the node calls an aggregate function, i.e. is evaluated by the
/// aggregate.
fn is_aggregate(expr: &CoreExpr<'_>, udafs: &UdafNames<'_>) -> bool {
    match expr {
        CoreExpr::AggregateFunction { .. } => true,
        CoreExpr::UdfCall { name, .. } => udafs.contains(name),
        _ => false,
    }
}

/// The children of a node other than a window or lambda function, outside
/// subqueries.
fn children(expr: &CoreExpr<'_>) -> SmallVec<[CoreExprId; 4]> {
    match expr {
        CoreExpr::ColumnRef { .. }
        | CoreExpr::Literal { .. }
        | CoreExpr::StageLocation { .. }
        | CoreExpr::LambdaFunction { .. }
        | CoreExpr::AggregateWindowFunction { .. }
        | CoreExpr::GeneralWindowFunction { .. }
        | CoreExpr::CountAllWindowFunction { .. } => SmallVec::new(),
        CoreExpr::Array { exprs, .. } | CoreExpr::Tuple { exprs, .. } => exprs.clone(),
        CoreExpr::Map { kvs, .. } => kvs.iter().map(|(_, value)| *value).collect(),
        CoreExpr::MapAccess { expr, .. } | CoreExpr::Cast { expr, .. } => {
            SmallVec::from_slice(&[*expr])
        }
        CoreExpr::Call { args, .. } | CoreExpr::SetReturningFunction { args, .. } => args.clone(),
        CoreExpr::UdfCall { args, .. } => args.iter().map(|(_, arg)| *arg).collect(),
        CoreExpr::ScalarFunction { params, args, .. } => params
            .iter()
            .map(|(_, param)| *param)
            .chain(args.iter().copied())
            .collect(),
        CoreExpr::InList { expr, list, .. } => {
            std::iter::once(*expr).chain(list.iter().copied()).collect()
        }
        CoreExpr::Subquery { child_expr, .. } => child_expr.iter().copied().collect(),
        CoreExpr::SpecialFunction { function, .. } => function.args().into_iter().collect(),
        CoreExpr::SearchFunction { function, .. } => function.args().collect(),
        CoreExpr::AsyncFunction { function, .. } => function.args().into_iter().collect(),
        CoreExpr::AggregateFunction {
            params,
            args,
            order_by,
            ..
        } => params
            .iter()
            .map(|(_, param)| *param)
            .chain(args.iter().copied())
            .chain(order_by.iter().map(|order| order.expr))
            .collect(),
    }
}

fn window_exprs<'b>(desc: &'b CoreWindowDesc<'_>) -> impl Iterator<Item = CoreExprId> + 'b {
    window_spec_exprs(&desc.window)
}

/// The expressions of an inline window specification. A named window is
/// lowered where it is defined.
fn window_spec_exprs<'b>(window: &'b CoreWindow<'_>) -> impl Iterator<Item = CoreExprId> + 'b {
    let spec = match window {
        CoreWindow::WindowSpec(spec) => Some(spec),
        CoreWindow::WindowReference(_) => None,
    };
    spec.into_iter().flat_map(|spec| {
        spec.partition_by
            .iter()
            .copied()
            .chain(spec.order_by.iter().map(|order| order.expr))
            .chain(spec.window_frame.iter().flat_map(|frame| frame.exprs()))
    })
}

#[cfg(test)]
mod tests {
    use databend_common_ast::parser::Dialect;
    use databend_common_ast::parser::parse_expr;
    use databend_common_ast::parser::tokenize_sql;

    use super::*;

    /// `(contains_grouping_aggregate, contains_aggregate_outside_windows,
    /// contains_aggregate)` of `sql`, with `my_udaf` a user-defined aggregate
    /// function.
    fn facts(sql: &str) -> (bool, bool, bool) {
        let tokens = tokenize_sql(sql).unwrap();
        let expr = parse_expr(&tokens, Dialect::PostgreSQL).unwrap();
        let mut arena = CoreExprArena::new(0);
        let root = arena.lower_ast_expr(&expr).unwrap();
        let names = HashSet::from(["my_udaf".to_string()]);
        let name_resolution_ctx = NameResolutionContext::default();
        let udafs = UdafNames {
            names: &names,
            name_resolution_ctx: &name_resolution_ctx,
        };
        (
            arena.contains_grouping_aggregate(root, &udafs),
            arena.contains_aggregate_outside_windows(root, &udafs),
            arena.contains_aggregate(root, &udafs),
        )
    }

    #[test]
    fn classifies_aggregates_by_region() {
        for (sql, expected) in [
            ("a + 1", (false, false, false)),
            ("sum(a) + 1", (true, true, true)),
            ("count(*)", (true, true, true)),
            ("my_udaf(a)", (true, true, true)),
            ("my_scalar_udf(a)", (false, false, false)),
            // A window function itself is evaluated after the aggregate.
            ("sum(a) OVER ()", (false, false, false)),
            ("count(*) OVER ()", (false, false, false)),
            // An aggregate in its arguments makes the SELECT grouped.
            ("sum(sum(a)) OVER ()", (true, false, true)),
            ("first_value(my_udaf(a)) OVER ()", (true, false, true)),
            // One in its specification does not, but is evaluated by the
            // aggregate.
            ("row_number() OVER (ORDER BY max(a))", (false, false, true)),
            (
                "row_number() OVER (PARTITION BY my_udaf(a))",
                (false, false, true),
            ),
            ("sum(sum(a)) OVER (ORDER BY max(a))", (true, false, true)),
            ("grouping(a)", (false, false, true)),
            ("(SELECT sum(a) FROM t)", (false, false, false)),
            ("a IN (SELECT max(b) FROM t)", (false, false, false)),
            ("max(b) IN (SELECT 1)", (true, true, true)),
            ("array_transform([1], x -> x + a)", (false, false, false)),
            ("array_transform([sum(a)], x -> x + 1)", (true, true, true)),
            ("coalesce(a, max(b))", (true, true, true)),
            ("CAST(sum(a) AS STRING)", (true, true, true)),
        ] {
            assert_eq!(facts(sql), expected, "{sql}");
        }
    }

    #[test]
    fn collects_names_outside_subqueries() {
        let tokens = tokenize_sql("f(a, t.b) + (SELECT g(c)) + h(d)").unwrap();
        let expr = parse_expr(&tokens, Dialect::PostgreSQL).unwrap();
        let mut arena = CoreExprArena::new(0);
        let root = arena.lower_ast_expr(&expr).unwrap();
        let mut columns = arena
            .column_refs(root)
            .iter()
            .map(|column| column.to_string())
            .collect::<Vec<_>>();
        columns.sort();
        assert_eq!(columns, ["a", "d", "t.b"]);
        let mut udfs = arena
            .udf_call_names(root)
            .iter()
            .map(|name| name.name.clone())
            .collect::<Vec<_>>();
        udfs.sort();
        assert_eq!(udfs, ["f", "h"]);
    }
}
