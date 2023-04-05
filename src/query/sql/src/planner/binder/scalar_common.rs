// Copyright 2022 Datafuse Labs.
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

use std::collections::HashSet;

use common_exception::Result;
use common_expression::types::DataType;

use crate::binder::scalar_visitor::Recursion;
use crate::binder::scalar_visitor::ScalarVisitor;
use crate::optimizer::RelationalProperty;
use crate::plans::AndExpr;
use crate::plans::BoundColumnRef;
use crate::plans::CastExpr;
use crate::plans::ComparisonExpr;
use crate::plans::ComparisonOp;
use crate::plans::FunctionCall;
use crate::plans::NotExpr;
use crate::plans::OrExpr;
use crate::plans::ScalarExpr;
use crate::plans::WindowFuncType;

// Visitor that find Expressions that match a particular predicate
struct Finder<'a, F>
where F: Fn(&ScalarExpr) -> bool
{
    find_fn: &'a F,
    scalars: Vec<ScalarExpr>,
}

impl<'a, F> Finder<'a, F>
where F: Fn(&ScalarExpr) -> bool
{
    /// Create a new finder with the `test_fn`
    #[allow(dead_code)]
    fn new(find_fn: &'a F) -> Self {
        Self {
            find_fn,
            scalars: Vec::new(),
        }
    }
}

impl<'a, F> ScalarVisitor for Finder<'a, F>
where F: Fn(&ScalarExpr) -> bool
{
    fn pre_visit(mut self, scalar: &ScalarExpr) -> Result<Recursion<Self>> {
        if (self.find_fn)(scalar) {
            if !(self.scalars.contains(scalar)) {
                self.scalars.push((*scalar).clone())
            }
            // stop recursing down this expr once we find a match
            return Ok(Recursion::Stop(self));
        }

        Ok(Recursion::Continue(self))
    }
}

pub fn split_conjunctions(scalar: &ScalarExpr) -> Vec<ScalarExpr> {
    match scalar {
        ScalarExpr::AndExpr(AndExpr { left, right, .. }) => {
            vec![split_conjunctions(left), split_conjunctions(right)].concat()
        }
        _ => {
            vec![scalar.clone()]
        }
    }
}

pub fn split_equivalent_predicate(scalar: &ScalarExpr) -> Option<(ScalarExpr, ScalarExpr)> {
    match scalar {
        ScalarExpr::ComparisonExpr(ComparisonExpr {
            op, left, right, ..
        }) if *op == ComparisonOp::Equal => Some((*left.clone(), *right.clone())),
        _ => None,
    }
}

pub fn satisfied_by(scalar: &ScalarExpr, prop: &RelationalProperty) -> bool {
    scalar.used_columns().is_subset(&prop.output_columns)
}

/// Helper to determine join condition type from a scalar expression.
/// Given a query: `SELECT * FROM t(a), t1(b) WHERE a = 1 AND b = 1 AND a = b AND a+b = 1`,
/// the predicate types are:
/// - Left: `a = 1`
/// - Right: `b = 1`
/// - Both: `a = b`
/// - Other: `a+b = 1`
#[derive(Clone, Debug)]
pub enum JoinPredicate<'a> {
    Left(&'a ScalarExpr),
    Right(&'a ScalarExpr),
    Both {
        left: &'a ScalarExpr,
        right: &'a ScalarExpr,
        equal: bool,
    },
    Other(&'a ScalarExpr),
}

impl<'a> JoinPredicate<'a> {
    pub fn new(
        scalar: &'a ScalarExpr,
        left_prop: &RelationalProperty,
        right_prop: &RelationalProperty,
    ) -> Self {
        if contain_subquery(scalar) {
            return Self::Other(scalar);
        }
        if satisfied_by(scalar, left_prop) {
            return Self::Left(scalar);
        }

        if satisfied_by(scalar, right_prop) {
            return Self::Right(scalar);
        }

        if let ScalarExpr::ComparisonExpr(ComparisonExpr {
            op, left, right, ..
        }) = scalar
        {
            if satisfied_by(left, left_prop) && satisfied_by(right, right_prop) {
                return Self::Both {
                    left,
                    right,
                    equal: op == &ComparisonOp::Equal,
                };
            }

            if satisfied_by(right, left_prop) && satisfied_by(left, right_prop) {
                return Self::Both {
                    left: right,
                    right: left,
                    equal: op == &ComparisonOp::Equal,
                };
            }
        }

        Self::Other(scalar)
    }
}

pub fn contain_subquery(scalar: &ScalarExpr) -> bool {
    match scalar {
        ScalarExpr::BoundColumnRef(BoundColumnRef { column, .. }) => {
            // For example: SELECT * FROM c WHERE c_id=(SELECT c_id FROM o WHERE ship='WA' AND bill='FL');
            // predicate `c_id = scalar_subquery_{}` can't be pushed down to the join condition.
            // TODO(xudong963): need a better way to handle this, such as add a field to predicate to indicate if it derives from subquery.
            column.column_name == format!("scalar_subquery_{}", column.index)
        }
        ScalarExpr::ComparisonExpr(ComparisonExpr { left, right, .. }) => {
            contain_subquery(left) || contain_subquery(right)
        }
        ScalarExpr::AndExpr(AndExpr { left, right, .. }) => {
            contain_subquery(left) || contain_subquery(right)
        }
        ScalarExpr::OrExpr(OrExpr { left, right, .. }) => {
            contain_subquery(left) || contain_subquery(right)
        }
        ScalarExpr::NotExpr(NotExpr { argument, .. }) => contain_subquery(argument),
        ScalarExpr::FunctionCall(FunctionCall { arguments, .. }) => {
            arguments.iter().any(contain_subquery)
        }
        ScalarExpr::CastExpr(CastExpr { argument, .. }) => contain_subquery(argument),
        _ => false,
    }
}

/// check if the scalar could be constructed by the columns
pub fn prune_by_children(scalar: &ScalarExpr, columns: &HashSet<ScalarExpr>) -> bool {
    if columns.contains(scalar) {
        return true;
    }

    match scalar {
        ScalarExpr::BoundColumnRef(_) => false,
        ScalarExpr::BoundInternalColumnRef(_) => false,
        ScalarExpr::ConstantExpr(_) => true,
        ScalarExpr::AndExpr(scalar) => {
            prune_by_children(&scalar.left, columns) && prune_by_children(&scalar.right, columns)
        }
        ScalarExpr::OrExpr(scalar) => {
            prune_by_children(&scalar.left, columns) && prune_by_children(&scalar.right, columns)
        }
        ScalarExpr::NotExpr(scalar) => prune_by_children(&scalar.argument, columns),
        ScalarExpr::ComparisonExpr(scalar) => {
            prune_by_children(&scalar.left, columns) && prune_by_children(&scalar.right, columns)
        }
        ScalarExpr::WindowFunction(scalar) => {
            let flag = match &scalar.func {
                WindowFuncType::Aggregate(agg) => {
                    agg.args.iter().all(|arg| prune_by_children(arg, columns))
                }
                _ => false,
            };
            flag || scalar
                .partition_by
                .iter()
                .all(|arg| prune_by_children(arg, columns))
                || scalar
                    .order_by
                    .iter()
                    .all(|arg| prune_by_children(&arg.expr, columns))
        }
        ScalarExpr::AggregateFunction(scalar) => scalar
            .args
            .iter()
            .all(|arg| prune_by_children(arg, columns)),
        ScalarExpr::FunctionCall(scalar) => scalar
            .arguments
            .iter()
            .all(|arg| prune_by_children(arg, columns)),
        ScalarExpr::CastExpr(expr) => prune_by_children(expr.argument.as_ref(), columns),
        ScalarExpr::SubqueryExpr(_) => false,
    }
}

/// Wrap a cast expression with given target type
pub fn wrap_cast(scalar: &ScalarExpr, target_type: &DataType) -> ScalarExpr {
    ScalarExpr::CastExpr(CastExpr {
        span: scalar.span(),
        is_try: false,
        argument: Box::new(scalar.clone()),
        target_type: Box::new(target_type.clone()),
    })
}
