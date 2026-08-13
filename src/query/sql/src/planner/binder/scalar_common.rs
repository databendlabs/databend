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
use std::collections::HashSet;

use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::Scalar;
use databend_common_expression::types::DataType;

use crate::ColumnSet;
use crate::optimizer::ir::RelationalProperty;
use crate::plans::BoundColumnRef;
use crate::plans::CastExpr;
use crate::plans::ConstantExpr;
use crate::plans::FunctionCall;
use crate::plans::ScalarExpr;
use crate::plans::ScalarItem;
use crate::plans::Visitor;
use crate::plans::walk_expr;

pub const GROUPING_FUNC_NAME: &str = "grouping";
pub const GROUPING_ID_COLUMN_NAME: &str = "_grouping_id";

// Visitor that collects references to expressions that match a predicate.
pub struct Finder<'a, F>
where F: Fn(&ScalarExpr) -> bool
{
    find_fn: &'a F,
    scalars: Vec<&'a ScalarExpr>,
}

impl<'a, F> Finder<'a, F>
where F: Fn(&ScalarExpr) -> bool
{
    pub fn new(find_fn: &'a F) -> Self {
        Self {
            find_fn,
            scalars: Vec::new(),
        }
    }

    pub fn scalars(&self) -> &[&'a ScalarExpr] {
        &self.scalars
    }

    pub fn reset_finder(&mut self) {
        self.scalars.clear()
    }

    pub fn find_fn(&self) -> &'a F {
        self.find_fn
    }
}

impl<'a, F> Visitor<'a> for Finder<'a, F>
where F: Fn(&ScalarExpr) -> bool
{
    fn visit(&mut self, expr: &'a ScalarExpr) -> Result<()> {
        if (self.find_fn)(expr) {
            self.scalars.push(expr);
            // stop recursing down this expr once we find a match
        } else {
            walk_expr(self, expr)?;
        }
        Ok(())
    }
}

/// Visitor that checks whether any expression matches a predicate.
pub struct Any<'a, F>
where F: Fn(&ScalarExpr) -> bool
{
    find_fn: &'a F,
    result: bool,
}

impl<'a, F> Any<'a, F>
where F: Fn(&ScalarExpr) -> bool
{
    pub fn new(find_fn: &'a F) -> Self {
        Self {
            find_fn,
            result: false,
        }
    }

    pub fn result(&self) -> bool {
        self.result
    }

    pub fn reset(&mut self) {
        self.result = false;
    }
}

impl<'a, F> Visitor<'a> for Any<'_, F>
where F: Fn(&ScalarExpr) -> bool
{
    fn visit(&mut self, expr: &'a ScalarExpr) -> Result<()> {
        if self.result {
            return Ok(());
        }
        if (self.find_fn)(expr) {
            self.result = true;
        } else {
            walk_expr(self, expr)?;
        }
        Ok(())
    }
}

pub fn is_grouping_function(scalar: &ScalarExpr) -> bool {
    matches!(
        scalar,
        ScalarExpr::FunctionCall(func) if func.func_name.eq_ignore_ascii_case(GROUPING_FUNC_NAME)
    )
}

pub fn is_grouping_id_item(
    item: &ScalarItem,
    grouping_id_index: databend_common_expression::Symbol,
) -> bool {
    item.index == grouping_id_index
}

pub fn grouping_clause_error(function: &FunctionCall, clause_name: &str) -> ErrorCode {
    let err = if function.params.is_empty() && function.arguments.is_empty() {
        ErrorCode::BadArguments("grouping requires at least one argument")
    } else {
        ErrorCode::SemanticError(format!("{clause_name} can't contain grouping functions"))
    };

    err.set_span(function.span)
}

pub fn reject_grouping_functions<'a>(
    scalars: impl IntoIterator<Item = &'a ScalarExpr>,
    clause_name: &str,
) -> Result<()> {
    for scalar in scalars {
        let mut finder = Finder::new(&is_grouping_function);
        finder.visit(scalar)?;
        if let Some(ScalarExpr::FunctionCall(func)) = finder.scalars().first().copied() {
            return Err(grouping_clause_error(func, clause_name));
        }
    }

    Ok(())
}

#[inline]
fn conjunctions_with<T>(
    scalar: T,
    mut expand: impl FnMut(T, &mut Vec<T>) -> Option<T>,
) -> impl Iterator<Item = T> {
    let mut stack = vec![scalar];

    std::iter::from_fn(move || {
        loop {
            let scalar = stack.pop()?;
            if let Some(scalar) = expand(scalar, &mut stack) {
                return Some(scalar);
            }
        }
    })
}

fn is_conjunction(func: &FunctionCall) -> bool {
    matches!(func.func_name.as_str(), "and" | "and_filters")
}

pub fn conjunctions(scalar: &ScalarExpr) -> impl Iterator<Item = &ScalarExpr> {
    conjunctions_with(scalar, |scalar, stack| match scalar {
        ScalarExpr::FunctionCall(func) if is_conjunction(func) => {
            stack.extend(func.arguments.iter().rev());
            None
        }
        _ => Some(scalar),
    })
}

pub fn into_conjunctions(scalar: ScalarExpr) -> impl Iterator<Item = ScalarExpr> {
    conjunctions_with(scalar, |scalar, stack| match scalar {
        ScalarExpr::FunctionCall(func) if is_conjunction(&func) => {
            stack.extend(func.arguments.into_iter().rev());
            None
        }
        _ => Some(scalar),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn bool_constant(value: bool) -> ScalarExpr {
        ScalarExpr::ConstantExpr(ConstantExpr {
            span: None,
            value: Scalar::Boolean(value),
        })
    }

    fn and(lhs: ScalarExpr, rhs: ScalarExpr) -> ScalarExpr {
        ScalarExpr::FunctionCall(FunctionCall::new(None, "and".to_string(), vec![], vec![
            lhs, rhs,
        ]))
    }

    #[test]
    fn test_conjunctions_handles_deep_and_chain() {
        let mut expr = bool_constant(true);
        for _ in 0..1024 {
            expr = and(expr, bool_constant(true));
        }

        assert_eq!(conjunctions(&expr).count(), 1025);
    }

    #[test]
    fn test_conjunctions_handles_and_filters() {
        let expr = ScalarExpr::FunctionCall(FunctionCall::new(
            None,
            "and_filters".to_string(),
            vec![],
            vec![
                bool_constant(true),
                and(bool_constant(true), bool_constant(true)),
                bool_constant(false),
            ],
        ));

        assert_eq!(conjunctions(&expr).count(), 4);
        assert_eq!(into_conjunctions(expr).count(), 4);
    }

    #[test]
    fn test_finder_borrows_matches() {
        let expr = and(
            bool_constant(false),
            and(bool_constant(false), bool_constant(true)),
        );
        let ScalarExpr::FunctionCall(function) = &expr else {
            unreachable!();
        };
        let expected = &function.arguments[0];

        let predicate = |scalar: &ScalarExpr| {
            matches!(
                scalar,
                ScalarExpr::ConstantExpr(ConstantExpr {
                    value: Scalar::Boolean(false),
                    ..
                })
            )
        };
        let mut finder = Finder::new(&predicate);
        finder.visit(&expr).unwrap();
        let found = finder.scalars()[0];

        assert!(std::ptr::eq(found, expected));
    }

    #[test]
    fn test_any_stops_after_first_match() {
        let expr = and(
            bool_constant(false),
            and(bool_constant(false), bool_constant(true)),
        );
        let visits = std::cell::Cell::new(0);
        let predicate = |scalar: &ScalarExpr| {
            visits.set(visits.get() + 1);
            matches!(
                scalar,
                ScalarExpr::ConstantExpr(ConstantExpr {
                    value: Scalar::Boolean(false),
                    ..
                })
            )
        };
        let mut any = Any::new(&predicate);
        any.visit(&expr).unwrap();

        assert!(any.result());
        assert_eq!(visits.get(), 2);
    }
}

pub fn split_equivalent_predicate(scalar: &ScalarExpr) -> Option<(ScalarExpr, ScalarExpr)> {
    match scalar {
        ScalarExpr::FunctionCall(func) if func.func_name == "eq" => {
            Some((func.arguments[0].clone(), func.arguments[1].clone()))
        }
        _ => None,
    }
}

pub fn satisfied_by(scalar: &ScalarExpr, prop: &RelationalProperty) -> bool {
    satisfied_by_columns(&scalar.used_columns(), prop)
}

fn satisfied_by_columns(columns: &ColumnSet, prop: &RelationalProperty) -> bool {
    !columns.is_empty() && columns.is_subset(&prop.output_columns)
}

/// Helper to determine join condition type from a scalar expression.
/// Given a query: `SELECT * FROM t(a), t1(b) WHERE a = 1 AND b = 1 AND a = b AND a+b = 1`,
/// the predicate types are:
/// - ALL: `true`, `false`: SELECT * FROM t(a), t1(b) ON a = b AND true
/// - Left: `a = 1`
/// - Right: `b = 1`
/// - Both: `a = b`
/// - Other: `a+b = 1`
#[derive(Clone, Debug)]
pub enum JoinPredicate<'a> {
    ALL(&'a ScalarExpr),
    Left(&'a ScalarExpr),
    Right(&'a ScalarExpr),
    Both {
        left: Box<Cow<'a, ScalarExpr>>,
        right: Box<Cow<'a, ScalarExpr>>,
        is_equal_op: bool,
    },
    Other(&'a ScalarExpr),
}

fn fold_or_arguments(iter: impl Iterator<Item = ScalarExpr>) -> ScalarExpr {
    iter.fold(
        ConstantExpr {
            span: None,
            value: Scalar::Boolean(false),
        }
        .into(),
        |acc, arg| FunctionCall::new(None, "or".to_string(), vec![], vec![acc, arg]).into(),
    )
}

impl<'a> JoinPredicate<'a> {
    pub fn new(
        scalar: &'a ScalarExpr,
        left_prop: &RelationalProperty,
        right_prop: &RelationalProperty,
    ) -> Self {
        let used_columns = scalar.used_columns();
        if used_columns.is_empty() {
            return Self::ALL(scalar);
        }

        if satisfied_by_columns(&used_columns, left_prop) {
            return Self::Left(scalar);
        }

        if satisfied_by_columns(&used_columns, right_prop) {
            return Self::Right(scalar);
        }

        if let ScalarExpr::FunctionCall(func) = scalar {
            if func.func_name == "or_filters" && func.arguments.len() > 1 {
                let mut left_exprs = Vec::new();
                let mut right_exprs = Vec::new();

                for expr in func.arguments.iter() {
                    let used_columns = expr.used_columns();
                    if satisfied_by_columns(&used_columns, left_prop) {
                        left_exprs.push(expr.clone());
                    } else if satisfied_by_columns(&used_columns, right_prop) {
                        right_exprs.push(expr.clone());
                    } else {
                        return Self::Other(scalar);
                    }
                }
                return Self::Both {
                    left: Box::new(Cow::Owned(fold_or_arguments(left_exprs.into_iter()))),
                    right: Box::new(Cow::Owned(fold_or_arguments(right_exprs.into_iter()))),
                    is_equal_op: false,
                };
            }

            // Most join predicates are binary functions. `st_dwithin(left_geom, right_geom, d)`
            // is a special-case: the third argument is only a constant distance threshold, so
            // it should still be treated as a predicate between the first two join sides.
            if (func.arguments.len() == 2)
                || (func.arguments.len() == 3
                    && func.func_name == "st_dwithin"
                    && func.arguments[2].used_columns().is_empty())
            {
                let is_equal_op = func.func_name.as_str() == "eq";
                let left = &func.arguments[0];
                let right = &func.arguments[1];
                let left_used_columns = left.used_columns();
                let right_used_columns = right.used_columns();

                if satisfied_by_columns(&left_used_columns, left_prop)
                    && satisfied_by_columns(&right_used_columns, right_prop)
                {
                    return Self::Both {
                        left: Box::new(Cow::Borrowed(left)),
                        right: Box::new(Cow::Borrowed(right)),
                        is_equal_op,
                    };
                }

                if satisfied_by_columns(&right_used_columns, left_prop)
                    && satisfied_by_columns(&left_used_columns, right_prop)
                {
                    return Self::Both {
                        left: Box::new(Cow::Borrowed(right)),
                        right: Box::new(Cow::Borrowed(left)),
                        is_equal_op,
                    };
                }
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
        ScalarExpr::FunctionCall(func) => func.arguments.iter().any(contain_subquery),
        ScalarExpr::CastExpr(CastExpr { argument, .. }) => contain_subquery(argument),
        ScalarExpr::UDFCall(udf) => udf.arguments.iter().any(contain_subquery),
        _ => false,
    }
}

/// check if the scalar could be constructed by the columns
pub fn prune_by_children(scalar: &ScalarExpr, columns: &HashSet<ScalarExpr>) -> bool {
    struct PruneVisitor<'a> {
        columns: &'a HashSet<ScalarExpr>,
        can_prune: bool,
    }

    impl<'a> PruneVisitor<'a> {
        fn new(columns: &'a HashSet<ScalarExpr>) -> Self {
            Self {
                columns,
                can_prune: true,
            }
        }
    }

    impl<'a> Visitor<'a> for PruneVisitor<'a> {
        fn visit(&mut self, expr: &'a ScalarExpr) -> Result<()> {
            if self.columns.contains(expr) {
                return Ok(());
            }

            walk_expr(self, expr)
        }

        fn visit_bound_column_ref(&mut self, _: &'a BoundColumnRef) -> Result<()> {
            self.can_prune = false;
            Ok(())
        }

        fn visit_subquery(&mut self, _: &'a crate::plans::SubqueryExpr) -> Result<()> {
            self.can_prune = false;
            Ok(())
        }

        fn visit_constant(&mut self, _constant: &'a crate::plans::ConstantExpr) -> Result<()> {
            self.can_prune = false;
            Ok(())
        }
    }

    let mut visitor = PruneVisitor::new(columns);
    visitor.visit(scalar).unwrap();

    visitor.can_prune
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

pub fn wrap_nullable(scalar: ScalarExpr, source_type: &DataType) -> ScalarExpr {
    if source_type.is_nullable_or_null() {
        scalar
    } else {
        let target_type = source_type.wrap_nullable();
        ScalarExpr::CastExpr(CastExpr {
            span: scalar.span(),
            is_try: false,
            argument: Box::new(scalar),
            target_type: Box::new(target_type),
        })
    }
}
