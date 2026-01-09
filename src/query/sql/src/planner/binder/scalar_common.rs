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

use databend_common_exception::Result;
use databend_common_expression::Scalar;
use databend_common_expression::types::DataType;

use crate::optimizer::ir::RelationalProperty;
use crate::plans::BoundColumnRef;
use crate::plans::CastExpr;
use crate::plans::ConstantExpr;
use crate::plans::FunctionCall;
use crate::plans::ScalarExpr;
use crate::plans::Visitor;
use crate::plans::walk_expr;

// Visitor that find Expressions that match a particular predicate
pub struct Finder<'a, F>
where F: Fn(&ScalarExpr) -> bool
{
    find_fn: &'a F,
    scalars: Vec<ScalarExpr>,
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

    pub fn scalars(&self) -> &[ScalarExpr] {
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
            if !(self.scalars.contains(expr)) {
                self.scalars.push((*expr).clone())
            }
            // stop recursing down this expr once we find a match
        } else {
            walk_expr(self, expr)?;
        }
        Ok(())
    }
}

pub fn split_conjunctions(scalar: &ScalarExpr) -> Vec<ScalarExpr> {
    match scalar {
        ScalarExpr::FunctionCall(func) if func.func_name == "and" => [
            split_conjunctions(&func.arguments[0]),
            split_conjunctions(&func.arguments[1]),
        ]
        .concat(),
        _ => {
            vec![scalar.clone()]
        }
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
    scalar.used_columns().is_subset(&prop.output_columns) && !scalar.used_columns().is_empty()
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
        |acc, arg| {
            FunctionCall {
                span: None,
                func_name: "or".to_string(),
                params: vec![],
                arguments: vec![acc, arg],
            }
            .into()
        },
    )
}

impl<'a> JoinPredicate<'a> {
    pub fn new(
        scalar: &'a ScalarExpr,
        left_prop: &RelationalProperty,
        right_prop: &RelationalProperty,
    ) -> Self {
        if scalar.used_columns().is_empty() {
            return Self::ALL(scalar);
        }

        if satisfied_by(scalar, left_prop) {
            return Self::Left(scalar);
        }

        if satisfied_by(scalar, right_prop) {
            return Self::Right(scalar);
        }

        if let ScalarExpr::FunctionCall(func) = scalar {
            if func.func_name == "or_filters" && func.arguments.len() > 1 {
                let mut left_exprs = Vec::new();
                let mut right_exprs = Vec::new();

                for expr in func.arguments.iter() {
                    if satisfied_by(expr, left_prop) {
                        left_exprs.push(expr.clone());
                    } else if satisfied_by(expr, right_prop) {
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

            if func.arguments.len() > 2 {
                return Self::Other(scalar);
            }

            if func.arguments.len() == 2 {
                let is_equal_op = func.func_name.as_str() == "eq";
                let left = &func.arguments[0];
                let right = &func.arguments[1];

                if satisfied_by(left, left_prop) && satisfied_by(right, right_prop) {
                    return Self::Both {
                        left: Box::new(Cow::Borrowed(left)),
                        right: Box::new(Cow::Borrowed(right)),
                        is_equal_op,
                    };
                }

                if satisfied_by(right, left_prop) && satisfied_by(left, right_prop) {
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
