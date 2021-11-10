// Copyright 2021 Datafuse Labs.
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

use common_exception::ErrorCode;
use common_exception::Result;
use common_functions::scalars::FunctionFactory;
use common_functions::scalars::MonotonicityNode;
use common_planners::col;
use common_planners::Expression;
use common_planners::ExpressionVisitor;
use common_planners::Recursion;

pub struct RequireColumnsVisitor {
    pub required_columns: HashSet<String>,
}

impl RequireColumnsVisitor {
    pub fn default() -> Self {
        Self {
            required_columns: HashSet::new(),
        }
    }
}

impl ExpressionVisitor for RequireColumnsVisitor {
    fn pre_visit(self, expr: &Expression) -> Result<Recursion<Self>> {
        match expr {
            Expression::Column(c) => {
                let mut v = self;
                v.required_columns.insert(c.clone());
                Ok(Recursion::Continue(v))
            }
            _ => Ok(Recursion::Continue(self)),
        }
    }
}

pub struct MonotonicityCheckVisitor {
    // represents the monotonicity of an expression
    pub node: Option<MonotonicityNode>,

    pub columns: HashSet<String>,
}

impl MonotonicityCheckVisitor {
    pub fn new(root: MonotonicityNode, mut columns: HashSet<String>) -> Self {
        if let MonotonicityNode::Variable(name) = root.clone() {
            columns.insert(name);
        }

        MonotonicityCheckVisitor {
            node: Some(root),
            columns,
        }
    }

    pub fn default() -> Self {
        MonotonicityCheckVisitor {
            node: None,
            columns: HashSet::new(),
        }
    }

    fn visit_func_args(op: &str, children: &[Expression]) -> Result<MonotonicityCheckVisitor> {
        let mut columns: HashSet<String> = HashSet::new();
        let mut nodes = vec![];
        for child_expr in children.iter() {
            let visitor = MonotonicityCheckVisitor::default();
            let visitor = child_expr.accept(visitor)?;

            // one of the children node is None, no need to check any more.
            if visitor.node.is_none() {
                return Ok(MonotonicityCheckVisitor::default());
            }
            nodes.push(visitor.node.unwrap());
            columns.extend(visitor.columns);
        }

        let func = FunctionFactory::instance().get(op)?;
        let root_node = func.get_monotonicity(nodes.as_ref())?;
        let visitor = MonotonicityCheckVisitor::new(root_node, columns);
        Ok(visitor)
    }

    // Extract sort column from sort expression. It checks the monotonicity information
    // of the sort expression (like f(x) = x+2 is a monotonic function), and extract the
    // column information, returns as Expression::Column.
    pub fn extract_sort_column(sort_expr: &Expression) -> Result<Expression> {
        if let Expression::Sort {
            expr: _,
            asc,
            nulls_first,
            origin_expr,
        } = sort_expr
        {
            let visitor = Self::default();
            let visitor = origin_expr.accept(visitor)?;

            // One of the expression has more than one column, we don't know the monotonicity.
            // Skip the optimization, just return the input
            if visitor.columns.len() != 1 {
                return Ok(sort_expr.clone());
            }

            let column_name = visitor.columns.iter().next().unwrap();

            match visitor.node {
                None => return Ok(sort_expr.clone()),
                Some(MonotonicityNode::Function(mono)) => {
                    // the current function is not monotonic, we skip the optimization
                    if !mono.is_monotonic {
                        return Ok(sort_expr.clone());
                    }

                    // need to flip the asc when is_positive is false
                    let new_asc = if mono.is_positive { *asc } else { !*asc };
                    return Ok(Expression::Sort {
                        expr: Box::new(col(column_name)),
                        asc: new_asc,
                        nulls_first: *nulls_first,
                        origin_expr: origin_expr.clone(),
                    });
                }
                Some(MonotonicityNode::Variable(column_name)) => {
                    return Ok(Expression::Sort {
                        expr: Box::new(col(&column_name)),
                        asc: *asc,
                        nulls_first: *nulls_first,
                        origin_expr: origin_expr.clone(),
                    });
                }
                Some(MonotonicityNode::Constant(_value)) => return Ok(sort_expr.clone()),
            };
        }
        Err(ErrorCode::BadArguments(format!(
            "expect sort expression, get {:?}",
            sort_expr
        )))
    }
}

impl ExpressionVisitor for MonotonicityCheckVisitor {
    fn pre_visit(self, _expr: &Expression) -> Result<Recursion<Self>> {
        Ok(Recursion::Continue(self))
    }

    fn visit(self, expr: &Expression) -> Result<Self> {
        match expr {
            Expression::ScalarFunction { op, args } => {
                MonotonicityCheckVisitor::visit_func_args(op, args)
            }
            Expression::BinaryExpression { op, left, right } => {
                MonotonicityCheckVisitor::visit_func_args(op, &[*left.clone(), *right.clone()])
            }
            Expression::UnaryExpression { op, expr } => {
                MonotonicityCheckVisitor::visit_func_args(op, &[*expr.clone()])
            }
            Expression::Column(col) => {
                let columns: HashSet<String> = HashSet::new();
                let node = MonotonicityNode::Variable(col.clone());
                Ok(MonotonicityCheckVisitor::new(node, columns))
            }
            Expression::Literal { value, .. } => {
                let columns: HashSet<String> = HashSet::new();
                let node = MonotonicityNode::Constant(value.clone());
                Ok(MonotonicityCheckVisitor::new(node, columns))
            }
            _ => Ok(MonotonicityCheckVisitor::default()),
        }
    }
}
