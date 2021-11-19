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

use common_datavalues::prelude::DataColumn;
use common_datavalues::prelude::DataColumnWithField;
use common_datavalues::DataField;
use common_exception::ErrorCode;
use common_exception::Result;
use common_functions::scalars::FunctionFactory;
use common_functions::scalars::Monotonicity;
use common_functions::scalars::MonotonicityNode;
use common_functions::scalars::Range;
use common_planners::col;
use common_planners::Expression;
use common_planners::ExpressionVisitor;
use common_planners::Recursion;

// This visitor is for recursively visiting expression tree and collects all columns.
pub struct RequireColumnsVisitor {
    pub required_columns: HashSet<String>,
}

impl RequireColumnsVisitor {
    pub fn default() -> Self {
        Self {
            required_columns: HashSet::new(),
        }
    }

    pub fn collect_columns_from_expr(expr: &Expression) -> Result<HashSet<String>> {
        let mut visitor = Self::default();
        visitor = expr.accept(visitor)?;
        Ok(visitor.required_columns)
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

#[derive(Clone)]
pub struct MonotonicityCheckVisitor {
    // represents the monotonicity node of an expression
    pub node: Option<MonotonicityNode>,

    pub columns: HashSet<String>,

    // the variable range, we assume only one variable.
    pub variable_range: Option<Range>,
}

impl MonotonicityCheckVisitor {
    fn create_from_node(
        root: MonotonicityNode,
        columns: HashSet<String>,
        variable_range: Option<Range>,
    ) -> Self {
        MonotonicityCheckVisitor {
            node: Some(root),
            columns,
            variable_range,
        }
    }

    // Create an instance of MonotonicityCheckVisitor for checking functions with ONLY ONE variable.
    // Specify the variable range if you know it. Otherwise just pass in None.
    fn new(variable_range: Option<Range>) -> Self {
        MonotonicityCheckVisitor {
            node: None,
            columns: HashSet::new(),
            variable_range,
        }
    }

    fn visit_func_args(
        self,
        op: &str,
        children: &[Expression],
    ) -> Result<MonotonicityCheckVisitor> {
        let mut columns: HashSet<String> = HashSet::new();
        let mut nodes = vec![];
        for child_expr in children.iter() {
            let visitor = MonotonicityCheckVisitor::new(self.variable_range.clone());
            let visitor = child_expr.accept(visitor)?;

            // one of the children node is None, no need to check any more.
            if visitor.node.is_none() {
                return Ok(MonotonicityCheckVisitor::new(self.variable_range));
            }

            nodes.push(visitor.node.unwrap());
            columns.extend(visitor.columns);
        }

        let func = FunctionFactory::instance().get(op)?;
        let root_node = func.get_monotonicity(nodes.as_ref())?;
        let visitor =
            MonotonicityCheckVisitor::create_from_node(root_node, columns, self.variable_range);
        Ok(visitor)
    }

    /// Check whether the expression is monotonic or not.
    /// Return the monotonicity information, together with column name if any.
    pub fn check_expression(
        expr: &Expression,
        variable_range: Option<Range>,
    ) -> Result<(Monotonicity, String)> {
        let visitor = Self::new(variable_range);
        let visitor = expr.accept(visitor)?;

        // One of the expression has more than one column, we don't know the monotonicity.
        if visitor.columns.len() != 1 {
            return Ok((Monotonicity::default(), String::new()));
        }

        let column_name = visitor.columns.iter().next().unwrap();

        match visitor.node {
            None => Ok((Monotonicity::default(), String::new())),
            Some(MonotonicityNode::Function(mono, _)) => Ok((mono, column_name.to_string())),
            Some(MonotonicityNode::Variable(column_name, _)) => Ok((
                Monotonicity {
                    is_monotonic: true,
                    is_positive: true,
                },
                column_name,
            )),
            Some(MonotonicityNode::Constant(_value)) => Ok((
                Monotonicity {
                    is_monotonic: true,
                    is_positive: true,
                },
                String::new(),
            )),
        }
    }

    /// Extract sort column from sort expression. It checks the monotonicity information
    /// of the sort expression (like f(x) = x+2 is a monotonic function), and extract the
    /// column information, returns as Expression::Column.
    pub fn extract_sort_column(
        sort_expr: &Expression,
        variable_range: Option<Range>,
    ) -> Result<Expression> {
        if let Expression::Sort {
            expr: _,
            asc,
            nulls_first,
            origin_expr,
        } = sort_expr
        {
            let visitor = Self::new(variable_range);
            let visitor = origin_expr.accept(visitor)?;

            // One of the expression has more than one column, we don't know the monotonicity.
            // Skip the optimization, just return the input
            if visitor.columns.len() != 1 {
                return Ok(sort_expr.clone());
            }

            let column_name = visitor.columns.iter().next().unwrap();

            match visitor.node {
                None => return Ok(sort_expr.clone()),
                Some(MonotonicityNode::Function(mono, _)) => {
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
                Some(MonotonicityNode::Variable(column_name, _)) => {
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
            Expression::ScalarFunction { op, args } => self.visit_func_args(op, args),
            Expression::BinaryExpression { op, left, right } => {
                self.visit_func_args(op, &[*left.clone(), *right.clone()])
            }
            Expression::UnaryExpression { op, expr } => self.visit_func_args(op, &[*expr.clone()]),
            Expression::Column(col) => {
                let mut columns = HashSet::new();
                columns.insert(col.clone());

                let range = match &self.variable_range {
                    None => Range {
                        begin: None,
                        end: None,
                    },
                    Some(range) => range.clone(),
                };
                let node = MonotonicityNode::Variable(col.clone(), range);

                Ok(MonotonicityCheckVisitor::create_from_node(
                    node,
                    columns,
                    self.variable_range,
                ))
            }
            Expression::Literal {
                value,
                column_name,
                data_type,
            } => {
                let columns = HashSet::new();

                let name = match column_name {
                    Some(n) => n.clone(),
                    None => format!("{}", value),
                };
                let data_field = DataField::new(&name, data_type.clone(), false);
                let data_column =
                    DataColumnWithField::new(DataColumn::Constant(value.clone(), 1), data_field);

                let node = MonotonicityNode::Constant(data_column);
                Ok(MonotonicityCheckVisitor::create_from_node(
                    node,
                    columns,
                    self.variable_range,
                ))
            }
            _ => Ok(MonotonicityCheckVisitor::new(self.variable_range)),
        }
    }
}
