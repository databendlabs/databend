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

// MonotonicityCheckVisitor visit the expression tree to calculate monotonicity.
// For example, a function of Add(Neg(number), 5) for number < -100 will have a tree like this:
//
// .                   MonotonicityNode::Function -- 'Add'
//                      (mono: is_positive=true, Range{105, MAX})
//                         /                          \
//                        /                            \
//      MonotonicityNode::Function -- f(x)=-x         Monotonicity::Constant -- 5
//    (mono: is_positive=true, range{100, MAX})
//                     /
//                    /
//     MonotonicityNode::Function -- f(x)=x
//         (range{MIN, -100})
//
// The structure of the tree is basically the structure of the expression.
// Simple depth first search visit the expression tree and gete monotonicity from
// every function. Each function is responsible to implement its own monotonicity
// function.
// Notice!! the mechanism doesn't solve multiple variables case.
#[derive(Clone)]
pub struct MonotonicityCheckVisitor {
    // represents the monotonicity of an expression
    pub mono: Option<Monotonicity>,

    // the variable range left, we assume only one variable.
    pub variable_left: Option<DataColumnWithField>,

    // the variable range right, we assume only one variable.
    pub variable_right: Option<DataColumnWithField>,

    pub columns: HashSet<String>,
}

impl MonotonicityCheckVisitor {
    fn empty() -> Self {
        Self {
            mono: None,
            variable_left: None,
            variable_right: None,
            columns: HashSet::new(),
        }
    }

    fn visit_func_args(
        self,
        op: &str,
        children: &[Expression],
    ) -> Result<MonotonicityCheckVisitor> {
        let mut monotonicity_vec = vec![];
        let mut left_vec = vec![];
        let mut right_vec = vec![];
        let mut columns = HashSet::<String>::new();

        for child_expr in children.iter() {
            let visitor = self.clone();
            let visitor = child_expr.accept(visitor)?;

            columns.extend(visitor.columns);

            // one of the children node is None, no need to check any more.
            if visitor.mono.is_none() {
                return Ok(MonotonicityCheckVisitor::empty());
            }

            let monotonicity = visitor.mono.unwrap();
            if !monotonicity.is_monotonic {
                return Ok(MonotonicityCheckVisitor::empty());
            }

            left_vec.push(monotonicity.left.clone());
            right_vec.push(monotonicity.right.clone());
            monotonicity_vec.push(monotonicity);
        }

        let func = FunctionFactory::instance().get(op)?;

        let mut root_mono = func.get_monotonicity(monotonicity_vec.as_ref())?;

        // neither a monotonic expression nor constant, no need to calculating boundary
        if !root_mono.is_monotonic && !root_mono.is_constant {
            return Ok(MonotonicityCheckVisitor::empty());
        }

        // lambda to convert data column to data column with field
        let data_column_to_column_field = |data_column: DataColumn| -> Option<DataColumnWithField> {
            let data_field = DataField::new("", data_column.data_type(), false);
            let data_column_field = DataColumnWithField::new(data_column, data_field);
            Some(data_column_field)
        };

        // if any one left boundary is None(unknown), we set new_left as None too.
        let new_left: Option<DataColumnWithField> = if left_vec.iter().any(|col| col.is_none()) {
            None
        } else {
            let args = left_vec
                .into_iter()
                .map(|col_opt| col_opt.unwrap())
                .collect::<Vec<_>>();
            let new_left_data_column = func.eval(&args, 1)?;
            data_column_to_column_field(new_left_data_column)
        };

        let new_right: Option<DataColumnWithField> = if right_vec.iter().any(|col| col.is_none()) {
            None
        } else {
            let args = right_vec
                .into_iter()
                .map(|col_opt| col_opt.unwrap())
                .collect::<Vec<_>>();
            let new_right_data_column = func.eval(&args, 1)?;
            data_column_to_column_field(new_right_data_column)
        };

        root_mono.left = new_left;
        root_mono.right = new_right;

        Ok(MonotonicityCheckVisitor {
            mono: Some(root_mono),
            variable_left: self.variable_left,
            variable_right: self.variable_right,
            columns,
        })
    }

    /// Check whether the expression is monotonic or not. The left should be <= right.
    /// Return the monotonicity information, together with column name if any.
    pub fn check_expression(
        expr: &Expression,
        left: Option<DataColumnWithField>,
        right: Option<DataColumnWithField>,
    ) -> Result<(Monotonicity, String)> {
        let visitor = MonotonicityCheckVisitor {
            mono: None,
            variable_left: left,
            variable_right: right,
            columns: HashSet::new(),
        };

        let visitor = expr.accept(visitor)?;

        // The expression has more than one column, we don't know the monotonicity.
        if visitor.columns.len() != 1 {
            return Ok((Monotonicity::default(), String::new()));
        }

        if visitor.mono.is_none() {
            return Ok((Monotonicity::default(), String::new()));
        }

        let column_name = visitor.columns.iter().next().unwrap().clone();
        let monotonicity = visitor.mono.unwrap();
        Ok((monotonicity, column_name))
    }

    /// Extract sort column from sort expression. It checks the monotonicity information
    /// of the sort expression (like f(x) = x+2 is a monotonic function), and extract the
    /// column information, returns as Expression::Column.
    pub fn extract_sort_column(
        sort_expr: &Expression,
        left: Option<DataColumnWithField>,
        right: Option<DataColumnWithField>,
    ) -> Result<Expression> {
        if let Expression::Sort {
            expr: _,
            asc,
            nulls_first,
            origin_expr,
        } = sort_expr
        {
            let visitor = MonotonicityCheckVisitor {
                mono: None,
                variable_left: left,
                variable_right: right,
                columns: HashSet::new(),
            };

            let visitor = origin_expr.accept(visitor)?;

            // The expression has more than one column, we don't know the monotonicity.
            // Skip the optimization, just return the input
            if visitor.columns.len() != 1 {
                return Ok(sort_expr.clone());
            }

            // not a monotonicity expression
            if visitor.mono.is_none() {
                return Ok(sort_expr.clone());
            }

            let mono = visitor.mono.unwrap();
            if !mono.is_monotonic {
                return Ok(sort_expr.clone());
            }

            let column_name = visitor.columns.iter().next().unwrap();

            // need to flip the asc when is_positive is false
            let new_asc = if mono.is_positive { *asc } else { !*asc };
            return Ok(Expression::Sort {
                expr: Box::new(col(column_name)),
                asc: new_asc,
                nulls_first: *nulls_first,
                origin_expr: origin_expr.clone(),
            });
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
                let mono_node = Monotonicity {
                    is_monotonic: true,
                    is_positive: true,
                    is_constant: false,
                    left: self.variable_left.clone(),
                    right: self.variable_right.clone(),
                };

                let mut new_columns: HashSet<String> = HashSet::new();
                new_columns.insert(col.clone());
                new_columns.extend(self.columns);

                Ok(MonotonicityCheckVisitor {
                    mono: Some(mono_node),
                    variable_left: self.variable_left,
                    variable_right: self.variable_right,
                    columns: new_columns,
                })
            }
            Expression::Literal {
                value,
                column_name,
                data_type,
            } => {
                let name = match column_name {
                    Some(n) => n.clone(),
                    None => format!("{}", value),
                };
                let data_field = DataField::new(&name, data_type.clone(), false);
                let data_column_field =
                    DataColumnWithField::new(DataColumn::Constant(value.clone(), 1), data_field);

                let mono_node = Monotonicity {
                    is_monotonic: true,
                    is_positive: true,
                    is_constant: true,
                    left: Some(data_column_field.clone()),
                    right: Some(data_column_field),
                };

                Ok(MonotonicityCheckVisitor {
                    mono: Some(mono_node),
                    variable_left: self.variable_left,
                    variable_right: self.variable_right,
                    columns: self.columns,
                })
            }
            _ => Ok(MonotonicityCheckVisitor::empty()),
        }
    }
}
