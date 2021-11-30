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
use common_functions::scalars::Function;
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
    // the variable range left, we assume only one variable.
    pub variable_left: Option<DataColumnWithField>,

    // the variable range right, we assume only one variable.
    pub variable_right: Option<DataColumnWithField>,

    pub column_name: String,
}

impl MonotonicityCheckVisitor {
    fn try_create(
        variable_left: Option<DataColumnWithField>,
        variable_right: Option<DataColumnWithField>,
    ) -> Result<Self> {
        let column_name = match (variable_left.clone(), variable_right.clone()) {
            (Some(l), Some(r)) => {
                if l.field().name() != r.field().name() {
                    return Err(ErrorCode::MonotonicityCheckError(format!(
                        "variable_left and variable_right should have the same name, but got {} and {}",
                        l.field().name(),
                        r.field().name()
                    )));
                }
                l.field().name().clone()
            }
            (Some(l), None) => l.field().name().clone(),
            (None, Some(r)) => r.field().name().clone(),
            _ => String::new(),
        };

        Ok(Self {
            variable_left,
            variable_right,
            column_name,
        })
    }

    fn try_calculate_boundary(
        func: &dyn Function,
        expr_name: &str,
        args: Vec<Option<DataColumnWithField>>,
    ) -> Result<Option<DataColumnWithField>> {
        let res = if args.iter().any(|col| col.is_none()) {
            Ok(None)
        } else {
            let input_columns = args
                .into_iter()
                .map(|col_opt| {
                    col_opt.ok_or_else(|| {
                        ErrorCode::UnknownException("Unable to get DataColumnWithField")
                    })
                })
                .collect::<Result<Vec<_>>>()?;

            let col = func.eval(input_columns.as_ref(), 1)?;
            let data_field = DataField::new(expr_name, col.data_type(), false);
            let data_column_field = DataColumnWithField::new(col, data_field);
            Ok(Some(data_column_field))
        };
        res
    }

    fn visit(&mut self, expr: &Expression) -> Result<Monotonicity> {
        match expr {
            Expression::ScalarFunction { op, args } => {
                self.visit_func_args(op, &expr.column_name(), args)
            }
            Expression::BinaryExpression { op, left, right } => {
                self.visit_func_args(op, &expr.column_name(), &[*left.clone(), *right.clone()])
            }
            Expression::UnaryExpression { op, expr } => {
                self.visit_func_args(op, &expr.column_name(), &[*expr.clone()])
            }
            Expression::Column(col) => {
                if self.column_name.is_empty() {
                    self.column_name = col.clone();
                } else if self.column_name != *col {
                    return Err(ErrorCode::BadArguments(format!(
                        "expect column name {:?}, get {:?}",
                        self.column_name, col
                    )));
                }

                Ok(Monotonicity {
                    is_monotonic: true,
                    is_positive: true,
                    is_constant: false,
                    left: self.variable_left.clone(),
                    right: self.variable_right.clone(),
                })
            }
            Expression::Literal {
                value,
                column_name,
                data_type,
            } => {
                let name = column_name.clone().unwrap_or(format!("{}", value));
                let data_field = DataField::new(&name, data_type.clone(), false);
                let data_column_field =
                    DataColumnWithField::new(DataColumn::Constant(value.clone(), 1), data_field);

                Ok(Monotonicity {
                    is_monotonic: true,
                    is_positive: true,
                    is_constant: true,
                    left: Some(data_column_field.clone()),
                    right: Some(data_column_field),
                })
            }
            _ => Err(ErrorCode::UnknownException("Unable to get monotonicity")),
        }
    }

    fn visit_func_args(
        &mut self,
        op: &str,
        expr_name: &str,
        children: &[Expression],
    ) -> Result<Monotonicity> {
        let mut monotonicity_vec = vec![];
        let mut left_vec = vec![];
        let mut right_vec = vec![];

        for child_expr in children.iter() {
            let monotonicity = self.visit(child_expr)?;
            if !monotonicity.is_monotonic {
                return Ok(Monotonicity::default());
            }

            left_vec.push(monotonicity.left.clone());
            right_vec.push(monotonicity.right.clone());
            monotonicity_vec.push(monotonicity);
        }

        let func = FunctionFactory::instance().get(op)?;
        let mut root_mono = func.get_monotonicity(monotonicity_vec.as_ref())?;

        // neither a monotonic expression nor constant, no need to calculating boundary
        if !root_mono.is_monotonic && !root_mono.is_constant {
            return Ok(Monotonicity::default());
        }

        root_mono.left = Self::try_calculate_boundary(func.as_ref(), expr_name, left_vec)?;
        root_mono.right = Self::try_calculate_boundary(func.as_ref(), expr_name, right_vec)?;

        Ok(root_mono)
    }

    /// Check whether the expression is monotonic or not. The left should be <= right.
    /// Return the monotonicity information, together with column name if any.
    pub fn check_expression(
        expr: &Expression,
        left: Option<DataColumnWithField>,
        right: Option<DataColumnWithField>,
    ) -> Result<Monotonicity> {
        let mut visitor = Self::try_create(left, right)?;
        visitor.visit(expr)
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
            let mut visitor = Self::try_create(left, right)?;
            let mono = visitor
                .visit(origin_expr)
                .unwrap_or_else(|_| Monotonicity::default());
            if !mono.is_monotonic {
                return Ok(sort_expr.clone());
            }

            // need to flip the asc when is_positive is false
            let new_asc = if mono.is_positive { *asc } else { !*asc };
            return Ok(Expression::Sort {
                expr: Box::new(col(&visitor.column_name)),
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
