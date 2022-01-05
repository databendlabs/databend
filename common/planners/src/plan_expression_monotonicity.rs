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

use std::collections::HashMap;

use common_datavalues::prelude::DataColumn;
use common_datavalues::prelude::DataColumnWithField;
use common_datavalues::DataField;
use common_datavalues::DataSchemaRef;
use common_datavalues::DataType;
use common_datavalues::DataTypeAndNullable;
use common_exception::ErrorCode;
use common_exception::Result;
use common_functions::scalars::Function;
use common_functions::scalars::FunctionFactory;
use common_functions::scalars::Monotonicity;

use crate::col;
use crate::Expression;
use crate::ExpressionVisitor;
use crate::Recursion;

// ExpressionMonotonicityVisitor visit the expression tree to calculate monotonicity.
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
pub struct ExpressionMonotonicityVisitor {
    input_schema: DataSchemaRef,

    // HashMap<column_name, (variable_left, variable_right)>
    // variable_left: the variable range left.
    // variable_right: the variable range right.
    variables: HashMap<String, (Option<DataColumnWithField>, Option<DataColumnWithField>)>,

    stack: Vec<(DataTypeAndNullable, Monotonicity)>,

    single_point: bool,
}

impl ExpressionMonotonicityVisitor {
    fn create(
        input_schema: DataSchemaRef,
        variables: HashMap<String, (Option<DataColumnWithField>, Option<DataColumnWithField>)>,
        single_point: bool,
    ) -> Self {
        Self {
            input_schema,
            variables,
            stack: vec![],
            single_point,
        }
    }

    pub fn finalize(mut self) -> Result<Monotonicity> {
        match self.stack.len() {
            1 => {
                let (_, monotonic) = self.stack.remove(0);
                Ok(monotonic)
            }
            _ => Err(ErrorCode::LogicalError(
                "Stack has too many elements in ExpressionMonotonicityVisitor::finalize",
            )),
        }
    }

    fn try_calculate_boundary(
        func: &dyn Function,
        result_type: &DataType,
        args: Vec<Option<DataColumnWithField>>,
    ) -> Result<Option<DataColumnWithField>> {
        let res = if args.iter().any(|col| col.is_none()) {
            Ok(None)
        } else {
            let input_columns = args
                .into_iter()
                .map(|col_opt| col_opt.unwrap())
                .collect::<Vec<_>>();

            let col = func.eval(input_columns.as_ref(), 1)?;
            let data_field = DataField::new("dummy", result_type.clone(), false);
            let data_column_field = DataColumnWithField::new(col, data_field);
            Ok(Some(data_column_field))
        };
        res
    }

    fn visit_function(mut self, op: &str, args_size: usize) -> Result<Self> {
        let mut left_vec = Vec::with_capacity(args_size);
        let mut right_vec = Vec::with_capacity(args_size);
        let mut args_type = Vec::with_capacity(args_size);
        let mut monotonicity_vec = Vec::with_capacity(args_size);

        for index in 0..args_size {
            match self.stack.pop() {
                None => {
                    return Err(ErrorCode::LogicalError(format!(
                        "Expected {} arguments, actual {}.",
                        args_size, index
                    )))
                }
                Some((arg_type, monotonic)) => {
                    left_vec.push(monotonic.left.clone());
                    right_vec.push(monotonic.right.clone());
                    args_type.push(arg_type);
                    monotonicity_vec.push(monotonic);
                }
            }
        }

        let func = FunctionFactory::instance().get(op, &args_type)?;

        let return_type = func.return_type(&args_type)?;

        let mut monotonic = match self.single_point {
            false => func.get_monotonicity(monotonicity_vec.as_ref())?,
            true => Monotonicity::create_constant(),
        };

        // Neither a monotonic expression nor constant, no need to calculating boundary.
        // The boundary may have been calculated during get_monotonicity.
        if (monotonic.is_monotonic || monotonic.is_constant) && monotonic.left.is_none() {
            monotonic.left =
                Self::try_calculate_boundary(func.as_ref(), return_type.data_type(), left_vec)?;
            monotonic.right =
                Self::try_calculate_boundary(func.as_ref(), return_type.data_type(), right_vec)?;
        }

        self.stack.push((return_type, monotonic));
        Ok(self)
    }

    /// Check whether the expression is monotonic or not. The left should be <= right.
    /// Return the monotonicity information, together with column name if any.
    pub fn check_expression(
        schema: DataSchemaRef,
        expr: &Expression,
        variables: HashMap<String, (Option<DataColumnWithField>, Option<DataColumnWithField>)>,
        single_point: bool,
    ) -> Result<Monotonicity> {
        let visitor = Self::create(schema, variables, single_point);
        visitor.visit(expr)?.finalize()
    }

    /// Extract sort column from sort expression. It checks the monotonicity information
    /// of the sort expression (like f(x) = x+2 is a monotonic function), and extract the
    /// column information, returns as Expression::Column.
    pub fn extract_sort_column(
        schema: DataSchemaRef,
        sort_expr: &Expression,
        column_name: &str,
    ) -> Result<Expression> {
        if let Expression::Sort {
            expr: _,
            asc,
            nulls_first,
            origin_expr,
        } = sort_expr
        {
            let mut variables = HashMap::new();
            variables.insert(column_name.to_owned(), (None, None));
            let mono = Self::check_expression(schema, origin_expr, variables, false)
                .unwrap_or_else(|_| Monotonicity::default());
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
        Err(ErrorCode::BadArguments(format!(
            "expect sort expression, get {:?}",
            sort_expr
        )))
    }
}

impl ExpressionVisitor for ExpressionMonotonicityVisitor {
    fn pre_visit(self, _expr: &Expression) -> Result<Recursion<Self>> {
        Ok(Recursion::Continue(self))
    }

    fn post_visit(mut self, expr: &Expression) -> Result<Self> {
        match expr {
            Expression::Column(s) => {
                let (left, right) = self.variables.get(&*s).ok_or_else(|| {
                    ErrorCode::BadArguments(format!("Cannot find the column name '{:?}'", *s))
                })?;

                let field = self.input_schema.field_with_name(s)?;
                let return_type =
                    DataTypeAndNullable::create(field.data_type(), field.is_nullable());

                let monotonic = Monotonicity {
                    is_monotonic: true,
                    is_positive: true,
                    is_constant: false,
                    left: left.clone(),
                    right: right.clone(),
                };

                self.stack.push((return_type, monotonic));
                Ok(self)
            }
            Expression::Literal {
                value,
                column_name,
                data_type,
            } => {
                let return_type = DataTypeAndNullable::create(data_type, true);

                let name = column_name.clone().unwrap_or(format!("{}", value));
                let data_field = DataField::new(&name, data_type.clone(), false);
                let data_column_field =
                    DataColumnWithField::new(DataColumn::Constant(value.clone(), 1), data_field);
                let monotonic = Monotonicity {
                    is_monotonic: true,
                    is_positive: true,
                    is_constant: true,
                    left: Some(data_column_field.clone()),
                    right: Some(data_column_field),
                };

                self.stack.push((return_type, monotonic));
                Ok(self)
            }
            Expression::BinaryExpression { op, .. } => self.visit_function(op, 2),
            Expression::UnaryExpression { op, .. } => self.visit_function(op, 1),
            Expression::ScalarFunction { op, args } => self.visit_function(op, args.len()),
            // Todo: Expression::Cast
            _ => Err(ErrorCode::UnknownException("Unable to get monotonicity")),
        }
    }
}
