// Copyright 2020 Datafuse Labs.
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

use common_datavalues::DataType;
use common_datavalues::DataValue;

use crate::sql::parser::ast::BinaryOperator;
use crate::sql::parser::ast::UnaryOperator;
use crate::sql::planner::ColumnBinding;
use crate::sql::planner::Logical;

#[derive(Debug, Clone)]
pub enum Expression {
    // Column reference
    ColumnRef {
        name: String,
        binding: ColumnBinding,
    },
    // Constant value.
    // Note: When literal represents a column, its column_name will not be None
    Literal {
        value: DataValue,
    },
    // A unary expression such as "NOT foo"
    UnaryExpression {
        op: UnaryOperator,
        expr: Box<Expression>,
    },

    // A binary expression such as "age > 40"
    BinaryExpression {
        left: Box<Expression>,
        op: BinaryOperator,
        right: Box<Expression>,
    },

    // ScalarFunction with a set of arguments.
    // Note: BinaryFunction is a also kind of functions function
    ScalarFunction {
        op: String,
        args: Vec<Expression>,
    },

    // AggregateFunction with a set of arguments.
    AggregateFunction {
        op: String,
        distinct: bool,
        args: Vec<Expression>,
    },

    // Casts the expression to a given type and will return a runtime error if the expression cannot be cast.
    // This expression is guaranteed to have a fixed type.
    Cast {
        /// The expression being cast
        expr: Box<Expression>,
        /// The `DataType` the expression will yield
        data_type: DataType,
    },
    Subquery {
        tp: SubqueryType,
        subquery: Box<Logical>,
    },
}

#[derive(Debug, Clone, PartialEq)]
pub enum SubqueryType {
    In,
    Exists,
    Scalar,
}

impl Expression {
    // Split predicate expression(CNF expression) into conjunctions
    pub fn split_predicates(expr: Self) -> Vec<Self> {
        todo!()
    }

    // Get return type of Expression
    pub fn data_type(&self) -> DataType {
        todo!()
    }

    pub fn get_children(&self) -> Vec<Self> {
        match self {
            Expression::ColumnRef { .. } => vec![],
            Expression::Literal { .. } => vec![],
            Expression::UnaryExpression { expr, .. } => vec![*expr.clone()],
            Expression::BinaryExpression { left, right, .. } => {
                vec![*left.clone(), *right.clone()]
            }
            Expression::ScalarFunction { args, .. } => args.to_owned(),
            Expression::AggregateFunction { args, .. } => args.to_owned(),
            Expression::Cast { expr, .. } => vec![*expr.clone()],
            _ => todo!(),
        }
    }

    pub fn get_column_bindings(&self) -> HashSet<ColumnBinding> {
        match self {
            Expression::ColumnRef { binding, .. } => [binding.to_owned()].into_iter().collect(),
            _ => self
                .get_children()
                .into_iter()
                .map(|expr| expr.get_column_bindings())
                .flatten()
                .collect(),
        }
    }

    pub fn is_column_ref(&self) -> bool {
        match self {
            Self::ColumnRef { .. } => true,
            _ => false,
        }
    }

    pub fn is_comparison(&self) -> bool {
        match self {
            Self::BinaryExpression { op, .. } => match op {
                BinaryOperator::Eq
                | BinaryOperator::NotEq
                | BinaryOperator::Lt
                | BinaryOperator::Lte
                | BinaryOperator::Gt
                | BinaryOperator::Gte => true,
                _ => false,
            },
            _ => false,
        }
    }

    pub fn is_equal(&self) -> bool {
        match self {
            Self::BinaryExpression { op, .. } => *op == BinaryOperator::Eq,
            _ => false,
        }
    }

    pub fn is_constant(&self) -> bool {
        self.get_column_bindings().is_empty()
    }

    pub fn is_aggregate(&self) -> bool {
        // TODO: recursive check, since `COUNT(a) + COUNT(b)` is still aggregate
        match self {
            Expression::AggregateFunction { .. } => true,
            _ => false,
        }
    }

    pub fn get_aggregate_functions(&self) -> Vec<Expression> {
        match self {
            Expression::AggregateFunction { .. } => vec![self.to_owned()],
            _ => self
                .get_children()
                .iter()
                .map(|expr| expr.get_aggregate_functions())
                .flatten()
                .collect(),
        }
    }
}
