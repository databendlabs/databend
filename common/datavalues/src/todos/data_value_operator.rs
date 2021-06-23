// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

#[derive(Clone, Debug, PartialEq)]
pub enum DataValueAggregateOperator {
    Min,
    Max,
    Sum,
    Avg,
    Count,
}

impl std::fmt::Display for DataValueAggregateOperator {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        let display = match &self {
            DataValueAggregateOperator::Min => "min",
            DataValueAggregateOperator::Max => "max",
            DataValueAggregateOperator::Sum => "sum",
            DataValueAggregateOperator::Avg => "avg",
            DataValueAggregateOperator::Count => "count",
        };
        write!(f, "{}", display)
    }
}

#[derive(Clone)]
pub enum DataValueComparisonOperator {
    Eq,
    Lt,
    LtEq,
    Gt,
    GtEq,
    NotEq,
    Like,
    NotLike,
}

impl std::fmt::Display for DataValueComparisonOperator {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        let display = match &self {
            DataValueComparisonOperator::Eq => "=",
            DataValueComparisonOperator::Lt => "<",
            DataValueComparisonOperator::LtEq => "<=",
            DataValueComparisonOperator::Gt => ">",
            DataValueComparisonOperator::GtEq => ">=",
            DataValueComparisonOperator::NotEq => "!=",
            DataValueComparisonOperator::Like => "LIKE",
            DataValueComparisonOperator::NotLike => "NOT LIKE",
        };
        write!(f, "{}", display)
    }
}

#[derive(Clone, Debug)]
pub enum DataValueArithmeticOperator {
    Plus,
    Minus,
    Mul,
    Div,
    Modulo,
}

impl std::fmt::Display for DataValueArithmeticOperator {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        let display = match &self {
            DataValueArithmeticOperator::Plus => "plus",
            DataValueArithmeticOperator::Minus => "minus",
            DataValueArithmeticOperator::Mul => "multiply",
            DataValueArithmeticOperator::Div => "divide",
            DataValueArithmeticOperator::Modulo => "modulo",
        };
        write!(f, "{}", display)
    }
}

#[derive(Clone)]
pub enum DataValueLogicOperator {
    And,
    Or,
    Not,
}

impl std::fmt::Display for DataValueLogicOperator {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        let display = match &self {
            DataValueLogicOperator::And => "and",
            DataValueLogicOperator::Or => "or",
            DataValueLogicOperator::Not => "not",
        };
        write!(f, "{}", display)
    }
}
