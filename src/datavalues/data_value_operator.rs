// Copyright 2020 The FuseQuery Authors.
//
// Code is licensed under AGPL License, Version 3.0.

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
    // /// Matches a wildcard pattern
    // Like,
    // /// Does not match a wildcard pattern
    // NotLike,
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
            // DataValueComparisonOperator::Like => "LIKE",
            // DataValueComparisonOperator::NotLike => "NOT LIKE",
        };
        write!(f, "{}", display)
    }
}

#[derive(Clone)]
pub enum DataValueArithmeticOperator {
    Add,
    Sub,
    Mul,
    Div,
}

impl std::fmt::Display for DataValueArithmeticOperator {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        let display = match &self {
            DataValueArithmeticOperator::Add => "+",
            DataValueArithmeticOperator::Sub => "-",
            DataValueArithmeticOperator::Mul => "*",
            DataValueArithmeticOperator::Div => "/",
        };
        write!(f, "{}", display)
    }
}

#[derive(Clone)]
pub enum DataValueLogicOperator {
    And,
    Or,
}

impl std::fmt::Display for DataValueLogicOperator {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        let display = match &self {
            DataValueLogicOperator::And => "and",
            DataValueLogicOperator::Or => "or",
        };
        write!(f, "{}", display)
    }
}
