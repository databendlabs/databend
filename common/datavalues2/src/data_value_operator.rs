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
    Regexp,
    NotRegexp,
    RLike,
    NotRLike,
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
            DataValueComparisonOperator::Regexp => "REGEXP",
            DataValueComparisonOperator::NotRegexp => "NOT REGEXP",
            DataValueComparisonOperator::RLike => "RLIKE",
            DataValueComparisonOperator::NotRLike => "NOT RLIKE",
        };
        write!(f, "{}", display)
    }
}

#[derive(Clone, Debug)]
pub enum DataValueBinaryOperator {
    Plus,
    Minus,
    Mul,
    Div,
    IntDiv,
    Modulo,
}

impl std::fmt::Display for DataValueBinaryOperator {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        let display = match &self {
            DataValueBinaryOperator::Plus => "plus",
            DataValueBinaryOperator::Minus => "minus",
            DataValueBinaryOperator::Mul => "multiply",
            DataValueBinaryOperator::Div => "divide",
            DataValueBinaryOperator::IntDiv => "div",
            DataValueBinaryOperator::Modulo => "modulo",
        };
        write!(f, "{}", display)
    }
}

#[derive(Clone, Debug)]
pub enum DataValueUnaryOperator {
    Negate,
}

impl std::fmt::Display for DataValueUnaryOperator {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        let display = match &self {
            DataValueUnaryOperator::Negate => "negate",
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
