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

use std::fmt;

use common_datavalues::columns::DataColumn;
use common_datavalues::prelude::DataColumnWithField;
use common_datavalues::prelude::DataColumnsWithField;
use common_datavalues::DataField;
use common_datavalues::DataSchema;
use common_datavalues::DataType;
use common_datavalues::DataValue;
use common_exception::Result;
use dyn_clone::DynClone;

use super::ComparisonGtEqFunction;
use super::ComparisonLtEqFunction;

// Represents the range of data column for monotonic function calculation.
// For example, f(x) = x+2 where x > 10 should have variable range [10, MAX), and f(x) should
// have range [12, MAX). The range calculation from [10, MAX) to [12, MAX) should rely on the
// function's eval method.
#[derive(Clone)]
pub struct Range {
    pub begin: Option<DataColumnWithField>, // should have constant DataValue
    pub end: Option<DataColumnWithField>,   // should have constant DataValue
}

impl Range {
    /// Check whether the range is greater-equal than zero.
    /// The function will compare the 'begin' field in the range with zero, return true if begin >= 0.
    pub fn gt_eq_zero(&self) -> Result<bool> {
        if self.begin.is_none() {
            return Ok(false);
        }

        let zero = DataColumn::Constant(DataValue::Int8(Some(0)), 1);
        let zero_column_field =
            DataColumnWithField::new(zero, DataField::new("", DataType::Int8, false));

        let min_val = self.begin.clone().unwrap();
        let f = ComparisonGtEqFunction::try_create_func("")?;
        let res = f.eval(&[min_val, zero_column_field], 1)?;
        let res_value = res.try_get(0)?;
        res_value.as_bool()
    }

    /// Check whether the range is less-equal than zero.
    /// The function will compare the 'end' field in the range with zero, return true if end <= 0.
    pub fn lt_eq_zero(&self) -> Result<bool> {
        if self.end.is_none() {
            return Ok(false);
        }

        let zero = DataColumn::Constant(DataValue::Int8(Some(0)), 1);
        let zero_column_field =
            DataColumnWithField::new(zero, DataField::new("", DataType::Int8, false));

        let max_val = self.end.clone().unwrap();
        let f = ComparisonLtEqFunction::try_create_func("")?;
        let res = f.eval(&[max_val, zero_column_field], 1)?;
        let res_value = res.try_get(0)?;
        res_value.as_bool()
    }
}

// Represents the node of function tree for calculating monotonicity.
// For example, a function of Add(Neg(number), 5) for number < -100 will have a tree like this:
//
// .                   MonotonicityNode::Function -- 'Add'
//                      (mono: is_positive=true, Range{105, MAX})
//                         /                          \
//                        /                            \
//      MonotonicityNode::Function -- 'Neg'         Monotonicity::Constant -- 5
//    (mono: is_positive=true, range{100, MAX})
//                     /
//                    /
//     MonotonicityNode::Variable number -- 'number'
//         (range{MIN, -100})
//
// The structure of the tree is basically the structure of the expression.
// Simple depth first search visit the expression tree and gete monotonicity from
// every function. Each function is responsible to implement its own monotonicity
// function.
// Notice!! the mechanism doesn't solve multiple variables case.
#[derive(Clone)]
pub enum MonotonicityNode {
    // Describe a function monotonicity information for the range of data.
    Function(Monotonicity, Range),
    Constant(DataColumnWithField),
    Variable(String, Range),
}

#[derive(Clone)]
pub struct Monotonicity {
    // Is the function monotonous (non-decreasing or non-increasing).
    pub is_monotonic: bool,
    // true if the function is non-decreasing, false, if non-increasing. If is_monotonic = false, then it does not matter.
    pub is_positive: bool,
}

impl Monotonicity {
    pub fn default() -> Self {
        Monotonicity {
            // Field for indicating whether the function is monotonic in the data range
            is_monotonic: false,

            // Field for indicating monotonic increase or decrease
            //   1. is_positive=true means non-decreasing
            //   2. is_positive=false means non-increasing
            // when is_monotonic is false, just ignore ths is_positive information.
            is_positive: true,
        }
    }

    pub fn flip_clone(&self) -> Self {
        Self {
            is_monotonic: self.is_monotonic,
            is_positive: !self.is_positive,
        }
    }
}

pub trait Function: fmt::Display + Sync + Send + DynClone {
    fn name(&self) -> &str;

    fn num_arguments(&self) -> usize {
        0
    }

    // (1, 2) means we only accept [1, 2] arguments
    // None means it's not variadic function
    fn variadic_arguments(&self) -> Option<(usize, usize)> {
        None
    }

    // return monotonicity node, should always return MonotonicityNode::Function
    fn get_monotonicity(&self, _args: &[MonotonicityNode]) -> Result<MonotonicityNode> {
        Ok(MonotonicityNode::Function(Monotonicity::default(), Range {
            begin: None,
            end: None,
        }))
    }

    fn return_type(&self, args: &[DataType]) -> Result<DataType>;
    fn nullable(&self, _input_schema: &DataSchema) -> Result<bool>;
    fn eval(&self, columns: &DataColumnsWithField, _input_rows: usize) -> Result<DataColumn>;
}
