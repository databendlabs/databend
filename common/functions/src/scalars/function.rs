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
use common_datavalues::DataSchema;
use common_datavalues::DataType;
use common_datavalues::DataValue;
use common_exception::Result;
use dyn_clone::DynClone;

// Represents the range of data column for monotonic function calculation.
// For example, f(x) = x+2 where x > 10 should have variable range [10, MAX), and f(x) should
// have range [12, MAX). The range calculation from [10, MAX) to [12, MAX) should rely on the
// function's eval method.
#[derive(Clone)]
pub struct Range {
    pub begin: DataColumnWithField, // should have constant DataValue
    pub end: DataColumnWithField,   // should have constant DataValue
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
    Function(Monotonicity, Option<Range>),
    Constant(DataValue),
    Variable(String, Option<Range>),
}

#[derive(Clone)]
pub struct Monotonicity {
    // Is the function monotonous (nondecreasing or nonincreasing).
    pub is_monotonic: bool,
    // true if the function is nondecreasing, false, if notincreasing. If is_monotonic = false, then it does not matter.
    pub is_positive: bool,
    // Is true if function is monotonic on the whole input range.
    pub is_always_monotonic: bool,
}

impl Monotonicity {
    pub fn default() -> Self {
        Monotonicity {
            // Field for indicating whether the function is monotonic in the data range
            is_monotonic: false,

            // Field for indicating monotonic increase or decrease
            //   1. is_positive=true means non-decreasing
            //   2. is_positive=false means non-increasing
            // when is_monotonic and is_always_monotonic are both false, just ignore ths is_positive information.
            is_positive: true,

            // Field for indicating whether the function is always monotonic regardless of data rage.
            // For example, f(x) = x+5 should have is_always_monotonic = true. When this field is true,
            // is_monotonic MUST also be true.
            is_always_monotonic: false,
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
        Ok(MonotonicityNode::Function(Monotonicity::default(), None))
    }

    fn return_type(&self, args: &[DataType]) -> Result<DataType>;
    fn nullable(&self, _input_schema: &DataSchema) -> Result<bool>;
    fn eval(&self, columns: &DataColumnsWithField, _input_rows: usize) -> Result<DataColumn>;
}
