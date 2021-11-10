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
use common_datavalues::prelude::DataColumnsWithField;
use common_datavalues::DataSchema;
use common_datavalues::DataType;
use common_datavalues::DataValue;
use common_exception::Result;
use dyn_clone::DynClone;

// Represents the node of function tree for calculating monotonicity.
// For example, a function of Add(Neg(number), 5) will have a tree like this:
//
// .                   MonotonicityNode::Function --> Add
//                         /                       \
//                        /                         \
//      MonotonicityNode::Function --> Neg        Monotonicity::Constant --> 5
//                     /
//                    /
//     MonotonicityNode::Variable --> number
//
// The structure of the tree is basically the structure of the expression.
// Simple depth first search visit the expression tree and gete monotonicity from
// every function. Each function is responsible to implement its own monotonicity
// function.
// Notice!! the mechanism doesn't solve multiple variables case.
#[derive(Clone)]
pub enum MonotonicityNode {
    Function(Monotonicity),
    Constant(DataValue),
    Variable(String),
}

#[derive(Clone)]
pub struct Monotonicity {
    // Is the function monotonous (nondecreasing or nonincreasing).
    pub is_monotonic: bool,
    // true if the function is nondecreasing, false, if notincreasing. If is_monotonic = false, then it does not matter.
    pub is_positive: bool,
    // Is true if function is monotonic on the whole input range
    pub is_always_monotonic: bool,
}

impl Monotonicity {
    pub fn default() -> Self {
        Monotonicity {
            is_monotonic: false,
            is_positive: false,
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
        Ok(MonotonicityNode::Function(Monotonicity::default()))
    }

    fn return_type(&self, args: &[DataType]) -> Result<DataType>;
    fn nullable(&self, _input_schema: &DataSchema) -> Result<bool>;
    fn eval(&self, columns: &DataColumnsWithField, _input_rows: usize) -> Result<DataColumn>;
}
