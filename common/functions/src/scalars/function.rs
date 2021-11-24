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

use std::fmt;

use common_datavalues::columns::DataColumn;
use common_datavalues::prelude::DataColumnWithField;
use common_datavalues::prelude::DataColumnsWithField;
use common_datavalues::DataSchema;
use common_datavalues::DataType;
use common_exception::Result;
use dyn_clone::DynClone;

#[derive(Clone)]
pub struct Monotonicity {
    // Is the function monotonic (non-decreasing or non-increasing).
    pub is_monotonic: bool,

    // Field for indicating monotonic increase or decrease
    //   1. is_positive=true means non-decreasing
    //   2. is_positive=false means non-increasing
    // when is_monotonic is false, just ignore the is_positive information.
    pub is_positive: bool,

    // Is the monotonicity from constant value
    pub is_constant: bool,

    pub left: Option<DataColumnWithField>,

    pub right: Option<DataColumnWithField>,
}

impl Monotonicity {
    pub fn default() -> Self {
        Monotonicity {
            is_monotonic: false,
            is_positive: true,
            is_constant: false,
            left: None,
            right: None,
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

    /// Calculate the monotonicity from arguments' monotonicity information.
    /// The input should be argument's monotonicity. For binary function it should be an array of left expression's monotonicity and right expression's monotonicity.
    /// For unary function, the input should be an array of the only argument's monotonicity.
    fn get_monotonicity(&self, _args: &[Monotonicity]) -> Result<Monotonicity> {
        Ok(Monotonicity::default())
    }

    fn return_type(&self, args: &[DataType]) -> Result<DataType>;
    fn nullable(&self, _input_schema: &DataSchema) -> Result<bool>;
    fn eval(&self, columns: &DataColumnsWithField, _input_rows: usize) -> Result<DataColumn>;
}
