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
use common_datavalues::prelude::DataColumnsWithField;
use common_datavalues::DataSchema;
use common_datavalues::DataType;
use common_exception::Result;
use dyn_clone::DynClone;

use crate::scalars::Monotonicity;

pub trait Function: fmt::Display + Sync + Send + DynClone {
    /// Returns the name of the function, should be unique.
    fn name(&self) -> &str;

    /// Calculate the monotonicity from arguments' monotonicity information.
    /// The input should be argument's monotonicity. For binary function it should be an
    /// array of left expression's monotonicity and right expression's monotonicity.
    /// For unary function, the input should be an array of the only argument's monotonicity.
    /// The returned monotonicity should have 'left' and 'right' fields None -- the boundary
    /// calculation relies on the function.eval method.
    fn get_monotonicity(&self, _args: &[Monotonicity]) -> Result<Monotonicity> {
        Ok(Monotonicity::default())
    }

    /// The method returns the return_type of this function.
    fn return_type(&self, args: &[DataType]) -> Result<DataType>;

    /// Whether the function may return null with specific input schema.
    /// The default implementation check whether any nullable input exists.
    /// If yes, return true; otherwise false.
    fn nullable(&self, input_schema: &DataSchema) -> Result<bool> {
        let any_input_nullable = input_schema
            .fields()
            .iter()
            .any(|field| field.is_nullable());
        Ok(any_input_nullable)
    }

    /// Evaluate the function, e.g. run/execute the function.
    fn eval(&self, _columns: &DataColumnsWithField, _input_rows: usize) -> Result<DataColumn>;

    /// Whether the function passes through null input.
    /// Return true is the function just return null with any given null input.
    /// Return false if the function may return non-null with null input.
    ///
    /// For example, arithmetic plus('+') will output null for any null input, like '12 + null = null'.
    /// It has no idea of how to handle null, but just pass through.
    ///
    /// While ISNULL function  treats null input as a valid one. For example ISNULL(NULL, 'test') will return 'test'.
    fn passthrough_null(&self) -> bool {
        true
    }
}

dyn_clone::clone_trait_object!(Function);
