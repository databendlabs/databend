// Copyright 2021 Datafuse Labs
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

use common_expression::DataBlock;
use pyo3::prelude::*;

#[pyclass]

pub struct Block(pub DataBlock);

#[pymethods]
impl Block {
    fn __repr__(&self, _py: Python) -> PyResult<String> {
        Ok(self.0.to_string())
    }

    fn show(&self) {
        println!("{}", self.0);
    }

    fn num_rows(&self) -> usize {
        self.0.num_rows()
    }

    fn num_columns(&self) -> usize {
        self.0.num_columns()
    }
}
