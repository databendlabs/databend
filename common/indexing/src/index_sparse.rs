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
//

use common_datavalues::DataValue;
use common_exception::Result;

pub struct SparseIndexValue {
    // The sparse key.
    pub key: DataValue,
    // The page number to read in the data file.
    // If the page is None, we will read the whole file.
    pub page: Option<i64>,
}

/// Sparse index.
pub struct SparseIndex {
    pub file_name: String,
    pub values: Vec<SparseIndexValue>,
}

impl SparseIndex {
    pub fn create(file_name: String) -> Self {
        SparseIndex {
            file_name,
            values: vec![],
        }
    }

    pub fn typ(&self) -> &str {
        "sparse"
    }

    // Push one sparse value to the sparse index.
    pub fn push(&mut self, val: SparseIndexValue) -> Result<()> {
        self.values.push(val);
        Ok(())
    }
}
