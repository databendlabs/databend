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

use crate::IndexSchemaVersion;

#[derive(Debug, PartialEq)]
pub struct SparseIndexValue {
    // Min value of this granule.
    pub min: DataValue,
    // Max value of this granule.
    pub max: DataValue,
    // The page number to read in the data file.
    // If the page is None, we will read the whole file.
    pub page_no: Option<i64>,
}

/// Sparse index.
#[derive(Debug, PartialEq)]
pub struct SparseIndex {
    pub col: String,
    // Sparse index.
    pub values: Vec<SparseIndexValue>,
    // Version.
    pub version: IndexSchemaVersion,
}

impl SparseIndex {
    pub fn create(col: String) -> Self {
        SparseIndex {
            col,
            values: vec![],
            version: IndexSchemaVersion::V1,
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
