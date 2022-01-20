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

use std::collections::HashMap;

use common_datablocks::DataBlock;
use common_datavalues::DataValue;
use common_exception::Result;
use common_planners::Expression;

use crate::storages::index::IndexSchemaVersion;

#[derive(Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct SparseIndexValue {
    // Min value of this granule.
    pub min: DataValue,
    // Max value of this granule.
    pub max: DataValue,
    // The page number to read in the data file.
    pub page_no: i64,
}

/// Sparse index.
#[derive(Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct SparseIndex {
    pub col: String,
    // Sparse index.
    pub values: Vec<SparseIndexValue>,
    // Version.
    pub version: IndexSchemaVersion,
}

#[allow(dead_code)]
impl SparseIndex {
    fn create(col: String) -> Self {
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

    pub fn create_index(keys: &[String], blocks: &[DataBlock]) -> Result<Vec<SparseIndex>> {
        let mut keys_idx = vec![];

        for key in keys {
            let mut sparse = SparseIndex::create(key.clone());
            for (page_no, page) in blocks.iter().enumerate() {
                let min = page.first(key.as_str())?;
                let max = page.last(key.as_str())?;
                sparse.push(SparseIndexValue {
                    min,
                    max,
                    page_no: page_no as i64,
                })?;
            }
            keys_idx.push(sparse);
        }
        Ok(keys_idx)
    }

    /// Apply the index and get the result:
    /// (true, ...) : need read the whole file
    /// (false, [0, 3]) : need to read the page-0 and page-3 only.
    pub fn apply_index(
        _idx_map: HashMap<String, SparseIndex>,
        _expr: &Expression,
    ) -> Result<(bool, Vec<i64>)> {
        // TODO(bohu): expression check.
        Ok((true, vec![]))
    }
}
