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

/// Min and Max index.
#[derive(Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct MinMaxIndex {
    pub col: String,
    pub min: DataValue,
    pub max: DataValue,
    pub version: IndexSchemaVersion,
}

#[allow(dead_code)]
impl MinMaxIndex {
    fn create(col: String, min: DataValue, max: DataValue) -> Self {
        MinMaxIndex {
            col,
            min,
            max,
            version: IndexSchemaVersion::V1,
        }
    }

    pub fn typ(&self) -> &str {
        "min_max"
    }

    /// Create index for one parquet file.
    /// All blocks are belong to a Parquet file and globally sorted.
    /// Each block is one row group of a parquet file and sorted by primary key.
    /// For example:
    /// parquet.file
    /// | sorted-block | sorted-block | ... |
    pub fn create_index(keys: &[String], blocks: &[DataBlock]) -> Result<Vec<MinMaxIndex>> {
        let first = 0;
        let last = blocks.len() - 1;
        let mut keys_idx = vec![];

        for key in keys {
            let min = blocks[first].first(key)?;
            let max = blocks[last].last(key)?;
            let min_max = MinMaxIndex::create(key.clone(), min, max);
            keys_idx.push(min_max);
        }
        Ok(keys_idx)
    }

    /// Apply the expr against the idx_map, and get the result:
    /// true: need
    /// false: skip
    pub fn apply_index(_idx_map: HashMap<String, MinMaxIndex>, _expr: &Expression) -> Result<bool> {
        // TODO(bohu): expression apply.
        Ok(true)
    }
}
