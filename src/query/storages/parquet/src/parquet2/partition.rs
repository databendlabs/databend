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

use std::collections::HashMap;

use databend_common_arrow::parquet::compression::Compression;
use databend_common_arrow::parquet::indexes::Interval;
use databend_common_expression::FieldIndex;
use databend_common_expression::Scalar;

#[derive(serde::Serialize, serde::Deserialize, PartialEq, Eq, Clone, Debug)]
pub struct ColumnMeta {
    pub offset: u64,
    pub length: u64,
    pub num_values: i64,
    pub compression: Compression,
    pub uncompressed_size: u64,
    pub min_max: Option<(Scalar, Scalar)>,

    // if has dictionary, we can not push down predicate to deserialization.
    pub has_dictionary: bool,
}

#[derive(serde::Serialize, serde::Deserialize, PartialEq, Eq, Debug, Clone)]
pub struct Parquet2RowGroupPart {
    pub location: String,
    pub num_rows: usize,
    pub column_metas: HashMap<FieldIndex, ColumnMeta>,
    pub row_selection: Option<Vec<Interval>>,

    pub sort_min_max: Option<(Scalar, Scalar)>,
}

impl Parquet2RowGroupPart {
    pub fn uncompressed_size(&self) -> u64 {
        self.column_metas
            .values()
            .map(|c| c.uncompressed_size)
            .sum()
    }

    pub fn compressed_size(&self) -> u64 {
        self.column_metas.values().map(|c| c.length).sum()
    }
}
