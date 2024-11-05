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

use databend_common_storage::Datum;
use databend_common_storage::Histogram;

use crate::IndexType;

pub type ColumnStatSet = HashMap<IndexType, ColumnStat>;

#[derive(Debug, Clone)]
/// Statistics information of a column
pub struct ColumnStat {
    /// Min value of the column
    pub min: Datum,

    /// Max value of the column
    pub max: Datum,

    /// Number of distinct values
    pub ndv: f64,

    /// Count of null values
    pub null_count: u64,

    /// Histogram of column
    pub histogram: Option<Histogram>,
}

#[derive(Debug, Clone)]
pub struct NewStatistic {
    pub min: Option<Datum>,
    pub max: Option<Datum>,
    pub ndv: Option<f64>,
}
