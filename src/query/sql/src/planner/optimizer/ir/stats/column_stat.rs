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

use databend_common_expression::Domain;
use databend_common_expression::function_stat::ArgStat;
use databend_common_expression::types::DataType;
use databend_common_statistics::Datum;
use databend_common_statistics::Histogram;
pub use databend_common_statistics::Ndv;

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
    pub ndv: Ndv,

    /// Count of null values
    pub null_count: u64,

    /// Histogram of column
    pub histogram: Option<Histogram>,
}

impl ColumnStat {
    pub fn to_arg_stat(&self, data_type: &DataType) -> Result<ArgStat<'_>, String> {
        let domain = Domain::from_datum(
            data_type,
            self.min.clone(),
            self.max.clone(),
            self.null_count != 0,
        )?;
        Ok(ArgStat {
            domain,
            ndv: self.ndv,
            null_count: self.null_count,
            histogram: self.histogram.as_ref(),
        })
    }

    pub fn from_const(datum: Datum) -> Self {
        Self {
            min: datum.clone(),
            max: datum,
            ndv: Ndv::Stat(1.0),
            null_count: 0,
            histogram: None,
        }
    }
}
