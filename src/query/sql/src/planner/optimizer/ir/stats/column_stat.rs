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
use databend_common_expression::stat_distribution::ArgStat;
use databend_common_expression::stat_distribution::BorrowedDistribution;
use databend_common_expression::stat_distribution::StatCount;
use databend_common_expression::stat_distribution::StatEstimate;
use databend_common_expression::types::DataType;
use databend_common_statistics::Datum;
use databend_common_statistics::Histogram;

use crate::Symbol;

pub type ColumnStatSet = HashMap<Symbol, ColumnStat>;

#[derive(Debug, Clone)]
/// Statistics information of a column
pub struct ColumnStat {
    /// Min value of the column
    pub min: Datum,

    /// Max value of the column
    pub max: Datum,

    /// Number of distinct values
    pub ndv: StatEstimate,

    /// Count of null values
    pub null_count: StatCount,

    /// Histogram of column
    pub histogram: Option<Histogram>,
}

impl ColumnStat {
    pub(crate) fn refine_ndv_from_histogram(&mut self, histogram: &Histogram) {
        let histogram_ndv = histogram.ndv();
        if histogram.accuracy() {
            self.ndv = self.ndv.min(histogram_ndv);
            return;
        }

        // Inaccurate histograms keep derived bucket distinct counts as the best
        // expected value, but their NDV bounds are only coarse consistency bounds.
        let expected = self.ndv.expected.min(histogram_ndv.expected);
        self.ndv = StatEstimate::new(self.ndv.lower, expected.max(self.ndv.lower), self.ndv.upper);
    }

    pub fn to_arg_stat(&self, data_type: &DataType) -> Result<ArgStat<'_>, String> {
        let domain = Domain::from_datum(
            data_type,
            self.min.clone(),
            self.max.clone(),
            self.null_count.upper() > 0.0,
        )?;
        Ok(ArgStat {
            domain,
            ndv: self.ndv,
            null_count: self.null_count,
            distribution: self
                .histogram
                .as_ref()
                .map(BorrowedDistribution::Histogram)
                .unwrap_or(BorrowedDistribution::Unknown),
        })
    }

    pub fn from_const(datum: Datum) -> Self {
        Self {
            min: datum.clone(),
            max: datum,
            ndv: StatEstimate::exact(1.0),
            null_count: StatCount::exact(0),
            histogram: None,
        }
    }
}
