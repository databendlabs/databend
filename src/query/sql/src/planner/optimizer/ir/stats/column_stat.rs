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
use databend_common_expression::stat_distribution::NdvEstimate;
use databend_common_expression::stat_distribution::StatCount;
use databend_common_expression::types::DataType;
use databend_common_statistics::Datum;
use databend_common_statistics::Histogram;
use databend_storages_common_table_meta::meta::ColumnCountMinSketch;
use databend_storages_common_table_meta::meta::ColumnTopN;

use crate::Symbol;

pub type ColumnStatSet = HashMap<Symbol, ColumnStat>;
pub type TopNSet = HashMap<Symbol, ColumnTopN>;
pub type CountMinSketchSet = HashMap<Symbol, ColumnCountMinSketch>;

#[derive(Debug, Clone, PartialEq)]
/// Statistics information of a column
pub struct ColumnStat {
    /// Min value of the column
    pub min: Datum,

    /// Max value of the column
    pub max: Datum,

    /// Number of distinct values
    pub ndv: NdvEstimate,

    /// Count of null values
    pub null_count: StatCount,

    /// Histogram of column
    pub histogram: Option<Histogram>,
}

impl ColumnStat {
    pub(crate) fn ndv_bounded_by_discrete_domain(&self) -> (NdvEstimate, bool) {
        let domain_ndv = match (&self.min, &self.max) {
            (Datum::Bool(min), Datum::Bool(max)) if min <= max => {
                Some((*max as u8 - *min as u8 + 1) as f64)
            }
            (Datum::Int(min), Datum::Int(max)) if min <= max => {
                Some((*max as i128 - *min as i128 + 1) as f64)
            }
            (Datum::UInt(min), Datum::UInt(max)) if min <= max => {
                Some((*max as u128 - *min as u128 + 1) as f64)
            }
            _ => None,
        };
        let Some(domain_ndv) = domain_ndv else {
            return (self.ndv, false);
        };
        let bounded = NdvEstimate {
            lower: self.ndv.lower.min(domain_ndv),
            expected: self.ndv.expected.map(|expected| expected.min(domain_ndv)),
            upper: self.ndv.upper.min(domain_ndv),
        };
        let estimate_exceeded_domain = self.ndv.lower > domain_ndv
            || self
                .ndv
                .expected
                .is_some_and(|expected| expected > domain_ndv);
        (bounded, estimate_exceeded_domain)
    }

    pub(crate) fn join_key_null_count_for_cardinality(&self, cardinality: f64) -> f64 {
        // Keep at least the trusted NDV lower-bound rows as non-NULL. Derived
        // filters can otherwise leave a stale NULL estimate that is subtracted
        // from the row count a second time.
        let max_null_count = (cardinality - self.ndv.lower).max(0.0);
        self.null_count.expected().min(max_null_count)
    }

    pub(crate) fn refine_ndv_from_histogram(&mut self, histogram: &Histogram) {
        let histogram_ndv = histogram.ndv();
        if histogram.accuracy() {
            self.ndv = self.ndv.min(histogram_ndv);
            return;
        }

        let upper = self.ndv.upper.min(histogram_ndv.upper);
        self.ndv = match histogram_ndv.expected {
            Some(expected) => NdvEstimate::new(expected.min(upper), upper),
            None => NdvEstimate::upper_bound(upper),
        };
    }

    pub fn to_arg_stat(&self, data_type: &DataType) -> Result<ArgStat<'_>, String> {
        let domain = Domain::from_datum(
            data_type,
            self.min.clone(),
            self.max.clone(),
            self.null_count.upper() > 0.0,
        )?;
        let ndv = domain
            .finite_cardinality_upper()
            .map_or(self.ndv, |upper| self.ndv.reduce(upper as f64));
        Ok(ArgStat {
            domain,
            ndv,
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
            ndv: NdvEstimate::exact(1.0),
            null_count: StatCount::exact(0),
            histogram: None,
        }
    }
}

#[cfg(test)]
mod tests {
    use databend_common_expression::stat_distribution::StatCount;
    use databend_common_expression::types::NumberDataType;

    use super::*;

    #[test]
    fn test_loose_ndv_upper_is_bounded_without_rejecting_estimate() {
        let stat = ColumnStat {
            min: Datum::UInt(1),
            max: Datum::UInt(5),
            ndv: NdvEstimate::new(3.0, 9.0),
            null_count: StatCount::exact(0),
            histogram: None,
        };

        let (bounded, estimate_exceeded_domain) = stat.ndv_bounded_by_discrete_domain();

        assert_eq!(bounded, NdvEstimate::new(3.0, 5.0));
        assert!(!estimate_exceeded_domain);
    }

    #[test]
    fn test_impossible_ndv_estimate_is_rejected_after_bounding() {
        let stat = ColumnStat {
            min: Datum::UInt(1),
            max: Datum::UInt(5),
            ndv: NdvEstimate::new(6.0, 9.0),
            null_count: StatCount::exact(0),
            histogram: None,
        };

        let (bounded, estimate_exceeded_domain) = stat.ndv_bounded_by_discrete_domain();

        assert_eq!(bounded, NdvEstimate::new(5.0, 5.0));
        assert!(estimate_exceeded_domain);
    }

    #[test]
    fn test_arg_stat_bounds_loose_ndv_upper_by_finite_domain() {
        let stat = ColumnStat {
            min: Datum::Int(1),
            max: Datum::Int(3),
            ndv: NdvEstimate::new(3.0, 9.0),
            null_count: StatCount::exact(0),
            histogram: None,
        };

        let arg_stat = stat
            .to_arg_stat(&DataType::Number(NumberDataType::Int64))
            .unwrap();

        assert_eq!(arg_stat.ndv, NdvEstimate::new(3.0, 3.0));
        arg_stat.check_consistency().unwrap();
    }
}
