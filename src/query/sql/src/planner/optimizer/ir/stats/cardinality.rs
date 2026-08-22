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

use databend_common_statistics::NdvEstimate;
use databend_common_statistics::StatCount;

use crate::optimizer::ir::StatInfo;

pub(crate) fn cap_stat_info_by_rows(mut stat_info: StatInfo, limit: usize) -> StatInfo {
    let input_cardinality = stat_info.cardinality;
    let limit = limit as f64;
    stat_info.max_cardinality = stat_info.max_cardinality.max(input_cardinality).min(limit);
    if limit == 0.0 {
        stat_info.cardinality = 0.0;
        stat_info.statistics.precise_cardinality =
            stat_info.statistics.precise_cardinality.map(|_| 0);
        stat_info.statistics.column_stats.clear();
        stat_info.statistics.top_n.clear();
        stat_info.statistics.count_min_sketch.clear();
        return stat_info;
    }

    if input_cardinality <= limit {
        return stat_info;
    }

    stat_info.cardinality = limit;
    stat_info.statistics.precise_cardinality = stat_info
        .statistics
        .precise_cardinality
        .map(|cardinality| cardinality.min(limit as u64));

    for column_stat in stat_info.statistics.column_stats.values_mut() {
        let ndv_upper = column_stat.ndv.upper.min(limit);
        column_stat.ndv = match column_stat.ndv.expected {
            Some(expected) => NdvEstimate::new(expected.min(ndv_upper), ndv_upper),
            None => NdvEstimate::upper_bound(ndv_upper),
        };

        column_stat.null_count = if column_stat.null_count == StatCount::exact(0) {
            StatCount::exact(0)
        } else {
            let upper = column_stat.null_count.upper().min(limit);
            let expected = column_stat.null_count.expected() * limit / input_cardinality;
            StatCount::estimate(expected.min(upper), upper)
        };
        column_stat.histogram = None;
    }

    stat_info.statistics.top_n.clear();
    stat_info.statistics.count_min_sketch.clear();
    stat_info
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use databend_common_statistics::Datum;

    use super::*;
    use crate::Symbol;
    use crate::optimizer::ir::ColumnStat;
    use crate::optimizer::ir::Statistics;

    fn stat_info(cardinality: f64, precise_cardinality: Option<u64>) -> StatInfo {
        StatInfo {
            cardinality,
            max_cardinality: cardinality,
            statistics: Statistics {
                precise_cardinality,
                column_stats: HashMap::from([(Symbol::new(0), ColumnStat {
                    min: Datum::Int(1),
                    max: Datum::Int(100),
                    ndv: NdvEstimate::new(80.0, 90.0),
                    null_count: StatCount::estimate(20.0, 30.0),
                    histogram: None,
                })]),
                top_n: Default::default(),
                count_min_sketch: Default::default(),
            },
        }
    }

    #[test]
    fn test_cap_stat_info_by_zero_rows() {
        let capped = cap_stat_info_by_rows(stat_info(100.0, Some(100)), 0);

        assert_eq!(capped.cardinality, 0.0);
        assert_eq!(capped.statistics.precise_cardinality, Some(0));
        assert!(capped.statistics.column_stats.is_empty());
    }

    #[test]
    fn test_cap_stat_info_without_reduction() {
        let capped = cap_stat_info_by_rows(stat_info(10.0, Some(10)), 20);
        let column_stat = &capped.statistics.column_stats[&Symbol::new(0)];

        assert_eq!(capped.cardinality, 10.0);
        assert_eq!(capped.statistics.precise_cardinality, Some(10));
        assert_eq!(column_stat.ndv, NdvEstimate::new(80.0, 90.0));
        assert_eq!(column_stat.null_count, StatCount::estimate(20.0, 30.0));
    }

    #[test]
    fn test_cap_stat_info_reduces_count_bounds() {
        let capped = cap_stat_info_by_rows(stat_info(100.0, Some(100)), 10);
        let column_stat = &capped.statistics.column_stats[&Symbol::new(0)];

        assert_eq!(capped.cardinality, 10.0);
        assert_eq!(capped.statistics.precise_cardinality, Some(10));
        assert_eq!(column_stat.ndv, NdvEstimate::new(10.0, 10.0));
        assert_eq!(column_stat.ndv.lower, 0.0);
        assert_eq!(column_stat.null_count, StatCount::estimate(2.0, 10.0));
        assert!(column_stat.histogram.is_none());
    }

    #[test]
    fn test_cap_stat_info_reduces_risk_bound_even_when_expected_rows_fit() {
        let mut input = stat_info(10.0, None);
        input.max_cardinality = 1_000.0;

        let capped = cap_stat_info_by_rows(input, 20);

        assert_eq!(capped.cardinality, 10.0);
        assert_eq!(capped.max_cardinality, 20.0);
    }
}
