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

use crate::ColumnStat;
use crate::DEFAULT_HISTOGRAM_BUCKETS;
use crate::Datum;
use crate::Histogram;
use crate::HistogramBuilder;
use crate::NdvEstimate;
use crate::StatBounds;
use crate::StatCount;

/// Runtime statistics information of a column.
#[derive(Clone, Debug, Default)]
pub struct BasicColumnStatistics {
    /// Min value of the column
    pub min: Option<Datum>,
    /// Max value of the column
    pub max: Option<Datum>,
    /// Non-NULL distinct values. `None` means no NDV statistics were supplied;
    /// `Some` can carry an expected value and/or only a conservative upper bound.
    pub ndv: Option<NdvEstimate>,
    // Count of null values
    pub null_count: u64,
    // Memory size of the column
    pub in_memory_size: u64,
}

/// The single intersection rule for finite-domain and physical NDV constraints.
pub(crate) fn ndv_upper(bounds: Option<&StatBounds>, physical_upper: f64) -> f64 {
    bounds
        .and_then(StatBounds::finite_ndv_upper)
        .map_or(physical_upper, |upper| upper.min(physical_upper))
}

impl BasicColumnStatistics {
    /// Attach a measured distinct count, not a sketch estimate. The caller
    /// establishes exactness; row count supplies only a physical constraint.
    pub fn with_exact_ndv(mut self, ndv: Option<u64>, num_rows: u64) -> Self {
        let upper = self.ndv_upper(Some(num_rows), None);
        self.ndv = ndv.map(|ndv| NdvEstimate::exact(ndv as f64).reduce(upper));
        self
    }

    /// Attach a distinct count that holds for this chunk only. Summing chunk
    /// bounds bounds their union, but the sum is not a point estimate. A
    /// missing or zero count over non-NULL rows still contributes the chunk's
    /// row and domain bound, so one unknown chunk does not discard the others.
    pub fn with_chunk_ndv_upper(mut self, ndv: Option<u64>, num_rows: u64) -> Self {
        let bounds = match (&self.min, &self.max) {
            (Some(min), Some(max)) => StatBounds::new(min.clone(), max.clone()).ok(),
            _ => None,
        };
        let upper = self.ndv_upper(Some(num_rows), bounds.as_ref());
        let ndv = match ndv {
            Some(ndv) if ndv > 0 || upper == 0.0 => (ndv as f64).min(upper),
            _ => upper,
        };
        self.ndv = Some(NdvEstimate::upper_bound(ndv));
        self
    }

    fn is_all_null(&self, num_rows: Option<u64>) -> bool {
        num_rows.is_some_and(|rows| rows > 0 && self.null_count == rows)
    }

    fn ndv_upper(&self, num_rows: Option<u64>, bounds: Option<&StatBounds>) -> f64 {
        let non_null_rows = num_rows
            .map(|rows| rows.saturating_sub(self.null_count))
            .unwrap_or(u64::MAX) as f64;
        ndv_upper(bounds, non_null_rows)
    }

    /// Normalize a source estimate with the same rules whether it arrives via a storage
    /// provider or directly as a constant input. Statistical coverage extends
    /// the source upper; population/domain constraints only reduce it.
    fn ndv_for_rows(
        &self,
        num_rows: Option<u64>,
        stats_row_count: Option<u64>,
        bounds: Option<&StatBounds>,
    ) -> NdvEstimate {
        let upper = self.ndv_upper(num_rows, bounds);
        let Some(ndv) = self.ndv else {
            return NdvEstimate::upper_bound(upper);
        };
        if ndv.expected == Some(0.0) && upper > 0.0 {
            return NdvEstimate::upper_bound(upper);
        }
        if let (Some(num_rows), Some(stats_row_count)) = (num_rows, stats_row_count)
            && stats_row_count < num_rows
        {
            if stats_row_count == 0 {
                return NdvEstimate::upper_bound(upper);
            }
            // The sample need not be random. Each uncovered row can add at
            // most one distinct value; do not extrapolate confidence endpoints.
            let upper = (ndv.upper + (num_rows - stats_row_count) as f64).min(upper);
            return match ndv.expected {
                Some(expected) if expected > 0.0 => NdvEstimate::new(
                    (Self::estimate_ndv(expected as u64, stats_row_count, num_rows) as f64)
                        .min(upper),
                    upper,
                ),
                _ => NdvEstimate::upper_bound(upper),
            };
        }
        ndv.reduce(upper)
    }

    /// Interpret base statistics for a single population. Storage adapters and
    /// constant inputs share this entry; operators subsequently transform the
    /// resulting ColumnStat without reinterpreting missing source statistics.
    /// Histogram construction stays on demand at the caller's used-column boundary.
    pub fn to_column_stat(
        &self,
        num_rows: Option<u64>,
        histogram: Option<Histogram>,
        histogram_spacing_unit: f64,
    ) -> Result<Option<ColumnStat>, String> {
        let null_count = StatCount::exact(self.null_count);
        if self.is_all_null(num_rows) {
            return Ok(Some(ColumnStat::AllNull { null_count }));
        }
        let (Some(min), Some(max)) = (&self.min, &self.max) else {
            return Ok(None);
        };
        let bounds = StatBounds::new(min.clone(), max.clone()).map_err(|err| err.to_string())?;
        let ndv = self.ndv_for_rows(num_rows, num_rows, Some(&bounds));
        // Synthesis needs a point NDV; an uncertain one is still usable. The
        // histogram contributes only expected NDV, so the column keeps its upper.
        let histogram = histogram.or_else(|| {
            let rows = num_rows?.saturating_sub(self.null_count);
            if rows == 0 {
                return None;
            }
            let expected = ndv.expected?;
            HistogramBuilder::from_ndv(
                expected.round() as u64,
                rows,
                Some((min.clone(), max.clone())),
                DEFAULT_HISTOGRAM_BUCKETS,
                histogram_spacing_unit,
            )
            .ok()
        });
        ColumnStat::new(bounds, ndv, null_count, histogram).map(Some)
    }

    pub fn new_null() -> Self {
        Self {
            min: None,
            max: None,
            ndv: Some(NdvEstimate::exact(0.0)),
            null_count: 0,
            in_memory_size: 0,
        }
    }

    pub fn merge(&mut self, other: BasicColumnStatistics) {
        // Distinct counts add only for disjoint value sets. For overlapping
        // row groups the sum is an upper bound, not a measured union NDV.
        let disjoint = match (&self.min, &self.max, &other.min, &other.max) {
            (Some(min), Some(max), Some(other_min), Some(other_max)) => {
                StatBounds::new(min.clone(), max.clone())
                    .and_then(|bounds| {
                        bounds.is_disjoint(&StatBounds::new(other_min.clone(), other_max.clone())?)
                    })
                    .unwrap_or(false)
            }
            _ => false,
        };
        self.min = Datum::min(self.min.clone(), other.min);
        self.max = Datum::max(self.max.clone(), other.max);
        self.ndv = match (self.ndv, other.ndv) {
            (Some(left), other) if left.upper == 0.0 => other,
            (other, Some(right)) if right.upper == 0.0 => other,
            (Some(left), Some(right)) => {
                let upper = left.upper + right.upper;
                Some(match (left.expected, right.expected) {
                    (Some(left), Some(right)) if disjoint => NdvEstimate::new(left + right, upper),
                    _ => NdvEstimate::upper_bound(upper),
                })
            }
            _ => None,
        };
        self.null_count += other.null_count;
        self.in_memory_size += other.in_memory_size;
    }

    // Normalize useful statistics without turning a range/row-count bound
    // into an observed NDV. Apply domain caps after extrapolating the sample.
    // When coverage is partial, the source NDV must describe rows retained in
    // this population; arbitrary replacement/deletion needs refreshed statistics.
    pub fn get_useful_stat(&self, num_rows: u64, stats_row_count: u64) -> Option<Self> {
        if self.is_all_null(Some(num_rows)) {
            return Some(Self {
                ndv: Some(NdvEstimate::exact(0.0)),
                ..self.clone()
            });
        }
        let bounds = StatBounds::new(self.min.clone()?, self.max.clone()?).ok()?;
        let ndv = self.ndv_for_rows(Some(num_rows), Some(stats_row_count), Some(&bounds));
        Some(Self {
            min: self.min.clone(),
            max: self.max.clone(),
            ndv: Some(ndv),
            null_count: self.null_count,
            in_memory_size: self.in_memory_size,
        })
    }

    // Inspired by duckdb (https://github.com/duckdb/duckdb/blob/main/src/storage/statistics/distinct_statistics.cpp#L55-L69)
    fn estimate_ndv(ndv: u64, stats_row_count: u64, num_rows: u64) -> u64 {
        // Zero also means an unknown sample size in upgraded legacy HLL
        // metadata. Do not assume that its NDV covers the current rows.
        if stats_row_count == 0 || ndv == 0 {
            return num_rows;
        }

        if stats_row_count >= num_rows {
            return ndv.min(num_rows);
        }

        let s = stats_row_count as f64;
        let n = num_rows as f64;
        let u = ndv.min(stats_row_count) as f64;

        let u1 = (u / s).powi(2) * u;
        // Good–Turing Estimation
        let estimate = u + u1 / s * (n - s);

        estimate.round().clamp(0.0, n) as u64
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn source(ndv: Option<NdvEstimate>) -> BasicColumnStatistics {
        BasicColumnStatistics {
            min: Some(Datum::Int(0)),
            max: Some(Datum::Int(10_000)),
            ndv,
            null_count: 20,
            in_memory_size: 800,
        }
    }

    #[test]
    fn test_shared_source_interpretation() {
        for (input, expected, histogram) in [
            (None, NdvEstimate::upper_bound(80.0), false),
            (
                Some(NdvEstimate::exact(0.0)),
                NdvEstimate::upper_bound(80.0),
                false,
            ),
            (
                Some(NdvEstimate::new(12.0, 15.0)),
                NdvEstimate::new(12.0, 15.0),
                true,
            ),
            (
                Some(NdvEstimate::new(12.0, 3000.0)),
                NdvEstimate::new(12.0, 80.0),
                true,
            ),
            (
                Some(NdvEstimate::exact(12.0)),
                NdvEstimate::exact(12.0),
                true,
            ),
        ] {
            let stat = source(input)
                .to_column_stat(Some(100), None, 1.0)
                .unwrap()
                .unwrap();
            assert_eq!(stat.ndv(), expected);
            assert_eq!(stat.histogram().is_some(), histogram);
            assert_eq!(stat.null_count(), StatCount::exact(20));
        }
        let unknown_rows = source(None)
            .to_column_stat(None, None, 1.0)
            .unwrap()
            .unwrap();
        assert_eq!(unknown_rows.ndv(), NdvEstimate::upper_bound(10_001.0));
        assert!(unknown_rows.histogram().is_none());
    }

    #[test]
    fn test_all_null_before_missing_bounds() {
        let all_null = BasicColumnStatistics {
            null_count: 100,
            ..Default::default()
        };
        let direct = all_null.to_column_stat(Some(100), None, 1.0).unwrap();
        assert!(matches!(direct, Some(ColumnStat::AllNull { .. })));
        let normalized = all_null.get_useful_stat(100, 100).unwrap();
        assert_eq!(normalized.ndv, Some(NdvEstimate::exact(0.0)));
        assert_eq!(
            normalized.to_column_stat(Some(100), None, 1.0).unwrap(),
            direct
        );
        assert!(
            BasicColumnStatistics::default()
                .to_column_stat(Some(0), None, 1.0)
                .unwrap()
                .is_none()
        );
    }

    #[test]
    fn test_synthesized_histogram_keeps_uncertain_upper() {
        // Uncertain NDV synthesizes a histogram. Restricting it narrows the
        // expected NDV, but bucket estimates must not become the column upper.
        let mut stat = source(Some(NdvEstimate::new(12.0, 15.0)))
            .to_column_stat(Some(100), None, 1.0)
            .unwrap()
            .unwrap();
        assert!(stat.histogram().is_some());
        stat.restrict_to_bounds(StatBounds::Int { min: 0, max: 5_000 })
            .unwrap();
        let ndv = stat.ndv();
        assert!(ndv.expected.unwrap() < 12.0);
        assert_eq!(ndv.upper, 15.0);
    }

    #[test]
    fn test_domain_capped_ndv_synthesizes_within_domain() {
        let mut input = source(Some(NdvEstimate::new(1.0, 2.0)));
        input.max = input.min.clone();
        let stat = input.to_column_stat(Some(100), None, 1.0).unwrap().unwrap();
        assert_eq!(stat.ndv(), NdvEstimate::exact(1.0));
        assert!(stat.histogram().is_some());
    }

    #[test]
    fn test_supplied_histogram_does_not_replace_ndv() {
        let input = source(Some(NdvEstimate::new(12.0, 15.0)));
        let histogram =
            HistogramBuilder::from_ndv(8, 80, Some((Datum::Int(0), Datum::Int(10_000))), 32, 1.0)
                .unwrap();
        let stat = input
            .to_column_stat(Some(100), Some(histogram.clone()), 1.0)
            .unwrap()
            .unwrap();
        assert_eq!(stat.ndv(), NdvEstimate::new(12.0, 15.0));
        let ColumnStat::Int {
            histogram: Some(actual),
            ..
        } = stat
        else {
            panic!("source histogram must be retained");
        };
        assert_eq!(Histogram::Int(actual), histogram);
    }

    #[test]
    fn test_invalid_source_statistics_are_rejected() {
        let mut input = source(None);
        input.max = Some(Datum::Int(-1));
        assert!(input.to_column_stat(Some(100), None, 1.0).is_err());
        let histogram = HistogramBuilder::from_ndv(
            8,
            80,
            Some((Datum::Float(0.0.into()), Datum::Float(100.0.into()))),
            32,
            1.0,
        )
        .unwrap();
        assert!(
            source(None)
                .to_column_stat(Some(100), Some(histogram), 1.0)
                .is_err()
        );
    }

    #[test]
    fn test_estimate_ndv() {
        assert_eq!(BasicColumnStatistics::estimate_ndv(0, 1, 3), 3);
        assert_eq!(BasicColumnStatistics::estimate_ndv(1, 1, 3), 3);
        assert_eq!(BasicColumnStatistics::estimate_ndv(12, 100, 3000), 17);
        assert_eq!(
            BasicColumnStatistics::estimate_ndv(6000, 10000, 1000000),
            219840
        );
    }
}
