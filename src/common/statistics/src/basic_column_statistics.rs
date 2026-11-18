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

/// Column statistics as a `ColumnStatisticsProvider` exposes them.
///
/// This is the one shared contract between statistics producers (storage
/// providers, constant inputs, replay fixtures) and consumers (the planner,
/// system tables, DDL checks). Producers derive every value themselves;
/// [`Self::new`] only verifies the invariants below and never adjusts input:
///
/// - `null_count` does not exceed the row count;
/// - `min` and `max` are both present and form valid bounds, or both absent;
/// - `ndv` describes non-NULL values: its upper does not exceed the non-NULL
///   rows or the size of a discrete domain, and is positive when non-NULL rows
///   exist. These structural bounds are the only hard limits; inside them the
///   upper is a conservative cap, see [`NdvEstimate`].
#[derive(Clone, Debug, PartialEq)]
pub struct BasicColumnStatistics {
    min: Option<Datum>,
    max: Option<Datum>,
    null_count: u64,
    in_memory_size: u64,
    ndv: NdvEstimate,
}

impl BasicColumnStatistics {
    /// The NDV upper that a row count and bounds impose on a column; it is
    /// the bound [`Self::new`] verifies. Unknown row counts and invalid or
    /// one-sided bounds impose no constraint.
    pub fn ndv_upper_bound(
        min: Option<&Datum>,
        max: Option<&Datum>,
        null_count: u64,
        num_rows: Option<u64>,
    ) -> f64 {
        let bounds = match (min, max) {
            (Some(min), Some(max)) => StatBounds::new(min.clone(), max.clone()).ok(),
            _ => None,
        };
        Self::ndv_upper(bounds.as_ref(), null_count, num_rows)
    }

    /// `min(non-NULL rows, discrete domain size)`; an unknown row count or
    /// absent bounds impose no constraint.
    fn ndv_upper(bounds: Option<&StatBounds>, null_count: u64, num_rows: Option<u64>) -> f64 {
        let non_null_rows =
            num_rows.map_or(f64::MAX, |rows| rows.saturating_sub(null_count) as f64);
        bounds.map_or(non_null_rows, |bounds| bounds.ndv_upper(non_null_rows))
    }

    /// Verify statistics derived by a producer. See the type-level invariants.
    /// `num_rows` is the row count the statistics describe; `None` means it is
    /// unknown and imposes no constraint.
    pub fn new(
        min: Option<Datum>,
        max: Option<Datum>,
        null_count: u64,
        in_memory_size: u64,
        ndv: NdvEstimate,
        num_rows: Option<u64>,
    ) -> Result<Self, String> {
        ndv.check_consistency()?;
        if let Some(rows) = num_rows
            && null_count > rows
        {
            return Err(format!("null count {null_count} exceeds row count {rows}"));
        }
        let bounds = match (&min, &max) {
            (Some(min), Some(max)) => {
                Some(StatBounds::new(min.clone(), max.clone()).map_err(|err| err.to_string())?)
            }
            (None, None) => None,
            _ => return Err(format!("one-sided column bounds: min={min:?}, max={max:?}")),
        };
        let upper = Self::ndv_upper(bounds.as_ref(), null_count, num_rows);
        if ndv.upper > upper {
            return Err(format!("NDV {ndv:?} exceeds its upper bound {upper}"));
        }
        if num_rows.is_some_and(|rows| rows > null_count) && ndv.upper == 0.0 {
            return Err(format!("NDV {ndv:?} excludes the column's non-NULL rows"));
        }
        Ok(Self {
            min,
            max,
            null_count,
            in_memory_size,
            ndv,
        })
    }

    pub fn min(&self) -> Option<&Datum> {
        self.min.as_ref()
    }

    pub fn max(&self) -> Option<&Datum> {
        self.max.as_ref()
    }

    pub fn null_count(&self) -> u64 {
        self.null_count
    }

    pub fn in_memory_size(&self) -> u64 {
        self.in_memory_size
    }

    /// Distinct non-NULL values. Upper-only when the provider has no source
    /// that may serve as a point; see [`NdvEstimate`].
    pub fn ndv(&self) -> NdvEstimate {
        self.ndv
    }

    /// Build the planner statistics of a scan over `num_rows` rows. A scan may
    /// cover fewer rows than the provider described, which only tightens the
    /// NDV upper; the domain cap is applied by [`ColumnStat::new`]. Histogram
    /// construction stays on demand at the caller's used-column boundary; any
    /// point NDV, exact or not, may synthesize one.
    pub fn to_column_stat(
        &self,
        num_rows: Option<u64>,
        histogram: Option<Histogram>,
        histogram_spacing_unit: f64,
    ) -> Result<Option<ColumnStat>, String> {
        let null_count = StatCount::exact(self.null_count);
        if num_rows.is_some_and(|rows| rows > 0 && self.null_count == rows) {
            return Ok(Some(ColumnStat::AllNull { null_count }));
        }
        let (Some(min), Some(max)) = (&self.min, &self.max) else {
            return Ok(None);
        };
        // Verified by `new`; rebuilt here only because the fields hold datums.
        let bounds = StatBounds::new(min.clone(), max.clone()).map_err(|err| err.to_string())?;
        let non_null_rows = num_rows.map(|rows| rows.saturating_sub(self.null_count));
        let ndv = match non_null_rows {
            Some(rows) => self.ndv.reduce(rows as f64),
            None => self.ndv,
        };
        let histogram = histogram.or_else(|| {
            HistogramBuilder::from_ndv(
                ndv.expected?.round() as u64,
                non_null_rows.filter(|rows| *rows > 0)?,
                Some((min.clone(), max.clone())),
                DEFAULT_HISTOGRAM_BUCKETS,
                histogram_spacing_unit,
            )
            .ok()
        });
        ColumnStat::new(bounds, ndv, null_count, histogram).map(Some)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn stats(ndv: NdvEstimate) -> BasicColumnStatistics {
        BasicColumnStatistics::new(
            Some(Datum::Int(0)),
            Some(Datum::Int(10_000)),
            20,
            800,
            ndv,
            Some(100),
        )
        .unwrap()
    }

    fn int_stats(min: i64, max: i64, ndv: NdvEstimate, rows: Option<u64>) -> Result<(), String> {
        BasicColumnStatistics::new(
            Some(Datum::Int(min)),
            Some(Datum::Int(max)),
            0,
            0,
            ndv,
            rows,
        )
        .map(|_| ())
    }

    #[test]
    fn test_new_accepts_derived_statistics() {
        for ndv in [
            NdvEstimate::exact(12.0),
            NdvEstimate::new(12.0, 80.0),
            NdvEstimate::upper_bound(80.0),
        ] {
            assert_eq!(stats(ndv).ndv(), ndv);
        }
        let all_null =
            BasicColumnStatistics::new(None, None, 100, 0, NdvEstimate::exact(0.0), Some(100))
                .unwrap();
        assert_eq!(all_null.min(), None);
        assert_eq!(all_null.null_count(), 100);
        // Facts survive without bounds; only the row count bounds the NDV.
        let unbounded = BasicColumnStatistics::new(
            None,
            None,
            20,
            0,
            NdvEstimate::upper_bound(80.0),
            Some(100),
        )
        .unwrap();
        assert_eq!(unbounded.null_count(), 20);
        // An unknown row count imposes no row constraint.
        int_stats(0, 1_000_000, NdvEstimate::exact(1_000.0), None).unwrap();
    }

    #[test]
    fn test_new_rejects_violated_invariants_without_adjusting() {
        let invalid = [
            // NULL count exceeds rows.
            BasicColumnStatistics::new(None, None, 101, 0, NdvEstimate::exact(0.0), Some(100)),
            // One-sided and reversed bounds.
            BasicColumnStatistics::new(
                Some(Datum::Int(0)),
                None,
                0,
                0,
                NdvEstimate::upper_bound(10.0),
                Some(100),
            ),
            BasicColumnStatistics::new(
                Some(Datum::Int(1)),
                Some(Datum::Int(0)),
                0,
                0,
                NdvEstimate::upper_bound(1.0),
                Some(100),
            ),
            // NDV above the non-NULL rows: 100 rows, 20 NULL.
            BasicColumnStatistics::new(
                Some(Datum::Int(0)),
                Some(Datum::Int(10_000)),
                20,
                0,
                NdvEstimate::exact(81.0),
                Some(100),
            ),
            // Inconsistent estimate.
            BasicColumnStatistics::new(
                None,
                None,
                0,
                0,
                NdvEstimate {
                    expected: Some(20.0),
                    upper: 10.0,
                },
                Some(100),
            ),
            // Zero NDV over non-NULL rows.
            BasicColumnStatistics::new(None, None, 0, 0, NdvEstimate::exact(0.0), Some(100)),
        ];
        for result in invalid {
            assert!(result.is_err(), "{result:?}");
        }
        // NDV above a discrete domain: [0, 8] holds nine values.
        assert!(int_stats(0, 8, NdvEstimate::upper_bound(10.0), Some(100)).is_err());
        int_stats(0, 8, NdvEstimate::upper_bound(9.0), Some(100)).unwrap();
    }

    #[test]
    fn test_ndv_upper_bound_uses_exact_discrete_domains_only() {
        for (min, max, expected) in [
            (Datum::Int(-4), Datum::Int(4), 9.0),
            (Datum::Int(i64::MAX - 1), Datum::Int(i64::MAX), 2.0),
            (
                Datum::UInt(1_u64 << 53),
                Datum::UInt((1_u64 << 53) + 1),
                2.0,
            ),
            (Datum::UInt(u64::MAX - 10), Datum::UInt(u64::MAX), 11.0),
            (Datum::Bool(false), Datum::Bool(true), 2.0),
            (Datum::Bool(true), Datum::Bool(true), 1.0),
            (Datum::Int(i64::MIN), Datum::Int(i64::MAX), 100.0),
            (Datum::UInt(0), Datum::UInt(u64::MAX), 100.0),
            // Decimal is stored as Float; neither fractional widths nor short
            // byte strings bound cardinality.
            (Datum::Float(0.0.into()), Datum::Float(2.5.into()), 100.0),
            (
                Datum::Bytes(b"a".to_vec()),
                Datum::Bytes(b"b".to_vec()),
                100.0,
            ),
            // Reversed bounds impose nothing; validation rejects them later.
            (Datum::Int(1), Datum::Int(0), 100.0),
        ] {
            assert_eq!(
                BasicColumnStatistics::ndv_upper_bound(Some(&min), Some(&max), 0, Some(100)),
                expected,
                "{min:?}..{max:?}"
            );
        }
        assert_eq!(
            BasicColumnStatistics::ndv_upper_bound(None, None, 20, Some(100)),
            80.0
        );
        assert_eq!(
            BasicColumnStatistics::ndv_upper_bound(None, None, 20, None),
            f64::MAX
        );
    }

    #[test]
    fn test_to_column_stat() {
        // Any point NDV synthesizes a histogram; an upper-only NDV does not.
        for (ndv, histogram) in [
            (NdvEstimate::upper_bound(80.0), false),
            (NdvEstimate::new(12.0, 15.0), true),
            (NdvEstimate::new(12.0, 80.0), true),
            (NdvEstimate::exact(12.0), true),
        ] {
            let stat = stats(ndv)
                .to_column_stat(Some(100), None, 1.0)
                .unwrap()
                .unwrap();
            assert_eq!(stat.ndv(), ndv);
            assert_eq!(stat.histogram().is_some(), histogram);
            assert_eq!(stat.null_count(), StatCount::exact(20));
        }
        // A scan over fewer rows tightens the upper; unknown rows skip synthesis.
        let stat = stats(NdvEstimate::new(12.0, 80.0))
            .to_column_stat(Some(50), None, 1.0)
            .unwrap()
            .unwrap();
        assert_eq!(stat.ndv(), NdvEstimate::new(12.0, 30.0));
        let stat = stats(NdvEstimate::exact(12.0))
            .to_column_stat(None, None, 1.0)
            .unwrap()
            .unwrap();
        assert!(stat.histogram().is_none());
        // Bounds are required except for an all-NULL column.
        let unbounded = BasicColumnStatistics::new(
            None,
            None,
            20,
            0,
            NdvEstimate::upper_bound(80.0),
            Some(100),
        )
        .unwrap();
        assert!(
            unbounded
                .to_column_stat(Some(100), None, 1.0)
                .unwrap()
                .is_none()
        );
        let all_null =
            BasicColumnStatistics::new(None, None, 100, 0, NdvEstimate::exact(0.0), Some(100))
                .unwrap();
        assert!(matches!(
            all_null.to_column_stat(Some(100), None, 1.0).unwrap(),
            Some(ColumnStat::AllNull { .. })
        ));
    }

    #[test]
    fn test_supplied_histogram() {
        // A supplied histogram is retained and does not replace the NDV.
        let histogram =
            HistogramBuilder::from_ndv(8, 80, Some((Datum::Int(0), Datum::Int(10_000))), 32, 1.0)
                .unwrap();
        let stat = stats(NdvEstimate::new(12.0, 15.0))
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
        // A histogram of another type is rejected.
        let histogram = HistogramBuilder::from_ndv(
            8,
            80,
            Some((Datum::Float(0.0.into()), Datum::Float(100.0.into()))),
            32,
            1.0,
        )
        .unwrap();
        assert!(
            stats(NdvEstimate::upper_bound(80.0))
                .to_column_stat(Some(100), Some(histogram), 1.0)
                .is_err()
        );
    }
}
