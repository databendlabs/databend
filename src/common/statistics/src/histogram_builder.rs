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

use crate::Datum;
use crate::F64;
use crate::Histogram;
use crate::TypedHistogramBuilder;

pub struct HistogramBuilder;

impl HistogramBuilder {
    pub fn from_ndv(
        ndv: u64,
        num_rows: u64,
        bound: Option<(Datum, Datum)>,
        num_buckets: usize,
    ) -> std::result::Result<Histogram, String> {
        let Some((min, max)) = bound else {
            return TypedHistogramBuilder::from_ndv::<F64>(ndv, num_rows, None, num_buckets)
                .map(Histogram::Float);
        };

        match (min, max) {
            (Datum::Int(min), Datum::Int(max)) => {
                TypedHistogramBuilder::from_ndv(ndv, num_rows, Some((min, max)), num_buckets)
                    .map(Histogram::Int)
            }
            (Datum::UInt(min), Datum::UInt(max)) => {
                TypedHistogramBuilder::from_ndv(ndv, num_rows, Some((min, max)), num_buckets)
                    .map(Histogram::UInt)
            }
            (Datum::Float(min), Datum::Float(max)) => {
                TypedHistogramBuilder::from_ndv(ndv, num_rows, Some((min, max)), num_buckets)
                    .map(Histogram::Float)
            }
            (Datum::Bytes(min), Datum::Bytes(max)) => {
                TypedHistogramBuilder::from_ndv(ndv, num_rows, Some((min, max)), num_buckets)
                    .map(Histogram::Bytes)
            }
            (min, max) => Err(format!(
                "Unsupported datum type for histogram calculation: {} (type: {}), {} (type: {}).",
                min,
                min.type_name(),
                max,
                max.type_name()
            )),
        }
    }
}

#[cfg(test)]
mod tests {
    use databend_common_base::base::OrderedFloat;

    use super::*;
    use crate::HistogramBucket;
    use crate::TypedHistogram;
    use crate::TypedHistogramBucket;

    #[test]
    fn test_histogram_builder_from_ndv_preserves_avg_spacing() {
        let histogram =
            HistogramBuilder::from_ndv(8, 16, Some((Datum::UInt(0), Datum::UInt(80))), 4).unwrap();
        let buckets = histogram.bucket_iter().collect::<Vec<_>>();

        assert_eq!(histogram.num_buckets(), 4);
        assert_eq!(buckets.first().unwrap().lower_bound(), Datum::UInt(0));
        assert_eq!(buckets.last().unwrap().upper_bound(), Datum::UInt(80));
    }

    #[test]
    fn test_histogram_bucket_rejects_mixed_numeric_bounds() {
        let err = HistogramBucket::try_from_bounds(Datum::UInt(0), Datum::Int(10), 10.0, 10.0)
            .unwrap_err();

        assert_eq!(
            err,
            "histogram bucket bounds must have the same supported type"
        );
    }

    #[test]
    fn test_synthetic_histogram_range_sparse_after_scaling() {
        // 73049 unique values over [0, 73048] in 10 buckets, fully dense.
        let mut histogram =
            TypedHistogramBuilder::from_ndv::<u64>(73049, 73049, Some((0u64, 73048u64)), 10)
                .unwrap();
        let retention = histogram.synthetic_distinct_retention().unwrap();
        assert!((retention - 1.0).abs() < 1e-3, "{retention}");
        assert!(!histogram.is_range_sparse(0.5));

        // A filter on other columns keeps one year: 365 of 73049 values (0.5%).
        histogram.scale_counts(365.0 / 73049.0);
        let retention = histogram.synthetic_distinct_retention().unwrap();
        assert!((retention - 0.005).abs() < 0.001, "{retention}");
        assert!(histogram.is_range_sparse(0.5));
        assert!(!histogram.is_range_sparse(0.001));

        // Keeping 90% of the values is not sparse at a 50% threshold.
        let mut light =
            TypedHistogramBuilder::from_ndv::<u64>(73049, 73049, Some((0u64, 73048u64)), 10)
                .unwrap();
        light.scale_counts(0.9);
        assert!(!light.is_range_sparse(0.5));
    }

    #[test]
    fn test_range_sparse_survives_row_scale_materialisation() {
        let mut histogram =
            TypedHistogramBuilder::from_ndv::<u64>(73049, 73049, Some((0u64, 73048u64)), 10)
                .unwrap();
        histogram.scale_counts(365.0 / 73049.0);
        assert!(histogram.is_range_sparse(0.5));

        // `restrict_discrete_buckets` folds `row_scale` into the buckets and
        // resets it. The surviving values are still 365 spread over a range
        // built for 73049; the verdict must not change.
        let restricted = histogram.restrict_discrete_buckets(0u64, 36524u64).unwrap();
        assert_eq!(restricted.row_scale, 1.0);
        assert!(restricted.is_range_sparse(0.5), "{restricted:?}");

        // A join output histogram is rebuilt from matched counts with
        // `row_scale == 1`; it keeps `avg_spacing`, so the same range check
        // applies.
        let dense =
            TypedHistogramBuilder::from_ndv::<u64>(73049, 73049, Some((0u64, 73048u64)), 10)
                .unwrap();
        let joined = histogram.estimate_join(&dense).histogram.unwrap();
        assert!(joined.is_range_sparse(0.5), "{joined:?}");
    }

    #[test]
    fn test_restrict_folds_row_scale_into_distinct_counts() {
        // 73049 unique values, 10 buckets, thinned to 365 rows by a filter on
        // another column. Before restricting, each bucket still stores
        // nd = 7305 with row_scale = 0.005; `ndv()` accounts for the scale.
        let mut histogram =
            TypedHistogramBuilder::from_ndv::<u64>(73049, 73049, Some((0u64, 73048u64)), 10)
                .unwrap();
        histogram.scale_counts(365.0 / 73049.0);
        let ndv_before = histogram.ndv().expected.unwrap();
        assert!((ndv_before - 365.0).abs() < 1.0, "{ndv_before}");

        // Restrict to the first half. `row_scale` is folded away; buckets must
        // now be self-consistent (nd <= nv) and `ndv()` must be half of before.
        let restricted = histogram.restrict_discrete_buckets(0u64, 36523u64).unwrap();
        assert_eq!(restricted.row_scale, 1.0);
        for bucket in &restricted.buckets {
            assert!(
                bucket.num_distinct() <= bucket.num_values() + 1e-9,
                "bucket claims more distinct values than rows: {bucket:?}"
            );
        }
        let stored: f64 = restricted.buckets.iter().map(|b| b.num_distinct()).sum();
        let ndv_after = restricted.ndv().expected.unwrap();
        assert!(
            (stored - ndv_after).abs() < 1e-6,
            "stored {stored} vs ndv() {ndv_after}"
        );
        assert!((ndv_after - ndv_before / 2.0).abs() < 1.0, "{ndv_after}");

        // Float and bytes restrictions follow the same contract.
        let mut float = TypedHistogramBuilder::from_ndv::<OrderedFloat<f64>>(
            1000,
            100_000,
            Some((OrderedFloat(0.0), OrderedFloat(1000.0))),
            10,
        )
        .unwrap();
        float.scale_counts(0.01);
        let float_ndv = float.ndv().expected.unwrap();
        let restricted = float
            .restrict_float_buckets(OrderedFloat(0.0), OrderedFloat(500.0))
            .unwrap();
        for bucket in &restricted.buckets {
            assert!(
                bucket.num_distinct() <= bucket.num_values() + 1e-9,
                "{bucket:?}"
            );
        }
        let after = restricted.ndv().expected.unwrap();
        assert!(
            (after - float_ndv / 2.0).abs() < float_ndv * 0.05,
            "{after} vs {float_ndv}"
        );
    }

    #[test]
    fn test_range_sparse_tracks_distinct_values_not_rows() {
        // 100 distinct values over 1M rows on a range of exactly 100 integers:
        // each value has ~10k rows. Keeping 10% of the rows keeps almost every
        // value, so the uniform position assumption is as valid as before.
        // `row_scale` alone would call this sparse; distinct retention must not.
        let mut histogram =
            TypedHistogramBuilder::from_ndv::<u64>(100, 1_000_000, Some((0u64, 99u64)), 10)
                .unwrap();
        histogram.scale_counts(0.1);
        assert!((histogram.row_scale - 0.1).abs() < 1e-12);
        let retention = histogram.synthetic_distinct_retention().unwrap();
        assert!(retention > 0.95, "{retention}");
        assert!(!histogram.is_range_sparse(0.5));
    }

    #[test]
    fn test_sampled_histogram_is_never_range_sparse() {
        let histogram = Histogram::UInt(TypedHistogram {
            row_scale: 1e-6,
            buckets: vec![TypedHistogramBucket::new(0u64, 10u64, 1.0, 1.0)],
            avg_spacing: None,
        });
        assert!(!histogram.is_range_sparse(0.5));
        assert!(!histogram.is_range_sparse(1.0));
    }

    #[test]
    fn test_synthetic_histogram_with_outlier_bounds_is_range_sparse() {
        // A sentinel max makes the synthesised bucket width ~1e17: the bounds
        // say nothing about where the 1000 real values are.
        let histogram =
            TypedHistogramBuilder::from_ndv::<i64>(1000, 1000, Some((0, i64::MAX)), 100).unwrap();
        assert_eq!(histogram.avg_spacing, Some(f64::INFINITY));
        assert_eq!(histogram.synthetic_distinct_retention(), None);
        assert!(histogram.is_range_sparse(0.0));

        let histogram = HistogramBuilder::from_ndv(
            1000,
            1000,
            Some((Datum::Int(0), Datum::Int(i64::MAX))),
            100,
        )
        .unwrap();
        assert!(histogram.is_range_sparse(0.0));

        // The distortion is a property of synthesis; restricting the range
        // does not make the positions trustworthy again.
        let restricted =
            TypedHistogramBuilder::from_ndv::<i64>(1000, 1000, Some((0, i64::MAX)), 100)
                .unwrap()
                .restrict_discrete_buckets(0, 1000)
                .unwrap();
        assert!(restricted.is_range_sparse(0.0));
    }

    #[test]
    fn test_build_time_sparse_domain_is_not_range_sparse() {
        // 1.5M keys over [1, 6M] (TPC-H `o_orderkey`): the domain is sparse, but
        // nothing has been filtered, so the synthesised positions are as good
        // as they will ever be.
        let mut histogram =
            TypedHistogramBuilder::from_ndv::<u64>(1_500_000, 1_500_000, Some((1, 6_000_000)), 100)
                .unwrap();
        let retention = histogram.synthetic_distinct_retention().unwrap();
        assert!((retention - 1.0).abs() < 1e-6, "{retention}");
        assert!(!histogram.is_range_sparse(0.5));

        // Thinning it is still detected.
        histogram.scale_counts(0.01);
        assert!(histogram.is_range_sparse(0.5));

        // A microsecond timestamp: 100k distinct seconds over ~28 hours.
        let start = 1_600_000_000_000_000_i64;
        let timestamp = TypedHistogramBuilder::from_ndv::<i64>(
            100_000,
            100_000,
            Some((start, start + 100_000 * 1_000_000)),
            100,
        )
        .unwrap();
        assert!(timestamp.avg_spacing.unwrap().is_finite());
        assert!(!timestamp.is_range_sparse(0.5));
    }

    #[test]
    fn test_float_histogram_thinning_is_range_sparse() {
        let mut histogram = TypedHistogramBuilder::from_ndv::<OrderedFloat<f64>>(
            1000,
            1000,
            Some((OrderedFloat(0.0), OrderedFloat(1000.0))),
            10,
        )
        .unwrap();
        assert!(!histogram.is_range_sparse(0.5));
        histogram.scale_counts(0.01);
        assert!(histogram.is_range_sparse(0.5));
    }

    #[test]
    fn test_collapse_folds_row_scale_into_distinct_counts() {
        let mut histogram =
            TypedHistogramBuilder::from_ndv::<u64>(73049, 73049, Some((0u64, 73048u64)), 10)
                .unwrap();
        histogram.scale_counts(0.01);
        let ndv_before = histogram.ndv().expected.unwrap();

        histogram.collapse_counts_to_distinct();
        assert_eq!(histogram.row_scale, 1.0);
        let ndv_after = histogram.ndv().expected.unwrap();
        assert!(
            (ndv_after - ndv_before).abs() < 1e-6,
            "{ndv_after} vs {ndv_before}"
        );
        for bucket in &histogram.buckets {
            assert_eq!(bucket.num_values(), bucket.num_distinct());
        }
        // Still thinned relative to synthesis.
        assert!(histogram.is_range_sparse(0.5));
    }

    #[test]
    fn test_join_of_build_time_sparse_histograms_is_not_range_sparse() {
        // orders.o_orderkey ⋈ lineitem.l_orderkey shape: both sides sparse at
        // synthesis with the same spacing; neither has been thinned.
        let left = TypedHistogramBuilder::from_ndv::<u64>(1500, 1500, Some((1, 6000)), 10).unwrap();
        let right =
            TypedHistogramBuilder::from_ndv::<u64>(1500, 6000, Some((1, 6000)), 10).unwrap();
        let joined = left.estimate_join(&right).histogram.unwrap();
        assert!(!joined.is_range_sparse(0.5), "{joined:?}");
    }

    #[test]
    fn test_estimate_histogram_join() {
        let left = Histogram::UInt(TypedHistogram {
            row_scale: 1.0,
            buckets: vec![TypedHistogramBucket::new(0, 10, 10.0, 10.0)],
            avg_spacing: None,
        });
        let right = Histogram::UInt(TypedHistogram {
            row_scale: 1.0,
            buckets: vec![TypedHistogramBucket::new(5, 15, 10.0, 10.0)],
            avg_spacing: None,
        });

        let estimation = left.estimate_join(&right).unwrap();

        assert_eq!(estimation.cardinality.expected, 5.0);
        assert_eq!(estimation.ndv.expected, Some(5.0));
    }

    #[test]
    fn test_estimate_histogram_join_rejects_mixed_numeric_types() {
        let left = Histogram::UInt(TypedHistogram {
            row_scale: 1.0,
            buckets: vec![TypedHistogramBucket::new(0, 10, 10.0, 10.0)],
            avg_spacing: None,
        });
        let right = Histogram::Int(TypedHistogram {
            row_scale: 1.0,
            buckets: vec![TypedHistogramBucket::new(5, 15, 10.0, 10.0)],
            avg_spacing: None,
        });

        let err = left.estimate_join(&right).unwrap_err();

        assert_eq!(
            err.message(),
            "cannot estimate join for histograms with different bucket types"
        );
    }
}
