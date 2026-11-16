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
        spacing_unit: f64,
    ) -> std::result::Result<Histogram, String> {
        let Some((min, max)) = bound else {
            return TypedHistogramBuilder::from_ndv::<F64>(
                ndv,
                num_rows,
                None,
                num_buckets,
                spacing_unit,
            )
            .map(Histogram::Float);
        };

        match (min, max) {
            (Datum::Int(min), Datum::Int(max)) => TypedHistogramBuilder::from_ndv(
                ndv,
                num_rows,
                Some((min, max)),
                num_buckets,
                spacing_unit,
            )
            .map(Histogram::Int),
            (Datum::UInt(min), Datum::UInt(max)) => TypedHistogramBuilder::from_ndv(
                ndv,
                num_rows,
                Some((min, max)),
                num_buckets,
                spacing_unit,
            )
            .map(Histogram::UInt),
            (Datum::Float(min), Datum::Float(max)) => TypedHistogramBuilder::from_ndv(
                ndv,
                num_rows,
                Some((min, max)),
                num_buckets,
                spacing_unit,
            )
            .map(Histogram::Float),
            (Datum::Bytes(min), Datum::Bytes(max)) => TypedHistogramBuilder::from_ndv(
                ndv,
                num_rows,
                Some((min, max)),
                num_buckets,
                spacing_unit,
            )
            .map(Histogram::Bytes),
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
    fn test_histogram_builder_from_ndv_preserves_bounds() {
        let histogram =
            HistogramBuilder::from_ndv(8, 16, Some((Datum::UInt(0), Datum::UInt(80))), 4, 1.0)
                .unwrap();
        let buckets = histogram.bucket_iter().collect::<Vec<_>>();

        assert_eq!(histogram.num_buckets(), 4);
        assert_eq!(buckets.first().unwrap().lower_bound(), Datum::UInt(0));
        assert_eq!(buckets.last().unwrap().upper_bound(), Datum::UInt(80));

        for unit in [0.0, -1.0, f64::INFINITY, f64::NAN] {
            assert!(
                HistogramBuilder::from_ndv(8, 16, Some((Datum::UInt(0), Datum::UInt(80))), 4, unit)
                    .is_err()
            );
        }
        let sparse = HistogramBuilder::from_ndv(
            1000,
            1000,
            Some((Datum::UInt(0), Datum::UInt(1_000_000))),
            100,
            1.0,
        )
        .unwrap();
        assert!(sparse.is_range_sparse(0.5));
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
            TypedHistogramBuilder::from_ndv::<u64>(73049, 73049, Some((0u64, 73048u64)), 10, 1.0)
                .unwrap();
        let retention = histogram.distinct_retention();
        assert!((retention - 1.0).abs() < 1e-3, "{retention}");
        assert!(!histogram.is_range_sparse(0.5));

        // A filter on other columns keeps one year: 365 of 73049 values (0.5%).
        histogram.scale_counts(365.0 / 73049.0);
        let retention = histogram.distinct_retention();
        assert!((retention - 0.005).abs() < 0.001, "{retention}");
        assert!(histogram.is_range_sparse(0.5));
        assert!(!histogram.is_range_sparse(0.001));

        // Keeping 90% of the values is not sparse at a 50% threshold.
        let mut light =
            TypedHistogramBuilder::from_ndv::<u64>(73049, 73049, Some((0u64, 73048u64)), 10, 1.0)
                .unwrap();
        light.scale_counts(0.9);
        assert!(!light.is_range_sparse(0.5));
    }

    #[test]
    fn test_range_sparse_survives_row_scale_materialisation() {
        let mut histogram =
            TypedHistogramBuilder::from_ndv::<u64>(73049, 73049, Some((0u64, 73048u64)), 10, 1.0)
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
        // `row_scale == 1`; folded retention preserves the same range check.
        let dense =
            TypedHistogramBuilder::from_ndv::<u64>(73049, 73049, Some((0u64, 73048u64)), 10, 1.0)
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
            TypedHistogramBuilder::from_ndv::<u64>(73049, 73049, Some((0u64, 73048u64)), 10, 1.0)
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
            1.0,
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
            TypedHistogramBuilder::from_ndv::<u64>(100, 1_000_000, Some((0u64, 99u64)), 10, 1.0)
                .unwrap();
        histogram.scale_counts(0.1);
        assert!((histogram.row_scale - 0.1).abs() < 1e-12);
        let retention = histogram.distinct_retention();
        assert!(retention > 0.95, "{retention}");
        assert!(!histogram.is_range_sparse(0.5));
    }

    #[test]
    fn test_sampled_histogram_is_range_sparse_after_thinning() {
        let histogram = Histogram::UInt(TypedHistogram {
            row_scale: 1e-6,
            distinct_retention: 1.0,
            buckets: vec![TypedHistogramBucket::new(0u64, 10u64, 1.0, 1.0)],
        });
        assert!(histogram.is_range_sparse(0.5));
        assert!(histogram.is_range_sparse(1.0));
    }

    #[test]
    fn test_sampled_range_sparse_survives_materialization() {
        let dense = TypedHistogram::new(vec![TypedHistogramBucket::new(0u64, 999, 1000.0, 1000.0)]);
        assert!(!dense.is_range_sparse(0.5));
        // Restricting the column's own range is not independent thinning.
        let restricted = dense.restrict_discrete_buckets(0, 9).unwrap();
        assert!(!restricted.is_range_sparse(0.5));
        assert_eq!(restricted.distinct_retention(), 1.0);

        let mut thinned = dense.clone();
        thinned.scale_counts(0.25);
        assert_eq!(thinned.distinct_retention(), 0.25);
        assert!(thinned.is_range_sparse(0.5));
        let restricted = thinned.restrict_discrete_buckets(0, 499).unwrap();
        assert_eq!(restricted.row_scale, 1.0);
        assert_eq!(restricted.distinct_retention(), 0.25);
        assert!(restricted.is_range_sparse(0.5));
        let mut collapsed = thinned.clone();
        collapsed.collapse_counts_to_distinct();
        assert_eq!(collapsed.row_scale, 1.0);
        assert_eq!(collapsed.distinct_retention(), 0.25);
        assert!(collapsed.is_range_sparse(0.5));
        let joined = thinned.estimate_join(&dense);
        for histogram in [
            joined.histogram,
            joined.left_matched_histogram,
            joined.right_matched_histogram,
        ] {
            assert!(histogram.unwrap().is_range_sparse(0.5));
        }

        // Cumulative independent thinning must survive a row_scale reset.
        let mut sequential = dense.clone();
        sequential.scale_counts(0.75);
        sequential = sequential.restrict_discrete_buckets(0, 499).unwrap();
        sequential.scale_counts(0.75);
        assert!(!sequential.is_range_sparse(0.5));
        sequential.collapse_counts_to_distinct();
        sequential.scale_counts(0.75);
        assert!(sequential.is_range_sparse(0.5));

        // Row loss alone is not value loss, including in a naturally sparse
        // sampled domain. High multiplicity keeps nearly all distinct values.
        let mut duplicated = TypedHistogram::new(vec![TypedHistogramBucket::new(
            0u64,
            1_000_000,
            1_000_000.0,
            100.0,
        )]);
        duplicated.scale_counts(0.1);
        assert!(duplicated.distinct_retention() > 0.95);
        assert!(!duplicated.is_range_sparse(0.5));
    }

    #[test]
    fn test_sampled_float_and_bytes_sparse_survive_restriction() {
        let mut float = TypedHistogram::new(vec![TypedHistogramBucket::new(
            OrderedFloat(0.0),
            OrderedFloat(1000.0),
            1000.0,
            1000.0,
        )]);
        float.scale_counts(0.01);
        let restricted = float
            .restrict_float_buckets(OrderedFloat(0.0), OrderedFloat(500.0))
            .unwrap();
        assert_eq!(restricted.row_scale, 1.0);
        assert!(restricted.is_range_sparse(0.5));

        let mut bytes = TypedHistogram::new(vec![TypedHistogramBucket::new(
            b"a".to_vec(),
            b"z".to_vec(),
            1000.0,
            1000.0,
        )]);
        bytes.scale_counts(0.01);
        let restricted = bytes.restrict_bytes_buckets(b"a", b"m").unwrap();
        assert_eq!(restricted.row_scale, 1.0);
        assert!(restricted.is_range_sparse(0.5));
    }

    #[test]
    fn test_synthetic_histogram_with_outlier_bounds_is_range_sparse() {
        // A sentinel max makes the synthesised bucket width ~1e17: the bounds
        // say nothing about where the 1000 real values are.
        let histogram =
            TypedHistogramBuilder::from_ndv::<i64>(1000, 1000, Some((0, i64::MAX)), 100, 1.0)
                .unwrap();
        assert!(histogram.distinct_retention() < 1e-12);
        assert!(histogram.is_range_sparse(0.5));
        let signed_range = TypedHistogramBuilder::from_ndv::<i64>(
            1000,
            1000,
            Some((i64::MIN, i64::MAX)),
            100,
            1.0,
        )
        .unwrap();
        assert!(signed_range.distinct_retention() < 1e-12);
        let float_range = TypedHistogramBuilder::from_ndv::<OrderedFloat<f64>>(
            1000,
            1000,
            Some((OrderedFloat(-f64::MAX), OrderedFloat(f64::MAX))),
            100,
            1.0,
        )
        .unwrap();
        assert_eq!(float_range.distinct_retention(), 0.0);

        let histogram = HistogramBuilder::from_ndv(
            1000,
            1000,
            Some((Datum::Int(0), Datum::Int(i64::MAX))),
            100,
            1.0,
        )
        .unwrap();
        assert!(histogram.is_range_sparse(0.5));

        // The distortion is a property of synthesis; restricting the range
        // does not make the positions trustworthy again.
        let restricted =
            TypedHistogramBuilder::from_ndv::<i64>(1000, 1000, Some((0, i64::MAX)), 100, 1.0)
                .unwrap()
                .restrict_discrete_buckets(0, 1000)
                .unwrap();
        assert!(restricted.is_range_sparse(0.5));
    }

    #[test]
    fn test_build_time_sparse_domain_is_not_range_sparse() {
        // 1.5M keys over [1, 6M] (TPC-H `o_orderkey`): the domain is sparse, but
        // nothing has been filtered, so the synthesised positions are as good
        // as they will ever be.
        let mut histogram = TypedHistogramBuilder::from_ndv::<u64>(
            1_500_000,
            1_500_000,
            Some((1, 6_000_000)),
            100,
            1.0,
        )
        .unwrap();
        let retention = histogram.distinct_retention();
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
            86_400.0 * 1_000_000.0,
        )
        .unwrap();
        assert_eq!(timestamp.distinct_retention(), 1.0);
        assert!(!timestamp.is_range_sparse(0.5));

        // The same distribution in days or microseconds has the same score.
        // Even long timestamp ranges must not be rejected by a raw bucket-width
        // threshold: that would change the meaning of the dimensionless cutoff.
        let micros_per_day = 86_400_i64 * 1_000_000;
        for unit in [1_i64, micros_per_day] {
            let mut histogram = TypedHistogramBuilder::from_ndv::<i64>(
                1000,
                1000,
                Some((0, 80_000 * unit)),
                100,
                unit as f64,
            )
            .unwrap();
            assert!((histogram.distinct_retention() - 0.8).abs() < 1e-12);
            histogram.scale_counts(0.75);
            assert!((histogram.distinct_retention() - 0.6).abs() < 1e-12);
            assert!(!histogram.is_range_sparse(0.5));
            let restricted = histogram
                .restrict_discrete_buckets(0, 40_000 * unit)
                .unwrap();
            assert!((restricted.distinct_retention() - 0.6).abs() < 1e-12);
            let mut collapsed = restricted;
            collapsed.collapse_counts_to_distinct();
            assert!((collapsed.distinct_retention() - 0.6).abs() < 1e-12);
            // Combined support, not just NDV survival: .8 × .75 × .8 < .5.
            collapsed.scale_counts(0.8);
            assert!(collapsed.is_range_sparse(0.5));
        }
    }

    #[test]
    fn test_float_histogram_thinning_is_range_sparse() {
        let mut histogram = TypedHistogramBuilder::from_ndv::<OrderedFloat<f64>>(
            1000,
            1000,
            Some((OrderedFloat(0.0), OrderedFloat(1000.0))),
            10,
            1.0,
        )
        .unwrap();
        assert!(!histogram.is_range_sparse(0.5));
        histogram.scale_counts(0.01);
        assert!(histogram.is_range_sparse(0.5));
    }

    #[test]
    fn test_collapse_folds_row_scale_into_distinct_counts() {
        let mut histogram =
            TypedHistogramBuilder::from_ndv::<u64>(73049, 73049, Some((0u64, 73048u64)), 10, 1.0)
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
        let left =
            TypedHistogramBuilder::from_ndv::<u64>(1500, 1500, Some((1, 6000)), 10, 1.0).unwrap();
        let right =
            TypedHistogramBuilder::from_ndv::<u64>(1500, 6000, Some((1, 6000)), 10, 1.0).unwrap();
        let joined = left.estimate_join(&right).histogram.unwrap();
        assert!(!joined.is_range_sparse(0.5), "{joined:?}");

        // Observed singleton positions remain useful even over a wide range;
        // a synthetic input must not inject a second density criterion.
        let dense = TypedHistogramBuilder::from_ndv::<u64>(
            1_000_000,
            1_000_000,
            Some((0, 999_999)),
            100,
            1.0,
        )
        .unwrap();
        let observed = TypedHistogram::new(
            (0..1000)
                .map(|i| TypedHistogramBucket::new(i * 1000, i * 1000, 1.0, 1.0))
                .collect(),
        );
        let joined = dense.estimate_join(&observed).histogram.unwrap();
        assert!(!joined.is_range_sparse(0.5), "{joined:?}");

        let sparse =
            TypedHistogramBuilder::from_ndv::<u64>(1000, 1000, Some((0, 999_999)), 100, 1.0)
                .unwrap();
        let joined = dense.estimate_join(&sparse).histogram.unwrap();
        assert!(joined.is_range_sparse(0.5), "{joined:?}");
    }

    #[test]
    fn test_estimate_histogram_join() {
        let left = Histogram::UInt(TypedHistogram {
            row_scale: 1.0,
            distinct_retention: 1.0,
            buckets: vec![TypedHistogramBucket::new(0, 10, 10.0, 10.0)],
        });
        let right = Histogram::UInt(TypedHistogram {
            row_scale: 1.0,
            distinct_retention: 1.0,
            buckets: vec![TypedHistogramBucket::new(5, 15, 10.0, 10.0)],
        });

        let estimation = left.estimate_join(&right).unwrap();

        assert_eq!(estimation.cardinality.expected, 5.0);
        assert_eq!(estimation.ndv.expected, Some(5.0));
    }

    #[test]
    fn test_estimate_histogram_join_rejects_mixed_numeric_types() {
        let left = Histogram::UInt(TypedHistogram {
            row_scale: 1.0,
            distinct_retention: 1.0,
            buckets: vec![TypedHistogramBucket::new(0, 10, 10.0, 10.0)],
        });
        let right = Histogram::Int(TypedHistogram {
            row_scale: 1.0,
            distinct_retention: 1.0,
            buckets: vec![TypedHistogramBucket::new(5, 15, 10.0, 10.0)],
        });

        let err = left.estimate_join(&right).unwrap_err();

        assert_eq!(
            err.message(),
            "cannot estimate join for histograms with different bucket types"
        );
    }
}
