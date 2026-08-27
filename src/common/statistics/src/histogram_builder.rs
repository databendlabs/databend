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
    use super::*;
    use crate::HistogramBucket;
    use crate::TypedHistogram;
    use crate::TypedHistogramBucket;

    #[test]
    fn test_histogram_builder_from_ndv_preserves_avg_spacing() {
        let histogram =
            HistogramBuilder::from_ndv(8, 16, Some((Datum::UInt(0), Datum::UInt(80))), 4).unwrap();
        let buckets = histogram.bucket_iter().collect::<Vec<_>>();

        assert!(!histogram.accuracy());
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
    fn test_is_histogram_range_distorted() {
        let histogram = Histogram::Float(TypedHistogram {
            accuracy: false,
            row_scale: 1.0,
            buckets: vec![],
            avg_spacing: Some(1e13),
        });

        assert!(histogram.is_range_distorted());
    }

    #[test]
    fn test_estimate_histogram_join() {
        let left = Histogram::UInt(TypedHistogram {
            accuracy: true,
            row_scale: 1.0,
            buckets: vec![TypedHistogramBucket::new(0, 10, 10.0, 10.0)],
            avg_spacing: None,
        });
        let right = Histogram::UInt(TypedHistogram {
            accuracy: true,
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
            accuracy: true,
            row_scale: 1.0,
            buckets: vec![TypedHistogramBucket::new(0, 10, 10.0, 10.0)],
            avg_spacing: None,
        });
        let right = Histogram::Int(TypedHistogram {
            accuracy: true,
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
