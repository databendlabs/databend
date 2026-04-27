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

use databend_common_exception::Result;

use crate::Datum;
use crate::F64;
use crate::Histogram;
use crate::HistogramBounds;
use crate::TypedHistogramBounds;
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

pub type UniformSampleSet = HistogramBounds;

impl HistogramBounds {
    pub fn has_intersection(&self, other: &HistogramBounds) -> Result<bool> {
        match (
            self.lower_bound(),
            self.upper_bound(),
            other.lower_bound(),
            other.upper_bound(),
        ) {
            (left_min, left_max, right_min, right_max)
                if left_min.is_numeric()
                    && left_max.is_numeric()
                    && right_min.is_numeric()
                    && right_max.is_numeric() =>
            {
                Ok(TypedHistogramBounds::new(
                    F64::from(left_min.as_double()?),
                    F64::from(left_max.as_double()?),
                )
                .has_intersection(&TypedHistogramBounds::new(
                    F64::from(right_min.as_double()?),
                    F64::from(right_max.as_double()?),
                )))
            }
            (
                Datum::Bytes(left_min),
                Datum::Bytes(left_max),
                Datum::Bytes(right_min),
                Datum::Bytes(right_max),
            ) => Ok(
                TypedHistogramBounds::new(left_min.clone(), left_max.clone()).has_intersection(
                    &TypedHistogramBounds::new(right_min.clone(), right_max.clone()),
                ),
            ),
            _ => Ok(false),
        }
    }

    pub fn intersection(&self, other: &HistogramBounds) -> Result<(Option<Datum>, Option<Datum>)> {
        match (
            self.lower_bound(),
            self.upper_bound(),
            other.lower_bound(),
            other.upper_bound(),
        ) {
            (
                Datum::Int(left_min),
                Datum::Int(left_max),
                Datum::Int(right_min),
                Datum::Int(right_max),
            ) => {
                let (min, max) = TypedHistogramBounds::new(*left_min, *left_max)
                    .intersection(&TypedHistogramBounds::new(*right_min, *right_max));
                Ok((min.map(Datum::Int), max.map(Datum::Int)))
            }
            (
                Datum::UInt(left_min),
                Datum::UInt(left_max),
                Datum::UInt(right_min),
                Datum::UInt(right_max),
            ) => {
                let (min, max) = TypedHistogramBounds::new(*left_min, *left_max)
                    .intersection(&TypedHistogramBounds::new(*right_min, *right_max));
                Ok((min.map(Datum::UInt), max.map(Datum::UInt)))
            }
            (
                Datum::Float(left_min),
                Datum::Float(left_max),
                Datum::Float(right_min),
                Datum::Float(right_max),
            ) => {
                let (min, max) = TypedHistogramBounds::new(*left_min, *left_max)
                    .intersection(&TypedHistogramBounds::new(*right_min, *right_max));
                Ok((min.map(Datum::Float), max.map(Datum::Float)))
            }
            (left_min, left_max, right_min, right_max)
                if left_min.is_numeric()
                    && left_max.is_numeric()
                    && right_min.is_numeric()
                    && right_max.is_numeric() =>
            {
                let (min, max) = TypedHistogramBounds::new(
                    F64::from(left_min.as_double()?),
                    F64::from(left_max.as_double()?),
                )
                .intersection(&TypedHistogramBounds::new(
                    F64::from(right_min.as_double()?),
                    F64::from(right_max.as_double()?),
                ));
                Ok((min.map(Datum::Float), max.map(Datum::Float)))
            }
            (
                Datum::Bytes(left_min),
                Datum::Bytes(left_max),
                Datum::Bytes(right_min),
                Datum::Bytes(right_max),
            ) => {
                let (min, max) =
                    TypedHistogramBounds::new(left_min.clone(), left_max.clone()).intersection(
                        &TypedHistogramBounds::new(right_min.clone(), right_max.clone()),
                    );
                Ok((min.map(Datum::Bytes), max.map(Datum::Bytes)))
            }
            _ => Ok((None, None)),
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
    fn test_uniform_sample_set_numeric_intersection() {
        let left = UniformSampleSet::new(Datum::UInt(0), Datum::UInt(10));
        let right = UniformSampleSet::new(Datum::UInt(5), Datum::UInt(15));

        assert!(left.has_intersection(&right).unwrap());
        assert_eq!(
            left.intersection(&right).unwrap(),
            (Some(Datum::UInt(5)), Some(Datum::UInt(10)))
        );
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
            buckets: vec![],
            avg_spacing: Some(1e13),
        });

        assert!(histogram.is_range_distorted());
    }

    #[test]
    fn test_estimate_histogram_join() {
        let left = Histogram::UInt(TypedHistogram {
            accuracy: true,
            buckets: vec![TypedHistogramBucket::new(0, 10, 10.0, 10.0)],
            avg_spacing: None,
        });
        let right = Histogram::UInt(TypedHistogram {
            accuracy: true,
            buckets: vec![TypedHistogramBucket::new(5, 15, 10.0, 10.0)],
            avg_spacing: None,
        });

        let estimation = left.estimate_join(&right).unwrap();

        assert_eq!(estimation.cardinality.expected, 5.0);
        assert_eq!(estimation.ndv.expected, 5.0);
    }

    #[test]
    fn test_estimate_histogram_join_rejects_mixed_numeric_types() {
        let left = Histogram::UInt(TypedHistogram {
            accuracy: true,
            buckets: vec![TypedHistogramBucket::new(0, 10, 10.0, 10.0)],
            avg_spacing: None,
        });
        let right = Histogram::Int(TypedHistogram {
            accuracy: true,
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
