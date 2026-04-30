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

use databend_common_statistics::Histogram;
pub use databend_common_statistics::Ndv;

use crate::Domain;
use crate::Scalar;
use crate::types::DataType;
use crate::types::boolean::BooleanDomain;
use crate::types::nullable::NullableDomain;

#[derive(Debug, Clone)]
pub struct StatDistribution<D> {
    pub domain: Domain,
    pub ndv: Ndv,
    pub null_count: u64,
    pub distribution: D,
}

pub type ArgStat<'a> = StatDistribution<BorrowedDistribution<'a>>;
pub type ReturnStat = StatDistribution<OwnedDistribution>;

pub trait DistributionInvariant {
    fn check_distribution(&self, stat: &StatDistribution<Self>) -> Result<(), String>
    where Self: Sized;
}

impl<D: DistributionInvariant> StatDistribution<D> {
    pub fn check_consistency(&self) -> Result<(), String> {
        self.check_consistency_with_type(None)
    }

    pub fn check_consistency_with_type(&self, data_type: Option<&DataType>) -> Result<(), String> {
        if !self.ndv.value().is_finite() || self.ndv.value() < 0.0 {
            return Err(format!("invalid ndv {:?}", self.ndv));
        }
        if let Some(data_type) = data_type {
            if data_type.has_generic() {
                return Err(format!(
                    "statistics consistency check requires concrete data type, got {data_type:?}"
                ));
            }
            self.domain.check_data_type(data_type)?;
        }
        if let Domain::Nullable(domain) = &self.domain {
            if self.null_count != 0 && !domain.has_null {
                return Err(
                    "null_count is positive but nullable domain has_null is false".to_string(),
                );
            }
            if !domain.has_null && domain.value.is_none() {
                return Err("nullable domain without nulls must carry a value domain".to_string());
            }
        } else if self.null_count != 0 {
            return Err("non-nullable domain has positive null_count".to_string());
        }
        self.distribution.check_distribution(self)
    }
}

impl<D> StatDistribution<D> {
    pub fn singleton(&self) -> Option<Scalar> {
        self.domain.as_singleton()
    }

    pub fn has_null(&self) -> bool {
        self.null_count != 0
            || matches!(
                self.domain,
                Domain::Nullable(NullableDomain { has_null: true, .. })
            )
    }
}

impl<'a> ArgStat<'a> {
    pub fn histogram(&self) -> Option<&'a Histogram> {
        self.distribution.as_histogram()
    }

    pub fn boolean_distribution(&self) -> Option<&BooleanDistribution> {
        self.distribution.as_boolean()
    }
}

impl ReturnStat {
    pub fn histogram(&self) -> Option<&Histogram> {
        self.distribution.as_histogram()
    }

    pub fn boolean_distribution(&self) -> Option<&BooleanDistribution> {
        self.distribution.as_boolean()
    }
}

#[derive(Debug, Clone)]
pub struct StatUnaryArg<'a> {
    pub cardinality: f64,
    pub args: &'a [ArgStat<'a>; 1],
}

#[derive(Debug, Clone)]
pub struct StatBinaryArg<'a> {
    pub cardinality: f64,
    pub args: &'a [ArgStat<'a>; 2],
}

#[derive(Debug, Clone)]
pub struct StatArgs<'a> {
    pub cardinality: f64,
    pub args: &'a [ArgStat<'a>],
}

#[derive(Debug, Clone, Copy)]
pub enum BorrowedDistribution<'a> {
    Unknown,
    Histogram(&'a Histogram),
    Boolean(BooleanDistribution),
}

impl<'a> BorrowedDistribution<'a> {
    pub fn as_histogram(&self) -> Option<&'a Histogram> {
        match self {
            BorrowedDistribution::Histogram(histogram) => Some(*histogram),
            BorrowedDistribution::Unknown | BorrowedDistribution::Boolean(_) => None,
        }
    }

    pub fn as_boolean(&self) -> Option<&BooleanDistribution> {
        match self {
            BorrowedDistribution::Boolean(distribution) => Some(distribution),
            BorrowedDistribution::Unknown | BorrowedDistribution::Histogram(_) => None,
        }
    }
}

#[derive(Debug, Clone)]
pub enum OwnedDistribution {
    Unknown,
    Histogram(Histogram),
    Boolean(BooleanDistribution),
}

impl OwnedDistribution {
    pub fn as_histogram(&self) -> Option<&Histogram> {
        match self {
            OwnedDistribution::Histogram(histogram) => Some(histogram),
            OwnedDistribution::Unknown | OwnedDistribution::Boolean(_) => None,
        }
    }

    pub fn as_boolean(&self) -> Option<&BooleanDistribution> {
        match self {
            OwnedDistribution::Boolean(distribution) => Some(distribution),
            OwnedDistribution::Unknown | OwnedDistribution::Histogram(_) => None,
        }
    }

    pub fn as_borrowed_distribution(&self) -> BorrowedDistribution<'_> {
        match self {
            OwnedDistribution::Unknown => BorrowedDistribution::Unknown,
            OwnedDistribution::Histogram(histogram) => BorrowedDistribution::Histogram(histogram),
            OwnedDistribution::Boolean(distribution) => {
                BorrowedDistribution::Boolean(*distribution)
            }
        }
    }
}

impl<'a> DistributionInvariant for BorrowedDistribution<'a> {
    fn check_distribution(&self, stat: &StatDistribution<Self>) -> Result<(), String> {
        match self {
            BorrowedDistribution::Unknown => Ok(()),
            BorrowedDistribution::Histogram(histogram) => {
                check_histogram_distribution(stat, histogram)
            }
            BorrowedDistribution::Boolean(distribution) => {
                check_boolean_distribution(stat, distribution)
            }
        }
    }
}

impl DistributionInvariant for OwnedDistribution {
    fn check_distribution(&self, stat: &StatDistribution<Self>) -> Result<(), String> {
        match self {
            OwnedDistribution::Unknown => Ok(()),
            OwnedDistribution::Histogram(histogram) => {
                check_histogram_distribution(stat, histogram)
            }
            OwnedDistribution::Boolean(distribution) => {
                check_boolean_distribution(stat, distribution)
            }
        }
    }
}

fn check_histogram_distribution<D>(
    stat: &StatDistribution<D>,
    histogram: &Histogram,
) -> Result<(), String> {
    let num_values = histogram.num_values();
    if !num_values.is_finite() || num_values < 0.0 {
        return Err(format!("histogram num_values is invalid: {num_values}"));
    }
    let histogram_ndv = histogram.num_distinct_values();
    if !histogram_ndv.is_finite() || histogram_ndv < 0.0 {
        return Err(format!("histogram ndv is invalid: {histogram_ndv}"));
    }
    if matches!(
        stat.domain,
        Domain::Nullable(NullableDomain { value: None, .. })
    ) {
        return Err("histogram distribution requires a non-null value domain".to_string());
    }
    Ok(())
}

fn check_boolean_distribution<D>(
    stat: &StatDistribution<D>,
    distribution: &BooleanDistribution,
) -> Result<(), String> {
    let valid_domain = match &stat.domain {
        Domain::Boolean(_) => true,
        Domain::Nullable(NullableDomain {
            value: Some(box Domain::Boolean(_)),
            ..
        }) => true,
        Domain::Nullable(NullableDomain { value: None, .. }) => {
            distribution.true_count.upper == 0.0
        }
        _ => false,
    };
    if !valid_domain {
        return Err(format!(
            "boolean distribution requires boolean non-null value domain, got {:?}",
            stat.domain
        ));
    }
    distribution.true_count.check_consistency()?;
    Ok(())
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct StatEstimate {
    pub lower: f64,
    pub expected: f64,
    pub upper: f64,
}

impl StatEstimate {
    pub fn check_consistency(&self) -> Result<(), String> {
        if !self.lower.is_finite() || !self.expected.is_finite() || !self.upper.is_finite() {
            return Err(format!("estimate must be finite: {:?}", self));
        }
        if self.lower < 0.0 {
            return Err(format!(
                "estimate lower bound must be non-negative: {:?}",
                self
            ));
        }
        if self.lower > self.expected || self.expected > self.upper {
            return Err(format!("estimate bounds are inconsistent: {:?}", self));
        }
        Ok(())
    }

    pub fn new(lower: f64, expected: f64, upper: f64) -> Self {
        let lower = lower.max(0.0);
        let upper = upper.max(lower);
        let expected = expected.clamp(lower, upper);
        Self {
            lower,
            expected,
            upper,
        }
    }

    pub fn exact(value: f64) -> Self {
        let value = value.max(0.0);
        Self {
            lower: value,
            expected: value,
            upper: value,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct BooleanDistribution {
    pub true_count: StatEstimate,
}

#[cfg(test)]
mod tests {
    use databend_common_statistics::Histogram;
    use databend_common_statistics::TypedHistogram;

    use super::*;
    use crate::types::boolean::BooleanDomain;

    #[test]
    fn test_empty_histogram_requires_non_null_value_domain() {
        let stat = ReturnStat {
            domain: Domain::Nullable(NullableDomain {
                has_null: true,
                value: None,
            }),
            ndv: Ndv::Stat(0.0),
            null_count: 10,
            distribution: OwnedDistribution::Histogram(Histogram::Int(TypedHistogram::new(
                vec![],
                true,
            ))),
        };

        let err = stat.check_consistency().unwrap_err();
        assert!(err.contains("non-null value domain"));
    }

    #[test]
    fn test_nullable_boolean_distribution_checks_non_null_value_domain() {
        let valid = ReturnStat {
            domain: Domain::Nullable(NullableDomain {
                has_null: true,
                value: Some(Box::new(Domain::Boolean(BooleanDomain {
                    has_true: true,
                    has_false: true,
                }))),
            }),
            ndv: Ndv::Stat(2.0),
            null_count: 1,
            distribution: OwnedDistribution::Boolean(BooleanDistribution {
                true_count: StatEstimate::exact(1.0),
            }),
        };

        valid.check_consistency().unwrap();

        let all_null = ReturnStat {
            domain: Domain::Nullable(NullableDomain {
                has_null: true,
                value: None,
            }),
            ndv: Ndv::Stat(0.0),
            null_count: 10,
            distribution: OwnedDistribution::Boolean(BooleanDistribution {
                true_count: StatEstimate::exact(0.0),
            }),
        };

        all_null.check_consistency().unwrap();

        let invalid = ReturnStat {
            domain: Domain::Nullable(NullableDomain {
                has_null: true,
                value: None,
            }),
            ndv: Ndv::Max(1.0),
            null_count: 10,
            distribution: OwnedDistribution::Boolean(BooleanDistribution {
                true_count: StatEstimate::new(0.0, 0.5, 1.0),
            }),
        };

        let err = invalid.check_consistency().unwrap_err();
        assert!(err.contains("boolean non-null value domain"));
    }
}
