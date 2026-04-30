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

use std::cmp::Ordering;
use std::marker::PhantomData;

use crate::Scalar;
use crate::property::Domain;
use crate::stat_distribution::ArgStat;
use crate::stat_distribution::BooleanDistribution;
use crate::stat_distribution::Ndv;
use crate::stat_distribution::OwnedDistribution;
use crate::stat_distribution::ReturnStat;
use crate::stat_distribution::StatBinaryArg;
use crate::stat_distribution::StatEstimate;
use crate::types::boolean::BooleanDomain;
use crate::types::nullable::NullableDomain;

pub trait StatComparisonOp {
    type Reverse: StatComparisonOp;

    const SELECT_LESS: bool;
    const INCLUDE_EQUAL: bool;

    fn estimate_minmax_range_true_count(
        ndv: Ndv,
        cardinality: f64,
        cmp_min: Ordering,
        cmp_max: Ordering,
    ) -> Option<StatEstimate> {
        if Self::SELECT_LESS {
            if matches!(cmp_min, Ordering::Less)
                || (!Self::INCLUDE_EQUAL && matches!(cmp_min, Ordering::Equal))
            {
                return Some(StatEstimate::exact(0.0));
            }
            if matches!(cmp_max, Ordering::Greater)
                || (Self::INCLUDE_EQUAL && matches!(cmp_max, Ordering::Equal))
            {
                return Some(StatEstimate::exact(cardinality));
            }
            if Self::INCLUDE_EQUAL && matches!(cmp_min, Ordering::Equal) {
                return Some(estimate_ndv_true_count(ndv, false, cardinality));
            }
            if !Self::INCLUDE_EQUAL && matches!(cmp_max, Ordering::Equal) {
                return Some(estimate_ndv_true_count(ndv, true, cardinality));
            }
        } else {
            if matches!(cmp_max, Ordering::Greater)
                || (!Self::INCLUDE_EQUAL && matches!(cmp_max, Ordering::Equal))
            {
                return Some(StatEstimate::exact(0.0));
            }
            if matches!(cmp_min, Ordering::Less)
                || (Self::INCLUDE_EQUAL && matches!(cmp_min, Ordering::Equal))
            {
                return Some(StatEstimate::exact(cardinality));
            }
            if Self::INCLUDE_EQUAL && matches!(cmp_max, Ordering::Equal) {
                return Some(estimate_ndv_true_count(ndv, false, cardinality));
            }
            if !Self::INCLUDE_EQUAL && matches!(cmp_min, Ordering::Equal) {
                return Some(estimate_ndv_true_count(ndv, true, cardinality));
            }
        }

        None
    }
}

#[derive(Default, Clone, Copy)]
pub struct LtOp;
#[derive(Default, Clone, Copy)]
pub struct LteOp;
#[derive(Default, Clone, Copy)]
pub struct GtOp;
#[derive(Default, Clone, Copy)]
pub struct GteOp;

impl StatComparisonOp for LtOp {
    type Reverse = GtOp;

    const SELECT_LESS: bool = true;
    const INCLUDE_EQUAL: bool = false;
}

impl StatComparisonOp for LteOp {
    type Reverse = GteOp;

    const SELECT_LESS: bool = true;
    const INCLUDE_EQUAL: bool = true;
}

impl StatComparisonOp for GtOp {
    type Reverse = LtOp;

    const SELECT_LESS: bool = false;
    const INCLUDE_EQUAL: bool = false;
}

impl StatComparisonOp for GteOp {
    type Reverse = LteOp;

    const SELECT_LESS: bool = false;
    const INCLUDE_EQUAL: bool = true;
}

pub struct ConstantComparison<'s, 'a, A: ConstantComparisonAdapter> {
    pub stat: &'s ArgStat<'a>,
    pub constant: A::Value,
    pub domain: Option<A::Domain>,
    pub non_null_cardinality: f64,
    pub null_count: u64,
    pub nullable: bool,
    _a: PhantomData<fn(A)>,
}

pub trait ConstantComparisonAdapter {
    type Value;
    type Domain;

    fn constant(scalar: Scalar) -> Result<Self::Value, String>;

    fn domain(domain: &Domain) -> Result<Self::Domain, String>;

    fn compare(left: &Self::Value, right: &Self::Value) -> Ordering;
}

impl<'s, 'a, A: ConstantComparisonAdapter> ConstantComparison<'s, 'a, A> {
    pub fn from_constant_args(stat: &'s StatBinaryArg<'a>) -> Result<Option<(Self, bool)>, String> {
        if let Some(input) =
            Self::new(&stat.args[0], &stat.args[1], stat.cardinality)?.map(|input| (input, false))
        {
            return Ok(Some(input));
        }
        Ok(Self::new(&stat.args[1], &stat.args[0], stat.cardinality)?.map(|input| (input, true)))
    }

    fn new(
        stat: &'s ArgStat<'a>,
        constant_stat: &ArgStat<'_>,
        input_cardinality: f64,
    ) -> Result<Option<Self>, String> {
        let Some(constant) = constant_stat.singleton() else {
            return Ok(None);
        };
        if constant.is_null() {
            return Err(
                "constant comparison null constant was not handled before typed comparison"
                    .to_string(),
            );
        }
        let nullable = stat.domain.is_nullable() || constant_stat.domain.is_nullable();
        let null_count = stat.null_count.min(input_cardinality.ceil() as u64);
        let non_null_cardinality = (input_cardinality - null_count as f64).max(0.0);
        let domain = match &stat.domain {
            Domain::Nullable(NullableDomain { value: None, .. }) => None,
            Domain::Nullable(NullableDomain {
                value: Some(box domain),
                ..
            })
            | domain => match A::domain(domain) {
                Ok(domain) => Some(domain),
                Err(err) => {
                    return Err(err);
                }
            },
        };
        let constant = match A::constant(constant) {
            Ok(constant) => constant,
            Err(err) => {
                return Err(err);
            }
        };

        Ok(Some(Self {
            stat,
            constant,
            domain,
            non_null_cardinality,
            null_count,
            nullable,
            _a: PhantomData,
        }))
    }

    pub fn boolean_stat(&self, true_count: StatEstimate) -> ReturnStat {
        let domain = if self.nullable {
            Domain::Nullable(NullableDomain {
                has_null: self.null_count != 0,
                value: Some(Box::new(Domain::Boolean(BooleanDomain {
                    has_true: true,
                    has_false: true,
                }))),
            })
        } else {
            Domain::Boolean(BooleanDomain {
                has_true: true,
                has_false: true,
            })
        };

        ReturnStat {
            domain,
            ndv: Ndv::Stat(2.0),
            null_count: self.null_count,
            distribution: OwnedDistribution::Boolean(BooleanDistribution { true_count }),
        }
    }

    pub fn constant_equality_true_count(
        &self,
        minmax_cmp: Option<(Ordering, Ordering)>,
        not_eq: bool,
    ) -> StatEstimate {
        let Some((cmp_min, cmp_max)) = minmax_cmp else {
            return estimate_ndv_true_count(self.stat.ndv, not_eq, self.non_null_cardinality);
        };
        if cmp_min == Ordering::Less || cmp_max == Ordering::Greater {
            return StatEstimate::exact(if not_eq {
                self.non_null_cardinality
            } else {
                0.0
            });
        }

        estimate_ndv_true_count(self.stat.ndv, not_eq, self.non_null_cardinality)
    }
}

pub fn null_comparison_stat(stat: &StatBinaryArg) -> Option<ReturnStat> {
    if stat.args.iter().any(|arg| {
        arg.domain
            .as_singleton()
            .is_some_and(|scalar| scalar.is_null())
    }) {
        Some(ReturnStat {
            domain: Domain::Nullable(NullableDomain {
                has_null: true,
                value: None,
            }),
            ndv: Ndv::Stat(0.0),
            null_count: stat.cardinality.ceil() as u64,
            distribution: OwnedDistribution::Unknown,
        })
    } else {
        None
    }
}

pub fn estimate_ndv_true_count(ndv: Ndv, not_eq: bool, cardinality: f64) -> StatEstimate {
    let value = ndv.value();
    let selectivity = if value == 0.0 {
        0.0
    } else if not_eq {
        1.0 - 1.0 / value
    } else {
        1.0 / value
    };
    let expected = selectivity * cardinality;
    match ndv {
        Ndv::Stat(_) => StatEstimate::exact(expected),
        Ndv::Max(_) => StatEstimate::new(0.0, expected, cardinality),
    }
}
