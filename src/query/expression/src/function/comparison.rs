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
use crate::stat_distribution::OwnedDistribution;
use crate::stat_distribution::ReturnStat;
use crate::stat_distribution::StatBinaryArg;
use crate::stat_distribution::StatCardinality;
use crate::stat_distribution::StatCount;
use crate::stat_distribution::StatEstimate;
use crate::types::boolean::BooleanDomain;
use crate::types::nullable::NullableDomain;

pub trait StatComparisonOp {
    type Reverse: StatComparisonOp;

    const SELECT_LESS: bool;
    const INCLUDE_EQUAL: bool;

    fn range_true_count(
        ndv: StatEstimate,
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
    pub null_count: StatCount,
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
    pub fn from_args(stat: &'s StatBinaryArg<'a>) -> Result<Option<(Self, bool)>, String> {
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
        input_cardinality: StatCardinality,
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
        let cardinality = input_cardinality.value();
        let null_count = stat.effective_null_count(input_cardinality);
        let non_null_cardinality = (cardinality - null_count.expected()).max(0.0);
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
        let has_true = true_count.upper > 0.0;
        let has_false = true_count.lower < self.non_null_cardinality;
        let boolean_domain = BooleanDomain {
            has_true,
            has_false,
        };
        let value_domain = (has_true || has_false || self.null_count.upper() == 0.0)
            .then(|| Box::new(Domain::Boolean(boolean_domain)));
        let domain = if self.nullable {
            Domain::Nullable(NullableDomain {
                has_null: self.null_count.upper() > 0.0,
                value: value_domain,
            })
        } else {
            Domain::Boolean(boolean_domain)
        };
        let possible_values = has_true as u8 + has_false as u8;
        ReturnStat {
            domain,
            ndv: StatEstimate::exact(possible_values as f64),
            null_count: self.null_count,
            distribution: OwnedDistribution::Boolean(BooleanDistribution { true_count }),
        }
    }

    pub fn equality_true_count(
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
            ndv: StatEstimate::exact(0.0),
            null_count: stat.cardinality.as_null_count(),
            distribution: OwnedDistribution::Unknown,
        })
    } else {
        None
    }
}

pub fn estimate_ndv_true_count(ndv: StatEstimate, not_eq: bool, cardinality: f64) -> StatEstimate {
    let cardinality = cardinality.max(0.0);
    if ndv.upper == 0.0 || cardinality == 0.0 {
        return StatEstimate::exact(0.0);
    }

    let eq_count = StatEstimate::new(
        (cardinality / ndv.upper).min(cardinality),
        if ndv.expected == 0.0 {
            0.0
        } else {
            (cardinality / ndv.expected).min(cardinality)
        },
        if ndv.lower == 0.0 {
            cardinality
        } else {
            (cardinality / ndv.lower).min(cardinality)
        },
    );
    if not_eq {
        StatEstimate::new(
            (cardinality - eq_count.upper).max(0.0),
            (cardinality - eq_count.expected).max(0.0),
            (cardinality - eq_count.lower).max(0.0),
        )
    } else {
        eq_count
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    struct TestAdapter;

    impl ConstantComparisonAdapter for TestAdapter {
        type Value = ();
        type Domain = ();

        fn constant(_scalar: Scalar) -> Result<Self::Value, String> {
            unimplemented!()
        }

        fn domain(_domain: &Domain) -> Result<Self::Domain, String> {
            unimplemented!()
        }

        fn compare(_left: &Self::Value, _right: &Self::Value) -> Ordering {
            unimplemented!()
        }
    }

    fn test_comparison<'a>(
        stat: &'a ArgStat<'a>,
        non_null_cardinality: f64,
        null_count: StatCount,
        nullable: bool,
    ) -> ConstantComparison<'a, 'a, TestAdapter> {
        ConstantComparison {
            stat,
            constant: (),
            domain: None,
            non_null_cardinality,
            null_count,
            nullable,
            _a: PhantomData,
        }
    }

    #[test]
    fn test_boolean_stat_uses_true_count_domain_and_ndv() {
        let stat = ArgStat {
            domain: Domain::Boolean(BooleanDomain {
                has_true: true,
                has_false: true,
            }),
            ndv: StatEstimate::exact(2.0),
            null_count: StatCount::exact(0),
            distribution: crate::stat_distribution::BorrowedDistribution::Unknown,
        };
        let comparison = test_comparison(&stat, 100.0, StatCount::exact(0), false);

        let all_false = comparison.boolean_stat(StatEstimate::exact(0.0));
        assert_eq!(
            all_false.domain,
            Domain::Boolean(BooleanDomain {
                has_true: false,
                has_false: true,
            })
        );
        assert_eq!(all_false.ndv, StatEstimate::exact(1.0));

        let all_true = comparison.boolean_stat(StatEstimate::exact(100.0));
        assert_eq!(
            all_true.domain,
            Domain::Boolean(BooleanDomain {
                has_true: true,
                has_false: false,
            })
        );
        assert_eq!(all_true.ndv, StatEstimate::exact(1.0));

        let uncertain = comparison.boolean_stat(StatEstimate::new(10.0, 10.0, 100.0));
        assert_eq!(
            uncertain.domain,
            Domain::Boolean(BooleanDomain {
                has_true: true,
                has_false: true,
            })
        );
        assert_eq!(uncertain.ndv, StatEstimate::exact(2.0));
    }

    #[test]
    fn test_boolean_stat_omits_nullable_value_domain_without_non_null_values() {
        let stat = ArgStat {
            domain: Domain::Nullable(NullableDomain {
                has_null: true,
                value: None,
            }),
            ndv: StatEstimate::exact(0.0),
            null_count: StatCount::exact(10),
            distribution: crate::stat_distribution::BorrowedDistribution::Unknown,
        };
        let comparison = test_comparison(&stat, 0.0, StatCount::exact(10), true);

        let output = comparison.boolean_stat(StatEstimate::exact(0.0));
        assert_eq!(
            output.domain,
            Domain::Nullable(NullableDomain {
                has_null: true,
                value: None,
            })
        );
        assert_eq!(output.ndv, StatEstimate::exact(0.0));
        output.check_consistency().unwrap();
    }

    #[test]
    fn test_estimate_ndv_true_count_uses_max_ndv_bounds() {
        let eq_count = estimate_ndv_true_count(StatEstimate::new(0.0, 10.0, 10.0), false, 100.0);
        assert_eq!(eq_count, StatEstimate::new(10.0, 10.0, 100.0));

        let not_eq_count = estimate_ndv_true_count(StatEstimate::new(0.0, 10.0, 10.0), true, 100.0);
        assert_eq!(not_eq_count, StatEstimate::new(0.0, 90.0, 90.0));
    }
}
