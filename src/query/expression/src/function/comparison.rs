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

use crate::Scalar;
use crate::stat_distribution::ArgStat;
use crate::stat_distribution::Ndv;
use crate::stat_distribution::StatBinaryArg;
use crate::stat_distribution::StatEstimate;

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

#[derive(Default)]
pub struct LtOp;
#[derive(Default)]
pub struct LteOp;
#[derive(Default)]
pub struct GtOp;
#[derive(Default)]
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

pub struct ConstantComparison<'s, 'a> {
    pub stat: &'s ArgStat<'a>,
    pub constant: Scalar,
    pub cardinality: f64,
}

impl<'s, 'a> ConstantComparison<'s, 'a> {
    pub fn from_equality_args(stat: &'s StatBinaryArg<'a>) -> Option<Self> {
        Self::from_right_constant(stat).or_else(|| Self::from_left_constant(stat))
    }

    pub fn from_right_constant(stat: &'s StatBinaryArg<'a>) -> Option<Self> {
        Some(Self {
            stat: &stat.args[0],
            constant: stat.args[1].singleton()?,
            cardinality: stat.cardinality,
        })
    }

    pub fn from_left_constant(stat: &'s StatBinaryArg<'a>) -> Option<Self> {
        Some(Self {
            stat: &stat.args[1],
            constant: stat.args[0].singleton()?,
            cardinality: stat.cardinality,
        })
    }

    pub fn equality_true_count(
        &self,
        not_eq: bool,
        compare: impl Fn(&Scalar, &Scalar) -> Option<Ordering>,
    ) -> Option<StatEstimate> {
        let Some((min, max)) = self.stat.value_minmax() else {
            return Some(StatEstimate::exact(if not_eq {
                self.cardinality
            } else {
                0.0
            }));
        };
        if compare(&self.constant, &min)? == Ordering::Less
            || compare(&self.constant, &max)? == Ordering::Greater
        {
            return Some(StatEstimate::exact(if not_eq {
                self.cardinality
            } else {
                0.0
            }));
        }

        Some(estimate_ndv_true_count(
            self.stat.ndv,
            not_eq,
            self.cardinality,
        ))
    }

    pub fn minmax_range_true_count<Op: StatComparisonOp>(
        &self,
        compare: impl Fn(&Scalar, &Scalar) -> Option<Ordering>,
    ) -> Option<StatEstimate> {
        try {
            let (min, max) = self.stat.value_minmax()?;
            let cmp_min = compare(&self.constant, &min)?;
            let cmp_max = compare(&self.constant, &max)?;
            Op::estimate_minmax_range_true_count(self.stat.ndv, self.cardinality, cmp_min, cmp_max)?
        }
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
