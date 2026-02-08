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
use std::fmt::Display;

use databend_common_column::buffer::Buffer;
use databend_common_column::types::months_days_micros;
use databend_common_io::Interval;

use super::ArgType;
use super::DataType;
use super::SimpleType;
use super::SimpleValueType;
use super::number::SimpleDomain;
use crate::ColumnBuilder;
use crate::ScalarRef;
use crate::property::Domain;
use crate::values::Column;
use crate::values::Scalar;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CoreInterval;

pub type IntervalType = SimpleValueType<CoreInterval>;

impl SimpleType for CoreInterval {
    type Scalar = months_days_micros;
    type Domain = SimpleDomain<months_days_micros>;

    fn downcast_scalar(scalar: &ScalarRef) -> Option<Self::Scalar> {
        match scalar {
            ScalarRef::Interval(scalar) => Some(*scalar),
            _ => None,
        }
    }

    fn downcast_column(col: &Column) -> Option<Buffer<Self::Scalar>> {
        match col {
            Column::Interval(column) => Some(column.clone()),
            _ => None,
        }
    }

    fn downcast_domain(domain: &Domain) -> Option<Self::Domain> {
        domain.as_interval().cloned()
    }

    fn downcast_builder(builder: &mut ColumnBuilder) -> Option<&mut Vec<Self::Scalar>> {
        match builder {
            ColumnBuilder::Interval(builder) => Some(builder),
            _ => None,
        }
    }

    fn downcast_owned_builder(builder: ColumnBuilder) -> Option<Vec<Self::Scalar>> {
        match builder {
            ColumnBuilder::Interval(builder) => Some(builder),
            _ => None,
        }
    }

    fn upcast_column_builder(
        builder: Vec<Self::Scalar>,
        data_type: &DataType,
    ) -> Option<ColumnBuilder> {
        debug_assert!(data_type.is_interval());
        Some(ColumnBuilder::Interval(builder))
    }

    fn upcast_scalar(scalar: Self::Scalar, data_type: &DataType) -> Scalar {
        debug_assert!(data_type.is_interval());
        Scalar::Interval(scalar)
    }

    fn upcast_column(col: Buffer<Self::Scalar>, data_type: &DataType) -> Column {
        debug_assert!(data_type.is_interval());
        Column::Interval(col)
    }

    fn upcast_domain(domain: Self::Domain, data_type: &DataType) -> Domain {
        debug_assert!(data_type.is_interval());
        Domain::Interval(domain)
    }

    #[inline(always)]
    fn compare(lhs: &Self::Scalar, rhs: &Self::Scalar) -> Ordering {
        lhs.cmp(rhs)
    }

    #[inline(always)]
    fn greater_than(left: &Self::Scalar, right: &Self::Scalar) -> bool {
        left > right
    }

    #[inline(always)]
    fn less_than(left: &Self::Scalar, right: &Self::Scalar) -> bool {
        left < right
    }

    #[inline(always)]
    fn greater_than_equal(left: &Self::Scalar, right: &Self::Scalar) -> bool {
        left >= right
    }

    #[inline(always)]
    fn less_than_equal(left: &Self::Scalar, right: &Self::Scalar) -> bool {
        left <= right
    }
}

impl ArgType for IntervalType {
    fn data_type() -> DataType {
        DataType::Interval
    }

    fn full_domain() -> Self::Domain {
        SimpleDomain {
            min: months_days_micros::new(-12 * 200, -365 * 200, -7200000000000000000),
            max: months_days_micros::new(12 * 200, 365 * 200, 7200000000000000000),
        }
    }
}

#[inline]
pub fn string_to_interval(interval_str: &str) -> databend_common_exception::Result<Interval> {
    Interval::from_string(interval_str)
}

#[inline]
pub fn interval_to_string(i: &months_days_micros) -> impl Display {
    let interval = Interval {
        months: i.months(),
        days: i.days(),
        micros: i.microseconds(),
    };
    interval
}
