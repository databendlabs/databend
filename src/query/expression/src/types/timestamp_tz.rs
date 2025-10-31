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

use databend_common_column::buffer::Buffer;
use databend_common_column::types::timestamp_tz;
use jiff::fmt;
use jiff::tz;
use jiff::tz::TimeZone;

use crate::types::simple_type::SimpleType;
use crate::types::simple_type::SimpleValueType;
use crate::types::timestamp::TIMESTAMP_MAX;
use crate::types::timestamp::TIMESTAMP_MIN;
use crate::types::ArgType;
use crate::types::DataType;
use crate::types::SimpleDomain;
use crate::Column;
use crate::ColumnBuilder;
use crate::Domain;
use crate::Scalar;
use crate::ScalarRef;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CoreTimestampTz;

pub type TimestampTzType = SimpleValueType<CoreTimestampTz>;

impl SimpleType for CoreTimestampTz {
    type Scalar = timestamp_tz;
    type Domain = SimpleDomain<timestamp_tz>;

    fn downcast_scalar<'a>(scalar: &ScalarRef<'a>) -> Option<Self::Scalar> {
        match scalar {
            ScalarRef::TimestampTz(scalar) => Some(*scalar),
            _ => None,
        }
    }

    fn upcast_scalar(scalar: Self::Scalar, data_type: &DataType) -> Scalar {
        debug_assert!(data_type.is_timestamp_tz());
        Scalar::TimestampTz(scalar)
    }

    fn downcast_column(col: &Column) -> Option<Buffer<Self::Scalar>> {
        match col {
            Column::TimestampTz(column) => Some(column.clone()),
            _ => None,
        }
    }

    fn upcast_column(col: Buffer<Self::Scalar>, data_type: &DataType) -> Column {
        debug_assert!(data_type.is_timestamp_tz());
        Column::TimestampTz(col)
    }

    fn downcast_domain(domain: &Domain) -> Option<Self::Domain> {
        domain.as_timestamp_tz().cloned()
    }

    fn upcast_domain(domain: Self::Domain, data_type: &DataType) -> Domain {
        debug_assert!(data_type.is_timestamp_tz());
        Domain::TimestampTz(domain)
    }

    fn downcast_builder(builder: &mut ColumnBuilder) -> Option<&mut Vec<Self::Scalar>> {
        match builder {
            ColumnBuilder::TimestampTz(builder) => Some(builder),
            _ => None,
        }
    }

    fn downcast_owned_builder(builder: ColumnBuilder) -> Option<Vec<Self::Scalar>> {
        match builder {
            ColumnBuilder::TimestampTz(builder) => Some(builder),
            _ => None,
        }
    }

    fn upcast_column_builder(
        builder: Vec<Self::Scalar>,
        data_type: &DataType,
    ) -> Option<ColumnBuilder> {
        debug_assert!(data_type.is_timestamp_tz());
        Some(ColumnBuilder::TimestampTz(builder))
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

impl ArgType for TimestampTzType {
    fn data_type() -> DataType {
        DataType::TimestampTz
    }

    fn full_domain() -> Self::Domain {
        SimpleDomain {
            min: timestamp_tz::new(TIMESTAMP_MIN, 0),
            max: timestamp_tz::new(TIMESTAMP_MAX, 0),
        }
    }
}

#[inline]
pub fn string_to_timestamp_tz<'a, F: FnOnce() -> &'a TimeZone>(
    ts_str: &[u8],
    fn_tz: F,
) -> databend_common_exception::Result<timestamp_tz> {
    let time = fmt::strtime::parse("%Y-%m-%d", ts_str)
        .or_else(|_| fmt::strtime::parse("%Y-%m-%dT%H:%M:%S%.f %z", ts_str))
        .or_else(|_| fmt::strtime::parse("%Y-%m-%dT%H:%M:%S%.f %:z", ts_str))
        .or_else(|_| fmt::strtime::parse("%Y-%m-%dT%H:%M:%S%.f", ts_str))
        .or_else(|_| fmt::strtime::parse("%Y-%m-%d %H:%M:%S%.f %z", ts_str))
        .or_else(|_| fmt::strtime::parse("%Y-%m-%d %H:%M:%S%.f %:z", ts_str))
        .or_else(|_| fmt::strtime::parse("%Y-%m-%d %H:%M:%S%.f", ts_str))?;
    let datetime = time.to_datetime()?;
    let timestamp = tz::offset(0).to_timestamp(datetime)?;
    let offset = time
        .offset()
        .unwrap_or_else(|| fn_tz().to_offset(timestamp));

    Ok(timestamp_tz::new(
        timestamp.as_microsecond(),
        offset.seconds(),
    ))
}
