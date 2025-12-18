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
use std::io::Cursor;

use databend_common_column::buffer::Buffer;
use databend_common_exception::ErrorCode;
use databend_common_io::cursor_ext::BufferReadDateTimeExt;
use databend_common_io::cursor_ext::DateTimeResType;
use databend_common_io::cursor_ext::ReadBytesExt;
use jiff::Zoned;
use jiff::fmt::strtime;
use jiff::tz::TimeZone;

use super::ArgType;
use super::DataType;
use super::SimpleType;
use super::SimpleValueType;
use super::number::SimpleDomain;
use crate::ColumnBuilder;
use crate::ScalarRef;
use crate::property::Domain;
use crate::utils::date_helper::DateConverter;
use crate::values::Column;
use crate::values::Scalar;

pub const TIMESTAMP_FORMAT: &str = "%Y-%m-%d %H:%M:%S%.6f";
/// Minimum valid timestamp `0001-01-01 00:00:00.000000`, represented by the microsecs offset from 1970-01-01.
pub const TIMESTAMP_MIN: i64 = -62135596800000000;
/// Maximum valid timestamp `9999-12-31 23:59:59.999999`, represented by the microsecs offset from 1970-01-01.
pub const TIMESTAMP_MAX: i64 = 253402300799999999;

pub const MICROS_PER_SEC: i64 = 1_000_000;
pub const MICROS_PER_MILLI: i64 = 1_000;

pub const PRECISION_MICRO: u8 = 6;
pub const PRECISION_MILLI: u8 = 3;
pub const PRECISION_SEC: u8 = 0;

/// Check if the timestamp value is valid.
/// If timestamp is invalid convert to TIMESTAMP_MIN.
#[inline]
pub fn clamp_timestamp(micros: &mut i64) {
    if !(TIMESTAMP_MIN..=TIMESTAMP_MAX).contains(micros) {
        *micros = TIMESTAMP_MIN;
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CoreTimestamp;

pub type TimestampType = SimpleValueType<CoreTimestamp>;

impl SimpleType for CoreTimestamp {
    type Scalar = i64;
    type Domain = SimpleDomain<i64>;

    fn downcast_scalar(scalar: &ScalarRef) -> Option<Self::Scalar> {
        match scalar {
            ScalarRef::Timestamp(scalar) => Some(*scalar),
            _ => None,
        }
    }

    fn downcast_column(col: &Column) -> Option<Buffer<Self::Scalar>> {
        match col {
            Column::Timestamp(column) => Some(column.clone()),
            _ => None,
        }
    }

    fn downcast_domain(domain: &Domain) -> Option<Self::Domain> {
        domain.as_timestamp().cloned()
    }

    fn downcast_builder(builder: &mut ColumnBuilder) -> Option<&mut Vec<Self::Scalar>> {
        match builder {
            ColumnBuilder::Timestamp(builder) => Some(builder),
            _ => None,
        }
    }

    fn downcast_owned_builder(builder: ColumnBuilder) -> Option<Vec<Self::Scalar>> {
        match builder {
            ColumnBuilder::Timestamp(builder) => Some(builder),
            _ => None,
        }
    }

    fn upcast_column_builder(
        builder: Vec<Self::Scalar>,
        data_type: &DataType,
    ) -> Option<ColumnBuilder> {
        debug_assert!(data_type.is_timestamp());
        Some(ColumnBuilder::Timestamp(builder))
    }

    fn upcast_scalar(scalar: Self::Scalar, data_type: &DataType) -> Scalar {
        debug_assert!(data_type.is_timestamp());
        Scalar::Timestamp(scalar)
    }

    fn upcast_column(col: Buffer<Self::Scalar>, data_type: &DataType) -> Column {
        debug_assert!(data_type.is_timestamp());
        Column::Timestamp(col)
    }

    fn upcast_domain(domain: Self::Domain, data_type: &DataType) -> Domain {
        debug_assert!(data_type.is_timestamp());
        Domain::Timestamp(domain)
    }

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

impl ArgType for TimestampType {
    fn data_type() -> DataType {
        DataType::Timestamp
    }

    fn full_domain() -> Self::Domain {
        SimpleDomain {
            min: TIMESTAMP_MIN,
            max: TIMESTAMP_MAX,
        }
    }
}

pub fn microseconds_to_seconds(micros: i64) -> i64 {
    micros / MICROS_PER_SEC
}

pub fn microseconds_to_days(micros: i64) -> i32 {
    (microseconds_to_seconds(micros) / 24 / 3600) as i32
}

#[inline]
pub fn string_to_timestamp(
    ts_str: impl AsRef<[u8]>,
    tz: &TimeZone,
) -> databend_common_exception::Result<Zoned> {
    let raw = std::str::from_utf8(ts_str.as_ref()).unwrap();
    let mut reader = Cursor::new(raw.as_bytes());
    match reader.read_timestamp_text(tz) {
        Ok(DateTimeResType::Datetime(dt)) => {
            if reader.must_eof().is_err() {
                Err(ErrorCode::BadArguments("unexpected argument"))
            } else {
                Ok(dt)
            }
        }
        Ok(DateTimeResType::Date(_)) => Err(ErrorCode::BadArguments("unexpected argument")),
        Err(e) => match e.code() {
            ErrorCode::BAD_BYTES => Err(e),
            _ => Err(ErrorCode::BadArguments("unexpected argument")),
        },
    }
}

#[inline]
pub fn timestamp_to_string(ts: i64, tz: &TimeZone) -> impl Display {
    let zdt = ts.to_timestamp(tz);
    strtime::format(TIMESTAMP_FORMAT, &zdt).unwrap()
}
