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

//! `TIMESTAMP_NTZ`: a wall-clock timestamp without a timezone.
//!
//! The value is the number of microseconds since `1970-01-01 00:00:00` of the
//! civil calendar. No offset is ever applied, so text input and output never
//! read the session timezone. The physical representation is the same `i64` as
//! `TIMESTAMP`.

use std::cmp::Ordering;
use std::fmt::Display;

use chrono_tz::Tz;
use databend_common_column::buffer::Buffer;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;

use super::ArgType;
use super::DataType;
use super::SimpleType;
use super::SimpleValueType;
use super::number::SimpleDomain;
use super::timestamp::TIMESTAMP_FORMAT;
use super::timestamp::TIMESTAMP_MAX;
use super::timestamp::timestamp_from_micros;
use super::timestamp_tz::string_to_timestamp_tz;
use crate::ColumnBuilder;
use crate::ScalarRef;
use crate::property::Domain;
use crate::values::Column;
use crate::values::Scalar;

/// The timezone that turns the existing timezone-parameterised kernels into
/// civil-calendar kernels. It means "no offset", not "the value is in UTC".
pub const CIVIL_TZ: Tz = Tz::UTC;

/// `0001-01-01 00:00:00.000000`, in civil microseconds.
///
/// `TIMESTAMP_MIN` is one day earlier because it leaves room for local
/// year-1 midnight after timezone resolution. That room has no meaning for a
/// value that is never resolved against a timezone.
pub const TIMESTAMP_NTZ_MIN: i64 = -62_135_596_800_000_000;
/// `9999-12-31 23:59:59.999999`, in civil microseconds.
pub const TIMESTAMP_NTZ_MAX: i64 = TIMESTAMP_MAX;

#[inline]
pub fn check_timestamp_ntz(micros: i64) -> std::result::Result<i64, String> {
    if (TIMESTAMP_NTZ_MIN..=TIMESTAMP_NTZ_MAX).contains(&micros) {
        Ok(micros)
    } else {
        Err(
            "Invalid date: timestamp_ntz is out of range [0001-01-01, 9999-12-31 23:59:59.999999]"
                .to_string(),
        )
    }
}

/// Parses wall-clock text.
///
/// An absent offset and a zero offset are accepted. A non-zero offset is
/// rejected instead of dropped, because dropping it silently changes the
/// value.
pub fn string_to_timestamp_ntz(ts_str: impl AsRef<[u8]>) -> Result<i64> {
    let ts_str = ts_str.as_ref();
    let value = string_to_timestamp_tz(ts_str, || &CIVIL_TZ)?;
    if value.seconds_offset() != 0 {
        return Err(ErrorCode::BadArguments(format!(
            "cannot cast '{}' to TIMESTAMP_NTZ: the value has a timezone offset; use TIMESTAMP or TIMESTAMP_TZ to keep it",
            String::from_utf8_lossy(ts_str).trim()
        )));
    }
    check_timestamp_ntz(value.timestamp()).map_err(ErrorCode::BadArguments)
}

#[inline]
pub fn timestamp_ntz_to_string(micros: i64) -> impl Display {
    timestamp_from_micros(micros, &CIVIL_TZ).format(TIMESTAMP_FORMAT)
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CoreTimestampNtz;

pub type TimestampNtzType = SimpleValueType<CoreTimestampNtz>;

impl SimpleType for CoreTimestampNtz {
    type Scalar = i64;
    type Domain = SimpleDomain<i64>;

    fn downcast_scalar(scalar: &ScalarRef) -> Option<Self::Scalar> {
        match scalar {
            ScalarRef::TimestampNtz(scalar) => Some(*scalar),
            _ => None,
        }
    }

    fn downcast_column(col: &Column) -> Option<Buffer<Self::Scalar>> {
        match col {
            Column::TimestampNtz(column) => Some(column.clone()),
            _ => None,
        }
    }

    #[inline]
    fn from_for_raw(raw: i128) -> Self::Scalar {
        raw as i64
    }

    fn downcast_domain(domain: &Domain) -> Option<Self::Domain> {
        domain.as_timestamp_ntz().cloned()
    }

    fn downcast_builder(builder: &mut ColumnBuilder) -> Option<&mut Vec<Self::Scalar>> {
        match builder {
            ColumnBuilder::TimestampNtz(builder) => Some(builder),
            _ => None,
        }
    }

    fn downcast_owned_builder(builder: ColumnBuilder) -> Option<Vec<Self::Scalar>> {
        match builder {
            ColumnBuilder::TimestampNtz(builder) => Some(builder),
            _ => None,
        }
    }

    fn upcast_column_builder(
        builder: Vec<Self::Scalar>,
        data_type: &DataType,
    ) -> Option<ColumnBuilder> {
        debug_assert!(data_type.is_timestamp_ntz());
        Some(ColumnBuilder::TimestampNtz(builder))
    }

    fn upcast_scalar(scalar: Self::Scalar, data_type: &DataType) -> Scalar {
        debug_assert!(data_type.is_timestamp_ntz());
        Scalar::TimestampNtz(scalar)
    }

    fn upcast_column(col: Buffer<Self::Scalar>, data_type: &DataType) -> Column {
        debug_assert!(data_type.is_timestamp_ntz());
        Column::TimestampNtz(col)
    }

    fn upcast_domain(domain: Self::Domain, data_type: &DataType) -> Domain {
        debug_assert!(data_type.is_timestamp_ntz());
        Domain::TimestampNtz(domain)
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

impl ArgType for TimestampNtzType {
    fn data_type() -> DataType {
        DataType::TimestampNtz
    }

    fn full_domain() -> Self::Domain {
        SimpleDomain {
            min: TIMESTAMP_NTZ_MIN,
            max: TIMESTAMP_NTZ_MAX,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_wall_clock_without_timezone() {
        let micros = string_to_timestamp_ntz("2024-03-10 02:30:00.123456").unwrap();
        // 02:30 on the US DST gap day exists as a wall clock.
        assert_eq!(
            timestamp_ntz_to_string(micros).to_string(),
            "2024-03-10 02:30:00.123456"
        );
    }

    #[test]
    fn accepts_zero_offset_and_rejects_others() {
        let plain = string_to_timestamp_ntz("2024-01-01 12:00:00").unwrap();
        assert_eq!(
            string_to_timestamp_ntz("2024-01-01T12:00:00Z").unwrap(),
            plain
        );
        assert_eq!(
            string_to_timestamp_ntz("2024-01-01 12:00:00 +00:00").unwrap(),
            plain
        );
        assert!(string_to_timestamp_ntz("2024-01-01 12:00:00 +08:00").is_err());
    }

    #[test]
    fn uses_civil_bounds() {
        assert_eq!(
            string_to_timestamp_ntz("0001-01-01 00:00:00").unwrap(),
            TIMESTAMP_NTZ_MIN
        );
        assert_eq!(
            string_to_timestamp_ntz("9999-12-31 23:59:59.999999").unwrap(),
            TIMESTAMP_NTZ_MAX
        );
        assert!(check_timestamp_ntz(TIMESTAMP_NTZ_MIN - 1).is_err());
    }
}
