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
use std::io::Cursor;

use chrono::NaiveDate;
use chrono::TimeDelta;
use databend_common_column::buffer::Buffer;
use databend_common_exception::ErrorCode;
use databend_common_io::cursor_ext::BufferReadDateTimeExt;
use databend_common_io::cursor_ext::ReadBytesExt;
pub use databend_common_io::datetime::check_input_year;
use databend_common_timezone::Tz;
use num_traits::AsPrimitive;

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

pub const DATE_FORMAT: &str = "%Y-%m-%d";
/// Internal SQL DATE bounds, represented as days since 1970-01-01.
/// Years through 11000 provide headroom for arithmetic and timezone conversion;
/// calendar text and explicit date-part constructors still accept 0001..=9999.
/// This keeps the UInt16 year extraction API. Computed extended dates can be
/// displayed, but their text is not necessarily accepted as calendar input.
/// 0001-01-01
pub const DATE_MIN: i32 = -719_162;
/// 11000-12-31
pub const DATE_MAX: i32 = 3_298_504;

/// Converts internal epoch days. SQL inputs must pass `check_date` first.
pub fn date_from_days(days: impl AsPrimitive<i64>) -> NaiveDate {
    NaiveDate::from_ymd_opt(1970, 1, 1)
        .expect("epoch date is valid")
        .checked_add_signed(TimeDelta::days(days.as_()))
        .expect("date day count is inside the chrono civil range")
}

/// Validate the SQL DATE range without silently changing the value.
#[inline]
pub fn check_date(days: i64) -> Result<i32, String> {
    if (i64::from(DATE_MIN)..=i64::from(DATE_MAX)).contains(&days) {
        Ok(days as i32)
    } else {
        Err("Invalid date: date is out of range [0001-01-01, 11000-12-31]".to_string())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CoreDate;

pub type DateType = SimpleValueType<CoreDate>;

impl SimpleType for CoreDate {
    type Scalar = i32;
    type Domain = SimpleDomain<i32>;

    fn downcast_scalar(scalar: &ScalarRef) -> Option<Self::Scalar> {
        match scalar {
            ScalarRef::Date(scalar) => Some(*scalar),
            _ => None,
        }
    }

    fn downcast_column(col: &Column) -> Option<Buffer<Self::Scalar>> {
        match col {
            Column::Date(column) => Some(column.clone()),
            _ => None,
        }
    }

    fn downcast_domain(domain: &Domain) -> Option<Self::Domain> {
        domain.as_date().cloned()
    }

    fn downcast_builder(builder: &mut ColumnBuilder) -> Option<&mut Vec<Self::Scalar>> {
        match builder {
            ColumnBuilder::Date(builder) => Some(builder),
            _ => None,
        }
    }

    fn downcast_owned_builder(builder: ColumnBuilder) -> Option<Vec<Self::Scalar>> {
        match builder {
            ColumnBuilder::Date(builder) => Some(builder),
            _ => None,
        }
    }

    fn upcast_column_builder(
        builder: Vec<Self::Scalar>,
        data_type: &DataType,
    ) -> Option<ColumnBuilder> {
        debug_assert!(data_type.is_date());
        Some(ColumnBuilder::Date(builder))
    }

    fn upcast_scalar(scalar: Self::Scalar, data_type: &DataType) -> Scalar {
        debug_assert!(data_type.is_date());
        Scalar::Date(scalar)
    }

    fn upcast_column(col: Buffer<Self::Scalar>, data_type: &DataType) -> Column {
        debug_assert!(data_type.is_date());
        Column::Date(col)
    }

    fn upcast_domain(domain: SimpleDomain<i32>, data_type: &DataType) -> Domain {
        debug_assert!(data_type.is_date());
        Domain::Date(domain)
    }

    #[inline(always)]
    fn compare(lhs: &Self::Scalar, rhs: &Self::Scalar) -> Ordering {
        lhs.cmp(rhs)
    }
}

impl ArgType for DateType {
    fn data_type() -> DataType {
        DataType::Date
    }

    fn full_domain() -> Self::Domain {
        SimpleDomain {
            min: DATE_MIN,
            max: DATE_MAX,
        }
    }
}

#[inline]
pub fn string_to_date(
    date_str: impl AsRef<[u8]>,
    tz: &Tz,
) -> databend_common_exception::Result<i32> {
    let raw = std::str::from_utf8(date_str.as_ref()).unwrap();
    let mut reader = Cursor::new(raw.as_bytes());
    match reader.read_date_text(tz) {
        Ok(days) => {
            if reader.must_eof().is_err() {
                return Err(ErrorCode::BadArguments("unexpected argument"));
            }
            check_date(i64::from(days)).map_err(ErrorCode::BadArguments)
        }
        Err(e) => match e.code() {
            ErrorCode::BAD_BYTES => Err(e),
            _ => Err(ErrorCode::BadArguments("unexpected argument")),
        },
    }
}

#[inline]
pub fn date_to_string(date: impl AsPrimitive<i64>) -> String {
    date_from_days(date).format(DATE_FORMAT).to_string()
}
