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
use databend_common_io::cursor_ext::ReadBytesExt;
use jiff::civil::Date;
use jiff::fmt::strtime;
use jiff::tz::TimeZone;
use num_traits::AsPrimitive;

use super::number::SimpleDomain;
use super::ArgType;
use super::DataType;
use super::DecimalSize;
use super::GenericMap;
use super::ReturnType;
use super::SimpleType;
use super::SimpleValueType;
use crate::date_helper::DateConverter;
use crate::property::Domain;
use crate::values::Column;
use crate::values::Scalar;
use crate::ColumnBuilder;
use crate::ScalarRef;

pub const DATE_FORMAT: &str = "%Y-%m-%d";
/// Minimum valid date, represented by the day offset from 1970-01-01.
/// 0001-01-01
pub const DATE_MIN: i32 = -719162;
/// Maximum valid date, represented by the day offset from 1970-01-01.
/// 9999-12-31
pub const DATE_MAX: i32 = 2932896;

/// Check if date is within range.
/// /// If days is invalid convert to DATE_MIN.
#[inline]
pub fn clamp_date(days: i64) -> i32 {
    if (DATE_MIN as i64..=DATE_MAX as i64).contains(&days) {
        days as i32
    } else {
        DATE_MIN
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
        _decimal_size: Option<DecimalSize>,
    ) -> Option<ColumnBuilder> {
        Some(ColumnBuilder::Date(builder))
    }

    fn upcast_scalar(scalar: Self::Scalar) -> Scalar {
        Scalar::Date(scalar)
    }

    fn upcast_column(col: Buffer<Self::Scalar>) -> Column {
        Column::Date(col)
    }

    fn upcast_domain(domain: SimpleDomain<i32>) -> Domain {
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

impl ReturnType for DateType {
    fn create_builder(capacity: usize, _generics: &GenericMap) -> Self::ColumnBuilder {
        Vec::with_capacity(capacity)
    }

    fn column_from_vec(vec: Vec<Self::Scalar>, _generics: &GenericMap) -> Self::Column {
        vec.into()
    }

    fn column_from_iter(iter: impl Iterator<Item = Self::Scalar>, _: &GenericMap) -> Self::Column {
        iter.collect()
    }

    fn column_from_ref_iter<'a>(
        iter: impl Iterator<Item = Self::ScalarRef<'a>>,
        _: &GenericMap,
    ) -> Self::Column {
        iter.collect()
    }
}

#[inline]
pub fn string_to_date(
    date_str: impl AsRef<[u8]>,
    tz: &TimeZone,
) -> databend_common_exception::Result<Date> {
    let mut reader = Cursor::new(std::str::from_utf8(date_str.as_ref()).unwrap().as_bytes());
    match reader.read_date_text(tz) {
        Ok(d) => match reader.must_eof() {
            Ok(..) => Ok(d),
            Err(_) => Err(ErrorCode::BadArguments("unexpected argument")),
        },
        Err(e) => match e.code() {
            ErrorCode::BAD_BYTES => Err(e),
            _ => Err(ErrorCode::BadArguments("unexpected argument")),
        },
    }
}

#[inline]
pub fn date_to_string(date: impl AsPrimitive<i64>, tz: &TimeZone) -> impl Display {
    let res = date.as_().to_date(tz.clone());
    strtime::format(DATE_FORMAT, res).unwrap()
}
