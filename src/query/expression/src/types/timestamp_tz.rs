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

use chrono::DateTime;
use chrono::Datelike;
use chrono::NaiveDate;
use chrono::NaiveDateTime;
use databend_common_column::buffer::Buffer;
use databend_common_column::types::timestamp_tz;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_io::datetime::check_input_year;
use databend_common_io::datetime::parse_standard_timestamp as parse_iso_timestamp;
use databend_common_timezone::LocalTimeResolution;
use databend_common_timezone::Tz;
use databend_common_timezone::resolve_local_datetime;

use super::ArgType;
use super::DataType;
use super::SimpleDomain;
use super::simple_type::SimpleType;
use super::simple_type::SimpleValueType;
use super::timestamp::TIMESTAMP_MAX;
use super::timestamp::TIMESTAMP_MIN;
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

fn try_parse_standard_timestamp_with_offset(ts_str: &[u8]) -> Option<Result<timestamp_tz>> {
    let mut start = 0;
    let mut end = ts_str.len();
    while start < end && ts_str[start].is_ascii_whitespace() {
        start += 1;
    }
    while end > start && ts_str[end - 1].is_ascii_whitespace() {
        end -= 1;
    }
    let bytes = &ts_str[start..end];
    let parsed = match parse_iso_timestamp(bytes)? {
        Ok(parsed) => parsed,
        Err(err) => return Some(Err(err)),
    };
    let offset_seconds = parsed.provided_offset?;

    Some(build_timestamp_tz_from_components(
        parsed.year,
        parsed.month,
        parsed.day,
        parsed.hour,
        parsed.minute,
        parsed.second,
        parsed.micro,
        offset_seconds,
    ))
}

const PARSE_FORMATS_WITH_OFFSET: &[&str] = &[
    "%Y-%m-%dT%H:%M:%S%.f %z",
    "%Y-%m-%d %H:%M:%S%.f %z",
    "%Y-%m-%dT%H:%M:%S%.f%z",
    "%Y-%m-%d %H:%M:%S%.f%z",
];

const PARSE_FORMATS_NAIVE: &[&str] = &["%Y-%m-%dT%H:%M:%S%.f", "%Y-%m-%d %H:%M:%S%.f"];

fn build_timestamp_tz(micros: i128, offset_seconds: i32) -> Result<timestamp_tz> {
    if micros < i128::from(TIMESTAMP_MIN) || micros > i128::from(TIMESTAMP_MAX) {
        return Err(ErrorCode::BadBytes(
            "Timestamp is out of range after applying timezone offset".to_string(),
        ));
    }
    Ok(timestamp_tz::new(micros as i64, offset_seconds))
}

fn build_timestamp_tz_from_components(
    year: i32,
    month: u8,
    day: u8,
    hour: u8,
    minute: u8,
    second: u8,
    micro: u32,
    offset_seconds: i32,
) -> Result<timestamp_tz> {
    let (year, month, day) = if year == 0 && month == 0 && day == 0 {
        (1970, 1, 1)
    } else {
        (year, month, day)
    };

    check_input_year(year)?;
    let date = NaiveDate::from_ymd_opt(year, month as u32, day as u32).ok_or_else(|| {
        ErrorCode::BadBytes(format!(
            "Invalid date value {:04}-{:02}-{:02}",
            year, month, day
        ))
    })?;
    let local = date
        .and_hms_opt(hour as u32, minute as u32, second as u32)
        .ok_or_else(|| {
            ErrorCode::BadBytes(format!(
                "Invalid time value {:02}:{:02}:{:02}",
                hour, minute, second
            ))
        })?;

    // The civil fields are relative to `offset_seconds`, so removing that offset
    // yields the UTC instant.
    let micros = i128::from(local.and_utc().timestamp()) * 1_000_000 + i128::from(micro)
        - i128::from(offset_seconds) * 1_000_000;

    build_timestamp_tz(micros, offset_seconds)
}

/// Interpret a local civil datetime in `tz`.
///
/// DST folds and gaps follow the shared timezone policy.
fn timestamp_tz_from_local(local: &NaiveDateTime, tz: &Tz) -> Result<timestamp_tz> {
    check_input_year(local.year())?;
    let micro = local.and_utc().timestamp_subsec_micros();
    let resolved = resolve_local_datetime(tz, *local, LocalTimeResolution::Compatible, None)
        .ok_or_else(|| {
            ErrorCode::BadBytes(format!(
                "Invalid local datetime {} for timezone {tz}",
                local.format("%Y-%m-%d %H:%M:%S")
            ))
        })?;

    let micros = i128::from(resolved.unix_seconds) * 1_000_000 + i128::from(micro);
    build_timestamp_tz(micros, resolved.offset_seconds)
}

#[inline]
pub fn string_to_timestamp_tz<'a, F: FnOnce() -> &'a Tz>(
    ts_str: &[u8],
    fn_tz: F,
) -> databend_common_exception::Result<timestamp_tz> {
    if let Some(parsed) = try_parse_standard_timestamp_with_offset(ts_str) {
        return parsed;
    }

    let text = std::str::from_utf8(ts_str)
        .map_err(|_| ErrorCode::BadBytes("Timestamp text is not valid UTF-8".to_string()))?
        .trim();

    // A bare date is midnight in the session timezone.
    if let Ok(date) = NaiveDate::parse_from_str(text, "%Y-%m-%d") {
        let local = date.and_hms_opt(0, 0, 0).expect("midnight is valid");
        return timestamp_tz_from_local(&local, fn_tz());
    }

    for format in PARSE_FORMATS_WITH_OFFSET {
        if let Ok(value) = DateTime::parse_from_str(text, format) {
            check_input_year(value.year())?;
            let micros = i128::from(value.timestamp()) * 1_000_000
                + i128::from(value.timestamp_subsec_micros());
            return build_timestamp_tz(micros, value.offset().local_minus_utc());
        }
    }

    for format in PARSE_FORMATS_NAIVE {
        if let Ok(local) = NaiveDateTime::parse_from_str(text, format) {
            return timestamp_tz_from_local(&local, fn_tz());
        }
    }

    Err(ErrorCode::BadBytes(format!(
        "Timestamp Parsing Error: The value '{text}' could not be parsed into a valid timestamp with timezone"
    )))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn stores_utc_in_timestamp_field() {
        let tz = "Asia/Shanghai".parse::<Tz>().unwrap();
        let value = string_to_timestamp_tz(b"2021-12-20 17:01:01 +0800", || &tz).expect("parse tz");
        assert_eq!(value.seconds_offset(), 28_800);
        // timestamp() keeps the UTC instant (09:01:01).
        assert_eq!(value.timestamp(), 1_639_990_861_000_000);
        assert_eq!(value.to_string(), "2021-12-20 17:01:01.000000 +0800");
    }
}
