// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

use std::sync::Arc;

use arrow_arith::arity::binary;
use arrow_arith::temporal::{DatePart, date_part};
use arrow_array::types::Date32Type;
use arrow_array::{
    Array, ArrayRef, Date32Array, Int32Array, TimestampMicrosecondArray, TimestampNanosecondArray,
};
use arrow_schema::{DataType, TimeUnit};
use chrono::{DateTime, Datelike, Duration};

use super::TransformFunction;
use crate::spec::{Datum, PrimitiveLiteral, PrimitiveType};
use crate::{Error, ErrorKind, Result};

/// Microseconds in one hour.
const MICROSECONDS_PER_HOUR: i64 = 3_600_000_000;
/// Nanoseconds in one hour.
const NANOSECONDS_PER_HOUR: i64 = 3_600_000_000_000;
/// Year of unix epoch.
const UNIX_EPOCH_YEAR: i32 = 1970;
/// One second in micros.
const MICROS_PER_SECOND: i64 = 1_000_000;
/// One second in nanos.
const NANOS_PER_SECOND: i64 = 1_000_000_000;

/// Extract a date or timestamp year, as years from 1970
#[derive(Debug)]
pub struct Year;

impl Year {
    #[inline]
    fn timestamp_to_year_micros(timestamp: i64) -> Result<i32> {
        Ok(DateTime::from_timestamp_micros(timestamp)
            .ok_or_else(|| {
                Error::new(
                    ErrorKind::DataInvalid,
                    "Fail to convert timestamp to date in year transform",
                )
            })?
            .year()
            - UNIX_EPOCH_YEAR)
    }

    #[inline]
    fn timestamp_to_year_nanos(timestamp: i64) -> Result<i32> {
        Ok(DateTime::from_timestamp_nanos(timestamp).year() - UNIX_EPOCH_YEAR)
    }
}

impl TransformFunction for Year {
    fn transform(&self, input: ArrayRef) -> Result<ArrayRef> {
        let array = date_part(&input, DatePart::Year)
            .map_err(|err| Error::new(ErrorKind::Unexpected, format!("{err}")))?;
        Ok(Arc::<Int32Array>::new(
            array
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap()
                .unary(|v| v - UNIX_EPOCH_YEAR),
        ))
    }

    fn transform_literal(&self, input: &crate::spec::Datum) -> Result<Option<crate::spec::Datum>> {
        let val = match (input.data_type(), input.literal()) {
            (PrimitiveType::Date, PrimitiveLiteral::Int(v)) => {
                Date32Type::to_naive_date(*v).year() - UNIX_EPOCH_YEAR
            }
            (PrimitiveType::Timestamp, PrimitiveLiteral::Long(v)) => {
                Self::timestamp_to_year_micros(*v)?
            }
            (PrimitiveType::Timestamptz, PrimitiveLiteral::Long(v)) => {
                Self::timestamp_to_year_micros(*v)?
            }
            (PrimitiveType::TimestampNs, PrimitiveLiteral::Long(v)) => {
                Self::timestamp_to_year_nanos(*v)?
            }
            (PrimitiveType::TimestamptzNs, PrimitiveLiteral::Long(v)) => {
                Self::timestamp_to_year_nanos(*v)?
            }
            _ => {
                return Err(crate::Error::new(
                    crate::ErrorKind::FeatureUnsupported,
                    format!(
                        "Unsupported data type for year transform: {:?}",
                        input.data_type()
                    ),
                ));
            }
        };
        Ok(Some(Datum::int(val)))
    }
}

/// Extract a date or timestamp month, as months from 1970-01-01
#[derive(Debug)]
pub struct Month;

impl Month {
    #[inline]
    fn timestamp_to_month_micros(timestamp: i64) -> Result<i32> {
        // date: aaaa-aa-aa
        // unix epoch date: 1970-01-01
        // if date > unix epoch date, delta month = (aa - 1) + 12 * (aaaa-1970)
        // if date < unix epoch date, delta month = (12 - (aa - 1)) + 12 * (1970-aaaa-1)
        let date = DateTime::from_timestamp_micros(timestamp).ok_or_else(|| {
            Error::new(
                ErrorKind::DataInvalid,
                "Fail to convert timestamp to date in month transform",
            )
        })?;
        let unix_epoch_date = DateTime::from_timestamp_micros(0)
            .expect("0 timestamp from unix epoch should be valid");
        if date > unix_epoch_date {
            Ok((date.month0() as i32) + 12 * (date.year() - UNIX_EPOCH_YEAR))
        } else {
            let delta = (12 - date.month0() as i32) + 12 * (UNIX_EPOCH_YEAR - date.year() - 1);
            Ok(-delta)
        }
    }

    #[inline]
    fn timestamp_to_month_nanos(timestamp: i64) -> Result<i32> {
        // date: aaaa-aa-aa
        // unix epoch date: 1970-01-01
        // if date > unix epoch date, delta month = (aa - 1) + 12 * (aaaa-1970)
        // if date < unix epoch date, delta month = (12 - (aa - 1)) + 12 * (1970-aaaa-1)
        let date = DateTime::from_timestamp_nanos(timestamp);
        let unix_epoch_date = DateTime::from_timestamp_nanos(0);
        if date > unix_epoch_date {
            Ok((date.month0() as i32) + 12 * (date.year() - UNIX_EPOCH_YEAR))
        } else {
            let delta = (12 - date.month0() as i32) + 12 * (UNIX_EPOCH_YEAR - date.year() - 1);
            Ok(-delta)
        }
    }
}

impl TransformFunction for Month {
    fn transform(&self, input: ArrayRef) -> Result<ArrayRef> {
        let year_array = date_part(&input, DatePart::Year)
            .map_err(|err| Error::new(ErrorKind::Unexpected, format!("{err}")))?;
        let year_array: Int32Array = year_array
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap()
            .unary(|v| 12 * (v - UNIX_EPOCH_YEAR));
        let month_array = date_part(&input, DatePart::Month)
            .map_err(|err| Error::new(ErrorKind::Unexpected, format!("{err}")))?;
        Ok(Arc::<Int32Array>::new(
            binary(
                month_array.as_any().downcast_ref::<Int32Array>().unwrap(),
                year_array.as_any().downcast_ref::<Int32Array>().unwrap(),
                // Compute month from 1970-01-01, so minus 1 here.
                |a, b| a + b - 1,
            )
            .unwrap(),
        ))
    }

    fn transform_literal(&self, input: &crate::spec::Datum) -> Result<Option<crate::spec::Datum>> {
        let val = match (input.data_type(), input.literal()) {
            (PrimitiveType::Date, PrimitiveLiteral::Int(v)) => {
                (Date32Type::to_naive_date(*v).year() - UNIX_EPOCH_YEAR) * 12
                    + Date32Type::to_naive_date(*v).month0() as i32
            }
            (PrimitiveType::Timestamp, PrimitiveLiteral::Long(v)) => {
                Self::timestamp_to_month_micros(*v)?
            }
            (PrimitiveType::Timestamptz, PrimitiveLiteral::Long(v)) => {
                Self::timestamp_to_month_micros(*v)?
            }
            (PrimitiveType::TimestampNs, PrimitiveLiteral::Long(v)) => {
                Self::timestamp_to_month_nanos(*v)?
            }
            (PrimitiveType::TimestamptzNs, PrimitiveLiteral::Long(v)) => {
                Self::timestamp_to_month_nanos(*v)?
            }
            _ => {
                return Err(crate::Error::new(
                    crate::ErrorKind::FeatureUnsupported,
                    format!(
                        "Unsupported data type for month transform: {:?}",
                        input.data_type()
                    ),
                ));
            }
        };
        Ok(Some(Datum::int(val)))
    }
}

/// Extract a date or timestamp day, as days from 1970-01-01
#[derive(Debug)]
pub struct Day;

impl Day {
    #[inline]
    fn day_timestamp_micro(v: i64) -> Result<i32> {
        let secs = v / MICROS_PER_SECOND;

        let (nanos, offset) = if v >= 0 {
            let nanos = (v.rem_euclid(MICROS_PER_SECOND) * 1_000) as u32;
            let offset = 0i64;
            (nanos, offset)
        } else {
            let v = v + 1;
            let nanos = (v.rem_euclid(MICROS_PER_SECOND) * 1_000) as u32;
            let offset = 1i64;
            (nanos, offset)
        };

        let delta = Duration::new(secs, nanos).ok_or_else(|| {
            Error::new(
                ErrorKind::DataInvalid,
                format!("Failed to create 'TimeDelta' from seconds {secs} and nanos {nanos}"),
            )
        })?;

        let days = (delta.num_days() - offset) as i32;

        Ok(days)
    }

    fn day_timestamp_nano(v: i64) -> Result<i32> {
        let secs = v / NANOS_PER_SECOND;

        let (nanos, offset) = if v >= 0 {
            let nanos = (v.rem_euclid(NANOS_PER_SECOND)) as u32;
            let offset = 0i64;
            (nanos, offset)
        } else {
            let v = v + 1;
            let nanos = (v.rem_euclid(NANOS_PER_SECOND)) as u32;
            let offset = 1i64;
            (nanos, offset)
        };

        let delta = Duration::new(secs, nanos).ok_or_else(|| {
            Error::new(
                ErrorKind::DataInvalid,
                format!("Failed to create 'TimeDelta' from seconds {secs} and nanos {nanos}"),
            )
        })?;

        let days = (delta.num_days() - offset) as i32;

        Ok(days)
    }
}

impl TransformFunction for Day {
    fn transform(&self, input: ArrayRef) -> Result<ArrayRef> {
        let res: Date32Array = match input.data_type() {
            DataType::Timestamp(TimeUnit::Microsecond, _) => input
                .as_any()
                .downcast_ref::<TimestampMicrosecondArray>()
                .unwrap()
                .try_unary(|v| -> Result<i32> { Self::day_timestamp_micro(v) })?,
            DataType::Timestamp(TimeUnit::Nanosecond, _) => input
                .as_any()
                .downcast_ref::<TimestampNanosecondArray>()
                .unwrap()
                .try_unary(|v| -> Result<i32> { Self::day_timestamp_nano(v) })?,
            DataType::Date32 => input
                .as_any()
                .downcast_ref::<Date32Array>()
                .unwrap()
                .unary(|v| -> i32 { v }),
            _ => {
                return Err(Error::new(
                    ErrorKind::Unexpected,
                    format!(
                        "Should not call internally for unsupported data type {:?}",
                        input.data_type()
                    ),
                ));
            }
        };
        Ok(Arc::new(res))
    }

    fn transform_literal(&self, input: &crate::spec::Datum) -> Result<Option<crate::spec::Datum>> {
        let val = match (input.data_type(), input.literal()) {
            (PrimitiveType::Date, PrimitiveLiteral::Int(v)) => *v,
            (PrimitiveType::Timestamp, PrimitiveLiteral::Long(v)) => Self::day_timestamp_micro(*v)?,
            (PrimitiveType::Timestamptz, PrimitiveLiteral::Long(v)) => {
                Self::day_timestamp_micro(*v)?
            }
            (PrimitiveType::TimestampNs, PrimitiveLiteral::Long(v)) => {
                Self::day_timestamp_nano(*v)?
            }
            (PrimitiveType::TimestamptzNs, PrimitiveLiteral::Long(v)) => {
                Self::day_timestamp_nano(*v)?
            }
            _ => {
                return Err(crate::Error::new(
                    crate::ErrorKind::FeatureUnsupported,
                    format!(
                        "Unsupported data type for day transform: {:?}",
                        input.data_type()
                    ),
                ));
            }
        };
        Ok(Some(Datum::date(val)))
    }
}

/// Extract a timestamp hour, as hours from 1970-01-01 00:00:00
#[derive(Debug)]
pub struct Hour;

impl Hour {
    #[inline]
    fn hour_timestamp_micro(v: i64) -> i32 {
        v.div_euclid(MICROSECONDS_PER_HOUR) as i32
    }

    #[inline]
    fn hour_timestamp_nano(v: i64) -> i32 {
        v.div_euclid(NANOSECONDS_PER_HOUR) as i32
    }
}

impl TransformFunction for Hour {
    fn transform(&self, input: ArrayRef) -> Result<ArrayRef> {
        let res: Int32Array = match input.data_type() {
            DataType::Timestamp(TimeUnit::Microsecond, _) => input
                .as_any()
                .downcast_ref::<TimestampMicrosecondArray>()
                .unwrap()
                .unary(|v| -> i32 { Self::hour_timestamp_micro(v) }),
            _ => {
                return Err(crate::Error::new(
                    crate::ErrorKind::FeatureUnsupported,
                    format!(
                        "Unsupported data type for hour transform: {:?}",
                        input.data_type()
                    ),
                ));
            }
        };
        Ok(Arc::new(res))
    }

    fn transform_literal(&self, input: &crate::spec::Datum) -> Result<Option<crate::spec::Datum>> {
        let val = match (input.data_type(), input.literal()) {
            (PrimitiveType::Timestamp, PrimitiveLiteral::Long(v)) => Self::hour_timestamp_micro(*v),
            (PrimitiveType::Timestamptz, PrimitiveLiteral::Long(v)) => {
                Self::hour_timestamp_micro(*v)
            }
            (PrimitiveType::TimestampNs, PrimitiveLiteral::Long(v)) => {
                Self::hour_timestamp_nano(*v)
            }
            (PrimitiveType::TimestamptzNs, PrimitiveLiteral::Long(v)) => {
                Self::hour_timestamp_nano(*v)
            }
            _ => {
                return Err(crate::Error::new(
                    crate::ErrorKind::FeatureUnsupported,
                    format!(
                        "Unsupported data type for hour transform: {:?}",
                        input.data_type()
                    ),
                ));
            }
        };
        Ok(Some(Datum::int(val)))
    }
}

#[cfg(test)]
mod test {
    use std::sync::Arc;

    use arrow_array::{ArrayRef, Date32Array, Int32Array, TimestampMicrosecondArray};
    use chrono::{NaiveDate, NaiveDateTime};

    use crate::Result;
    use crate::expr::PredicateOperator;
    use crate::spec::PrimitiveType::{
        Binary, Date, Decimal, Fixed, Int, Long, String as StringType, Time, Timestamp,
        TimestampNs, Timestamptz, TimestamptzNs, Uuid,
    };
    use crate::spec::Type::{Primitive, Struct};
    use crate::spec::{Datum, NestedField, PrimitiveType, StructType, Transform, Type};
    use crate::transform::test::{TestProjectionFixture, TestTransformFixture};
    use crate::transform::{BoxedTransformFunction, TransformFunction};

    #[test]
    fn test_year_transform() {
        let trans = Transform::Year;

        let fixture = TestTransformFixture {
            display: "year".to_string(),
            json: r#""year""#.to_string(),
            dedup_name: "time".to_string(),
            preserves_order: true,
            satisfies_order_of: vec![
                (Transform::Year, true),
                (Transform::Month, false),
                (Transform::Day, false),
                (Transform::Hour, false),
                (Transform::Void, false),
                (Transform::Identity, false),
            ],
            trans_types: vec![
                (Primitive(Binary), None),
                (Primitive(Date), Some(Primitive(Int))),
                (
                    Primitive(Decimal {
                        precision: 8,
                        scale: 5,
                    }),
                    None,
                ),
                (Primitive(Fixed(8)), None),
                (Primitive(Int), None),
                (Primitive(Long), None),
                (Primitive(StringType), None),
                (Primitive(Uuid), None),
                (Primitive(Time), None),
                (Primitive(Timestamp), Some(Primitive(Int))),
                (Primitive(Timestamptz), Some(Primitive(Int))),
                (Primitive(TimestampNs), Some(Primitive(Int))),
                (Primitive(TimestamptzNs), Some(Primitive(Int))),
                (
                    Struct(StructType::new(vec![
                        NestedField::optional(1, "a", Primitive(Timestamp)).into(),
                    ])),
                    None,
                ),
            ],
        };

        fixture.assert_transform(trans);
    }

    #[test]
    fn test_month_transform() {
        let trans = Transform::Month;

        let fixture = TestTransformFixture {
            display: "month".to_string(),
            json: r#""month""#.to_string(),
            dedup_name: "time".to_string(),
            preserves_order: true,
            satisfies_order_of: vec![
                (Transform::Year, true),
                (Transform::Month, true),
                (Transform::Day, false),
                (Transform::Hour, false),
                (Transform::Void, false),
                (Transform::Identity, false),
            ],
            trans_types: vec![
                (Primitive(Binary), None),
                (Primitive(Date), Some(Primitive(Int))),
                (
                    Primitive(Decimal {
                        precision: 8,
                        scale: 5,
                    }),
                    None,
                ),
                (Primitive(Fixed(8)), None),
                (Primitive(Int), None),
                (Primitive(Long), None),
                (Primitive(StringType), None),
                (Primitive(Uuid), None),
                (Primitive(Time), None),
                (Primitive(Timestamp), Some(Primitive(Int))),
                (Primitive(Timestamptz), Some(Primitive(Int))),
                (Primitive(TimestampNs), Some(Primitive(Int))),
                (Primitive(TimestamptzNs), Some(Primitive(Int))),
                (
                    Struct(StructType::new(vec![
                        NestedField::optional(1, "a", Primitive(Timestamp)).into(),
                    ])),
                    None,
                ),
            ],
        };

        fixture.assert_transform(trans);
    }

    #[test]
    fn test_day_transform() {
        let trans = Transform::Day;

        let fixture = TestTransformFixture {
            display: "day".to_string(),
            json: r#""day""#.to_string(),
            dedup_name: "time".to_string(),
            preserves_order: true,
            satisfies_order_of: vec![
                (Transform::Year, true),
                (Transform::Month, true),
                (Transform::Day, true),
                (Transform::Hour, false),
                (Transform::Void, false),
                (Transform::Identity, false),
            ],
            trans_types: vec![
                (Primitive(Binary), None),
                (Primitive(Date), Some(Primitive(Date))),
                (
                    Primitive(Decimal {
                        precision: 8,
                        scale: 5,
                    }),
                    None,
                ),
                (Primitive(Fixed(8)), None),
                (Primitive(Int), None),
                (Primitive(Long), None),
                (Primitive(StringType), None),
                (Primitive(Uuid), None),
                (Primitive(Time), None),
                (Primitive(Timestamp), Some(Primitive(Date))),
                (Primitive(Timestamptz), Some(Primitive(Date))),
                (Primitive(TimestampNs), Some(Primitive(Date))),
                (Primitive(TimestamptzNs), Some(Primitive(Date))),
                (
                    Struct(StructType::new(vec![
                        NestedField::optional(1, "a", Primitive(Timestamp)).into(),
                    ])),
                    None,
                ),
            ],
        };

        fixture.assert_transform(trans);
    }

    #[test]
    fn test_hour_transform() {
        let trans = Transform::Hour;

        let fixture = TestTransformFixture {
            display: "hour".to_string(),
            json: r#""hour""#.to_string(),
            dedup_name: "time".to_string(),
            preserves_order: true,
            satisfies_order_of: vec![
                (Transform::Year, true),
                (Transform::Month, true),
                (Transform::Day, true),
                (Transform::Hour, true),
                (Transform::Void, false),
                (Transform::Identity, false),
            ],
            trans_types: vec![
                (Primitive(Binary), None),
                (Primitive(Date), None),
                (
                    Primitive(Decimal {
                        precision: 8,
                        scale: 5,
                    }),
                    None,
                ),
                (Primitive(Fixed(8)), None),
                (Primitive(Int), None),
                (Primitive(Long), None),
                (Primitive(StringType), None),
                (Primitive(Uuid), None),
                (Primitive(Time), None),
                (Primitive(Timestamp), Some(Primitive(Int))),
                (Primitive(Timestamptz), Some(Primitive(Int))),
                (Primitive(TimestampNs), Some(Primitive(Int))),
                (Primitive(TimestamptzNs), Some(Primitive(Int))),
                (
                    Struct(StructType::new(vec![
                        NestedField::optional(1, "a", Primitive(Timestamp)).into(),
                    ])),
                    None,
                ),
            ],
        };

        fixture.assert_transform(trans);
    }

    #[test]
    fn test_projection_timestamp_hour_upper_bound() -> Result<()> {
        // 420034
        let value = "2017-12-01T10:59:59.999999";
        // 412007
        let another = "2016-12-31T23:59:59.999999";

        let fixture = TestProjectionFixture::new(
            Transform::Hour,
            "name",
            NestedField::required(1, "value", Type::Primitive(PrimitiveType::Timestamp)),
        );

        fixture.assert_projection(
            &fixture.binary_predicate(
                PredicateOperator::LessThan,
                Datum::timestamp_from_str(value)?,
            ),
            Some("name <= 420034"),
        )?;

        fixture.assert_projection(
            &fixture.binary_predicate(
                PredicateOperator::LessThanOrEq,
                Datum::timestamp_from_str(value)?,
            ),
            Some("name <= 420034"),
        )?;

        fixture.assert_projection(
            &fixture.binary_predicate(
                PredicateOperator::GreaterThan,
                Datum::timestamp_from_str(value)?,
            ),
            Some("name >= 420035"),
        )?;

        fixture.assert_projection(
            &fixture.binary_predicate(
                PredicateOperator::GreaterThanOrEq,
                Datum::timestamp_from_str(value)?,
            ),
            Some("name >= 420034"),
        )?;

        fixture.assert_projection(
            &fixture.binary_predicate(PredicateOperator::Eq, Datum::timestamp_from_str(value)?),
            Some("name = 420034"),
        )?;

        fixture.assert_projection(
            &fixture.binary_predicate(PredicateOperator::NotEq, Datum::timestamp_from_str(value)?),
            None,
        )?;

        fixture.assert_projection(
            &fixture.set_predicate(PredicateOperator::In, vec![
                Datum::timestamp_from_str(value)?,
                Datum::timestamp_from_str(another)?,
            ]),
            Some("name IN (420034, 412007)"),
        )?;

        fixture.assert_projection(
            &fixture.set_predicate(PredicateOperator::NotIn, vec![
                Datum::timestamp_from_str(value)?,
                Datum::timestamp_from_str(another)?,
            ]),
            None,
        )?;

        Ok(())
    }

    #[test]
    fn test_projection_timestamp_hour_lower_bound() -> Result<()> {
        // 420034
        let value = "2017-12-01T10:00:00.000000";
        // 411288
        let another = "2016-12-02T00:00:00.000000";

        let fixture = TestProjectionFixture::new(
            Transform::Hour,
            "name",
            NestedField::required(1, "value", Type::Primitive(PrimitiveType::Timestamp)),
        );

        fixture.assert_projection(
            &fixture.binary_predicate(
                PredicateOperator::LessThan,
                Datum::timestamp_from_str(value)?,
            ),
            Some("name <= 420033"),
        )?;

        fixture.assert_projection(
            &fixture.binary_predicate(
                PredicateOperator::LessThanOrEq,
                Datum::timestamp_from_str(value)?,
            ),
            Some("name <= 420034"),
        )?;

        fixture.assert_projection(
            &fixture.binary_predicate(
                PredicateOperator::GreaterThan,
                Datum::timestamp_from_str(value)?,
            ),
            Some("name >= 420034"),
        )?;

        fixture.assert_projection(
            &fixture.binary_predicate(
                PredicateOperator::GreaterThanOrEq,
                Datum::timestamp_from_str(value)?,
            ),
            Some("name >= 420034"),
        )?;

        fixture.assert_projection(
            &fixture.binary_predicate(PredicateOperator::Eq, Datum::timestamp_from_str(value)?),
            Some("name = 420034"),
        )?;

        fixture.assert_projection(
            &fixture.binary_predicate(PredicateOperator::NotEq, Datum::timestamp_from_str(value)?),
            None,
        )?;

        fixture.assert_projection(
            &fixture.set_predicate(PredicateOperator::In, vec![
                Datum::timestamp_from_str(value)?,
                Datum::timestamp_from_str(another)?,
            ]),
            Some("name IN (411288, 420034)"),
        )?;

        fixture.assert_projection(
            &fixture.set_predicate(PredicateOperator::NotIn, vec![
                Datum::timestamp_from_str(value)?,
                Datum::timestamp_from_str(another)?,
            ]),
            None,
        )?;

        Ok(())
    }

    #[test]
    fn test_projection_timestamp_year_upper_bound() -> Result<()> {
        let value = "2017-12-31T23:59:59.999999";
        let another = "2016-12-31T23:59:59.999999";

        let fixture = TestProjectionFixture::new(
            Transform::Year,
            "name",
            NestedField::required(1, "value", Type::Primitive(PrimitiveType::Timestamp)),
        );

        fixture.assert_projection(
            &fixture.binary_predicate(
                PredicateOperator::LessThan,
                Datum::timestamp_from_str(value)?,
            ),
            Some("name <= 47"),
        )?;

        fixture.assert_projection(
            &fixture.binary_predicate(
                PredicateOperator::LessThanOrEq,
                Datum::timestamp_from_str(value)?,
            ),
            Some("name <= 47"),
        )?;

        fixture.assert_projection(
            &fixture.binary_predicate(
                PredicateOperator::GreaterThan,
                Datum::timestamp_from_str(value)?,
            ),
            Some("name >= 48"),
        )?;

        fixture.assert_projection(
            &fixture.binary_predicate(
                PredicateOperator::GreaterThanOrEq,
                Datum::timestamp_from_str(value)?,
            ),
            Some("name >= 47"),
        )?;

        fixture.assert_projection(
            &fixture.binary_predicate(PredicateOperator::Eq, Datum::timestamp_from_str(value)?),
            Some("name = 47"),
        )?;

        fixture.assert_projection(
            &fixture.binary_predicate(PredicateOperator::NotEq, Datum::timestamp_from_str(value)?),
            None,
        )?;

        fixture.assert_projection(
            &fixture.set_predicate(PredicateOperator::In, vec![
                Datum::timestamp_from_str(value)?,
                Datum::timestamp_from_str(another)?,
            ]),
            Some("name IN (47, 46)"),
        )?;

        fixture.assert_projection(
            &fixture.set_predicate(PredicateOperator::NotIn, vec![
                Datum::timestamp_from_str(value)?,
                Datum::timestamp_from_str(another)?,
            ]),
            None,
        )?;

        Ok(())
    }

    #[test]
    fn test_projection_timestamp_year_lower_bound() -> Result<()> {
        let value = "2017-01-01T00:00:00.000000";
        let another = "2016-12-02T00:00:00.000000";

        let fixture = TestProjectionFixture::new(
            Transform::Year,
            "name",
            NestedField::required(1, "value", Type::Primitive(PrimitiveType::Timestamp)),
        );

        fixture.assert_projection(
            &fixture.binary_predicate(
                PredicateOperator::LessThan,
                Datum::timestamp_from_str(value)?,
            ),
            Some("name <= 46"),
        )?;

        fixture.assert_projection(
            &fixture.binary_predicate(
                PredicateOperator::LessThanOrEq,
                Datum::timestamp_from_str(value)?,
            ),
            Some("name <= 47"),
        )?;

        fixture.assert_projection(
            &fixture.binary_predicate(
                PredicateOperator::GreaterThan,
                Datum::timestamp_from_str(value)?,
            ),
            Some("name >= 47"),
        )?;

        fixture.assert_projection(
            &fixture.binary_predicate(
                PredicateOperator::GreaterThanOrEq,
                Datum::timestamp_from_str(value)?,
            ),
            Some("name >= 47"),
        )?;

        fixture.assert_projection(
            &fixture.binary_predicate(PredicateOperator::Eq, Datum::timestamp_from_str(value)?),
            Some("name = 47"),
        )?;

        fixture.assert_projection(
            &fixture.binary_predicate(PredicateOperator::NotEq, Datum::timestamp_from_str(value)?),
            None,
        )?;

        fixture.assert_projection(
            &fixture.set_predicate(PredicateOperator::In, vec![
                Datum::timestamp_from_str(value)?,
                Datum::timestamp_from_str(another)?,
            ]),
            Some("name IN (47, 46)"),
        )?;

        fixture.assert_projection(
            &fixture.set_predicate(PredicateOperator::NotIn, vec![
                Datum::timestamp_from_str(value)?,
                Datum::timestamp_from_str(another)?,
            ]),
            None,
        )?;

        Ok(())
    }

    #[test]
    fn test_projection_timestamp_month_negative_upper_bound() -> Result<()> {
        let value = "1969-12-31T23:59:59.999999";
        let another = "1970-01-01T00:00:00.000000";

        let fixture = TestProjectionFixture::new(
            Transform::Month,
            "name",
            NestedField::required(1, "value", Type::Primitive(PrimitiveType::Timestamp)),
        );

        fixture.assert_projection(
            &fixture.binary_predicate(
                PredicateOperator::LessThan,
                Datum::timestamp_from_str(value)?,
            ),
            Some("name <= 0"),
        )?;

        fixture.assert_projection(
            &fixture.binary_predicate(
                PredicateOperator::LessThanOrEq,
                Datum::timestamp_from_str(value)?,
            ),
            Some("name <= 0"),
        )?;

        fixture.assert_projection(
            &fixture.binary_predicate(
                PredicateOperator::GreaterThan,
                Datum::timestamp_from_str(value)?,
            ),
            Some("name >= 0"),
        )?;

        fixture.assert_projection(
            &fixture.binary_predicate(
                PredicateOperator::GreaterThanOrEq,
                Datum::timestamp_from_str(value)?,
            ),
            Some("name >= -1"),
        )?;

        fixture.assert_projection(
            &fixture.binary_predicate(PredicateOperator::Eq, Datum::timestamp_from_str(value)?),
            Some("name IN (-1, 0)"),
        )?;

        fixture.assert_projection(
            &fixture.binary_predicate(PredicateOperator::NotEq, Datum::timestamp_from_str(value)?),
            None,
        )?;

        fixture.assert_projection(
            &fixture.set_predicate(PredicateOperator::In, vec![
                Datum::timestamp_from_str(value)?,
                Datum::timestamp_from_str(another)?,
            ]),
            Some("name IN (0, -1)"),
        )?;

        fixture.assert_projection(
            &fixture.set_predicate(PredicateOperator::NotIn, vec![
                Datum::timestamp_from_str(value)?,
                Datum::timestamp_from_str(another)?,
            ]),
            None,
        )?;

        Ok(())
    }

    #[test]
    fn test_projection_timestamp_month_upper_bound() -> Result<()> {
        let value = "2017-12-01T23:59:59.999999";
        let another = "2017-11-02T00:00:00.000000";

        let fixture = TestProjectionFixture::new(
            Transform::Month,
            "name",
            NestedField::required(1, "value", Type::Primitive(PrimitiveType::Timestamp)),
        );

        fixture.assert_projection(
            &fixture.binary_predicate(
                PredicateOperator::LessThan,
                Datum::timestamp_from_str(value)?,
            ),
            Some("name <= 575"),
        )?;

        fixture.assert_projection(
            &fixture.binary_predicate(
                PredicateOperator::LessThanOrEq,
                Datum::timestamp_from_str(value)?,
            ),
            Some("name <= 575"),
        )?;

        fixture.assert_projection(
            &fixture.binary_predicate(
                PredicateOperator::GreaterThan,
                Datum::timestamp_from_str(value)?,
            ),
            Some("name >= 575"),
        )?;

        fixture.assert_projection(
            &fixture.binary_predicate(
                PredicateOperator::GreaterThanOrEq,
                Datum::timestamp_from_str(value)?,
            ),
            Some("name >= 575"),
        )?;

        fixture.assert_projection(
            &fixture.binary_predicate(PredicateOperator::Eq, Datum::timestamp_from_str(value)?),
            Some("name = 575"),
        )?;

        fixture.assert_projection(
            &fixture.binary_predicate(PredicateOperator::NotEq, Datum::timestamp_from_str(value)?),
            None,
        )?;

        fixture.assert_projection(
            &fixture.set_predicate(PredicateOperator::In, vec![
                Datum::timestamp_from_str(value)?,
                Datum::timestamp_from_str(another)?,
            ]),
            Some("name IN (575, 574)"),
        )?;

        fixture.assert_projection(
            &fixture.set_predicate(PredicateOperator::NotIn, vec![
                Datum::timestamp_from_str(value)?,
                Datum::timestamp_from_str(another)?,
            ]),
            None,
        )?;
        Ok(())
    }

    #[test]
    fn test_projection_timestamp_month_negative_lower_bound() -> Result<()> {
        let value = "1969-01-01T00:00:00.000000";
        let another = "1969-03-01T00:00:00.000000";

        let fixture = TestProjectionFixture::new(
            Transform::Month,
            "name",
            NestedField::required(1, "value", Type::Primitive(PrimitiveType::Timestamp)),
        );

        fixture.assert_projection(
            &fixture.binary_predicate(
                PredicateOperator::LessThan,
                Datum::timestamp_from_str(value)?,
            ),
            Some("name <= -12"),
        )?;

        fixture.assert_projection(
            &fixture.binary_predicate(
                PredicateOperator::LessThanOrEq,
                Datum::timestamp_from_str(value)?,
            ),
            Some("name <= -11"),
        )?;

        fixture.assert_projection(
            &fixture.binary_predicate(
                PredicateOperator::GreaterThan,
                Datum::timestamp_from_str(value)?,
            ),
            Some("name >= -12"),
        )?;

        fixture.assert_projection(
            &fixture.binary_predicate(
                PredicateOperator::GreaterThanOrEq,
                Datum::timestamp_from_str(value)?,
            ),
            Some("name >= -12"),
        )?;

        fixture.assert_projection(
            &fixture.binary_predicate(PredicateOperator::Eq, Datum::timestamp_from_str(value)?),
            Some("name IN (-12, -11)"),
        )?;

        fixture.assert_projection(
            &fixture.binary_predicate(PredicateOperator::NotEq, Datum::timestamp_from_str(value)?),
            None,
        )?;

        fixture.assert_projection(
            &fixture.set_predicate(PredicateOperator::In, vec![
                Datum::timestamp_from_str(value)?,
                Datum::timestamp_from_str(another)?,
            ]),
            Some("name IN (-10, -9, -12, -11)"),
        )?;

        fixture.assert_projection(
            &fixture.set_predicate(PredicateOperator::NotIn, vec![
                Datum::timestamp_from_str(value)?,
                Datum::timestamp_from_str(another)?,
            ]),
            None,
        )?;

        Ok(())
    }

    #[test]
    fn test_projection_timestamp_month_lower_bound() -> Result<()> {
        let value = "2017-12-01T00:00:00.000000";
        let another = "2017-12-02T00:00:00.000000";

        let fixture = TestProjectionFixture::new(
            Transform::Month,
            "name",
            NestedField::required(1, "value", Type::Primitive(PrimitiveType::Timestamp)),
        );

        fixture.assert_projection(
            &fixture.binary_predicate(
                PredicateOperator::LessThan,
                Datum::timestamp_from_str(value)?,
            ),
            Some("name <= 574"),
        )?;

        fixture.assert_projection(
            &fixture.binary_predicate(
                PredicateOperator::LessThanOrEq,
                Datum::timestamp_from_str(value)?,
            ),
            Some("name <= 575"),
        )?;

        fixture.assert_projection(
            &fixture.binary_predicate(
                PredicateOperator::GreaterThan,
                Datum::timestamp_from_str(value)?,
            ),
            Some("name >= 575"),
        )?;

        fixture.assert_projection(
            &fixture.binary_predicate(
                PredicateOperator::GreaterThanOrEq,
                Datum::timestamp_from_str(value)?,
            ),
            Some("name >= 575"),
        )?;

        fixture.assert_projection(
            &fixture.binary_predicate(PredicateOperator::Eq, Datum::timestamp_from_str(value)?),
            Some("name = 575"),
        )?;

        fixture.assert_projection(
            &fixture.binary_predicate(PredicateOperator::NotEq, Datum::timestamp_from_str(value)?),
            None,
        )?;

        fixture.assert_projection(
            &fixture.set_predicate(PredicateOperator::In, vec![
                Datum::timestamp_from_str(value)?,
                Datum::timestamp_from_str(another)?,
            ]),
            Some("name IN (575)"),
        )?;

        fixture.assert_projection(
            &fixture.set_predicate(PredicateOperator::NotIn, vec![
                Datum::timestamp_from_str(value)?,
                Datum::timestamp_from_str(another)?,
            ]),
            None,
        )?;

        Ok(())
    }

    #[test]
    fn test_projection_timestamp_day_negative_upper_bound() -> Result<()> {
        // -1
        let value = "1969-12-31T23:59:59.999999";
        // 0
        let another = "1970-01-01T00:00:00.000000";

        let fixture = TestProjectionFixture::new(
            Transform::Day,
            "name",
            NestedField::required(1, "value", Type::Primitive(PrimitiveType::Timestamp)),
        );

        fixture.assert_projection(
            &fixture.binary_predicate(
                PredicateOperator::LessThan,
                Datum::timestamp_from_str(value)?,
            ),
            Some("name <= 1970-01-01"),
        )?;

        fixture.assert_projection(
            &fixture.binary_predicate(
                PredicateOperator::LessThanOrEq,
                Datum::timestamp_from_str(value)?,
            ),
            Some("name <= 1970-01-01"),
        )?;

        fixture.assert_projection(
            &fixture.binary_predicate(
                PredicateOperator::GreaterThan,
                Datum::timestamp_from_str(value)?,
            ),
            Some("name >= 1970-01-01"),
        )?;

        fixture.assert_projection(
            &fixture.binary_predicate(
                PredicateOperator::GreaterThanOrEq,
                Datum::timestamp_from_str(value)?,
            ),
            Some("name >= 1969-12-31"),
        )?;

        fixture.assert_projection(
            &fixture.binary_predicate(PredicateOperator::Eq, Datum::timestamp_from_str(value)?),
            Some("name IN (1969-12-31, 1970-01-01)"),
        )?;

        fixture.assert_projection(
            &fixture.binary_predicate(PredicateOperator::NotEq, Datum::timestamp_from_str(value)?),
            None,
        )?;

        fixture.assert_projection(
            &fixture.set_predicate(PredicateOperator::In, vec![
                Datum::timestamp_from_str(value)?,
                Datum::timestamp_from_str(another)?,
            ]),
            Some("name IN (1970-01-01, 1969-12-31)"),
        )?;

        fixture.assert_projection(
            &fixture.set_predicate(PredicateOperator::NotIn, vec![
                Datum::timestamp_from_str(value)?,
                Datum::timestamp_from_str(another)?,
            ]),
            None,
        )?;

        Ok(())
    }

    #[test]
    fn test_projection_timestamp_day_upper_bound() -> Result<()> {
        // 17501
        let value = "2017-12-01T23:59:59.999999";
        // 17502
        let another = "2017-12-02T00:00:00.000000";

        let fixture = TestProjectionFixture::new(
            Transform::Day,
            "name",
            NestedField::required(1, "value", Type::Primitive(PrimitiveType::Timestamp)),
        );

        fixture.assert_projection(
            &fixture.binary_predicate(
                PredicateOperator::LessThan,
                Datum::timestamp_from_str(value)?,
            ),
            Some("name <= 2017-12-01"),
        )?;

        fixture.assert_projection(
            &fixture.binary_predicate(
                PredicateOperator::LessThanOrEq,
                Datum::timestamp_from_str(value)?,
            ),
            Some("name <= 2017-12-01"),
        )?;

        fixture.assert_projection(
            &fixture.binary_predicate(
                PredicateOperator::GreaterThan,
                Datum::timestamp_from_str(value)?,
            ),
            Some("name >= 2017-12-02"),
        )?;

        fixture.assert_projection(
            &fixture.binary_predicate(
                PredicateOperator::GreaterThanOrEq,
                Datum::timestamp_from_str(value)?,
            ),
            Some("name >= 2017-12-01"),
        )?;

        fixture.assert_projection(
            &fixture.binary_predicate(PredicateOperator::Eq, Datum::timestamp_from_str(value)?),
            Some("name = 2017-12-01"),
        )?;

        fixture.assert_projection(
            &fixture.binary_predicate(PredicateOperator::NotEq, Datum::timestamp_from_str(value)?),
            None,
        )?;

        fixture.assert_projection(
            &fixture.set_predicate(PredicateOperator::In, vec![
                Datum::timestamp_from_str(value)?,
                Datum::timestamp_from_str(another)?,
            ]),
            Some("name IN (2017-12-02, 2017-12-01)"),
        )?;

        fixture.assert_projection(
            &fixture.set_predicate(PredicateOperator::NotIn, vec![
                Datum::timestamp_from_str(value)?,
                Datum::timestamp_from_str(another)?,
            ]),
            None,
        )?;

        Ok(())
    }

    #[test]
    fn test_projection_timestamp_day_negative_lower_bound() -> Result<()> {
        // -365
        let value = "1969-01-01T00:00:00.000000";
        // -364
        let another = "1969-01-02T00:00:00.000000";

        let fixture = TestProjectionFixture::new(
            Transform::Day,
            "name",
            NestedField::required(1, "value", Type::Primitive(PrimitiveType::Timestamp)),
        );

        fixture.assert_projection(
            &fixture.binary_predicate(
                PredicateOperator::LessThan,
                Datum::timestamp_from_str(value)?,
            ),
            Some("name <= 1969-01-01"),
        )?;

        fixture.assert_projection(
            &fixture.binary_predicate(
                PredicateOperator::LessThanOrEq,
                Datum::timestamp_from_str(value)?,
            ),
            Some("name <= 1969-01-02"),
        )?;

        fixture.assert_projection(
            &fixture.binary_predicate(
                PredicateOperator::GreaterThan,
                Datum::timestamp_from_str(value)?,
            ),
            Some("name >= 1969-01-01"),
        )?;

        fixture.assert_projection(
            &fixture.binary_predicate(
                PredicateOperator::GreaterThanOrEq,
                Datum::timestamp_from_str(value)?,
            ),
            Some("name >= 1969-01-01"),
        )?;

        fixture.assert_projection(
            &fixture.binary_predicate(PredicateOperator::Eq, Datum::timestamp_from_str(value)?),
            Some("name IN (1969-01-01, 1969-01-02)"),
        )?;

        fixture.assert_projection(
            &fixture.binary_predicate(PredicateOperator::NotEq, Datum::timestamp_from_str(value)?),
            None,
        )?;

        fixture.assert_projection(
            &fixture.set_predicate(PredicateOperator::In, vec![
                Datum::timestamp_from_str(value)?,
                Datum::timestamp_from_str(another)?,
            ]),
            Some("name IN (1969-01-02, 1969-01-01, 1969-01-03)"),
        )?;

        fixture.assert_projection(
            &fixture.set_predicate(PredicateOperator::NotIn, vec![
                Datum::timestamp_from_str(value)?,
                Datum::timestamp_from_str(another)?,
            ]),
            None,
        )?;

        Ok(())
    }

    #[test]
    fn test_projection_timestamp_day_lower_bound() -> Result<()> {
        // 17501
        let value = "2017-12-01T00:00:00.000000";
        // 17502
        let another = "2017-12-02T00:00:00.000000";

        let fixture = TestProjectionFixture::new(
            Transform::Day,
            "name",
            NestedField::required(1, "value", Type::Primitive(PrimitiveType::Timestamp)),
        );

        fixture.assert_projection(
            &fixture.binary_predicate(
                PredicateOperator::LessThan,
                Datum::timestamp_from_str(value)?,
            ),
            Some("name <= 2017-11-30"),
        )?;

        fixture.assert_projection(
            &fixture.binary_predicate(
                PredicateOperator::LessThanOrEq,
                Datum::timestamp_from_str(value)?,
            ),
            Some("name <= 2017-12-01"),
        )?;

        fixture.assert_projection(
            &fixture.binary_predicate(
                PredicateOperator::GreaterThan,
                Datum::timestamp_from_str(value)?,
            ),
            Some("name >= 2017-12-01"),
        )?;

        fixture.assert_projection(
            &fixture.binary_predicate(
                PredicateOperator::GreaterThanOrEq,
                Datum::timestamp_from_str(value)?,
            ),
            Some("name >= 2017-12-01"),
        )?;

        fixture.assert_projection(
            &fixture.binary_predicate(PredicateOperator::Eq, Datum::timestamp_from_str(value)?),
            Some("name = 2017-12-01"),
        )?;

        fixture.assert_projection(
            &fixture.binary_predicate(PredicateOperator::NotEq, Datum::timestamp_from_str(value)?),
            None,
        )?;

        fixture.assert_projection(
            &fixture.set_predicate(PredicateOperator::In, vec![
                Datum::timestamp_from_str(value)?,
                Datum::timestamp_from_str(another)?,
            ]),
            Some("name IN (2017-12-02, 2017-12-01)"),
        )?;

        fixture.assert_projection(
            &fixture.set_predicate(PredicateOperator::NotIn, vec![
                Datum::timestamp_from_str(value)?,
                Datum::timestamp_from_str(another)?,
            ]),
            None,
        )?;

        Ok(())
    }

    #[test]
    fn test_projection_timestamp_day_epoch() -> Result<()> {
        // 0
        let value = "1970-01-01T00:00:00.00000";
        // 1
        let another = "1970-01-02T00:00:00.00000";

        let fixture = TestProjectionFixture::new(
            Transform::Day,
            "name",
            NestedField::required(1, "value", Type::Primitive(PrimitiveType::Timestamp)),
        );

        fixture.assert_projection(
            &fixture.binary_predicate(
                PredicateOperator::LessThan,
                Datum::timestamp_from_str(value)?,
            ),
            Some("name <= 1970-01-01"),
        )?;

        fixture.assert_projection(
            &fixture.binary_predicate(
                PredicateOperator::LessThanOrEq,
                Datum::timestamp_from_str(value)?,
            ),
            Some("name <= 1970-01-01"),
        )?;

        fixture.assert_projection(
            &fixture.binary_predicate(
                PredicateOperator::GreaterThan,
                Datum::timestamp_from_str(value)?,
            ),
            Some("name >= 1970-01-01"),
        )?;

        fixture.assert_projection(
            &fixture.binary_predicate(
                PredicateOperator::GreaterThanOrEq,
                Datum::timestamp_from_str(value)?,
            ),
            Some("name >= 1970-01-01"),
        )?;

        fixture.assert_projection(
            &fixture.binary_predicate(PredicateOperator::Eq, Datum::timestamp_from_str(value)?),
            Some("name = 1970-01-01"),
        )?;

        fixture.assert_projection(
            &fixture.binary_predicate(PredicateOperator::NotEq, Datum::timestamp_from_str(value)?),
            None,
        )?;

        fixture.assert_projection(
            &fixture.set_predicate(PredicateOperator::In, vec![
                Datum::timestamp_from_str(value)?,
                Datum::timestamp_from_str(another)?,
            ]),
            Some("name IN (1970-01-01, 1970-01-02)"),
        )?;

        fixture.assert_projection(
            &fixture.set_predicate(PredicateOperator::NotIn, vec![
                Datum::timestamp_from_str(value)?,
                Datum::timestamp_from_str(another)?,
            ]),
            None,
        )?;

        Ok(())
    }

    #[test]
    fn test_projection_date_day_negative() -> Result<()> {
        // -2
        let value = "1969-12-30";
        // -4
        let another = "1969-12-28";

        let fixture = TestProjectionFixture::new(
            Transform::Day,
            "name",
            NestedField::required(1, "value", Type::Primitive(PrimitiveType::Date)),
        );

        fixture.assert_projection(
            &fixture.binary_predicate(PredicateOperator::LessThan, Datum::date_from_str(value)?),
            Some("name <= 1969-12-29"),
        )?;

        fixture.assert_projection(
            &fixture.binary_predicate(
                PredicateOperator::LessThanOrEq,
                Datum::date_from_str(value)?,
            ),
            Some("name <= 1969-12-30"),
        )?;

        fixture.assert_projection(
            &fixture.binary_predicate(PredicateOperator::GreaterThan, Datum::date_from_str(value)?),
            Some("name >= 1969-12-31"),
        )?;

        fixture.assert_projection(
            &fixture.binary_predicate(
                PredicateOperator::GreaterThanOrEq,
                Datum::date_from_str(value)?,
            ),
            Some("name >= 1969-12-30"),
        )?;

        fixture.assert_projection(
            &fixture.binary_predicate(PredicateOperator::Eq, Datum::date_from_str(value)?),
            Some("name = 1969-12-30"),
        )?;

        fixture.assert_projection(
            &fixture.binary_predicate(PredicateOperator::NotEq, Datum::date_from_str(value)?),
            None,
        )?;

        fixture.assert_projection(
            &fixture.set_predicate(PredicateOperator::In, vec![
                Datum::date_from_str(value)?,
                Datum::date_from_str(another)?,
            ]),
            Some("name IN (1969-12-28, 1969-12-30)"),
        )?;

        fixture.assert_projection(
            &fixture.set_predicate(PredicateOperator::NotIn, vec![
                Datum::date_from_str(value)?,
                Datum::date_from_str(another)?,
            ]),
            None,
        )?;

        Ok(())
    }

    #[test]
    fn test_projection_date_day() -> Result<()> {
        // 17167
        let value = "2017-01-01";
        // 17531
        let another = "2017-12-31";

        let fixture = TestProjectionFixture::new(
            Transform::Day,
            "name",
            NestedField::required(1, "value", Type::Primitive(PrimitiveType::Date)),
        );

        fixture.assert_projection(
            &fixture.binary_predicate(PredicateOperator::LessThan, Datum::date_from_str(value)?),
            Some("name <= 2016-12-31"),
        )?;

        fixture.assert_projection(
            &fixture.binary_predicate(
                PredicateOperator::LessThanOrEq,
                Datum::date_from_str(value)?,
            ),
            Some("name <= 2017-01-01"),
        )?;

        fixture.assert_projection(
            &fixture.binary_predicate(PredicateOperator::GreaterThan, Datum::date_from_str(value)?),
            Some("name >= 2017-01-02"),
        )?;

        fixture.assert_projection(
            &fixture.binary_predicate(
                PredicateOperator::GreaterThanOrEq,
                Datum::date_from_str(value)?,
            ),
            Some("name >= 2017-01-01"),
        )?;

        fixture.assert_projection(
            &fixture.binary_predicate(PredicateOperator::Eq, Datum::date_from_str(value)?),
            Some("name = 2017-01-01"),
        )?;

        fixture.assert_projection(
            &fixture.binary_predicate(PredicateOperator::NotEq, Datum::date_from_str(value)?),
            None,
        )?;

        fixture.assert_projection(
            &fixture.set_predicate(PredicateOperator::In, vec![
                Datum::date_from_str(value)?,
                Datum::date_from_str(another)?,
            ]),
            Some("name IN (2017-01-01, 2017-12-31)"),
        )?;

        fixture.assert_projection(
            &fixture.set_predicate(PredicateOperator::NotIn, vec![
                Datum::date_from_str(value)?,
                Datum::date_from_str(another)?,
            ]),
            None,
        )?;

        Ok(())
    }

    #[test]
    fn test_projection_date_month_negative_upper_bound() -> Result<()> {
        // -1 => 1969-12
        let value = "1969-12-31";
        // -12 => 1969-01
        let another = "1969-01-01";

        let fixture = TestProjectionFixture::new(
            Transform::Month,
            "name",
            NestedField::required(1, "value", Type::Primitive(PrimitiveType::Date)),
        );

        fixture.assert_projection(
            &fixture.binary_predicate(PredicateOperator::LessThan, Datum::date_from_str(value)?),
            Some("name <= 0"),
        )?;

        fixture.assert_projection(
            &fixture.binary_predicate(
                PredicateOperator::LessThanOrEq,
                Datum::date_from_str(value)?,
            ),
            Some("name <= 0"),
        )?;

        fixture.assert_projection(
            &fixture.binary_predicate(PredicateOperator::GreaterThan, Datum::date_from_str(value)?),
            Some("name >= 0"),
        )?;

        fixture.assert_projection(
            &fixture.binary_predicate(
                PredicateOperator::GreaterThanOrEq,
                Datum::date_from_str(value)?,
            ),
            Some("name >= -1"),
        )?;

        fixture.assert_projection(
            &fixture.binary_predicate(PredicateOperator::Eq, Datum::date_from_str(value)?),
            Some("name IN (-1, 0)"),
        )?;

        fixture.assert_projection(
            &fixture.binary_predicate(PredicateOperator::NotEq, Datum::date_from_str(value)?),
            None,
        )?;

        fixture.assert_projection(
            &fixture.set_predicate(PredicateOperator::In, vec![
                Datum::date_from_str(value)?,
                Datum::date_from_str(another)?,
            ]),
            Some("name IN (-1, -12, -11, 0)"),
        )?;

        fixture.assert_projection(
            &fixture.set_predicate(PredicateOperator::NotIn, vec![
                Datum::date_from_str(value)?,
                Datum::date_from_str(another)?,
            ]),
            None,
        )?;

        Ok(())
    }

    #[test]
    fn test_projection_date_month_upper_bound() -> Result<()> {
        // 575 => 2017-12
        let value = "2017-12-31";
        // 564 => 2017-01
        let another = "2017-01-01";

        let fixture = TestProjectionFixture::new(
            Transform::Month,
            "name",
            NestedField::required(1, "value", Type::Primitive(PrimitiveType::Date)),
        );

        fixture.assert_projection(
            &fixture.binary_predicate(PredicateOperator::LessThan, Datum::date_from_str(value)?),
            Some("name <= 575"),
        )?;

        fixture.assert_projection(
            &fixture.binary_predicate(
                PredicateOperator::LessThanOrEq,
                Datum::date_from_str(value)?,
            ),
            Some("name <= 575"),
        )?;

        fixture.assert_projection(
            &fixture.binary_predicate(PredicateOperator::GreaterThan, Datum::date_from_str(value)?),
            Some("name >= 576"),
        )?;

        fixture.assert_projection(
            &fixture.binary_predicate(
                PredicateOperator::GreaterThanOrEq,
                Datum::date_from_str(value)?,
            ),
            Some("name >= 575"),
        )?;

        fixture.assert_projection(
            &fixture.binary_predicate(PredicateOperator::Eq, Datum::date_from_str(value)?),
            Some("name = 575"),
        )?;

        fixture.assert_projection(
            &fixture.binary_predicate(PredicateOperator::NotEq, Datum::date_from_str(value)?),
            None,
        )?;

        fixture.assert_projection(
            &fixture.set_predicate(PredicateOperator::In, vec![
                Datum::date_from_str(value)?,
                Datum::date_from_str(another)?,
            ]),
            Some("name IN (575, 564)"),
        )?;

        fixture.assert_projection(
            &fixture.set_predicate(PredicateOperator::NotIn, vec![
                Datum::date_from_str(value)?,
                Datum::date_from_str(another)?,
            ]),
            None,
        )?;

        Ok(())
    }

    #[test]
    fn test_projection_date_month_negative_lower_bound() -> Result<()> {
        // -12 => 1969-01
        let value = "1969-01-01";
        // -1 => 1969-12
        let another = "1969-12-31";

        let fixture = TestProjectionFixture::new(
            Transform::Month,
            "name",
            NestedField::required(1, "value", Type::Primitive(PrimitiveType::Date)),
        );

        fixture.assert_projection(
            &fixture.binary_predicate(PredicateOperator::LessThan, Datum::date_from_str(value)?),
            Some("name <= -12"),
        )?;

        fixture.assert_projection(
            &fixture.binary_predicate(
                PredicateOperator::LessThanOrEq,
                Datum::date_from_str(value)?,
            ),
            Some("name <= -11"),
        )?;

        fixture.assert_projection(
            &fixture.binary_predicate(PredicateOperator::GreaterThan, Datum::date_from_str(value)?),
            Some("name >= -12"),
        )?;

        fixture.assert_projection(
            &fixture.binary_predicate(
                PredicateOperator::GreaterThanOrEq,
                Datum::date_from_str(value)?,
            ),
            Some("name >= -12"),
        )?;

        fixture.assert_projection(
            &fixture.binary_predicate(PredicateOperator::Eq, Datum::date_from_str(value)?),
            Some("name IN (-12, -11)"),
        )?;

        fixture.assert_projection(
            &fixture.binary_predicate(PredicateOperator::NotEq, Datum::date_from_str(value)?),
            None,
        )?;

        fixture.assert_projection(
            &fixture.set_predicate(PredicateOperator::In, vec![
                Datum::date_from_str(value)?,
                Datum::date_from_str(another)?,
            ]),
            Some("name IN (-1, -12, -11, 0)"),
        )?;

        fixture.assert_projection(
            &fixture.set_predicate(PredicateOperator::NotIn, vec![
                Datum::date_from_str(value)?,
                Datum::date_from_str(another)?,
            ]),
            None,
        )?;

        Ok(())
    }

    #[test]
    fn test_projection_date_month_lower_bound() -> Result<()> {
        // 575 => 2017-12
        let value = "2017-12-01";
        // 564 => 2017-01
        let another = "2017-01-01";

        let fixture = TestProjectionFixture::new(
            Transform::Month,
            "name",
            NestedField::required(1, "value", Type::Primitive(PrimitiveType::Date)),
        );

        fixture.assert_projection(
            &fixture.binary_predicate(PredicateOperator::LessThan, Datum::date_from_str(value)?),
            Some("name <= 574"),
        )?;

        fixture.assert_projection(
            &fixture.binary_predicate(
                PredicateOperator::LessThanOrEq,
                Datum::date_from_str(value)?,
            ),
            Some("name <= 575"),
        )?;

        fixture.assert_projection(
            &fixture.binary_predicate(PredicateOperator::GreaterThan, Datum::date_from_str(value)?),
            Some("name >= 575"),
        )?;

        fixture.assert_projection(
            &fixture.binary_predicate(
                PredicateOperator::GreaterThanOrEq,
                Datum::date_from_str(value)?,
            ),
            Some("name >= 575"),
        )?;

        fixture.assert_projection(
            &fixture.binary_predicate(PredicateOperator::Eq, Datum::date_from_str(value)?),
            Some("name = 575"),
        )?;

        fixture.assert_projection(
            &fixture.binary_predicate(PredicateOperator::NotEq, Datum::date_from_str(value)?),
            None,
        )?;

        fixture.assert_projection(
            &fixture.set_predicate(PredicateOperator::In, vec![
                Datum::date_from_str(value)?,
                Datum::date_from_str(another)?,
            ]),
            Some("name IN (575, 564)"),
        )?;

        fixture.assert_projection(
            &fixture.set_predicate(PredicateOperator::NotIn, vec![
                Datum::date_from_str(value)?,
                Datum::date_from_str(another)?,
            ]),
            None,
        )?;

        Ok(())
    }

    #[test]
    fn test_projection_date_month_epoch() -> Result<()> {
        // 0 => 1970-01
        let value = "1970-01-01";
        // -1 => 1969-12
        let another = "1969-12-31";

        let fixture = TestProjectionFixture::new(
            Transform::Month,
            "name",
            NestedField::required(1, "value", Type::Primitive(PrimitiveType::Date)),
        );

        fixture.assert_projection(
            &fixture.binary_predicate(PredicateOperator::LessThan, Datum::date_from_str(value)?),
            Some("name <= 0"),
        )?;

        fixture.assert_projection(
            &fixture.binary_predicate(
                PredicateOperator::LessThanOrEq,
                Datum::date_from_str(value)?,
            ),
            Some("name <= 0"),
        )?;

        fixture.assert_projection(
            &fixture.binary_predicate(PredicateOperator::GreaterThan, Datum::date_from_str(value)?),
            Some("name >= 0"),
        )?;

        fixture.assert_projection(
            &fixture.binary_predicate(
                PredicateOperator::GreaterThanOrEq,
                Datum::date_from_str(value)?,
            ),
            Some("name >= 0"),
        )?;

        fixture.assert_projection(
            &fixture.binary_predicate(PredicateOperator::Eq, Datum::date_from_str(value)?),
            Some("name = 0"),
        )?;

        fixture.assert_projection(
            &fixture.binary_predicate(PredicateOperator::NotEq, Datum::date_from_str(value)?),
            None,
        )?;

        fixture.assert_projection(
            &fixture.set_predicate(PredicateOperator::In, vec![
                Datum::date_from_str(value)?,
                Datum::date_from_str(another)?,
            ]),
            Some("name IN (0, -1)"),
        )?;

        fixture.assert_projection(
            &fixture.set_predicate(PredicateOperator::NotIn, vec![
                Datum::date_from_str(value)?,
                Datum::date_from_str(another)?,
            ]),
            None,
        )?;

        Ok(())
    }

    #[test]
    fn test_projection_date_year_negative_upper_bound() -> Result<()> {
        // -1 => 1969
        let value = "1969-12-31";
        let another = "1969-01-01";

        let fixture = TestProjectionFixture::new(
            Transform::Year,
            "name",
            NestedField::required(1, "value", Type::Primitive(PrimitiveType::Date)),
        );

        fixture.assert_projection(
            &fixture.binary_predicate(PredicateOperator::LessThan, Datum::date_from_str(value)?),
            Some("name <= 0"),
        )?;

        fixture.assert_projection(
            &fixture.binary_predicate(
                PredicateOperator::LessThanOrEq,
                Datum::date_from_str(value)?,
            ),
            Some("name <= 0"),
        )?;

        fixture.assert_projection(
            &fixture.binary_predicate(PredicateOperator::GreaterThan, Datum::date_from_str(value)?),
            Some("name >= 0"),
        )?;

        fixture.assert_projection(
            &fixture.binary_predicate(
                PredicateOperator::GreaterThanOrEq,
                Datum::date_from_str(value)?,
            ),
            Some("name >= -1"),
        )?;

        fixture.assert_projection(
            &fixture.binary_predicate(PredicateOperator::Eq, Datum::date_from_str(value)?),
            Some("name IN (-1, 0)"),
        )?;

        fixture.assert_projection(
            &fixture.binary_predicate(PredicateOperator::NotEq, Datum::date_from_str(value)?),
            None,
        )?;

        fixture.assert_projection(
            &fixture.set_predicate(PredicateOperator::In, vec![
                Datum::date_from_str(value)?,
                Datum::date_from_str(another)?,
            ]),
            Some("name IN (0, -1)"),
        )?;

        fixture.assert_projection(
            &fixture.set_predicate(PredicateOperator::NotIn, vec![
                Datum::date_from_str(value)?,
                Datum::date_from_str(another)?,
            ]),
            None,
        )?;

        Ok(())
    }

    #[test]
    fn test_projection_date_year_upper_bound() -> Result<()> {
        // 47 => 2017
        let value = "2017-12-31";
        // 46 => 2016
        let another = "2016-01-01";

        let fixture = TestProjectionFixture::new(
            Transform::Year,
            "name",
            NestedField::required(1, "value", Type::Primitive(PrimitiveType::Date)),
        );

        fixture.assert_projection(
            &fixture.binary_predicate(PredicateOperator::LessThan, Datum::date_from_str(value)?),
            Some("name <= 47"),
        )?;

        fixture.assert_projection(
            &fixture.binary_predicate(
                PredicateOperator::LessThanOrEq,
                Datum::date_from_str(value)?,
            ),
            Some("name <= 47"),
        )?;

        fixture.assert_projection(
            &fixture.binary_predicate(PredicateOperator::GreaterThan, Datum::date_from_str(value)?),
            Some("name >= 48"),
        )?;

        fixture.assert_projection(
            &fixture.binary_predicate(
                PredicateOperator::GreaterThanOrEq,
                Datum::date_from_str(value)?,
            ),
            Some("name >= 47"),
        )?;

        fixture.assert_projection(
            &fixture.binary_predicate(PredicateOperator::Eq, Datum::date_from_str(value)?),
            Some("name = 47"),
        )?;

        fixture.assert_projection(
            &fixture.binary_predicate(PredicateOperator::NotEq, Datum::date_from_str(value)?),
            None,
        )?;

        fixture.assert_projection(
            &fixture.set_predicate(PredicateOperator::In, vec![
                Datum::date_from_str(value)?,
                Datum::date_from_str(another)?,
            ]),
            Some("name IN (47, 46)"),
        )?;

        fixture.assert_projection(
            &fixture.set_predicate(PredicateOperator::NotIn, vec![
                Datum::date_from_str(value)?,
                Datum::date_from_str(another)?,
            ]),
            None,
        )?;

        Ok(())
    }

    #[test]
    fn test_projection_date_year_negative_lower_bound() -> Result<()> {
        // 0 => 1970
        let value = "1970-01-01";
        // -1 => 1969
        let another = "1969-12-31";

        let fixture = TestProjectionFixture::new(
            Transform::Year,
            "name",
            NestedField::required(1, "value", Type::Primitive(PrimitiveType::Date)),
        );

        fixture.assert_projection(
            &fixture.binary_predicate(PredicateOperator::LessThan, Datum::date_from_str(value)?),
            Some("name <= 0"),
        )?;

        fixture.assert_projection(
            &fixture.binary_predicate(
                PredicateOperator::LessThanOrEq,
                Datum::date_from_str(value)?,
            ),
            Some("name <= 0"),
        )?;

        fixture.assert_projection(
            &fixture.binary_predicate(PredicateOperator::GreaterThan, Datum::date_from_str(value)?),
            Some("name >= 0"),
        )?;

        fixture.assert_projection(
            &fixture.binary_predicate(
                PredicateOperator::GreaterThanOrEq,
                Datum::date_from_str(value)?,
            ),
            Some("name >= 0"),
        )?;

        fixture.assert_projection(
            &fixture.binary_predicate(PredicateOperator::Eq, Datum::date_from_str(value)?),
            Some("name = 0"),
        )?;

        fixture.assert_projection(
            &fixture.binary_predicate(PredicateOperator::NotEq, Datum::date_from_str(value)?),
            None,
        )?;

        fixture.assert_projection(
            &fixture.set_predicate(PredicateOperator::In, vec![
                Datum::date_from_str(value)?,
                Datum::date_from_str(another)?,
            ]),
            Some("name IN (0, -1)"),
        )?;

        fixture.assert_projection(
            &fixture.set_predicate(PredicateOperator::NotIn, vec![
                Datum::date_from_str(value)?,
                Datum::date_from_str(another)?,
            ]),
            None,
        )?;

        Ok(())
    }

    #[test]
    fn test_projection_date_year_lower_bound() -> Result<()> {
        // 47 => 2017
        let value = "2017-01-01";
        // 46 => 2016
        let another = "2016-12-31";

        let fixture = TestProjectionFixture::new(
            Transform::Year,
            "name",
            NestedField::required(1, "value", Type::Primitive(PrimitiveType::Date)),
        );

        fixture.assert_projection(
            &fixture.binary_predicate(PredicateOperator::LessThan, Datum::date_from_str(value)?),
            Some("name <= 46"),
        )?;

        fixture.assert_projection(
            &fixture.binary_predicate(
                PredicateOperator::LessThanOrEq,
                Datum::date_from_str(value)?,
            ),
            Some("name <= 47"),
        )?;

        fixture.assert_projection(
            &fixture.binary_predicate(PredicateOperator::GreaterThan, Datum::date_from_str(value)?),
            Some("name >= 47"),
        )?;

        fixture.assert_projection(
            &fixture.binary_predicate(
                PredicateOperator::GreaterThanOrEq,
                Datum::date_from_str(value)?,
            ),
            Some("name >= 47"),
        )?;

        fixture.assert_projection(
            &fixture.binary_predicate(PredicateOperator::Eq, Datum::date_from_str(value)?),
            Some("name = 47"),
        )?;

        fixture.assert_projection(
            &fixture.binary_predicate(PredicateOperator::NotEq, Datum::date_from_str(value)?),
            None,
        )?;

        fixture.assert_projection(
            &fixture.set_predicate(PredicateOperator::In, vec![
                Datum::date_from_str(value)?,
                Datum::date_from_str(another)?,
            ]),
            Some("name IN (47, 46)"),
        )?;

        fixture.assert_projection(
            &fixture.set_predicate(PredicateOperator::NotIn, vec![
                Datum::date_from_str(value)?,
                Datum::date_from_str(another)?,
            ]),
            None,
        )?;

        Ok(())
    }

    #[test]
    fn test_transform_years() {
        let year = super::Year;

        // Test Date32
        let ori_date = vec![
            NaiveDate::from_ymd_opt(1970, 1, 1).unwrap(),
            NaiveDate::from_ymd_opt(2000, 1, 1).unwrap(),
            NaiveDate::from_ymd_opt(2030, 1, 1).unwrap(),
            NaiveDate::from_ymd_opt(2060, 1, 1).unwrap(),
            NaiveDate::from_ymd_opt(1969, 1, 1).unwrap(),
        ];
        let date_array: ArrayRef = Arc::new(Date32Array::from(
            ori_date
                .into_iter()
                .map(|date| {
                    date.signed_duration_since(NaiveDate::from_ymd_opt(1970, 1, 1).unwrap())
                        .num_days() as i32
                })
                .collect::<Vec<i32>>(),
        ));
        let res = year.transform(date_array).unwrap();
        let res = res.as_any().downcast_ref::<Int32Array>().unwrap();
        assert_eq!(res.len(), 5);
        assert_eq!(res.value(0), 0);
        assert_eq!(res.value(1), 30);
        assert_eq!(res.value(2), 60);
        assert_eq!(res.value(3), 90);
        assert_eq!(res.value(4), -1);

        // Test TimestampMicrosecond
        let ori_timestamp = vec![
            NaiveDateTime::parse_from_str("1970-01-01 12:30:42.123", "%Y-%m-%d %H:%M:%S.%f")
                .unwrap(),
            NaiveDateTime::parse_from_str("2000-01-01 19:30:42.123", "%Y-%m-%d %H:%M:%S.%f")
                .unwrap(),
            NaiveDateTime::parse_from_str("2030-01-01 10:30:42.123", "%Y-%m-%d %H:%M:%S.%f")
                .unwrap(),
            NaiveDateTime::parse_from_str("2060-01-01 11:30:42.123", "%Y-%m-%d %H:%M:%S.%f")
                .unwrap(),
            NaiveDateTime::parse_from_str("1969-01-01 00:00:00.00", "%Y-%m-%d %H:%M:%S.%f")
                .unwrap(),
        ];
        let date_array: ArrayRef = Arc::new(TimestampMicrosecondArray::from(
            ori_timestamp
                .into_iter()
                .map(|timestamp| {
                    timestamp
                        .signed_duration_since(
                            NaiveDateTime::parse_from_str(
                                "1970-01-01 00:00:00.0",
                                "%Y-%m-%d %H:%M:%S.%f",
                            )
                            .unwrap(),
                        )
                        .num_microseconds()
                        .unwrap()
                })
                .collect::<Vec<i64>>(),
        ));
        let res = year.transform(date_array).unwrap();
        let res = res.as_any().downcast_ref::<Int32Array>().unwrap();
        assert_eq!(res.len(), 5);
        assert_eq!(res.value(0), 0);
        assert_eq!(res.value(1), 30);
        assert_eq!(res.value(2), 60);
        assert_eq!(res.value(3), 90);
        assert_eq!(res.value(4), -1);
    }

    fn test_timestamp_and_tz_transform(
        time: &str,
        transform: &BoxedTransformFunction,
        expect: Datum,
    ) {
        let timestamp = Datum::timestamp_from_str(time).unwrap();
        let timestamp_tz = Datum::timestamptz_from_str(time.to_owned() + " +00:00").unwrap();
        let res = transform.transform_literal(&timestamp).unwrap().unwrap();
        assert_eq!(res, expect);
        let res = transform.transform_literal(&timestamp_tz).unwrap().unwrap();
        assert_eq!(res, expect);
    }

    fn test_timestamp_and_tz_transform_using_i64(
        time: i64,
        transform: &BoxedTransformFunction,
        expect: Datum,
    ) {
        let timestamp = Datum::timestamp_micros(time);
        let timestamp_tz = Datum::timestamptz_micros(time);
        let res = transform.transform_literal(&timestamp).unwrap().unwrap();
        assert_eq!(res, expect);
        let res = transform.transform_literal(&timestamp_tz).unwrap().unwrap();
        assert_eq!(res, expect);
    }

    fn test_date(date: i32, transform: &BoxedTransformFunction, expect: Datum) {
        let date = Datum::date(date);
        let res = transform.transform_literal(&date).unwrap().unwrap();
        assert_eq!(res, expect);
    }

    fn test_timestamp_ns_and_tz_transform(
        time: &str,
        transform: &BoxedTransformFunction,
        expect: Datum,
    ) {
        let timestamp_ns = Datum::timestamp_from_str(time).unwrap();
        let timestamptz_ns = Datum::timestamptz_from_str(time.to_owned() + " +00:00").unwrap();
        let res = transform.transform_literal(&timestamp_ns).unwrap().unwrap();
        assert_eq!(res, expect);
        let res = transform
            .transform_literal(&timestamptz_ns)
            .unwrap()
            .unwrap();
        assert_eq!(res, expect);
    }

    fn test_timestamp_ns_and_tz_transform_using_i64(
        time: i64,
        transform: &BoxedTransformFunction,
        expect: Datum,
    ) {
        let timestamp_ns = Datum::timestamp_nanos(time);
        let timestamptz_ns = Datum::timestamptz_nanos(time);
        let res = transform.transform_literal(&timestamp_ns).unwrap().unwrap();
        assert_eq!(res, expect);
        let res = transform
            .transform_literal(&timestamptz_ns)
            .unwrap()
            .unwrap();
        assert_eq!(res, expect);
    }

    #[test]
    fn test_transform_year_literal() {
        let year = Box::new(super::Year) as BoxedTransformFunction;

        // Test Date32
        test_date(18628, &year, Datum::int(2021 - super::UNIX_EPOCH_YEAR));
        test_date(-365, &year, Datum::int(-1));

        // Test TimestampMicrosecond
        test_timestamp_and_tz_transform_using_i64(
            186280000000,
            &year,
            Datum::int(1970 - super::UNIX_EPOCH_YEAR),
        );
        test_timestamp_and_tz_transform("1969-01-01T00:00:00.000000", &year, Datum::int(-1));

        // Test TimestampNanosecond
        test_timestamp_ns_and_tz_transform_using_i64(
            186280000000,
            &year,
            Datum::int(1970 - super::UNIX_EPOCH_YEAR),
        );
        test_timestamp_ns_and_tz_transform("1969-01-01T00:00:00.000000", &year, Datum::int(-1));
    }

    #[test]
    fn test_transform_months() {
        let month = super::Month;

        // Test Date32
        let ori_date = vec![
            NaiveDate::from_ymd_opt(1970, 1, 1).unwrap(),
            NaiveDate::from_ymd_opt(2000, 4, 1).unwrap(),
            NaiveDate::from_ymd_opt(2030, 7, 1).unwrap(),
            NaiveDate::from_ymd_opt(2060, 10, 1).unwrap(),
            NaiveDate::from_ymd_opt(1969, 12, 1).unwrap(),
        ];
        let date_array: ArrayRef = Arc::new(Date32Array::from(
            ori_date
                .into_iter()
                .map(|date| {
                    date.signed_duration_since(NaiveDate::from_ymd_opt(1970, 1, 1).unwrap())
                        .num_days() as i32
                })
                .collect::<Vec<i32>>(),
        ));
        let res = month.transform(date_array).unwrap();
        let res = res.as_any().downcast_ref::<Int32Array>().unwrap();
        assert_eq!(res.len(), 5);
        assert_eq!(res.value(0), 0);
        assert_eq!(res.value(1), 30 * 12 + 3);
        assert_eq!(res.value(2), 60 * 12 + 6);
        assert_eq!(res.value(3), 90 * 12 + 9);
        assert_eq!(res.value(4), -1);

        // Test TimestampMicrosecond
        let ori_timestamp = vec![
            NaiveDateTime::parse_from_str("1970-01-01 12:30:42.123", "%Y-%m-%d %H:%M:%S.%f")
                .unwrap(),
            NaiveDateTime::parse_from_str("2000-04-01 19:30:42.123", "%Y-%m-%d %H:%M:%S.%f")
                .unwrap(),
            NaiveDateTime::parse_from_str("2030-07-01 10:30:42.123", "%Y-%m-%d %H:%M:%S.%f")
                .unwrap(),
            NaiveDateTime::parse_from_str("2060-10-01 11:30:42.123", "%Y-%m-%d %H:%M:%S.%f")
                .unwrap(),
            NaiveDateTime::parse_from_str("1969-12-01 00:00:00.00", "%Y-%m-%d %H:%M:%S.%f")
                .unwrap(),
        ];
        let date_array: ArrayRef = Arc::new(TimestampMicrosecondArray::from(
            ori_timestamp
                .into_iter()
                .map(|timestamp| {
                    timestamp
                        .signed_duration_since(
                            NaiveDateTime::parse_from_str(
                                "1970-01-01 00:00:00.0",
                                "%Y-%m-%d %H:%M:%S.%f",
                            )
                            .unwrap(),
                        )
                        .num_microseconds()
                        .unwrap()
                })
                .collect::<Vec<i64>>(),
        ));
        let res = month.transform(date_array).unwrap();
        let res = res.as_any().downcast_ref::<Int32Array>().unwrap();
        assert_eq!(res.len(), 5);
        assert_eq!(res.value(0), 0);
        assert_eq!(res.value(1), 30 * 12 + 3);
        assert_eq!(res.value(2), 60 * 12 + 6);
        assert_eq!(res.value(3), 90 * 12 + 9);
        assert_eq!(res.value(4), -1);
    }

    #[test]
    fn test_transform_month_literal() {
        let month = Box::new(super::Month) as BoxedTransformFunction;

        // Test Date32
        test_date(
            18628,
            &month,
            Datum::int((2021 - super::UNIX_EPOCH_YEAR) * 12),
        );
        test_date(-31, &month, Datum::int(-1));

        // Test TimestampMicrosecond
        test_timestamp_and_tz_transform_using_i64(
            186280000000,
            &month,
            Datum::int((1970 - super::UNIX_EPOCH_YEAR) * 12),
        );
        test_timestamp_and_tz_transform("1969-12-01T23:00:00.000000", &month, Datum::int(-1));
        test_timestamp_and_tz_transform("2017-12-01T00:00:00.000000", &month, Datum::int(575));
        test_timestamp_and_tz_transform("1970-01-01T00:00:00.000000", &month, Datum::int(0));
        test_timestamp_and_tz_transform("1969-12-31T00:00:00.000000", &month, Datum::int(-1));

        // Test TimestampNanosecond
        test_timestamp_ns_and_tz_transform_using_i64(
            186280000000,
            &month,
            Datum::int((1970 - super::UNIX_EPOCH_YEAR) * 12),
        );
        test_timestamp_ns_and_tz_transform("1969-12-01T23:00:00.000000", &month, Datum::int(-1));
        test_timestamp_ns_and_tz_transform("2017-12-01T00:00:00.000000", &month, Datum::int(575));
        test_timestamp_ns_and_tz_transform("1970-01-01T00:00:00.000000", &month, Datum::int(0));
        test_timestamp_ns_and_tz_transform("1969-12-31T00:00:00.000000", &month, Datum::int(-1));
    }

    #[test]
    fn test_transform_days() {
        let day = super::Day;
        let ori_date = vec![
            NaiveDate::from_ymd_opt(1970, 1, 1).unwrap(),
            NaiveDate::from_ymd_opt(2000, 4, 1).unwrap(),
            NaiveDate::from_ymd_opt(2030, 7, 1).unwrap(),
            NaiveDate::from_ymd_opt(2060, 10, 1).unwrap(),
            NaiveDate::from_ymd_opt(1969, 12, 31).unwrap(),
        ];
        let expect_day = ori_date
            .clone()
            .into_iter()
            .map(|data| {
                data.signed_duration_since(NaiveDate::from_ymd_opt(1970, 1, 1).unwrap())
                    .num_days() as i32
            })
            .collect::<Vec<i32>>();

        // Test Date32
        let date_array: ArrayRef = Arc::new(Date32Array::from(
            ori_date
                .into_iter()
                .map(|date| {
                    date.signed_duration_since(NaiveDate::from_ymd_opt(1970, 1, 1).unwrap())
                        .num_days() as i32
                })
                .collect::<Vec<i32>>(),
        ));
        let res = day.transform(date_array).unwrap();
        let res = res.as_any().downcast_ref::<Date32Array>().unwrap();
        assert_eq!(res.len(), 5);
        assert_eq!(res.value(0), expect_day[0]);
        assert_eq!(res.value(1), expect_day[1]);
        assert_eq!(res.value(2), expect_day[2]);
        assert_eq!(res.value(3), expect_day[3]);
        assert_eq!(res.value(4), -1);

        // Test TimestampMicrosecond
        let ori_timestamp = vec![
            NaiveDateTime::parse_from_str("1970-01-01 12:30:42.123", "%Y-%m-%d %H:%M:%S.%f")
                .unwrap(),
            NaiveDateTime::parse_from_str("2000-04-01 19:30:42.123", "%Y-%m-%d %H:%M:%S.%f")
                .unwrap(),
            NaiveDateTime::parse_from_str("2030-07-01 10:30:42.123", "%Y-%m-%d %H:%M:%S.%f")
                .unwrap(),
            NaiveDateTime::parse_from_str("2060-10-01 11:30:42.123", "%Y-%m-%d %H:%M:%S.%f")
                .unwrap(),
            NaiveDateTime::parse_from_str("1969-12-31 00:00:00.00", "%Y-%m-%d %H:%M:%S.%f")
                .unwrap(),
        ];
        let date_array: ArrayRef = Arc::new(TimestampMicrosecondArray::from(
            ori_timestamp
                .into_iter()
                .map(|timestamp| {
                    timestamp
                        .signed_duration_since(
                            NaiveDateTime::parse_from_str(
                                "1970-01-01 00:00:00.0",
                                "%Y-%m-%d %H:%M:%S.%f",
                            )
                            .unwrap(),
                        )
                        .num_microseconds()
                        .unwrap()
                })
                .collect::<Vec<i64>>(),
        ));
        let res = day.transform(date_array).unwrap();
        let res = res.as_any().downcast_ref::<Date32Array>().unwrap();
        assert_eq!(res.len(), 5);
        assert_eq!(res.value(0), expect_day[0]);
        assert_eq!(res.value(1), expect_day[1]);
        assert_eq!(res.value(2), expect_day[2]);
        assert_eq!(res.value(3), expect_day[3]);
        assert_eq!(res.value(4), -1);
    }

    #[test]
    fn test_transform_days_literal() {
        let day = Box::new(super::Day) as BoxedTransformFunction;
        // Test Date32
        test_date(18628, &day, Datum::date(18628));
        test_date(-31, &day, Datum::date(-31));

        // Test TimestampMicrosecond
        test_timestamp_and_tz_transform_using_i64(1512151975038194, &day, Datum::date(17501));
        test_timestamp_and_tz_transform_using_i64(-115200000000, &day, Datum::date(-2));
        test_timestamp_and_tz_transform("2017-12-01T10:30:42.123000", &day, Datum::date(17501));

        // Test TimestampNanosecond
        test_timestamp_ns_and_tz_transform_using_i64(1512151975038194, &day, Datum::date(17));
        test_timestamp_ns_and_tz_transform_using_i64(-115200000000, &day, Datum::date(-1));
        test_timestamp_ns_and_tz_transform("2017-12-01T10:30:42.123000", &day, Datum::date(17501));
    }

    #[test]
    fn test_transform_hours() {
        let hour = super::Hour;
        let ori_timestamp = vec![
            NaiveDateTime::parse_from_str("1970-01-01 19:01:23.123", "%Y-%m-%d %H:%M:%S.%f")
                .unwrap(),
            NaiveDateTime::parse_from_str("2000-03-01 12:01:23.123", "%Y-%m-%d %H:%M:%S.%f")
                .unwrap(),
            NaiveDateTime::parse_from_str("2030-10-02 10:01:23.123", "%Y-%m-%d %H:%M:%S.%f")
                .unwrap(),
            NaiveDateTime::parse_from_str("2060-09-01 05:03:23.123", "%Y-%m-%d %H:%M:%S.%f")
                .unwrap(),
            NaiveDateTime::parse_from_str("1969-12-31 23:00:00.00", "%Y-%m-%d %H:%M:%S.%f")
                .unwrap(),
        ];
        let expect_hour = ori_timestamp
            .clone()
            .into_iter()
            .map(|timestamp| {
                timestamp
                    .signed_duration_since(
                        NaiveDateTime::parse_from_str(
                            "1970-01-01 00:00:0.0",
                            "%Y-%m-%d %H:%M:%S.%f",
                        )
                        .unwrap(),
                    )
                    .num_hours() as i32
            })
            .collect::<Vec<i32>>();

        // Test TimestampMicrosecond
        let date_array: ArrayRef = Arc::new(TimestampMicrosecondArray::from(
            ori_timestamp
                .into_iter()
                .map(|timestamp| {
                    timestamp
                        .signed_duration_since(
                            NaiveDateTime::parse_from_str(
                                "1970-01-01 00:00:0.0",
                                "%Y-%m-%d %H:%M:%S.%f",
                            )
                            .unwrap(),
                        )
                        .num_microseconds()
                        .unwrap()
                })
                .collect::<Vec<i64>>(),
        ));
        let res = hour.transform(date_array).unwrap();
        let res = res.as_any().downcast_ref::<Int32Array>().unwrap();
        assert_eq!(res.len(), 5);
        assert_eq!(res.value(0), expect_hour[0]);
        assert_eq!(res.value(1), expect_hour[1]);
        assert_eq!(res.value(2), expect_hour[2]);
        assert_eq!(res.value(3), expect_hour[3]);
        assert_eq!(res.value(4), -1);
    }

    #[test]
    fn test_transform_hours_literal() {
        let hour = Box::new(super::Hour) as BoxedTransformFunction;

        test_timestamp_and_tz_transform("2017-12-01T18:00:00.000000", &hour, Datum::int(420042));
        test_timestamp_and_tz_transform("1970-01-01T22:01:01.000000", &hour, Datum::int(22));
        test_timestamp_and_tz_transform("1969-12-31T23:00:00.000000", &hour, Datum::int(-1));
        test_timestamp_and_tz_transform("1969-12-31T22:01:01.000000", &hour, Datum::int(-2));
        test_timestamp_and_tz_transform("0022-05-01T22:01:01.000000", &hour, Datum::int(-17072906));

        // Test TimestampNanosecond
        test_timestamp_ns_and_tz_transform(
            "2017-12-01T18:00:00.0000000000",
            &hour,
            Datum::int(420042),
        );
        test_timestamp_ns_and_tz_transform("1969-12-31T23:00:00.0000000000", &hour, Datum::int(-1));
        test_timestamp_ns_and_tz_transform(
            "1900-05-01T22:01:01.0000000000",
            &hour,
            Datum::int(-610706),
        );
    }
}
