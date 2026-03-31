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

use arrow_array::ArrayRef;
use arrow_schema::{DataType, TimeUnit};

use super::TransformFunction;
use crate::spec::{Datum, PrimitiveLiteral, PrimitiveType};

#[derive(Debug)]
pub struct Bucket {
    mod_n: u32,
}

impl Bucket {
    pub fn new(mod_n: u32) -> Self {
        Self { mod_n }
    }
}

impl Bucket {
    /// When switch the hash function, we only need to change this function.
    #[inline]
    fn hash_bytes(mut v: &[u8]) -> i32 {
        murmur3::murmur3_32(&mut v, 0).unwrap() as i32
    }

    #[inline]
    fn hash_int(v: i32) -> i32 {
        Self::hash_long(v as i64)
    }

    #[inline]
    fn hash_long(v: i64) -> i32 {
        Self::hash_bytes(v.to_le_bytes().as_slice())
    }

    /// v is days from unix epoch
    #[inline]
    fn hash_date(v: i32) -> i32 {
        Self::hash_int(v)
    }

    /// v is microseconds from midnight
    #[inline]
    fn hash_time(v: i64) -> i32 {
        Self::hash_long(v)
    }

    /// v is microseconds from unix epoch
    #[inline]
    fn hash_timestamp(v: i64) -> i32 {
        Self::hash_long(v)
    }

    #[inline]
    fn hash_str(s: &str) -> i32 {
        Self::hash_bytes(s.as_bytes())
    }

    /// Decimal values are hashed using the minimum number of bytes required to hold the unscaled value as a two’s complement big-endian
    /// ref: https://iceberg.apache.org/spec/#appendix-b-32-bit-hash-requirements
    #[inline]
    fn hash_decimal(v: i128) -> i32 {
        let bytes = v.to_be_bytes();
        if let Some(start) = bytes.iter().position(|&x| x != 0) {
            Self::hash_bytes(&bytes[start..])
        } else {
            Self::hash_bytes(&[0])
        }
    }

    /// def bucket_N(x) = (murmur3_x86_32_hash(x) & Integer.MAX_VALUE) % N
    /// ref: https://iceberg.apache.org/spec/#partitioning
    #[inline]
    fn bucket_n(&self, v: i32) -> i32 {
        (v & i32::MAX) % (self.mod_n as i32)
    }

    #[inline]
    fn bucket_int(&self, v: i32) -> i32 {
        self.bucket_n(Self::hash_int(v))
    }

    #[inline]
    fn bucket_long(&self, v: i64) -> i32 {
        self.bucket_n(Self::hash_long(v))
    }

    #[inline]
    fn bucket_decimal(&self, v: i128) -> i32 {
        self.bucket_n(Self::hash_decimal(v))
    }

    #[inline]
    fn bucket_date(&self, v: i32) -> i32 {
        self.bucket_n(Self::hash_date(v))
    }

    #[inline]
    fn bucket_time(&self, v: i64) -> i32 {
        self.bucket_n(Self::hash_time(v))
    }

    #[inline]
    fn bucket_timestamp(&self, v: i64) -> i32 {
        self.bucket_n(Self::hash_timestamp(v))
    }

    #[inline]
    fn bucket_str(&self, v: &str) -> i32 {
        self.bucket_n(Self::hash_str(v))
    }

    #[inline]
    fn bucket_bytes(&self, v: &[u8]) -> i32 {
        self.bucket_n(Self::hash_bytes(v))
    }
}

impl TransformFunction for Bucket {
    fn transform(&self, input: ArrayRef) -> crate::Result<ArrayRef> {
        let res: arrow_array::Int32Array = match input.data_type() {
            DataType::Int32 => input
                .as_any()
                .downcast_ref::<arrow_array::Int32Array>()
                .unwrap()
                .unary(|v| self.bucket_int(v)),
            DataType::Int64 => input
                .as_any()
                .downcast_ref::<arrow_array::Int64Array>()
                .unwrap()
                .unary(|v| self.bucket_long(v)),
            DataType::Decimal128(_, _) => input
                .as_any()
                .downcast_ref::<arrow_array::Decimal128Array>()
                .unwrap()
                .unary(|v| self.bucket_decimal(v)),
            DataType::Date32 => input
                .as_any()
                .downcast_ref::<arrow_array::Date32Array>()
                .unwrap()
                .unary(|v| self.bucket_date(v)),
            DataType::Time64(TimeUnit::Microsecond) => input
                .as_any()
                .downcast_ref::<arrow_array::Time64MicrosecondArray>()
                .unwrap()
                .unary(|v| self.bucket_time(v)),
            DataType::Timestamp(TimeUnit::Microsecond, _) => input
                .as_any()
                .downcast_ref::<arrow_array::TimestampMicrosecondArray>()
                .unwrap()
                .unary(|v| self.bucket_timestamp(v)),
            DataType::Time64(TimeUnit::Nanosecond) => input
                .as_any()
                .downcast_ref::<arrow_array::Time64NanosecondArray>()
                .unwrap()
                .unary(|v| self.bucket_time(v / 1000)),
            DataType::Timestamp(TimeUnit::Nanosecond, _) => input
                .as_any()
                .downcast_ref::<arrow_array::TimestampNanosecondArray>()
                .unwrap()
                .unary(|v| self.bucket_timestamp(v / 1000)),
            DataType::Utf8 => arrow_array::Int32Array::from_iter(
                input
                    .as_any()
                    .downcast_ref::<arrow_array::StringArray>()
                    .unwrap()
                    .iter()
                    .map(|v| v.map(|v| self.bucket_str(v))),
            ),
            DataType::LargeUtf8 => arrow_array::Int32Array::from_iter(
                input
                    .as_any()
                    .downcast_ref::<arrow_array::LargeStringArray>()
                    .unwrap()
                    .iter()
                    .map(|v| v.map(|v| self.bucket_str(v))),
            ),
            DataType::Binary => arrow_array::Int32Array::from_iter(
                input
                    .as_any()
                    .downcast_ref::<arrow_array::BinaryArray>()
                    .unwrap()
                    .iter()
                    .map(|v| v.map(|v| self.bucket_bytes(v))),
            ),
            DataType::LargeBinary => arrow_array::Int32Array::from_iter(
                input
                    .as_any()
                    .downcast_ref::<arrow_array::LargeBinaryArray>()
                    .unwrap()
                    .iter()
                    .map(|v| v.map(|v| self.bucket_bytes(v))),
            ),
            DataType::FixedSizeBinary(_) => arrow_array::Int32Array::from_iter(
                input
                    .as_any()
                    .downcast_ref::<arrow_array::FixedSizeBinaryArray>()
                    .unwrap()
                    .iter()
                    .map(|v| v.map(|v| self.bucket_bytes(v))),
            ),
            _ => {
                return Err(crate::Error::new(
                    crate::ErrorKind::FeatureUnsupported,
                    format!(
                        "Unsupported data type for bucket transform: {:?}",
                        input.data_type()
                    ),
                ));
            }
        };
        Ok(Arc::new(res))
    }

    fn transform_literal(&self, input: &Datum) -> crate::Result<Option<Datum>> {
        let val = match (input.data_type(), input.literal()) {
            (PrimitiveType::Int, PrimitiveLiteral::Int(v)) => self.bucket_int(*v),
            (PrimitiveType::Long, PrimitiveLiteral::Long(v)) => self.bucket_long(*v),
            (PrimitiveType::Decimal { .. }, PrimitiveLiteral::Int128(v)) => self.bucket_decimal(*v),
            (PrimitiveType::Date, PrimitiveLiteral::Int(v)) => self.bucket_date(*v),
            (PrimitiveType::Time, PrimitiveLiteral::Long(v)) => self.bucket_time(*v),
            (PrimitiveType::Timestamp, PrimitiveLiteral::Long(v)) => self.bucket_timestamp(*v),
            (PrimitiveType::Timestamptz, PrimitiveLiteral::Long(v)) => self.bucket_timestamp(*v),
            (PrimitiveType::TimestampNs, PrimitiveLiteral::Long(v)) => {
                self.bucket_timestamp(*v / 1000)
            }
            (PrimitiveType::TimestamptzNs, PrimitiveLiteral::Long(v)) => {
                self.bucket_timestamp(*v / 1000)
            }
            (PrimitiveType::String, PrimitiveLiteral::String(v)) => self.bucket_str(v.as_str()),
            (PrimitiveType::Uuid, PrimitiveLiteral::UInt128(v)) => {
                self.bucket_bytes(uuid::Uuid::from_u128(*v).as_ref())
            }
            (PrimitiveType::Binary, PrimitiveLiteral::Binary(v)) => self.bucket_bytes(v.as_ref()),
            (PrimitiveType::Fixed(_), PrimitiveLiteral::Binary(v)) => self.bucket_bytes(v.as_ref()),
            _ => {
                return Err(crate::Error::new(
                    crate::ErrorKind::FeatureUnsupported,
                    format!(
                        "Unsupported data type for bucket transform: {:?}",
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

    use arrow_array::{ArrayRef, Int32Array, TimestampMicrosecondArray, TimestampNanosecondArray};
    use chrono::{DateTime, NaiveDate, NaiveDateTime, NaiveTime};

    use super::Bucket;
    use crate::Result;
    use crate::expr::PredicateOperator;
    use crate::spec::PrimitiveType::{
        Binary, Date, Decimal, Fixed, Int, Long, String as StringType, Time, Timestamp,
        TimestampNs, Timestamptz, TimestamptzNs, Uuid,
    };
    use crate::spec::Type::{Primitive, Struct};
    use crate::spec::{Datum, NestedField, PrimitiveType, StructType, Transform, Type};
    use crate::transform::TransformFunction;
    use crate::transform::test::{TestProjectionFixture, TestTransformFixture};

    #[test]
    fn test_bucket_transform() {
        let trans = Transform::Bucket(8);

        let fixture = TestTransformFixture {
            display: "bucket[8]".to_string(),
            json: r#""bucket[8]""#.to_string(),
            dedup_name: "bucket[8]".to_string(),
            preserves_order: false,
            satisfies_order_of: vec![
                (Transform::Bucket(8), true),
                (Transform::Bucket(4), false),
                (Transform::Void, false),
                (Transform::Day, false),
            ],
            trans_types: vec![
                (Primitive(Binary), Some(Primitive(Int))),
                (Primitive(Date), Some(Primitive(Int))),
                (
                    Primitive(Decimal {
                        precision: 8,
                        scale: 5,
                    }),
                    Some(Primitive(Int)),
                ),
                (Primitive(Fixed(8)), Some(Primitive(Int))),
                (Primitive(Int), Some(Primitive(Int))),
                (Primitive(Long), Some(Primitive(Int))),
                (Primitive(StringType), Some(Primitive(Int))),
                (Primitive(Uuid), Some(Primitive(Int))),
                (Primitive(Time), Some(Primitive(Int))),
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
    fn test_projection_bucket_uuid() -> Result<()> {
        let value = uuid::Uuid::from_u64_pair(123, 456);
        let another = uuid::Uuid::from_u64_pair(456, 123);

        let fixture = TestProjectionFixture::new(
            Transform::Bucket(10),
            "name",
            NestedField::required(1, "value", Type::Primitive(PrimitiveType::Uuid)),
        );

        fixture.assert_projection(
            &fixture.binary_predicate(PredicateOperator::Eq, Datum::uuid(value)),
            Some("name = 4"),
        )?;

        fixture.assert_projection(
            &fixture.binary_predicate(PredicateOperator::NotEq, Datum::uuid(value)),
            None,
        )?;

        fixture.assert_projection(
            &fixture.binary_predicate(PredicateOperator::LessThan, Datum::uuid(value)),
            None,
        )?;

        fixture.assert_projection(
            &fixture.binary_predicate(PredicateOperator::LessThanOrEq, Datum::uuid(value)),
            None,
        )?;

        fixture.assert_projection(
            &fixture.binary_predicate(PredicateOperator::GreaterThan, Datum::uuid(value)),
            None,
        )?;

        fixture.assert_projection(
            &fixture.binary_predicate(PredicateOperator::GreaterThanOrEq, Datum::uuid(value)),
            None,
        )?;

        fixture.assert_projection(
            &fixture.set_predicate(PredicateOperator::In, vec![
                Datum::uuid(value),
                Datum::uuid(another),
            ]),
            Some("name IN (4, 6)"),
        )?;

        fixture.assert_projection(
            &fixture.set_predicate(PredicateOperator::NotIn, vec![
                Datum::uuid(value),
                Datum::uuid(another),
            ]),
            None,
        )?;

        Ok(())
    }

    #[test]
    fn test_projection_bucket_fixed() -> Result<()> {
        let value = "abcdefg".as_bytes().to_vec();
        let another = "abcdehij".as_bytes().to_vec();

        let fixture = TestProjectionFixture::new(
            Transform::Bucket(10),
            "name",
            NestedField::required(
                1,
                "value",
                Type::Primitive(PrimitiveType::Fixed(value.len() as u64)),
            ),
        );

        fixture.assert_projection(
            &fixture.binary_predicate(PredicateOperator::Eq, Datum::fixed(value.clone())),
            Some("name = 4"),
        )?;

        fixture.assert_projection(
            &fixture.binary_predicate(PredicateOperator::NotEq, Datum::fixed(value.clone())),
            None,
        )?;

        fixture.assert_projection(
            &fixture.binary_predicate(PredicateOperator::LessThan, Datum::fixed(value.clone())),
            None,
        )?;

        fixture.assert_projection(
            &fixture.binary_predicate(PredicateOperator::LessThanOrEq, Datum::fixed(value.clone())),
            None,
        )?;

        fixture.assert_projection(
            &fixture.binary_predicate(PredicateOperator::GreaterThan, Datum::fixed(value.clone())),
            None,
        )?;

        fixture.assert_projection(
            &fixture.binary_predicate(
                PredicateOperator::GreaterThanOrEq,
                Datum::fixed(value.clone()),
            ),
            None,
        )?;

        fixture.assert_projection(
            &fixture.set_predicate(PredicateOperator::In, vec![
                Datum::fixed(value.clone()),
                Datum::fixed(another.clone()),
            ]),
            Some("name IN (4, 6)"),
        )?;

        fixture.assert_projection(
            &fixture.set_predicate(PredicateOperator::NotIn, vec![
                Datum::fixed(value.clone()),
                Datum::fixed(another.clone()),
            ]),
            None,
        )?;

        Ok(())
    }

    #[test]
    fn test_projection_bucket_string() -> Result<()> {
        let value = "abcdefg";
        let another = "abcdefgabc";

        let fixture = TestProjectionFixture::new(
            Transform::Bucket(10),
            "name",
            NestedField::required(1, "value", Type::Primitive(PrimitiveType::String)),
        );

        fixture.assert_projection(
            &fixture.binary_predicate(PredicateOperator::Eq, Datum::string(value)),
            Some("name = 4"),
        )?;

        fixture.assert_projection(
            &fixture.binary_predicate(PredicateOperator::NotEq, Datum::string(value)),
            None,
        )?;

        fixture.assert_projection(
            &fixture.binary_predicate(PredicateOperator::LessThan, Datum::string(value)),
            None,
        )?;

        fixture.assert_projection(
            &fixture.binary_predicate(PredicateOperator::LessThanOrEq, Datum::string(value)),
            None,
        )?;

        fixture.assert_projection(
            &fixture.binary_predicate(PredicateOperator::GreaterThan, Datum::string(value)),
            None,
        )?;

        fixture.assert_projection(
            &fixture.binary_predicate(PredicateOperator::GreaterThanOrEq, Datum::string(value)),
            None,
        )?;

        fixture.assert_projection(
            &fixture.set_predicate(PredicateOperator::In, vec![
                Datum::string(value),
                Datum::string(another),
            ]),
            Some("name IN (9, 4)"),
        )?;

        fixture.assert_projection(
            &fixture.set_predicate(PredicateOperator::NotIn, vec![
                Datum::string(value),
                Datum::string(another),
            ]),
            None,
        )?;

        Ok(())
    }

    #[test]
    fn test_projection_bucket_decimal() -> Result<()> {
        let prev = "99.00";
        let curr = "100.00";
        let next = "101.00";

        let fixture = TestProjectionFixture::new(
            Transform::Bucket(10),
            "name",
            NestedField::required(
                1,
                "value",
                Type::Primitive(PrimitiveType::Decimal {
                    precision: 9,
                    scale: 2,
                }),
            ),
        );

        fixture.assert_projection(
            &fixture.binary_predicate(PredicateOperator::Eq, Datum::decimal_from_str(curr)?),
            Some("name = 2"),
        )?;

        fixture.assert_projection(
            &fixture.binary_predicate(PredicateOperator::NotEq, Datum::decimal_from_str(curr)?),
            None,
        )?;

        fixture.assert_projection(
            &fixture.binary_predicate(PredicateOperator::LessThan, Datum::decimal_from_str(curr)?),
            None,
        )?;

        fixture.assert_projection(
            &fixture.binary_predicate(
                PredicateOperator::LessThanOrEq,
                Datum::decimal_from_str(curr)?,
            ),
            None,
        )?;

        fixture.assert_projection(
            &fixture.binary_predicate(
                PredicateOperator::GreaterThan,
                Datum::decimal_from_str(curr)?,
            ),
            None,
        )?;

        fixture.assert_projection(
            &fixture.binary_predicate(
                PredicateOperator::GreaterThanOrEq,
                Datum::decimal_from_str(curr)?,
            ),
            None,
        )?;

        fixture.assert_projection(
            &fixture.set_predicate(PredicateOperator::In, vec![
                Datum::decimal_from_str(next)?,
                Datum::decimal_from_str(curr)?,
                Datum::decimal_from_str(prev)?,
            ]),
            Some("name IN (2, 6)"),
        )?;

        fixture.assert_projection(
            &fixture.set_predicate(PredicateOperator::NotIn, vec![
                Datum::decimal_from_str(curr)?,
                Datum::decimal_from_str(next)?,
            ]),
            None,
        )?;

        Ok(())
    }

    #[test]
    fn test_projection_bucket_long() -> Result<()> {
        let value = 100;
        let fixture = TestProjectionFixture::new(
            Transform::Bucket(10),
            "name",
            NestedField::required(1, "value", Type::Primitive(PrimitiveType::Long)),
        );

        fixture.assert_projection(
            &fixture.binary_predicate(PredicateOperator::Eq, Datum::long(value)),
            Some("name = 6"),
        )?;

        fixture.assert_projection(
            &fixture.binary_predicate(PredicateOperator::NotEq, Datum::long(value)),
            None,
        )?;

        fixture.assert_projection(
            &fixture.binary_predicate(PredicateOperator::LessThan, Datum::long(value)),
            None,
        )?;

        fixture.assert_projection(
            &fixture.binary_predicate(PredicateOperator::LessThanOrEq, Datum::long(value)),
            None,
        )?;

        fixture.assert_projection(
            &fixture.binary_predicate(PredicateOperator::GreaterThan, Datum::long(value)),
            None,
        )?;

        fixture.assert_projection(
            &fixture.binary_predicate(PredicateOperator::GreaterThanOrEq, Datum::long(value)),
            None,
        )?;

        fixture.assert_projection(
            &fixture.set_predicate(PredicateOperator::In, vec![
                Datum::long(value - 1),
                Datum::long(value),
                Datum::long(value + 1),
            ]),
            Some("name IN (8, 7, 6)"),
        )?;

        fixture.assert_projection(
            &fixture.set_predicate(PredicateOperator::NotIn, vec![
                Datum::long(value),
                Datum::long(value + 1),
            ]),
            None,
        )?;

        Ok(())
    }

    #[test]
    fn test_projection_bucket_integer() -> Result<()> {
        let value = 100;

        let fixture = TestProjectionFixture::new(
            Transform::Bucket(10),
            "name",
            NestedField::required(1, "value", Type::Primitive(PrimitiveType::Int)),
        );

        fixture.assert_projection(
            &fixture.binary_predicate(PredicateOperator::Eq, Datum::int(value)),
            Some("name = 6"),
        )?;

        fixture.assert_projection(
            &fixture.binary_predicate(PredicateOperator::NotEq, Datum::int(value)),
            None,
        )?;

        fixture.assert_projection(
            &fixture.binary_predicate(PredicateOperator::LessThan, Datum::int(value)),
            None,
        )?;

        fixture.assert_projection(
            &fixture.binary_predicate(PredicateOperator::LessThanOrEq, Datum::int(value)),
            None,
        )?;

        fixture.assert_projection(
            &fixture.binary_predicate(PredicateOperator::GreaterThan, Datum::int(value)),
            None,
        )?;

        fixture.assert_projection(
            &fixture.binary_predicate(PredicateOperator::GreaterThanOrEq, Datum::int(value)),
            None,
        )?;

        fixture.assert_projection(
            &fixture.set_predicate(PredicateOperator::In, vec![
                Datum::int(value - 1),
                Datum::int(value),
                Datum::int(value + 1),
            ]),
            Some("name IN (8, 7, 6)"),
        )?;

        fixture.assert_projection(
            &fixture.set_predicate(PredicateOperator::NotIn, vec![
                Datum::int(value),
                Datum::int(value + 1),
            ]),
            None,
        )?;

        Ok(())
    }

    #[test]
    fn test_hash() {
        // test int
        assert_eq!(Bucket::hash_int(34), 2017239379);
        // test long
        assert_eq!(Bucket::hash_long(34), 2017239379);
        // test decimal
        assert_eq!(Bucket::hash_decimal(1420), -500754589);
        // test date
        let date = NaiveDate::from_ymd_opt(2017, 11, 16).unwrap();
        assert_eq!(
            Bucket::hash_date(
                date.signed_duration_since(NaiveDate::from_ymd_opt(1970, 1, 1).unwrap())
                    .num_days() as i32
            ),
            -653330422
        );
        // test time
        let time = NaiveTime::from_hms_opt(22, 31, 8).unwrap();
        assert_eq!(
            Bucket::hash_time(
                time.signed_duration_since(NaiveTime::from_hms_opt(0, 0, 0).unwrap())
                    .num_microseconds()
                    .unwrap()
            ),
            -662762989
        );
        // test timestamp
        let timestamp =
            NaiveDateTime::parse_from_str("2017-11-16 22:31:08", "%Y-%m-%d %H:%M:%S").unwrap();
        assert_eq!(
            Bucket::hash_timestamp(
                timestamp
                    .signed_duration_since(
                        NaiveDateTime::parse_from_str("1970-01-01 00:00:00", "%Y-%m-%d %H:%M:%S")
                            .unwrap()
                    )
                    .num_microseconds()
                    .unwrap()
            ),
            -2047944441
        );
        // test timestamp with tz
        let timestamp = DateTime::parse_from_rfc3339("2017-11-16T14:31:08-08:00").unwrap();
        assert_eq!(
            Bucket::hash_timestamp(
                timestamp
                    .signed_duration_since(
                        DateTime::parse_from_rfc3339("1970-01-01T00:00:00-00:00").unwrap()
                    )
                    .num_microseconds()
                    .unwrap()
            ),
            -2047944441
        );
        // test str
        assert_eq!(Bucket::hash_str("iceberg"), 1210000089);
        // test uuid
        assert_eq!(
            Bucket::hash_bytes(
                [
                    0xF7, 0x9C, 0x3E, 0x09, 0x67, 0x7C, 0x4B, 0xBD, 0xA4, 0x79, 0x3F, 0x34, 0x9C,
                    0xB7, 0x85, 0xE7
                ]
                .as_ref()
            ),
            1488055340
        );
        // test fixed and binary
        assert_eq!(
            Bucket::hash_bytes([0x00, 0x01, 0x02, 0x03].as_ref()),
            -188683207
        );
    }

    #[test]
    fn test_int_literal() {
        let bucket = Bucket::new(10);
        assert_eq!(
            bucket.transform_literal(&Datum::int(34)).unwrap().unwrap(),
            Datum::int(9)
        );
    }

    #[test]
    fn test_long_literal() {
        let bucket = Bucket::new(10);
        assert_eq!(
            bucket.transform_literal(&Datum::long(34)).unwrap().unwrap(),
            Datum::int(9)
        );
    }

    #[test]
    fn test_decimal_literal() {
        let bucket = Bucket::new(10);
        assert_eq!(
            bucket
                .transform_literal(&Datum::decimal(1420).unwrap())
                .unwrap()
                .unwrap(),
            Datum::int(9)
        );
    }

    #[test]
    fn test_date_literal() {
        let bucket = Bucket::new(100);
        assert_eq!(
            bucket
                .transform_literal(&Datum::date(17486))
                .unwrap()
                .unwrap(),
            Datum::int(26)
        );
    }

    #[test]
    fn test_time_literal() {
        let bucket = Bucket::new(100);
        assert_eq!(
            bucket
                .transform_literal(&Datum::time_micros(81068000000).unwrap())
                .unwrap()
                .unwrap(),
            Datum::int(59)
        );
    }

    #[test]
    fn test_timestamp_literal() {
        let bucket = Bucket::new(100);
        assert_eq!(
            bucket
                .transform_literal(&Datum::timestamp_micros(1510871468000000))
                .unwrap()
                .unwrap(),
            Datum::int(7)
        );
    }

    #[test]
    fn test_str_literal() {
        let bucket = Bucket::new(100);
        assert_eq!(
            bucket
                .transform_literal(&Datum::string("iceberg"))
                .unwrap()
                .unwrap(),
            Datum::int(89)
        );
    }

    #[test]
    fn test_uuid_literal() {
        let bucket = Bucket::new(100);
        assert_eq!(
            bucket
                .transform_literal(&Datum::uuid(
                    "F79C3E09-677C-4BBD-A479-3F349CB785E7".parse().unwrap()
                ))
                .unwrap()
                .unwrap(),
            Datum::int(40)
        );
    }

    #[test]
    fn test_binary_literal() {
        let bucket = Bucket::new(128);
        assert_eq!(
            bucket
                .transform_literal(&Datum::binary(b"\x00\x01\x02\x03".to_vec()))
                .unwrap()
                .unwrap(),
            Datum::int(57)
        );
    }

    #[test]
    fn test_fixed_literal() {
        let bucket = Bucket::new(128);
        assert_eq!(
            bucket
                .transform_literal(&Datum::fixed(b"foo".to_vec()))
                .unwrap()
                .unwrap(),
            Datum::int(32)
        );
    }

    #[test]
    fn test_timestamptz_literal() {
        let bucket = Bucket::new(100);
        assert_eq!(
            bucket
                .transform_literal(&Datum::timestamptz_micros(1510871468000000))
                .unwrap()
                .unwrap(),
            Datum::int(7)
        );
    }

    #[test]
    fn test_timestamp_ns_literal() {
        let bucket = Bucket::new(100);
        let ns_value = 1510871468000000i64 * 1000;
        assert_eq!(
            bucket
                .transform_literal(&Datum::timestamp_nanos(ns_value))
                .unwrap()
                .unwrap(),
            Datum::int(7)
        );
    }

    #[test]
    fn test_timestamptz_ns_literal() {
        let bucket = Bucket::new(100);
        let ns_value = 1510871468000000i64 * 1000;
        assert_eq!(
            bucket
                .transform_literal(&Datum::timestamptz_nanos(ns_value))
                .unwrap()
                .unwrap(),
            Datum::int(7)
        );
    }

    #[test]
    fn test_transform_timestamp_nanos_and_micros_array_equivalence() {
        let bucket = Bucket::new(100);
        let micros_value = 1510871468000000;
        let nanos_value = micros_value * 1000;

        let micro_array = TimestampMicrosecondArray::from_iter_values(vec![micros_value]);
        let nano_array = TimestampNanosecondArray::from_iter_values(vec![nanos_value]);

        let transformed_micro: ArrayRef = bucket.transform(Arc::new(micro_array)).unwrap();
        let transformed_nano: ArrayRef = bucket.transform(Arc::new(nano_array)).unwrap();

        let micro_result = transformed_micro
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        let nano_result = transformed_nano
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();

        assert_eq!(micro_result.value(0), nano_result.value(0));
    }
}
