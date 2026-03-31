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
use arrow_schema::DataType;

use super::TransformFunction;
use crate::Error;
use crate::spec::{Datum, PrimitiveLiteral};

#[derive(Debug)]
pub struct Truncate {
    width: u32,
}

impl Truncate {
    pub fn new(width: u32) -> Self {
        Self { width }
    }

    #[inline]
    fn truncate_str(s: &str, width: usize) -> &str {
        match s.char_indices().nth(width) {
            None => s,
            Some((idx, _)) => &s[..idx],
        }
    }

    #[inline]
    fn truncate_binary(s: &[u8], width: usize) -> &[u8] {
        if s.len() > width { &s[0..width] } else { s }
    }

    #[inline]
    fn truncate_i32(v: i32, width: i32) -> i32 {
        v - v.rem_euclid(width)
    }

    #[inline]
    fn truncate_i64(v: i64, width: i64) -> i64 {
        v - (((v % width) + width) % width)
    }

    #[inline]
    fn truncate_decimal_i128(v: i128, width: i128) -> i128 {
        v - (((v % width) + width) % width)
    }
}

impl TransformFunction for Truncate {
    fn transform(&self, input: ArrayRef) -> crate::Result<ArrayRef> {
        match input.data_type() {
            DataType::Int32 => {
                let width: i32 = self.width.try_into().map_err(|_| {
                    Error::new(
                        crate::ErrorKind::DataInvalid,
                        "width is failed to convert to i32 when truncate Int32Array",
                    )
                })?;
                let res: arrow_array::Int32Array = input
                    .as_any()
                    .downcast_ref::<arrow_array::Int32Array>()
                    .unwrap()
                    .unary(|v| Self::truncate_i32(v, width));
                Ok(Arc::new(res))
            }
            DataType::Int64 => {
                let width = self.width as i64;
                let res: arrow_array::Int64Array = input
                    .as_any()
                    .downcast_ref::<arrow_array::Int64Array>()
                    .unwrap()
                    .unary(|v| Self::truncate_i64(v, width));
                Ok(Arc::new(res))
            }
            DataType::Decimal128(precision, scale) => {
                let width = self.width as i128;
                let res: arrow_array::Decimal128Array = input
                    .as_any()
                    .downcast_ref::<arrow_array::Decimal128Array>()
                    .unwrap()
                    .unary(|v| Self::truncate_decimal_i128(v, width))
                    .with_precision_and_scale(*precision, *scale)
                    .map_err(|err| Error::new(crate::ErrorKind::Unexpected, format!("{err}")))?;
                Ok(Arc::new(res))
            }
            DataType::Utf8 => {
                let len = self.width as usize;
                let res: arrow_array::StringArray = arrow_array::StringArray::from_iter(
                    input
                        .as_any()
                        .downcast_ref::<arrow_array::StringArray>()
                        .unwrap()
                        .iter()
                        .map(|v| v.map(|v| Self::truncate_str(v, len))),
                );
                Ok(Arc::new(res))
            }
            DataType::LargeUtf8 => {
                let len = self.width as usize;
                let res: arrow_array::LargeStringArray = arrow_array::LargeStringArray::from_iter(
                    input
                        .as_any()
                        .downcast_ref::<arrow_array::LargeStringArray>()
                        .unwrap()
                        .iter()
                        .map(|v| v.map(|v| Self::truncate_str(v, len))),
                );
                Ok(Arc::new(res))
            }
            DataType::Binary => {
                let len = self.width as usize;
                let res: arrow_array::BinaryArray = arrow_array::BinaryArray::from_iter(
                    input
                        .as_any()
                        .downcast_ref::<arrow_array::BinaryArray>()
                        .unwrap()
                        .iter()
                        .map(|v| v.map(|v| Self::truncate_binary(v, len))),
                );
                Ok(Arc::new(res))
            }
            _ => Err(crate::Error::new(
                crate::ErrorKind::FeatureUnsupported,
                format!(
                    "Unsupported data type for truncate transform: {:?}",
                    input.data_type()
                ),
            )),
        }
    }

    fn transform_literal(&self, input: &Datum) -> crate::Result<Option<Datum>> {
        match input.literal() {
            PrimitiveLiteral::Int(v) => Ok(Some({
                let width: i32 = self.width.try_into().map_err(|_| {
                    Error::new(
                        crate::ErrorKind::DataInvalid,
                        "width is failed to convert to i32 when truncate Int32Array",
                    )
                })?;
                Datum::int(Self::truncate_i32(*v, width))
            })),
            PrimitiveLiteral::Long(v) => Ok(Some({
                let width = self.width as i64;
                Datum::long(Self::truncate_i64(*v, width))
            })),
            PrimitiveLiteral::Int128(v) => Ok(Some({
                let width = self.width as i128;
                Datum::decimal(Self::truncate_decimal_i128(*v, width))?
            })),
            PrimitiveLiteral::String(v) => Ok(Some({
                let len = self.width as usize;
                Datum::string(Self::truncate_str(v, len).to_string())
            })),
            _ => Err(crate::Error::new(
                crate::ErrorKind::FeatureUnsupported,
                format!(
                    "Unsupported data type for truncate transform: {:?}",
                    input.data_type()
                ),
            )),
        }
    }
}

#[cfg(test)]
mod test {
    use std::sync::Arc;

    use arrow_array::builder::PrimitiveBuilder;
    use arrow_array::types::Decimal128Type;
    use arrow_array::{Decimal128Array, Int32Array, Int64Array};

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
    fn test_truncate_transform() {
        let trans = Transform::Truncate(4);

        let fixture = TestTransformFixture {
            display: "truncate[4]".to_string(),
            json: r#""truncate[4]""#.to_string(),
            dedup_name: "truncate[4]".to_string(),
            preserves_order: true,
            satisfies_order_of: vec![
                (Transform::Truncate(4), true),
                (Transform::Truncate(2), false),
                (Transform::Bucket(4), false),
                (Transform::Void, false),
                (Transform::Day, false),
            ],
            trans_types: vec![
                (Primitive(Binary), Some(Primitive(Binary))),
                (Primitive(Date), None),
                (
                    Primitive(Decimal {
                        precision: 8,
                        scale: 5,
                    }),
                    Some(Primitive(Decimal {
                        precision: 8,
                        scale: 5,
                    })),
                ),
                (Primitive(Fixed(8)), None),
                (Primitive(Int), Some(Primitive(Int))),
                (Primitive(Long), Some(Primitive(Long))),
                (Primitive(StringType), Some(Primitive(StringType))),
                (Primitive(Uuid), None),
                (Primitive(Time), None),
                (Primitive(Timestamp), None),
                (Primitive(Timestamptz), None),
                (Primitive(TimestampNs), None),
                (Primitive(TimestamptzNs), None),
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
    fn test_projection_truncate_string_rewrite_op() -> Result<()> {
        let value = "abcde";

        let fixture = TestProjectionFixture::new(
            Transform::Truncate(5),
            "name",
            NestedField::required(1, "value", Type::Primitive(PrimitiveType::String)),
        );

        fixture.assert_projection(
            &fixture.binary_predicate(PredicateOperator::StartsWith, Datum::string(value)),
            Some(r#"name = "abcde""#),
        )?;

        fixture.assert_projection(
            &fixture.binary_predicate(PredicateOperator::NotStartsWith, Datum::string(value)),
            Some(r#"name != "abcde""#),
        )?;

        let value = "abcdefg";
        fixture.assert_projection(
            &fixture.binary_predicate(PredicateOperator::StartsWith, Datum::string(value)),
            Some(r#"name STARTS WITH "abcde""#),
        )?;

        fixture.assert_projection(
            &fixture.binary_predicate(PredicateOperator::NotStartsWith, Datum::string(value)),
            None,
        )?;

        Ok(())
    }

    #[test]
    fn test_projection_truncate_string() -> Result<()> {
        let value = "abcdefg";

        let fixture = TestProjectionFixture::new(
            Transform::Truncate(5),
            "name",
            NestedField::required(1, "value", Type::Primitive(PrimitiveType::String)),
        );

        fixture.assert_projection(
            &fixture.binary_predicate(PredicateOperator::LessThan, Datum::string(value)),
            Some(r#"name <= "abcde""#),
        )?;

        fixture.assert_projection(
            &fixture.binary_predicate(PredicateOperator::LessThanOrEq, Datum::string(value)),
            Some(r#"name <= "abcde""#),
        )?;

        fixture.assert_projection(
            &fixture.binary_predicate(PredicateOperator::GreaterThan, Datum::string(value)),
            Some(r#"name >= "abcde""#),
        )?;

        fixture.assert_projection(
            &fixture.binary_predicate(PredicateOperator::GreaterThanOrEq, Datum::string(value)),
            Some(r#"name >= "abcde""#),
        )?;

        fixture.assert_projection(
            &fixture.binary_predicate(PredicateOperator::Eq, Datum::string(value)),
            Some(r#"name = "abcde""#),
        )?;

        fixture.assert_projection(
            &fixture.set_predicate(PredicateOperator::In, vec![
                Datum::string(value),
                Datum::string(format!("{value}abc")),
            ]),
            Some(r#"name IN ("abcde")"#),
        )?;

        fixture.assert_projection(
            &fixture.set_predicate(PredicateOperator::NotIn, vec![
                Datum::string(value),
                Datum::string(format!("{value}abc")),
            ]),
            None,
        )?;

        Ok(())
    }

    #[test]
    fn test_projection_truncate_upper_bound_decimal() -> Result<()> {
        let prev = "98.99";
        let curr = "99.99";
        let next = "100.99";

        let fixture = TestProjectionFixture::new(
            Transform::Truncate(10),
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
            &fixture.binary_predicate(PredicateOperator::LessThan, Datum::decimal_from_str(curr)?),
            Some("name <= 9990"),
        )?;

        fixture.assert_projection(
            &fixture.binary_predicate(
                PredicateOperator::LessThanOrEq,
                Datum::decimal_from_str(curr)?,
            ),
            Some("name <= 9990"),
        )?;

        fixture.assert_projection(
            &fixture.binary_predicate(
                PredicateOperator::GreaterThanOrEq,
                Datum::decimal_from_str(curr)?,
            ),
            Some("name >= 9990"),
        )?;

        fixture.assert_projection(
            &fixture.binary_predicate(PredicateOperator::Eq, Datum::decimal_from_str(curr)?),
            Some("name = 9990"),
        )?;

        fixture.assert_projection(
            &fixture.binary_predicate(PredicateOperator::NotEq, Datum::decimal_from_str(curr)?),
            None,
        )?;

        fixture.assert_projection(
            &fixture.set_predicate(PredicateOperator::In, vec![
                Datum::decimal_from_str(prev)?,
                Datum::decimal_from_str(curr)?,
                Datum::decimal_from_str(next)?,
            ]),
            Some("name IN (9890, 9990, 10090)"),
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
    fn test_projection_truncate_lower_bound_decimal() -> Result<()> {
        let prev = "99.00";
        let curr = "100.00";
        let next = "101.00";

        let fixture = TestProjectionFixture::new(
            Transform::Truncate(10),
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
            &fixture.binary_predicate(PredicateOperator::LessThan, Datum::decimal_from_str(curr)?),
            Some("name <= 9990"),
        )?;

        fixture.assert_projection(
            &fixture.binary_predicate(
                PredicateOperator::LessThanOrEq,
                Datum::decimal_from_str(curr)?,
            ),
            Some("name <= 10000"),
        )?;

        fixture.assert_projection(
            &fixture.binary_predicate(
                PredicateOperator::GreaterThanOrEq,
                Datum::decimal_from_str(curr)?,
            ),
            Some("name >= 10000"),
        )?;

        fixture.assert_projection(
            &fixture.binary_predicate(PredicateOperator::Eq, Datum::decimal_from_str(curr)?),
            Some("name = 10000"),
        )?;

        fixture.assert_projection(
            &fixture.binary_predicate(PredicateOperator::NotEq, Datum::decimal_from_str(curr)?),
            None,
        )?;

        fixture.assert_projection(
            &fixture.set_predicate(PredicateOperator::In, vec![
                Datum::decimal_from_str(prev)?,
                Datum::decimal_from_str(curr)?,
                Datum::decimal_from_str(next)?,
            ]),
            Some("name IN (10000, 10100, 9900)"),
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
    fn test_projection_truncate_upper_bound_long() -> Result<()> {
        let value = 99i64;

        let fixture = TestProjectionFixture::new(
            Transform::Truncate(10),
            "name",
            NestedField::required(1, "value", Type::Primitive(PrimitiveType::Long)),
        );

        fixture.assert_projection(
            &fixture.binary_predicate(PredicateOperator::LessThan, Datum::long(value)),
            Some("name <= 90"),
        )?;

        fixture.assert_projection(
            &fixture.binary_predicate(PredicateOperator::LessThanOrEq, Datum::long(value)),
            Some("name <= 90"),
        )?;

        fixture.assert_projection(
            &fixture.binary_predicate(PredicateOperator::GreaterThanOrEq, Datum::long(value)),
            Some("name >= 90"),
        )?;

        fixture.assert_projection(
            &fixture.binary_predicate(PredicateOperator::Eq, Datum::long(value)),
            Some("name = 90"),
        )?;

        fixture.assert_projection(
            &fixture.binary_predicate(PredicateOperator::NotEq, Datum::long(value)),
            None,
        )?;

        fixture.assert_projection(
            &fixture.set_predicate(PredicateOperator::In, vec![
                Datum::long(value - 1),
                Datum::long(value),
                Datum::long(value + 1),
            ]),
            Some("name IN (100, 90)"),
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
    fn test_projection_truncate_lower_bound_long() -> Result<()> {
        let value = 100i64;

        let fixture = TestProjectionFixture::new(
            Transform::Truncate(10),
            "name",
            NestedField::required(1, "value", Type::Primitive(PrimitiveType::Long)),
        );

        fixture.assert_projection(
            &fixture.binary_predicate(PredicateOperator::LessThan, Datum::long(value)),
            Some("name <= 90"),
        )?;

        fixture.assert_projection(
            &fixture.binary_predicate(PredicateOperator::LessThanOrEq, Datum::long(value)),
            Some("name <= 100"),
        )?;

        fixture.assert_projection(
            &fixture.binary_predicate(PredicateOperator::GreaterThanOrEq, Datum::long(value)),
            Some("name >= 100"),
        )?;

        fixture.assert_projection(
            &fixture.binary_predicate(PredicateOperator::Eq, Datum::long(value)),
            Some("name = 100"),
        )?;

        fixture.assert_projection(
            &fixture.binary_predicate(PredicateOperator::NotEq, Datum::long(value)),
            None,
        )?;

        fixture.assert_projection(
            &fixture.set_predicate(PredicateOperator::In, vec![
                Datum::long(value - 1),
                Datum::long(value),
                Datum::long(value + 1),
            ]),
            Some("name IN (100, 90)"),
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
    fn test_projection_truncate_upper_bound_integer() -> Result<()> {
        let value = 99;

        let fixture = TestProjectionFixture::new(
            Transform::Truncate(10),
            "name",
            NestedField::required(1, "value", Type::Primitive(PrimitiveType::Int)),
        );

        fixture.assert_projection(
            &fixture.binary_predicate(PredicateOperator::LessThan, Datum::int(value)),
            Some("name <= 90"),
        )?;

        fixture.assert_projection(
            &fixture.binary_predicate(PredicateOperator::LessThanOrEq, Datum::int(value)),
            Some("name <= 90"),
        )?;

        fixture.assert_projection(
            &fixture.binary_predicate(PredicateOperator::GreaterThanOrEq, Datum::int(value)),
            Some("name >= 90"),
        )?;

        fixture.assert_projection(
            &fixture.binary_predicate(PredicateOperator::Eq, Datum::int(value)),
            Some("name = 90"),
        )?;

        fixture.assert_projection(
            &fixture.binary_predicate(PredicateOperator::NotEq, Datum::int(value)),
            None,
        )?;

        fixture.assert_projection(
            &fixture.set_predicate(PredicateOperator::In, vec![
                Datum::int(value - 1),
                Datum::int(value),
                Datum::int(value + 1),
            ]),
            Some("name IN (100, 90)"),
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
    fn test_projection_truncate_lower_bound_integer() -> Result<()> {
        let value = 100;

        let fixture = TestProjectionFixture::new(
            Transform::Truncate(10),
            "name",
            NestedField::required(1, "value", Type::Primitive(PrimitiveType::Int)),
        );

        fixture.assert_projection(
            &fixture.binary_predicate(PredicateOperator::LessThan, Datum::int(value)),
            Some("name <= 90"),
        )?;

        fixture.assert_projection(
            &fixture.binary_predicate(PredicateOperator::LessThanOrEq, Datum::int(value)),
            Some("name <= 100"),
        )?;

        fixture.assert_projection(
            &fixture.binary_predicate(PredicateOperator::GreaterThanOrEq, Datum::int(value)),
            Some("name >= 100"),
        )?;

        fixture.assert_projection(
            &fixture.binary_predicate(PredicateOperator::Eq, Datum::int(value)),
            Some("name = 100"),
        )?;

        fixture.assert_projection(
            &fixture.binary_predicate(PredicateOperator::NotEq, Datum::int(value)),
            None,
        )?;

        fixture.assert_projection(
            &fixture.set_predicate(PredicateOperator::In, vec![
                Datum::int(value - 1),
                Datum::int(value),
                Datum::int(value + 1),
            ]),
            Some("name IN (100, 90)"),
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

    // Test case ref from: https://iceberg.apache.org/spec/#truncate-transform-details
    #[test]
    fn test_truncate_simple() {
        // test truncate int
        let input = Arc::new(Int32Array::from(vec![1, -1]));
        let res = super::Truncate::new(10).transform(input).unwrap();
        assert_eq!(
            res.as_any().downcast_ref::<Int32Array>().unwrap().value(0),
            0
        );
        assert_eq!(
            res.as_any().downcast_ref::<Int32Array>().unwrap().value(1),
            -10
        );

        // test truncate long
        let input = Arc::new(Int64Array::from(vec![1, -1]));
        let res = super::Truncate::new(10).transform(input).unwrap();
        assert_eq!(
            res.as_any().downcast_ref::<Int64Array>().unwrap().value(0),
            0
        );
        assert_eq!(
            res.as_any().downcast_ref::<Int64Array>().unwrap().value(1),
            -10
        );

        // test decimal
        let mut builder = PrimitiveBuilder::<Decimal128Type>::new()
            .with_precision_and_scale(20, 2)
            .unwrap();
        builder.append_value(1065);
        let input = Arc::new(builder.finish());
        let res = super::Truncate::new(50).transform(input).unwrap();
        assert_eq!(
            res.as_any()
                .downcast_ref::<Decimal128Array>()
                .unwrap()
                .value(0),
            1050
        );

        // test string
        let input = Arc::new(arrow_array::StringArray::from(vec!["iceberg"]));
        let res = super::Truncate::new(3).transform(input).unwrap();
        assert_eq!(
            res.as_any()
                .downcast_ref::<arrow_array::StringArray>()
                .unwrap()
                .value(0),
            "ice"
        );

        // test large string
        let input = Arc::new(arrow_array::LargeStringArray::from(vec!["iceberg"]));
        let res = super::Truncate::new(3).transform(input).unwrap();
        assert_eq!(
            res.as_any()
                .downcast_ref::<arrow_array::LargeStringArray>()
                .unwrap()
                .value(0),
            "ice"
        );

        // test binary
        let input = Arc::new(arrow_array::BinaryArray::from_vec(vec![b"iceberg"]));
        let res = super::Truncate::new(3).transform(input).unwrap();
        assert_eq!(
            res.as_any()
                .downcast_ref::<arrow_array::BinaryArray>()
                .unwrap()
                .value(0),
            b"ice"
        );
    }

    #[test]
    fn test_string_truncate() {
        let test1 = "イロハニホヘト";
        let test1_2_expected = "イロ";
        assert_eq!(super::Truncate::truncate_str(test1, 2), test1_2_expected);

        let test1_3_expected = "イロハ";
        assert_eq!(super::Truncate::truncate_str(test1, 3), test1_3_expected);

        let test2 = "щщаεはчωいにπάほхεろへσκζ";
        let test2_7_expected = "щщаεはчω";
        assert_eq!(super::Truncate::truncate_str(test2, 7), test2_7_expected);

        let test3 = "\u{FFFF}\u{FFFF}";
        assert_eq!(super::Truncate::truncate_str(test3, 2), test3);

        let test4 = "\u{10000}\u{10000}";
        let test4_1_expected = "\u{10000}";
        assert_eq!(super::Truncate::truncate_str(test4, 1), test4_1_expected);
    }

    #[test]
    fn test_literal_int() {
        let input = Datum::int(1);
        let res = super::Truncate::new(10)
            .transform_literal(&input)
            .unwrap()
            .unwrap();
        assert_eq!(res, Datum::int(0),);

        let input = Datum::int(-1);
        let res = super::Truncate::new(10)
            .transform_literal(&input)
            .unwrap()
            .unwrap();
        assert_eq!(res, Datum::int(-10),);
    }

    #[test]
    fn test_literal_long() {
        let input = Datum::long(1);
        let res = super::Truncate::new(10)
            .transform_literal(&input)
            .unwrap()
            .unwrap();
        assert_eq!(res, Datum::long(0),);

        let input = Datum::long(-1);
        let res = super::Truncate::new(10)
            .transform_literal(&input)
            .unwrap()
            .unwrap();
        assert_eq!(res, Datum::long(-10),);
    }

    #[test]
    fn test_decimal_literal() {
        let input = Datum::decimal(1065).unwrap();
        let res = super::Truncate::new(50)
            .transform_literal(&input)
            .unwrap()
            .unwrap();
        assert_eq!(res, Datum::decimal(1050).unwrap(),);
    }

    #[test]
    fn test_string_literal() {
        let input = Datum::string("iceberg".to_string());
        let res = super::Truncate::new(3)
            .transform_literal(&input)
            .unwrap()
            .unwrap();
        assert_eq!(res, Datum::string("ice".to_string()),);
    }
}
