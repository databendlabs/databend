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

use std::collections::BTreeMap;

use apache_avro::Schema;
use apache_avro::types::Value;
use databend_common_expression::types::Decimal;
use databend_common_expression::types::i256;
use num_bigint::BigInt;
use num_traits::ToPrimitive;
use num_traits::Zero;

pub(super) fn to_jsonb<'a>(value: &'a Value, schema: &Schema) -> Result<jsonb::Value<'a>, String> {
    let jvalue = match (value, schema) {
        // primaries
        (Value::Null, Schema::Null) => jsonb::Value::Null,
        (Value::Boolean(v), Schema::Boolean) => jsonb::Value::Bool(*v),
        (Value::Int(v), Schema::Int) => jsonb::Value::from(*v),
        (Value::Long(v), Schema::Long) => jsonb::Value::from(*v),
        (Value::Float(v), Schema::Float) => jsonb::Value::from(*v),
        (Value::Double(v), Schema::Double) => jsonb::Value::from(*v),
        (Value::String(v), Schema::String) => jsonb::Value::from(v.as_str()),
        (Value::Enum(_, v), Schema::Enum(_)) => jsonb::Value::from(v.as_str()),
        (Value::Uuid(v), Schema::Uuid) => jsonb::Value::from(v.to_string()),
        (Value::Bytes(v), Schema::Bytes) | (Value::Fixed(_, v), Schema::Fixed(_)) => {
            jsonb::Value::Binary(v)
        }
        (Value::Decimal(d), Schema::Decimal(schema)) => {
            let big_int = <BigInt>::from(d.to_owned());
            let scale = i64::try_from(schema.scale).unwrap_or(i64::MAX);
            jsonb::Value::Number(convert_decimal(schema.precision, scale, big_int))
        }
        (Value::BigDecimal(d), Schema::BigDecimal) => {
            let (big_int, scale) = d.clone().into_bigint_and_exponent();
            jsonb::Value::Number(convert_decimal(0, scale, big_int))
        }
        (Value::Date(d), Schema::Date) => jsonb::Value::Date(jsonb::Date { value: *d }),
        (Value::TimeMillis(t), Schema::TimeMillis) => {
            jsonb::Value::Number(jsonb::Number::Int64(*t as i64))
        }
        (Value::TimeMicros(t), Schema::TimeMicros) => {
            jsonb::Value::Number(jsonb::Number::Int64(*t))
        }
        (Value::TimestampMillis(v), Schema::TimestampMillis)
        | (Value::LocalTimestampMillis(v), Schema::LocalTimestampMillis) => {
            jsonb::Value::Timestamp(jsonb::Timestamp {
                value: (*v) * 1_000,
            })
        }
        (Value::TimestampMicros(v), Schema::TimestampMicros)
        | (Value::LocalTimestampMicros(v), Schema::LocalTimestampMicros) => {
            jsonb::Value::Timestamp(jsonb::Timestamp { value: *v })
        }
        (Value::TimestampNanos(v), Schema::TimestampNanos)
        | (Value::LocalTimestampNanos(v), Schema::LocalTimestampNanos) => {
            jsonb::Value::Timestamp(jsonb::Timestamp {
                value: (*v) / 1_000,
            })
        }
        (
            Value::TimestampMicros(v)
            | Value::LocalTimestampMicros(v)
            | Value::TimestampMillis(v)
            | Value::LocalTimestampMillis(v)
            | Value::TimestampNanos(v)
            | Value::LocalTimestampNanos(v),
            Schema::Long,
        ) => jsonb::Value::Number(jsonb::Number::Int64(*v)),
        (Value::Date(v), Schema::Int) => jsonb::Value::Number(jsonb::Number::Int64((*v).into())),
        (Value::Duration(d), Schema::Duration) => {
            let months: u32 = d.months().into();
            let days: u32 = d.days().into();
            let millis: u32 = d.millis().into();
            let months =
                i32::try_from(months).map_err(|_| "duration months out of range".to_string())?;
            let days = i32::try_from(days).map_err(|_| "duration days out of range".to_string())?;
            jsonb::Value::Interval(jsonb::Interval {
                months,
                days,
                micros: <i64 as From<u32>>::from(millis) * 1000,
            })
        }

        // container
        (Value::Union(i, v), Schema::Union(union_schema)) => {
            to_jsonb(v, &union_schema.variants()[(*i) as usize])?
        }
        (Value::Array(v), Schema::Array(array_schema)) => {
            let mut array = Vec::with_capacity(v.len());
            for v in v {
                array.push(to_jsonb(v, &array_schema.items)?)
            }
            jsonb::Value::Array(array)
        }
        (Value::Map(v), Schema::Map(map_schema)) => {
            let mut array = Vec::with_capacity(v.len());
            for (k, v) in v {
                array.push((k.clone(), to_jsonb(v, &map_schema.types)?));
            }
            jsonb::Value::Object(BTreeMap::from_iter(array))
        }
        (Value::Record(v), Schema::Record(record_schema)) => {
            let mut array = Vec::with_capacity(v.len());
            for (i, (k, v)) in v.iter().enumerate() {
                array.push((k.clone(), to_jsonb(v, &record_schema.fields[i].schema)?));
            }
            jsonb::Value::Object(BTreeMap::from_iter(array))
        }
        _ => {
            return Err(format!(
                "bug: avro schema and value not match: schema = {:?}, value = {:?}",
                schema, value
            ));
        }
    };
    Ok(jvalue)
}

/// Converts `value / 10^scale` to the narrowest JSONB decimal that holds its
/// scale and digits and is at least as wide as the declared `precision`, or to
/// the nearest `f64` when no JSONB decimal holds it exactly.
///
/// Avro does not ensure that a value fits its declared precision, and a
/// `BigDecimal` scale can exceed its digits or be negative, so the width and
/// the scale come from the value. Trailing zeros are dropped only when the
/// scale exceeds 76.
fn convert_decimal(precision: usize, scale: i64, value: BigInt) -> jsonb::Number {
    let max_scale = i64::from(i256::MAX_PRECISION);
    let (value, scale) = if value.is_zero() {
        (value, scale.clamp(0, max_scale))
    } else if scale < 0 {
        // Beyond 76 digits, `value * 10^-scale` fits no decimal.
        if scale < -max_scale {
            return decimal_to_f64(&value, scale);
        }
        (value * BigInt::from(10).pow(scale.unsigned_abs() as u32), 0)
    } else {
        let (mut value, mut scale) = (value, scale);
        let ten = BigInt::from(10);
        while scale > max_scale && (&value % &ten).is_zero() {
            value /= &ten;
            scale -= 1;
        }
        if scale > max_scale {
            return decimal_to_f64(&value, scale);
        }
        (value, scale)
    };
    let scale = scale as u8;
    let precision = precision.min(usize::from(i256::MAX_PRECISION));
    if let Some(v) = value.to_i64().filter(|v| fits(*v, scale, precision)) {
        return jsonb::Number::Decimal64(jsonb::Decimal64 { scale, value: v });
    }
    if let Some(v) = value.to_i128().filter(|v| fits(*v, scale, precision)) {
        return jsonb::Number::Decimal128(jsonb::Decimal128 { scale, value: v });
    }
    match i256::from_bigint(value.clone()).filter(|v| fits(*v, scale, precision)) {
        Some(v) => jsonb::Number::Decimal256(jsonb::Decimal256 { scale, value: v.0 }),
        None => decimal_to_f64(&value, i64::from(scale)),
    }
}

/// Returns whether `T` stores a decimal of `scale` and `value` within its
/// precision, and that precision is at least `precision`.
fn fits<T: Decimal>(value: T, scale: u8, precision: usize) -> bool {
    precision <= usize::from(T::MAX_PRECISION)
        && scale <= T::MAX_PRECISION
        && value >= T::min_for_precision(T::MAX_PRECISION)
        && value <= T::max_for_precision(T::MAX_PRECISION)
}

/// Returns the `f64` nearest to `value / 10^scale`, or an infinity or zero
/// beyond the `f64` range.
fn decimal_to_f64(value: &BigInt, scale: i64) -> jsonb::Number {
    // The standard parser rounds correctly, and the text is `-`, digits, `e`
    // and the exponent.
    let text = format!("{value}e{}", -i128::from(scale));
    jsonb::Number::Float64(text.parse().expect("decimal text is a valid float"))
}

#[cfg(test)]
mod tests {
    use std::ops::Mul;
    use std::str::FromStr;

    use apache_avro::Schema;
    use apache_avro::schema::DecimalSchema;
    use databend_common_expression::types::Decimal;
    use databend_common_expression::types::i256;
    use num_bigint::BigInt;

    use crate::read::avro::avro_to_jsonb::to_jsonb;

    fn create_avro_decimal(
        bigint: &str,
        precision: usize,
        scale: usize,
    ) -> (apache_avro::types::Value, Schema) {
        let avro_schema = Schema::Decimal(DecimalSchema {
            precision,
            scale,
            inner: Box::new(Schema::Null),
        });
        let big_int = BigInt::from_str(bigint).unwrap();
        let value = apache_avro::types::Value::Decimal(apache_avro::Decimal::from(
            big_int.to_signed_bytes_be(),
        ));
        (value, avro_schema)
    }

    fn create_avro_big_decimal(bigint: &str, scale: usize) -> (apache_avro::types::Value, Schema) {
        let avro_schema = Schema::BigDecimal;
        let big_int = BigInt::from_str(bigint).unwrap();
        let value = apache_avro::types::Value::BigDecimal(apache_avro::BigDecimal::new(
            big_int,
            scale as i64,
        ));
        (value, avro_schema)
    }

    #[test]
    fn test_decimal_128_ok() {
        let cases = vec![
            (7, 4, 1234567i128),
            (7, 4, 123456i128),
            (38, 10, i128::DECIMAL_MAX),
        ];
        for (p, s, v) in cases {
            let (value, schema) = create_avro_decimal(&v.to_string(), p, s);
            let jsonb_value = to_jsonb(&value, &schema).unwrap();
            let expected = jsonb::Value::Number(jsonb::Number::Decimal128(jsonb::Decimal128 {
                scale: s as u8,
                value: v,
            }));
            assert_eq!(jsonb_value, expected);

            let (value, schema) = create_avro_big_decimal(&v.to_string(), s);
            let jsonb_value = to_jsonb(&value, &schema).unwrap();
            let expected = jsonb::Value::Number(jsonb::Number::Decimal128(jsonb::Decimal128 {
                scale: s as u8,
                value: v,
            }));
            assert_eq!(jsonb_value, expected);
        }
    }

    #[test]
    fn test_decimal_256_ok() {
        let cases = vec![
            (
                39,
                10,
                i256::from_i128_uncheck(i128::MAX).mul(i256::from_i64(10)),
            ),
            (
                72,
                10,
                i256::from_i128_uncheck(i128::MAX).mul(i256::from_i64(10)),
            ),
            (72, 10, i256::DECIMAL_MAX),
        ];
        for (p, s, v) in cases {
            let (value, schema) = create_avro_decimal(&v.to_string(), p, s);
            let jsonb_value = to_jsonb(&value, &schema).unwrap();
            let expected = jsonb::Value::Number(jsonb::Number::Decimal256(jsonb::Decimal256 {
                scale: s as u8,
                value: v.0,
            }));
            assert_eq!(jsonb_value, expected);

            let (value, schema) = create_avro_big_decimal(&v.to_string(), s);
            let jsonb_value = to_jsonb(&value, &schema).unwrap();
            let expected = jsonb::Value::Number(jsonb::Number::Decimal256(jsonb::Decimal256 {
                scale: s as u8,
                value: v.0,
            }));
            assert_eq!(jsonb_value, expected);
        }
    }

    // Avro does not ensure that a value fits its declared precision, and a
    // `BigDecimal` scale can exceed its digits or be negative. These used to
    // become JSONB decimals beyond their precision, or wrap the scale: `1E+5`
    // became `1e-251`.
    #[test]
    fn test_decimal_width_and_scale_follow_the_value() {
        use jsonb::Decimal64;
        use jsonb::Decimal128;
        use jsonb::Decimal256;
        use jsonb::Number;

        use super::convert_decimal;

        let int = |text: &str| BigInt::from_str(text).unwrap();
        let e76 = format!("1{}", "0".repeat(76));
        let cases = [
            // The declared width is kept when the value fits it.
            (
                0,
                2,
                "12345",
                Number::Decimal64(Decimal64 {
                    scale: 2,
                    value: 12345,
                }),
            ),
            (
                20,
                2,
                "12345",
                Number::Decimal128(Decimal128 {
                    scale: 2,
                    value: 12345,
                }),
            ),
            // More digits than the declared precision.
            (
                10,
                0,
                "1000000000000000000",
                Number::Decimal128(Decimal128 {
                    scale: 0,
                    value: 10_i128.pow(18),
                }),
            ),
            // A scale beyond the digits.
            (
                0,
                30,
                "1",
                Number::Decimal128(Decimal128 {
                    scale: 30,
                    value: 1,
                }),
            ),
            (
                0,
                76,
                "1",
                Number::Decimal256(Decimal256 {
                    scale: 76,
                    value: 1.into(),
                }),
            ),
            // Negative scales.
            (
                0,
                -5,
                "1",
                Number::Decimal64(Decimal64 {
                    scale: 0,
                    value: 100000,
                }),
            ),
            (
                0,
                -5,
                "-12",
                Number::Decimal64(Decimal64 {
                    scale: 0,
                    value: -1200000,
                }),
            ),
            // Trailing zeros are dropped only beyond scale 76.
            (
                0,
                78,
                "1000",
                Number::Decimal256(Decimal256 {
                    scale: 76,
                    value: 10.into(),
                }),
            ),
            (
                0,
                100,
                "0",
                Number::Decimal256(Decimal256 {
                    scale: 76,
                    value: 0.into(),
                }),
            ),
            (
                0,
                -100,
                "0",
                Number::Decimal64(Decimal64 { scale: 0, value: 0 }),
            ),
            // No JSONB decimal holds these exactly.
            (0, 80, "1", Number::Float64(1e-80)),
            (0, 80, "-15", Number::Float64(-1.5e-79)),
            (0, 0, e76.as_str(), Number::Float64(1e76)),
            (0, -100, "1", Number::Float64(1e100)),
            (0, i64::MIN, "1", Number::Float64(f64::INFINITY)),
            (0, i64::MAX, "1", Number::Float64(0.0)),
        ];
        for (precision, scale, value, expected) in cases {
            let number = convert_decimal(precision, scale, int(value));
            assert_eq!(
                format!("{number:?}"),
                format!("{expected:?}"),
                "{value}e{}",
                -i128::from(scale)
            );
        }

        // Through Avro values.
        let value = apache_avro::types::Value::BigDecimal(apache_avro::BigDecimal::new(
            BigInt::from(1),
            -5,
        ));
        let jsonb_value = to_jsonb(&value, &Schema::BigDecimal).unwrap();
        assert_eq!(
            format!("{jsonb_value:?}"),
            format!(
                "{:?}",
                jsonb::Value::Number(Number::Decimal64(Decimal64 {
                    scale: 0,
                    value: 100000,
                }))
            )
        );
        let (value, schema) = create_avro_decimal(&10_i64.pow(18).to_string(), 10, 2);
        let jsonb_value = to_jsonb(&value, &schema).unwrap();
        assert_eq!(
            format!("{jsonb_value:?}"),
            format!(
                "{:?}",
                jsonb::Value::Number(Number::Decimal128(Decimal128 {
                    scale: 2,
                    value: 10_i128.pow(18),
                }))
            )
        );
    }
}
