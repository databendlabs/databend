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

use apache_avro::types::Value;
use apache_avro::Schema;
use databend_common_expression::types::i256;
use databend_common_expression::types::Decimal;
use databend_common_expression::types::MAX_DECIMAL128_PRECISION;
use databend_common_expression::types::MAX_DECIMAL256_PRECISION;
use num_bigint::BigInt;

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
            jsonb::Value::Number(convert_decimal(schema.precision, schema.scale, big_int)?)
        }
        (Value::BigDecimal(d), Schema::BigDecimal) => {
            let precision = d.digits() as usize;
            let (big_int, scale) = d.clone().into_bigint_and_exponent();
            jsonb::Value::Number(convert_decimal(precision, scale as usize, big_int)?)
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
                value: (*v) * 1_000_000,
            })
        }
        (Value::LocalTimestampMicros(v), Schema::LocalTimestampMicros) => {
            jsonb::Value::Timestamp(jsonb::Timestamp {
                value: (*v) * 1_000,
            })
        }
        (Value::LocalTimestampNanos(v), Schema::LocalTimestampNanos) => {
            jsonb::Value::Timestamp(jsonb::Timestamp { value: (*v) })
        }
        (Value::Duration(d), Schema::Duration) => {
            let months: u32 = d.months().into();
            let days: u32 = d.days().into();
            let millis: u32 = d.millis().into();
            jsonb::Value::Interval(jsonb::Interval {
                months: months as i32,
                days: days as i32,
                micros: (millis * 1000) as i64,
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
            ))
        }
    };
    Ok(jvalue)
}

fn convert_decimal(
    precision: usize,
    scale: usize,
    big_int: BigInt,
) -> Result<jsonb::Number, String> {
    let max_128 = MAX_DECIMAL128_PRECISION as usize;
    let max_256 = MAX_DECIMAL256_PRECISION as usize;
    if precision <= max_128 {
        Ok(jsonb::Number::Decimal128(jsonb::Decimal128 {
            precision: precision as u8,
            scale: scale as u8,
            value: i128::from_bigint(big_int).ok_or("too many bits for i128".to_string())?,
        }))
    } else if precision <= max_256 {
        Ok(jsonb::Number::Decimal256(jsonb::Decimal256 {
            precision: precision as u8,
            scale: scale as u8,
            value: i256::from_bigint(big_int)
                .ok_or("too many bits for i256".to_string())?
                .0,
        }))
    } else {
        return Err(format!("Decimal precision too large: {}", precision));
    }
}
