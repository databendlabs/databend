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

use std::any::Any;

use chrono_tz::Tz;
use databend_common_column::types::months_days_micros;
use databend_common_column::types::timestamp_tz;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::ColumnBuilder;
use databend_common_expression::TableDataType;
use databend_common_expression::serialize::read_decimal_with_size;
use databend_common_expression::types::AnyType;
use databend_common_expression::types::MutableBitmap;
use databend_common_expression::types::NumberColumnBuilder;
use databend_common_expression::types::VectorColumnBuilder;
use databend_common_expression::types::VectorScalarRef;
use databend_common_expression::types::array::ArrayColumnBuilder;
use databend_common_expression::types::binary::BinaryColumnBuilder;
use databend_common_expression::types::date::clamp_date;
use databend_common_expression::types::decimal::Decimal;
use databend_common_expression::types::decimal::DecimalColumnBuilder;
use databend_common_expression::types::decimal::DecimalSize;
use databend_common_expression::types::nullable::NullableColumnBuilder;
use databend_common_expression::types::number::Number;
use databend_common_expression::types::string::StringColumnBuilder;
use databend_common_expression::utils::auto_detect_datetime::int64_to_timestamp;
use databend_common_expression::utils::auto_detect_datetime::parse_date_with_auto;
use databend_common_expression::utils::auto_detect_datetime::parse_timestamp_tz_with_auto;
use databend_common_expression::utils::auto_detect_datetime::parse_timestamp_with_auto;
use databend_common_expression::with_decimal_type;
use databend_common_expression::with_number_mapped_type;
use databend_common_io::HybridBitmap;
use databend_common_io::Interval;
use databend_common_io::geography::geography_from_ewkt;
use databend_common_io::geometry_from_ewkt;
use databend_common_io::parse_bitmap;
use databend_common_io::prelude::InputFormatSettings;
use lexical_core::FromLexical;
use num_traits::NumCast;
use serde_json::Value;

use crate::FieldDecoder;
use crate::json::JsonObjectLookup;
use crate::json::JsonValue;

pub struct FieldJsonAstDecoder {
    timezone: Tz,
    pub ident_case_sensitive: bool,
    pub is_select: bool,
    is_rounding_mode: bool,
    enable_auto_detect_datetime_format: bool,
}

impl FieldDecoder for FieldJsonAstDecoder {
    fn as_any(&self) -> &dyn Any {
        self
    }
}

impl FieldJsonAstDecoder {
    pub fn create(settings: &InputFormatSettings, is_select: bool) -> Self {
        FieldJsonAstDecoder {
            timezone: settings.timezone,
            ident_case_sensitive: false,
            is_select,
            is_rounding_mode: settings.is_rounding_mode,
            enable_auto_detect_datetime_format: settings.enable_auto_detect_datetime_format,
        }
    }

    pub fn read_field_with_data_type(
        &self,
        column: &mut ColumnBuilder,
        value: &impl JsonValue,
        data_type: &TableDataType,
    ) -> Result<()> {
        match column {
            ColumnBuilder::Null { len } => self.read_null(len, value),
            ColumnBuilder::Nullable(c) => self.read_nullable(c, value, nullable_inner(data_type)),
            ColumnBuilder::Boolean(c) => self.read_bool(c, value),
            ColumnBuilder::Number(c) => with_number_mapped_type!(|NUM_TYPE| match c {
                NumberColumnBuilder::NUM_TYPE(c) => {
                    if NUM_TYPE::FLOATING {
                        self.read_float(c, value)
                    } else if NUM_TYPE::NEGATIVE {
                        self.read_int(c, value)
                    } else {
                        self.read_uint(c, value)
                    }
                }
            }),
            ColumnBuilder::Decimal(c) => with_decimal_type!(|DECIMAL_TYPE| match c {
                DecimalColumnBuilder::DECIMAL_TYPE(c, size) => self.read_decimal(c, *size, value),
            }),
            ColumnBuilder::Date(c) => self.read_date(c, value),
            ColumnBuilder::Timestamp(c) => self.read_timestamp(c, value),
            ColumnBuilder::TimestampTz(c) => self.read_timestamp_tz(c, value),
            ColumnBuilder::Binary(_c) => unimplemented!("binary literal is not supported"),
            ColumnBuilder::String(c) => self.read_string(c, value),
            ColumnBuilder::Array(c) => self.read_array(c, value, array_inner(data_type)),
            ColumnBuilder::Map(c) => self.read_map(c, value, map_fields(data_type)),
            ColumnBuilder::Tuple(fields) => self.read_tuple(fields, value, tuple_fields(data_type)),
            ColumnBuilder::Bitmap(c) => self.read_bitmap(c, value),
            ColumnBuilder::Variant(c) => self.read_variant(c, value),
            ColumnBuilder::Geometry(c) => self.read_geometry(c, value),
            ColumnBuilder::Geography(c) => self.read_geography(c, value),
            ColumnBuilder::Interval(c) => self.read_interval(c, value),
            ColumnBuilder::Vector(c) => self.read_vector(c, value),
            ColumnBuilder::EmptyArray { len } => match value.as_array() {
                Some([]) => {
                    *len += 1;
                    Ok(())
                }
                _ => Err(ErrorCode::BadBytes("Incorrect empty array value")),
            },
            ColumnBuilder::EmptyMap { len } => match value.object_iter() {
                Some(object) if object.len() == 0 => {
                    *len += 1;
                    Ok(())
                }
                _ => Err(ErrorCode::BadBytes("Incorrect empty map value")),
            },
            ColumnBuilder::Opaque(_) => Err(ErrorCode::Unimplemented(
                "Opaque type not supported in json_ast",
            )),
        }
    }

    fn read_bool(&self, column: &mut MutableBitmap, value: &impl JsonValue) -> Result<()> {
        let v = value
            .as_bool()
            .ok_or_else(|| ErrorCode::BadBytes("Incorrect boolean value"))?;
        column.push(v);
        Ok(())
    }

    fn read_null(&self, len: &mut usize, _value: &impl JsonValue) -> Result<()> {
        *len += 1;
        Ok(())
    }

    fn read_nullable(
        &self,
        column: &mut NullableColumnBuilder<AnyType>,
        value: &impl JsonValue,
        inner_type: &TableDataType,
    ) -> Result<()> {
        if value.is_null() {
            column.push_null();
        } else {
            self.read_field_with_data_type(&mut column.builder, value, inner_type)?;
            column.validity.push(true);
        }
        Ok(())
    }

    fn read_int<T>(&self, column: &mut Vec<T>, value: &impl JsonValue) -> Result<()>
    where
        T: Number + From<T::Native>,
        T::Native: FromLexical + NumCast,
    {
        match value.as_number() {
            Some(v) => {
                let new_val: Option<T::Native> = match v.as_i64() {
                    Some(v) => num_traits::cast::cast(v),
                    None => match v.as_f64() {
                        Some(v) => {
                            if self.is_rounding_mode {
                                num_traits::cast::cast(v.round())
                            } else {
                                num_traits::cast::cast(v)
                            }
                        }
                        None => None,
                    },
                };
                match new_val {
                    Some(v) => {
                        column.push(v.into());
                        Ok(())
                    }
                    None => Err(ErrorCode::BadBytes(format!("Incorrect json number {}", v))),
                }
            }
            _ => Err(ErrorCode::BadBytes("Incorrect json value, must be number")),
        }
    }

    fn read_uint<T>(&self, column: &mut Vec<T>, value: &impl JsonValue) -> Result<()>
    where
        T: Number + From<T::Native>,
        T::Native: FromLexical + NumCast,
    {
        match value.as_number() {
            Some(v) => {
                let new_val: Option<T::Native> = match v.as_u64() {
                    Some(v) => num_traits::cast::cast(v),
                    None => match v.as_f64() {
                        Some(v) => {
                            if self.is_rounding_mode {
                                num_traits::cast::cast(v.round())
                            } else {
                                num_traits::cast::cast(v)
                            }
                        }
                        None => None,
                    },
                };
                match new_val {
                    Some(v) => {
                        column.push(v.into());
                        Ok(())
                    }
                    None => Err(ErrorCode::BadBytes(format!("Incorrect json number {}", v))),
                }
            }
            _ => Err(ErrorCode::BadBytes("Incorrect json value, must be number")),
        }
    }

    fn read_float<T>(&self, column: &mut Vec<T>, value: &impl JsonValue) -> Result<()>
    where
        T: Number + From<T::Native>,
        T::Native: FromLexical + NumCast,
    {
        match value.as_number() {
            Some(v) => {
                let new_val: Option<T::Native> = match v.as_f64() {
                    Some(v) => num_traits::cast::cast(v),
                    None => None,
                };
                match new_val {
                    Some(v) => {
                        column.push(v.into());
                        Ok(())
                    }
                    None => Err(ErrorCode::BadBytes(format!("Incorrect json number {}", v))),
                }
            }
            _ => Err(ErrorCode::BadBytes("Incorrect json value, must be number")),
        }
    }

    fn read_decimal<D: Decimal>(
        &self,
        column: &mut Vec<D>,
        size: DecimalSize,
        value: &impl JsonValue,
    ) -> Result<()> {
        let number = value.as_number();
        let text = if let Some(number) = &number {
            // Match read_decimal_from_json: keep sign-directed integer input
            // on the native with_size fast path, including precision checks.
            let text = number.as_str();
            let integer = if text.starts_with('-') {
                text.parse::<i64>().ok().map(D::from_i64)
            } else {
                text.parse::<u64>().ok().and_then(D::from_i128)
            };
            if let Some(n) = integer {
                column.push(
                    n.with_size(size)
                        .ok_or_else(|| "Decimal overflow".to_string())?,
                );
                return Ok(());
            }
            text
        } else {
            value
                .as_str()
                .ok_or_else(|| "Incorrect json value for decimal".to_string())?
        };
        let (n, _) =
            read_decimal_with_size::<D>(text.as_bytes(), size, true, self.is_rounding_mode)?;
        column.push(n);
        Ok(())
    }

    fn read_string(&self, column: &mut StringColumnBuilder, value: &impl JsonValue) -> Result<()> {
        if let Some(s) = value.as_str() {
            column.put_str(s);
        } else if let Some(v) = value.as_bool() {
            column.put_str(if v { "true" } else { "false" });
        } else if let Some(n) = value.as_number() {
            column.put_str(&n.string_text()?);
        } else if value.is_null() {
            column.put_str("null");
        } else {
            return Err(ErrorCode::BadBytes("Incorrect json value, must be string"));
        }
        column.commit_row();
        Ok(())
    }

    fn read_date(&self, column: &mut Vec<i32>, value: &impl JsonValue) -> Result<()> {
        match (value.as_str(), value.as_number()) {
            (Some(v), _) => {
                let days = parse_date_with_auto(
                    v,
                    &self.timezone,
                    self.enable_auto_detect_datetime_format,
                )?;
                column.push(days);
                Ok(())
            }
            (_, Some(number)) => match number.as_i64() {
                Some(n) => {
                    column.push(clamp_date(n));
                    Ok(())
                }
                None => Err(ErrorCode::BadArguments("Incorrect date value")),
            },
            _ => Err(ErrorCode::BadBytes("Incorrect date value")),
        }
    }

    fn read_timestamp(&self, column: &mut Vec<i64>, value: &impl JsonValue) -> Result<()> {
        match (value.as_str(), value.as_number()) {
            (Some(v), _) => {
                let micros = parse_timestamp_with_auto(
                    v,
                    &self.timezone,
                    self.enable_auto_detect_datetime_format,
                )?;
                column.push(micros);
                Ok(())
            }
            (_, Some(number)) => match number.as_i64() {
                Some(n) => {
                    let n = int64_to_timestamp(n);
                    column.push(n);
                    Ok(())
                }
                None => Err(ErrorCode::BadArguments(
                    "Incorrect timestamp value, must be i64",
                )),
            },
            _ => Err(ErrorCode::BadBytes("Incorrect timestamp value")),
        }
    }

    fn read_timestamp_tz(
        &self,
        column: &mut Vec<timestamp_tz>,
        value: &impl JsonValue,
    ) -> Result<()> {
        match value.as_str() {
            Some(s) => {
                let ts_tz = parse_timestamp_tz_with_auto(
                    s,
                    &self.timezone,
                    self.enable_auto_detect_datetime_format,
                )?;
                column.push(ts_tz);
                Ok(())
            }
            _ => Err(ErrorCode::BadBytes(
                "Incorrect TimestampTz value, must be string",
            )),
        }
    }

    fn read_interval(
        &self,
        column: &mut Vec<months_days_micros>,
        value: &impl JsonValue,
    ) -> Result<()> {
        match value.as_str() {
            Some(s) => {
                let i = Interval::from_string(s)?;
                column.push(months_days_micros::new(i.months, i.days, i.micros));
                Ok(())
            }
            _ => Err(ErrorCode::BadBytes(
                "Incorrect interval value, must be string",
            )),
        }
    }

    fn read_bitmap(&self, column: &mut BinaryColumnBuilder, value: &impl JsonValue) -> Result<()> {
        match (value.as_str(), value.as_number()) {
            (Some(v), _) => {
                let rb = parse_bitmap(v.as_bytes())?;
                rb.serialize_into(&mut column.data).unwrap();
                column.commit_row();
                Ok(())
            }
            (_, Some(number)) => match number.as_u64() {
                Some(n) => {
                    let mut rb = HybridBitmap::new();
                    rb.insert(n);
                    rb.serialize_into(&mut column.data).unwrap();
                    column.commit_row();
                    Ok(())
                }
                None => Err(ErrorCode::BadArguments(
                    "Incorrect Bitmap value, must be u64",
                )),
            },
            _ => Err(ErrorCode::BadBytes("Incorrect Bitmap value")),
        }
    }

    fn read_variant(&self, column: &mut BinaryColumnBuilder, value: &impl JsonValue) -> Result<()> {
        value.write_jsonb(&mut column.data)?;
        column.commit_row();
        Ok(())
    }

    fn read_geometry(
        &self,
        column: &mut BinaryColumnBuilder,
        value: &impl JsonValue,
    ) -> Result<()> {
        match value.as_str() {
            Some(v) => {
                let geom = geometry_from_ewkt(v, None)?;
                column.put_slice(&geom);
                column.commit_row();
                Ok(())
            }
            _ => Err(ErrorCode::BadBytes("Incorrect Geometry value")),
        }
    }

    fn read_geography(
        &self,
        column: &mut BinaryColumnBuilder,
        value: &impl JsonValue,
    ) -> Result<()> {
        match value.as_str() {
            Some(v) => {
                let geog = geography_from_ewkt(v)?;
                column.put_slice(&geog);
                column.commit_row();
                Ok(())
            }
            _ => Err(ErrorCode::BadBytes("Incorrect Geography value")),
        }
    }

    fn read_array(
        &self,
        column: &mut ArrayColumnBuilder<AnyType>,
        value: &impl JsonValue,
        inner_type: &TableDataType,
    ) -> Result<()> {
        match value.as_array() {
            Some(vals) => {
                for val in vals {
                    self.read_field_with_data_type(&mut column.builder, val, inner_type)?;
                }
                column.commit_row();
                Ok(())
            }
            _ => Err(ErrorCode::BadBytes("Incorrect json value, must be array")),
        }
    }

    fn read_map(
        &self,
        column: &mut ArrayColumnBuilder<AnyType>,
        value: &impl JsonValue,
        fields_type: &[TableDataType],
    ) -> Result<()> {
        const KEY: usize = 0;
        const VALUE: usize = 1;
        let map_builder = column.builder.as_tuple_mut().unwrap();
        if fields_type.len() != 2 {
            return Err(ErrorCode::BadBytes(
                "Incorrect json value for map, missing map field types",
            ));
        }
        let key_type = &fields_type[KEY];
        let value_type = &fields_type[VALUE];
        match value.object_iter() {
            Some(obj) => {
                for (key, val) in obj {
                    let key = Value::String(key.to_string());
                    self.read_field_with_data_type(&mut map_builder[KEY], &key, key_type)?;
                    self.read_field_with_data_type(&mut map_builder[VALUE], val, value_type)?;
                }
                column.commit_row();
                Ok(())
            }
            _ => Err(ErrorCode::BadBytes("Incorrect json value, must be object")),
        }
    }

    fn read_tuple(
        &self,
        fields: &mut [ColumnBuilder],
        value: &impl JsonValue,
        tuple_fields: Option<(&[String], &[TableDataType])>,
    ) -> Result<()> {
        match (value.object_iter(), value.as_array()) {
            (Some(obj), _) => {
                if fields.len() != obj.len() {
                    return Err(ErrorCode::BadBytes(format!(
                        "Incorrect json value, expect {} values, but get {} values",
                        fields.len(),
                        obj.len()
                    )));
                }
                if let Some((fields_name, fields_type)) = tuple_fields {
                    let object =
                        JsonObjectLookup::for_tuple(value, self.ident_case_sensitive, fields.len());
                    for (idx, (field, field_name)) in
                        fields.iter_mut().zip(fields_name.iter()).enumerate()
                    {
                        let val = object.get(field_name).ok_or_else(|| {
                            ErrorCode::BadBytes(format!(
                                "Incorrect json value for tuple, missing field {}",
                                field_name
                            ))
                        })?;
                        self.read_field_with_data_type(field, val, &fields_type[idx])?;
                    }
                } else {
                    return Err(ErrorCode::BadBytes(
                        "Incorrect json value for tuple object, missing tuple field names",
                    ));
                }
                Ok(())
            }
            (_, Some(values)) => {
                if fields.len() != values.len() {
                    return Err(ErrorCode::BadBytes(format!(
                        "Incorrect json value, expect {} values, but get {} values",
                        fields.len(),
                        values.len()
                    )));
                }
                let fields_type = tuple_fields.map(|(_, fields_type)| fields_type);
                for (idx, (field, val)) in fields.iter_mut().zip(values.iter()).enumerate() {
                    let field_type = fields_type.and_then(|types| types.get(idx)).ok_or_else(
                        || {
                            ErrorCode::BadBytes(
                                "Incorrect json value for tuple array, missing tuple field types",
                            )
                        },
                    )?;
                    self.read_field_with_data_type(field, val, field_type)?;
                }
                Ok(())
            }
            _ => Err(ErrorCode::BadBytes(
                "Incorrect json value for tuple, must be object or array",
            )),
        }
    }

    fn read_vector(&self, column: &mut VectorColumnBuilder, value: &impl JsonValue) -> Result<()> {
        match value.as_array() {
            Some(vals) => {
                let dimension = column.dimension();
                if dimension != vals.len() {
                    return Err(ErrorCode::BadBytes(format!(
                        "Incorrect vector value, dimension must be {}",
                        dimension
                    )));
                }
                let mut values = Vec::with_capacity(dimension);
                for val in vals {
                    if let Some(num) = val.as_number() {
                        if let Some(v) = num.as_f64() {
                            let v = v as f32;
                            values.push(v.into());
                            continue;
                        }
                    }
                    return Err(ErrorCode::BadArguments(
                        "Incorrect vector value, must be f32",
                    ));
                }
                column.push(&VectorScalarRef::Float32(&values));
                Ok(())
            }
            _ => Err(ErrorCode::BadBytes("Incorrect json value, must be array")),
        }
    }
}

fn nullable_inner(data_type: &TableDataType) -> &TableDataType {
    match data_type {
        TableDataType::Nullable(inner) => inner.as_ref(),
        other => other,
    }
}

fn array_inner(data_type: &TableDataType) -> &TableDataType {
    match nullable_inner(data_type) {
        TableDataType::Array(inner) => inner.as_ref(),
        other => other,
    }
}

fn map_fields(data_type: &TableDataType) -> &[TableDataType] {
    match nullable_inner(data_type) {
        TableDataType::Map(inner) => match inner.as_ref() {
            TableDataType::Tuple { fields_type, .. } => fields_type.as_slice(),
            _ => &[],
        },
        _ => &[],
    }
}

fn tuple_fields(data_type: &TableDataType) -> Option<(&[String], &[TableDataType])> {
    match nullable_inner(data_type) {
        TableDataType::Tuple {
            fields_name,
            fields_type,
        } => Some((fields_name.as_slice(), fields_type.as_slice())),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use databend_common_expression::ScalarRef;
    use databend_common_expression::types::DataType;
    use databend_common_expression::types::NumberDataType;
    use databend_common_expression::types::NumberScalar;
    use databend_common_io::prelude::InputFormatSettings;

    use super::*;

    #[test]
    fn test_read_float64() -> Result<()> {
        let decoder = FieldJsonAstDecoder::create(&InputFormatSettings::default(), false);
        let data_type = TableDataType::Number(NumberDataType::Float64);
        let cases = [
            "0",
            "-0.0",
            "165.6263",
            "-0.0003",
            "99999999999999.9999",
            "1.23456789012345678e13",
            "18446744073709551615",
            "2.2250738585072014e-308",
            "5e-324",
            "1.7976931348623157e308",
        ];
        for text in cases {
            let value: serde_json::Value = serde_json::from_str(text).unwrap();
            let mut builder = ColumnBuilder::with_capacity(&DataType::from(&data_type), 1);
            decoder.read_field_with_data_type(&mut builder, &value, &data_type)?;
            let column = builder.build();
            let ScalarRef::Number(NumberScalar::Float64(actual)) = column.index(0).unwrap() else {
                unreachable!()
            };
            assert_eq!(
                actual.0.to_bits(),
                text.parse::<f64>().unwrap().to_bits(),
                "{text}"
            );
        }
        Ok(())
    }

    #[test]
    fn test_sonic_decimal_precision_issue_20748() -> Result<()> {
        let decoder = FieldJsonAstDecoder::create(&InputFormatSettings::default(), false);
        let size = DecimalSize::new(18, 4)?;
        for (text, expected) in [
            ("165.6263", 1_656_263i128),
            ("-0.0003", -3),
            ("99999999999999.9999", 999_999_999_999_999_999),
            ("12345678901234.5678", 123_456_789_012_345_678),
            ("-3e-4", -3),
            ("1.23456789012345678E13", 123_456_789_012_345_678),
        ] {
            let sonic = crate::json::parse_sonic(text.as_bytes()).unwrap();
            let mut values = Vec::<i128>::new();
            decoder.read_decimal(&mut values, size, &sonic)?;
            assert_eq!(values, vec![expected], "{text}");
        }
        Ok(())
    }

    #[test]
    fn test_decimal_backends_match_upstream() -> Result<()> {
        fn check<D: Decimal>(size: DecimalSize, text: &str, rounding: bool) {
            let value: serde_json::Value = serde_json::from_str(text).unwrap();
            let sonic = crate::json::parse_sonic(text.as_bytes()).unwrap();
            let settings = InputFormatSettings {
                is_rounding_mode: rounding,
                ..Default::default()
            };
            let decoder = FieldJsonAstDecoder::create(&settings, false);
            let expected = databend_common_expression::serialize::read_decimal_from_json::<D>(
                &value, size, rounding,
            );
            for result in [
                {
                    let mut column = Vec::<D>::new();
                    decoder
                        .read_decimal(&mut column, size, &value)
                        .map(|_| column[0])
                },
                {
                    let mut column = Vec::<D>::new();
                    decoder
                        .read_decimal(&mut column, size, &sonic)
                        .map(|_| column[0])
                },
            ] {
                match (&expected, result) {
                    (Ok(expected), Ok(actual)) => {
                        assert_eq!(*expected, actual, "{size:?}: {text}, rounding={rounding}")
                    }
                    (Err(expected), Err(actual)) => assert!(
                        actual.to_string().contains(expected),
                        "{actual}: {expected}"
                    ),
                    (expected, actual) => {
                        panic!("{size:?}: {text}, rounding={rounding}: {expected:?} != {actual:?}")
                    }
                }
            }
        }
        for rounding in [false, true] {
            for text in [
                "1.235",
                "-1.235",
                "99.995",
                "-99.995",
                "\"1.235\"",
                "\"-1.235\"",
                "\"1e\"",
                "\"1e+\"",
                "\"1e-\"",
                "1",
                "-1",
                "-0",
                "9223372036854775807",
                "-9223372036854775808",
                "18446744073709551615",
                "18446744073709551616",
                "1e-2147483648",
                "1e-2147483649",
                "0e9999999999",
                "true",
                "null",
                "\"not-a-number\"",
            ] {
                check::<i64>(DecimalSize::new(4, 2)?, text, rounding);
                check::<i128>(DecimalSize::new(38, 2)?, text, rounding);
                check::<databend_common_expression::types::i256>(
                    DecimalSize::new(76, 2)?,
                    text,
                    rounding,
                );
            }
        }
        Ok(())
    }

    #[test]
    fn test_sonic_field_decoder_matches_serde() -> Result<()> {
        let decimal = TableDataType::Decimal(
            databend_common_expression::types::decimal::DecimalDataType::Decimal128(
                DecimalSize::new(38, 18)?,
            ),
        );
        let cases = [
            (decimal.clone(), "165.6263"),
            (decimal.clone(), "-0.0003"),
            (decimal.clone(), "99999999999999.9999"),
            (decimal.clone(), "12345678901234.5678"),
            (decimal.clone(), "1.23456789012345678e13"),
            (decimal.clone(), "\"165.6263\""),
            (decimal.clone(), "1e99"),
            (decimal.clone(), "true"),
            (
                TableDataType::Decimal(
                    databend_common_expression::types::decimal::DecimalDataType::Decimal256(
                        DecimalSize::new(76, 18)?,
                    ),
                ),
                "1234567890123456789012345678901234567890123456789012345678.123456789012345678",
            ),
            (
                TableDataType::Array(Box::new(decimal)),
                "[165.6263,-0.0003,99999999999999.9999]",
            ),
            (TableDataType::Number(NumberDataType::Float64), "-0.0"),
            (TableDataType::Number(NumberDataType::Float64), "-0"),
            (TableDataType::Number(NumberDataType::Float64), "1E999"),
            (TableDataType::Number(NumberDataType::UInt64), "-0"),
            (TableDataType::Date, "-0"),
            (TableDataType::Timestamp, "-0"),
            (TableDataType::Bitmap, "-0"),
            (TableDataType::Number(NumberDataType::Float64), "5e-324"),
            (TableDataType::Number(NumberDataType::Int64), "165.6263"),
            (
                TableDataType::Number(NumberDataType::UInt64),
                "18446744073709551615",
            ),
            (
                TableDataType::Nullable(Box::new(TableDataType::String)),
                "null",
            ),
            (TableDataType::Boolean, "true"),
            (TableDataType::String, "1E+03"),
            (TableDataType::String, "\"汉字\\ntext\""),
            (TableDataType::Variant, "{\"n\":99999999999999.9999}"),
            (
                TableDataType::Map(Box::new(TableDataType::Tuple {
                    fields_name: vec!["key".to_string(), "value".to_string()],
                    fields_type: vec![TableDataType::String, TableDataType::String],
                })),
                "{\"a\":true,\"b\":1E+3}",
            ),
            (
                TableDataType::Tuple {
                    fields_name: vec!["a".to_string(), "b".to_string()],
                    fields_type: vec![
                        TableDataType::Number(NumberDataType::Int64),
                        TableDataType::String,
                    ],
                },
                "{\"B\":\"x\",\"A\":1}",
            ),
        ];
        for rounding in [false, true] {
            let settings = InputFormatSettings {
                is_rounding_mode: rounding,
                ..Default::default()
            };
            let decoder = FieldJsonAstDecoder::create(&settings, false);
            for (data_type, text) in &cases {
                let serde: serde_json::Value = serde_json::from_str(text).unwrap();
                let sonic = crate::json::parse_sonic(text.as_bytes()).unwrap();
                let mut expected = ColumnBuilder::with_capacity(&DataType::from(data_type), 1);
                let mut actual = ColumnBuilder::with_capacity(&DataType::from(data_type), 1);
                let expected_result =
                    decoder.read_field_with_data_type(&mut expected, &serde, data_type);
                let actual_result =
                    decoder.read_field_with_data_type(&mut actual, &sonic, data_type);
                assert_eq!(
                    expected_result.map_err(|e| e.to_string()),
                    actual_result.map_err(|e| e.to_string()),
                    "{data_type}: {text}"
                );
                assert_eq!(expected.build(), actual.build(), "{data_type}: {text}");
            }
        }
        Ok(())
    }

    #[test]
    fn test_wide_tuple_field_lookup() -> Result<()> {
        for width in [4, 16, 64] {
            let text = format!(
                "{{{}}}",
                (0..width)
                    .rev()
                    .map(|i| format!("\"K{i:04}\":{i}"))
                    .collect::<Vec<_>>()
                    .join(",")
            );
            let serde: serde_json::Value = serde_json::from_str(&text).unwrap();
            let sonic = crate::json::parse_sonic(text.as_bytes()).unwrap();
            for case_sensitive in [false, true] {
                let mut decoder =
                    FieldJsonAstDecoder::create(&InputFormatSettings::default(), false);
                decoder.ident_case_sensitive = case_sensitive;
                let data_type = TableDataType::Tuple {
                    fields_name: (0..width)
                        .map(|i| {
                            if case_sensitive {
                                format!("K{i:04}")
                            } else {
                                format!("k{i:04}")
                            }
                        })
                        .collect(),
                    fields_type: vec![TableDataType::Number(NumberDataType::Int64); width],
                };
                let mut expected = ColumnBuilder::with_capacity(&DataType::from(&data_type), 1);
                let mut actual = ColumnBuilder::with_capacity(&DataType::from(&data_type), 1);
                decoder.read_field_with_data_type(&mut expected, &serde, &data_type)?;
                decoder.read_field_with_data_type(&mut actual, &sonic, &data_type)?;
                let column = actual.build();
                assert_eq!(column, expected.build());
                let ScalarRef::Tuple(fields) = column.index(0).unwrap() else {
                    unreachable!()
                };
                for (i, value) in fields.iter().enumerate() {
                    assert_eq!(*value, ScalarRef::Number(NumberScalar::Int64(i as i64)));
                }
                let mut missing_type = data_type.clone();
                let TableDataType::Tuple { fields_name, .. } = &mut missing_type else {
                    unreachable!()
                };
                fields_name[width - 1] = "missing".to_string();
                let mut expected = ColumnBuilder::with_capacity(&DataType::from(&missing_type), 1);
                let mut actual = ColumnBuilder::with_capacity(&DataType::from(&missing_type), 1);
                let expected = decoder
                    .read_field_with_data_type(&mut expected, &serde, &missing_type)
                    .unwrap_err();
                let actual = decoder
                    .read_field_with_data_type(&mut actual, &sonic, &missing_type)
                    .unwrap_err();
                assert_eq!(actual.to_string(), expected.to_string());
                assert!(actual.to_string().contains("missing field missing"));
            }
        }
        Ok(())
    }

    #[test]
    fn test_read_tuple_object_by_field_name() -> Result<()> {
        let decoder = FieldJsonAstDecoder::create(&InputFormatSettings::default(), false);
        let data_type = TableDataType::Tuple {
            fields_name: vec!["a".to_string(), "b".to_string()],
            fields_type: vec![
                TableDataType::Number(NumberDataType::Int64),
                TableDataType::String,
            ],
        };
        let mut builder = ColumnBuilder::with_capacity(&DataType::from(&data_type), 1);
        let value = serde_json::json!({"b": "x", "a": 1});

        decoder.read_field_with_data_type(&mut builder, &value, &data_type)?;
        let column = builder.build();
        let ScalarRef::Tuple(fields) = column.index(0).unwrap() else {
            unreachable!()
        };
        assert_eq!(fields[0], ScalarRef::Number(NumberScalar::Int64(1)));
        assert_eq!(fields[1], ScalarRef::String("x"));
        Ok(())
    }

    #[test]
    fn test_read_tuple_object_by_unicode_field_name_case_insensitive() -> Result<()> {
        let decoder = FieldJsonAstDecoder::create(&InputFormatSettings::default(), false);
        let data_type = TableDataType::Tuple {
            fields_name: vec!["Å".to_string()],
            fields_type: vec![TableDataType::Number(NumberDataType::Int64)],
        };
        let mut builder = ColumnBuilder::with_capacity(&DataType::from(&data_type), 1);
        let value = serde_json::json!({"å": 1});

        decoder.read_field_with_data_type(&mut builder, &value, &data_type)?;
        let column = builder.build();
        let ScalarRef::Tuple(fields) = column.index(0).unwrap() else {
            unreachable!()
        };
        assert_eq!(fields[0], ScalarRef::Number(NumberScalar::Int64(1)));
        Ok(())
    }

    #[test]
    fn test_read_array_tuple_object_by_field_name() -> Result<()> {
        let decoder = FieldJsonAstDecoder::create(&InputFormatSettings::default(), false);
        let tuple_type = TableDataType::Tuple {
            fields_name: vec!["a".to_string(), "b".to_string()],
            fields_type: vec![
                TableDataType::Number(NumberDataType::Int64),
                TableDataType::String,
            ],
        };
        let data_type = TableDataType::Array(Box::new(tuple_type));
        let mut builder = ColumnBuilder::with_capacity(&DataType::from(&data_type), 1);
        let value = serde_json::json!([{"b": "x", "a": 1}]);

        decoder.read_field_with_data_type(&mut builder, &value, &data_type)?;
        let column = builder.build();
        let ScalarRef::Array(array) = column.index(0).unwrap() else {
            unreachable!()
        };
        let ScalarRef::Tuple(fields) = array.index(0).unwrap() else {
            unreachable!()
        };
        assert_eq!(fields[0], ScalarRef::Number(NumberScalar::Int64(1)));
        assert_eq!(fields[1], ScalarRef::String("x"));
        Ok(())
    }
}
