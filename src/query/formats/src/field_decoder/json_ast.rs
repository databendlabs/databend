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

use databend_common_column::types::months_days_micros;
use databend_common_column::types::timestamp_tz;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::ColumnBuilder;
use databend_common_expression::TableDataType;
use databend_common_expression::serialize::read_decimal_from_json;
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
use jiff::tz::TimeZone;
use lexical_core::FromLexical;
use num_traits::NumCast;
use serde_json::Value;

use crate::FieldDecoder;

pub struct FieldJsonAstDecoder {
    jiff_timezone: TimeZone,
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
            jiff_timezone: settings.jiff_timezone.clone(),
            ident_case_sensitive: false,
            is_select,
            is_rounding_mode: settings.is_rounding_mode,
            enable_auto_detect_datetime_format: settings.enable_auto_detect_datetime_format,
        }
    }

    pub fn read_field_with_data_type(
        &self,
        column: &mut ColumnBuilder,
        value: &Value,
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
                Some(array) if array.is_empty() => {
                    *len += 1;
                    Ok(())
                }
                _ => Err(ErrorCode::BadBytes("Incorrect empty array value")),
            },
            ColumnBuilder::EmptyMap { len } => match value.as_object() {
                Some(array) if array.is_empty() => {
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

    fn read_bool(&self, column: &mut MutableBitmap, value: &Value) -> Result<()> {
        match value {
            Value::Bool(v) => column.push(*v),
            _ => return Err(ErrorCode::BadBytes("Incorrect boolean value")),
        }
        Ok(())
    }

    fn read_null(&self, len: &mut usize, _value: &Value) -> Result<()> {
        *len += 1;
        Ok(())
    }

    fn read_nullable(
        &self,
        column: &mut NullableColumnBuilder<AnyType>,
        value: &Value,
        inner_type: &TableDataType,
    ) -> Result<()> {
        match value {
            Value::Null => {
                column.push_null();
            }
            other => {
                self.read_field_with_data_type(&mut column.builder, other, inner_type)?;
                column.validity.push(true);
            }
        }
        Ok(())
    }

    fn read_int<T>(&self, column: &mut Vec<T>, value: &Value) -> Result<()>
    where
        T: Number + From<T::Native>,
        T::Native: FromLexical + NumCast,
    {
        match value {
            Value::Number(v) => {
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

    fn read_uint<T>(&self, column: &mut Vec<T>, value: &Value) -> Result<()>
    where
        T: Number + From<T::Native>,
        T::Native: FromLexical + NumCast,
    {
        match value {
            Value::Number(v) => {
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

    fn read_float<T>(&self, column: &mut Vec<T>, value: &Value) -> Result<()>
    where
        T: Number + From<T::Native>,
        T::Native: FromLexical + NumCast,
    {
        match value {
            Value::Number(v) => {
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
        value: &Value,
    ) -> Result<()> {
        column.push(read_decimal_from_json(value, size)?);
        Ok(())
    }

    fn read_string(&self, column: &mut StringColumnBuilder, value: &Value) -> Result<()> {
        match value {
            Value::String(s) => {
                column.put_str(s.as_str());
            }
            Value::Bool(v) => {
                if *v {
                    column.put_str("true");
                } else {
                    column.put_str("false");
                }
            }
            Value::Number(n) => {
                column.put_str(n.to_string().as_str());
            }
            Value::Null => {
                column.put_str("null");
            }
            _ => return Err(ErrorCode::BadBytes("Incorrect json value, must be string")),
        }
        column.commit_row();
        Ok(())
    }

    fn read_date(&self, column: &mut Vec<i32>, value: &Value) -> Result<()> {
        match value {
            Value::String(v) => {
                let days = parse_date_with_auto(
                    v,
                    &self.jiff_timezone,
                    self.enable_auto_detect_datetime_format,
                )?;
                column.push(days);
                Ok(())
            }
            Value::Number(number) => match number.as_i64() {
                Some(n) => {
                    column.push(clamp_date(n));
                    Ok(())
                }
                None => Err(ErrorCode::BadArguments("Incorrect date value")),
            },
            _ => Err(ErrorCode::BadBytes("Incorrect date value")),
        }
    }

    fn read_timestamp(&self, column: &mut Vec<i64>, value: &Value) -> Result<()> {
        match value {
            Value::String(v) => {
                let micros = parse_timestamp_with_auto(
                    v,
                    &self.jiff_timezone,
                    self.enable_auto_detect_datetime_format,
                )?;
                column.push(micros);
                Ok(())
            }
            Value::Number(number) => match number.as_i64() {
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

    fn read_timestamp_tz(&self, column: &mut Vec<timestamp_tz>, value: &Value) -> Result<()> {
        match value {
            Value::String(s) => {
                let ts_tz = parse_timestamp_tz_with_auto(
                    s,
                    &self.jiff_timezone,
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

    fn read_interval(&self, column: &mut Vec<months_days_micros>, value: &Value) -> Result<()> {
        match value {
            Value::String(s) => {
                let i = Interval::from_string(s)?;
                column.push(months_days_micros::new(i.months, i.days, i.micros));
                Ok(())
            }
            _ => Err(ErrorCode::BadBytes(
                "Incorrect interval value, must be string",
            )),
        }
    }

    fn read_bitmap(&self, column: &mut BinaryColumnBuilder, value: &Value) -> Result<()> {
        match value {
            Value::String(v) => {
                let rb = parse_bitmap(v.as_bytes())?;
                rb.serialize_into(&mut column.data).unwrap();
                column.commit_row();
                Ok(())
            }
            Value::Number(number) => match number.as_u64() {
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

    fn read_variant(&self, column: &mut BinaryColumnBuilder, value: &Value) -> Result<()> {
        let v = jsonb::Value::from(value);
        v.write_to_vec(&mut column.data);
        column.commit_row();
        Ok(())
    }

    fn read_geometry(&self, column: &mut BinaryColumnBuilder, value: &Value) -> Result<()> {
        match value {
            Value::String(v) => {
                let geom = geometry_from_ewkt(v, None)?;
                column.put_slice(&geom);
                column.commit_row();
                Ok(())
            }
            _ => Err(ErrorCode::BadBytes("Incorrect Geometry value")),
        }
    }

    fn read_geography(&self, column: &mut BinaryColumnBuilder, value: &Value) -> Result<()> {
        match value {
            Value::String(v) => {
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
        value: &Value,
        inner_type: &TableDataType,
    ) -> Result<()> {
        match value {
            Value::Array(vals) => {
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
        value: &Value,
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
        match value {
            Value::Object(obj) => {
                for (key, val) in obj.iter() {
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
        value: &Value,
        tuple_fields: Option<(&[String], &[TableDataType])>,
    ) -> Result<()> {
        match value {
            Value::Object(obj) => {
                if fields.len() != obj.len() {
                    return Err(ErrorCode::BadBytes(format!(
                        "Incorrect json value, expect {} values, but get {} values",
                        fields.len(),
                        obj.len()
                    )));
                }
                if let Some((fields_name, fields_type)) = tuple_fields {
                    for (idx, (field, field_name)) in
                        fields.iter_mut().zip(fields_name.iter()).enumerate()
                    {
                        let val = object_value(obj, field_name, self.ident_case_sensitive)
                            .ok_or_else(|| {
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
            Value::Array(values) => {
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

    fn read_vector(&self, column: &mut VectorColumnBuilder, value: &Value) -> Result<()> {
        match value {
            Value::Array(vals) => {
                let dimension = column.dimension();
                if dimension != vals.len() {
                    return Err(ErrorCode::BadBytes(format!(
                        "Incorrect vector value, dimension must be {}",
                        dimension
                    )));
                }
                let mut values = Vec::with_capacity(dimension);
                for val in vals {
                    if let Value::Number(num) = val {
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

fn object_value<'a>(
    object: &'a serde_json::Map<String, Value>,
    field_name: &str,
    case_sensitive: bool,
) -> Option<&'a Value> {
    if case_sensitive {
        object.get(field_name)
    } else {
        let field_name = field_name.to_lowercase();
        object
            .iter()
            .find_map(|(key, value)| (key.to_lowercase() == field_name).then_some(value))
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
