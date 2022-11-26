// Copyright 2022 Datafuse Labs.
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
use std::io::Cursor;

use chrono_tz::Tz;
use common_datavalues::check_date;
use common_datavalues::check_timestamp;
use common_datavalues::deserializations::ArrayDeserializer;
use common_datavalues::deserializations::BooleanDeserializer;
use common_datavalues::deserializations::DateDeserializer;
use common_datavalues::deserializations::NullableDeserializer;
use common_datavalues::deserializations::NumberDeserializer;
use common_datavalues::deserializations::StringDeserializer;
use common_datavalues::deserializations::StructDeserializer;
use common_datavalues::deserializations::TimestampDeserializer;
use common_datavalues::deserializations::VariantDeserializer;
use common_datavalues::uniform_date;
use common_datavalues::ArrayValue;
use common_datavalues::MutableColumn;
use common_datavalues::NullDeserializer;
use common_datavalues::PrimitiveType;
use common_datavalues::StructValue;
use common_datavalues::TypeDeserializer;
use common_datavalues::TypeDeserializerImpl;
use common_datavalues::VariantValue;
use common_exception::ErrorCode;
use common_exception::Result;
use common_io::cursor_ext::BufferReadDateTimeExt;
use common_io::cursor_ext::ReadNumberExt;
use common_io::prelude::StatBuffer;
use lexical_core::FromLexical;
use micromarshal::Unmarshal;
use num::cast::AsPrimitive;
use serde_json::Value;

use crate::FieldDecoder;
use crate::FileFormatOptionsExt;

pub struct FieldJsonAstDecoder {
    pub timezone: Tz,
    pub ident_case_sensitive: bool,
}

impl FieldDecoder for FieldJsonAstDecoder {
    fn as_any(&self) -> &dyn Any {
        self
    }
}

impl FieldJsonAstDecoder {
    pub fn create(options: &FileFormatOptionsExt) -> Self {
        FieldJsonAstDecoder {
            timezone: options.timezone,
            ident_case_sensitive: options.ident_case_sensitive,
        }
    }

    pub fn read_field(&self, column: &mut TypeDeserializerImpl, value: &Value) -> Result<()> {
        match column {
            TypeDeserializerImpl::Null(c) => self.read_null(c, value),
            TypeDeserializerImpl::Nullable(c) => self.read_nullable(c, value),
            TypeDeserializerImpl::Boolean(c) => self.read_bool(c, value),
            TypeDeserializerImpl::Int8(c) => self.read_int(c, value),
            TypeDeserializerImpl::Int16(c) => self.read_int(c, value),
            TypeDeserializerImpl::Int32(c) => self.read_int(c, value),
            TypeDeserializerImpl::Int64(c) => self.read_int(c, value),
            TypeDeserializerImpl::UInt8(c) => self.read_int(c, value),
            TypeDeserializerImpl::UInt16(c) => self.read_int(c, value),
            TypeDeserializerImpl::UInt32(c) => self.read_int(c, value),
            TypeDeserializerImpl::UInt64(c) => self.read_int(c, value),
            TypeDeserializerImpl::Float32(c) => self.read_float(c, value),
            TypeDeserializerImpl::Float64(c) => self.read_float(c, value),
            TypeDeserializerImpl::Date(c) => self.read_date(c, value),
            TypeDeserializerImpl::Interval(c) => self.read_date(c, value),
            TypeDeserializerImpl::Timestamp(c) => self.read_timestamp(c, value),
            TypeDeserializerImpl::String(c) => self.read_string(c, value),
            TypeDeserializerImpl::Array(c) => self.read_array(c, value),
            TypeDeserializerImpl::Struct(c) => self.read_struct(c, value),
            TypeDeserializerImpl::Variant(c) => self.read_variant(c, value),
        }
    }

    fn read_bool(&self, column: &mut BooleanDeserializer, value: &Value) -> Result<()> {
        match value {
            Value::Bool(v) => column.builder.append_value(*v),
            _ => return Err(ErrorCode::BadBytes("Incorrect boolean value")),
        }
        Ok(())
    }

    fn read_null(&self, column: &mut NullDeserializer, _value: &Value) -> Result<()> {
        column.builder.append_default();
        Ok(())
    }

    fn read_nullable(&self, column: &mut NullableDeserializer, value: &Value) -> Result<()> {
        match value {
            Value::Null => {
                column.bitmap.push(false);
                column.inner.de_default();
            }
            other => {
                self.read_field(&mut column.inner, other)?;
                column.bitmap.push(true);
            }
        }
        Ok(())
    }

    fn read_int<T>(&self, column: &mut NumberDeserializer<T>, value: &Value) -> Result<()>
    where T: PrimitiveType + Unmarshal<T> + StatBuffer + FromLexical {
        match value {
            Value::Number(v) => {
                let v = v.to_string();
                let mut reader = Cursor::new(v.as_bytes());
                let v: T = if !T::FLOATING {
                    reader.read_int_text()
                } else {
                    reader.read_float_text()
                }?;

                column.builder.append_value(v);
                Ok(())
            }
            _ => Err(ErrorCode::BadBytes("Incorrect json value, must be number")),
        }
    }

    fn read_float<T>(&self, column: &mut NumberDeserializer<T>, value: &Value) -> Result<()>
    where T: PrimitiveType + Unmarshal<T> + StatBuffer + FromLexical {
        match value {
            Value::Number(v) => {
                let v = v.to_string();
                let mut reader = Cursor::new(v.as_bytes());
                let v: T = if !T::FLOATING {
                    reader.read_int_text()
                } else {
                    reader.read_float_text()
                }?;

                column.builder.append_value(v);
                Ok(())
            }
            _ => Err(ErrorCode::BadBytes("Incorrect json value, must be number")),
        }
    }

    fn read_string(&self, column: &mut StringDeserializer, value: &Value) -> Result<()> {
        match value {
            Value::String(s) => {
                column.builder.append_value(s);
                Ok(())
            }
            _ => Err(ErrorCode::BadBytes("Incorrect json value, must be string")),
        }
    }

    fn read_date<T>(&self, column: &mut DateDeserializer<T>, value: &Value) -> Result<()>
    where
        i32: AsPrimitive<T>,
        T: PrimitiveType,
        T: Unmarshal<T> + StatBuffer + FromLexical,
    {
        match value {
            Value::String(v) => {
                let mut reader = Cursor::new(v.as_bytes());
                let date = reader.read_date_text(&self.timezone)?;
                let days = uniform_date(date);
                check_date(days.as_i32())?;
                column.builder.append_value(days);
                Ok(())
            }
            _ => Err(ErrorCode::BadBytes("Incorrect boolean value")),
        }
    }

    fn read_timestamp(&self, column: &mut TimestampDeserializer, value: &Value) -> Result<()> {
        match value {
            Value::String(v) => {
                let v = v.clone();
                let mut reader = Cursor::new(v.as_bytes());
                let ts = reader.read_timestamp_text(&self.timezone)?;

                let micros = ts.timestamp_micros();
                check_timestamp(micros)?;
                column.builder.append_value(micros.as_());
                Ok(())
            }
            _ => Err(ErrorCode::BadBytes("Incorrect boolean value")),
        }
    }

    fn read_variant(&self, column: &mut VariantDeserializer, value: &Value) -> Result<()> {
        let val = VariantValue::from(value);
        column.memory_size += val.calculate_memory_size();
        column.builder.append_value(val);
        Ok(())
    }

    fn read_array(&self, column: &mut ArrayDeserializer, value: &Value) -> Result<()> {
        match value {
            Value::Array(vals) => {
                for val in vals {
                    self.read_field(&mut column.inner, val)?;
                }
                let mut values = Vec::with_capacity(vals.len());
                for _ in 0..vals.len() {
                    let value = column.inner.pop_data_value().unwrap();
                    values.push(value);
                }
                values.reverse();
                column.builder.append_value(ArrayValue::new(values));
                Ok(())
            }
            _ => Err(ErrorCode::BadBytes("Incorrect json value, must be array")),
        }
    }

    fn read_struct(&self, column: &mut StructDeserializer, value: &Value) -> Result<()> {
        match value {
            Value::Object(obj) => {
                if column.inners.len() != obj.len() {
                    return Err(ErrorCode::BadBytes(format!(
                        "Incorrect json value, expect {} values, but get {} values",
                        column.inners.len(),
                        obj.len()
                    )));
                }
                let mut values = Vec::with_capacity(column.inners.len());
                for (inner, item) in column.inners.iter_mut().zip(obj.iter()) {
                    let (_, val) = item;
                    self.read_field(inner, val)?;
                    values.push(inner.pop_data_value()?);
                }
                column.builder.append_value(StructValue::new(values));
                Ok(())
            }
            _ => Err(ErrorCode::BadBytes("Incorrect json value, must be object")),
        }
    }
}
