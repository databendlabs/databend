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
use common_exception::ErrorCode;
use common_exception::Result;
use common_expression::types::date::check_date;
use common_expression::types::number::Number;
use common_expression::types::timestamp::check_timestamp;
use common_expression::uniform_date;
use common_expression::ArrayDeserializer;
use common_expression::BooleanDeserializer;
use common_expression::DateDeserializer;
use common_expression::NullDeserializer;
use common_expression::NullableDeserializer;
use common_expression::NumberDeserializer;
use common_expression::StringDeserializer;
use common_expression::StructDeserializer;
use common_expression::TimestampDeserializer;
use common_expression::TypeDeserializer;
use common_expression::TypeDeserializerImpl;
use common_expression::VariantDeserializer;
use common_io::cursor_ext::BufferReadDateTimeExt;
use common_io::cursor_ext::ReadNumberExt;
use common_io::prelude::FormatSettings;
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
            TypeDeserializerImpl::Timestamp(c) => self.read_timestamp(c, value),
            TypeDeserializerImpl::String(c) => self.read_string(c, value),
            TypeDeserializerImpl::Array(c) => self.read_array(c, value),
            TypeDeserializerImpl::Struct(c) => self.read_struct(c, value),
            TypeDeserializerImpl::Variant(c) => self.read_variant(c, value),
        }
    }

    fn read_bool(&self, column: &mut BooleanDeserializer, value: &Value) -> Result<()> {
        match value {
            Value::Bool(v) => column.push(*v),
            _ => return Err(ErrorCode::BadBytes("Incorrect boolean value")),
        }
        Ok(())
    }

    fn read_null(&self, column: &mut NullDeserializer, _value: &Value) -> Result<()> {
        column.de_default();
        Ok(())
    }

    fn read_nullable(&self, column: &mut NullableDeserializer, value: &Value) -> Result<()> {
        match value {
            Value::Null => {
                column.validity.push(false);
                column.inner.de_default();
            }
            other => {
                self.read_field(column.inner.as_mut(), other)?;
                column.validity.push(true);
            }
        }
        Ok(())
    }

    fn read_int<T, P>(&self, column: &mut NumberDeserializer<T, P>, value: &Value) -> Result<()>
    where T: Number + Unmarshal<T> + StatBuffer + FromLexical {
        match value {
            Value::Number(v) => {
                let v = v.to_string();
                let mut reader = Cursor::new(v.as_bytes());
                let v: T = if !T::FLOATING {
                    reader.read_int_text()
                } else {
                    reader.read_float_text()
                }?;

                column.builder.push(v);
                Ok(())
            }
            _ => Err(ErrorCode::BadBytes("Incorrect json value, must be number")),
        }
    }

    fn read_float<T, P>(&self, column: &mut NumberDeserializer<T, P>, value: &Value) -> Result<()>
    where
        T: Number + Unmarshal<T> + StatBuffer + From<P>,
        P: Unmarshal<P> + StatBuffer + FromLexical,
    {
        match value {
            Value::Number(v) => {
                let v = v.to_string();
                let mut reader = Cursor::new(v.as_bytes());
                let v: P = if !T::FLOATING {
                    reader.read_int_text()
                } else {
                    reader.read_float_text()
                }?;

                column.builder.push(v.into());
                Ok(())
            }
            _ => Err(ErrorCode::BadBytes("Incorrect json value, must be number")),
        }
    }

    fn read_string(&self, column: &mut StringDeserializer, value: &Value) -> Result<()> {
        match value {
            Value::String(s) => {
                column.put_str(s.as_str());
                column.commit_row();
                Ok(())
            }
            _ => Err(ErrorCode::BadBytes("Incorrect json value, must be string")),
        }
    }

    fn read_date(&self, column: &mut DateDeserializer, value: &Value) -> Result<()> {
        match value {
            Value::String(v) => {
                let mut reader = Cursor::new(v.as_bytes());
                let date = reader.read_date_text(&self.timezone)?;
                let days = uniform_date(date);
                check_date(days as i64)?;
                column.builder.push(days);
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
                column.builder.push(micros.as_());
                Ok(())
            }
            Value::Number(number) => match number.as_i64() {
                Some(n) => {
                    check_timestamp(n)?;
                    column.builder.push(n);
                    Ok(())
                }
                None => Err(ErrorCode::BadArguments(
                    "Incorrect timestamp value, must be i64",
                )),
            },
            _ => Err(ErrorCode::BadBytes("Incorrect boolean value")),
        }
    }

    fn read_variant(&self, column: &mut VariantDeserializer, value: &Value) -> Result<()> {
        column.de_json(value, &FormatSettings::default())?;
        Ok(())
    }

    fn read_array(&self, column: &mut ArrayDeserializer, value: &Value) -> Result<()> {
        match value {
            Value::Array(vals) => {
                for val in vals {
                    self.read_field(column.inner.as_mut(), val)?;
                }
                column.add_offset(vals.len());
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
                for (inner, item) in column.inners.iter_mut().zip(obj.iter()) {
                    let (_, val) = item;
                    self.read_field(inner, val)?;
                }
                Ok(())
            }
            _ => Err(ErrorCode::BadBytes("Incorrect json value, must be object")),
        }
    }
}
