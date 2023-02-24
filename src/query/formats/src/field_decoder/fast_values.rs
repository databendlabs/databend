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
use std::collections::VecDeque;
use std::io::BufRead;
use std::io::Cursor;

use bstr::ByteSlice;
use chrono_tz::Tz;
use common_exception::ErrorCode;
use common_exception::Result;
use common_expression::read_decimal_with_size;
use common_expression::types::date::check_date;
use common_expression::types::decimal::Decimal;
use common_expression::types::number::Number;
use common_expression::types::timestamp::check_timestamp;
use common_expression::uniform_date;
use common_expression::ArrayDeserializer;
use common_expression::BooleanDeserializer;
use common_expression::DateDeserializer;
use common_expression::DecimalDeserializer;
use common_expression::NullDeserializer;
use common_expression::NullableDeserializer;
use common_expression::NumberDeserializer;
use common_expression::StringDeserializer;
use common_expression::StructDeserializer;
use common_expression::TimestampDeserializer;
use common_expression::TypeDeserializer;
use common_expression::TypeDeserializerImpl;
use common_expression::VariantDeserializer;
use common_io::constants::FALSE_BYTES_LOWER;
use common_io::constants::INF_BYTES_LOWER;
use common_io::constants::NAN_BYTES_LOWER;
use common_io::constants::NULL_BYTES_UPPER;
use common_io::constants::TRUE_BYTES_LOWER;
use common_io::cursor_ext::BufferReadDateTimeExt;
use common_io::cursor_ext::BufferReadStringExt;
use common_io::cursor_ext::ReadBytesExt;
use common_io::cursor_ext::ReadCheckPointExt;
use common_io::cursor_ext::ReadNumberExt;
use common_io::prelude::StatBuffer;
use lexical_core::FromLexical;
use micromarshal::Unmarshal;
use num::cast::AsPrimitive;

use crate::CommonSettings;
use crate::FieldDecoder;

#[derive(Clone)]
pub struct FastFieldDecoderValues {
    pub common_settings: CommonSettings,
}

impl FieldDecoder for FastFieldDecoderValues {
    fn as_any(&self) -> &dyn Any {
        self
    }
}

impl FastFieldDecoderValues {
    pub fn create_for_insert(timezone: Tz) -> Self {
        FastFieldDecoderValues {
            common_settings: CommonSettings {
                true_bytes: TRUE_BYTES_LOWER.as_bytes().to_vec(),
                false_bytes: FALSE_BYTES_LOWER.as_bytes().to_vec(),
                null_bytes: NULL_BYTES_UPPER.as_bytes().to_vec(),
                nan_bytes: NAN_BYTES_LOWER.as_bytes().to_vec(),
                inf_bytes: INF_BYTES_LOWER.as_bytes().to_vec(),
                timezone,
            },
        }
    }

    fn common_settings(&self) -> &CommonSettings {
        &self.common_settings
    }

    fn ignore_field_end<R: AsRef<[u8]>>(&self, reader: &mut Cursor<R>) -> bool {
        reader.ignore_white_spaces();
        matches!(reader.peek(), None | Some(',') | Some(')') | Some(']'))
    }

    fn match_bytes<R: AsRef<[u8]>>(&self, reader: &mut Cursor<R>, bs: &[u8]) -> bool {
        let pos = reader.checkpoint();
        if reader.ignore_bytes(bs) && self.ignore_field_end(reader) {
            true
        } else {
            reader.rollback(pos);
            false
        }
    }

    pub fn read_field<R: AsRef<[u8]>>(
        &self,
        column: &mut TypeDeserializerImpl,
        reader: &mut Cursor<R>,
        positions: &mut VecDeque<usize>,
    ) -> Result<()> {
        match column {
            TypeDeserializerImpl::Null(c) => self.read_null(c, reader),
            TypeDeserializerImpl::Nullable(c) => self.read_nullable(c, reader, positions),
            TypeDeserializerImpl::Boolean(c) => self.read_bool(c, reader),
            TypeDeserializerImpl::Int8(c) => self.read_int(c, reader),
            TypeDeserializerImpl::Int16(c) => self.read_int(c, reader),
            TypeDeserializerImpl::Int32(c) => self.read_int(c, reader),
            TypeDeserializerImpl::Int64(c) => self.read_int(c, reader),
            TypeDeserializerImpl::UInt8(c) => self.read_int(c, reader),
            TypeDeserializerImpl::UInt16(c) => self.read_int(c, reader),
            TypeDeserializerImpl::UInt32(c) => self.read_int(c, reader),
            TypeDeserializerImpl::UInt64(c) => self.read_int(c, reader),
            TypeDeserializerImpl::Float32(c) => self.read_float(c, reader),
            TypeDeserializerImpl::Float64(c) => self.read_float(c, reader),
            TypeDeserializerImpl::Decimal128(c) => self.read_decimal(c, reader),
            TypeDeserializerImpl::Decimal256(c) => self.read_decimal(c, reader),
            TypeDeserializerImpl::Date(c) => self.read_date(c, reader, positions),
            TypeDeserializerImpl::Timestamp(c) => self.read_timestamp(c, reader, positions),
            TypeDeserializerImpl::String(c) => self.read_string(c, reader, positions),
            TypeDeserializerImpl::Array(c) => self.read_array(c, reader, positions),
            TypeDeserializerImpl::Struct(c) => self.read_struct(c, reader, positions),
            TypeDeserializerImpl::Variant(c) => self.read_variant(c, reader, positions),
        }
    }

    fn read_bool<R: AsRef<[u8]>>(
        &self,
        column: &mut BooleanDeserializer,
        reader: &mut Cursor<R>,
    ) -> Result<()> {
        if self.match_bytes(reader, &self.common_settings().true_bytes) {
            column.push(true);
            Ok(())
        } else if self.match_bytes(reader, &self.common_settings().false_bytes) {
            column.push(false);
            Ok(())
        } else {
            let err_msg = format!(
                "Incorrect boolean value, expect {} or {}",
                self.common_settings().true_bytes.to_str().unwrap(),
                self.common_settings().false_bytes.to_str().unwrap()
            );
            Err(ErrorCode::BadBytes(err_msg))
        }
    }

    fn read_null<R: AsRef<[u8]>>(
        &self,
        column: &mut NullDeserializer,
        _reader: &mut Cursor<R>,
    ) -> Result<()> {
        column.de_default();
        Ok(())
    }

    fn read_nullable<R: AsRef<[u8]>>(
        &self,
        column: &mut NullableDeserializer,
        reader: &mut Cursor<R>,
        positions: &mut VecDeque<usize>,
    ) -> Result<()> {
        if reader.eof() {
            column.de_default();
        } else if reader.ignore_bytes(b"NULL") || reader.ignore_bytes(b"null") {
            column.de_default();
            return Ok(());
        } else {
            self.read_field(column.inner.as_mut(), reader, positions)?;
            column.validity.push(true);
        }
        Ok(())
    }

    fn read_int<T, P, R: AsRef<[u8]>>(
        &self,
        column: &mut NumberDeserializer<T, P>,
        reader: &mut Cursor<R>,
    ) -> Result<()>
    where
        T: Number + Unmarshal<T> + StatBuffer + From<P>,
        P: Unmarshal<P> + StatBuffer + FromLexical,
    {
        let v: P = reader.read_int_text()?;
        column.builder.push(v.into());
        Ok(())
    }

    fn read_float<T, P, R: AsRef<[u8]>>(
        &self,
        column: &mut NumberDeserializer<T, P>,
        reader: &mut Cursor<R>,
    ) -> Result<()>
    where
        T: Number + Unmarshal<T> + StatBuffer + From<P>,
        P: Unmarshal<P> + StatBuffer + FromLexical,
    {
        let v: P = reader.read_float_text()?;
        column.builder.push(v.into());
        Ok(())
    }

    fn read_decimal<R: AsRef<[u8]>, D: Decimal>(
        &self,
        column: &mut DecimalDeserializer<D>,
        reader: &mut Cursor<R>,
    ) -> Result<()>
where {
        let buf = reader.remaining_slice();
        let (n, n_read) = read_decimal_with_size(buf, column.size, false)?;
        column.values.push(n);
        reader.consume(n_read);
        Ok(())
    }

    fn read_string_inner<R: AsRef<[u8]>>(
        &self,
        reader: &mut Cursor<R>,
        out_buf: &mut Vec<u8>,
        positions: &mut VecDeque<usize>,
    ) -> Result<()> {
        reader.fast_read_quoted_text(out_buf, positions)?;
        Ok(())
    }

    fn read_string<R: AsRef<[u8]>>(
        &self,
        column: &mut StringDeserializer,
        reader: &mut Cursor<R>,
        positions: &mut VecDeque<usize>,
    ) -> Result<()> {
        self.read_string_inner(reader, &mut column.data, positions)?;
        column.commit_row();
        Ok(())
    }

    fn read_date<R: AsRef<[u8]>>(
        &self,
        column: &mut DateDeserializer,
        reader: &mut Cursor<R>,
        positions: &mut VecDeque<usize>,
    ) -> Result<()> {
        column.buffer.clear();
        self.read_string_inner(reader, &mut column.buffer, positions)?;
        let mut buffer_readr = Cursor::new(&column.buffer);
        let date = buffer_readr.read_date_text(&self.common_settings().timezone)?;
        let days = uniform_date(date);
        check_date(days as i64)?;
        column.builder.push(days);
        Ok(())
    }

    fn read_timestamp<R: AsRef<[u8]>>(
        &self,
        column: &mut TimestampDeserializer,
        reader: &mut Cursor<R>,
        positions: &mut VecDeque<usize>,
    ) -> Result<()> {
        column.buffer.clear();
        self.read_string_inner(reader, &mut column.buffer, positions)?;
        let mut buffer_readr = Cursor::new(&column.buffer);
        let ts = buffer_readr.read_timestamp_text(&self.common_settings().timezone)?;
        if !buffer_readr.eof() {
            let data = column.buffer.to_str().unwrap_or("not utf8");
            let msg = format!(
                "fail to deserialize timestamp, unexpected end at pos {} of {}",
                buffer_readr.position(),
                data
            );
            return Err(ErrorCode::BadBytes(msg));
        }
        let micros = ts.timestamp_micros();
        check_timestamp(micros)?;
        column.builder.push(micros.as_());
        Ok(())
    }

    fn read_array<R: AsRef<[u8]>>(
        &self,
        column: &mut ArrayDeserializer,
        reader: &mut Cursor<R>,
        positions: &mut VecDeque<usize>,
    ) -> Result<()> {
        reader.must_ignore_byte(b'[')?;
        let mut idx = 0;
        loop {
            let _ = reader.ignore_white_spaces();
            if reader.ignore_byte(b']') {
                break;
            }
            if idx != 0 {
                reader.must_ignore_byte(b',')?;
            }
            let _ = reader.ignore_white_spaces();
            self.read_field(column.inner.as_mut(), reader, positions)?;
            idx += 1;
        }

        column.add_offset(idx);
        Ok(())
    }

    fn read_struct<R: AsRef<[u8]>>(
        &self,
        column: &mut StructDeserializer,
        reader: &mut Cursor<R>,
        positions: &mut VecDeque<usize>,
    ) -> Result<()> {
        reader.must_ignore_byte(b'(')?;
        for (idx, inner) in column.inners.iter_mut().enumerate() {
            let _ = reader.ignore_white_spaces();
            if idx != 0 {
                reader.must_ignore_byte(b',')?;
            }
            let _ = reader.ignore_white_spaces();
            self.read_field(inner, reader, positions)?;
        }
        reader.must_ignore_byte(b')')?;
        Ok(())
    }

    fn read_variant<R: AsRef<[u8]>>(
        &self,
        column: &mut VariantDeserializer,
        reader: &mut Cursor<R>,
        positions: &mut VecDeque<usize>,
    ) -> Result<()> {
        self.read_string_inner(reader, &mut column.builder.data, positions)?;
        column.builder.commit_row();
        Ok(())
    }
}
