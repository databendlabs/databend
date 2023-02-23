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

use std::io::BufRead;
use std::io::Cursor;

use bstr::ByteSlice;
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
use common_io::cursor_ext::BufferReadDateTimeExt;
use common_io::cursor_ext::ReadBytesExt;
use common_io::cursor_ext::ReadCheckPointExt;
use common_io::cursor_ext::ReadNumberExt;
use common_io::prelude::StatBuffer;
use lexical_core::FromLexical;
use micromarshal::Unmarshal;
use num::cast::AsPrimitive;

use crate::field_decoder::FieldDecoder;
use crate::CommonSettings;

pub trait FieldDecoderRowBased: FieldDecoder {
    fn common_settings(&self) -> &CommonSettings;

    fn ignore_field_end<R: AsRef<[u8]>>(&self, reader: &mut Cursor<R>) -> bool;

    fn match_bytes<R: AsRef<[u8]>>(&self, reader: &mut Cursor<R>, bs: &[u8]) -> bool {
        let pos = reader.checkpoint();
        if reader.ignore_bytes(bs) && self.ignore_field_end(reader) {
            true
        } else {
            reader.rollback(pos);
            false
        }
    }

    fn read_field<R: AsRef<[u8]>>(
        &self,
        column: &mut TypeDeserializerImpl,
        reader: &mut Cursor<R>,
        raw: bool,
    ) -> Result<()> {
        match column {
            TypeDeserializerImpl::Null(c) => self.read_null(c, reader, raw),
            TypeDeserializerImpl::Nullable(c) => self.read_nullable(c, reader, raw),
            TypeDeserializerImpl::Boolean(c) => self.read_bool(c, reader, raw),
            TypeDeserializerImpl::Int8(c) => self.read_int(c, reader, raw),
            TypeDeserializerImpl::Int16(c) => self.read_int(c, reader, raw),
            TypeDeserializerImpl::Int32(c) => self.read_int(c, reader, raw),
            TypeDeserializerImpl::Int64(c) => self.read_int(c, reader, raw),
            TypeDeserializerImpl::UInt8(c) => self.read_int(c, reader, raw),
            TypeDeserializerImpl::UInt16(c) => self.read_int(c, reader, raw),
            TypeDeserializerImpl::UInt32(c) => self.read_int(c, reader, raw),
            TypeDeserializerImpl::UInt64(c) => self.read_int(c, reader, raw),
            TypeDeserializerImpl::Float32(c) => self.read_float(c, reader, raw),
            TypeDeserializerImpl::Float64(c) => self.read_float(c, reader, raw),
            TypeDeserializerImpl::Decimal128(c) => self.read_decimal(c, reader, raw),
            TypeDeserializerImpl::Decimal256(c) => self.read_decimal(c, reader, raw),
            TypeDeserializerImpl::Date(c) => self.read_date(c, reader, raw),
            TypeDeserializerImpl::Timestamp(c) => self.read_timestamp(c, reader, raw),
            TypeDeserializerImpl::String(c) => self.read_string(c, reader, raw),
            TypeDeserializerImpl::Array(c) => self.read_array(c, reader, raw),
            TypeDeserializerImpl::Struct(c) => self.read_struct(c, reader, raw),
            TypeDeserializerImpl::Variant(c) => self.read_variant(c, reader, raw),
        }
    }

    fn read_bool<R: AsRef<[u8]>>(
        &self,
        column: &mut BooleanDeserializer,
        reader: &mut Cursor<R>,
        _raw: bool,
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
        _raw: bool,
    ) -> Result<()> {
        column.de_default();
        Ok(())
    }

    fn read_nullable<R: AsRef<[u8]>>(
        &self,
        column: &mut NullableDeserializer,
        reader: &mut Cursor<R>,
        raw: bool,
    ) -> Result<()> {
        if reader.eof() {
            column.de_default();
        } else if self.match_bytes(reader, &self.common_settings().null_bytes)
            && self.ignore_field_end(reader)
        {
            column.de_default();
            return Ok(());
        } else {
            self.read_field(column.inner.as_mut(), reader, raw)?;
            column.validity.push(true);
        }
        Ok(())
    }

    fn read_string_inner<R: AsRef<[u8]>>(
        &self,
        reader: &mut Cursor<R>,
        out_buf: &mut Vec<u8>,
        raw: bool,
    ) -> Result<()>;

    fn read_int<T, P, R: AsRef<[u8]>>(
        &self,
        column: &mut NumberDeserializer<T, P>,
        reader: &mut Cursor<R>,
        _raw: bool,
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
        _raw: bool,
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
        _raw: bool,
    ) -> Result<()> {
        let buf = reader.remaining_slice();
        let (n, n_read) = read_decimal_with_size(buf, column.size, false)?;
        column.values.push(n);
        reader.consume(n_read);
        Ok(())
    }

    fn read_string<R: AsRef<[u8]>>(
        &self,
        column: &mut StringDeserializer,
        reader: &mut Cursor<R>,
        _raw: bool,
    ) -> Result<()>;

    fn read_date<R: AsRef<[u8]>>(
        &self,
        column: &mut DateDeserializer,
        reader: &mut Cursor<R>,
        raw: bool,
    ) -> Result<()> {
        column.buffer.clear();
        self.read_string_inner(reader, &mut column.buffer, raw)?;
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
        raw: bool,
    ) -> Result<()> {
        column.buffer.clear();
        self.read_string_inner(reader, &mut column.buffer, raw)?;
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

    fn read_variant<R: AsRef<[u8]>>(
        &self,
        column: &mut VariantDeserializer,
        reader: &mut Cursor<R>,
        raw: bool,
    ) -> Result<()> {
        self.read_string_inner(reader, &mut column.builder.data, raw)?;
        column.builder.commit_row();
        Ok(())
    }

    fn read_array<R: AsRef<[u8]>>(
        &self,
        column: &mut ArrayDeserializer,
        reader: &mut Cursor<R>,
        raw: bool,
    ) -> Result<()>;

    fn read_struct<R: AsRef<[u8]>>(
        &self,
        column: &mut StructDeserializer,
        reader: &mut Cursor<R>,
        raw: bool,
    ) -> Result<()>;
}
