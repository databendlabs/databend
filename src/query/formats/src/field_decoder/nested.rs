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

use std::collections::HashSet;
use std::io::BufRead;
use std::io::Cursor;

use bstr::ByteSlice;
use databend_common_column::types::months_days_micros;
use databend_common_column::types::timestamp_tz;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_exception::ToErrorCode;
use databend_common_expression::ColumnBuilder;
use databend_common_expression::serialize::read_decimal_with_size;
use databend_common_expression::serialize::uniform_date;
use databend_common_expression::types::AnyType;
use databend_common_expression::types::MutableBitmap;
use databend_common_expression::types::NumberColumnBuilder;
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
use databend_common_expression::types::vector::VectorColumnBuilder;
use databend_common_expression::with_decimal_type;
use databend_common_expression::with_number_mapped_type;
use databend_common_io::Interval;
use databend_common_io::constants::NULL_BYTES_LOWER;
use databend_common_io::constants::NULL_BYTES_UPPER;
use databend_common_io::cursor_ext::BufferReadDateTimeExt;
use databend_common_io::cursor_ext::BufferReadStringExt;
use databend_common_io::cursor_ext::ReadBytesExt;
use databend_common_io::cursor_ext::ReadCheckPointExt;
use databend_common_io::cursor_ext::ReadNumberExt;
use databend_common_io::geography::geography_from_ewkt_bytes;
use databend_common_io::parse_bitmap;
use databend_common_io::parse_bytes_to_ewkb;
use databend_common_io::prelude::InputFormatSettings;
use jsonb::parse_owned_jsonb_with_buf;
use lexical_core::FromLexical;
use serde::Deserialize;
use serde_json::Deserializer;
use serde_json::value::RawValue;

use crate::InputCommonSettings;
use crate::binary::decode_binary;
use crate::field_decoder::common::read_timestamp;
use crate::field_decoder::common::read_timestamp_tz;

#[derive(Clone)]
pub struct NestedValues {
    pub common_settings: InputCommonSettings,
}

impl NestedValues {
    /// Consider map/tuple/array as a private object format like JSON.
    /// Currently, we assume it as a fixed format, embed it in "strings" of other formats.
    /// So we can use the same code to encode/decode in clients.
    /// It maybe needs to be configurable in the future,
    /// to read data from other DB which also support map/tuple/array.
    pub fn create(settings: InputFormatSettings) -> Self {
        NestedValues {
            common_settings: InputCommonSettings {
                null_if: vec![
                    NULL_BYTES_UPPER.as_bytes().to_vec(),
                    NULL_BYTES_LOWER.as_bytes().to_vec(),
                ],
                settings,
                binary_format: Default::default(),
            },
        }
    }
}

impl NestedValues {
    fn match_bytes<R: AsRef<[u8]>>(&self, reader: &mut Cursor<R>, bs: &[u8]) -> bool {
        let pos = reader.checkpoint();
        if reader.ignore_bytes(bs) {
            true
        } else {
            reader.rollback(pos);
            false
        }
    }

    fn match_byte<R: AsRef<[u8]>>(&self, reader: &mut Cursor<R>, bs: u8) -> bool {
        let pos = reader.checkpoint();
        if reader.ignore_byte(bs) {
            true
        } else {
            reader.rollback(pos);
            false
        }
    }

    fn read_field<R: AsRef<[u8]>>(
        &self,
        column: &mut ColumnBuilder,
        reader: &mut Cursor<R>,
    ) -> Result<()> {
        match column {
            ColumnBuilder::Null { len } => {
                *len += 1;
                Ok(())
            }
            ColumnBuilder::Nullable(c) => self.read_nullable(c, reader),
            ColumnBuilder::Boolean(c) => self.read_bool(c, reader),
            ColumnBuilder::Number(c) => with_number_mapped_type!(|NUM_TYPE| match c {
                NumberColumnBuilder::NUM_TYPE(c) => {
                    if NUM_TYPE::FLOATING {
                        self.read_float(c, reader)
                    } else {
                        self.read_int(c, reader)
                    }
                }
            }),
            ColumnBuilder::Decimal(c) => with_decimal_type!(|DECIMAL_TYPE| match c {
                DecimalColumnBuilder::DECIMAL_TYPE(c, size) => self.read_decimal(c, *size, reader),
            }),
            ColumnBuilder::Date(c) => self.read_date(c, reader),
            ColumnBuilder::Interval(c) => self.read_interval(c, reader),
            ColumnBuilder::Timestamp(c) => self.read_timestamp(c, reader),
            ColumnBuilder::TimestampTz(c) => self.read_timestamp_tz(c, reader),
            ColumnBuilder::Binary(c) => self.read_binary(c, reader),
            ColumnBuilder::String(c) => self.read_string(c, reader),
            ColumnBuilder::Array(c) => self.read_array(c, reader),
            ColumnBuilder::Map(c) => self.read_map(c, reader),
            ColumnBuilder::Bitmap(c) => self.read_bitmap(c, reader),
            ColumnBuilder::Tuple(fields) => self.read_tuple(fields, reader),
            ColumnBuilder::Variant(c) => self.read_variant(c, reader),
            ColumnBuilder::Geometry(c) => self.read_geometry(c, reader),
            ColumnBuilder::Geography(c) => self.read_geography(c, reader),
            ColumnBuilder::Vector(c) => self.read_vector(c, reader),
            ColumnBuilder::EmptyArray { .. } => {
                unreachable!("EmptyArray")
            }
            ColumnBuilder::EmptyMap { .. } => {
                unreachable!("EmptyMap")
            }
            ColumnBuilder::Opaque(_) => Err(ErrorCode::Unimplemented(
                "Opaque type not supported in nested",
            )),
        }
    }

    fn read_bool<R: AsRef<[u8]>>(
        &self,
        column: &mut MutableBitmap,
        reader: &mut Cursor<R>,
    ) -> Result<()> {
        if self.match_byte(reader, b'1') {
            column.push(true);
        } else if self.match_byte(reader, b'0') {
            column.push(false);
        } else if self.match_bytes(reader, b"true") {
            column.push(true);
        } else if self.match_bytes(reader, b"false") {
            column.push(false);
        } else {
            return Err(ErrorCode::BadBytes(
                "Incorrect boolean value, expect one of 0/1/false/true",
            ));
        }
        Ok(())
    }

    fn read_int<T, R: AsRef<[u8]>>(&self, column: &mut Vec<T>, reader: &mut Cursor<R>) -> Result<()>
    where
        T: Number + From<T::Native>,
        T::Native: FromLexical,
    {
        let v: T::Native = reader.read_int_text()?;
        column.push(v.into());
        Ok(())
    }

    fn read_float<T, R: AsRef<[u8]>>(
        &self,
        column: &mut Vec<T>,
        reader: &mut Cursor<R>,
    ) -> Result<()>
    where
        T: Number + From<T::Native>,
        T::Native: FromLexical,
    {
        let v: T::Native = reader.read_float_text()?;
        column.push(v.into());
        Ok(())
    }

    fn read_string<R: AsRef<[u8]>>(
        &self,
        column: &mut StringColumnBuilder,
        reader: &mut Cursor<R>,
    ) -> Result<()> {
        self.read_string_inner(reader, &mut column.row_buffer)?;
        column.commit_row();
        Ok(())
    }

    fn read_binary<R: AsRef<[u8]>>(
        &self,
        column: &mut BinaryColumnBuilder,
        reader: &mut Cursor<R>,
    ) -> Result<()> {
        let mut buf = Vec::new();
        self.read_string_inner(reader, &mut buf)?;
        let decoded = decode_binary(&buf, self.common_settings.binary_format)?;
        column.put_slice(&decoded);
        column.commit_row();
        Ok(())
    }

    fn read_string_inner<R: AsRef<[u8]>>(
        &self,
        reader: &mut Cursor<R>,
        out_buf: &mut Vec<u8>,
    ) -> Result<()> {
        if reader.read_quoted_text(out_buf, b'"').is_err() {
            // Read single quoted text, compatible with previous implementations
            reader.read_quoted_text(out_buf, b'\'')?;
        }
        Ok(())
    }

    fn read_decimal<R: AsRef<[u8]>, D: Decimal>(
        &self,
        column: &mut Vec<D>,
        size: DecimalSize,
        reader: &mut Cursor<R>,
    ) -> Result<()> {
        let buf = Cursor::split(reader).1;
        let (n, n_read) = read_decimal_with_size(buf, size, false, true)?;
        column.push(n);
        reader.consume(n_read);
        Ok(())
    }

    fn read_date<R: AsRef<[u8]>>(
        &self,
        column: &mut Vec<i32>,
        reader: &mut Cursor<R>,
    ) -> Result<()> {
        let mut buf = Vec::new();
        self.read_string_inner(reader, &mut buf)?;
        let mut buffer_readr = Cursor::new(&buf);
        let date = buffer_readr.read_date_text(&self.common_settings.settings.jiff_timezone)?;
        let days = uniform_date(date);
        column.push(clamp_date(days as i64));
        Ok(())
    }

    fn read_interval<R: AsRef<[u8]>>(
        &self,
        column: &mut Vec<months_days_micros>,
        reader: &mut Cursor<R>,
    ) -> Result<()> {
        let mut buf = Vec::new();
        self.read_string_inner(reader, &mut buf)?;
        let res =
            std::str::from_utf8(buf.as_slice()).map_err_to_code(ErrorCode::BadBytes, || {
                format!(
                    "UTF-8 Conversion Failed: Unable to convert value {:?} to UTF-8",
                    buf
                )
            })?;
        let i = Interval::from_string(res)?;
        column.push(months_days_micros::new(i.months, i.days, i.micros));
        Ok(())
    }

    fn read_timestamp<R: AsRef<[u8]>>(
        &self,
        column: &mut Vec<i64>,
        reader: &mut Cursor<R>,
    ) -> Result<()> {
        let mut buf = Vec::new();
        self.read_string_inner(reader, &mut buf)?;
        read_timestamp(column, &buf, &self.common_settings)
    }

    fn read_timestamp_tz<R: AsRef<[u8]>>(
        &self,
        column: &mut Vec<timestamp_tz>,
        reader: &mut Cursor<R>,
    ) -> Result<()> {
        let mut buf = Vec::new();
        self.read_string_inner(reader, &mut buf)?;
        read_timestamp_tz(column, &buf, &self.common_settings)
    }

    fn read_bitmap<R: AsRef<[u8]>>(
        &self,
        column: &mut BinaryColumnBuilder,
        reader: &mut Cursor<R>,
    ) -> Result<()> {
        let mut buf = Vec::new();
        self.read_string_inner(reader, &mut buf)?;
        let rb = parse_bitmap(&buf)?;
        rb.serialize_into(&mut column.data).unwrap();
        column.commit_row();
        Ok(())
    }

    fn read_variant<R: AsRef<[u8]>>(
        &self,
        column: &mut BinaryColumnBuilder,
        reader: &mut Cursor<R>,
    ) -> Result<()> {
        let buf = if let Ok(val) = self.read_json(reader) {
            val.as_bytes().to_vec()
        } else {
            let mut buf = Vec::new();
            reader.read_quoted_text(&mut buf, b'\'')?;
            buf
        };
        match parse_owned_jsonb_with_buf(&buf, &mut column.data) {
            Ok(_) => {
                column.commit_row();
            }
            Err(e) => {
                if self.common_settings.settings.disable_variant_check {
                    column.commit_row();
                } else {
                    return Err(ErrorCode::BadBytes(e.to_string()));
                }
            }
        }
        Ok(())
    }

    fn read_geometry<R: AsRef<[u8]>>(
        &self,
        column: &mut BinaryColumnBuilder,
        reader: &mut Cursor<R>,
    ) -> Result<()> {
        let mut buf = Vec::new();
        if reader.read_quoted_text(&mut buf, b'"').is_err()
            && reader.read_quoted_text(&mut buf, b'\'').is_err()
        {
            let val = self.read_json(reader)?;
            buf = val.as_bytes().to_vec();
        }
        let geom = parse_bytes_to_ewkb(&buf, None)?;
        column.put_slice(geom.as_bytes());
        column.commit_row();
        Ok(())
    }

    fn read_geography<R: AsRef<[u8]>>(
        &self,
        column: &mut BinaryColumnBuilder,
        reader: &mut Cursor<R>,
    ) -> Result<()> {
        let mut buf = Vec::new();
        if reader.read_quoted_text(&mut buf, b'"').is_err()
            && reader.read_quoted_text(&mut buf, b'\'').is_err()
        {
            let val = self.read_json(reader)?;
            buf = val.as_bytes().to_vec();
        }
        let geog = geography_from_ewkt_bytes(&buf)?;
        column.put_slice(geog.as_bytes());
        column.commit_row();
        Ok(())
    }

    fn read_json<R: AsRef<[u8]>>(&self, reader: &mut Cursor<R>) -> Result<String> {
        let start = reader.position() as usize;
        let data = reader.get_ref().as_ref();
        let mut deserializer = Deserializer::from_slice(&data[start..]);
        let raw: Box<RawValue> = Box::<RawValue>::deserialize(&mut deserializer)?;
        reader.set_position((start + raw.get().len()) as u64);
        Ok(raw.to_string())
    }

    fn read_nullable<R: AsRef<[u8]>>(
        &self,
        column: &mut NullableColumnBuilder<AnyType>,
        reader: &mut Cursor<R>,
    ) -> Result<()> {
        for null in &self.common_settings.null_if {
            if self.match_bytes(reader, null) {
                column.push_null();
                return Ok(());
            }
        }
        self.read_field(&mut column.builder, reader)?;
        column.validity.push(true);
        Ok(())
    }

    pub(crate) fn read_array<R: AsRef<[u8]>>(
        &self,
        column: &mut ArrayColumnBuilder<AnyType>,
        reader: &mut Cursor<R>,
    ) -> Result<()> {
        reader.must_ignore_byte(b'[')?;
        for idx in 0.. {
            let _ = reader.ignore_white_spaces_or_comments();
            if reader.ignore_byte(b']') {
                break;
            }
            if idx != 0 {
                reader.must_ignore_byte(b',')?;
            }
            let _ = reader.ignore_white_spaces_or_comments();
            self.read_field(&mut column.builder, reader)?;
        }
        column.commit_row();
        Ok(())
    }

    pub(crate) fn read_map<R: AsRef<[u8]>>(
        &self,
        column: &mut ArrayColumnBuilder<AnyType>,
        reader: &mut Cursor<R>,
    ) -> Result<()> {
        const KEY: usize = 0;
        const VALUE: usize = 1;
        reader.must_ignore_byte(b'{')?;
        let mut set = HashSet::new();
        let map_builder = column.builder.as_tuple_mut().unwrap();
        for idx in 0.. {
            let _ = reader.ignore_white_spaces_or_comments();
            if reader.ignore_byte(b'}') {
                break;
            }
            if idx != 0 {
                reader.must_ignore_byte(b',')?;
            }
            let _ = reader.ignore_white_spaces_or_comments();
            self.read_field(&mut map_builder[KEY], reader)?;
            // check duplicate map keys
            let key = map_builder[KEY].pop().unwrap();
            if set.contains(&key) {
                return Err(ErrorCode::BadBytes(
                    "map keys have to be unique".to_string(),
                ));
            }
            map_builder[KEY].push(key.as_ref());
            set.insert(key);
            let _ = reader.ignore_white_spaces_or_comments();
            reader.must_ignore_byte(b':')?;
            let _ = reader.ignore_white_spaces_or_comments();
            self.read_field(&mut map_builder[VALUE], reader)?;
        }
        column.commit_row();
        Ok(())
    }

    pub(crate) fn read_tuple<R: AsRef<[u8]>>(
        &self,
        fields: &mut [ColumnBuilder],
        reader: &mut Cursor<R>,
    ) -> Result<()> {
        let closing = if reader.ignore_byte(b'(') {
            b')'
        } else {
            reader.must_ignore_byte(b'[')?;
            b']'
        };
        for (idx, field) in fields.iter_mut().enumerate() {
            let _ = reader.ignore_white_spaces_or_comments();
            if idx != 0 {
                reader.must_ignore_byte(b',')?;
            }
            let _ = reader.ignore_white_spaces_or_comments();
            self.read_field(field, reader)?;
        }
        reader.must_ignore_byte(closing)?;
        Ok(())
    }

    pub(crate) fn read_vector<R: AsRef<[u8]>>(
        &self,
        column: &mut VectorColumnBuilder,
        reader: &mut Cursor<R>,
    ) -> Result<()> {
        reader.must_ignore_byte(b'[')?;
        let dimension = column.dimension();
        let mut values = Vec::with_capacity(dimension);
        for idx in 0..dimension {
            let _ = reader.ignore_white_spaces_or_comments();
            if idx != 0 {
                reader.must_ignore_byte(b',')?;
            }
            let _ = reader.ignore_white_spaces_or_comments();
            let v: f32 = reader.read_float_text()?;
            values.push(v.into());
        }
        reader.must_ignore_byte(b']')?;
        column.push(&VectorScalarRef::Float32(&values));
        Ok(())
    }
}
