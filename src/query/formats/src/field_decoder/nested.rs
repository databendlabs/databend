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
use databend_common_expression::types::AnyType;
use databend_common_expression::types::MutableBitmap;
use databend_common_expression::types::NumberColumnBuilder;
use databend_common_expression::types::VectorScalarRef;
use databend_common_expression::types::array::ArrayColumnBuilder;
use databend_common_expression::types::binary::BinaryColumnBuilder;
use databend_common_expression::types::decimal::Decimal;
use databend_common_expression::types::decimal::DecimalColumnBuilder;
use databend_common_expression::types::decimal::DecimalSize;
use databend_common_expression::types::nullable::NullableColumnBuilder;
use databend_common_expression::types::number::Number;
use databend_common_expression::types::string::StringColumnBuilder;
use databend_common_expression::types::variant::JSONB_NULL;
use databend_common_expression::types::vector::VectorColumnBuilder;
use databend_common_expression::with_decimal_type;
use databend_common_expression::with_number_mapped_type;
use databend_common_io::Interval;
use databend_common_io::constants::NULL_BYTES_LOWER;
use databend_common_io::constants::NULL_BYTES_UPPER;
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
use crate::field_decoder::common::read_timestamp_ntz;
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
        scratch: &mut Vec<u8>,
    ) -> Result<()> {
        match column {
            ColumnBuilder::Null { len } => {
                *len += 1;
                Ok(())
            }
            ColumnBuilder::Nullable(c) => self.read_nullable(c, reader, scratch),
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
            ColumnBuilder::Date(c) => self.read_date(c, reader, scratch),
            ColumnBuilder::Interval(c) => self.read_interval(c, reader, scratch),
            ColumnBuilder::Timestamp(c) => self.read_timestamp(c, reader, scratch),
            ColumnBuilder::TimestampTz(c) => self.read_timestamp_tz(c, reader, scratch),
            ColumnBuilder::TimestampNtz(c) => self.read_timestamp_ntz(c, reader, scratch),
            ColumnBuilder::Binary(c) => self.read_binary(c, reader, scratch),
            ColumnBuilder::String(c) => self.read_string(c, reader),
            ColumnBuilder::Array(c) => self.read_array(c, reader, scratch),
            ColumnBuilder::Map(c) => self.read_map(c, reader, scratch),
            ColumnBuilder::Bitmap(c) => self.read_bitmap(c, reader, scratch),
            ColumnBuilder::Tuple(fields) => self.read_tuple(fields, reader, scratch),
            ColumnBuilder::Variant(c) => self.read_variant(c, reader, scratch),
            ColumnBuilder::Geometry(c) => self.read_geometry(c, reader, scratch),
            ColumnBuilder::Geography(c) => self.read_geography(c, reader, scratch),
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
        scratch: &mut Vec<u8>,
    ) -> Result<()> {
        self.read_quoted_text_into_scratch(reader, scratch)?;
        let decoded = decode_binary(scratch, self.common_settings.binary_format)?;
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

    fn read_quoted_text_into_scratch<R: AsRef<[u8]>>(
        &self,
        reader: &mut Cursor<R>,
        scratch: &mut Vec<u8>,
    ) -> Result<()> {
        scratch.clear();
        self.read_string_inner(reader, scratch)
    }

    fn read_decimal<R: AsRef<[u8]>, D: Decimal>(
        &self,
        column: &mut Vec<D>,
        size: DecimalSize,
        reader: &mut Cursor<R>,
    ) -> Result<()> {
        let buf = Cursor::split(reader).1;
        let (n, n_read) = read_decimal_with_size(
            buf,
            size,
            false,
            self.common_settings.settings.is_rounding_mode,
        )?;
        column.push(n);
        reader.consume(n_read);
        Ok(())
    }

    fn read_date<R: AsRef<[u8]>>(
        &self,
        column: &mut Vec<i32>,
        reader: &mut Cursor<R>,
        scratch: &mut Vec<u8>,
    ) -> Result<()> {
        self.read_quoted_text_into_scratch(reader, scratch)?;
        super::common::read_date(column, scratch, &self.common_settings)
    }

    fn read_interval<R: AsRef<[u8]>>(
        &self,
        column: &mut Vec<months_days_micros>,
        reader: &mut Cursor<R>,
        scratch: &mut Vec<u8>,
    ) -> Result<()> {
        self.read_quoted_text_into_scratch(reader, scratch)?;
        let res = std::str::from_utf8(scratch).map_err_to_code(ErrorCode::BadBytes, || {
            format!("UTF-8 Conversion Failed: Unable to convert value {scratch:?} to UTF-8")
        })?;
        let i = Interval::from_string(res)?;
        column.push(months_days_micros::new(i.months, i.days, i.micros));
        Ok(())
    }

    fn read_timestamp<R: AsRef<[u8]>>(
        &self,
        column: &mut Vec<i64>,
        reader: &mut Cursor<R>,
        scratch: &mut Vec<u8>,
    ) -> Result<()> {
        self.read_quoted_text_into_scratch(reader, scratch)?;
        read_timestamp(column, scratch, &self.common_settings)
    }

    fn read_timestamp_tz<R: AsRef<[u8]>>(
        &self,
        column: &mut Vec<timestamp_tz>,
        reader: &mut Cursor<R>,
        scratch: &mut Vec<u8>,
    ) -> Result<()> {
        self.read_quoted_text_into_scratch(reader, scratch)?;
        read_timestamp_tz(column, scratch, &self.common_settings)
    }

    fn read_timestamp_ntz<R: AsRef<[u8]>>(
        &self,
        column: &mut Vec<i64>,
        reader: &mut Cursor<R>,
        scratch: &mut Vec<u8>,
    ) -> Result<()> {
        self.read_quoted_text_into_scratch(reader, scratch)?;
        read_timestamp_ntz(column, scratch)
    }

    fn read_bitmap<R: AsRef<[u8]>>(
        &self,
        column: &mut BinaryColumnBuilder,
        reader: &mut Cursor<R>,
        scratch: &mut Vec<u8>,
    ) -> Result<()> {
        self.read_quoted_text_into_scratch(reader, scratch)?;
        let rb = parse_bitmap(scratch)?;
        rb.serialize_into(&mut column.data).unwrap();
        column.commit_row();
        Ok(())
    }

    fn read_variant<R: AsRef<[u8]>>(
        &self,
        column: &mut BinaryColumnBuilder,
        reader: &mut Cursor<R>,
        scratch: &mut Vec<u8>,
    ) -> Result<()> {
        if self.read_json_into_scratch(reader, scratch).is_err() {
            scratch.clear();
            reader.read_quoted_text(scratch, b'\'')?;
        }
        match parse_owned_jsonb_with_buf(scratch, &mut column.data) {
            Ok(_) => {
                column.commit_row();
            }
            Err(e) => {
                if self.common_settings.settings.disable_variant_check {
                    // Store an unparsable value as JSON null: empty bytes are not valid JSONB.
                    column.put_slice(JSONB_NULL);
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
        scratch: &mut Vec<u8>,
    ) -> Result<()> {
        if self.read_quoted_text_into_scratch(reader, scratch).is_err() {
            self.read_json_into_scratch(reader, scratch)?;
        }
        let geom = parse_bytes_to_ewkb(scratch, None)?;
        column.put_slice(geom.as_bytes());
        column.commit_row();
        Ok(())
    }

    fn read_geography<R: AsRef<[u8]>>(
        &self,
        column: &mut BinaryColumnBuilder,
        reader: &mut Cursor<R>,
        scratch: &mut Vec<u8>,
    ) -> Result<()> {
        if self.read_quoted_text_into_scratch(reader, scratch).is_err() {
            self.read_json_into_scratch(reader, scratch)?;
        }
        let geog = geography_from_ewkt_bytes(scratch)?;
        column.put_slice(geog.as_bytes());
        column.commit_row();
        Ok(())
    }

    fn read_json_into_scratch<R: AsRef<[u8]>>(
        &self,
        reader: &mut Cursor<R>,
        scratch: &mut Vec<u8>,
    ) -> Result<()> {
        let start = reader.position() as usize;
        let data = reader.get_ref().as_ref();
        let mut deserializer = Deserializer::from_slice(&data[start..]);
        let raw = <&RawValue>::deserialize(&mut deserializer)?
            .get()
            .as_bytes();
        scratch.clear();
        scratch.extend_from_slice(raw);
        reader.set_position((start + raw.len()) as u64);
        Ok(())
    }

    fn read_nullable<R: AsRef<[u8]>>(
        &self,
        column: &mut NullableColumnBuilder<AnyType>,
        reader: &mut Cursor<R>,
        scratch: &mut Vec<u8>,
    ) -> Result<()> {
        for null in &self.common_settings.null_if {
            if self.match_bytes(reader, null) {
                column.push_null();
                return Ok(());
            }
        }
        self.read_field(&mut column.builder, reader, scratch)?;
        column.validity.push(true);
        Ok(())
    }

    pub(crate) fn read_array<R: AsRef<[u8]>>(
        &self,
        column: &mut ArrayColumnBuilder<AnyType>,
        reader: &mut Cursor<R>,
        scratch: &mut Vec<u8>,
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
            self.read_field(&mut column.builder, reader, scratch)?;
        }
        column.commit_row();
        Ok(())
    }

    pub(crate) fn read_map<R: AsRef<[u8]>>(
        &self,
        column: &mut ArrayColumnBuilder<AnyType>,
        reader: &mut Cursor<R>,
        scratch: &mut Vec<u8>,
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
            self.read_field(&mut map_builder[KEY], reader, scratch)?;
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
            self.read_field(&mut map_builder[VALUE], reader, scratch)?;
        }
        column.commit_row();
        Ok(())
    }

    pub(crate) fn read_tuple<R: AsRef<[u8]>>(
        &self,
        fields: &mut [ColumnBuilder],
        reader: &mut Cursor<R>,
        scratch: &mut Vec<u8>,
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
            self.read_field(field, reader, scratch)?;
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
