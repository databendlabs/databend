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
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_exception::ToErrorCode;
use databend_common_expression::serialize::read_decimal_with_size;
use databend_common_expression::serialize::uniform_date;
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
use databend_common_expression::types::AnyType;
use databend_common_expression::types::MutableBitmap;
use databend_common_expression::types::NumberColumnBuilder;
use databend_common_expression::types::VectorScalarRef;
use databend_common_expression::with_decimal_type;
use databend_common_expression::with_number_mapped_type;
use databend_common_expression::ColumnBuilder;
use databend_common_io::constants::FALSE_BYTES_LOWER;
use databend_common_io::constants::NULL_BYTES_LOWER;
use databend_common_io::constants::NULL_BYTES_UPPER;
use databend_common_io::constants::TRUE_BYTES_LOWER;
use databend_common_io::cursor_ext::BufferReadDateTimeExt;
use databend_common_io::cursor_ext::BufferReadStringExt;
use databend_common_io::cursor_ext::ReadBytesExt;
use databend_common_io::cursor_ext::ReadCheckPointExt;
use databend_common_io::cursor_ext::ReadNumberExt;
use databend_common_io::geography::geography_from_ewkt_bytes;
use databend_common_io::parse_bitmap;
use databend_common_io::parse_bytes_to_ewkb;
use databend_common_io::Interval;
use jsonb::parse_value;
use lexical_core::FromLexical;

use crate::binary::decode_binary;
use crate::field_decoder::common::read_timestamp;
use crate::FileFormatOptionsExt;
use crate::InputCommonSettings;

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
    pub fn create(options_ext: &FileFormatOptionsExt) -> Self {
        NestedValues {
            common_settings: InputCommonSettings {
                true_bytes: TRUE_BYTES_LOWER.as_bytes().to_vec(),
                false_bytes: FALSE_BYTES_LOWER.as_bytes().to_vec(),
                null_if: vec![
                    NULL_BYTES_UPPER.as_bytes().to_vec(),
                    NULL_BYTES_LOWER.as_bytes().to_vec(),
                ],
                timezone: options_ext.timezone,
                jiff_timezone: options_ext.jiff_timezone.clone(),
                disable_variant_check: options_ext.disable_variant_check,
                binary_format: Default::default(),
                is_rounding_mode: options_ext.is_rounding_mode,
                enable_dst_hour_fix: options_ext.enable_dst_hour_fix,
            },
        }
    }
}

impl NestedValues {
    fn common_settings(&self) -> &InputCommonSettings {
        &self.common_settings
    }

    fn match_bytes<R: AsRef<[u8]>>(&self, reader: &mut Cursor<R>, bs: &[u8]) -> bool {
        let pos = reader.checkpoint();
        if reader.ignore_bytes(bs) {
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
        }
    }

    fn read_bool<R: AsRef<[u8]>>(
        &self,
        column: &mut MutableBitmap,
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
        reader.read_quoted_text(&mut column.row_buffer, b'\'')?;
        column.commit_row();
        Ok(())
    }

    fn read_binary<R: AsRef<[u8]>>(
        &self,
        column: &mut BinaryColumnBuilder,
        reader: &mut Cursor<R>,
    ) -> Result<()> {
        let mut buf = Vec::new();
        reader.read_quoted_text(&mut buf, b'\'')?;
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
        reader.read_quoted_text(out_buf, b'\'')?;
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
        let date = buffer_readr.read_date_text(&self.common_settings().jiff_timezone)?;
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
        read_timestamp(column, &buf, self.common_settings())
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
        let mut buf = Vec::new();
        self.read_string_inner(reader, &mut buf)?;
        match parse_value(&buf) {
            Ok(value) => {
                value.write_to_vec(&mut column.data);
                column.commit_row();
            }
            Err(e) => {
                if self.common_settings().disable_variant_check {
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
        self.read_string_inner(reader, &mut buf)?;
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
        self.read_string_inner(reader, &mut buf)?;
        let geog = geography_from_ewkt_bytes(&buf)?;
        column.put_slice(geog.as_bytes());
        column.commit_row();
        Ok(())
    }

    fn read_nullable<R: AsRef<[u8]>>(
        &self,
        column: &mut NullableColumnBuilder<AnyType>,
        reader: &mut Cursor<R>,
    ) -> Result<()> {
        for null in &self.common_settings().null_if {
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
        reader.must_ignore_byte(b'(')?;
        for (idx, field) in fields.iter_mut().enumerate() {
            let _ = reader.ignore_white_spaces_or_comments();
            if idx != 0 {
                reader.must_ignore_byte(b',')?;
            }
            let _ = reader.ignore_white_spaces_or_comments();
            self.read_field(field, reader)?;
        }
        reader.must_ignore_byte(b')')?;
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
        for _ in 0..dimension {
            let _ = reader.ignore_white_spaces_or_comments();
            reader.must_ignore_byte(b',')?;
            let _ = reader.ignore_white_spaces_or_comments();
            let res: Result<f32> = reader.read_float_text();
            match res {
                Ok(v) => {
                    values.push(v.into());
                }
                Err(err) => {
                    return Err(err);
                }
            }
        }
        reader.must_ignore_byte(b']')?;
        column.push(&VectorScalarRef::Float32(&values));
        Ok(())
    }
}
