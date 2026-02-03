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
use std::collections::HashSet;
use std::collections::VecDeque;
use std::io::BufRead;
use std::io::Cursor;
use std::ops::Not;
use std::sync::LazyLock;

use aho_corasick::AhoCorasick;
use bstr::ByteSlice;
use databend_common_column::types::months_days_micros;
use databend_common_column::types::timestamp_tz;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_exception::ToErrorCode;
use databend_common_expression::ColumnBuilder;
use databend_common_expression::Scalar;
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
use databend_common_io::constants::NAN_BYTES_LOWER;
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
use num_traits::NumCast;

use crate::FieldDecoder;
use crate::InputCommonSettings;
use crate::field_decoder::common::read_timestamp;
use crate::field_decoder::common::read_timestamp_tz;

#[derive(Clone)]
pub struct FastFieldDecoderValues {
    common_settings: InputCommonSettings,
}

impl FieldDecoder for FastFieldDecoderValues {
    fn as_any(&self) -> &dyn Any {
        self
    }
}

impl FastFieldDecoderValues {
    pub fn create_for_insert(settings: InputFormatSettings) -> Self {
        FastFieldDecoderValues {
            common_settings: InputCommonSettings {
                null_if: vec![
                    NULL_BYTES_UPPER.as_bytes().to_vec(),
                    NAN_BYTES_LOWER.as_bytes().to_vec(),
                ],
                settings,
                binary_format: Default::default(),
            },
        }
    }

    fn ignore_field_end<R: AsRef<[u8]>>(&self, reader: &mut Cursor<R>) -> bool {
        reader.ignore_white_spaces_or_comments();
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

    fn pop_inner_values(&self, column: &mut ColumnBuilder, size: usize) {
        for _ in 0..size {
            let _ = column.pop();
        }
    }

    pub fn read_field<R: AsRef<[u8]>>(
        &self,
        column: &mut ColumnBuilder,
        reader: &mut Cursor<R>,
        positions: &mut VecDeque<usize>,
    ) -> Result<()> {
        match column {
            ColumnBuilder::Null { len } => self.read_null(len, reader),
            ColumnBuilder::Nullable(c) => self.read_nullable(c, reader, positions),
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
            ColumnBuilder::Date(c) => self.read_date(c, reader, positions),
            ColumnBuilder::Timestamp(c) => self.read_timestamp(c, reader, positions),
            ColumnBuilder::TimestampTz(c) => self.read_timestamp_tz(c, reader, positions),
            ColumnBuilder::String(c) => self.read_string(c, reader, positions),
            ColumnBuilder::Array(c) => self.read_array(c, reader, positions),
            ColumnBuilder::Map(c) => self.read_map(c, reader, positions),
            ColumnBuilder::Bitmap(c) => self.read_bitmap(c, reader, positions),
            ColumnBuilder::Tuple(fields) => self.read_tuple(fields, reader, positions),
            ColumnBuilder::Variant(c) => self.read_variant(c, reader, positions),
            ColumnBuilder::Geometry(c) => self.read_geometry(c, reader, positions),
            ColumnBuilder::Geography(c) => self.read_geography(c, reader, positions),
            ColumnBuilder::Binary(_) => Err(ErrorCode::Unimplemented("binary literal")),
            ColumnBuilder::Interval(c) => self.read_interval(c, reader, positions),
            ColumnBuilder::Vector(c) => self.read_vector(c, reader),
            ColumnBuilder::EmptyArray { .. } | ColumnBuilder::EmptyMap { .. } => {
                Err(ErrorCode::Unimplemented("empty array/map literal"))
            }
            ColumnBuilder::Opaque(_) => Err(ErrorCode::Unimplemented(
                "Opaque type not supported in fast_values",
            )),
        }
    }

    fn read_bool<R: AsRef<[u8]>>(
        &self,
        column: &mut MutableBitmap,
        reader: &mut Cursor<R>,
    ) -> Result<()> {
        if self.match_bytes(reader, b"true") {
            column.push(true);
            Ok(())
        } else if self.match_bytes(reader, b"false") {
            column.push(false);
            Ok(())
        } else {
            Err(ErrorCode::BadBytes(
                "Incorrect boolean value, expect true or false",
            ))
        }
    }

    fn read_null<R: AsRef<[u8]>>(&self, len: &mut usize, _reader: &mut Cursor<R>) -> Result<()> {
        *len += 1;
        Ok(())
    }

    fn read_nullable<R: AsRef<[u8]>>(
        &self,
        column: &mut NullableColumnBuilder<AnyType>,
        reader: &mut Cursor<R>,
        positions: &mut VecDeque<usize>,
    ) -> Result<()> {
        if reader.eof() || reader.ignore_bytes(b"NULL") || reader.ignore_bytes(b"null") {
            column.push_null();
        } else {
            self.read_field(&mut column.builder, reader, positions)?;
            column.validity.push(true);
        }
        Ok(())
    }

    fn read_int<T, R: AsRef<[u8]>>(&self, column: &mut Vec<T>, reader: &mut Cursor<R>) -> Result<()>
    where
        T: Number + From<T::Native>,
        T::Native: FromLexical + NumCast,
    {
        let val: Result<T::Native> = reader.read_int_text();
        let v = match val {
            Ok(v) => v,
            Err(_) => {
                // cast float value to integer value
                let val: f64 = reader.read_float_text()?;
                let new_val: Option<T::Native> = if self.common_settings.settings.is_rounding_mode {
                    num_traits::cast::cast(val.round())
                } else {
                    num_traits::cast::cast(val)
                };
                if let Some(v) = new_val {
                    v
                } else {
                    return Err(ErrorCode::BadBytes(format!("number {} is overflowed", val)));
                }
            }
        };
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
        column: &mut StringColumnBuilder,
        reader: &mut Cursor<R>,
        positions: &mut VecDeque<usize>,
    ) -> Result<()> {
        self.read_string_inner(reader, &mut column.row_buffer, positions)?;
        column.commit_row();
        Ok(())
    }

    fn read_interval<R: AsRef<[u8]>>(
        &self,
        column: &mut Vec<months_days_micros>,
        reader: &mut Cursor<R>,
        positions: &mut VecDeque<usize>,
    ) -> Result<()> {
        let mut buf = Vec::new();
        self.read_string_inner(reader, &mut buf, positions)?;
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

    fn read_date<R: AsRef<[u8]>>(
        &self,
        column: &mut Vec<i32>,
        reader: &mut Cursor<R>,
        positions: &mut VecDeque<usize>,
    ) -> Result<()> {
        let mut buf = Vec::new();
        self.read_string_inner(reader, &mut buf, positions)?;
        let mut buffer_readr = Cursor::new(&buf);
        let date = buffer_readr.read_date_text(&self.common_settings.settings.jiff_timezone)?;
        let days = uniform_date(date);
        column.push(clamp_date(days as i64));
        Ok(())
    }

    fn read_timestamp<R: AsRef<[u8]>>(
        &self,
        column: &mut Vec<i64>,
        reader: &mut Cursor<R>,
        positions: &mut VecDeque<usize>,
    ) -> Result<()> {
        let mut buf = Vec::new();
        self.read_string_inner(reader, &mut buf, positions)?;
        read_timestamp(column, &buf, &self.common_settings)
    }

    fn read_timestamp_tz<R: AsRef<[u8]>>(
        &self,
        column: &mut Vec<timestamp_tz>,
        reader: &mut Cursor<R>,
        positions: &mut VecDeque<usize>,
    ) -> Result<()> {
        let mut buf = Vec::new();
        self.read_string_inner(reader, &mut buf, positions)?;
        read_timestamp_tz(column, &buf, &self.common_settings)
    }

    fn read_array<R: AsRef<[u8]>>(
        &self,
        column: &mut ArrayColumnBuilder<AnyType>,
        reader: &mut Cursor<R>,
        positions: &mut VecDeque<usize>,
    ) -> Result<()> {
        reader.must_ignore_byte(b'[')?;
        for idx in 0.. {
            let _ = reader.ignore_white_spaces_or_comments();
            if reader.ignore_byte(b']') {
                break;
            }
            if idx != 0 {
                if let Err(err) = reader.must_ignore_byte(b',') {
                    self.pop_inner_values(&mut column.builder, idx);
                    return Err(err.into());
                }
            }
            let _ = reader.ignore_white_spaces_or_comments();
            if let Err(err) = self.read_field(&mut column.builder, reader, positions) {
                self.pop_inner_values(&mut column.builder, idx);
                return Err(err);
            }
        }
        column.commit_row();
        Ok(())
    }

    fn read_map<R: AsRef<[u8]>>(
        &self,
        column: &mut ArrayColumnBuilder<AnyType>,
        reader: &mut Cursor<R>,
        positions: &mut VecDeque<usize>,
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
                if let Err(err) = reader.must_ignore_byte(b',') {
                    self.pop_inner_values(&mut map_builder[KEY], idx);
                    self.pop_inner_values(&mut map_builder[VALUE], idx);
                    return Err(err.into());
                }
            }
            let _ = reader.ignore_white_spaces_or_comments();
            if let Err(err) = self.read_field(&mut map_builder[KEY], reader, positions) {
                self.pop_inner_values(&mut map_builder[KEY], idx);
                self.pop_inner_values(&mut map_builder[VALUE], idx);
                return Err(err);
            }
            // check duplicate map keys
            let key = map_builder[KEY].pop().unwrap();
            if set.contains(&key) {
                self.pop_inner_values(&mut map_builder[KEY], idx);
                self.pop_inner_values(&mut map_builder[VALUE], idx);
                return Err(ErrorCode::BadBytes(
                    "map keys have to be unique".to_string(),
                ));
            }
            set.insert(key.clone());
            map_builder[KEY].push(key.as_ref());
            let _ = reader.ignore_white_spaces_or_comments();
            if let Err(err) = reader.must_ignore_byte(b':') {
                self.pop_inner_values(&mut map_builder[KEY], idx + 1);
                self.pop_inner_values(&mut map_builder[VALUE], idx);
                return Err(err.into());
            }
            let _ = reader.ignore_white_spaces_or_comments();
            if let Err(err) = self.read_field(&mut map_builder[VALUE], reader, positions) {
                self.pop_inner_values(&mut map_builder[KEY], idx + 1);
                self.pop_inner_values(&mut map_builder[VALUE], idx);
                return Err(err);
            }
        }
        column.commit_row();
        Ok(())
    }

    fn read_tuple<R: AsRef<[u8]>>(
        &self,
        fields: &mut [ColumnBuilder],
        reader: &mut Cursor<R>,
        positions: &mut VecDeque<usize>,
    ) -> Result<()> {
        reader.must_ignore_byte(b'(')?;
        for idx in 0..fields.len() {
            let _ = reader.ignore_white_spaces_or_comments();
            if idx != 0 {
                if let Err(err) = reader.must_ignore_byte(b',') {
                    for field in fields.iter_mut().take(idx) {
                        self.pop_inner_values(field, 1);
                    }
                    return Err(err.into());
                }
            }
            let _ = reader.ignore_white_spaces_or_comments();
            if let Err(err) = self.read_field(&mut fields[idx], reader, positions) {
                for field in fields.iter_mut().take(idx) {
                    self.pop_inner_values(field, 1);
                }
                return Err(err);
            }
        }
        if let Err(err) = reader.must_ignore_byte(b')') {
            for field in fields.iter_mut() {
                self.pop_inner_values(field, 1);
            }
            return Err(err.into());
        }
        Ok(())
    }

    fn read_bitmap<R: AsRef<[u8]>>(
        &self,
        column: &mut BinaryColumnBuilder,
        reader: &mut Cursor<R>,
        positions: &mut VecDeque<usize>,
    ) -> Result<()> {
        let mut buf = Vec::new();
        self.read_string_inner(reader, &mut buf, positions)?;
        let rb = parse_bitmap(&buf)?;
        rb.serialize_into(&mut column.data).unwrap();
        column.commit_row();
        Ok(())
    }

    fn read_variant<R: AsRef<[u8]>>(
        &self,
        column: &mut BinaryColumnBuilder,
        reader: &mut Cursor<R>,
        positions: &mut VecDeque<usize>,
    ) -> Result<()> {
        let mut buf = Vec::new();
        self.read_string_inner(reader, &mut buf, positions)?;
        match parse_owned_jsonb_with_buf(&buf, &mut column.data) {
            Ok(_) => {
                column.commit_row();
            }
            Err(_) => {
                if self.common_settings.settings.disable_variant_check {
                    column.commit_row();
                } else {
                    return Err(ErrorCode::BadBytes(format!(
                        "Invalid JSON value: {:?}",
                        String::from_utf8_lossy(&buf)
                    )));
                }
            }
        }
        Ok(())
    }

    fn read_geometry<R: AsRef<[u8]>>(
        &self,
        column: &mut BinaryColumnBuilder,
        reader: &mut Cursor<R>,
        positions: &mut VecDeque<usize>,
    ) -> Result<()> {
        let mut buf = Vec::new();
        self.read_string_inner(reader, &mut buf, positions)?;
        let geom = parse_bytes_to_ewkb(&buf, None)?;
        column.put_slice(geom.as_bytes());
        column.commit_row();
        Ok(())
    }

    fn read_geography<R: AsRef<[u8]>>(
        &self,
        column: &mut BinaryColumnBuilder,
        reader: &mut Cursor<R>,
        positions: &mut VecDeque<usize>,
    ) -> Result<()> {
        let mut buf = Vec::new();
        self.read_string_inner(reader, &mut buf, positions)?;
        let geog = geography_from_ewkt_bytes(&buf)?;
        column.put_slice(geog.as_bytes());
        column.commit_row();
        Ok(())
    }

    fn read_vector<R: AsRef<[u8]>>(
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
                if let Err(err) = reader.must_ignore_byte(b',') {
                    return Err(err.into());
                }
            }
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

pub struct FastValuesDecoder<'a> {
    field_decoder: &'a FastFieldDecoderValues,
    reader: Cursor<&'a [u8]>,
    estimated_rows: usize,
    positions: VecDeque<usize>,
}

#[async_trait::async_trait]
pub trait FastValuesDecodeFallback {
    async fn parse_fallback(&self, sql: &str) -> Result<Vec<Scalar>>;
}

// Pre-generate the positions of `(`, `'` and `\`
static PATTERNS: &[&str] = &["(", "'", "\\"];

static INSERT_TOKEN_FINDER: LazyLock<AhoCorasick> =
    LazyLock::new(|| AhoCorasick::new(PATTERNS).unwrap());

impl<'a> FastValuesDecoder<'a> {
    pub fn new(data: &'a str, field_decoder: &'a FastFieldDecoderValues) -> Self {
        let mut estimated_rows = 0;
        let mut positions = VecDeque::new();
        for mat in INSERT_TOKEN_FINDER.find_iter(data) {
            if mat.pattern() == 0.into() {
                estimated_rows += 1;
                continue;
            }
            positions.push_back(mat.start());
        }
        let reader = Cursor::new(data.as_bytes());
        FastValuesDecoder {
            reader,
            estimated_rows,
            positions,
            field_decoder,
        }
    }

    pub fn estimated_rows(&self) -> usize {
        self.estimated_rows
    }

    pub async fn parse(
        &mut self,
        columns: &mut [ColumnBuilder],
        fallback_fn: &impl FastValuesDecodeFallback,
    ) -> Result<()> {
        for row in 0.. {
            let _ = self.reader.ignore_white_spaces_or_comments();
            if self.reader.eof() {
                break;
            }

            // Not the first row
            if row != 0 {
                if self.reader.ignore_byte(b';') {
                    break;
                }
                self.reader.must_ignore_byte(b',')?;
            }

            self.parse_next_row(columns, fallback_fn).await?;
        }
        Ok(())
    }

    async fn parse_next_row(
        &mut self,
        columns: &mut [ColumnBuilder],
        fallback: &impl FastValuesDecodeFallback,
    ) -> Result<()> {
        let _ = self.reader.ignore_white_spaces_or_comments();
        let col_size = columns.len();
        let start_pos_of_row = self.reader.checkpoint();

        // Start of the row --- '('
        if !self.reader.ignore_byte(b'(') {
            return Err(ErrorCode::BadDataValueType(
                "Must start with parentheses".to_string(),
            ));
        }
        // Ignore the positions in the previous row.
        while let Some(pos) = self.positions.front() {
            if *pos < start_pos_of_row as usize {
                self.positions.pop_front();
            } else {
                break;
            }
        }

        for col_idx in 0..col_size {
            let _ = self.reader.ignore_white_spaces_or_comments();
            let col_end = if col_idx + 1 == col_size { b')' } else { b',' };

            let col = columns
                .get_mut(col_idx)
                .ok_or_else(|| ErrorCode::Internal("ColumnBuilder is None"))?;

            let (need_fallback, pop_count) = self
                .field_decoder
                .read_field(col, &mut self.reader, &mut self.positions)
                .map(|_| {
                    let _ = self.reader.ignore_white_spaces_or_comments();
                    let need_fallback = self.reader.ignore_byte(col_end).not();
                    (need_fallback, col_idx + 1)
                })
                .unwrap_or((true, col_idx));

            // ColumnBuilder and expr-parser both will eat the end ')' of the row.
            if need_fallback {
                for col in columns.iter_mut().take(pop_count) {
                    col.pop();
                }
                // rollback to start position of the row
                self.reader.rollback(start_pos_of_row + 1);
                skip_to_next_row(&mut self.reader, 1)?;
                let end_pos_of_row = self.reader.position();

                // Parse from expression and append all columns.
                self.reader.set_position(start_pos_of_row);
                let row_len = end_pos_of_row - start_pos_of_row;
                let buf = Cursor::split(&self.reader).1;
                let buf = &buf[..row_len as usize];

                let sql = std::str::from_utf8(buf).unwrap();
                let values = fallback.parse_fallback(sql).await?;

                for (col, scalar) in columns.iter_mut().zip(values) {
                    col.push(scalar.as_ref());
                }
                self.reader.set_position(end_pos_of_row);
                return Ok(());
            }
        }

        Ok(())
    }
}

// Values |(xxx), (yyy), (zzz)
pub fn skip_to_next_row<R: AsRef<[u8]>>(reader: &mut Cursor<R>, mut balance: i32) -> Result<()> {
    let _ = reader.ignore_white_spaces_or_comments();

    let mut quoted = false;
    let mut escaped = false;

    while balance > 0 {
        let buffer = Cursor::split(reader).1;
        if buffer.is_empty() {
            break;
        }

        let size = buffer.len();

        let it = buffer
            .iter()
            .position(|&c| c == b'(' || c == b')' || c == b'\\' || c == b'\'');

        if let Some(it) = it {
            let c = buffer[it];
            reader.consume(it + 1);

            if it == 0 && escaped {
                escaped = false;
                continue;
            }
            escaped = false;

            match c {
                b'\\' => {
                    escaped = true;
                    continue;
                }
                b'\'' => {
                    quoted ^= true;
                    continue;
                }
                b')' => {
                    if !quoted {
                        balance -= 1;
                    }
                }
                b'(' => {
                    if !quoted {
                        balance += 1;
                    }
                }
                _ => {}
            }
        } else {
            escaped = false;
            reader.consume(size);
        }
    }
    Ok(())
}

#[cfg(test)]
mod test {

    use std::io::Write;

    use databend_common_exception::ErrorCode;
    use databend_common_exception::Result;
    use databend_common_expression::ColumnBuilder;
    use databend_common_expression::DataBlock;
    use databend_common_expression::Scalar;
    use databend_common_expression::types::DataType;
    use databend_common_expression::types::NumberDataType;
    use databend_common_io::prelude::InputFormatSettings;
    use goldenfile::Mint;

    use super::FastFieldDecoderValues;
    use super::FastValuesDecodeFallback;
    use super::FastValuesDecoder;

    struct DummyFastValuesDecodeFallback {}

    #[async_trait::async_trait]
    impl FastValuesDecodeFallback for DummyFastValuesDecodeFallback {
        async fn parse_fallback(&self, _data: &str) -> Result<Vec<Scalar>> {
            Err(ErrorCode::Unimplemented("fallback".to_string()))
        }
    }

    #[tokio::test]
    async fn test_fast_values_decoder_multi() -> Result<()> {
        let mut mint = Mint::new("testdata");
        let file = &mut mint.new_goldenfile("fast_values.txt").unwrap();

        struct Test {
            data: &'static str,
            column_types: Vec<DataType>,
        }

        let cases = vec![
            Test {
                data: "(0, 1, 2), (3,4,5)",
                column_types: vec![
                    DataType::Number(NumberDataType::Int16),
                    DataType::Number(NumberDataType::Int16),
                    DataType::Number(NumberDataType::Int16),
                ],
            },
            Test {
                data: "(0, 1, 2), (3,4,5), ",
                column_types: vec![
                    DataType::Number(NumberDataType::Int16),
                    DataType::Number(NumberDataType::Int16),
                    DataType::Number(NumberDataType::Int16),
                ],
            },
            Test {
                data: "('', '', '')",
                column_types: vec![
                    DataType::Number(NumberDataType::Int16),
                    DataType::Number(NumberDataType::Int16),
                    DataType::Number(NumberDataType::Int16),
                ],
            },
            Test {
                data: "( 1, '', '2022-10-01')",
                column_types: vec![
                    DataType::Number(NumberDataType::Int16),
                    DataType::String,
                    DataType::Date,
                ],
            },
            Test {
                data: "(1, 2, 3), (1, 1, 1), (1, 1, 1);",
                column_types: vec![
                    DataType::Number(NumberDataType::Int16),
                    DataType::Number(NumberDataType::Int16),
                    DataType::Number(NumberDataType::Int16),
                ],
            },
            Test {
                data: "(1, 2, 3), (1, 1, 1), (1, 1, 1);  ",
                column_types: vec![
                    DataType::Number(NumberDataType::Int16),
                    DataType::Number(NumberDataType::Int16),
                    DataType::Number(NumberDataType::Int16),
                ],
            },
            Test {
                data: "(1.2, -2.9, 3.55), (3.12e2, 3.45e+3, -1.9e-3);",
                column_types: vec![
                    DataType::Number(NumberDataType::Int16),
                    DataType::Number(NumberDataType::Int16),
                    DataType::Number(NumberDataType::Int16),
                ],
            },
        ];

        for case in cases {
            writeln!(file, "---------- Input Data ----------")?;
            writeln!(file, "{:?}", case.data)?;

            writeln!(file, "---------- Input Column Types ----------")?;
            writeln!(file, "{:?}", case.column_types)?;

            let field_decoder =
                FastFieldDecoderValues::create_for_insert(InputFormatSettings::default());
            let mut values_decoder = FastValuesDecoder::new(case.data, &field_decoder);
            let fallback = DummyFastValuesDecodeFallback {};
            let mut columns = case
                .column_types
                .into_iter()
                .map(|dt| ColumnBuilder::with_capacity(&dt, values_decoder.estimated_rows()))
                .collect::<Vec<_>>();
            let result = values_decoder.parse(&mut columns, &fallback).await;

            writeln!(file, "---------- Output ---------")?;

            if let Err(err) = result {
                writeln!(file, "{}", err)?;
            } else {
                let columns = columns.into_iter().map(|cb| cb.build()).collect::<Vec<_>>();
                let got = DataBlock::new_from_columns(columns);
                writeln!(file, "{}", got)?;
            }
            writeln!(file, "\n")?;
        }

        Ok(())
    }
}
