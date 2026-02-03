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
use databend_common_expression::types::Number;
use databend_common_expression::types::NumberColumnBuilder;
use databend_common_expression::types::array::ArrayColumnBuilder;
use databend_common_expression::types::binary::BinaryColumnBuilder;
use databend_common_expression::types::date::clamp_date;
use databend_common_expression::types::decimal::Decimal;
use databend_common_expression::types::decimal::DecimalColumnBuilder;
use databend_common_expression::types::decimal::DecimalSize;
use databend_common_expression::types::nullable::NullableColumnBuilder;
use databend_common_expression::types::vector::VectorColumnBuilder;
use databend_common_expression::with_decimal_type;
use databend_common_expression::with_number_mapped_type;
use databend_common_io::Interval;
use databend_common_io::constants::NULL_BYTES_ESCAPE;
use databend_common_io::cursor_ext::BufferReadDateTimeExt;
use databend_common_io::cursor_ext::collect_number;
use databend_common_io::cursor_ext::read_num_text_exact;
use databend_common_io::geography::geography_from_ewkt_bytes;
use databend_common_io::parse_bitmap;
use databend_common_io::parse_bytes_to_ewkb;
use databend_common_io::prelude::InputFormatSettings;
use databend_common_meta_app::principal::CsvFileFormatParams;
use databend_common_meta_app::principal::TsvFileFormatParams;
use jsonb::parse_owned_jsonb_with_buf;
use lexical_core::FromLexical;
use num_traits::NumCast;

use crate::InputCommonSettings;
use crate::NestedValues;
use crate::binary::decode_binary;
use crate::field_decoder::FieldDecoder;
use crate::field_decoder::common::read_timestamp;
use crate::field_decoder::common::read_timestamp_tz;

#[derive(Clone)]
pub struct SeparatedTextDecoder {
    common_settings: InputCommonSettings,
    nested_decoder: NestedValues,
}

impl FieldDecoder for SeparatedTextDecoder {
    fn as_any(&self) -> &dyn Any {
        self
    }
}

/// in CSV, we find the exact bound of each field before decode it to a type.
/// which is diff from the case when parsing values.
impl SeparatedTextDecoder {
    pub fn create_csv(params: &CsvFileFormatParams, settings: InputFormatSettings) -> Self {
        SeparatedTextDecoder {
            common_settings: InputCommonSettings {
                null_if: vec![params.null_display.as_bytes().to_vec()],
                settings: settings.clone(),
                binary_format: params.binary_format,
            },
            nested_decoder: NestedValues::create(settings),
        }
    }

    pub fn create_tsv(_params: &TsvFileFormatParams, settings: InputFormatSettings) -> Self {
        SeparatedTextDecoder {
            common_settings: InputCommonSettings {
                null_if: vec![NULL_BYTES_ESCAPE.as_bytes().to_vec()],
                settings: settings.clone(),
                binary_format: Default::default(),
            },
            nested_decoder: NestedValues::create(settings),
        }
    }

    pub fn read_field(
        &self,
        column: &mut ColumnBuilder,
        data: &[u8],
        allow_null: bool,
    ) -> Result<()> {
        match column {
            ColumnBuilder::Null { len } => {
                *len += 1;
                Ok(())
            }
            ColumnBuilder::Binary(c) => {
                let data = decode_binary(data, self.common_settings.binary_format)?;
                c.put_slice(&data);
                c.commit_row();
                Ok(())
            }
            ColumnBuilder::String(c) => {
                c.put_and_commit(std::str::from_utf8(data)?);
                Ok(())
            }
            ColumnBuilder::Boolean(c) => self.read_bool(c, data),
            ColumnBuilder::Nullable(c) => self.read_nullable(c, data, allow_null),
            ColumnBuilder::Number(c) => with_number_mapped_type!(|NUM_TYPE| match c {
                NumberColumnBuilder::NUM_TYPE(c) => {
                    if NUM_TYPE::FLOATING {
                        self.read_float(c, data)
                    } else {
                        self.read_int(c, data)
                    }
                }
            }),
            ColumnBuilder::Decimal(c) => with_decimal_type!(|DECIMAL_TYPE| match c {
                DecimalColumnBuilder::DECIMAL_TYPE(c, size) => self.read_decimal(c, *size, data),
            }),
            ColumnBuilder::Date(c) => self.read_date(c, data),
            ColumnBuilder::Interval(c) => self.read_interval(c, data),
            ColumnBuilder::Timestamp(c) => self.read_timestamp(c, data),
            ColumnBuilder::TimestampTz(c) => self.read_timestamp_tz(c, data),
            ColumnBuilder::Array(c) => self.read_array(c, data),
            ColumnBuilder::Map(c) => self.read_map(c, data),
            ColumnBuilder::Bitmap(c) => self.read_bitmap(c, data),
            ColumnBuilder::Tuple(fields) => self.read_tuple(fields, data),
            ColumnBuilder::Variant(c) => self.read_variant(c, data),
            ColumnBuilder::Geometry(c) => self.read_geometry(c, data),
            ColumnBuilder::Geography(c) => self.read_geography(c, data),
            ColumnBuilder::Vector(c) => self.read_vector(c, data),
            ColumnBuilder::EmptyArray { .. } => {
                unreachable!("EmptyArray")
            }
            ColumnBuilder::EmptyMap { .. } => {
                unreachable!("EmptyMap")
            }
            ColumnBuilder::Opaque(_) => Err(ErrorCode::Unimplemented(
                "Opaque type not supported in separated_text",
            )),
        }
    }

    fn read_bool(&self, column: &mut MutableBitmap, data: &[u8]) -> Result<()> {
        if data == b"0" || data == b"false" {
            column.push(false);
        } else if data == b"1" || data == b"true" {
            column.push(true);
        } else {
            let err_msg = format!(
                "Incorrect boolean value({}), expect one of 0/1/true/false",
                String::from_utf8_lossy(data)
            );
            return Err(ErrorCode::BadBytes(err_msg));
        }
        Ok(())
    }

    fn read_nullable(
        &self,
        column: &mut NullableColumnBuilder<AnyType>,
        data: &[u8],
        allow_null: bool,
    ) -> Result<()> {
        if allow_null {
            for null in &self.common_settings.null_if {
                if data == null {
                    column.push_null();
                    return Ok(());
                }
            }
        }
        self.read_field(&mut column.builder, data, false)?;
        column.validity.push(true);
        Ok(())
    }

    fn read_int<T>(&self, column: &mut Vec<T>, data: &[u8]) -> Result<()>
    where
        T: Number + From<T::Native>,
        T::Native: FromLexical + NumCast,
    {
        // can not use read_num_text_exact directly, because we need to allow int like '1.0'
        let (n_in, effective) = collect_number(data);
        if n_in != data.len() {
            return Err(ErrorCode::BadBytes("invalid text for number"));
        }
        let val: Result<T::Native> = read_num_text_exact(&data[..effective]);
        let v = match val {
            Ok(v) => v,
            Err(_) => {
                // cast float value to integer value
                let val: f64 = read_num_text_exact(&data[..effective])?;
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

    fn read_float<T>(&self, column: &mut Vec<T>, data: &[u8]) -> Result<()>
    where
        T: Number + From<T::Native>,
        T::Native: FromLexical,
    {
        let v: T::Native = read_num_text_exact(data)?;
        column.push(v.into());
        Ok(())
    }

    fn read_decimal<D: Decimal>(
        &self,
        column: &mut Vec<D>,
        size: DecimalSize,
        data: &[u8],
    ) -> Result<()> {
        let (n, n_read) = read_decimal_with_size(data, size, false, true)?;
        if n_read != data.len() {
            return Err(ErrorCode::BadBytes(
                "unexpected remaining bytes".to_string(),
            ));
        }
        column.push(n);
        Ok(())
    }

    fn read_date(&self, column: &mut Vec<i32>, data: &[u8]) -> Result<()> {
        let mut buffer_readr = Cursor::new(&data);
        let date = buffer_readr.read_date_text(&self.common_settings.settings.jiff_timezone)?;
        let days = uniform_date(date);
        column.push(clamp_date(days as i64));
        Ok(())
    }

    fn read_interval(&self, column: &mut Vec<months_days_micros>, data: &[u8]) -> Result<()> {
        let res = std::str::from_utf8(data).map_err_to_code(ErrorCode::BadBytes, || {
            format!(
                "UTF-8 Conversion Failed: Unable to convert value {:?} to UTF-8",
                data
            )
        })?;
        let i = Interval::from_string(res)?;
        column.push(months_days_micros::new(i.months, i.days, i.micros));
        Ok(())
    }

    fn read_timestamp(&self, column: &mut Vec<i64>, data: &[u8]) -> Result<()> {
        read_timestamp(column, data, &self.common_settings)
    }

    fn read_timestamp_tz(&self, column: &mut Vec<timestamp_tz>, data: &[u8]) -> Result<()> {
        read_timestamp_tz(column, data, &self.common_settings)
    }

    fn read_bitmap(&self, column: &mut BinaryColumnBuilder, data: &[u8]) -> Result<()> {
        let rb = parse_bitmap(data)?;
        rb.serialize_into(&mut column.data).unwrap();
        column.commit_row();
        Ok(())
    }

    fn read_variant(&self, column: &mut BinaryColumnBuilder, data: &[u8]) -> Result<()> {
        match parse_owned_jsonb_with_buf(data, &mut column.data) {
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

    fn read_geometry(&self, column: &mut BinaryColumnBuilder, data: &[u8]) -> Result<()> {
        let geom = parse_bytes_to_ewkb(data, None)?;
        column.put_slice(geom.as_bytes());
        column.commit_row();
        Ok(())
    }

    fn read_geography(&self, column: &mut BinaryColumnBuilder, data: &[u8]) -> Result<()> {
        let geog = geography_from_ewkt_bytes(data)?;
        column.put_slice(geog.as_bytes());
        column.commit_row();
        Ok(())
    }

    fn read_array(&self, column: &mut ArrayColumnBuilder<AnyType>, data: &[u8]) -> Result<()> {
        let mut cursor = Cursor::new(data);
        self.nested_decoder.read_array(column, &mut cursor)
    }

    fn read_map(&self, column: &mut ArrayColumnBuilder<AnyType>, data: &[u8]) -> Result<()> {
        let mut cursor = Cursor::new(data);
        self.nested_decoder.read_map(column, &mut cursor)
    }

    fn read_tuple(&self, column: &mut [ColumnBuilder], data: &[u8]) -> Result<()> {
        let mut cursor = Cursor::new(data);
        self.nested_decoder.read_tuple(column, &mut cursor)
    }

    fn read_vector(&self, column: &mut VectorColumnBuilder, data: &[u8]) -> Result<()> {
        let mut cursor = Cursor::new(data);
        self.nested_decoder.read_vector(column, &mut cursor)
    }
}
