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

use base64::Engine as _;
use base64::engine::general_purpose;
use databend_common_base::base::OrderedFloat;
use databend_common_column::types::months_days_micros;
use databend_common_column::types::timestamp_tz;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::Column;
use databend_common_expression::types::AnyType;
use databend_common_expression::types::BinaryColumn;
use databend_common_expression::types::Bitmap;
use databend_common_expression::types::Buffer;
use databend_common_expression::types::NumberColumn;
use databend_common_expression::types::VectorColumn;
use databend_common_expression::types::VectorScalarRef;
use databend_common_expression::types::array::ArrayColumn;
use databend_common_expression::types::date::date_to_string;
use databend_common_expression::types::decimal::DecimalColumn;
use databend_common_expression::types::geography::GeographyColumn;
use databend_common_expression::types::interval::interval_to_string;
use databend_common_expression::types::nullable::NullableColumn;
use databend_common_expression::types::opaque::OpaqueColumn;
use databend_common_expression::types::string::StringColumn;
use databend_common_expression::types::timestamp::timestamp_to_string;
use databend_common_io::GeometryDataType;
use databend_common_io::constants::FALSE_BYTES_LOWER;
use databend_common_io::constants::FALSE_BYTES_NUM;
use databend_common_io::constants::INF_BYTES_LONG;
use databend_common_io::constants::NAN_BYTES_SNAKE;
use databend_common_io::constants::NULL_BYTES_ESCAPE;
use databend_common_io::constants::NULL_BYTES_UPPER;
use databend_common_io::constants::TRUE_BYTES_LOWER;
use databend_common_io::constants::TRUE_BYTES_NUM;
use databend_common_io::ewkb_to_geo;
use databend_common_io::geo_to_ewkb;
use databend_common_io::geo_to_ewkt;
use databend_common_io::geo_to_json;
use databend_common_io::geo_to_wkb;
use databend_common_io::geo_to_wkt;
use databend_common_io::prelude::BinaryDisplayFormat;
use databend_common_io::prelude::OutputFormatSettings;
use databend_common_meta_app::principal::CsvFileFormatParams;
use databend_common_meta_app::principal::TsvFileFormatParams;
use geozero::wkb::Ewkb;
use jsonb::RawJsonb;
use lexical_core::ToLexical;
use micromarshal::Marshal;
use micromarshal::Unmarshal;

use crate::OutputCommonSettings;
use crate::field_encoder::helpers::PrimitiveWithFormat;
use crate::field_encoder::helpers::write_quoted_string;

pub struct FieldEncoderValues {
    pub common_settings: OutputCommonSettings,

    pub escape_char: u8,
    pub quote_char: u8,
}

impl FieldEncoderValues {
    pub fn create_for_csv(
        params: &CsvFileFormatParams,
        mut settings: OutputFormatSettings,
    ) -> Self {
        settings.binary_format = params.binary_format.to_display_format();
        settings.geometry_format = params.geometry_format;
        FieldEncoderValues {
            common_settings: OutputCommonSettings {
                true_bytes: TRUE_BYTES_LOWER.as_bytes().to_vec(),
                false_bytes: FALSE_BYTES_LOWER.as_bytes().to_vec(),
                null_bytes: params.null_display.as_bytes().to_vec(),
                nan_bytes: params.nan_display.as_bytes().to_vec(),
                inf_bytes: INF_BYTES_LONG.as_bytes().to_vec(),
                settings: settings.clone(),
            },
            escape_char: 0, // not used
            quote_char: 0,  // not used
        }
    }

    pub fn create_for_tsv(params: &TsvFileFormatParams, settings: OutputFormatSettings) -> Self {
        FieldEncoderValues {
            common_settings: OutputCommonSettings {
                true_bytes: TRUE_BYTES_NUM.as_bytes().to_vec(),
                false_bytes: FALSE_BYTES_NUM.as_bytes().to_vec(),
                null_bytes: NULL_BYTES_ESCAPE.as_bytes().to_vec(),
                nan_bytes: params.nan_display.as_bytes().to_vec(),
                inf_bytes: INF_BYTES_LONG.as_bytes().to_vec(),
                settings: settings.clone(),
            },
            escape_char: 0, // not used
            quote_char: 0,  // not used
        }
    }

    pub fn create_for_http_handler(settings: &OutputFormatSettings) -> Self {
        FieldEncoderValues {
            common_settings: OutputCommonSettings {
                true_bytes: TRUE_BYTES_NUM.as_bytes().to_vec(),
                false_bytes: FALSE_BYTES_NUM.as_bytes().to_vec(),
                null_bytes: NULL_BYTES_UPPER.as_bytes().to_vec(),
                nan_bytes: NAN_BYTES_SNAKE.as_bytes().to_vec(),
                inf_bytes: INF_BYTES_LONG.as_bytes().to_vec(),
                settings: settings.clone(),
            },
            escape_char: b'\\',
            quote_char: b'"',
        }
    }

    // JDBC only accept "NaN" and "Infinity".
    // mysql python client will decode to python float, which is printed as 'nan' and 'inf'
    // so we still use 'nan' and 'inf' in logic test.
    // https://github.com/datafuselabs/databend/discussions/8941
    pub fn create_for_mysql_handler(settings: &OutputFormatSettings) -> Self {
        FieldEncoderValues {
            common_settings: OutputCommonSettings {
                true_bytes: TRUE_BYTES_NUM.as_bytes().to_vec(),
                false_bytes: FALSE_BYTES_NUM.as_bytes().to_vec(),
                null_bytes: NULL_BYTES_UPPER.as_bytes().to_vec(),
                nan_bytes: NAN_BYTES_SNAKE.as_bytes().to_vec(),
                inf_bytes: INF_BYTES_LONG.as_bytes().to_vec(),
                settings: settings.clone(),
            },
            escape_char: b'\\',
            quote_char: b'"',
        }
    }

    pub fn write_field(
        &self,
        column: &Column,
        row_index: usize,
        out_buf: &mut Vec<u8>,
        in_nested: bool,
    ) -> Result<()> {
        match &column {
            Column::Null { .. } => self.write_null(out_buf),
            Column::EmptyArray { .. } => self.write_empty_array(out_buf),
            Column::EmptyMap { .. } => self.write_empty_map(out_buf),
            Column::Boolean(c) => self.write_bool(c, row_index, out_buf),
            Column::Number(col) => match col {
                NumberColumn::UInt8(c) => self.write_int(c, row_index, out_buf),
                NumberColumn::UInt16(c) => self.write_int(c, row_index, out_buf),
                NumberColumn::UInt32(c) => self.write_int(c, row_index, out_buf),
                NumberColumn::UInt64(c) => self.write_int(c, row_index, out_buf),
                NumberColumn::Int8(c) => self.write_int(c, row_index, out_buf),
                NumberColumn::Int16(c) => self.write_int(c, row_index, out_buf),
                NumberColumn::Int32(c) => self.write_int(c, row_index, out_buf),
                NumberColumn::Int64(c) => self.write_int(c, row_index, out_buf),
                NumberColumn::Float32(c) => self.write_float(c, row_index, out_buf),
                NumberColumn::Float64(c) => self.write_float(c, row_index, out_buf),
            },
            Column::Decimal(c) => self.write_decimal(c, row_index, out_buf),

            Column::Nullable(box c) => self.write_nullable(c, row_index, out_buf, in_nested)?,

            Column::Binary(c) => self.write_binary(c, row_index, out_buf)?,
            Column::String(c) => self.write_string(c, row_index, out_buf, in_nested),
            Column::Date(c) => self.write_date(c, row_index, out_buf, in_nested),
            Column::Interval(c) => self.write_interval(c, row_index, out_buf, in_nested),
            Column::Timestamp(c) => self.write_timestamp(c, row_index, out_buf, in_nested),
            Column::TimestampTz(c) => self.write_timestamp_tz(c, row_index, out_buf, in_nested),
            Column::Bitmap(b) => self.write_bitmap(b, row_index, out_buf, in_nested),
            Column::Variant(c) => self.write_variant(c, row_index, out_buf),
            Column::Geometry(c) => self.write_geometry(c, row_index, out_buf, in_nested),
            Column::Geography(c) => self.write_geography(c, row_index, out_buf, in_nested),

            Column::Array(box c) => self.write_array(c, row_index, out_buf)?,
            Column::Map(box c) => self.write_map(c, row_index, out_buf)?,
            Column::Tuple(fields) => self.write_tuple(fields, row_index, out_buf)?,
            Column::Vector(c) => self.write_vector(c, row_index, out_buf),
            Column::Opaque(c) => self.write_opaque(c, row_index, out_buf),
        }
        Ok(())
    }

    fn write_string_inner(&self, in_buf: &[u8], out_buf: &mut Vec<u8>, in_nested: bool) {
        if in_nested {
            out_buf.push(self.quote_char);
            // currently we do not support `Values` Output Format,
            // only use FieldEncoderValues internally.
            // so we do not expect the scalar literal to be used in sql.
            // it is better to keep it simple: minimal escape.
            // it make result easier to decode csv, tsv and http handler result.
            write_quoted_string(in_buf, out_buf, self.escape_char, self.quote_char);
            out_buf.push(self.quote_char);
        } else {
            out_buf.extend_from_slice(in_buf);
        }
    }

    fn write_bool(&self, column: &Bitmap, row_index: usize, out_buf: &mut Vec<u8>) {
        let v = if column.get_bit(row_index) {
            &self.common_settings.true_bytes
        } else {
            &self.common_settings.false_bytes
        };

        out_buf.extend_from_slice(v);
    }

    pub fn write_null(&self, out_buf: &mut Vec<u8>) {
        out_buf.extend_from_slice(&self.common_settings.null_bytes);
    }

    fn write_empty_array(&self, out_buf: &mut Vec<u8>) {
        out_buf.extend_from_slice(b"[]");
    }

    fn write_empty_map(&self, out_buf: &mut Vec<u8>) {
        out_buf.extend_from_slice(b"{}");
    }

    fn write_nullable(
        &self,
        column: &NullableColumn<AnyType>,
        row_index: usize,
        out_buf: &mut Vec<u8>,
        in_nested: bool,
    ) -> Result<()> {
        if !column.validity.get_bit(row_index) {
            self.write_null(out_buf);
            Ok(())
        } else {
            self.write_field(&column.column, row_index, out_buf, in_nested)
        }
    }

    fn write_int<T>(&self, column: &Buffer<T>, row_index: usize, out_buf: &mut Vec<u8>)
    where T: Marshal + Unmarshal<T> + ToLexical + PrimitiveWithFormat {
        let v = unsafe { column.get_unchecked(row_index) };
        v.write_field(out_buf, &self.common_settings)
    }

    fn write_float<T>(
        &self,
        column: &Buffer<OrderedFloat<T>>,
        row_index: usize,
        out_buf: &mut Vec<u8>,
    ) where
        T: Marshal + Unmarshal<T> + ToLexical + PrimitiveWithFormat,
    {
        let v = unsafe { column.get_unchecked(row_index) };
        v.0.write_field(out_buf, &self.common_settings)
    }

    fn write_decimal(&self, column: &DecimalColumn, row_index: usize, out_buf: &mut Vec<u8>) {
        let data = column.index(row_index).unwrap().to_string();
        out_buf.extend_from_slice(data.as_bytes());
    }

    fn write_binary(
        &self,
        column: &BinaryColumn,
        row_index: usize,
        out_buf: &mut Vec<u8>,
    ) -> Result<()> {
        let v = unsafe { column.index_unchecked(row_index) };
        match self.common_settings.settings.binary_format {
            BinaryDisplayFormat::Hex => {
                out_buf.extend_from_slice(hex::encode_upper(v).as_bytes());
                Ok(())
            }
            BinaryDisplayFormat::Base64 => {
                let encoded = general_purpose::STANDARD.encode(v);
                out_buf.extend_from_slice(encoded.as_bytes());
                Ok(())
            }
            BinaryDisplayFormat::Utf8 => match std::str::from_utf8(v) {
                Ok(text) => {
                    out_buf.extend_from_slice(text.as_bytes());
                    Ok(())
                }
                Err(err) => Err(ErrorCode::InvalidUtf8String(format!(
                    "Invalid UTF-8 sequence while formatting binary column: {err}. Consider \
setting binary_output_format to 'UTF-8-LOSSY'."
                ))),
            },
            BinaryDisplayFormat::Utf8Lossy => {
                out_buf.extend_from_slice(String::from_utf8_lossy(v).as_bytes());
                Ok(())
            }
        }
    }

    fn write_string(
        &self,
        column: &StringColumn,
        row_index: usize,
        out_buf: &mut Vec<u8>,
        in_nested: bool,
    ) {
        self.write_string_inner(
            unsafe { column.index_unchecked(row_index).as_bytes() },
            out_buf,
            in_nested,
        );
    }

    fn write_date(
        &self,
        column: &Buffer<i32>,
        row_index: usize,
        out_buf: &mut Vec<u8>,
        in_nested: bool,
    ) {
        let v = unsafe { column.get_unchecked(row_index) };
        let s = date_to_string(*v as i64, &self.common_settings.settings.jiff_timezone).to_string();
        self.write_string_inner(s.as_bytes(), out_buf, in_nested);
    }

    fn write_interval(
        &self,
        column: &Buffer<months_days_micros>,
        row_index: usize,
        out_buf: &mut Vec<u8>,
        in_nested: bool,
    ) {
        let v = unsafe { column.get_unchecked(row_index) };
        let s = interval_to_string(v).to_string();
        self.write_string_inner(s.as_bytes(), out_buf, in_nested);
    }

    fn write_timestamp(
        &self,
        column: &Buffer<i64>,
        row_index: usize,
        out_buf: &mut Vec<u8>,
        in_nested: bool,
    ) {
        let v = unsafe { column.get_unchecked(row_index) };
        let s = timestamp_to_string(*v, &self.common_settings.settings.jiff_timezone).to_string();
        self.write_string_inner(s.as_bytes(), out_buf, in_nested);
    }

    fn write_timestamp_tz(
        &self,
        column: &Buffer<timestamp_tz>,
        row_index: usize,
        out_buf: &mut Vec<u8>,
        in_nested: bool,
    ) {
        let v = unsafe { column.get_unchecked(row_index) };
        self.write_string_inner(v.to_string().as_bytes(), out_buf, in_nested);
    }

    fn write_bitmap(
        &self,
        _column: &BinaryColumn,
        _row_index: usize,
        out_buf: &mut Vec<u8>,
        in_nested: bool,
    ) {
        let bitmap_result = "<bitmap binary>".as_bytes();
        self.write_string_inner(bitmap_result, out_buf, in_nested);
    }

    fn write_variant(&self, column: &BinaryColumn, row_index: usize, out_buf: &mut Vec<u8>) {
        let v = unsafe { column.index_unchecked(row_index) };
        let s = RawJsonb::new(v).to_string();
        out_buf.extend_from_slice(s.as_bytes());
    }

    fn write_geometry(
        &self,
        column: &BinaryColumn,
        row_index: usize,
        out_buf: &mut Vec<u8>,
        in_nested: bool,
    ) {
        let v = unsafe { column.index_unchecked(row_index) };
        let s = ewkb_to_geo(&mut Ewkb(v))
            .and_then(
                |(geo, srid)| match self.common_settings.settings.geometry_format {
                    GeometryDataType::WKB => {
                        geo_to_wkb(geo).map(|v| hex::encode_upper(v).into_bytes())
                    }
                    GeometryDataType::WKT => geo_to_wkt(geo).map(|v| v.as_bytes().to_vec()),
                    GeometryDataType::EWKB => {
                        geo_to_ewkb(geo, srid).map(|v| hex::encode_upper(v).into_bytes())
                    }
                    GeometryDataType::EWKT => geo_to_ewkt(geo, srid).map(|v| v.as_bytes().to_vec()),
                    GeometryDataType::GEOJSON => geo_to_json(geo).map(|v| v.as_bytes().to_vec()),
                },
            )
            .unwrap_or_else(|_| v.to_vec());

        match self.common_settings.settings.geometry_format {
            GeometryDataType::GEOJSON => {
                out_buf.extend_from_slice(&s);
            }
            _ => {
                self.write_string_inner(&s, out_buf, in_nested);
            }
        }
    }

    fn write_geography(
        &self,
        column: &GeographyColumn,
        row_index: usize,
        out_buf: &mut Vec<u8>,
        in_nested: bool,
    ) {
        let v = unsafe { column.index_unchecked(row_index) };
        let s = ewkb_to_geo(&mut Ewkb(v.0))
            .and_then(
                |(geo, srid)| match self.common_settings.settings.geometry_format {
                    GeometryDataType::WKB => {
                        geo_to_wkb(geo).map(|v| hex::encode_upper(v).into_bytes())
                    }
                    GeometryDataType::WKT => geo_to_wkt(geo).map(|v| v.as_bytes().to_vec()),
                    GeometryDataType::EWKB => {
                        geo_to_ewkb(geo, srid).map(|v| hex::encode_upper(v).into_bytes())
                    }
                    GeometryDataType::EWKT => geo_to_ewkt(geo, srid).map(|v| v.as_bytes().to_vec()),
                    GeometryDataType::GEOJSON => geo_to_json(geo).map(|v| v.as_bytes().to_vec()),
                },
            )
            .unwrap_or_else(|_| v.0.to_vec());

        match self.common_settings.settings.geometry_format {
            GeometryDataType::GEOJSON => {
                out_buf.extend_from_slice(&s);
            }
            _ => {
                self.write_string_inner(&s, out_buf, in_nested);
            }
        }
    }

    fn write_array(
        &self,
        column: &ArrayColumn<AnyType>,
        row_index: usize,
        out_buf: &mut Vec<u8>,
    ) -> Result<()> {
        let start = unsafe { *column.offsets().get_unchecked(row_index) as usize };
        let end = unsafe { *column.offsets().get_unchecked(row_index + 1) as usize };
        out_buf.push(b'[');
        let inner = column.values();
        for i in start..end {
            if i != start {
                out_buf.push(b',');
            }
            self.write_field(inner, i, out_buf, true)?;
        }
        out_buf.push(b']');
        Ok(())
    }

    fn write_map(
        &self,
        column: &ArrayColumn<AnyType>,
        row_index: usize,
        out_buf: &mut Vec<u8>,
    ) -> Result<()> {
        let start = unsafe { *column.offsets().get_unchecked(row_index) as usize };
        let end = unsafe { *column.offsets().get_unchecked(row_index + 1) as usize };
        out_buf.push(b'{');
        let inner = column.values();
        match inner {
            Column::Tuple(fields) => {
                for i in start..end {
                    if i != start {
                        out_buf.push(b',');
                    }
                    self.write_field(&fields[0], i, out_buf, true)?;
                    out_buf.push(b':');
                    self.write_field(&fields[1], i, out_buf, true)?;
                }
            }
            _ => unreachable!(),
        }
        out_buf.push(b'}');
        Ok(())
    }

    fn write_tuple(
        &self,
        columns: &[Column],
        row_index: usize,
        out_buf: &mut Vec<u8>,
    ) -> Result<()> {
        out_buf.push(b'(');
        for (i, inner) in columns.iter().enumerate() {
            if i > 0 {
                out_buf.push(b',');
            }
            self.write_field(inner, row_index, out_buf, true)?;
        }
        out_buf.push(b')');
        Ok(())
    }

    pub fn write_vector(&self, column: &VectorColumn, row_index: usize, out_buf: &mut Vec<u8>) {
        let scalar_val = unsafe { column.index_unchecked(row_index) };
        out_buf.push(b'[');
        match scalar_val {
            VectorScalarRef::Int8(values) => {
                for (i, v) in values.iter().enumerate() {
                    if i > 0 {
                        out_buf.push(b',');
                    }
                    v.write_field(out_buf, &self.common_settings);
                }
            }
            VectorScalarRef::Float32(values) => {
                for (i, v) in values.iter().enumerate() {
                    if i > 0 {
                        out_buf.push(b',');
                    }
                    v.0.write_field(out_buf, &self.common_settings);
                }
            }
        }
        out_buf.push(b']');
    }

    fn write_opaque(&self, column: &OpaqueColumn, row_index: usize, out_buf: &mut Vec<u8>) {
        let scalar = unsafe { column.index_unchecked(row_index) };
        let hex_bytes = hex::encode_upper(scalar.to_le_bytes());
        out_buf.extend_from_slice(hex_bytes.as_bytes());
    }
}
