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

use databend_common_exception::Result;
use databend_common_expression::Column;
use databend_common_expression::types::AnyType;
use databend_common_expression::types::decimal::DecimalColumn;
use databend_common_expression::types::decimal::DecimalScalar;
use databend_common_expression::types::nullable::NullableColumn;
use databend_common_expression::types::opaque::OpaqueColumn;
use databend_common_io::constants::FALSE_BYTES_LOWER;
use databend_common_io::constants::FALSE_BYTES_NUM;
use databend_common_io::constants::INF_BYTES_LONG;
use databend_common_io::constants::NULL_BYTES_ESCAPE;
use databend_common_io::constants::TRUE_BYTES_LOWER;
use databend_common_io::constants::TRUE_BYTES_NUM;
use databend_common_io::display_decimal_128_trimmed;
use databend_common_io::display_decimal_256_trimmed;
use databend_common_io::prelude::BinaryDisplayFormat;
use databend_common_meta_app::principal::BinaryFormat;
use databend_common_meta_app::principal::CsvFileFormatParams;
use databend_common_meta_app::principal::TsvFileFormatParams;
use geozero::ToWkt;
use geozero::wkb::Ewkb;

use crate::FileFormatOptionsExt;
use crate::OutputCommonSettings;
use crate::field_encoder::FieldEncoderValues;
use crate::field_encoder::write_tsv_escaped_string;

pub enum StringFormatter {
    Csv { quote_char: u8 },
    Tsv { record_delimiter: u8 },
}

impl StringFormatter {
    fn write_string(&self, bytes: &[u8], buf: &mut Vec<u8>) {
        match self {
            StringFormatter::Csv { quote_char } => write_csv_string(bytes, buf, *quote_char),
            StringFormatter::Tsv { record_delimiter } => {
                write_tsv_escaped_string(bytes, buf, *record_delimiter)
            }
        }
    }
}

// todo(youngsofun): support quote style
pub fn write_csv_string(bytes: &[u8], buf: &mut Vec<u8>, quote: u8) {
    buf.push(quote);
    let mut start = 0;

    for (i, &byte) in bytes.iter().enumerate() {
        if byte == quote {
            if start < i {
                buf.extend_from_slice(&bytes[start..i]);
            }
            buf.push(quote);
            buf.push(quote);
            start = i + 1;
        }
    }

    if start != bytes.len() {
        buf.extend_from_slice(&bytes[start..]);
    }
    buf.push(quote);
}

pub struct FieldEncoderCSV {
    pub simple: FieldEncoderValues,
    pub nested: FieldEncoderValues,
    pub string_formatter: StringFormatter,
}

impl FieldEncoderCSV {
    pub fn create_csv(params: &CsvFileFormatParams, options_ext: &FileFormatOptionsExt) -> Self {
        let binary_format = Self::binary_settings(params, options_ext);
        let mut nested_options = options_ext.clone();
        nested_options.binary_format = binary_format;
        Self {
            nested: FieldEncoderValues::create(&nested_options),
            simple: FieldEncoderValues {
                common_settings: OutputCommonSettings {
                    true_bytes: TRUE_BYTES_LOWER.as_bytes().to_vec(),
                    false_bytes: FALSE_BYTES_LOWER.as_bytes().to_vec(),
                    null_bytes: params.null_display.as_bytes().to_vec(),
                    nan_bytes: params.nan_display.as_bytes().to_vec(),
                    inf_bytes: INF_BYTES_LONG.as_bytes().to_vec(),
                    jiff_timezone: options_ext.jiff_timezone.clone(),
                    binary_format: params.binary_format.to_display_format(),
                    geometry_format: params.geometry_format,
                },
                escape_char: 0, // not used
                quote_char: 0,  // not used
                binary_format,
            },
            string_formatter: StringFormatter::Csv {
                quote_char: params.quote.as_bytes()[0],
            },
        }
    }

    pub fn create_tsv(params: &TsvFileFormatParams, options_ext: &FileFormatOptionsExt) -> Self {
        Self {
            nested: FieldEncoderValues::create(options_ext),
            simple: FieldEncoderValues {
                common_settings: OutputCommonSettings {
                    true_bytes: TRUE_BYTES_NUM.as_bytes().to_vec(),
                    false_bytes: FALSE_BYTES_NUM.as_bytes().to_vec(),
                    null_bytes: NULL_BYTES_ESCAPE.as_bytes().to_vec(),
                    nan_bytes: params.nan_display.as_bytes().to_vec(),
                    inf_bytes: INF_BYTES_LONG.as_bytes().to_vec(),
                    jiff_timezone: options_ext.jiff_timezone.clone(),
                    binary_format: Default::default(),
                    geometry_format: Default::default(),
                },
                escape_char: 0, // not used
                quote_char: 0,  // not used
                binary_format: options_ext.binary_format,
            },
            string_formatter: StringFormatter::Tsv {
                record_delimiter: params.field_delimiter.as_bytes().to_vec()[0],
            },
        }
    }

    fn binary_settings(
        params: &CsvFileFormatParams,
        options_ext: &FileFormatOptionsExt,
    ) -> BinaryDisplayFormat {
        if options_ext.is_select {
            options_ext.binary_format
        } else {
            match params.binary_format {
                BinaryFormat::Hex => BinaryDisplayFormat::Hex,
                BinaryFormat::Base64 => BinaryDisplayFormat::Base64,
            }
        }
    }

    pub(crate) fn write_field(
        &self,
        column: &Column,
        row_index: usize,
        out_buf: &mut Vec<u8>,
    ) -> Result<()> {
        match &column {
            Column::Nullable(box c) => self.write_nullable(c, row_index, out_buf)?,

            Column::Binary(c) => {
                let buf = unsafe { c.index_unchecked(row_index) };
                let encoded = self.simple.common_settings.binary_format.encode(buf)?;
                out_buf.extend_from_slice(&encoded);
            }
            Column::Opaque(c) => self.write_opaque(c, row_index, out_buf)?,
            Column::String(c) => {
                let buf = unsafe { c.index_unchecked(row_index) };
                self.string_formatter.write_string(buf.as_bytes(), out_buf);
            }

            Column::Date(..)
            | Column::Timestamp(..)
            | Column::TimestampTz(..)
            | Column::Bitmap(..)
            | Column::Variant(..)
            | Column::Interval(_) => {
                let mut buf = Vec::new();
                self.simple
                    .write_field(column, row_index, &mut buf, false)?;
                self.string_formatter.write_string(&buf, out_buf);
            }

            Column::Geometry(g) => {
                let buf = unsafe { g.index_unchecked(row_index) };
                let geom = Ewkb(buf).to_ewkt(None).unwrap();
                self.string_formatter.write_string(geom.as_bytes(), out_buf);
            }

            Column::Geography(g) => {
                let geog = unsafe { g.index_unchecked(row_index) };
                let wkt = geog.to_ewkt().unwrap();
                self.string_formatter.write_string(wkt.as_bytes(), out_buf);
            }

            Column::Array(..) | Column::Map(..) | Column::Tuple(..) | Column::Vector(..) => {
                let mut buf = Vec::new();
                self.nested
                    .write_field(column, row_index, &mut buf, false)?;
                self.string_formatter.write_string(&buf, out_buf);
            }

            Column::Decimal(c) => self.write_decimal_trimmed(c, row_index, out_buf),

            Column::Null { .. }
            | Column::EmptyArray { .. }
            | Column::EmptyMap { .. }
            | Column::Number(_)
            | Column::Boolean(_) => self.simple.write_field(column, row_index, out_buf, false)?,
        }
        Ok(())
    }

    fn write_decimal_trimmed(
        &self,
        column: &DecimalColumn,
        row_index: usize,
        out_buf: &mut Vec<u8>,
    ) {
        let scalar = column.index(row_index).unwrap();
        let data = match scalar {
            DecimalScalar::Decimal64(v, size) => {
                display_decimal_128_trimmed(v as i128, size.scale()).to_string()
            }
            DecimalScalar::Decimal128(v, size) => {
                display_decimal_128_trimmed(v, size.scale()).to_string()
            }
            DecimalScalar::Decimal256(v, size) => {
                display_decimal_256_trimmed(v.0, size.scale()).to_string()
            }
        };
        out_buf.extend_from_slice(data.as_bytes());
    }

    fn write_nullable(
        &self,
        column: &NullableColumn<AnyType>,
        row_index: usize,
        out_buf: &mut Vec<u8>,
    ) -> Result<()> {
        if !column.validity.get_bit(row_index) {
            self.simple.write_null(out_buf);
            Ok(())
        } else {
            self.write_field(&column.column, row_index, out_buf)
        }
    }

    fn write_opaque(
        &self,
        column: &OpaqueColumn,
        row_index: usize,
        out_buf: &mut Vec<u8>,
    ) -> Result<()> {
        let scalar = unsafe { column.index_unchecked(row_index) };
        let bytes = scalar.to_le_bytes();
        let encoded = self.simple.common_settings.binary_format.encode(&bytes)?;
        out_buf.extend_from_slice(&encoded);
        Ok(())
    }
}
