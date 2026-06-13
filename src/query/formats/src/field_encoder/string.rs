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

use std::borrow::Cow;
use std::fmt::Write as _;

use base64::Engine as _;
use base64::engine::general_purpose;
use databend_common_column::types::timestamp_tz;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::Column;
use databend_common_expression::types::AnyType;
use databend_common_expression::types::BinaryColumn;
use databend_common_expression::types::NumberColumn;
use databend_common_expression::types::VectorColumn;
use databend_common_expression::types::VectorScalarRef;
use databend_common_expression::types::array::ArrayColumn;
use databend_common_expression::types::date::date_to_string;
use databend_common_expression::types::interval::interval_to_string;
use databend_common_expression::types::opaque::OpaqueColumn;
use databend_common_expression::types::timestamp::timestamp_to_string;
use databend_common_io::GEOGRAPHY_SRID;
use databend_common_io::GeometryDataType;
use databend_common_io::constants::FALSE_BYTES_NUM;
use databend_common_io::constants::INF_BYTES_LONG;
use databend_common_io::constants::NAN_BYTES_SNAKE;
use databend_common_io::constants::NEG_INF_BYTES_LONG;
use databend_common_io::constants::NULL_BYTES_UPPER;
use databend_common_io::constants::TRUE_BYTES_NUM;
use databend_common_io::ewkb_to_geo;
use databend_common_io::geo_to_ewkb;
use databend_common_io::geo_to_ewkt;
use databend_common_io::geo_to_json;
use databend_common_io::geo_to_wkb;
use databend_common_io::geo_to_wkt;
use databend_common_io::prelude::BinaryDisplayFormat;
use databend_common_io::prelude::HttpHandlerDataFormat;
use databend_common_io::prelude::OutputFormatSettings;
use geozero::ToGeo;
use geozero::wkb::Ewkb;
use jsonb::RawJsonb;

const BITMAP_BINARY: &str = "<bitmap binary>";

pub struct FieldEncoderToString {
    pub settings: OutputFormatSettings,

    // for nested string
    pub escape_char: u8,
    pub quote_char: u8,
}

impl FieldEncoderToString {
    pub fn create(settings: &OutputFormatSettings) -> Self {
        Self {
            settings: settings.clone(),
            escape_char: b'\\',
            quote_char: b'"',
        }
    }

    pub fn encode<'a>(&self, column: &'a Column, row_index: usize) -> Result<Cow<'a, str>> {
        match column {
            Column::Null { .. } => Ok(Cow::Borrowed(NULL_BYTES_UPPER)),
            Column::EmptyArray { .. } => Ok(Cow::Borrowed("[]")),
            Column::EmptyMap { .. } => Ok(Cow::Borrowed("{}")),
            Column::Boolean(c) => Ok(Cow::Borrowed(self.bool_text(c.get_bit(row_index)))),
            Column::Number(c) => Ok(self.encode_number(c, row_index)),
            Column::Decimal(c) => Ok(Cow::Owned(c.index(row_index).unwrap().to_string())),
            Column::Binary(c) => Ok(self.encode_binary(c, row_index)?),
            Column::String(c) => Ok(Cow::Borrowed(unsafe { c.index_unchecked(row_index) })),
            Column::Timestamp(c) => Ok(Cow::Owned(
                self.timestamp_text(unsafe { *c.get_unchecked(row_index) }),
            )),
            Column::TimestampTz(c) => Ok(Cow::Owned(
                self.timestamp_tz_text(unsafe { *c.get_unchecked(row_index) }),
            )),
            Column::Date(c) => Ok(Cow::Owned(
                self.date_text(unsafe { *c.get_unchecked(row_index) }),
            )),
            Column::Interval(c) => Ok(Cow::Owned(
                self.interval_text(unsafe { c.get_unchecked(row_index) }),
            )),
            Column::Bitmap(_) => Ok(Cow::Borrowed(self.bitmap_text())),
            Column::Nullable(c) => {
                if !c.validity.get_bit(row_index) {
                    Ok(Cow::Borrowed(NULL_BYTES_UPPER))
                } else {
                    self.encode(&c.column, row_index)
                }
            }
            Column::Variant(c) => Ok(Cow::Owned(
                self.variant_text(unsafe { c.index_unchecked(row_index) }),
            )),
            Column::Geometry(c) => self.encode_geometry(unsafe { c.index_unchecked(row_index) }),
            Column::Geography(c) => {
                self.encode_geography(unsafe { c.index_unchecked(row_index).0 })
            }
            Column::Opaque(c) => Ok(Cow::Owned(self.encode_opaque(c, row_index))),
            Column::Array(box c) => {
                let mut out = String::new();
                self.write_array_root_to(c, row_index, &mut out)?;
                Ok(Cow::Owned(out))
            }
            Column::Map(box c) => {
                let mut out = String::new();
                self.write_map_root_to(c, row_index, &mut out)?;
                Ok(Cow::Owned(out))
            }
            Column::Tuple(fields) => {
                let mut out = String::new();
                self.write_tuple_root_to(fields, row_index, &mut out)?;
                Ok(Cow::Owned(out))
            }
            Column::Vector(c) => {
                let mut out = String::new();
                self.write_vector_to(c, row_index, &mut out);
                Ok(Cow::Owned(out))
            }
        }
    }

    fn write_nested_to(&self, column: &Column, row_index: usize, out: &mut String) -> Result<()> {
        match column {
            // simple
            Column::Null { .. } => out.push_str(NULL_BYTES_UPPER),
            Column::EmptyArray { .. } => out.push_str("[]"),
            Column::EmptyMap { .. } => out.push_str("{}"),
            Column::Nullable(c) => {
                if !c.validity.get_bit(row_index) {
                    out.push_str(NULL_BYTES_UPPER);
                } else {
                    self.write_nested_to(&c.column, row_index, out)?;
                }
            }
            Column::Boolean(c) => out.push_str(self.bool_text(c.get_bit(row_index))),
            Column::Number(c) => self.write_number_to(c, row_index, out),
            Column::Decimal(c) => write!(out, "{}", c.index(row_index).unwrap()).unwrap(),

            // quoted string
            Column::Binary(c) => out.push_str(self.encode_binary(c, row_index)?.as_ref()),
            Column::String(c) => {
                self.write_quoted_string(unsafe { c.index_unchecked(row_index) }, out)
            }
            Column::Timestamp(c) => self.write_quoted_string(
                self.timestamp_text(unsafe { *c.get_unchecked(row_index) })
                    .as_str(),
                out,
            ),
            Column::TimestampTz(c) => self.write_quoted_string(
                self.timestamp_tz_text(unsafe { *c.get_unchecked(row_index) })
                    .as_str(),
                out,
            ),
            Column::Date(c) => self.write_quoted_string(
                self.date_text(unsafe { *c.get_unchecked(row_index) })
                    .as_str(),
                out,
            ),
            Column::Interval(c) => self.write_quoted_string(
                self.interval_text(unsafe { c.get_unchecked(row_index) })
                    .as_str(),
                out,
            ),
            Column::Bitmap(_) => self.write_quoted_string(self.bitmap_text(), out),
            Column::Opaque(c) => self.write_quoted_string(&self.encode_opaque(c, row_index), out),

            // string or JSON
            Column::Geometry(c) => {
                self.write_geometry_nested_to(unsafe { c.index_unchecked(row_index) }, out)?
            }
            Column::Geography(c) => {
                self.write_geography_nested_to(unsafe { c.index_unchecked(row_index).0 }, out)?
            }

            // JSON
            Column::Variant(c) => {
                out.push_str(&self.variant_text(unsafe { c.index_unchecked(row_index) }))
            }

            // nested
            Column::Array(box c) => self.write_array_root_to(c, row_index, out)?,
            Column::Map(box c) => self.write_map_root_to(c, row_index, out)?,
            Column::Tuple(fields) => self.write_tuple_root_to(fields, row_index, out)?,
            Column::Vector(c) => self.write_vector_to(c, row_index, out),
        }
        Ok(())
    }

    #[inline]
    fn bool_text(&self, value: bool) -> &'static str {
        if value {
            TRUE_BYTES_NUM
        } else {
            FALSE_BYTES_NUM
        }
    }

    #[inline]
    fn bitmap_text(&self) -> &'static str {
        BITMAP_BINARY
    }

    #[inline]
    fn timestamp_text(&self, value: i64) -> String {
        match self.settings.http_json_result_mode {
            HttpHandlerDataFormat::Display => {
                timestamp_to_string(value, &self.settings.jiff_timezone).to_string()
            }
            HttpHandlerDataFormat::Driver => value.to_string(),
        }
    }

    #[inline]
    fn date_text(&self, value: i32) -> String {
        match self.settings.http_json_result_mode {
            HttpHandlerDataFormat::Display => {
                date_to_string(value as i64, &self.settings.jiff_timezone).to_string()
            }
            HttpHandlerDataFormat::Driver => value.to_string(),
        }
    }

    #[inline]
    fn interval_text(&self, value: &databend_common_column::types::months_days_micros) -> String {
        interval_to_string(value).to_string()
    }

    #[inline]
    fn timestamp_tz_text(&self, value: timestamp_tz) -> String {
        match self.settings.http_json_result_mode {
            HttpHandlerDataFormat::Display => value.to_string(),
            HttpHandlerDataFormat::Driver => {
                format!("{} {}", value.timestamp(), value.seconds_offset())
            }
        }
    }

    #[inline]
    fn variant_text(&self, value: &[u8]) -> String {
        RawJsonb::new(value).to_string()
    }

    fn encode_number<'a>(&self, column: &'a NumberColumn, row_index: usize) -> Cow<'a, str> {
        match column {
            NumberColumn::UInt8(c) => Cow::Owned(unsafe { c.get_unchecked(row_index) }.to_string()),
            NumberColumn::UInt16(c) => {
                Cow::Owned(unsafe { c.get_unchecked(row_index) }.to_string())
            }
            NumberColumn::UInt32(c) => {
                Cow::Owned(unsafe { c.get_unchecked(row_index) }.to_string())
            }
            NumberColumn::UInt64(c) => {
                Cow::Owned(unsafe { c.get_unchecked(row_index) }.to_string())
            }
            NumberColumn::Int8(c) => Cow::Owned(unsafe { c.get_unchecked(row_index) }.to_string()),
            NumberColumn::Int16(c) => Cow::Owned(unsafe { c.get_unchecked(row_index) }.to_string()),
            NumberColumn::Int32(c) => Cow::Owned(unsafe { c.get_unchecked(row_index) }.to_string()),
            NumberColumn::Int64(c) => Cow::Owned(unsafe { c.get_unchecked(row_index) }.to_string()),
            NumberColumn::Float32(c) => self.encode_f32(unsafe { c.get_unchecked(row_index) }.0),
            NumberColumn::Float64(c) => self.encode_f64(unsafe { c.get_unchecked(row_index) }.0),
        }
    }

    fn write_number_to(&self, column: &NumberColumn, row_index: usize, out: &mut String) {
        match column {
            NumberColumn::UInt8(c) => {
                write!(out, "{}", unsafe { c.get_unchecked(row_index) }).unwrap()
            }
            NumberColumn::UInt16(c) => {
                write!(out, "{}", unsafe { c.get_unchecked(row_index) }).unwrap()
            }
            NumberColumn::UInt32(c) => {
                write!(out, "{}", unsafe { c.get_unchecked(row_index) }).unwrap()
            }
            NumberColumn::UInt64(c) => {
                write!(out, "{}", unsafe { c.get_unchecked(row_index) }).unwrap()
            }
            NumberColumn::Int8(c) => {
                write!(out, "{}", unsafe { c.get_unchecked(row_index) }).unwrap()
            }
            NumberColumn::Int16(c) => {
                write!(out, "{}", unsafe { c.get_unchecked(row_index) }).unwrap()
            }
            NumberColumn::Int32(c) => {
                write!(out, "{}", unsafe { c.get_unchecked(row_index) }).unwrap()
            }
            NumberColumn::Int64(c) => {
                write!(out, "{}", unsafe { c.get_unchecked(row_index) }).unwrap()
            }
            NumberColumn::Float32(c) => {
                self.write_f32_to(unsafe { c.get_unchecked(row_index) }.0, out)
            }
            NumberColumn::Float64(c) => {
                self.write_f64_to(unsafe { c.get_unchecked(row_index) }.0, out)
            }
        }
    }

    fn encode_f32<'a>(&self, value: f32) -> Cow<'a, str> {
        match value {
            f32::INFINITY => Cow::Borrowed(INF_BYTES_LONG),
            f32::NEG_INFINITY => Cow::Borrowed(NEG_INF_BYTES_LONG),
            _ if value.is_nan() => Cow::Borrowed(NAN_BYTES_SNAKE),
            _ => {
                let mut buffer = zmij::Buffer::new();
                Cow::Owned(buffer.format_finite(value).to_string())
            }
        }
    }

    fn encode_f64<'a>(&self, value: f64) -> Cow<'a, str> {
        match value {
            f64::INFINITY => Cow::Borrowed(INF_BYTES_LONG),
            f64::NEG_INFINITY => Cow::Borrowed(NEG_INF_BYTES_LONG),
            _ if value.is_nan() => Cow::Borrowed(NAN_BYTES_SNAKE),
            _ => {
                let mut buffer = zmij::Buffer::new();
                Cow::Owned(buffer.format_finite(value).to_string())
            }
        }
    }

    fn write_f32_to(&self, value: f32, out: &mut String) {
        out.push_str(self.encode_f32(value).as_ref());
    }

    fn write_f64_to(&self, value: f64, out: &mut String) {
        out.push_str(self.encode_f64(value).as_ref());
    }

    fn encode_binary<'a>(
        &self,
        column: &'a BinaryColumn,
        row_index: usize,
    ) -> Result<Cow<'a, str>> {
        let fmt = match self.settings.http_json_result_mode {
            HttpHandlerDataFormat::Driver => BinaryDisplayFormat::Hex,
            HttpHandlerDataFormat::Display => self.settings.binary_format,
        };
        let value = unsafe { column.index_unchecked(row_index) };
        match fmt {
            BinaryDisplayFormat::Hex => Ok(Cow::Owned(hex::encode_upper(value))),
            BinaryDisplayFormat::Base64 => Ok(Cow::Owned(general_purpose::STANDARD.encode(value))),
            BinaryDisplayFormat::Utf8 => std::str::from_utf8(value).map(Cow::Borrowed).map_err(
                |err| {
                    ErrorCode::InvalidUtf8String(format!(
                        "Invalid UTF-8 sequence while formatting binary column: {err}. Consider \
setting binary_output_format to 'UTF-8-LOSSY'."
                    ))
                },
            ),
            BinaryDisplayFormat::Utf8Lossy => Ok(String::from_utf8_lossy(value)),
        }
    }

    fn encode_geometry(&self, value: &[u8]) -> Result<Cow<'static, str>> {
        let (text, _) = self.geometry_text(value)?;
        Ok(text)
    }

    fn encode_geography(&self, value: &[u8]) -> Result<Cow<'static, str>> {
        let (text, _) = self.geography_text(value)?;
        Ok(text)
    }

    fn write_geometry_nested_to(&self, value: &[u8], out: &mut String) -> Result<()> {
        let (text, is_string) = self.geometry_text(value)?;
        if is_string {
            self.write_quoted_string(text.as_ref(), out);
        } else {
            out.push_str(text.as_ref());
        }
        Ok(())
    }

    fn write_geography_nested_to(&self, value: &[u8], out: &mut String) -> Result<()> {
        let (text, is_string) = self.geography_text(value)?;
        if is_string {
            self.write_quoted_string(text.as_ref(), out);
        } else {
            out.push_str(text.as_ref());
        }
        Ok(())
    }

    fn geometry_text(&self, value: &[u8]) -> Result<(Cow<'static, str>, bool)> {
        match ewkb_to_geo(&mut Ewkb(value)) {
            Ok((geo, srid)) => {
                let srid = srid.unwrap_or(0);
                match self.settings.geometry_format {
                    GeometryDataType::WKB => {
                        Ok((Cow::Owned(hex::encode_upper(geo_to_wkb(geo)?)), true))
                    }
                    GeometryDataType::WKT => Ok((Cow::Owned(geo_to_wkt(geo)?), true)),
                    GeometryDataType::EWKB => Ok((
                        Cow::Owned(hex::encode_upper(geo_to_ewkb(geo, Some(srid))?)),
                        true,
                    )),
                    GeometryDataType::EWKT => Ok((Cow::Owned(geo_to_ewkt(geo, Some(srid))?), true)),
                    GeometryDataType::GEOJSON => Ok((Cow::Owned(geo_to_json(geo)?), false)),
                }
            }
            Err(_) => Ok(self.lossy_geometry(value)),
        }
    }

    fn geography_text(&self, value: &[u8]) -> Result<(Cow<'static, str>, bool)> {
        match Ewkb(value).to_geo() {
            Ok(geo) => match self.settings.geometry_format {
                GeometryDataType::WKB => {
                    Ok((Cow::Owned(hex::encode_upper(geo_to_wkb(geo)?)), true))
                }
                GeometryDataType::WKT => Ok((Cow::Owned(geo_to_wkt(geo)?), true)),
                GeometryDataType::EWKB => Ok((
                    Cow::Owned(hex::encode_upper(geo_to_ewkb(geo, Some(GEOGRAPHY_SRID))?)),
                    true,
                )),
                GeometryDataType::EWKT => {
                    Ok((Cow::Owned(geo_to_ewkt(geo, Some(GEOGRAPHY_SRID))?), true))
                }
                GeometryDataType::GEOJSON => Ok((Cow::Owned(geo_to_json(geo)?), false)),
            },
            Err(_) => Ok(self.lossy_geometry(value)),
        }
    }

    fn lossy_geometry(&self, value: &[u8]) -> (Cow<'static, str>, bool) {
        let text = Cow::Owned(String::from_utf8_lossy(value).into_owned());
        match self.settings.geometry_format {
            GeometryDataType::GEOJSON => (text, false),
            _ => (text, true),
        }
    }

    fn write_array_root_to(
        &self,
        column: &ArrayColumn<AnyType>,
        row_index: usize,
        out: &mut String,
    ) -> Result<()> {
        let start = unsafe { *column.offsets().get_unchecked(row_index) as usize };
        let end = unsafe { *column.offsets().get_unchecked(row_index + 1) as usize };
        let inner = column.values();
        out.push('[');
        for i in start..end {
            if i != start {
                out.push(',');
            }
            self.write_nested_to(inner, i, out)?;
        }
        out.push(']');
        Ok(())
    }

    fn write_map_root_to(
        &self,
        column: &ArrayColumn<AnyType>,
        row_index: usize,
        out: &mut String,
    ) -> Result<()> {
        let start = unsafe { *column.offsets().get_unchecked(row_index) as usize };
        let end = unsafe { *column.offsets().get_unchecked(row_index + 1) as usize };
        out.push('{');
        match column.values() {
            Column::Tuple(fields) => {
                for i in start..end {
                    if i != start {
                        out.push(',');
                    }
                    self.write_nested_to(&fields[0], i, out)?;
                    out.push(':');
                    self.write_nested_to(&fields[1], i, out)?;
                }
            }
            _ => unreachable!(),
        }
        out.push('}');
        Ok(())
    }

    fn write_tuple_root_to(
        &self,
        columns: &[Column],
        row_index: usize,
        out: &mut String,
    ) -> Result<()> {
        out.push('(');
        for (idx, inner) in columns.iter().enumerate() {
            if idx > 0 {
                out.push(',');
            }
            self.write_nested_to(inner, row_index, out)?;
        }
        out.push(')');
        Ok(())
    }

    fn write_vector_to(&self, column: &VectorColumn, row_index: usize, out: &mut String) {
        let scalar = unsafe { column.index_unchecked(row_index) };
        out.push('[');
        match scalar {
            VectorScalarRef::Int8(values) => {
                for (idx, value) in values.iter().enumerate() {
                    if idx > 0 {
                        out.push(',');
                    }
                    write!(out, "{value}").unwrap();
                }
            }
            VectorScalarRef::Float32(values) => {
                for (idx, value) in values.iter().enumerate() {
                    if idx > 0 {
                        out.push(',');
                    }
                    self.write_f32_to(value.0, out);
                }
            }
        }
        out.push(']');
    }

    fn encode_opaque(&self, column: &OpaqueColumn, row_index: usize) -> String {
        hex::encode_upper(unsafe { column.index_unchecked(row_index) }.to_le_bytes())
    }

    fn write_quoted_string(&self, value: &str, out: &mut String) {
        let quote = self.quote_char as char;
        let escape = self.escape_char as char;
        out.push(quote);
        for ch in value.chars() {
            if ch == quote {
                out.push(escape);
            }
            out.push(ch);
        }
        out.push(quote);
    }
}

#[cfg(test)]
mod tests {
    use databend_common_expression::Column;
    use databend_common_expression::FromData;
    use databend_common_expression::types::BinaryType;
    use databend_common_expression::types::Int32Type;
    use databend_common_expression::types::StringType;
    use databend_common_expression::types::array::ArrayColumn;
    use databend_common_io::prelude::OutputFormatSettings;

    use super::FieldEncoderToString;

    #[test]
    fn test_write_string_field_top_level() {
        let encoder = FieldEncoderToString::create(&OutputFormatSettings::default());
        let column = StringType::from_data(vec!["a\"b"]);

        let rendered = encoder.encode(&column, 0).unwrap();

        assert_eq!(rendered.as_ref(), "a\"b");
    }

    #[test]
    fn test_write_array_with_string_buffer() {
        let encoder = FieldEncoderToString::create(&OutputFormatSettings::default());
        let values = StringType::from_data(vec!["x", "y"]);
        let column = Column::Array(Box::new(ArrayColumn::new(values, vec![0_u64, 2].into())));

        let rendered = encoder.encode(&column, 0).unwrap();

        assert_eq!(rendered.as_ref(), "[\"x\",\"y\"]");
    }

    #[test]
    fn test_write_tuple_with_nested_strings() {
        let encoder = FieldEncoderToString::create(&OutputFormatSettings::default());
        let column = Column::Tuple(vec![
            Int32Type::from_data(vec![7]),
            StringType::from_data(vec!["p\"q"]),
        ]);

        let rendered = encoder.encode(&column, 0).unwrap();

        assert_eq!(rendered.as_ref(), "(7,\"p\\\"q\")");
    }

    #[test]
    fn test_write_array_with_binary_keeps_legacy_nested_format() {
        let encoder = FieldEncoderToString::create(&OutputFormatSettings::default());
        let values = BinaryType::from_data(vec![b"x".as_slice(), b"y\"z".as_slice()]);
        let column = Column::Array(Box::new(ArrayColumn::new(values, vec![0_u64, 2].into())));

        let rendered = encoder.encode(&column, 0).unwrap();

        assert_eq!(rendered.as_ref(), "[78,79227A]");
    }
}
