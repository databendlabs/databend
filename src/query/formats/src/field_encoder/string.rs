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
use databend_common_io::GeometryDataType;
use databend_common_io::constants::FALSE_BYTES_NUM;
use databend_common_io::constants::INF_BYTES_LONG;
use databend_common_io::constants::NAN_BYTES_SNAKE;
use databend_common_io::constants::NULL_BYTES_UPPER;
use databend_common_io::constants::TRUE_BYTES_NUM;
use databend_common_io::ewkb_to_geo;
use databend_common_io::geo_to_ewkb;
use databend_common_io::geo_to_ewkt;
use databend_common_io::geo_to_json;
use databend_common_io::geo_to_wkb;
use databend_common_io::geo_to_wkt;
use databend_common_io::prelude::BinaryDisplayFormat;
use databend_common_io::prelude::OutputFormatSettings;
use geozero::wkb::Ewkb;
use jsonb::RawJsonb;

const BITMAP_BINARY: &str = "<bitmap binary>";

// `EncodedField` keeps the final field text plus whether nested contexts should
// treat it as a string literal that needs quoting.
//
// `Raw` means the text is already the final representation and must not be
// quoted again in nested containers. Typical cases:
// - NULL / [] / {}
// - booleans and numbers
// - Variant JSON text
// - GEOJSON output
//
// `String` means the text is the string payload. When nested inside
// Array/Map/Tuple, it must be emitted as `"..."`. Current cases:
// - String
// - Timestamp / TimestampTz / Date / Interval
// - Bitmap placeholder text
// - Geometry / Geography formats except GEOJSON
enum EncodedField<'a> {
    Raw(Cow<'a, str>),
    String(Cow<'a, str>),
}

impl<'a> EncodedField<'a> {
    fn into_cow(self, encoder: &FieldEncoderString, in_nested: bool) -> Cow<'a, str> {
        match self {
            EncodedField::Raw(value) => value,
            EncodedField::String(value) if !in_nested => value,
            EncodedField::String(value) => {
                let mut out = String::with_capacity(value.len() + 2);
                encoder.write_quoted_string(value.as_ref(), &mut out);
                Cow::Owned(out)
            }
        }
    }
}

pub struct FieldEncoderString {
    pub settings: OutputFormatSettings,

    pub escape_char: u8,
    pub quote_char: u8,
}

impl FieldEncoderString {
    pub fn create(settings: &OutputFormatSettings) -> Self {
        Self {
            settings: settings.clone(),
            escape_char: b'\\',
            quote_char: b'"',
        }
    }

    pub fn write_field<'a>(
        &self,
        column: &'a Column,
        row_index: usize,
        in_nested: bool,
    ) -> Result<Cow<'a, str>> {
        let encoded = match column {
            Column::Null { .. } => EncodedField::Raw(Cow::Borrowed(NULL_BYTES_UPPER)),
            Column::EmptyArray { .. } => EncodedField::Raw(Cow::Borrowed("[]")),
            Column::EmptyMap { .. } => EncodedField::Raw(Cow::Borrowed("{}")),
            Column::Boolean(c) => {
                let value = if c.get_bit(row_index) {
                    TRUE_BYTES_NUM
                } else {
                    FALSE_BYTES_NUM
                };
                EncodedField::Raw(Cow::Borrowed(value))
            }
            Column::Number(c) => EncodedField::Raw(self.write_number(c, row_index)),
            Column::Decimal(c) => {
                EncodedField::Raw(Cow::Owned(c.index(row_index).unwrap().to_string()))
            }
            Column::Binary(c) => EncodedField::Raw(self.write_binary(c, row_index)?),
            Column::String(c) => {
                EncodedField::String(Cow::Borrowed(unsafe { c.index_unchecked(row_index) }))
            }
            Column::Timestamp(c) => EncodedField::String(Cow::Owned(
                timestamp_to_string(
                    unsafe { *c.get_unchecked(row_index) },
                    &self.settings.jiff_timezone,
                )
                .to_string(),
            )),
            Column::TimestampTz(c) => EncodedField::String(Cow::Owned(
                unsafe { c.get_unchecked(row_index) }.to_string(),
            )),
            Column::Date(c) => EncodedField::String(Cow::Owned(
                date_to_string(
                    unsafe { *c.get_unchecked(row_index) as i64 },
                    &self.settings.jiff_timezone,
                )
                .to_string(),
            )),
            Column::Interval(c) => EncodedField::String(Cow::Owned(
                interval_to_string(unsafe { c.get_unchecked(row_index) }).to_string(),
            )),
            Column::Bitmap(_) => EncodedField::String(Cow::Borrowed(BITMAP_BINARY)),
            Column::Nullable(c) => {
                if !c.validity.get_bit(row_index) {
                    EncodedField::Raw(Cow::Borrowed(NULL_BYTES_UPPER))
                } else {
                    return self.write_field(&c.column, row_index, in_nested);
                }
            }
            Column::Variant(c) => EncodedField::Raw(Cow::Owned(
                RawJsonb::new(unsafe { c.index_unchecked(row_index) }).to_string(),
            )),
            Column::Geometry(c) => self.write_geometry(unsafe { c.index_unchecked(row_index) })?,
            Column::Geography(c) => {
                self.write_geography(unsafe { &c.index_unchecked(row_index).0 })?
            }
            Column::Array(_)
            | Column::Map(_)
            | Column::Tuple(_)
            | Column::Vector(_)
            | Column::Opaque(_) => {
                let mut out = String::new();
                let mut scratch = String::new();
                self.write_field_to(column, row_index, &mut out, &mut scratch, in_nested)?;
                return Ok(Cow::Owned(out));
            }
        };

        Ok(encoded.into_cow(self, in_nested))
    }

    pub fn write_field_to(
        &self,
        column: &Column,
        row_index: usize,
        out: &mut String,
        scratch: &mut String,
        in_nested: bool,
    ) -> Result<()> {
        match column {
            Column::Null { .. } => out.push_str(NULL_BYTES_UPPER),
            Column::EmptyArray { .. } => out.push_str("[]"),
            Column::EmptyMap { .. } => out.push_str("{}"),
            Column::Boolean(c) => {
                out.push_str(if c.get_bit(row_index) {
                    TRUE_BYTES_NUM
                } else {
                    FALSE_BYTES_NUM
                });
            }
            Column::Number(c) => self.write_number_to(c, row_index, out),
            Column::Decimal(c) => write!(out, "{}", c.index(row_index).unwrap()).unwrap(),
            Column::Binary(c) => {
                let encoded = EncodedField::Raw(self.write_binary(c, row_index)?);
                self.write_encoded_field_to(encoded, out, scratch, in_nested);
            }
            Column::String(c) => {
                let encoded =
                    EncodedField::String(Cow::Borrowed(unsafe { c.index_unchecked(row_index) }));
                self.write_encoded_field_to(encoded, out, scratch, in_nested);
            }
            Column::Timestamp(c) => {
                let encoded = EncodedField::String(Cow::Owned(
                    timestamp_to_string(
                        unsafe { *c.get_unchecked(row_index) },
                        &self.settings.jiff_timezone,
                    )
                    .to_string(),
                ));
                self.write_encoded_field_to(encoded, out, scratch, in_nested);
            }
            Column::TimestampTz(c) => {
                let encoded = EncodedField::String(Cow::Owned(
                    unsafe { c.get_unchecked(row_index) }.to_string(),
                ));
                self.write_encoded_field_to(encoded, out, scratch, in_nested);
            }
            Column::Date(c) => {
                let encoded = EncodedField::String(Cow::Owned(
                    date_to_string(
                        unsafe { *c.get_unchecked(row_index) as i64 },
                        &self.settings.jiff_timezone,
                    )
                    .to_string(),
                ));
                self.write_encoded_field_to(encoded, out, scratch, in_nested);
            }
            Column::Interval(c) => {
                let encoded = EncodedField::String(Cow::Owned(
                    interval_to_string(unsafe { c.get_unchecked(row_index) }).to_string(),
                ));
                self.write_encoded_field_to(encoded, out, scratch, in_nested);
            }
            Column::Bitmap(_) => {
                self.write_encoded_field_to(
                    EncodedField::String(Cow::Borrowed(BITMAP_BINARY)),
                    out,
                    scratch,
                    in_nested,
                );
            }
            Column::Nullable(c) => {
                if !c.validity.get_bit(row_index) {
                    out.push_str(NULL_BYTES_UPPER);
                } else {
                    self.write_field_to(&c.column, row_index, out, scratch, in_nested)?;
                }
            }
            Column::Variant(c) => {
                out.push_str(&RawJsonb::new(unsafe { c.index_unchecked(row_index) }).to_string());
            }
            Column::Geometry(c) => {
                let encoded = self.write_geometry(unsafe { c.index_unchecked(row_index) })?;
                self.write_encoded_field_to(encoded, out, scratch, in_nested);
            }
            Column::Geography(c) => {
                let encoded = self.write_geography(unsafe { &c.index_unchecked(row_index).0 })?;
                self.write_encoded_field_to(encoded, out, scratch, in_nested);
            }
            Column::Array(box c) => self.write_array_to(c, row_index, out, scratch)?,
            Column::Map(box c) => self.write_map_to(c, row_index, out, scratch)?,
            Column::Tuple(fields) => self.write_tuple_to(fields, row_index, out, scratch)?,
            Column::Vector(c) => self.write_vector_to(c, row_index, out),
            Column::Opaque(c) => out.push_str(&self.write_opaque(c, row_index)),
        }
        Ok(())
    }

    fn write_encoded_field_to<'a>(
        &self,
        field: EncodedField<'a>,
        out: &mut String,
        scratch: &mut String,
        in_nested: bool,
    ) {
        match field {
            EncodedField::Raw(value) => out.push_str(value.as_ref()),
            EncodedField::String(value) if !in_nested => out.push_str(value.as_ref()),
            EncodedField::String(value) => {
                scratch.clear();
                self.write_quoted_string(value.as_ref(), scratch);
                out.push_str(scratch.as_str());
            }
        }
    }

    fn write_number<'a>(&self, column: &'a NumberColumn, row_index: usize) -> Cow<'a, str> {
        let mut out = String::new();
        self.write_number_to(column, row_index, &mut out);
        Cow::Owned(out)
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
                self.write_float_to(unsafe { c.get_unchecked(row_index) }.0, out)
            }
            NumberColumn::Float64(c) => {
                self.write_float_to(unsafe { c.get_unchecked(row_index) }.0, out)
            }
        }
    }

    fn write_float_to<T>(&self, value: T, out: &mut String)
    where T: FloatFormat {
        value.write_to(out)
    }

    fn write_binary<'a>(&self, column: &'a BinaryColumn, row_index: usize) -> Result<Cow<'a, str>> {
        let value = unsafe { column.index_unchecked(row_index) };
        match self.settings.binary_format {
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

    fn write_geometry(&self, value: &[u8]) -> Result<EncodedField<'static>> {
        match ewkb_to_geo(&mut Ewkb(value)) {
            Ok((geo, srid)) => match self.settings.geometry_format {
                GeometryDataType::WKB => Ok(EncodedField::String(Cow::Owned(hex::encode_upper(
                    geo_to_wkb(geo)?,
                )))),
                GeometryDataType::WKT => Ok(EncodedField::String(Cow::Owned(geo_to_wkt(geo)?))),
                GeometryDataType::EWKB => Ok(EncodedField::String(Cow::Owned(hex::encode_upper(
                    geo_to_ewkb(geo, srid)?,
                )))),
                GeometryDataType::EWKT => {
                    Ok(EncodedField::String(Cow::Owned(geo_to_ewkt(geo, srid)?)))
                }
                GeometryDataType::GEOJSON => Ok(EncodedField::Raw(Cow::Owned(geo_to_json(geo)?))),
            },
            Err(_) => Ok(self.lossy_geometry(value)),
        }
    }

    fn write_geography(&self, value: &[u8]) -> Result<EncodedField<'static>> {
        match ewkb_to_geo(&mut Ewkb(value)) {
            Ok((geo, srid)) => match self.settings.geometry_format {
                GeometryDataType::WKB => Ok(EncodedField::String(Cow::Owned(hex::encode_upper(
                    geo_to_wkb(geo)?,
                )))),
                GeometryDataType::WKT => Ok(EncodedField::String(Cow::Owned(geo_to_wkt(geo)?))),
                GeometryDataType::EWKB => Ok(EncodedField::String(Cow::Owned(hex::encode_upper(
                    geo_to_ewkb(geo, srid)?,
                )))),
                GeometryDataType::EWKT => {
                    Ok(EncodedField::String(Cow::Owned(geo_to_ewkt(geo, srid)?)))
                }
                GeometryDataType::GEOJSON => Ok(EncodedField::Raw(Cow::Owned(geo_to_json(geo)?))),
            },
            Err(_) => Ok(self.lossy_geometry(value)),
        }
    }

    fn lossy_geometry(&self, value: &[u8]) -> EncodedField<'static> {
        let text = String::from_utf8_lossy(value).into_owned();
        match self.settings.geometry_format {
            GeometryDataType::GEOJSON => EncodedField::Raw(Cow::Owned(text)),
            _ => EncodedField::String(Cow::Owned(text)),
        }
    }

    fn write_array_to(
        &self,
        column: &ArrayColumn<AnyType>,
        row_index: usize,
        out: &mut String,
        scratch: &mut String,
    ) -> Result<()> {
        let start = unsafe { *column.offsets().get_unchecked(row_index) as usize };
        let end = unsafe { *column.offsets().get_unchecked(row_index + 1) as usize };
        let inner = column.values();
        out.push('[');
        for i in start..end {
            if i != start {
                out.push(',');
            }
            self.write_field_to(inner, i, out, scratch, true)?;
        }
        out.push(']');
        Ok(())
    }

    fn write_map_to(
        &self,
        column: &ArrayColumn<AnyType>,
        row_index: usize,
        out: &mut String,
        scratch: &mut String,
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
                    self.write_field_to(&fields[0], i, out, scratch, true)?;
                    out.push(':');
                    self.write_field_to(&fields[1], i, out, scratch, true)?;
                }
            }
            _ => unreachable!(),
        }
        out.push('}');
        Ok(())
    }

    fn write_tuple_to(
        &self,
        columns: &[Column],
        row_index: usize,
        out: &mut String,
        scratch: &mut String,
    ) -> Result<()> {
        out.push('(');
        for (idx, inner) in columns.iter().enumerate() {
            if idx > 0 {
                out.push(',');
            }
            self.write_field_to(inner, row_index, out, scratch, true)?;
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
                    self.write_float_to(value.0, out);
                }
            }
        }
        out.push(']');
    }

    fn write_opaque(&self, column: &OpaqueColumn, row_index: usize) -> String {
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

trait FloatFormat {
    fn write_to(self, out: &mut String);
}

impl FloatFormat for f32 {
    fn write_to(self, out: &mut String) {
        match self {
            f32::INFINITY => out.push_str(INF_BYTES_LONG),
            f32::NEG_INFINITY => {
                out.push('-');
                out.push_str(INF_BYTES_LONG);
            }
            _ if self.is_nan() => out.push_str(NAN_BYTES_SNAKE),
            _ => {
                let mut buffer = zmij::Buffer::new();
                out.push_str(buffer.format_finite(self));
            }
        }
    }
}

impl FloatFormat for f64 {
    fn write_to(self, out: &mut String) {
        match self {
            f64::INFINITY => out.push_str(INF_BYTES_LONG),
            f64::NEG_INFINITY => {
                out.push('-');
                out.push_str(INF_BYTES_LONG);
            }
            _ if self.is_nan() => out.push_str(NAN_BYTES_SNAKE),
            _ => {
                let mut buffer = zmij::Buffer::new();
                out.push_str(buffer.format_finite(self));
            }
        }
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

    use super::FieldEncoderString;

    #[test]
    fn test_write_string_field_nested_quotes() {
        let encoder = FieldEncoderString::create(&OutputFormatSettings::default());
        let column = StringType::from_data(vec!["a\"b"]);

        let rendered = encoder.write_field(&column, 0, true).unwrap();

        assert_eq!(rendered.as_ref(), "\"a\\\"b\"");
    }

    #[test]
    fn test_write_array_with_string_buffer() {
        let encoder = FieldEncoderString::create(&OutputFormatSettings::default());
        let values = StringType::from_data(vec!["x", "y"]);
        let column = Column::Array(Box::new(ArrayColumn::new(values, vec![0_u64, 2].into())));

        let rendered = encoder.write_field(&column, 0, false).unwrap();

        assert_eq!(rendered.as_ref(), "[\"x\",\"y\"]");
    }

    #[test]
    fn test_write_tuple_with_nested_strings() {
        let encoder = FieldEncoderString::create(&OutputFormatSettings::default());
        let column = Column::Tuple(vec![
            Int32Type::from_data(vec![7]),
            StringType::from_data(vec!["p\"q"]),
        ]);

        let rendered = encoder.write_field(&column, 0, false).unwrap();

        assert_eq!(rendered.as_ref(), "(7,\"p\\\"q\")");
    }

    #[test]
    fn test_write_array_with_binary_keeps_legacy_nested_format() {
        let encoder = FieldEncoderString::create(&OutputFormatSettings::default());
        let values = BinaryType::from_data(vec![b"x".as_slice(), b"y\"z".as_slice()]);
        let column = Column::Array(Box::new(ArrayColumn::new(values, vec![0_u64, 2].into())));

        let rendered = encoder.write_field(&column, 0, false).unwrap();

        assert_eq!(rendered.as_ref(), "[78,79227A]");
    }
}
