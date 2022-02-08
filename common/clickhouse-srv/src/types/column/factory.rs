// Copyright 2021 Datafuse Labs.
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

use chrono_tz::Tz;
use combine::any;
use combine::error::StringStreamError;
use combine::many;
use combine::many1;
use combine::none_of;
use combine::optional;
use combine::parser::char::digit;
use combine::parser::char::spaces;
use combine::parser::char::string;
use combine::sep_by1;
use combine::token;
use combine::Parser;

use crate::binary::ReadEx;
use crate::errors::Result;
use crate::types::column::array::ArrayColumnData;
use crate::types::column::column_data::ColumnData;
use crate::types::column::date::DateColumnData;
use crate::types::column::datetime64::DateTime64ColumnData;
use crate::types::column::decimal::DecimalColumnData;
use crate::types::column::enums::Enum16ColumnData;
use crate::types::column::enums::Enum8ColumnData;
use crate::types::column::fixed_string::FixedStringColumnData;
use crate::types::column::ip::IpColumnData;
use crate::types::column::ip::Ipv4;
use crate::types::column::ip::Ipv6;
use crate::types::column::ip::Uuid;
use crate::types::column::list::List;
use crate::types::column::nullable::NullableColumnData;
use crate::types::column::numeric::VectorColumnData;
use crate::types::column::string::StringColumnData;
use crate::types::column::tuple::TupleColumnData;
use crate::types::column::ArcColumnWrapper;
use crate::types::column::BoxColumnWrapper;
use crate::types::column::ColumnWrapper;
use crate::types::decimal::NoBits;
use crate::types::DateTimeType;
use crate::types::SqlType;

macro_rules! match_str {
    ($arg:ident, {
        $( $($var:literal)|* => $doit:expr,)*
        _ => $dothat:block
    }) => {
        $(
            $(
                if $arg.eq_ignore_ascii_case($var) {
                    $doit
                } else
            )*
        )*
        $dothat
    }
}

impl dyn ColumnData {
    #[allow(clippy::cognitive_complexity)]
    pub(crate) fn load_data<W: ColumnWrapper, T: ReadEx>(
        reader: &mut T,
        type_name: &str,
        size: usize,
        tz: Tz,
    ) -> Result<W::Wrapper> {
        Ok(match_str!(type_name, {
            "UInt8" => W::wrap(VectorColumnData::<u8>::load(reader, size)?),
            "UInt16" => W::wrap(VectorColumnData::<u16>::load(reader, size)?),
            "UInt32" => W::wrap(VectorColumnData::<u32>::load(reader, size)?),
            "UInt64" => W::wrap(VectorColumnData::<u64>::load(reader, size)?),
            "Int8" | "TinyInt" => W::wrap(VectorColumnData::<i8>::load(reader, size)?),
            "Int16" | "SmallInt" => W::wrap(VectorColumnData::<i16>::load(reader, size)?),
            "Int32" | "Int" | "Integer" => W::wrap(VectorColumnData::<i32>::load(reader, size)?),
            "Int64" | "BigInt" => W::wrap(VectorColumnData::<i64>::load(reader, size)?),
            "Float32" | "Float" => W::wrap(VectorColumnData::<f32>::load(reader, size)?),
            "Float64" | "Double" => W::wrap(VectorColumnData::<f64>::load(reader, size)?),
            "String" | "Char" | "Varchar" | "Text" | "TinyText" | "MediumText" | "LongText" | "Blob" | "TinyBlob" | "MediumBlob" | "LongBlob" => W::wrap(StringColumnData::load(reader, size)?),
            "Date" => W::wrap(DateColumnData::<u16>::load(reader, size, tz)?),
            "DateTime" | "Timestamp" => W::wrap(DateColumnData::<u32>::load(reader, size, tz)?),
            "IPv4" => W::wrap(IpColumnData::<Ipv4>::load(reader, size)?),
            "IPv6" => W::wrap(IpColumnData::<Ipv6>::load(reader, size)?),
            "UUID" => W::wrap(IpColumnData::<Uuid>::load(reader, size)?),
            _ => {
                if let Some(inner_type) = parse_nullable_type(type_name) {
                    W::wrap(NullableColumnData::load(reader, inner_type, size, tz)?)
                } else if let Some(str_len) = parse_fixed_string(type_name) {
                    W::wrap(FixedStringColumnData::load(reader, size, str_len)?)
                } else if let Some(inner_type) = parse_array_type(type_name) {
                    W::wrap(ArrayColumnData::load(reader, inner_type, size, tz)?)
                } else if let Some(inner_types) = parse_tuple_type(type_name) {
                    W::wrap(TupleColumnData::load(reader, inner_types, size, tz)?)
                }  else if let Some((precision, scale, nobits)) = parse_decimal(type_name) {
                    W::wrap(DecimalColumnData::load(
                        reader, precision, scale, nobits, size, tz,
                    )?)
                } else if let Some(items) = parse_enum8(type_name) {
                    W::wrap(Enum8ColumnData::load(reader, items, size, tz)?)
                } else if let Some(items) = parse_enum16(type_name) {
                    W::wrap(Enum16ColumnData::load(reader, items, size, tz)?)
                } else if let Some((precision, timezone)) = parse_date_time64(type_name) {
                    let column_timezone = get_timezone(&timezone, tz)?;
                    W::wrap(DateTime64ColumnData::load(reader, size, precision, column_timezone)?)
                } else {
                    let message = format!("Unsupported column type \"{}\".", type_name);
                    return Err(message.into());
                }
            }
        }))
    }

    pub(crate) fn from_type<W: ColumnWrapper>(
        sql_type: SqlType,
        timezone: Tz,
        capacity: usize,
    ) -> Result<W::Wrapper> {
        Ok(match sql_type {
            SqlType::UInt8 => W::wrap(VectorColumnData::<u8>::with_capacity(capacity)),
            SqlType::UInt16 => W::wrap(VectorColumnData::<u16>::with_capacity(capacity)),
            SqlType::UInt32 => W::wrap(VectorColumnData::<u32>::with_capacity(capacity)),
            SqlType::UInt64 => W::wrap(VectorColumnData::<u64>::with_capacity(capacity)),
            SqlType::Int8 => W::wrap(VectorColumnData::<i8>::with_capacity(capacity)),
            SqlType::Int16 => W::wrap(VectorColumnData::<i16>::with_capacity(capacity)),
            SqlType::Int32 => W::wrap(VectorColumnData::<i32>::with_capacity(capacity)),
            SqlType::Int64 => W::wrap(VectorColumnData::<i64>::with_capacity(capacity)),
            SqlType::String => W::wrap(StringColumnData::with_capacity(capacity)),
            SqlType::FixedString(len) => {
                W::wrap(FixedStringColumnData::with_capacity(capacity, len))
            }
            SqlType::Float32 => W::wrap(VectorColumnData::<f32>::with_capacity(capacity)),
            SqlType::Float64 => W::wrap(VectorColumnData::<f64>::with_capacity(capacity)),

            SqlType::Ipv4 => W::wrap(IpColumnData::<Ipv4>::with_capacity(capacity)),
            SqlType::Ipv6 => W::wrap(IpColumnData::<Ipv6>::with_capacity(capacity)),
            SqlType::Uuid => W::wrap(IpColumnData::<Uuid>::with_capacity(capacity)),

            SqlType::Date => W::wrap(DateColumnData::<u16>::with_capacity(capacity, timezone)),
            SqlType::DateTime(DateTimeType::DateTime64(precision, timezone)) => W::wrap(
                DateTime64ColumnData::with_capacity(capacity, precision, timezone),
            ),
            SqlType::DateTime(_) => {
                W::wrap(DateColumnData::<u32>::with_capacity(capacity, timezone))
            }
            SqlType::Nullable(inner_type) => W::wrap(NullableColumnData {
                inner: <dyn ColumnData>::from_type::<ArcColumnWrapper>(
                    inner_type.clone(),
                    timezone,
                    capacity,
                )?,
                nulls: Vec::new(),
            }),
            SqlType::Array(inner_type) => W::wrap(ArrayColumnData {
                inner: <dyn ColumnData>::from_type::<ArcColumnWrapper>(
                    inner_type.clone(),
                    timezone,
                    capacity,
                )?,
                offsets: List::with_capacity(capacity),
            }),
            SqlType::Decimal(precision, scale) => {
                let nobits = NoBits::from_precision(precision).unwrap();

                let inner_type = match nobits {
                    NoBits::N32 => SqlType::Int32,
                    NoBits::N64 => SqlType::Int64,
                };

                W::wrap(DecimalColumnData {
                    inner: <dyn ColumnData>::from_type::<BoxColumnWrapper>(
                        inner_type, timezone, capacity,
                    )?,
                    precision,
                    scale,
                    nobits,
                })
            }
            SqlType::Enum8(enum_values) => W::wrap(Enum8ColumnData {
                enum_values,
                inner: <dyn ColumnData>::from_type::<BoxColumnWrapper>(
                    SqlType::Int8,
                    timezone,
                    capacity,
                )?,
            }),
            SqlType::Enum16(enum_values) => W::wrap(Enum16ColumnData {
                enum_values,
                inner: <dyn ColumnData>::from_type::<BoxColumnWrapper>(
                    SqlType::Int16,
                    timezone,
                    capacity,
                )?,
            }),
            SqlType::Tuple(types) => W::wrap(TupleColumnData {
                inner: types
                    .into_iter()
                    .map(|t| {
                        <dyn ColumnData>::from_type::<ArcColumnWrapper>(
                            t.clone(),
                            timezone,
                            capacity,
                        )
                    })
                    .collect::<Result<Vec<_>>>()?,
            }),
        })
    }
}

pub fn parse_fixed_string(source: &str) -> Option<usize> {
    if !source.starts_with("FixedString") {
        return None;
    }

    let inner_size = &source[12..source.len() - 1];
    match inner_size.parse::<usize>() {
        Err(_) => None,
        Ok(value) => Some(value),
    }
}

pub fn parse_nullable_type(source: &str) -> Option<&str> {
    if !source.starts_with("Nullable") {
        return None;
    }

    let inner_type = &source[9..source.len() - 1];

    if inner_type.starts_with("Nullable") {
        return None;
    }

    Some(inner_type)
}

pub fn parse_array_type(source: &str) -> Option<&str> {
    if !source.starts_with("Array") {
        return None;
    }

    let inner_type = &source[6..source.len() - 1];
    Some(inner_type)
}

pub fn parse_decimal(source: &str) -> Option<(u8, u8, NoBits)> {
    if source.len() < 12 {
        return None;
    }

    if !source.starts_with("Decimal") {
        return None;
    }

    let mut nobits = None;
    let mut precision = None;
    let mut scale = None;

    let mut params_indexes = (None, None);

    for (idx, byte) in source.as_bytes().iter().enumerate() {
        if *byte == b'(' {
            match &source.as_bytes()[..idx] {
                b"Decimal" => {}
                b"Decimal32" => {
                    nobits = Some(NoBits::N32);
                }
                b"Decimal64" => {
                    nobits = Some(NoBits::N64);
                }
                _ => return None,
            }
            params_indexes.0 = Some(idx);
        }
        if *byte == b')' {
            params_indexes.1 = Some(idx);
        }
    }

    let params_indexes = match params_indexes {
        (Some(start), Some(end)) => (start, end),
        _ => return None,
    };

    match nobits {
        Some(_) => scale = source[params_indexes.0 + 1..params_indexes.1].parse().ok(),
        None => {
            for (idx, cell) in source[params_indexes.0 + 1..params_indexes.1]
                .split(',')
                .map(|s| s.trim())
                .enumerate()
            {
                match idx {
                    0 => precision = cell.parse().ok(),
                    1 => scale = cell.parse().ok(),
                    _ => return None,
                }
            }
        }
    }

    match (precision, scale, nobits) {
        (Some(precision), Some(scale), None) => {
            if scale > precision {
                return None;
            }

            NoBits::from_precision(precision).map(|nobits| (precision, scale, nobits))
        }
        (None, Some(scale), Some(bits)) => {
            let precision = match bits {
                NoBits::N32 => 9,
                NoBits::N64 => 18,
            };
            Some((precision, scale, bits))
        }
        _ => None,
    }
}

pub enum EnumSize {
    Enum8,
    Enum16,
}

pub fn parse_enum8(input: &str) -> Option<Vec<(String, i8)>> {
    parse_enum(EnumSize::Enum8, input).map(|result| {
        result
            .iter()
            .map(|(key, val)| (key.clone(), *val as i8))
            .collect::<Vec<(String, i8)>>()
    })
}
pub fn parse_enum16(input: &str) -> Option<Vec<(String, i16)>> {
    parse_enum(EnumSize::Enum16, input)
}

pub fn parse_enum(size: EnumSize, input: &str) -> Option<Vec<(String, i16)>> {
    let size = match size {
        EnumSize::Enum8 => "Enum8",
        EnumSize::Enum16 => "Enum16",
    };

    let integer = optional(token('-'))
        .and(many1::<String, _, _>(digit()))
        .and_then(|(x, mut digits)| {
            if let Some(x) = x {
                digits.insert(0, x);
            }
            digits
                .parse::<i16>()
                .map_err(|_| StringStreamError::UnexpectedParse)
        });

    let word_syms = token('\\').with(any()).or(none_of("'".chars()));
    let word = token('\'').with(many(word_syms)).skip(token('\''));

    let pair = spaces()
        .with(word)
        .skip(spaces())
        .skip(token('='))
        .skip(spaces())
        .and(integer)
        .skip(spaces());
    let enum_body = sep_by1::<Vec<(String, i16)>, _, _, _>(pair, token(','));

    let mut parser = spaces()
        .with(string(size))
        .skip(spaces())
        .skip(token('('))
        .skip(spaces())
        .with(enum_body)
        .skip(token(')'));
    let result = parser.parse(input);
    if let Ok((res, remain)) = result {
        if !remain.is_empty() {
            return None;
        }
        Some(res)
    } else {
        None
    }
}

pub fn parse_date_time64(source: &str) -> Option<(u32, Option<String>)> {
    let integer = many1::<String, _, _>(digit()).and_then(|digits| {
        digits
            .parse::<u32>()
            .map_err(|_| StringStreamError::UnexpectedParse)
    });

    let word_syms = token('\\').with(any()).or(none_of("'".chars()));
    let word = token('\'')
        .with(many::<String, _, _>(word_syms))
        .skip(token('\''));

    let timezone = optional(spaces().skip(token(',')).skip(spaces()).with(word));

    let pair = spaces()
        .with(integer)
        .skip(spaces())
        .and(timezone)
        .skip(spaces());

    let mut parser = spaces()
        .with(string("DateTime64"))
        .skip(spaces())
        .skip(token('('))
        .skip(spaces())
        .with(pair)
        .skip(spaces())
        .skip(token(')'));

    match parser.parse(source) {
        Ok((pair, remain)) if remain.is_empty() => Some(pair),
        _ => None,
    }
}

pub fn get_timezone(timezone: &Option<String>, tz: Tz) -> Result<Tz> {
    match timezone {
        None => Ok(tz),
        Some(t) => Ok(t.parse()?),
    }
}

pub fn parse_tuple_type(source: &str) -> Option<Vec<&str>> {
    if !source.starts_with("Tuple") {
        return None;
    }

    let types = &source[6..source.len() - 1];

    let mut inner_types = Vec::new();
    let chars = types.char_indices();
    let mut diff = 0;
    let mut last = 0;
    for (i, c) in chars {
        match c {
            '(' => diff += 1,
            ')' => diff -= 1,
            ',' => {
                if diff == 0 {
                    inner_types.push(types[last..i].trim());
                    last = i + 1;
                }
            }
            _ => {}
        }
    }

    if last < types.len() {
        inner_types.push(types[last..].trim());
    }

    if inner_types.is_empty() {
        return None;
    }

    Some(inner_types)
}
