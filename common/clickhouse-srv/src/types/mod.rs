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

use std::borrow::Cow;
use std::collections::HashMap;
use std::fmt;
use std::mem;
use std::pin::Pin;
use std::sync::Mutex;

use chrono::prelude::*;
use chrono_tz::Tz;
use hostname::get;
use once_cell::sync::Lazy;

pub use self::block::decompress_buffer;
pub use self::block::Block;
pub use self::block::RCons;
pub use self::block::RNil;
pub use self::block::Row;
pub use self::block::RowBuilder;
pub use self::block::Rows;
pub use self::column::Column;
pub use self::column::ColumnType;
pub use self::column::Complex;
pub use self::column::Either;
pub use self::column::Simple;
pub use self::column::StringPool;
pub use self::date_converter::DateConverter;
pub use self::decimal::decimal2str;
pub use self::decimal::Decimal;
pub use self::decimal::NoBits;
pub use self::enums::Enum16;
pub use self::enums::Enum8;
pub use self::from_sql::FromSql;
pub use self::from_sql::FromSqlResult;
pub use self::options::*;
pub use self::query::Query;
pub use self::stat_buffer::StatBuffer;
pub use self::value::Value;
pub use self::value_ref::ValueRef;
use crate::binary::Encoder;
use crate::protocols::DBMS_MIN_REVISION_WITH_CLIENT_WRITE_INFO;
use crate::protocols::SERVER_PROGRESS;

pub mod column;
mod stat_buffer;

mod from_sql;
mod value;
mod value_ref;

pub mod block;

mod date_converter;
mod query;

mod decimal;
mod enums;
mod options;

#[derive(Copy, Clone, Debug, Default, PartialEq)]
pub struct Progress {
    pub rows: u64,
    pub bytes: u64,
    pub total_rows: u64,
}

impl Progress {
    pub fn write(&self, encoder: &mut Encoder, client_revision: u64) {
        encoder.uvarint(SERVER_PROGRESS);
        encoder.uvarint(self.rows);
        encoder.uvarint(self.bytes);
        encoder.uvarint(self.total_rows);

        if client_revision >= DBMS_MIN_REVISION_WITH_CLIENT_WRITE_INFO {
            encoder.uvarint(0);
            encoder.uvarint(0);
        }
    }
}

#[derive(Copy, Clone, Default, Debug, PartialEq)]
pub(crate) struct ProfileInfo {
    pub rows: u64,
    pub bytes: u64,
    pub blocks: u64,
    pub applied_limit: bool,
    pub rows_before_limit: u64,
    pub calculated_rows_before_limit: bool,
}

#[derive(Clone, PartialEq)]
pub(crate) struct ServerInfo {
    pub name: String,
    pub revision: u64,
    pub minor_version: u64,
    pub major_version: u64,
    pub timezone: Tz,
}

impl fmt::Debug for ServerInfo {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "{} {}.{}.{} ({:?})",
            self.name, self.major_version, self.minor_version, self.revision, self.timezone
        )
    }
}

#[allow(dead_code)]
#[derive(Clone)]
pub(crate) struct Context {
    pub(crate) server_info: ServerInfo,
    pub(crate) hostname: String,
    pub(crate) options: OptionsSource,
}

impl Default for ServerInfo {
    fn default() -> Self {
        Self {
            name: String::new(),
            revision: 0,
            minor_version: 0,
            major_version: 0,
            timezone: Tz::Zulu,
        }
    }
}

impl fmt::Debug for Context {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.debug_struct("Context")
            .field("options", &self.options)
            .field("hostname", &self.hostname)
            .finish()
    }
}

impl Default for Context {
    fn default() -> Self {
        Self {
            server_info: ServerInfo::default(),
            hostname: get().unwrap().into_string().unwrap(),
            options: OptionsSource::default(),
        }
    }
}

pub trait HasSqlType {
    fn get_sql_type() -> SqlType;
}

macro_rules! has_sql_type {
    ( $( $t:ty : $k:expr ),* ) => {
        $(
            impl HasSqlType for $t {
                fn get_sql_type() -> SqlType {
                    $k
                }
            }
        )*
    };
}

has_sql_type! {
    u8: SqlType::UInt8,
    u16: SqlType::UInt16,
    u32: SqlType::UInt32,
    u64: SqlType::UInt64,
    i8: SqlType::Int8,
    i16: SqlType::Int16,
    i32: SqlType::Int32,
    i64: SqlType::Int64,
    &str: SqlType::String,
    String: SqlType::String,
    f32: SqlType::Float32,
    f64: SqlType::Float64,
    Date<Tz>: SqlType::Date,
    DateTime<Tz>: SqlType::DateTime(DateTimeType::DateTime32)
}

#[derive(Copy, Clone, Debug, Eq, PartialEq, Hash)]
pub enum DateTimeType {
    DateTime32,
    DateTime64(u32, Tz),
    Chrono,
}

#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub enum SqlType {
    UInt8,
    UInt16,
    UInt32,
    UInt64,
    Int8,
    Int16,
    Int32,
    Int64,
    String,
    FixedString(usize),
    Float32,
    Float64,
    Date,
    DateTime(DateTimeType),
    Ipv4,
    Ipv6,
    Uuid,
    Nullable(&'static SqlType),
    Array(&'static SqlType),
    Decimal(u8, u8),
    Enum8(Vec<(String, i8)>),
    Enum16(Vec<(String, i16)>),
    Tuple(Vec<&'static SqlType>),
}

static TYPES_CACHE: Lazy<Mutex<HashMap<SqlType, Pin<Box<SqlType>>>>> =
    Lazy::new(|| Mutex::new(HashMap::new()));

impl From<SqlType> for &'static SqlType {
    fn from(value: SqlType) -> Self {
        match value {
            SqlType::UInt8 => &SqlType::UInt8,
            SqlType::UInt16 => &SqlType::UInt16,
            SqlType::UInt32 => &SqlType::UInt32,
            SqlType::UInt64 => &SqlType::UInt64,
            SqlType::Int8 => &SqlType::Int8,
            SqlType::Int16 => &SqlType::Int16,
            SqlType::Int32 => &SqlType::Int32,
            SqlType::Int64 => &SqlType::Int64,
            SqlType::String => &SqlType::String,
            SqlType::Float32 => &SqlType::Float32,
            SqlType::Float64 => &SqlType::Float64,
            SqlType::Date => &SqlType::Date,
            _ => {
                let mut guard = TYPES_CACHE.lock().unwrap();
                loop {
                    if let Some(value_ref) = guard.get(&value.clone()) {
                        return unsafe { mem::transmute(value_ref.as_ref()) };
                    }
                    guard.insert(value.clone(), Box::pin(value.clone()));
                }
            }
        }
    }
}

impl SqlType {
    pub(crate) fn is_datetime(&self) -> bool {
        matches!(self, SqlType::DateTime(_))
    }

    pub fn to_string(&self) -> Cow<'static, str> {
        match self.clone() {
            SqlType::UInt8 => "UInt8".into(),
            SqlType::UInt16 => "UInt16".into(),
            SqlType::UInt32 => "UInt32".into(),
            SqlType::UInt64 => "UInt64".into(),
            SqlType::Int8 => "Int8".into(),
            SqlType::Int16 => "Int16".into(),
            SqlType::Int32 => "Int32".into(),
            SqlType::Int64 => "Int64".into(),
            SqlType::String => "String".into(),
            SqlType::FixedString(str_len) => format!("FixedString({})", str_len).into(),
            SqlType::Float32 => "Float32".into(),
            SqlType::Float64 => "Float64".into(),
            SqlType::Date => "Date".into(),
            SqlType::DateTime(DateTimeType::DateTime64(precision, tz)) => {
                format!("DateTime64({}, '{:?}')", precision, tz).into()
            }
            SqlType::DateTime(_) => "DateTime".into(),
            SqlType::Ipv4 => "IPv4".into(),
            SqlType::Ipv6 => "IPv6".into(),
            SqlType::Uuid => "UUID".into(),
            SqlType::Nullable(nested) => format!("Nullable({})", &nested).into(),
            SqlType::Array(nested) => format!("Array({})", &nested).into(),
            SqlType::Decimal(precision, scale) => {
                format!("Decimal({}, {})", precision, scale).into()
            }
            SqlType::Enum8(values) => {
                let a: Vec<String> = values
                    .iter()
                    .map(|(name, value)| format!("'{}' = {}", name, value))
                    .collect();
                format!("Enum8({})", a.join(",")).into()
            }
            SqlType::Enum16(values) => {
                let a: Vec<String> = values
                    .iter()
                    .map(|(name, value)| format!("'{}' = {}", name, value))
                    .collect();
                format!("Enum16({})", a.join(",")).into()
            }
            SqlType::Tuple(types) => {
                let a: Vec<String> = types.iter().map(|t| t.to_string()).collect();
                format!("Tuple({})", a.join(",")).into()
            }
        }
    }

    pub(crate) fn level(&self) -> u8 {
        match self {
            SqlType::Nullable(inner) => 1 + inner.level(),
            SqlType::Array(inner) => 1 + inner.level(),
            SqlType::Tuple(types) => types.iter().map(|t| t.level()).max().unwrap_or(0) + 1,
            _ => 0,
        }
    }
}

impl fmt::Display for SqlType {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", Self::to_string(self))
    }
}

#[test]
fn test_display() {
    let expected = "UInt8".to_string();
    let actual = format!("{}", SqlType::UInt8);
    assert_eq!(expected, actual);
}

#[test]
fn test_to_string() {
    let expected: Cow<'static, str> = "Nullable(UInt8)".into();
    let actual = SqlType::Nullable(&SqlType::UInt8).to_string();
    assert_eq!(expected, actual)
}
