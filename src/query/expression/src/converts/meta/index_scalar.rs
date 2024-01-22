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

// DO NOT EDIT.
// This crate keeps some Index codes for compatibility, it's locked by bincode of meta's v3 version

use databend_common_arrow::arrow::bitmap::Bitmap;
use databend_common_arrow::arrow::buffer::Buffer;
use databend_common_exception::Result;
use enum_as_inner::EnumAsInner;
use serde::Deserialize;
use serde::Deserializer;
use serde::Serialize;
use serde::Serializer;

use crate::types::array::ArrayColumn;
use crate::types::binary::BinaryColumn;
use crate::types::decimal::DecimalColumn;
use crate::types::decimal::DecimalScalar;
use crate::types::nullable::NullableColumn;
use crate::types::number::NumberColumn;
use crate::types::number::NumberScalar;
use crate::types::*;
use crate::Column;
use crate::Scalar;

#[derive(Clone, Serialize, Deserialize, Debug, PartialEq, Eq, EnumAsInner)]
pub enum IndexScalar {
    Null,
    Number(NumberScalar),
    Decimal(DecimalScalar),
    Timestamp(i64),
    Date(i32),
    Boolean(bool),
    // For compat reason, we keep this attribute which treat string/binary into string
    #[serde(alias = "String", alias = "Binary")]
    String(Vec<u8>),
    Array(IndexColumn),
    Map(IndexColumn),
    Bitmap(Vec<u8>),
    Tuple(Vec<IndexScalar>),
    Variant(Vec<u8>),
}

#[derive(Clone, Debug, EnumAsInner)]
pub enum IndexColumn {
    Null { len: usize },
    Number(NumberColumn),
    Decimal(DecimalColumn),
    Boolean(Bitmap),
    String(BinaryColumn),
    Timestamp(Buffer<i64>),
    Date(Buffer<i32>),
    Array(Box<IndexArrayColumn>),
    Map(Box<IndexArrayColumn>),
    Bitmap(BinaryColumn),
    Nullable(Box<IndexNullableColumn>),
    Tuple(Vec<IndexColumn>),
    Variant(BinaryColumn),
}

#[derive(Clone, Debug)]
pub struct IndexArrayColumn {
    pub values: IndexColumn,
    pub offsets: Buffer<u64>,
}

#[derive(Clone, Debug)]
pub struct IndexNullableColumn {
    pub column: IndexColumn,
    pub validity: Bitmap,
}

impl From<IndexScalar> for Scalar {
    fn from(value: IndexScalar) -> Self {
        match value {
            IndexScalar::Null => Scalar::Null,
            IndexScalar::Number(num_scalar) => Scalar::Number(num_scalar),
            IndexScalar::Decimal(dec_scalar) => Scalar::Decimal(dec_scalar),
            IndexScalar::Timestamp(ts) => Scalar::Timestamp(ts),
            IndexScalar::Date(date) => Scalar::Date(date),
            IndexScalar::Boolean(b) => Scalar::Boolean(b),
            IndexScalar::String(s) => Scalar::String(unsafe { String::from_utf8_unchecked(s) }),
            IndexScalar::Array(col) => Scalar::Array(col.into()),
            IndexScalar::Map(col) => Scalar::Map(col.into()),
            IndexScalar::Bitmap(bmp) => Scalar::Bitmap(bmp),
            IndexScalar::Tuple(tuple) => {
                Scalar::Tuple(tuple.into_iter().map(|c| c.into()).collect())
            }
            IndexScalar::Variant(variant) => Scalar::Variant(variant),
        }
    }
}

impl From<IndexColumn> for Column {
    fn from(value: IndexColumn) -> Self {
        match value {
            IndexColumn::Null { len } => Column::Null { len },
            IndexColumn::Number(num_col) => Column::Number(num_col),
            IndexColumn::Decimal(dec_col) => Column::Decimal(dec_col),
            IndexColumn::Boolean(bmp) => Column::Boolean(bmp),
            IndexColumn::String(str_col) => Column::String(str_col.try_into().unwrap()),
            IndexColumn::Timestamp(buf) => Column::Timestamp(buf),
            IndexColumn::Date(buf) => Column::Date(buf),
            IndexColumn::Array(arr_col) => Column::Array(Box::new(ArrayColumn::<AnyType> {
                values: arr_col.values.into(),
                offsets: arr_col.offsets,
            })),
            IndexColumn::Map(map_col) => Column::Map(Box::new(ArrayColumn::<AnyType> {
                values: map_col.values.into(),
                offsets: map_col.offsets,
            })),
            IndexColumn::Bitmap(str_col) => Column::Bitmap(str_col),
            IndexColumn::Nullable(nullable_col) => {
                Column::Nullable(Box::new(NullableColumn::<AnyType> {
                    column: nullable_col.column.into(),
                    validity: nullable_col.validity,
                }))
            }
            IndexColumn::Tuple(tuple) => {
                Column::Tuple(tuple.into_iter().map(|c| c.into()).collect())
            }
            IndexColumn::Variant(variant) => Column::Variant(variant),
        }
    }
}

impl From<Scalar> for IndexScalar {
    fn from(value: Scalar) -> Self {
        match value {
            Scalar::Null => IndexScalar::Null,
            Scalar::Number(num_scalar) => IndexScalar::Number(num_scalar),
            Scalar::Decimal(dec_scalar) => IndexScalar::Decimal(dec_scalar),
            Scalar::Timestamp(ts) => IndexScalar::Timestamp(ts),
            Scalar::Date(date) => IndexScalar::Date(date),
            Scalar::Boolean(b) => IndexScalar::Boolean(b),
            Scalar::String(string) => IndexScalar::String(string.as_bytes().to_vec()),
            Scalar::Binary(s) => IndexScalar::String(s),
            Scalar::Array(column) => IndexScalar::Array(column.into()),
            Scalar::Map(column) => IndexScalar::Map(column.into()),
            Scalar::Bitmap(bitmap) => IndexScalar::Bitmap(bitmap),
            Scalar::Tuple(tuple) => {
                IndexScalar::Tuple(tuple.into_iter().map(|c| c.into()).collect())
            }
            Scalar::Variant(variant) => IndexScalar::Variant(variant),
            Scalar::EmptyArray | Scalar::EmptyMap => unreachable!(),
        }
    }
}

impl From<Column> for IndexColumn {
    fn from(value: Column) -> Self {
        match value {
            Column::Null { len } => IndexColumn::Null { len },
            Column::Number(num_col) => IndexColumn::Number(num_col),
            Column::Decimal(dec_col) => IndexColumn::Decimal(dec_col),
            Column::Boolean(bmp) => IndexColumn::Boolean(bmp),
            Column::Binary(_) => unreachable!(),
            Column::String(str_col) => IndexColumn::String(str_col.into()),
            Column::Timestamp(buf) => IndexColumn::Timestamp(buf),
            Column::Date(buf) => IndexColumn::Date(buf),
            Column::Array(arr_col) => IndexColumn::Array(Box::new(IndexArrayColumn {
                values: arr_col.values.into(),
                offsets: arr_col.offsets,
            })),
            Column::Map(map_col) => IndexColumn::Map(Box::new(IndexArrayColumn {
                values: map_col.values.into(),
                offsets: map_col.offsets,
            })),
            Column::Bitmap(str_col) => IndexColumn::Bitmap(str_col),
            Column::Nullable(nullable_col) => {
                IndexColumn::Nullable(Box::new(IndexNullableColumn {
                    column: nullable_col.column.into(),
                    validity: nullable_col.validity,
                }))
            }
            Column::Tuple(tuple) => {
                IndexColumn::Tuple(tuple.into_iter().map(|c| c.into()).collect())
            }
            Column::Variant(variant) => IndexColumn::Variant(variant),
            Column::EmptyArray { .. } | Column::EmptyMap { .. } => unreachable!(),
        }
    }
}

// Serialize a column to a base64 string.
// Because we may use serde::json/bincode to serialize the column, so we wrap it into string
impl Serialize for IndexColumn {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where S: Serializer {
        let c: Column = self.clone().into();

        Serialize::serialize(&c, serializer)
    }
}

impl<'de> Deserialize<'de> for IndexColumn {
    fn deserialize<D>(deserializer: D) -> Result<IndexColumn, D::Error>
    where D: Deserializer<'de> {
        let c: Column = Deserialize::deserialize(deserializer)?;
        Ok(c.into())
    }
}

impl PartialEq for IndexColumn {
    fn eq(&self, other: &Self) -> bool {
        let a: Column = self.clone().into();
        let b: Column = other.clone().into();
        a.eq(&b)
    }
}

impl Eq for IndexColumn {}
