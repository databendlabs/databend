// Copyright 2023 Datafuse Labs.
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

use anyhow::{Error, Result};
use databend_client::response::SchemaField;
use enum_as_inner::EnumAsInner;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, EnumAsInner)]
pub enum NumberDataType {
    UInt8,
    UInt16,
    UInt32,
    UInt64,
    Int8,
    Int16,
    Int32,
    Int64,
    Float32,
    Float64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct DecimalSize {
    pub precision: u8,
    pub scale: u8,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, EnumAsInner)]
pub enum DecimalDataType {
    Decimal128(DecimalSize),
    Decimal256(DecimalSize),
}

#[derive(Debug, Clone)]
pub enum DataType {
    Null,
    EmptyArray,
    EmptyMap,
    Boolean,
    String,
    Number(NumberDataType),
    Decimal(DecimalDataType),
    Timestamp,
    Date,
    Nullable(Box<DataType>),
    Array(Box<DataType>),
    Map(Box<DataType>),
    Tuple(Vec<DataType>),
    Variant,
    Generic(usize),
}

impl TryFrom<String> for DataType {
    type Error = Error;

    fn try_from(_s: String) -> Result<Self> {
        // TODO:(everpcpc) parse schema
        Ok(Self::String)
    }
}

impl TryFrom<SchemaField> for DataType {
    type Error = Error;

    fn try_from(field: SchemaField) -> Result<Self> {
        Self::try_from(field.r#type)
    }
}

pub(crate) struct SchemaFieldList(Vec<SchemaField>);

impl SchemaFieldList {
    pub(crate) fn new(fields: Vec<SchemaField>) -> Self {
        Self(fields)
    }
}

impl TryFrom<SchemaFieldList> for Vec<DataType> {
    type Error = Error;

    fn try_from(fields: SchemaFieldList) -> Result<Self> {
        fields.0.into_iter().map(DataType::try_from).collect()
    }
}
