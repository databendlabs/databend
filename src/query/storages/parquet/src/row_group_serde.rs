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

use std::io::Cursor;
use std::sync::Arc;

use databend_common_exception::ErrorCode;
use parquet::file::metadata::RowGroupMetaData;
use parquet::format::RowGroup;
use parquet::format::SchemaElement;
use parquet::schema::types::from_thrift;
use parquet::schema::types::to_thrift;
use parquet::schema::types::SchemaDescriptor;
use parquet::thrift::TSerializable;
use serde::Deserialize;
use thrift::protocol::TCompactInputProtocol;
use thrift::protocol::TCompactOutputProtocol;
use thrift::protocol::TInputProtocol;
use thrift::protocol::TListIdentifier;
use thrift::protocol::TOutputProtocol;
use thrift::protocol::TType;

pub fn serialize_row_group_meta_to_bytes(meta: &RowGroupMetaData) -> Result<Vec<u8>, ErrorCode> {
    let mut transport = Vec::<u8>::new();
    let mut o_prot = TCompactOutputProtocol::new(&mut transport);

    let schema = meta.schema_descr();
    let schema_elements = to_thrift(schema.root_schema())
        .map_err(|e| wrap_error(e, " (while converting schema to thrift)"))?;
    o_prot
        .write_list_begin(&TListIdentifier::new(
            TType::Struct,
            schema_elements.len() as i32,
        ))
        .map_err(|e| wrap_error(e, " (while writing schema list header)"))?;
    for element in schema_elements {
        element
            .write_to_out_protocol(&mut o_prot)
            .map_err(|e| wrap_error(e, " (while writing schema element)"))?;
    }
    o_prot
        .write_list_end()
        .map_err(|e| wrap_error(e, " (while finishing schema list)"))?;

    let rg = meta.to_thrift();
    rg.write_to_out_protocol(&mut o_prot)
        .map_err(|e| wrap_error(e, " (while writing row group meta)"))?;

    Ok(transport)
}

pub fn deserialize_row_group_meta_from_bytes(bytes: &[u8]) -> Result<RowGroupMetaData, ErrorCode> {
    let cursor = Cursor::new(bytes);
    let mut i_prot = TCompactInputProtocol::new(cursor);

    let list_ident = i_prot
        .read_list_begin()
        .map_err(|e| wrap_error(e, " (while reading schema list header)"))?;
    let mut schema_elements: Vec<SchemaElement> = Vec::with_capacity(list_ident.size as usize);
    for _ in 0..list_ident.size {
        let element = SchemaElement::read_from_in_protocol(&mut i_prot)
            .map_err(|e| wrap_error(e, " (while reading schema element)"))?;
        schema_elements.push(element);
    }
    i_prot
        .read_list_end()
        .map_err(|e| wrap_error(e, " (while finishing schema list)"))?;
    let schema = from_thrift(&schema_elements)
        .map_err(|e| wrap_error(e, " (while converting thrift schema)"))?;
    let schema = Arc::new(SchemaDescriptor::new(schema));

    let rg = RowGroup::read_from_in_protocol(&mut i_prot)
        .map_err(|e| wrap_error(e, " (while reading row group meta)"))?;
    RowGroupMetaData::from_thrift(schema, rg)
        .map_err(|e| wrap_error(e, " (while constructing row group meta)"))
}

pub fn serialize_row_group_meta<S>(
    meta: &RowGroupMetaData,
    serializer: S,
) -> Result<S::Ok, S::Error>
where
    S: serde::Serializer,
{
    let bytes = serialize_row_group_meta_to_bytes(meta)
        .map_err(|e| serde::ser::Error::custom(e.to_string()))?;
    serializer.serialize_bytes(&bytes)
}

pub fn deserialize_row_group_meta<'de, D>(deserializer: D) -> Result<RowGroupMetaData, D::Error>
where D: serde::Deserializer<'de> {
    let bytes: Vec<u8> = Deserialize::deserialize(deserializer)?;
    deserialize_row_group_meta_from_bytes(&bytes)
        .map_err(|e| serde::de::Error::custom(e.to_string()))
}

fn wrap_error<E>(err: E, context: &'static str) -> ErrorCode
where E: std::error::Error + Send + Sync + 'static {
    ErrorCode::from_std_error(err).add_message_back(context)
}
