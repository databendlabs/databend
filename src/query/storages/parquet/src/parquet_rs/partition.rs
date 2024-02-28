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

use databend_common_expression::Scalar;
use parquet::arrow::arrow_reader::RowSelector;
use parquet::file::metadata::RowGroupMetaData;
use parquet::format::PageLocation;
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

/// Serializable row selector.
#[derive(serde::Serialize, serde::Deserialize, PartialEq, Eq, Clone, Debug)]
pub struct SerdeRowSelector {
    pub row_count: usize,
    pub skip: bool,
}

impl From<&RowSelector> for SerdeRowSelector {
    fn from(value: &RowSelector) -> Self {
        SerdeRowSelector {
            row_count: value.row_count,
            skip: value.skip,
        }
    }
}

impl From<&SerdeRowSelector> for RowSelector {
    fn from(value: &SerdeRowSelector) -> Self {
        RowSelector {
            row_count: value.row_count,
            skip: value.skip,
        }
    }
}

/// Serializable page location.
#[derive(serde::Serialize, serde::Deserialize, PartialEq, Eq, Clone, Debug)]
pub struct SerdePageLocation {
    pub offset: i64,
    pub compressed_page_size: i32,
    pub first_row_index: i64,
}

impl From<&PageLocation> for SerdePageLocation {
    fn from(value: &PageLocation) -> Self {
        SerdePageLocation {
            offset: value.offset,
            compressed_page_size: value.compressed_page_size,
            first_row_index: value.first_row_index,
        }
    }
}

impl From<&SerdePageLocation> for PageLocation {
    fn from(value: &SerdePageLocation) -> Self {
        PageLocation {
            offset: value.offset,
            compressed_page_size: value.compressed_page_size,
            first_row_index: value.first_row_index,
        }
    }
}

#[derive(serde::Serialize, serde::Deserialize, PartialEq, Clone, Debug)]
pub struct ParquetRSRowGroupPart {
    pub location: String,
    #[serde(
        serialize_with = "ser_row_group_meta",
        deserialize_with = "deser_row_group_meta"
    )]
    pub meta: RowGroupMetaData,
    pub selectors: Option<Vec<SerdeRowSelector>>,
    pub page_locations: Option<Vec<Vec<SerdePageLocation>>>,
    // `uncompressed_size` and `compressed_size` are the sizes of the actually read columns.
    pub uncompressed_size: u64,
    pub compressed_size: u64,
    pub sort_min_max: Option<(Scalar, Scalar)>,
    pub omit_filter: bool,

    pub schema_index: usize,
}

impl Eq for ParquetRSRowGroupPart {}

impl ParquetRSRowGroupPart {
    pub fn uncompressed_size(&self) -> u64 {
        self.uncompressed_size
    }

    pub fn compressed_size(&self) -> u64 {
        self.compressed_size
    }
}

fn deser_row_group_meta<'de, D>(deserializer: D) -> Result<RowGroupMetaData, D::Error>
where D: serde::Deserializer<'de> {
    let bytes: Vec<u8> = Deserialize::deserialize(deserializer)?;
    let cursor = Cursor::new(bytes);

    let mut i_prot = TCompactInputProtocol::new(cursor);
    // Deserialize schema first.
    let list_ident = i_prot.read_list_begin().unwrap();
    let mut schema_elements: Vec<SchemaElement> = Vec::with_capacity(list_ident.size as usize);
    for _ in 0..list_ident.size {
        let list_elem = SchemaElement::read_from_in_protocol(&mut i_prot).unwrap();
        schema_elements.push(list_elem);
    }
    i_prot.read_list_end().unwrap();
    let schema = from_thrift(&schema_elements).unwrap();
    let schema = Arc::new(SchemaDescriptor::new(schema));

    // Then deserialize row group meta.
    let rg = RowGroup::read_from_in_protocol(&mut i_prot).unwrap();
    let meta = RowGroupMetaData::from_thrift(schema, rg).unwrap();

    Ok(meta)
}

fn ser_row_group_meta<S>(meta: &RowGroupMetaData, serializer: S) -> Result<S::Ok, S::Error>
where S: serde::Serializer {
    let mut transport = Vec::<u8>::new();
    let mut o_prot = TCompactOutputProtocol::new(&mut transport);

    // Serialize schema first.
    let schema = meta.schema_descr();
    let schema_elements = to_thrift(schema.root_schema()).map_err(|e| {
        serde::ser::Error::custom(format!("Failed to convert schema to thrift: {:?}", e))
    })?;
    o_prot
        .write_list_begin(&TListIdentifier::new(
            TType::Struct,
            schema_elements.len() as i32,
        ))
        .unwrap();
    for e in schema_elements {
        e.write_to_out_protocol(&mut o_prot).unwrap();
    }
    o_prot.write_list_end().unwrap();

    // Then Serialize row group meta.
    let rg = meta.to_thrift();
    rg.write_to_out_protocol(&mut o_prot).map_err(|e| {
        serde::ser::Error::custom(format!(
            "Failed to convert row group meta to thrift: {:?}",
            e
        ))
    })?;
    serializer.serialize_bytes(&transport)
}
