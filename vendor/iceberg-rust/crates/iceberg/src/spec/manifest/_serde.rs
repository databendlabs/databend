// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

use std::collections::HashMap;

use serde_derive::{Deserialize, Serialize};
use serde_with::serde_as;

use super::{Datum, ManifestEntry, Schema, Struct};
use crate::spec::{FormatVersion, Literal, RawLiteral, StructType, Type};
use crate::{Error, ErrorKind, metadata_columns};

#[derive(Serialize, Deserialize)]
pub(super) struct ManifestEntryV2 {
    status: i32,
    snapshot_id: Option<i64>,
    sequence_number: Option<i64>,
    file_sequence_number: Option<i64>,
    data_file: DataFileSerde,
}

impl ManifestEntryV2 {
    pub fn try_from(value: ManifestEntry, partition_type: &StructType) -> Result<Self, Error> {
        Ok(Self {
            status: value.status as i32,
            snapshot_id: value.snapshot_id,
            sequence_number: value.sequence_number,
            file_sequence_number: value.file_sequence_number,
            data_file: DataFileSerde::try_from(value.data_file, partition_type, FormatVersion::V2)?,
        })
    }

    pub fn try_into(
        self,
        partition_spec_id: i32,
        partition_type: &StructType,
        schema: &Schema,
    ) -> Result<ManifestEntry, Error> {
        Ok(ManifestEntry {
            status: self.status.try_into()?,
            snapshot_id: self.snapshot_id,
            sequence_number: self.sequence_number,
            file_sequence_number: self.file_sequence_number,
            data_file: self
                .data_file
                .try_into(partition_spec_id, partition_type, schema)?,
        })
    }
}

#[derive(Serialize, Deserialize)]
pub(super) struct ManifestEntryV1 {
    status: i32,
    pub snapshot_id: i64,
    data_file: DataFileSerde,
}

impl ManifestEntryV1 {
    pub fn try_from(value: ManifestEntry, partition_type: &StructType) -> Result<Self, Error> {
        Ok(Self {
            status: value.status as i32,
            snapshot_id: value.snapshot_id.unwrap_or_default(),
            data_file: DataFileSerde::try_from(value.data_file, partition_type, FormatVersion::V1)?,
        })
    }

    pub fn try_into(
        self,
        partition_spec_id: i32,
        partition_type: &StructType,
        schema: &Schema,
    ) -> Result<ManifestEntry, Error> {
        Ok(ManifestEntry {
            status: self.status.try_into()?,
            snapshot_id: Some(self.snapshot_id),
            sequence_number: Some(0),
            file_sequence_number: Some(0),
            data_file: self
                .data_file
                .try_into(partition_spec_id, partition_type, schema)?,
        })
    }
}

#[serde_as]
#[derive(Serialize, Deserialize)]
pub(super) struct DataFileSerde {
    #[serde(default)]
    content: i32,
    file_path: String,
    file_format: String,
    partition: RawLiteral,
    record_count: i64,
    file_size_in_bytes: i64,
    #[serde(skip_deserializing, skip_serializing_if = "Option::is_none")]
    block_size_in_bytes: Option<i64>,
    column_sizes: Option<Vec<I64Entry>>,
    value_counts: Option<Vec<I64Entry>>,
    null_value_counts: Option<Vec<I64Entry>>,
    nan_value_counts: Option<Vec<I64Entry>>,
    lower_bounds: Option<Vec<BytesEntry>>,
    upper_bounds: Option<Vec<BytesEntry>>,
    key_metadata: Option<serde_bytes::ByteBuf>,
    split_offsets: Option<Vec<i64>>,
    equality_ids: Option<Vec<i32>>,
    sort_order_id: Option<i32>,
    first_row_id: Option<i64>,
    referenced_data_file: Option<String>,
    content_offset: Option<i64>,
    content_size_in_bytes: Option<i64>,
}

impl DataFileSerde {
    pub fn try_from(
        value: super::DataFile,
        partition_type: &StructType,
        format_version: FormatVersion,
    ) -> Result<Self, Error> {
        let block_size_in_bytes = if format_version == FormatVersion::V1 {
            Some(0)
        } else {
            None
        };
        Ok(Self {
            content: value.content as i32,
            file_path: value.file_path,
            file_format: value.file_format.to_string().to_ascii_uppercase(),
            partition: RawLiteral::try_from(
                Literal::Struct(value.partition),
                &Type::Struct(partition_type.clone()),
            )?,
            record_count: value.record_count.try_into()?,
            file_size_in_bytes: value.file_size_in_bytes.try_into()?,
            block_size_in_bytes,
            column_sizes: Some(to_i64_entry(value.column_sizes)?),
            value_counts: Some(to_i64_entry(value.value_counts)?),
            null_value_counts: Some(to_i64_entry(value.null_value_counts)?),
            nan_value_counts: Some(to_i64_entry(value.nan_value_counts)?),
            lower_bounds: Some(to_bytes_entry(value.lower_bounds)?),
            upper_bounds: Some(to_bytes_entry(value.upper_bounds)?),
            key_metadata: value.key_metadata.map(serde_bytes::ByteBuf::from),
            split_offsets: value.split_offsets,
            equality_ids: value.equality_ids,
            sort_order_id: value.sort_order_id,
            first_row_id: value.first_row_id,
            referenced_data_file: value.referenced_data_file,
            content_offset: value.content_offset,
            content_size_in_bytes: value.content_size_in_bytes,
        })
    }

    pub fn try_into(
        self,
        partition_spec_id: i32,
        partition_type: &StructType,
        schema: &Schema,
    ) -> Result<super::DataFile, Error> {
        let partition = self
            .partition
            .try_into(&Type::Struct(partition_type.clone()))?
            .map(|v| {
                if let Literal::Struct(v) = v {
                    Ok(v)
                } else {
                    Err(Error::new(
                        ErrorKind::DataInvalid,
                        "partition value is not a struct",
                    ))
                }
            })
            .transpose()?
            .unwrap_or(Struct::empty());
        Ok(super::DataFile {
            content: self.content.try_into()?,
            file_path: self.file_path,
            file_format: self.file_format.parse()?,
            partition,
            record_count: self.record_count.try_into()?,
            file_size_in_bytes: self.file_size_in_bytes.try_into()?,
            column_sizes: self
                .column_sizes
                .map(parse_i64_entry)
                .transpose()?
                .unwrap_or_default(),
            value_counts: self
                .value_counts
                .map(parse_i64_entry)
                .transpose()?
                .unwrap_or_default(),
            null_value_counts: self
                .null_value_counts
                .map(parse_i64_entry)
                .transpose()?
                .unwrap_or_default(),
            nan_value_counts: self
                .nan_value_counts
                .map(parse_i64_entry)
                .transpose()?
                .unwrap_or_default(),
            lower_bounds: self
                .lower_bounds
                .map(|v| parse_bytes_entry(v, schema))
                .transpose()?
                .unwrap_or_default(),
            upper_bounds: self
                .upper_bounds
                .map(|v| parse_bytes_entry(v, schema))
                .transpose()?
                .unwrap_or_default(),
            key_metadata: self.key_metadata.map(|v| v.to_vec()),
            split_offsets: self.split_offsets,
            equality_ids: self.equality_ids,
            sort_order_id: self.sort_order_id,
            partition_spec_id,
            first_row_id: self.first_row_id,
            referenced_data_file: self.referenced_data_file,
            content_offset: self.content_offset,
            content_size_in_bytes: self.content_size_in_bytes,
        })
    }
}

#[serde_as]
#[derive(Serialize, Deserialize)]
#[cfg_attr(test, derive(Debug, PartialEq, Eq))]
struct BytesEntry {
    key: i32,
    value: serde_bytes::ByteBuf,
}

fn parse_bytes_entry(v: Vec<BytesEntry>, schema: &Schema) -> Result<HashMap<i32, Datum>, Error> {
    let mut m = HashMap::with_capacity(v.len());
    for entry in v {
        // First try to find the field in the schema, or check if it's a reserved metadata field
        let field = schema
            .field_by_id(entry.key)
            .or_else(|| metadata_columns::get_metadata_field(entry.key).ok());

        if let Some(field) = field {
            let data_type = field
                .field_type
                .as_primitive_type()
                .ok_or_else(|| {
                    Error::new(
                        ErrorKind::DataInvalid,
                        format!("field {} is not a primitive type", field.name),
                    )
                })?
                .clone();
            m.insert(entry.key, Datum::try_from_bytes(&entry.value, data_type)?);
        }
        // We ignore the entry if the field is not found in schema or metadata columns (schema evolution).
    }
    Ok(m)
}

fn to_bytes_entry(v: impl IntoIterator<Item = (i32, Datum)>) -> Result<Vec<BytesEntry>, Error> {
    let iter = v.into_iter();
    // Reserve the capacity to the lower bound.
    let mut bs = Vec::with_capacity(iter.size_hint().0);
    for (k, d) in iter {
        bs.push(BytesEntry {
            key: k,
            value: d.to_bytes()?,
        });
    }
    Ok(bs)
}

#[derive(Serialize, Deserialize)]
#[cfg_attr(test, derive(Debug, PartialEq, Eq))]
struct I64Entry {
    key: i32,
    value: i64,
}

fn parse_i64_entry(v: Vec<I64Entry>) -> Result<HashMap<i32, u64>, Error> {
    let mut m = HashMap::with_capacity(v.len());
    for entry in v {
        // We ignore the entry if it's value is negative since these entries are supposed to be used for
        // counting, which should never be negative.
        if let Ok(v) = entry.value.try_into() {
            m.insert(entry.key, v);
        }
    }
    Ok(m)
}

#[allow(unused_mut)]
fn to_i64_entry(entries: HashMap<i32, u64>) -> Result<Vec<I64Entry>, Error> {
    let mut i64_entries = entries
        .iter()
        .map(|e| {
            Ok(I64Entry {
                key: *e.0,
                value: (*e.1).try_into()?,
            })
        })
        .collect::<Result<Vec<_>, Error>>()?;

    // Ensure that the order is deterministic during testing
    #[cfg(test)]
    i64_entries.sort_by_key(|e| e.key);

    Ok(i64_entries)
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::io::Cursor;
    use std::sync::Arc;

    use crate::spec::manifest::_serde::{I64Entry, parse_i64_entry};
    use crate::spec::{
        DataContentType, DataFile, DataFileFormat, Datum, FormatVersion, NestedField,
        PrimitiveType, Schema, Struct, StructType, Type, read_data_files_from_avro,
        write_data_files_to_avro,
    };

    #[test]
    fn test_parse_negative_manifest_entry() {
        let entries = vec![I64Entry { key: 1, value: -1 }, I64Entry {
            key: 2,
            value: 3,
        }];

        let ret = parse_i64_entry(entries).unwrap();

        let expected_ret = HashMap::from([(2, 3)]);
        assert_eq!(ret, expected_ret, "Negative i64 entry should be ignored!");
    }

    fn schema() -> Arc<Schema> {
        Arc::new(
            Schema::builder()
                .with_fields(vec![
                    Arc::new(NestedField::optional(
                        1,
                        "v1",
                        Type::Primitive(PrimitiveType::Int),
                    )),
                    Arc::new(NestedField::optional(
                        2,
                        "v2",
                        Type::Primitive(PrimitiveType::String),
                    )),
                    Arc::new(NestedField::optional(
                        3,
                        "v3",
                        Type::Primitive(PrimitiveType::String),
                    )),
                ])
                .build()
                .unwrap(),
        )
    }

    fn data_files() -> Vec<DataFile> {
        vec![DataFile {
            content: DataContentType::Data,
            file_path: "s3://testbucket/iceberg_data/iceberg_ctl/iceberg_db/iceberg_tbl/data/00000-7-45268d71-54eb-476c-b42c-942d880c04a1-00001.parquet".to_string(),
            file_format: DataFileFormat::Parquet,
            partition: Struct::empty(),
            record_count: 1,
            file_size_in_bytes: 875,
            column_sizes: HashMap::from([(1,47),(2,48),(3,52)]),
            value_counts: HashMap::from([(1,1),(2,1),(3,1)]),
            null_value_counts: HashMap::from([(1,0),(2,0),(3,0)]),
            nan_value_counts: HashMap::new(),
            lower_bounds: HashMap::from([(1,Datum::int(1)),(2,Datum::string("a")),(3,Datum::string("AC/DC"))]),
            upper_bounds: HashMap::from([(1,Datum::int(1)),(2,Datum::string("a")),(3,Datum::string("AC/DC"))]),
            key_metadata: None,
            split_offsets: Some(vec![4]),
            equality_ids: None,
            sort_order_id: Some(0),
            partition_spec_id: 0,
            first_row_id: None,
            referenced_data_file: None,
            content_offset: None,
            content_size_in_bytes: None,
        }]
    }

    #[tokio::test]
    async fn test_data_file_serialize_deserialize() {
        let schema = schema();
        let data_files = data_files();

        let mut buffer = Vec::new();
        let _ = write_data_files_to_avro(
            &mut buffer,
            data_files.clone().into_iter(),
            &StructType::new(vec![]),
            FormatVersion::V2,
        )
        .unwrap();

        let actual_data_file = read_data_files_from_avro(
            &mut Cursor::new(buffer),
            &schema,
            0,
            &StructType::new(vec![]),
            FormatVersion::V2,
        )
        .unwrap();

        assert_eq!(data_files, actual_data_file);
    }

    #[tokio::test]
    async fn test_data_file_serialize_deserialize_v1_data_on_v2_reader() {
        let schema = schema();
        let data_files = data_files();

        let mut buffer = Vec::new();
        let _ = write_data_files_to_avro(
            &mut buffer,
            data_files.clone().into_iter(),
            &StructType::new(vec![]),
            FormatVersion::V1,
        )
        .unwrap();

        let actual_data_file = read_data_files_from_avro(
            &mut Cursor::new(buffer),
            &schema,
            0,
            &StructType::new(vec![]),
            FormatVersion::V2,
        )
        .unwrap();

        assert_eq!(actual_data_file[0].content, DataContentType::Data)
    }

    #[test]
    fn test_manifest_entry_v1_to_v2_projection() {
        use crate::spec::manifest::_serde::{DataFileSerde, ManifestEntryV1};
        use crate::spec::{Literal, RawLiteral, Struct, StructType};

        let partition = RawLiteral::try_from(
            Literal::Struct(Struct::empty()),
            &Type::Struct(StructType::new(vec![])),
        )
        .unwrap();

        // Create a V1 manifest entry struct (lacks V2 sequence number fields)
        let v1_entry = ManifestEntryV1 {
            status: 1, // Added
            snapshot_id: 12345,
            data_file: DataFileSerde {
                content: 0, // DataFileSerde is shared between V1/V2
                file_path: "test/path.parquet".to_string(),
                file_format: "PARQUET".to_string(),
                partition,
                record_count: 100,
                file_size_in_bytes: 1024,
                block_size_in_bytes: Some(0), // V1 includes this field
                column_sizes: None,
                value_counts: None,
                null_value_counts: None,
                nan_value_counts: None,
                lower_bounds: None,
                upper_bounds: None,
                key_metadata: None,
                split_offsets: None,
                equality_ids: None, // Will be converted to empty vec
                sort_order_id: None,
                first_row_id: None,
                referenced_data_file: None,
                content_offset: None,
                content_size_in_bytes: None,
            },
        };

        // Test the explicit V1→V2 conversion logic in ManifestEntryV1::try_into()
        let v2_entry = v1_entry
            .try_into(
                0, // partition_spec_id
                &StructType::new(vec![]),
                &schema(),
            )
            .unwrap();

        // Verify that V1→V2 conversion adds the missing V2 sequence number fields
        assert_eq!(
            v2_entry.sequence_number,
            Some(0),
            "ManifestEntryV1::try_into() should set sequence_number to 0"
        );
        assert_eq!(
            v2_entry.file_sequence_number,
            Some(0),
            "ManifestEntryV1::try_into() should set file_sequence_number to 0"
        );
        assert_eq!(
            v2_entry.snapshot_id,
            Some(12345),
            "snapshot_id should be preserved during conversion"
        );

        // Verify that DataFileSerde conversion applies V2 defaults
        assert_eq!(
            v2_entry.data_file.content,
            DataContentType::Data,
            "DataFileSerde should convert content 0 to DataContentType::Data"
        );
        assert_eq!(
            v2_entry.data_file.equality_ids, None,
            "DataFileSerde should preserve None equality_ids as None"
        );

        // Verify other fields are preserved during conversion
        assert_eq!(v2_entry.data_file.file_path, "test/path.parquet");
        assert_eq!(v2_entry.data_file.record_count, 100);
        assert_eq!(v2_entry.data_file.file_size_in_bytes, 1024);
    }

    #[test]
    fn test_data_file_serde_v1_field_defaults() {
        use crate::spec::manifest::_serde::DataFileSerde;
        use crate::spec::{Literal, RawLiteral, Struct, StructType};

        let partition = RawLiteral::try_from(
            Literal::Struct(Struct::empty()),
            &Type::Struct(StructType::new(vec![])),
        )
        .unwrap();

        // Create a DataFileSerde that simulates V1 deserialization behavior
        // (missing V2 fields would be None due to #[serde(default)])
        let v1_style_data_file = DataFileSerde {
            content: 0, // V1 doesn't have this field, defaults to 0 via #[serde(default)]
            file_path: "test/data.parquet".to_string(),
            file_format: "PARQUET".to_string(),
            partition,
            record_count: 500,
            file_size_in_bytes: 2048,
            block_size_in_bytes: Some(1024), // V1 includes this field, V2 skips it
            column_sizes: None,
            value_counts: None,
            null_value_counts: None,
            nan_value_counts: None,
            lower_bounds: None,
            upper_bounds: None,
            key_metadata: None,
            split_offsets: None,
            equality_ids: None, // V1 doesn't have this field, defaults to None via #[serde(default)]
            sort_order_id: None,
            first_row_id: None,
            referenced_data_file: None,
            content_offset: None,
            content_size_in_bytes: None,
        };

        // Test the DataFileSerde::try_into() conversion that handles V1 field defaults
        let data_file = v1_style_data_file
            .try_into(
                0, // partition_spec_id
                &StructType::new(vec![]),
                &schema(),
            )
            .unwrap();

        // Verify that DataFileSerde::try_into() applies correct defaults for missing V2 fields
        assert_eq!(
            data_file.content,
            DataContentType::Data,
            "content 0 should convert to DataContentType::Data"
        );
        assert_eq!(
            data_file.equality_ids, None,
            "None equality_ids should remain as None"
        );

        // Verify other fields are handled correctly during conversion
        assert_eq!(data_file.file_path, "test/data.parquet");
        assert_eq!(data_file.file_format, DataFileFormat::Parquet);
        assert_eq!(data_file.record_count, 500);
        assert_eq!(data_file.file_size_in_bytes, 2048);
        assert_eq!(data_file.partition_spec_id, 0);
    }

    #[test]
    fn test_parse_bytes_entry_with_metadata_column() {
        use crate::metadata_columns::RESERVED_FIELD_ID_POS;
        use crate::spec::manifest::_serde::{BytesEntry, parse_bytes_entry};

        // Create a minimal schema that doesn't include the _pos metadata column
        let test_schema = schema();

        // Create a BytesEntry with the _pos field ID (reserved metadata field)
        // The _pos field is a Long (i64) in the metadata column definition
        let pos_value: i64 = 42;
        let bytes_entry = BytesEntry {
            key: RESERVED_FIELD_ID_POS,
            value: serde_bytes::ByteBuf::from(pos_value.to_le_bytes().to_vec()),
        };

        // Parse the bytes entry - should use metadata column definition for _pos
        let result = parse_bytes_entry(vec![bytes_entry], &test_schema).unwrap();

        // Verify that the _pos field was parsed correctly using metadata column definition
        assert!(
            result.contains_key(&RESERVED_FIELD_ID_POS),
            "_pos metadata field should be parsed"
        );
        assert_eq!(
            result.get(&RESERVED_FIELD_ID_POS),
            Some(&Datum::long(pos_value)),
            "_pos should be parsed as long with correct value"
        );
    }
}
