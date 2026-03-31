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

mod _serde;

mod data_file;
pub use data_file::*;
mod entry;
pub use entry::*;
mod metadata;
pub use metadata::*;
mod writer;
use std::sync::Arc;

use apache_avro::{Reader as AvroReader, from_value};
pub use writer::*;

use super::{
    Datum, FormatVersion, ManifestContentType, PartitionSpec, PrimitiveType, Schema, Struct,
    UNASSIGNED_SEQUENCE_NUMBER,
};
use crate::error::Result;
use crate::{Error, ErrorKind};

/// A manifest contains metadata and a list of entries.
#[derive(Debug, PartialEq, Eq, Clone)]
pub struct Manifest {
    metadata: ManifestMetadata,
    entries: Vec<ManifestEntryRef>,
}

impl Manifest {
    /// Parse manifest metadata and entries from bytes of avro file.
    pub(crate) fn try_from_avro_bytes(bs: &[u8]) -> Result<(ManifestMetadata, Vec<ManifestEntry>)> {
        let reader = AvroReader::new(bs)?;

        // Parse manifest metadata
        let meta = reader.user_metadata();
        let metadata = ManifestMetadata::parse(meta)?;

        // Parse manifest entries
        let partition_type = metadata.partition_spec.partition_type(&metadata.schema)?;

        let entries = match metadata.format_version {
            FormatVersion::V1 => {
                let schema = manifest_schema_v1(&partition_type)?;
                let reader = AvroReader::with_schema(&schema, bs)?;
                reader
                    .into_iter()
                    .map(|value| {
                        from_value::<_serde::ManifestEntryV1>(&value?)?.try_into(
                            metadata.partition_spec.spec_id(),
                            &partition_type,
                            &metadata.schema,
                        )
                    })
                    .collect::<Result<Vec<_>>>()?
            }
            // Manifest Schema & Manifest Entry did not change between V2 and V3
            FormatVersion::V2 | FormatVersion::V3 => {
                let schema = manifest_schema_v2(&partition_type)?;
                let reader = AvroReader::with_schema(&schema, bs)?;
                reader
                    .into_iter()
                    .map(|value| {
                        from_value::<_serde::ManifestEntryV2>(&value?)?.try_into(
                            metadata.partition_spec.spec_id(),
                            &partition_type,
                            &metadata.schema,
                        )
                    })
                    .collect::<Result<Vec<_>>>()?
            }
        };

        Ok((metadata, entries))
    }

    /// Parse manifest from bytes of avro file.
    pub fn parse_avro(bs: &[u8]) -> Result<Self> {
        let (metadata, entries) = Self::try_from_avro_bytes(bs)?;
        Ok(Self::new(metadata, entries))
    }

    /// Entries slice.
    pub fn entries(&self) -> &[ManifestEntryRef] {
        &self.entries
    }

    /// Get metadata.
    pub fn metadata(&self) -> &ManifestMetadata {
        &self.metadata
    }

    /// Consume this Manifest, returning its constituent parts
    pub fn into_parts(self) -> (Vec<ManifestEntryRef>, ManifestMetadata) {
        let Self { entries, metadata } = self;
        (entries, metadata)
    }

    /// Constructor from [`ManifestMetadata`] and [`ManifestEntry`]s.
    pub fn new(metadata: ManifestMetadata, entries: Vec<ManifestEntry>) -> Self {
        Self {
            metadata,
            entries: entries.into_iter().map(Arc::new).collect(),
        }
    }
}

/// Serialize a DataFile to a JSON string.
pub fn serialize_data_file_to_json(
    data_file: DataFile,
    partition_type: &super::StructType,
    format_version: FormatVersion,
) -> Result<String> {
    let serde = _serde::DataFileSerde::try_from(data_file, partition_type, format_version)?;
    serde_json::to_string(&serde).map_err(|e| {
        Error::new(
            ErrorKind::DataInvalid,
            "Failed to serialize DataFile to JSON!".to_string(),
        )
        .with_source(e)
    })
}

/// Deserialize a DataFile from a JSON string.
pub fn deserialize_data_file_from_json(
    json: &str,
    partition_spec_id: i32,
    partition_type: &super::StructType,
    schema: &Schema,
) -> Result<DataFile> {
    let serde = serde_json::from_str::<_serde::DataFileSerde>(json).map_err(|e| {
        Error::new(
            ErrorKind::DataInvalid,
            "Failed to deserialize JSON to DataFile!".to_string(),
        )
        .with_source(e)
    })?;

    serde.try_into(partition_spec_id, partition_type, schema)
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::fs;
    use std::sync::Arc;

    use serde_json::Value;
    use tempfile::TempDir;

    use super::*;
    use crate::io::FileIOBuilder;
    use crate::spec::{Literal, NestedField, PrimitiveType, Struct, Transform, Type};

    #[tokio::test]
    async fn test_parse_manifest_v2_unpartition() {
        let schema = Arc::new(
            Schema::builder()
                .with_fields(vec![
                    // id v_int v_long v_float v_double v_varchar v_bool v_date v_timestamp v_decimal v_ts_ntz
                    Arc::new(NestedField::optional(
                        1,
                        "id",
                        Type::Primitive(PrimitiveType::Long),
                    )),
                    Arc::new(NestedField::optional(
                        2,
                        "v_int",
                        Type::Primitive(PrimitiveType::Int),
                    )),
                    Arc::new(NestedField::optional(
                        3,
                        "v_long",
                        Type::Primitive(PrimitiveType::Long),
                    )),
                    Arc::new(NestedField::optional(
                        4,
                        "v_float",
                        Type::Primitive(PrimitiveType::Float),
                    )),
                    Arc::new(NestedField::optional(
                        5,
                        "v_double",
                        Type::Primitive(PrimitiveType::Double),
                    )),
                    Arc::new(NestedField::optional(
                        6,
                        "v_varchar",
                        Type::Primitive(PrimitiveType::String),
                    )),
                    Arc::new(NestedField::optional(
                        7,
                        "v_bool",
                        Type::Primitive(PrimitiveType::Boolean),
                    )),
                    Arc::new(NestedField::optional(
                        8,
                        "v_date",
                        Type::Primitive(PrimitiveType::Date),
                    )),
                    Arc::new(NestedField::optional(
                        9,
                        "v_timestamp",
                        Type::Primitive(PrimitiveType::Timestamptz),
                    )),
                    Arc::new(NestedField::optional(
                        10,
                        "v_decimal",
                        Type::Primitive(PrimitiveType::Decimal {
                            precision: 36,
                            scale: 10,
                        }),
                    )),
                    Arc::new(NestedField::optional(
                        11,
                        "v_ts_ntz",
                        Type::Primitive(PrimitiveType::Timestamp),
                    )),
                    Arc::new(NestedField::optional(
                        12,
                        "v_ts_ns_ntz",
                        Type::Primitive(PrimitiveType::TimestampNs),
                    )),
                ])
                .build()
                .unwrap(),
        );
        let metadata = ManifestMetadata {
            schema_id: 0,
            schema: schema.clone(),
            partition_spec: PartitionSpec::builder(schema)
                .with_spec_id(0)
                .build()
                .unwrap(),
            content: ManifestContentType::Data,
            format_version: FormatVersion::V2,
        };
        let mut entries = vec![
                ManifestEntry {
                    status: ManifestStatus::Added,
                    snapshot_id: None,
                    sequence_number: None,
                    file_sequence_number: None,
                    data_file: DataFile {content:DataContentType::Data,file_path:"s3a://icebergdata/demo/s1/t1/data/00000-0-ba56fbfa-f2ff-40c9-bb27-565ad6dc2be8-00000.parquet".to_string(),file_format:DataFileFormat::Parquet,partition:Struct::empty(),record_count:1,file_size_in_bytes:5442,column_sizes:HashMap::from([(0,73),(6,34),(2,73),(7,61),(3,61),(5,62),(9,79),(10,73),(1,61),(4,73),(8,73)]),value_counts:HashMap::from([(4,1),(5,1),(2,1),(0,1),(3,1),(6,1),(8,1),(1,1),(10,1),(7,1),(9,1)]),null_value_counts:HashMap::from([(1,0),(6,0),(2,0),(8,0),(0,0),(3,0),(5,0),(9,0),(7,0),(4,0),(10,0)]),nan_value_counts:HashMap::new(),lower_bounds:HashMap::new(),upper_bounds:HashMap::new(),key_metadata:None,split_offsets:Some(vec![4]),equality_ids:Some(Vec::new()),sort_order_id:None, partition_spec_id: 0,first_row_id: None,referenced_data_file: None,content_offset: None,content_size_in_bytes: None }
                }
            ];

        // write manifest to file
        let tmp_dir = TempDir::new().unwrap();
        let path = tmp_dir.path().join("test_manifest.avro");
        let io = FileIOBuilder::new_fs_io().build().unwrap();
        let output_file = io.new_output(path.to_str().unwrap()).unwrap();
        let mut writer = ManifestWriterBuilder::new(
            output_file,
            Some(1),
            None,
            metadata.schema.clone(),
            metadata.partition_spec.clone(),
        )
        .build_v2_data();
        for entry in &entries {
            writer.add_entry(entry.clone()).unwrap();
        }
        writer.write_manifest_file().await.unwrap();

        // read back the manifest file and check the content
        let actual_manifest =
            Manifest::parse_avro(fs::read(path).expect("read_file must succeed").as_slice())
                .unwrap();
        // The snapshot id is assigned when the entry is added to the manifest.
        entries[0].snapshot_id = Some(1);
        assert_eq!(actual_manifest, Manifest::new(metadata, entries));
    }

    #[tokio::test]
    async fn test_parse_manifest_v2_partition() {
        let schema = Arc::new(
            Schema::builder()
                .with_fields(vec![
                    Arc::new(NestedField::optional(
                        1,
                        "id",
                        Type::Primitive(PrimitiveType::Long),
                    )),
                    Arc::new(NestedField::optional(
                        2,
                        "v_int",
                        Type::Primitive(PrimitiveType::Int),
                    )),
                    Arc::new(NestedField::optional(
                        3,
                        "v_long",
                        Type::Primitive(PrimitiveType::Long),
                    )),
                    Arc::new(NestedField::optional(
                        4,
                        "v_float",
                        Type::Primitive(PrimitiveType::Float),
                    )),
                    Arc::new(NestedField::optional(
                        5,
                        "v_double",
                        Type::Primitive(PrimitiveType::Double),
                    )),
                    Arc::new(NestedField::optional(
                        6,
                        "v_varchar",
                        Type::Primitive(PrimitiveType::String),
                    )),
                    Arc::new(NestedField::optional(
                        7,
                        "v_bool",
                        Type::Primitive(PrimitiveType::Boolean),
                    )),
                    Arc::new(NestedField::optional(
                        8,
                        "v_date",
                        Type::Primitive(PrimitiveType::Date),
                    )),
                    Arc::new(NestedField::optional(
                        9,
                        "v_timestamp",
                        Type::Primitive(PrimitiveType::Timestamptz),
                    )),
                    Arc::new(NestedField::optional(
                        10,
                        "v_decimal",
                        Type::Primitive(PrimitiveType::Decimal {
                            precision: 36,
                            scale: 10,
                        }),
                    )),
                    Arc::new(NestedField::optional(
                        11,
                        "v_ts_ntz",
                        Type::Primitive(PrimitiveType::Timestamp),
                    )),
                    Arc::new(NestedField::optional(
                        12,
                        "v_ts_ns_ntz",
                        Type::Primitive(PrimitiveType::TimestampNs),
                    )),
                ])
                .build()
                .unwrap(),
        );
        let metadata = ManifestMetadata {
            schema_id: 0,
            schema: schema.clone(),
            partition_spec: PartitionSpec::builder(schema)
                .with_spec_id(0)
                .add_partition_field("v_int", "v_int", Transform::Identity)
                .unwrap()
                .add_partition_field("v_long", "v_long", Transform::Identity)
                .unwrap()
                .build()
                .unwrap(),
            content: ManifestContentType::Data,
            format_version: FormatVersion::V2,
        };
        let mut entries = vec![ManifestEntry {
                status: ManifestStatus::Added,
                snapshot_id: None,
                sequence_number: None,
                file_sequence_number: None,
                data_file: DataFile {
                    content: DataContentType::Data,
                    file_format: DataFileFormat::Parquet,
                    file_path: "s3a://icebergdata/demo/s1/t1/data/00000-0-378b56f5-5c52-4102-a2c2-f05f8a7cbe4a-00000.parquet".to_string(),
                    partition: Struct::from_iter(
                        vec![
                            Some(Literal::int(1)),
                            Some(Literal::long(1000)),
                        ]
                            .into_iter()
                    ),
                    record_count: 1,
                    file_size_in_bytes: 5442,
                    column_sizes: HashMap::from([
                        (0, 73),
                        (6, 34),
                        (2, 73),
                        (7, 61),
                        (3, 61),
                        (5, 62),
                        (9, 79),
                        (10, 73),
                        (1, 61),
                        (4, 73),
                        (8, 73)
                    ]),
                    value_counts: HashMap::from([
                        (4, 1),
                        (5, 1),
                        (2, 1),
                        (0, 1),
                        (3, 1),
                        (6, 1),
                        (8, 1),
                        (1, 1),
                        (10, 1),
                        (7, 1),
                        (9, 1)
                    ]),
                    null_value_counts: HashMap::from([
                        (1, 0),
                        (6, 0),
                        (2, 0),
                        (8, 0),
                        (0, 0),
                        (3, 0),
                        (5, 0),
                        (9, 0),
                        (7, 0),
                        (4, 0),
                        (10, 0)
                    ]),
                    nan_value_counts: HashMap::new(),
                    lower_bounds: HashMap::new(),
                    upper_bounds: HashMap::new(),
                    key_metadata: None,
                    split_offsets: Some(vec![4]),
                    equality_ids: Some(Vec::new()),
                    sort_order_id: None,
                    partition_spec_id: 0,
                    first_row_id: None,
                    referenced_data_file: None,
                    content_offset: None,
                    content_size_in_bytes: None,
                },
            }];

        // write manifest to file and check the return manifest file.
        let tmp_dir = TempDir::new().unwrap();
        let path = tmp_dir.path().join("test_manifest.avro");
        let io = FileIOBuilder::new_fs_io().build().unwrap();
        let output_file = io.new_output(path.to_str().unwrap()).unwrap();
        let mut writer = ManifestWriterBuilder::new(
            output_file,
            Some(2),
            None,
            metadata.schema.clone(),
            metadata.partition_spec.clone(),
        )
        .build_v2_data();
        for entry in &entries {
            writer.add_entry(entry.clone()).unwrap();
        }
        let manifest_file = writer.write_manifest_file().await.unwrap();
        assert_eq!(manifest_file.sequence_number, UNASSIGNED_SEQUENCE_NUMBER);
        assert_eq!(
            manifest_file.min_sequence_number,
            UNASSIGNED_SEQUENCE_NUMBER
        );

        // read back the manifest file and check the content
        let actual_manifest =
            Manifest::parse_avro(fs::read(path).expect("read_file must succeed").as_slice())
                .unwrap();
        // The snapshot id is assigned when the entry is added to the manifest.
        entries[0].snapshot_id = Some(2);
        assert_eq!(actual_manifest, Manifest::new(metadata, entries));
    }

    #[tokio::test]
    async fn test_parse_manifest_v1_unpartition() {
        let schema = Arc::new(
            Schema::builder()
                .with_schema_id(1)
                .with_fields(vec![
                    Arc::new(NestedField::optional(
                        1,
                        "id",
                        Type::Primitive(PrimitiveType::Int),
                    )),
                    Arc::new(NestedField::optional(
                        2,
                        "data",
                        Type::Primitive(PrimitiveType::String),
                    )),
                    Arc::new(NestedField::optional(
                        3,
                        "comment",
                        Type::Primitive(PrimitiveType::String),
                    )),
                ])
                .build()
                .unwrap(),
        );
        let metadata = ManifestMetadata {
            schema_id: 1,
            schema: schema.clone(),
            partition_spec: PartitionSpec::builder(schema)
                .with_spec_id(0)
                .build()
                .unwrap(),
            content: ManifestContentType::Data,
            format_version: FormatVersion::V1,
        };
        let mut entries = vec![ManifestEntry {
                status: ManifestStatus::Added,
                snapshot_id: Some(0),
                sequence_number: Some(0),
                file_sequence_number: Some(0),
                data_file: DataFile {
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
                }
            }];

        // write manifest to file
        let tmp_dir = TempDir::new().unwrap();
        let path = tmp_dir.path().join("test_manifest.avro");
        let io = FileIOBuilder::new_fs_io().build().unwrap();
        let output_file = io.new_output(path.to_str().unwrap()).unwrap();
        let mut writer = ManifestWriterBuilder::new(
            output_file,
            Some(3),
            None,
            metadata.schema.clone(),
            metadata.partition_spec.clone(),
        )
        .build_v1();
        for entry in &entries {
            writer.add_entry(entry.clone()).unwrap();
        }
        writer.write_manifest_file().await.unwrap();

        // read back the manifest file and check the content
        let actual_manifest =
            Manifest::parse_avro(fs::read(path).expect("read_file must succeed").as_slice())
                .unwrap();
        // The snapshot id is assigned when the entry is added to the manifest.
        entries[0].snapshot_id = Some(3);
        assert_eq!(actual_manifest, Manifest::new(metadata, entries));
    }

    #[tokio::test]
    async fn test_parse_manifest_v1_partition() {
        let schema = Arc::new(
            Schema::builder()
                .with_fields(vec![
                    Arc::new(NestedField::optional(
                        1,
                        "id",
                        Type::Primitive(PrimitiveType::Long),
                    )),
                    Arc::new(NestedField::optional(
                        2,
                        "data",
                        Type::Primitive(PrimitiveType::String),
                    )),
                    Arc::new(NestedField::optional(
                        3,
                        "category",
                        Type::Primitive(PrimitiveType::String),
                    )),
                ])
                .build()
                .unwrap(),
        );
        let metadata = ManifestMetadata {
            schema_id: 0,
            schema: schema.clone(),
            partition_spec: PartitionSpec::builder(schema)
                .add_partition_field("category", "category", Transform::Identity)
                .unwrap()
                .build()
                .unwrap(),
            content: ManifestContentType::Data,
            format_version: FormatVersion::V1,
        };
        let mut entries = vec![
                ManifestEntry {
                    status: ManifestStatus::Added,
                    snapshot_id: Some(0),
                    sequence_number: Some(0),
                    file_sequence_number: Some(0),
                    data_file: DataFile {
                        content: DataContentType::Data,
                        file_path: "s3://testbucket/prod/db/sample/data/category=x/00010-1-d5c93668-1e52-41ac-92a6-bba590cbf249-00001.parquet".to_string(),
                        file_format: DataFileFormat::Parquet,
                        partition: Struct::from_iter(
                            vec![
                                Some(
                                    Literal::string("x"),
                                ),
                            ]
                                .into_iter()
                        ),
                        record_count: 1,
                        file_size_in_bytes: 874,
                        column_sizes: HashMap::from([(1, 46), (2, 48), (3, 48)]),
                        value_counts: HashMap::from([(1, 1), (2, 1), (3, 1)]),
                        null_value_counts: HashMap::from([(1, 0), (2, 0), (3, 0)]),
                        nan_value_counts: HashMap::new(),
                        lower_bounds: HashMap::from([
                        (1, Datum::long(1)),
                        (2, Datum::string("a")),
                        (3, Datum::string("x"))
                        ]),
                        upper_bounds: HashMap::from([
                        (1, Datum::long(1)),
                        (2, Datum::string("a")),
                        (3, Datum::string("x"))
                        ]),
                        key_metadata: None,
                        split_offsets: Some(vec![4]),
                        equality_ids: None,
                        sort_order_id: Some(0),
                        partition_spec_id: 0,
                        first_row_id: None,
                        referenced_data_file: None,
                        content_offset: None,
                        content_size_in_bytes: None,
                    },
                }
            ];

        // write manifest to file
        let tmp_dir = TempDir::new().unwrap();
        let path = tmp_dir.path().join("test_manifest.avro");
        let io = FileIOBuilder::new_fs_io().build().unwrap();
        let output_file = io.new_output(path.to_str().unwrap()).unwrap();
        let mut writer = ManifestWriterBuilder::new(
            output_file,
            Some(2),
            None,
            metadata.schema.clone(),
            metadata.partition_spec.clone(),
        )
        .build_v1();
        for entry in &entries {
            writer.add_entry(entry.clone()).unwrap();
        }
        let manifest_file = writer.write_manifest_file().await.unwrap();
        let partitions = manifest_file.partitions.unwrap();
        assert_eq!(partitions.len(), 1);
        assert_eq!(
            partitions[0].clone().lower_bound.unwrap(),
            Datum::string("x").to_bytes().unwrap()
        );
        assert_eq!(
            partitions[0].clone().upper_bound.unwrap(),
            Datum::string("x").to_bytes().unwrap()
        );

        // read back the manifest file and check the content
        let actual_manifest =
            Manifest::parse_avro(fs::read(path).expect("read_file must succeed").as_slice())
                .unwrap();
        // The snapshot id is assigned when the entry is added to the manifest.
        entries[0].snapshot_id = Some(2);
        assert_eq!(actual_manifest, Manifest::new(metadata, entries));
    }

    #[tokio::test]
    async fn test_parse_manifest_with_schema_evolution() {
        let schema = Arc::new(
            Schema::builder()
                .with_fields(vec![
                    Arc::new(NestedField::optional(
                        1,
                        "id",
                        Type::Primitive(PrimitiveType::Long),
                    )),
                    Arc::new(NestedField::optional(
                        2,
                        "v_int",
                        Type::Primitive(PrimitiveType::Int),
                    )),
                ])
                .build()
                .unwrap(),
        );
        let metadata = ManifestMetadata {
            schema_id: 0,
            schema: schema.clone(),
            partition_spec: PartitionSpec::builder(schema)
                .with_spec_id(0)
                .build()
                .unwrap(),
            content: ManifestContentType::Data,
            format_version: FormatVersion::V2,
        };
        let entries = vec![ManifestEntry {
                status: ManifestStatus::Added,
                snapshot_id: None,
                sequence_number: None,
                file_sequence_number: None,
                data_file: DataFile {
                    content: DataContentType::Data,
                    file_format: DataFileFormat::Parquet,
                    file_path: "s3a://icebergdata/demo/s1/t1/data/00000-0-378b56f5-5c52-4102-a2c2-f05f8a7cbe4a-00000.parquet".to_string(),
                    partition: Struct::empty(),
                    record_count: 1,
                    file_size_in_bytes: 5442,
                    column_sizes: HashMap::from([
                        (1, 61),
                        (2, 73),
                        (3, 61),
                    ]),
                    value_counts: HashMap::default(),
                    null_value_counts: HashMap::default(),
                    nan_value_counts: HashMap::new(),
                    lower_bounds: HashMap::from([
                        (1, Datum::long(1)),
                        (2, Datum::int(2)),
                        (3, Datum::string("x"))
                    ]),
                    upper_bounds: HashMap::from([
                        (1, Datum::long(1)),
                        (2, Datum::int(2)),
                        (3, Datum::string("x"))
                    ]),
                    key_metadata: None,
                    split_offsets: Some(vec![4]),
                    equality_ids: None,
                    sort_order_id: None,
                    partition_spec_id: 0,
                    first_row_id: None,
                    referenced_data_file: None,
                    content_offset: None,
                    content_size_in_bytes: None,
                },
            }];

        // write manifest to file
        let tmp_dir = TempDir::new().unwrap();
        let path = tmp_dir.path().join("test_manifest.avro");
        let io = FileIOBuilder::new_fs_io().build().unwrap();
        let output_file = io.new_output(path.to_str().unwrap()).unwrap();
        let mut writer = ManifestWriterBuilder::new(
            output_file,
            Some(2),
            None,
            metadata.schema.clone(),
            metadata.partition_spec.clone(),
        )
        .build_v2_data();
        for entry in &entries {
            writer.add_entry(entry.clone()).unwrap();
        }
        writer.write_manifest_file().await.unwrap();

        // read back the manifest file and check the content
        let actual_manifest =
            Manifest::parse_avro(fs::read(path).expect("read_file must succeed").as_slice())
                .unwrap();

        // Compared with original manifest, the lower_bounds and upper_bounds no longer has data for field 3, and
        // other parts should be same.
        // The snapshot id is assigned when the entry is added to the manifest.
        let schema = Arc::new(
            Schema::builder()
                .with_fields(vec![
                    Arc::new(NestedField::optional(
                        1,
                        "id",
                        Type::Primitive(PrimitiveType::Long),
                    )),
                    Arc::new(NestedField::optional(
                        2,
                        "v_int",
                        Type::Primitive(PrimitiveType::Int),
                    )),
                ])
                .build()
                .unwrap(),
        );
        let expected_manifest = Manifest {
            metadata: ManifestMetadata {
                schema_id: 0,
                schema: schema.clone(),
                partition_spec: PartitionSpec::builder(schema).with_spec_id(0).build().unwrap(),
                content: ManifestContentType::Data,
                format_version: FormatVersion::V2,
            },
            entries: vec![Arc::new(ManifestEntry {
                status: ManifestStatus::Added,
                snapshot_id: Some(2),
                sequence_number: None,
                file_sequence_number: None,
                data_file: DataFile {
                    content: DataContentType::Data,
                    file_format: DataFileFormat::Parquet,
                    file_path: "s3a://icebergdata/demo/s1/t1/data/00000-0-378b56f5-5c52-4102-a2c2-f05f8a7cbe4a-00000.parquet".to_string(),
                    partition: Struct::empty(),
                    record_count: 1,
                    file_size_in_bytes: 5442,
                    column_sizes: HashMap::from([
                        (1, 61),
                        (2, 73),
                        (3, 61),
                    ]),
                    value_counts: HashMap::default(),
                    null_value_counts: HashMap::default(),
                    nan_value_counts: HashMap::new(),
                    lower_bounds: HashMap::from([
                        (1, Datum::long(1)),
                        (2, Datum::int(2)),
                    ]),
                    upper_bounds: HashMap::from([
                        (1, Datum::long(1)),
                        (2, Datum::int(2)),
                    ]),
                    key_metadata: None,
                    split_offsets: Some(vec![4]),
                    equality_ids: None,
                    sort_order_id: None,
                    partition_spec_id: 0,
                    first_row_id: None,
                    referenced_data_file: None,
                    content_offset: None,
                    content_size_in_bytes: None,
                },
            })],
        };

        assert_eq!(actual_manifest, expected_manifest);
    }

    #[tokio::test]
    async fn test_manifest_summary() {
        let schema = Arc::new(
            Schema::builder()
                .with_fields(vec![
                    Arc::new(NestedField::optional(
                        1,
                        "time",
                        Type::Primitive(PrimitiveType::Date),
                    )),
                    Arc::new(NestedField::optional(
                        2,
                        "v_float",
                        Type::Primitive(PrimitiveType::Float),
                    )),
                    Arc::new(NestedField::optional(
                        3,
                        "v_double",
                        Type::Primitive(PrimitiveType::Double),
                    )),
                ])
                .build()
                .unwrap(),
        );
        let partition_spec = PartitionSpec::builder(schema.clone())
            .with_spec_id(0)
            .add_partition_field("time", "year_of_time", Transform::Year)
            .unwrap()
            .add_partition_field("v_float", "f", Transform::Identity)
            .unwrap()
            .add_partition_field("v_double", "d", Transform::Identity)
            .unwrap()
            .build()
            .unwrap();
        let metadata = ManifestMetadata {
            schema_id: 0,
            schema,
            partition_spec,
            content: ManifestContentType::Data,
            format_version: FormatVersion::V2,
        };
        let entries = vec![
            ManifestEntry {
                status: ManifestStatus::Added,
                snapshot_id: None,
                sequence_number: None,
                file_sequence_number: None,
                data_file: DataFile {
                    content: DataContentType::Data,
                    file_path: "s3a://icebergdata/demo/s1/t1/data/00000-0-ba56fbfa-f2ff-40c9-bb27-565ad6dc2be8-00000.parquet".to_string(),
                    file_format: DataFileFormat::Parquet,
                    partition: Struct::from_iter(
                        vec![
                            Some(Literal::int(2021)),
                            Some(Literal::float(1.0)),
                            Some(Literal::double(2.0)),
                        ]
                    ),
                    record_count: 1,
                    file_size_in_bytes: 5442,
                    column_sizes: HashMap::from([(0,73),(6,34),(2,73),(7,61),(3,61),(5,62),(9,79),(10,73),(1,61),(4,73),(8,73)]),
                    value_counts: HashMap::from([(4,1),(5,1),(2,1),(0,1),(3,1),(6,1),(8,1),(1,1),(10,1),(7,1),(9,1)]),
                    null_value_counts: HashMap::from([(1,0),(6,0),(2,0),(8,0),(0,0),(3,0),(5,0),(9,0),(7,0),(4,0),(10,0)]),
                    nan_value_counts: HashMap::new(),
                    lower_bounds: HashMap::new(),
                    upper_bounds: HashMap::new(),
                    key_metadata: None,
                    split_offsets: Some(vec![4]),
                    equality_ids: None,
                    sort_order_id: None,
                    partition_spec_id: 0,
                    first_row_id: None,
                    referenced_data_file: None,
                    content_offset: None,
                    content_size_in_bytes: None,
                }
            },
                ManifestEntry {
                    status: ManifestStatus::Added,
                    snapshot_id: None,
                    sequence_number: None,
                    file_sequence_number: None,
                    data_file: DataFile {
                        content: DataContentType::Data,
                        file_path: "s3a://icebergdata/demo/s1/t1/data/00000-0-ba56fbfa-f2ff-40c9-bb27-565ad6dc2be8-00000.parquet".to_string(),
                        file_format: DataFileFormat::Parquet,
                        partition: Struct::from_iter(
                            vec![
                                Some(Literal::int(1111)),
                                Some(Literal::float(15.5)),
                                Some(Literal::double(25.5)),
                            ]
                        ),
                        record_count: 1,
                        file_size_in_bytes: 5442,
                        column_sizes: HashMap::from([(0,73),(6,34),(2,73),(7,61),(3,61),(5,62),(9,79),(10,73),(1,61),(4,73),(8,73)]),
                        value_counts: HashMap::from([(4,1),(5,1),(2,1),(0,1),(3,1),(6,1),(8,1),(1,1),(10,1),(7,1),(9,1)]),
                        null_value_counts: HashMap::from([(1,0),(6,0),(2,0),(8,0),(0,0),(3,0),(5,0),(9,0),(7,0),(4,0),(10,0)]),
                        nan_value_counts: HashMap::new(),
                        lower_bounds: HashMap::new(),
                        upper_bounds: HashMap::new(),
                        key_metadata: None,
                        split_offsets: Some(vec![4]),
                        equality_ids: None,
                        sort_order_id: None,
                        partition_spec_id: 0,
                        first_row_id: None,
                        referenced_data_file: None,
                        content_offset: None,
                        content_size_in_bytes: None,
                    }
                },
                ManifestEntry {
                    status: ManifestStatus::Added,
                    snapshot_id: None,
                    sequence_number: None,
                    file_sequence_number: None,
                    data_file: DataFile {
                        content: DataContentType::Data,
                        file_path: "s3a://icebergdata/demo/s1/t1/data/00000-0-ba56fbfa-f2ff-40c9-bb27-565ad6dc2be8-00000.parquet".to_string(),
                        file_format: DataFileFormat::Parquet,
                        partition: Struct::from_iter(
                            vec![
                                Some(Literal::int(1211)),
                                Some(Literal::float(f32::NAN)),
                                Some(Literal::double(1.0)),
                            ]
                        ),
                        record_count: 1,
                        file_size_in_bytes: 5442,
                        column_sizes: HashMap::from([(0,73),(6,34),(2,73),(7,61),(3,61),(5,62),(9,79),(10,73),(1,61),(4,73),(8,73)]),
                        value_counts: HashMap::from([(4,1),(5,1),(2,1),(0,1),(3,1),(6,1),(8,1),(1,1),(10,1),(7,1),(9,1)]),
                        null_value_counts: HashMap::from([(1,0),(6,0),(2,0),(8,0),(0,0),(3,0),(5,0),(9,0),(7,0),(4,0),(10,0)]),
                        nan_value_counts: HashMap::new(),
                        lower_bounds: HashMap::new(),
                        upper_bounds: HashMap::new(),
                        key_metadata: None,
                        split_offsets: Some(vec![4]),
                        equality_ids: None,
                        sort_order_id: None,
                        partition_spec_id: 0,
                        first_row_id: None,
                        referenced_data_file: None,
                        content_offset: None,
                        content_size_in_bytes: None,
                    }
                },
                ManifestEntry {
                    status: ManifestStatus::Added,
                    snapshot_id: None,
                    sequence_number: None,
                    file_sequence_number: None,
                    data_file: DataFile {
                        content: DataContentType::Data,
                        file_path: "s3a://icebergdata/demo/s1/t1/data/00000-0-ba56fbfa-f2ff-40c9-bb27-565ad6dc2be8-00000.parquet".to_string(),
                        file_format: DataFileFormat::Parquet,
                        partition: Struct::from_iter(
                            vec![
                                Some(Literal::int(1111)),
                                None,
                                Some(Literal::double(11.0)),
                            ]
                        ),
                        record_count: 1,
                        file_size_in_bytes: 5442,
                        column_sizes: HashMap::from([(0,73),(6,34),(2,73),(7,61),(3,61),(5,62),(9,79),(10,73),(1,61),(4,73),(8,73)]),
                        value_counts: HashMap::from([(4,1),(5,1),(2,1),(0,1),(3,1),(6,1),(8,1),(1,1),(10,1),(7,1),(9,1)]),
                        null_value_counts: HashMap::from([(1,0),(6,0),(2,0),(8,0),(0,0),(3,0),(5,0),(9,0),(7,0),(4,0),(10,0)]),
                        nan_value_counts: HashMap::new(),
                        lower_bounds: HashMap::new(),
                        upper_bounds: HashMap::new(),
                        key_metadata: None,
                        split_offsets: Some(vec![4]),
                        equality_ids: None,
                        sort_order_id: None,
                        partition_spec_id: 0,
                        first_row_id: None,
                        referenced_data_file: None,
                        content_offset: None,
                        content_size_in_bytes: None,
                    }
                },
        ];

        // write manifest to file
        let tmp_dir = TempDir::new().unwrap();
        let path = tmp_dir.path().join("test_manifest.avro");
        let io = FileIOBuilder::new_fs_io().build().unwrap();
        let output_file = io.new_output(path.to_str().unwrap()).unwrap();
        let mut writer = ManifestWriterBuilder::new(
            output_file,
            Some(1),
            None,
            metadata.schema.clone(),
            metadata.partition_spec.clone(),
        )
        .build_v2_data();
        for entry in &entries {
            writer.add_entry(entry.clone()).unwrap();
        }
        let res = writer.write_manifest_file().await.unwrap();

        let partitions = res.partitions.unwrap();

        assert_eq!(partitions.len(), 3);
        assert_eq!(
            partitions[0].clone().lower_bound.unwrap(),
            Datum::int(1111).to_bytes().unwrap()
        );
        assert_eq!(
            partitions[0].clone().upper_bound.unwrap(),
            Datum::int(2021).to_bytes().unwrap()
        );
        assert!(!partitions[0].clone().contains_null);
        assert_eq!(partitions[0].clone().contains_nan, Some(false));

        assert_eq!(
            partitions[1].clone().lower_bound.unwrap(),
            Datum::float(1.0).to_bytes().unwrap()
        );
        assert_eq!(
            partitions[1].clone().upper_bound.unwrap(),
            Datum::float(15.5).to_bytes().unwrap()
        );
        assert!(partitions[1].clone().contains_null);
        assert_eq!(partitions[1].clone().contains_nan, Some(true));

        assert_eq!(
            partitions[2].clone().lower_bound.unwrap(),
            Datum::double(1.0).to_bytes().unwrap()
        );
        assert_eq!(
            partitions[2].clone().upper_bound.unwrap(),
            Datum::double(25.5).to_bytes().unwrap()
        );
        assert!(!partitions[2].clone().contains_null);
        assert_eq!(partitions[2].clone().contains_nan, Some(false));
    }

    #[test]
    fn test_data_file_serialization() {
        // Create a simple schema
        let schema = Schema::builder()
            .with_schema_id(1)
            .with_identifier_field_ids(vec![1])
            .with_fields(vec![
                NestedField::required(1, "id", Type::Primitive(PrimitiveType::Long)).into(),
                NestedField::required(2, "name", Type::Primitive(PrimitiveType::String)).into(),
            ])
            .build()
            .unwrap();

        // Create a partition spec
        let partition_spec = PartitionSpec::builder(schema.clone())
            .with_spec_id(1)
            .add_partition_field("id", "id_partition", Transform::Identity)
            .unwrap()
            .build()
            .unwrap();

        // Get partition type from the partition spec
        let partition_type = partition_spec.partition_type(&schema).unwrap();

        // Create a vector of DataFile objects
        let data_files = vec![
            DataFileBuilder::default()
                .content(DataContentType::Data)
                .file_format(DataFileFormat::Parquet)
                .file_path("path/to/file1.parquet".to_string())
                .file_size_in_bytes(1024)
                .record_count(100)
                .partition_spec_id(1)
                .partition(Struct::empty())
                .column_sizes(HashMap::from([(1, 512), (2, 1024)]))
                .value_counts(HashMap::from([(1, 100), (2, 500)]))
                .null_value_counts(HashMap::from([(1, 0), (2, 1)]))
                .build()
                .unwrap(),
            DataFileBuilder::default()
                .content(DataContentType::Data)
                .file_format(DataFileFormat::Parquet)
                .file_path("path/to/file2.parquet".to_string())
                .file_size_in_bytes(2048)
                .record_count(200)
                .partition_spec_id(1)
                .partition(Struct::empty())
                .column_sizes(HashMap::from([(1, 1024), (2, 2048)]))
                .value_counts(HashMap::from([(1, 200), (2, 600)]))
                .null_value_counts(HashMap::from([(1, 10), (2, 999)]))
                .build()
                .unwrap(),
        ];

        // Serialize the DataFile objects
        let serialized_files = data_files
            .clone()
            .into_iter()
            .map(|f| serialize_data_file_to_json(f, &partition_type, FormatVersion::V2).unwrap())
            .collect::<Vec<String>>();

        // Verify we have the expected serialized files
        assert_eq!(serialized_files.len(), 2);
        let pretty_json1: Value = serde_json::from_str(serialized_files.first().unwrap()).unwrap();
        let pretty_json2: Value = serde_json::from_str(serialized_files.get(1).unwrap()).unwrap();
        let expected_serialized_file1 = serde_json::json!({
            "content": 0,
            "file_path": "path/to/file1.parquet",
            "file_format": "PARQUET",
            "partition": {},
            "record_count": 100,
            "file_size_in_bytes": 1024,
            "column_sizes": [
                { "key": 1, "value": 512 },
                { "key": 2, "value": 1024 }
            ],
            "value_counts": [
                { "key": 1, "value": 100 },
                { "key": 2, "value": 500 }
            ],
            "null_value_counts": [
                { "key": 1, "value": 0 },
                { "key": 2, "value": 1 }
            ],
            "nan_value_counts": [],
            "lower_bounds": [],
            "upper_bounds": [],
            "key_metadata": null,
            "split_offsets": null,
            "equality_ids": null,
            "sort_order_id": null,
            "first_row_id": null,
            "referenced_data_file": null,
            "content_offset": null,
            "content_size_in_bytes": null
        });
        let expected_serialized_file2 = serde_json::json!({
            "content": 0,
            "file_path": "path/to/file2.parquet",
            "file_format": "PARQUET",
            "partition": {},
            "record_count": 200,
            "file_size_in_bytes": 2048,
            "column_sizes": [
                { "key": 1, "value": 1024 },
                { "key": 2, "value": 2048 }
            ],
            "value_counts": [
                { "key": 1, "value": 200 },
                { "key": 2, "value": 600 }
            ],
            "null_value_counts": [
                { "key": 1, "value": 10 },
                { "key": 2, "value": 999 }
            ],
            "nan_value_counts": [],
            "lower_bounds": [],
            "upper_bounds": [],
            "key_metadata": null,
            "split_offsets": null,
            "equality_ids": null,
            "sort_order_id": null,
            "first_row_id": null,
            "referenced_data_file": null,
            "content_offset": null,
            "content_size_in_bytes": null
        });
        assert_eq!(pretty_json1, expected_serialized_file1);
        assert_eq!(pretty_json2, expected_serialized_file2);

        // Now deserialize the JSON strings back into DataFile objects
        let deserialized_files: Vec<DataFile> = serialized_files
            .into_iter()
            .map(|json| {
                deserialize_data_file_from_json(
                    &json,
                    partition_spec.spec_id(),
                    &partition_type,
                    &schema,
                )
                .unwrap()
            })
            .collect();

        // Verify we have the expected number of deserialized files
        assert_eq!(deserialized_files.len(), 2);
        let deserialized_data_file1 = deserialized_files.first().unwrap();
        let deserialized_data_file2 = deserialized_files.get(1).unwrap();
        let original_data_file1 = data_files.first().unwrap();
        let original_data_file2 = data_files.get(1).unwrap();

        assert_eq!(deserialized_data_file1, original_data_file1);
        assert_eq!(deserialized_data_file2, original_data_file2);
    }
}
