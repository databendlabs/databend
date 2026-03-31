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

//! ManifestList for Iceberg.

use std::collections::HashMap;
use std::str::FromStr;

use apache_avro::types::Value;
use apache_avro::{Reader, Writer, from_value};
use bytes::Bytes;
pub use serde_bytes::ByteBuf;
use serde_derive::{Deserialize, Serialize};

use self::_const_schema::{MANIFEST_LIST_AVRO_SCHEMA_V1, MANIFEST_LIST_AVRO_SCHEMA_V2};
use self::_serde::{ManifestFileV1, ManifestFileV2};
use super::{FormatVersion, Manifest};
use crate::error::Result;
use crate::io::{FileIO, OutputFile};
use crate::spec::manifest_list::_const_schema::MANIFEST_LIST_AVRO_SCHEMA_V3;
use crate::spec::manifest_list::_serde::ManifestFileV3;
use crate::{Error, ErrorKind};

/// Placeholder for sequence number. The field with this value must be replaced with the actual sequence number before it write.
pub const UNASSIGNED_SEQUENCE_NUMBER: i64 = -1;

/// Snapshots are embedded in table metadata, but the list of manifests for a
/// snapshot are stored in a separate manifest list file.
///
/// A new manifest list is written for each attempt to commit a snapshot
/// because the list of manifests always changes to produce a new snapshot.
/// When a manifest list is written, the (optimistic) sequence number of the
/// snapshot is written for all new manifest files tracked by the list.
///
/// A manifest list includes summary metadata that can be used to avoid
/// scanning all of the manifests in a snapshot when planning a table scan.
/// This includes the number of added, existing, and deleted files, and a
/// summary of values for each field of the partition spec used to write the
/// manifest.
#[derive(Debug, Clone, PartialEq)]
pub struct ManifestList {
    /// Entries in a manifest list.
    entries: Vec<ManifestFile>,
}

impl ManifestList {
    /// Parse manifest list from bytes.
    pub fn parse_with_version(bs: &[u8], version: FormatVersion) -> Result<ManifestList> {
        match version {
            FormatVersion::V1 => {
                let reader = Reader::with_schema(&MANIFEST_LIST_AVRO_SCHEMA_V1, bs)?;
                let values = Value::Array(reader.collect::<std::result::Result<Vec<Value>, _>>()?);
                from_value::<_serde::ManifestListV1>(&values)?.try_into()
            }
            FormatVersion::V2 => {
                let reader = Reader::new(bs)?;
                let values = Value::Array(reader.collect::<std::result::Result<Vec<Value>, _>>()?);
                from_value::<_serde::ManifestListV2>(&values)?.try_into()
            }
            FormatVersion::V3 => {
                let reader = Reader::new(bs)?;
                let values = Value::Array(reader.collect::<std::result::Result<Vec<Value>, _>>()?);
                from_value::<_serde::ManifestListV3>(&values)?.try_into()
            }
        }
    }

    /// Get the entries in the manifest list.
    pub fn entries(&self) -> &[ManifestFile] {
        &self.entries
    }

    /// Take ownership of the entries in the manifest list, consuming it
    pub fn consume_entries(self) -> impl IntoIterator<Item = ManifestFile> {
        Box::new(self.entries.into_iter())
    }
}

/// A manifest list writer.
pub struct ManifestListWriter {
    format_version: FormatVersion,
    output_file: OutputFile,
    avro_writer: Writer<'static, Vec<u8>>,
    sequence_number: i64,
    snapshot_id: i64,
    next_row_id: Option<u64>,
}

impl std::fmt::Debug for ManifestListWriter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ManifestListWriter")
            .field("format_version", &self.format_version)
            .field("output_file", &self.output_file)
            .field("avro_writer", &self.avro_writer.schema())
            .finish_non_exhaustive()
    }
}

impl ManifestListWriter {
    /// Get the next row ID that will be assigned to the next data manifest added.
    pub fn next_row_id(&self) -> Option<u64> {
        self.next_row_id
    }

    /// Construct a v1 [`ManifestListWriter`] that writes to a provided [`OutputFile`].
    pub fn v1(output_file: OutputFile, snapshot_id: i64, parent_snapshot_id: Option<i64>) -> Self {
        let mut metadata = HashMap::from_iter([
            ("snapshot-id".to_string(), snapshot_id.to_string()),
            ("format-version".to_string(), "1".to_string()),
        ]);
        if let Some(parent_snapshot_id) = parent_snapshot_id {
            metadata.insert(
                "parent-snapshot-id".to_string(),
                parent_snapshot_id.to_string(),
            );
        }
        Self::new(
            FormatVersion::V1,
            output_file,
            metadata,
            0,
            snapshot_id,
            None,
        )
    }

    /// Construct a v2 [`ManifestListWriter`] that writes to a provided [`OutputFile`].
    pub fn v2(
        output_file: OutputFile,
        snapshot_id: i64,
        parent_snapshot_id: Option<i64>,
        sequence_number: i64,
    ) -> Self {
        let mut metadata = HashMap::from_iter([
            ("snapshot-id".to_string(), snapshot_id.to_string()),
            ("sequence-number".to_string(), sequence_number.to_string()),
            ("format-version".to_string(), "2".to_string()),
        ]);
        metadata.insert(
            "parent-snapshot-id".to_string(),
            parent_snapshot_id
                .map(|v| v.to_string())
                .unwrap_or("null".to_string()),
        );
        Self::new(
            FormatVersion::V2,
            output_file,
            metadata,
            sequence_number,
            snapshot_id,
            None,
        )
    }

    /// Construct a v3 [`ManifestListWriter`] that writes to a provided [`OutputFile`].
    pub fn v3(
        output_file: OutputFile,
        snapshot_id: i64,
        parent_snapshot_id: Option<i64>,
        sequence_number: i64,
        first_row_id: Option<u64>, // Always None for delete manifests
    ) -> Self {
        let mut metadata = HashMap::from_iter([
            ("snapshot-id".to_string(), snapshot_id.to_string()),
            ("sequence-number".to_string(), sequence_number.to_string()),
            ("format-version".to_string(), "3".to_string()),
        ]);
        metadata.insert(
            "parent-snapshot-id".to_string(),
            parent_snapshot_id
                .map(|v| v.to_string())
                .unwrap_or("null".to_string()),
        );
        metadata.insert(
            "first-row-id".to_string(),
            first_row_id
                .map(|v| v.to_string())
                .unwrap_or("null".to_string()),
        );
        Self::new(
            FormatVersion::V3,
            output_file,
            metadata,
            sequence_number,
            snapshot_id,
            first_row_id,
        )
    }

    fn new(
        format_version: FormatVersion,
        output_file: OutputFile,
        metadata: HashMap<String, String>,
        sequence_number: i64,
        snapshot_id: i64,
        first_row_id: Option<u64>,
    ) -> Self {
        let avro_schema = match format_version {
            FormatVersion::V1 => &MANIFEST_LIST_AVRO_SCHEMA_V1,
            FormatVersion::V2 => &MANIFEST_LIST_AVRO_SCHEMA_V2,
            FormatVersion::V3 => &MANIFEST_LIST_AVRO_SCHEMA_V3,
        };
        let mut avro_writer = Writer::new(avro_schema, Vec::new());
        for (key, value) in metadata {
            avro_writer
                .add_user_metadata(key, value)
                .expect("Avro metadata should be added to the writer before the first record.");
        }
        Self {
            format_version,
            output_file,
            avro_writer,
            sequence_number,
            snapshot_id,
            next_row_id: first_row_id,
        }
    }

    /// Append manifests to be written.
    ///
    /// If V3 Manifests are added and the `first_row_id` of any data manifest is unassigned,
    /// it will be assigned based on the `next_row_id` of the writer, and the `next_row_id` of the writer will be updated accordingly.
    /// If `first_row_id` is already assigned, it will be validated against the `next_row_id` of the writer.
    pub fn add_manifests(&mut self, manifests: impl Iterator<Item = ManifestFile>) -> Result<()> {
        match self.format_version {
            FormatVersion::V1 => {
                for manifest in manifests {
                    let manifests: ManifestFileV1 = manifest.try_into()?;
                    self.avro_writer.append_ser(manifests)?;
                }
            }
            FormatVersion::V2 | FormatVersion::V3 => {
                for mut manifest in manifests {
                    self.assign_sequence_numbers(&mut manifest)?;

                    if self.format_version == FormatVersion::V2 {
                        let manifest_entry: ManifestFileV2 = manifest.try_into()?;
                        self.avro_writer.append_ser(manifest_entry)?;
                    } else if self.format_version == FormatVersion::V3 {
                        self.assign_first_row_id(&mut manifest)?;
                        let manifest_entry: ManifestFileV3 = manifest.try_into()?;
                        self.avro_writer.append_ser(manifest_entry)?;
                    }
                }
            }
        }
        Ok(())
    }

    /// Write the manifest list to the output file.
    pub async fn close(self) -> Result<()> {
        let data = self.avro_writer.into_inner()?;
        let mut writer = self.output_file.writer().await?;
        writer.write(Bytes::from(data)).await?;
        writer.close().await?;
        Ok(())
    }

    /// Assign sequence numbers to manifest if they are unassigned
    fn assign_sequence_numbers(&self, manifest: &mut ManifestFile) -> Result<()> {
        if manifest.sequence_number == UNASSIGNED_SEQUENCE_NUMBER {
            if manifest.added_snapshot_id != self.snapshot_id {
                return Err(Error::new(
                    ErrorKind::DataInvalid,
                    format!(
                        "Found unassigned sequence number for a manifest from snapshot {}.",
                        manifest.added_snapshot_id
                    ),
                ));
            }
            manifest.sequence_number = self.sequence_number;
        }

        if manifest.min_sequence_number == UNASSIGNED_SEQUENCE_NUMBER {
            if manifest.added_snapshot_id != self.snapshot_id {
                return Err(Error::new(
                    ErrorKind::DataInvalid,
                    format!(
                        "Found unassigned sequence number for a manifest from snapshot {}.",
                        manifest.added_snapshot_id
                    ),
                ));
            }
            manifest.min_sequence_number = self.sequence_number;
        }

        Ok(())
    }

    /// Returns number of newly assigned first-row-ids, if any.
    fn assign_first_row_id(&mut self, manifest: &mut ManifestFile) -> Result<()> {
        match manifest.content {
            ManifestContentType::Data => {
                match (self.next_row_id, manifest.first_row_id) {
                    (Some(_), Some(_)) => {
                        // Case: Manifest with already assigned first row ID.
                        // No need to increase next_row_id, as this manifest is already assigned.
                    }
                    (None, Some(manifest_first_row_id)) => {
                        // Case: Assigned first row ID for data manifest, but the writer does not have a next-row-id assigned.
                        return Err(Error::new(
                            ErrorKind::Unexpected,
                            format!(
                                "Found invalid first-row-id assignment for Manifest {}. Writer does not have a next-row-id assigned, but the manifest has first-row-id assigned to {}.",
                                manifest.manifest_path, manifest_first_row_id,
                            ),
                        ));
                    }
                    (Some(writer_next_row_id), None) => {
                        // Case: Unassigned first row ID for data manifest. This is either a new
                        // manifest, or a manifest from a pre-v3 snapshot. We need to assign one.
                        let (existing_rows_count, added_rows_count) =
                            require_row_counts_in_manifest(manifest)?;
                        manifest.first_row_id = Some(writer_next_row_id);

                        self.next_row_id = writer_next_row_id
                        .checked_add(existing_rows_count)
                        .and_then(|sum| sum.checked_add(added_rows_count))
                        .ok_or_else(|| {
                            Error::new(
                                ErrorKind::DataInvalid,
                                format!(
                                    "Row ID overflow when computing next row ID for Manifest {}. Next Row ID: {writer_next_row_id}, Existing Rows Count: {existing_rows_count}, Added Rows Count: {added_rows_count}",
                                    manifest.manifest_path
                                ),
                            )
                        }).map(Some)?;
                    }
                    (None, None) => {
                        // Case: Table without row lineage. No action needed.
                    }
                }
            }
            ManifestContentType::Deletes => {
                // Deletes never have a first-row-id assigned.
                manifest.first_row_id = None;
            }
        };

        Ok(())
    }
}

fn require_row_counts_in_manifest(manifest: &ManifestFile) -> Result<(u64, u64)> {
    let existing_rows_count = manifest.existing_rows_count.ok_or_else(|| {
        Error::new(
            ErrorKind::DataInvalid,
            format!(
                "Cannot include a Manifest without existing-rows-count to a table with row lineage enabled. Manifest path: {}",
                manifest.manifest_path,
            ),
        )
    })?;
    let added_rows_count = manifest.added_rows_count.ok_or_else(|| {
        Error::new(
            ErrorKind::DataInvalid,
            format!(
                "Cannot include a Manifest without added-rows-count to a table with row lineage enabled. Manifest path: {}",
                manifest.manifest_path,
            ),
        )
    })?;
    Ok((existing_rows_count, added_rows_count))
}

/// This is a helper module that defines the schema field of the manifest list entry.
mod _const_schema {
    use std::sync::Arc;

    use apache_avro::Schema as AvroSchema;
    use once_cell::sync::Lazy;

    use crate::avro::schema_to_avro_schema;
    use crate::spec::{
        ListType, NestedField, NestedFieldRef, PrimitiveType, Schema, StructType, Type,
    };

    static MANIFEST_PATH: Lazy<NestedFieldRef> = {
        Lazy::new(|| {
            Arc::new(NestedField::required(
                500,
                "manifest_path",
                Type::Primitive(PrimitiveType::String),
            ))
        })
    };
    static MANIFEST_LENGTH: Lazy<NestedFieldRef> = {
        Lazy::new(|| {
            Arc::new(NestedField::required(
                501,
                "manifest_length",
                Type::Primitive(PrimitiveType::Long),
            ))
        })
    };
    static PARTITION_SPEC_ID: Lazy<NestedFieldRef> = {
        Lazy::new(|| {
            Arc::new(NestedField::required(
                502,
                "partition_spec_id",
                Type::Primitive(PrimitiveType::Int),
            ))
        })
    };
    static CONTENT: Lazy<NestedFieldRef> = {
        Lazy::new(|| {
            Arc::new(NestedField::required(
                517,
                "content",
                Type::Primitive(PrimitiveType::Int),
            ))
        })
    };
    static SEQUENCE_NUMBER: Lazy<NestedFieldRef> = {
        Lazy::new(|| {
            Arc::new(NestedField::required(
                515,
                "sequence_number",
                Type::Primitive(PrimitiveType::Long),
            ))
        })
    };
    static MIN_SEQUENCE_NUMBER: Lazy<NestedFieldRef> = {
        Lazy::new(|| {
            Arc::new(NestedField::required(
                516,
                "min_sequence_number",
                Type::Primitive(PrimitiveType::Long),
            ))
        })
    };
    static ADDED_SNAPSHOT_ID: Lazy<NestedFieldRef> = {
        Lazy::new(|| {
            Arc::new(NestedField::required(
                503,
                "added_snapshot_id",
                Type::Primitive(PrimitiveType::Long),
            ))
        })
    };
    static ADDED_FILES_COUNT_V2: Lazy<NestedFieldRef> = {
        Lazy::new(|| {
            Arc::new(NestedField::required(
                504,
                "added_files_count",
                Type::Primitive(PrimitiveType::Int),
            ))
        })
    };
    static ADDED_FILES_COUNT_V1: Lazy<NestedFieldRef> = {
        Lazy::new(|| {
            Arc::new(NestedField::optional(
                504,
                "added_data_files_count",
                Type::Primitive(PrimitiveType::Int),
            ))
        })
    };
    static EXISTING_FILES_COUNT_V2: Lazy<NestedFieldRef> = {
        Lazy::new(|| {
            Arc::new(NestedField::required(
                505,
                "existing_files_count",
                Type::Primitive(PrimitiveType::Int),
            ))
        })
    };
    static EXISTING_FILES_COUNT_V1: Lazy<NestedFieldRef> = {
        Lazy::new(|| {
            Arc::new(NestedField::optional(
                505,
                "existing_data_files_count",
                Type::Primitive(PrimitiveType::Int),
            ))
        })
    };
    static DELETED_FILES_COUNT_V2: Lazy<NestedFieldRef> = {
        Lazy::new(|| {
            Arc::new(NestedField::required(
                506,
                "deleted_files_count",
                Type::Primitive(PrimitiveType::Int),
            ))
        })
    };
    static DELETED_FILES_COUNT_V1: Lazy<NestedFieldRef> = {
        Lazy::new(|| {
            Arc::new(NestedField::optional(
                506,
                "deleted_data_files_count",
                Type::Primitive(PrimitiveType::Int),
            ))
        })
    };
    static ADDED_ROWS_COUNT_V2: Lazy<NestedFieldRef> = {
        Lazy::new(|| {
            Arc::new(NestedField::required(
                512,
                "added_rows_count",
                Type::Primitive(PrimitiveType::Long),
            ))
        })
    };
    static ADDED_ROWS_COUNT_V1: Lazy<NestedFieldRef> = {
        Lazy::new(|| {
            Arc::new(NestedField::optional(
                512,
                "added_rows_count",
                Type::Primitive(PrimitiveType::Long),
            ))
        })
    };
    static EXISTING_ROWS_COUNT_V2: Lazy<NestedFieldRef> = {
        Lazy::new(|| {
            Arc::new(NestedField::required(
                513,
                "existing_rows_count",
                Type::Primitive(PrimitiveType::Long),
            ))
        })
    };
    static EXISTING_ROWS_COUNT_V1: Lazy<NestedFieldRef> = {
        Lazy::new(|| {
            Arc::new(NestedField::optional(
                513,
                "existing_rows_count",
                Type::Primitive(PrimitiveType::Long),
            ))
        })
    };
    static DELETED_ROWS_COUNT_V2: Lazy<NestedFieldRef> = {
        Lazy::new(|| {
            Arc::new(NestedField::required(
                514,
                "deleted_rows_count",
                Type::Primitive(PrimitiveType::Long),
            ))
        })
    };
    static DELETED_ROWS_COUNT_V1: Lazy<NestedFieldRef> = {
        Lazy::new(|| {
            Arc::new(NestedField::optional(
                514,
                "deleted_rows_count",
                Type::Primitive(PrimitiveType::Long),
            ))
        })
    };
    static PARTITIONS: Lazy<NestedFieldRef> = {
        Lazy::new(|| {
            // element type
            let fields = vec![
                Arc::new(NestedField::required(
                    509,
                    "contains_null",
                    Type::Primitive(PrimitiveType::Boolean),
                )),
                Arc::new(NestedField::optional(
                    518,
                    "contains_nan",
                    Type::Primitive(PrimitiveType::Boolean),
                )),
                Arc::new(NestedField::optional(
                    510,
                    "lower_bound",
                    Type::Primitive(PrimitiveType::Binary),
                )),
                Arc::new(NestedField::optional(
                    511,
                    "upper_bound",
                    Type::Primitive(PrimitiveType::Binary),
                )),
            ];
            let element_field = Arc::new(NestedField::required(
                508,
                "r_508",
                Type::Struct(StructType::new(fields)),
            ));
            Arc::new(NestedField::optional(
                507,
                "partitions",
                Type::List(ListType { element_field }),
            ))
        })
    };
    static KEY_METADATA: Lazy<NestedFieldRef> = {
        Lazy::new(|| {
            Arc::new(NestedField::optional(
                519,
                "key_metadata",
                Type::Primitive(PrimitiveType::Binary),
            ))
        })
    };
    static FIRST_ROW_ID: Lazy<NestedFieldRef> = {
        Lazy::new(|| {
            Arc::new(NestedField::optional(
                520,
                "first_row_id",
                Type::Primitive(PrimitiveType::Long),
            ))
        })
    };

    static V1_SCHEMA: Lazy<Schema> = {
        Lazy::new(|| {
            let fields = vec![
                MANIFEST_PATH.clone(),
                MANIFEST_LENGTH.clone(),
                PARTITION_SPEC_ID.clone(),
                ADDED_SNAPSHOT_ID.clone(),
                ADDED_FILES_COUNT_V1.clone().to_owned(),
                EXISTING_FILES_COUNT_V1.clone(),
                DELETED_FILES_COUNT_V1.clone(),
                ADDED_ROWS_COUNT_V1.clone(),
                EXISTING_ROWS_COUNT_V1.clone(),
                DELETED_ROWS_COUNT_V1.clone(),
                PARTITIONS.clone(),
                KEY_METADATA.clone(),
            ];
            Schema::builder().with_fields(fields).build().unwrap()
        })
    };

    static V2_SCHEMA: Lazy<Schema> = {
        Lazy::new(|| {
            let fields = vec![
                MANIFEST_PATH.clone(),
                MANIFEST_LENGTH.clone(),
                PARTITION_SPEC_ID.clone(),
                CONTENT.clone(),
                SEQUENCE_NUMBER.clone(),
                MIN_SEQUENCE_NUMBER.clone(),
                ADDED_SNAPSHOT_ID.clone(),
                ADDED_FILES_COUNT_V2.clone(),
                EXISTING_FILES_COUNT_V2.clone(),
                DELETED_FILES_COUNT_V2.clone(),
                ADDED_ROWS_COUNT_V2.clone(),
                EXISTING_ROWS_COUNT_V2.clone(),
                DELETED_ROWS_COUNT_V2.clone(),
                PARTITIONS.clone(),
                KEY_METADATA.clone(),
            ];
            Schema::builder().with_fields(fields).build().unwrap()
        })
    };

    static V3_SCHEMA: Lazy<Schema> = {
        Lazy::new(|| {
            let fields = vec![
                MANIFEST_PATH.clone(),
                MANIFEST_LENGTH.clone(),
                PARTITION_SPEC_ID.clone(),
                CONTENT.clone(),
                SEQUENCE_NUMBER.clone(),
                MIN_SEQUENCE_NUMBER.clone(),
                ADDED_SNAPSHOT_ID.clone(),
                ADDED_FILES_COUNT_V2.clone(),
                EXISTING_FILES_COUNT_V2.clone(),
                DELETED_FILES_COUNT_V2.clone(),
                ADDED_ROWS_COUNT_V2.clone(),
                EXISTING_ROWS_COUNT_V2.clone(),
                DELETED_ROWS_COUNT_V2.clone(),
                PARTITIONS.clone(),
                KEY_METADATA.clone(),
                FIRST_ROW_ID.clone(),
            ];
            Schema::builder().with_fields(fields).build().unwrap()
        })
    };

    pub(super) static MANIFEST_LIST_AVRO_SCHEMA_V1: Lazy<AvroSchema> =
        Lazy::new(|| schema_to_avro_schema("manifest_file", &V1_SCHEMA).unwrap());

    pub(super) static MANIFEST_LIST_AVRO_SCHEMA_V2: Lazy<AvroSchema> =
        Lazy::new(|| schema_to_avro_schema("manifest_file", &V2_SCHEMA).unwrap());

    pub(super) static MANIFEST_LIST_AVRO_SCHEMA_V3: Lazy<AvroSchema> =
        Lazy::new(|| schema_to_avro_schema("manifest_file", &V3_SCHEMA).unwrap());
}

/// Entry in a manifest list.
#[derive(Debug, PartialEq, Clone, Eq, Hash)]
pub struct ManifestFile {
    /// field: 500
    ///
    /// Location of the manifest file
    pub manifest_path: String,
    /// field: 501
    ///
    /// Length of the manifest file in bytes
    pub manifest_length: i64,
    /// field: 502
    ///
    /// ID of a partition spec used to write the manifest; must be listed
    /// in table metadata partition-specs
    pub partition_spec_id: i32,
    /// field: 517
    ///
    /// The type of files tracked by the manifest, either data or delete
    /// files; 0 for all v1 manifests
    pub content: ManifestContentType,
    /// field: 515
    ///
    /// The sequence number when the manifest was added to the table; use 0
    /// when reading v1 manifest lists
    pub sequence_number: i64,
    /// field: 516
    ///
    /// The minimum data sequence number of all live data or delete files in
    /// the manifest; use 0 when reading v1 manifest lists
    pub min_sequence_number: i64,
    /// field: 503
    ///
    /// ID of the snapshot where the manifest file was added
    pub added_snapshot_id: i64,
    /// field: 504
    ///
    /// Number of entries in the manifest that have status ADDED, when null
    /// this is assumed to be non-zero
    pub added_files_count: Option<u32>,
    /// field: 505
    ///
    /// Number of entries in the manifest that have status EXISTING (0),
    /// when null this is assumed to be non-zero
    pub existing_files_count: Option<u32>,
    /// field: 506
    ///
    /// Number of entries in the manifest that have status DELETED (2),
    /// when null this is assumed to be non-zero
    pub deleted_files_count: Option<u32>,
    /// field: 512
    ///
    /// Number of rows in all of files in the manifest that have status
    /// ADDED, when null this is assumed to be non-zero
    pub added_rows_count: Option<u64>,
    /// field: 513
    ///
    /// Number of rows in all of files in the manifest that have status
    /// EXISTING, when null this is assumed to be non-zero
    pub existing_rows_count: Option<u64>,
    /// field: 514
    ///
    /// Number of rows in all of files in the manifest that have status
    /// DELETED, when null this is assumed to be non-zero
    pub deleted_rows_count: Option<u64>,
    /// field: 507
    /// element_field: 508
    ///
    /// A list of field summaries for each partition field in the spec. Each
    /// field in the list corresponds to a field in the manifest file’s
    /// partition spec.
    pub partitions: Option<Vec<FieldSummary>>,
    /// field: 519
    ///
    /// Implementation-specific key metadata for encryption
    pub key_metadata: Option<Vec<u8>>,
    /// field 520
    ///
    /// The starting _row_id to assign to rows added by ADDED data files
    pub first_row_id: Option<u64>,
}

impl ManifestFile {
    /// Checks if the manifest file has any added files.
    pub fn has_added_files(&self) -> bool {
        self.added_files_count.map(|c| c > 0).unwrap_or(true)
    }

    /// Checks whether this manifest contains entries with DELETED status.
    pub fn has_deleted_files(&self) -> bool {
        self.deleted_files_count.map(|c| c > 0).unwrap_or(true)
    }

    /// Checks if the manifest file has any existed files.
    pub fn has_existing_files(&self) -> bool {
        self.existing_files_count.map(|c| c > 0).unwrap_or(true)
    }
}

/// The type of files tracked by the manifest, either data or delete files; Data(0) for all v1 manifests
#[derive(Debug, PartialEq, Clone, Copy, Eq, Hash, Default)]
pub enum ManifestContentType {
    /// The manifest content is data.
    #[default]
    Data = 0,
    /// The manifest content is deletes.
    Deletes = 1,
}

impl FromStr for ManifestContentType {
    type Err = Error;

    fn from_str(s: &str) -> Result<Self> {
        match s {
            "data" => Ok(ManifestContentType::Data),
            "deletes" => Ok(ManifestContentType::Deletes),
            _ => Err(Error::new(
                ErrorKind::DataInvalid,
                format!("Invalid manifest content type: {s}"),
            )),
        }
    }
}

impl std::fmt::Display for ManifestContentType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ManifestContentType::Data => write!(f, "data"),
            ManifestContentType::Deletes => write!(f, "deletes"),
        }
    }
}

impl TryFrom<i32> for ManifestContentType {
    type Error = Error;

    fn try_from(value: i32) -> std::result::Result<Self, Self::Error> {
        match value {
            0 => Ok(ManifestContentType::Data),
            1 => Ok(ManifestContentType::Deletes),
            _ => Err(Error::new(
                crate::ErrorKind::DataInvalid,
                format!("Invalid manifest content type. Expected 0 or 1, got {value}"),
            )),
        }
    }
}

impl ManifestFile {
    /// Load [`Manifest`].
    ///
    /// This method will also initialize inherited values of [`ManifestEntry`], such as `sequence_number`.
    pub async fn load_manifest(&self, file_io: &FileIO) -> Result<Manifest> {
        let avro = file_io.new_input(&self.manifest_path)?.read().await?;

        let (metadata, mut entries) = Manifest::try_from_avro_bytes(&avro)?;

        // Let entries inherit values from the manifest list entry.
        for entry in &mut entries {
            entry.inherit_data(self);
        }

        Ok(Manifest::new(metadata, entries))
    }
}

/// Field summary for partition field in the spec.
///
/// Each field in the list corresponds to a field in the manifest file’s partition spec.
#[derive(Debug, Serialize, Deserialize, PartialEq, Eq, Clone, Default, Hash)]
pub struct FieldSummary {
    /// field: 509
    ///
    /// Whether the manifest contains at least one partition with a null
    /// value for the field
    pub contains_null: bool,
    /// field: 518
    /// Whether the manifest contains at least one partition with a NaN
    /// value for the field
    pub contains_nan: Option<bool>,
    /// field: 510
    /// The minimum value for the field in the manifests
    /// partitions.
    pub lower_bound: Option<ByteBuf>,
    /// field: 511
    /// The maximum value for the field in the manifests
    /// partitions.
    pub upper_bound: Option<ByteBuf>,
}

/// This is a helper module that defines types to help with serialization/deserialization.
/// For deserialization the input first gets read into either the [ManifestFileV1] or [ManifestFileV2] struct
/// and then converted into the [ManifestFile] struct. Serialization works the other way around.
/// [ManifestFileV1] and [ManifestFileV2] are internal struct that are only used for serialization and deserialization.
pub(super) mod _serde {
    pub use serde_bytes::ByteBuf;
    use serde_derive::{Deserialize, Serialize};

    use super::ManifestFile;
    use crate::Error;
    use crate::error::Result;
    use crate::spec::FieldSummary;

    #[derive(Debug, Serialize, Deserialize, PartialEq, Eq)]
    #[serde(transparent)]
    pub(crate) struct ManifestListV3 {
        entries: Vec<ManifestFileV3>,
    }

    #[derive(Debug, Serialize, Deserialize, PartialEq, Eq)]
    #[serde(transparent)]
    pub(crate) struct ManifestListV2 {
        entries: Vec<ManifestFileV2>,
    }

    #[derive(Debug, Serialize, Deserialize, PartialEq, Eq)]
    #[serde(transparent)]
    pub(crate) struct ManifestListV1 {
        entries: Vec<ManifestFileV1>,
    }

    impl ManifestListV3 {
        /// Converts the [ManifestListV3] into a [ManifestList].
        pub fn try_into(self) -> Result<super::ManifestList> {
            Ok(super::ManifestList {
                entries: self
                    .entries
                    .into_iter()
                    .map(|v| v.try_into())
                    .collect::<Result<Vec<_>>>()?,
            })
        }
    }

    impl TryFrom<super::ManifestList> for ManifestListV3 {
        type Error = Error;

        fn try_from(value: super::ManifestList) -> std::result::Result<Self, Self::Error> {
            Ok(Self {
                entries: value
                    .entries
                    .into_iter()
                    .map(|v| v.try_into())
                    .collect::<std::result::Result<Vec<_>, _>>()?,
            })
        }
    }

    impl ManifestListV2 {
        /// Converts the [ManifestListV2] into a [ManifestList].
        pub fn try_into(self) -> Result<super::ManifestList> {
            Ok(super::ManifestList {
                entries: self
                    .entries
                    .into_iter()
                    .map(|v| v.try_into())
                    .collect::<Result<Vec<_>>>()?,
            })
        }
    }

    impl TryFrom<super::ManifestList> for ManifestListV2 {
        type Error = Error;

        fn try_from(value: super::ManifestList) -> std::result::Result<Self, Self::Error> {
            Ok(Self {
                entries: value
                    .entries
                    .into_iter()
                    .map(|v| v.try_into())
                    .collect::<std::result::Result<Vec<_>, _>>()?,
            })
        }
    }

    impl ManifestListV1 {
        /// Converts the [ManifestListV1] into a [ManifestList].
        pub fn try_into(self) -> Result<super::ManifestList> {
            Ok(super::ManifestList {
                entries: self
                    .entries
                    .into_iter()
                    .map(|v| v.try_into())
                    .collect::<Result<Vec<_>>>()?,
            })
        }
    }

    impl TryFrom<super::ManifestList> for ManifestListV1 {
        type Error = Error;

        fn try_from(value: super::ManifestList) -> std::result::Result<Self, Self::Error> {
            Ok(Self {
                entries: value
                    .entries
                    .into_iter()
                    .map(|v| v.try_into())
                    .collect::<std::result::Result<Vec<_>, _>>()?,
            })
        }
    }

    #[derive(Debug, Serialize, Deserialize, PartialEq, Eq)]
    pub(super) struct ManifestFileV1 {
        pub manifest_path: String,
        pub manifest_length: i64,
        pub partition_spec_id: i32,
        pub added_snapshot_id: i64,
        pub added_data_files_count: Option<i32>,
        pub existing_data_files_count: Option<i32>,
        pub deleted_data_files_count: Option<i32>,
        pub added_rows_count: Option<i64>,
        pub existing_rows_count: Option<i64>,
        pub deleted_rows_count: Option<i64>,
        pub partitions: Option<Vec<FieldSummary>>,
        pub key_metadata: Option<ByteBuf>,
    }

    // Aliases were added to fields that were renamed in Iceberg  1.5.0 (https://github.com/apache/iceberg/pull/5338), in order to support both conventions/versions.
    // In the current implementation deserialization is done using field names, and therefore these fields may appear as either.
    // see issue that raised this here: https://github.com/apache/iceberg-rust/issues/338
    #[derive(Debug, Serialize, Deserialize, PartialEq, Eq)]
    pub(super) struct ManifestFileV2 {
        pub manifest_path: String,
        pub manifest_length: i64,
        pub partition_spec_id: i32,
        #[serde(default = "v2_default_content_for_v1")]
        pub content: i32,
        #[serde(default = "v2_default_sequence_number_for_v1")]
        pub sequence_number: i64,
        #[serde(default = "v2_default_min_sequence_number_for_v1")]
        pub min_sequence_number: i64,
        pub added_snapshot_id: i64,
        #[serde(alias = "added_data_files_count", alias = "added_files_count")]
        pub added_files_count: i32,
        #[serde(alias = "existing_data_files_count", alias = "existing_files_count")]
        pub existing_files_count: i32,
        #[serde(alias = "deleted_data_files_count", alias = "deleted_files_count")]
        pub deleted_files_count: i32,
        pub added_rows_count: i64,
        pub existing_rows_count: i64,
        pub deleted_rows_count: i64,
        pub partitions: Option<Vec<FieldSummary>>,
        pub key_metadata: Option<ByteBuf>,
    }

    #[derive(Debug, Serialize, Deserialize, PartialEq, Eq)]
    pub(super) struct ManifestFileV3 {
        pub manifest_path: String,
        pub manifest_length: i64,
        pub partition_spec_id: i32,
        #[serde(default = "v2_default_content_for_v1")]
        pub content: i32,
        #[serde(default = "v2_default_sequence_number_for_v1")]
        pub sequence_number: i64,
        #[serde(default = "v2_default_min_sequence_number_for_v1")]
        pub min_sequence_number: i64,
        pub added_snapshot_id: i64,
        #[serde(alias = "added_data_files_count", alias = "added_files_count")]
        pub added_files_count: i32,
        #[serde(alias = "existing_data_files_count", alias = "existing_files_count")]
        pub existing_files_count: i32,
        #[serde(alias = "deleted_data_files_count", alias = "deleted_files_count")]
        pub deleted_files_count: i32,
        pub added_rows_count: i64,
        pub existing_rows_count: i64,
        pub deleted_rows_count: i64,
        pub partitions: Option<Vec<FieldSummary>>,
        pub key_metadata: Option<ByteBuf>,
        pub first_row_id: Option<u64>,
    }

    impl ManifestFileV3 {
        /// Converts the [ManifestFileV3] into a [ManifestFile].
        pub fn try_into(self) -> Result<ManifestFile> {
            let manifest_file = ManifestFile {
                manifest_path: self.manifest_path,
                manifest_length: self.manifest_length,
                partition_spec_id: self.partition_spec_id,
                content: self.content.try_into()?,
                sequence_number: self.sequence_number,
                min_sequence_number: self.min_sequence_number,
                added_snapshot_id: self.added_snapshot_id,
                added_files_count: Some(self.added_files_count.try_into()?),
                existing_files_count: Some(self.existing_files_count.try_into()?),
                deleted_files_count: Some(self.deleted_files_count.try_into()?),
                added_rows_count: Some(self.added_rows_count.try_into()?),
                existing_rows_count: Some(self.existing_rows_count.try_into()?),
                deleted_rows_count: Some(self.deleted_rows_count.try_into()?),
                partitions: self.partitions,
                key_metadata: self.key_metadata.map(|b| b.into_vec()),
                first_row_id: self.first_row_id,
            };

            Ok(manifest_file)
        }
    }

    impl ManifestFileV2 {
        /// Converts the [ManifestFileV2] into a [ManifestFile].
        pub fn try_into(self) -> Result<ManifestFile> {
            Ok(ManifestFile {
                manifest_path: self.manifest_path,
                manifest_length: self.manifest_length,
                partition_spec_id: self.partition_spec_id,
                content: self.content.try_into()?,
                sequence_number: self.sequence_number,
                min_sequence_number: self.min_sequence_number,
                added_snapshot_id: self.added_snapshot_id,
                added_files_count: Some(self.added_files_count.try_into()?),
                existing_files_count: Some(self.existing_files_count.try_into()?),
                deleted_files_count: Some(self.deleted_files_count.try_into()?),
                added_rows_count: Some(self.added_rows_count.try_into()?),
                existing_rows_count: Some(self.existing_rows_count.try_into()?),
                deleted_rows_count: Some(self.deleted_rows_count.try_into()?),
                partitions: self.partitions,
                key_metadata: self.key_metadata.map(|b| b.into_vec()),
                first_row_id: None,
            })
        }
    }

    fn v2_default_content_for_v1() -> i32 {
        super::ManifestContentType::Data as i32
    }

    fn v2_default_sequence_number_for_v1() -> i64 {
        0
    }

    fn v2_default_min_sequence_number_for_v1() -> i64 {
        0
    }

    impl ManifestFileV1 {
        /// Converts the [ManifestFileV1] into a [ManifestFile].
        pub fn try_into(self) -> Result<ManifestFile> {
            Ok(ManifestFile {
                manifest_path: self.manifest_path,
                manifest_length: self.manifest_length,
                partition_spec_id: self.partition_spec_id,
                added_snapshot_id: self.added_snapshot_id,
                added_files_count: self
                    .added_data_files_count
                    .map(TryInto::try_into)
                    .transpose()?,
                existing_files_count: self
                    .existing_data_files_count
                    .map(TryInto::try_into)
                    .transpose()?,
                deleted_files_count: self
                    .deleted_data_files_count
                    .map(TryInto::try_into)
                    .transpose()?,
                added_rows_count: self.added_rows_count.map(TryInto::try_into).transpose()?,
                existing_rows_count: self
                    .existing_rows_count
                    .map(TryInto::try_into)
                    .transpose()?,
                deleted_rows_count: self.deleted_rows_count.map(TryInto::try_into).transpose()?,
                partitions: self.partitions,
                key_metadata: self.key_metadata.map(|b| b.into_vec()),
                // as ref: https://iceberg.apache.org/spec/#partitioning
                // use 0 when reading v1 manifest lists
                content: super::ManifestContentType::Data,
                sequence_number: 0,
                min_sequence_number: 0,
                first_row_id: None,
            })
        }
    }

    fn convert_to_serde_key_metadata(key_metadata: Option<Vec<u8>>) -> Option<ByteBuf> {
        match key_metadata {
            Some(metadata) if !metadata.is_empty() => Some(ByteBuf::from(metadata)),
            _ => None,
        }
    }

    impl TryFrom<ManifestFile> for ManifestFileV3 {
        type Error = Error;

        fn try_from(value: ManifestFile) -> std::result::Result<Self, Self::Error> {
            let key_metadata = convert_to_serde_key_metadata(value.key_metadata);
            Ok(Self {
                manifest_path: value.manifest_path,
                manifest_length: value.manifest_length,
                partition_spec_id: value.partition_spec_id,
                content: value.content as i32,
                sequence_number: value.sequence_number,
                min_sequence_number: value.min_sequence_number,
                added_snapshot_id: value.added_snapshot_id,
                added_files_count: value
                    .added_files_count
                    .ok_or_else(|| {
                        Error::new(
                            crate::ErrorKind::DataInvalid,
                            "added_data_files_count in ManifestFileV3 is required",
                        )
                    })?
                    .try_into()?,
                existing_files_count: value
                    .existing_files_count
                    .ok_or_else(|| {
                        Error::new(
                            crate::ErrorKind::DataInvalid,
                            "existing_data_files_count in ManifestFileV3 is required",
                        )
                    })?
                    .try_into()?,
                deleted_files_count: value
                    .deleted_files_count
                    .ok_or_else(|| {
                        Error::new(
                            crate::ErrorKind::DataInvalid,
                            "deleted_data_files_count in ManifestFileV3 is required",
                        )
                    })?
                    .try_into()?,
                added_rows_count: value
                    .added_rows_count
                    .ok_or_else(|| {
                        Error::new(
                            crate::ErrorKind::DataInvalid,
                            "added_rows_count in ManifestFileV3 is required",
                        )
                    })?
                    .try_into()?,
                existing_rows_count: value
                    .existing_rows_count
                    .ok_or_else(|| {
                        Error::new(
                            crate::ErrorKind::DataInvalid,
                            "existing_rows_count in ManifestFileV3 is required",
                        )
                    })?
                    .try_into()?,
                deleted_rows_count: value
                    .deleted_rows_count
                    .ok_or_else(|| {
                        Error::new(
                            crate::ErrorKind::DataInvalid,
                            "deleted_rows_count in ManifestFileV3 is required",
                        )
                    })?
                    .try_into()?,
                partitions: value.partitions,
                key_metadata,
                first_row_id: value.first_row_id,
            })
        }
    }

    impl TryFrom<ManifestFile> for ManifestFileV2 {
        type Error = Error;

        fn try_from(value: ManifestFile) -> std::result::Result<Self, Self::Error> {
            let key_metadata = convert_to_serde_key_metadata(value.key_metadata);
            Ok(Self {
                manifest_path: value.manifest_path,
                manifest_length: value.manifest_length,
                partition_spec_id: value.partition_spec_id,
                content: value.content as i32,
                sequence_number: value.sequence_number,
                min_sequence_number: value.min_sequence_number,
                added_snapshot_id: value.added_snapshot_id,
                added_files_count: value
                    .added_files_count
                    .ok_or_else(|| {
                        Error::new(
                            crate::ErrorKind::DataInvalid,
                            "added_data_files_count in ManifestFileV2 should be require",
                        )
                    })?
                    .try_into()?,
                existing_files_count: value
                    .existing_files_count
                    .ok_or_else(|| {
                        Error::new(
                            crate::ErrorKind::DataInvalid,
                            "existing_data_files_count in ManifestFileV2 should be require",
                        )
                    })?
                    .try_into()?,
                deleted_files_count: value
                    .deleted_files_count
                    .ok_or_else(|| {
                        Error::new(
                            crate::ErrorKind::DataInvalid,
                            "deleted_data_files_count in ManifestFileV2 should be require",
                        )
                    })?
                    .try_into()?,
                added_rows_count: value
                    .added_rows_count
                    .ok_or_else(|| {
                        Error::new(
                            crate::ErrorKind::DataInvalid,
                            "added_rows_count in ManifestFileV2 should be require",
                        )
                    })?
                    .try_into()?,
                existing_rows_count: value
                    .existing_rows_count
                    .ok_or_else(|| {
                        Error::new(
                            crate::ErrorKind::DataInvalid,
                            "existing_rows_count in ManifestFileV2 should be require",
                        )
                    })?
                    .try_into()?,
                deleted_rows_count: value
                    .deleted_rows_count
                    .ok_or_else(|| {
                        Error::new(
                            crate::ErrorKind::DataInvalid,
                            "deleted_rows_count in ManifestFileV2 should be require",
                        )
                    })?
                    .try_into()?,
                partitions: value.partitions,
                key_metadata,
            })
        }
    }

    impl TryFrom<ManifestFile> for ManifestFileV1 {
        type Error = Error;

        fn try_from(value: ManifestFile) -> std::result::Result<Self, Self::Error> {
            let key_metadata = convert_to_serde_key_metadata(value.key_metadata);
            Ok(Self {
                manifest_path: value.manifest_path,
                manifest_length: value.manifest_length,
                partition_spec_id: value.partition_spec_id,
                added_snapshot_id: value.added_snapshot_id,
                added_data_files_count: value
                    .added_files_count
                    .map(TryInto::try_into)
                    .transpose()?,
                existing_data_files_count: value
                    .existing_files_count
                    .map(TryInto::try_into)
                    .transpose()?,
                deleted_data_files_count: value
                    .deleted_files_count
                    .map(TryInto::try_into)
                    .transpose()?,
                added_rows_count: value.added_rows_count.map(TryInto::try_into).transpose()?,
                existing_rows_count: value
                    .existing_rows_count
                    .map(TryInto::try_into)
                    .transpose()?,
                deleted_rows_count: value
                    .deleted_rows_count
                    .map(TryInto::try_into)
                    .transpose()?,
                partitions: value.partitions,
                key_metadata,
            })
        }
    }
}

#[cfg(test)]
mod test {
    use std::fs;

    use apache_avro::{Reader, Schema};
    use tempfile::TempDir;

    use super::_serde::ManifestListV2;
    use crate::io::FileIOBuilder;
    use crate::spec::manifest_list::_serde::{ManifestListV1, ManifestListV3};
    use crate::spec::{
        Datum, FieldSummary, ManifestContentType, ManifestFile, ManifestList, ManifestListWriter,
        UNASSIGNED_SEQUENCE_NUMBER,
    };

    #[tokio::test]
    async fn test_parse_manifest_list_v1() {
        let manifest_list = ManifestList {
            entries: vec![
                ManifestFile {
                    manifest_path: "/opt/bitnami/spark/warehouse/db/table/metadata/10d28031-9739-484c-92db-cdf2975cead4-m0.avro".to_string(),
                    manifest_length: 5806,
                    partition_spec_id: 0,
                    content: ManifestContentType::Data,
                    sequence_number: 0,
                    min_sequence_number: 0,
                    added_snapshot_id: 1646658105718557341,
                    added_files_count: Some(3),
                    existing_files_count: Some(0),
                    deleted_files_count: Some(0),
                    added_rows_count: Some(3),
                    existing_rows_count: Some(0),
                    deleted_rows_count: Some(0),
                    partitions: Some(vec![]),
                    key_metadata: None,
                    first_row_id: None,
                }
            ]
        };

        let file_io = FileIOBuilder::new_fs_io().build().unwrap();

        let tmp_dir = TempDir::new().unwrap();
        let file_name = "simple_manifest_list_v1.avro";
        let full_path = format!("{}/{}", tmp_dir.path().to_str().unwrap(), file_name);

        let mut writer = ManifestListWriter::v1(
            file_io.new_output(full_path.clone()).unwrap(),
            1646658105718557341,
            Some(1646658105718557341),
        );

        writer
            .add_manifests(manifest_list.entries.clone().into_iter())
            .unwrap();
        writer.close().await.unwrap();

        let bs = fs::read(full_path).expect("read_file must succeed");

        let parsed_manifest_list =
            ManifestList::parse_with_version(&bs, crate::spec::FormatVersion::V1).unwrap();

        assert_eq!(manifest_list, parsed_manifest_list);
    }

    #[tokio::test]
    async fn test_parse_manifest_list_v2() {
        let manifest_list = ManifestList {
            entries: vec![
                ManifestFile {
                    manifest_path: "s3a://icebergdata/demo/s1/t1/metadata/05ffe08b-810f-49b3-a8f4-e88fc99b254a-m0.avro".to_string(),
                    manifest_length: 6926,
                    partition_spec_id: 1,
                    content: ManifestContentType::Data,
                    sequence_number: 1,
                    min_sequence_number: 1,
                    added_snapshot_id: 377075049360453639,
                    added_files_count: Some(1),
                    existing_files_count: Some(0),
                    deleted_files_count: Some(0),
                    added_rows_count: Some(3),
                    existing_rows_count: Some(0),
                    deleted_rows_count: Some(0),
                    partitions: Some(
                        vec![FieldSummary { contains_null: false, contains_nan: Some(false), lower_bound: Some(Datum::long(1).to_bytes().unwrap()), upper_bound: Some(Datum::long(1).to_bytes().unwrap())}]
                    ),
                    key_metadata: None,
                    first_row_id: None,
                },
                ManifestFile {
                    manifest_path: "s3a://icebergdata/demo/s1/t1/metadata/05ffe08b-810f-49b3-a8f4-e88fc99b254a-m1.avro".to_string(),
                    manifest_length: 6926,
                    partition_spec_id: 2,
                    content: ManifestContentType::Data,
                    sequence_number: 1,
                    min_sequence_number: 1,
                    added_snapshot_id: 377075049360453639,
                    added_files_count: Some(1),
                    existing_files_count: Some(0),
                    deleted_files_count: Some(0),
                    added_rows_count: Some(3),
                    existing_rows_count: Some(0),
                    deleted_rows_count: Some(0),
                    partitions: Some(
                        vec![FieldSummary { contains_null: false, contains_nan: Some(false), lower_bound: Some(Datum::float(1.1).to_bytes().unwrap()), upper_bound: Some(Datum::float(2.1).to_bytes().unwrap())}]
                    ),
                    key_metadata: None,
                    first_row_id: None,
                }
            ]
        };

        let file_io = FileIOBuilder::new_fs_io().build().unwrap();

        let tmp_dir = TempDir::new().unwrap();
        let file_name = "simple_manifest_list_v1.avro";
        let full_path = format!("{}/{}", tmp_dir.path().to_str().unwrap(), file_name);

        let mut writer = ManifestListWriter::v2(
            file_io.new_output(full_path.clone()).unwrap(),
            1646658105718557341,
            Some(1646658105718557341),
            1,
        );

        writer
            .add_manifests(manifest_list.entries.clone().into_iter())
            .unwrap();
        writer.close().await.unwrap();

        let bs = fs::read(full_path).expect("read_file must succeed");

        let parsed_manifest_list =
            ManifestList::parse_with_version(&bs, crate::spec::FormatVersion::V2).unwrap();

        assert_eq!(manifest_list, parsed_manifest_list);
    }

    #[tokio::test]
    async fn test_parse_manifest_list_v3() {
        let manifest_list = ManifestList {
            entries: vec![
                ManifestFile {
                    manifest_path: "s3a://icebergdata/demo/s1/t1/metadata/05ffe08b-810f-49b3-a8f4-e88fc99b254a-m0.avro".to_string(),
                    manifest_length: 6926,
                    partition_spec_id: 1,
                    content: ManifestContentType::Data,
                    sequence_number: 1,
                    min_sequence_number: 1,
                    added_snapshot_id: 377075049360453639,
                    added_files_count: Some(1),
                    existing_files_count: Some(0),
                    deleted_files_count: Some(0),
                    added_rows_count: Some(3),
                    existing_rows_count: Some(0),
                    deleted_rows_count: Some(0),
                    partitions: Some(
                        vec![FieldSummary { contains_null: false, contains_nan: Some(false), lower_bound: Some(Datum::long(1).to_bytes().unwrap()), upper_bound: Some(Datum::long(1).to_bytes().unwrap())}]
                    ),
                    key_metadata: None,
                    first_row_id: Some(10),
                },
                ManifestFile {
                    manifest_path: "s3a://icebergdata/demo/s1/t1/metadata/05ffe08b-810f-49b3-a8f4-e88fc99b254a-m1.avro".to_string(),
                    manifest_length: 6926,
                    partition_spec_id: 2,
                    content: ManifestContentType::Data,
                    sequence_number: 1,
                    min_sequence_number: 1,
                    added_snapshot_id: 377075049360453639,
                    added_files_count: Some(1),
                    existing_files_count: Some(0),
                    deleted_files_count: Some(0),
                    added_rows_count: Some(3),
                    existing_rows_count: Some(0),
                    deleted_rows_count: Some(0),
                    partitions: Some(
                        vec![FieldSummary { contains_null: false, contains_nan: Some(false), lower_bound: Some(Datum::float(1.1).to_bytes().unwrap()), upper_bound: Some(Datum::float(2.1).to_bytes().unwrap())}]
                    ),
                    key_metadata: None,
                    first_row_id: Some(13),
                }
            ]
        };

        let file_io = FileIOBuilder::new_fs_io().build().unwrap();

        let tmp_dir = TempDir::new().unwrap();
        let file_name = "simple_manifest_list_v3.avro";
        let full_path = format!("{}/{}", tmp_dir.path().to_str().unwrap(), file_name);

        let mut writer = ManifestListWriter::v3(
            file_io.new_output(full_path.clone()).unwrap(),
            377075049360453639,
            Some(377075049360453639),
            1,
            Some(10),
        );

        writer
            .add_manifests(manifest_list.entries.clone().into_iter())
            .unwrap();
        writer.close().await.unwrap();

        let bs = fs::read(full_path).expect("read_file must succeed");

        let parsed_manifest_list =
            ManifestList::parse_with_version(&bs, crate::spec::FormatVersion::V3).unwrap();

        assert_eq!(manifest_list, parsed_manifest_list);
    }

    #[test]
    fn test_serialize_manifest_list_v1() {
        let manifest_list:ManifestListV1 = ManifestList {
            entries: vec![ManifestFile {
                manifest_path: "/opt/bitnami/spark/warehouse/db/table/metadata/10d28031-9739-484c-92db-cdf2975cead4-m0.avro".to_string(),
                manifest_length: 5806,
                partition_spec_id: 0,
                content: ManifestContentType::Data,
                sequence_number: 0,
                min_sequence_number: 0,
                added_snapshot_id: 1646658105718557341,
                added_files_count: Some(3),
                existing_files_count: Some(0),
                deleted_files_count: Some(0),
                added_rows_count: Some(3),
                existing_rows_count: Some(0),
                deleted_rows_count: Some(0),
                partitions: None,
                key_metadata: None,
                first_row_id: None,
            }]
        }.try_into().unwrap();
        let result = serde_json::to_string(&manifest_list).unwrap();
        assert_eq!(
            result,
            r#"[{"manifest_path":"/opt/bitnami/spark/warehouse/db/table/metadata/10d28031-9739-484c-92db-cdf2975cead4-m0.avro","manifest_length":5806,"partition_spec_id":0,"added_snapshot_id":1646658105718557341,"added_data_files_count":3,"existing_data_files_count":0,"deleted_data_files_count":0,"added_rows_count":3,"existing_rows_count":0,"deleted_rows_count":0,"partitions":null,"key_metadata":null}]"#
        );
    }

    #[test]
    fn test_serialize_manifest_list_v2() {
        let manifest_list:ManifestListV2 = ManifestList {
            entries: vec![ManifestFile {
                manifest_path: "s3a://icebergdata/demo/s1/t1/metadata/05ffe08b-810f-49b3-a8f4-e88fc99b254a-m0.avro".to_string(),
                manifest_length: 6926,
                partition_spec_id: 1,
                content: ManifestContentType::Data,
                sequence_number: 1,
                min_sequence_number: 1,
                added_snapshot_id: 377075049360453639,
                added_files_count: Some(1),
                existing_files_count: Some(0),
                deleted_files_count: Some(0),
                added_rows_count: Some(3),
                existing_rows_count: Some(0),
                deleted_rows_count: Some(0),
                partitions: Some(
                    vec![FieldSummary { contains_null: false, contains_nan: Some(false), lower_bound: Some(Datum::long(1).to_bytes().unwrap()), upper_bound: Some(Datum::long(1).to_bytes().unwrap())}]
                ),
                key_metadata: None,
                first_row_id: None,
            }]
        }.try_into().unwrap();
        let result = serde_json::to_string(&manifest_list).unwrap();
        assert_eq!(
            result,
            r#"[{"manifest_path":"s3a://icebergdata/demo/s1/t1/metadata/05ffe08b-810f-49b3-a8f4-e88fc99b254a-m0.avro","manifest_length":6926,"partition_spec_id":1,"content":0,"sequence_number":1,"min_sequence_number":1,"added_snapshot_id":377075049360453639,"added_files_count":1,"existing_files_count":0,"deleted_files_count":0,"added_rows_count":3,"existing_rows_count":0,"deleted_rows_count":0,"partitions":[{"contains_null":false,"contains_nan":false,"lower_bound":[1,0,0,0,0,0,0,0],"upper_bound":[1,0,0,0,0,0,0,0]}],"key_metadata":null}]"#
        );
    }

    #[test]
    fn test_serialize_manifest_list_v3() {
        let manifest_list: ManifestListV3 = ManifestList {
            entries: vec![ManifestFile {
                manifest_path: "s3a://icebergdata/demo/s1/t1/metadata/05ffe08b-810f-49b3-a8f4-e88fc99b254a-m0.avro".to_string(),
                manifest_length: 6926,
                partition_spec_id: 1,
                content: ManifestContentType::Data,
                sequence_number: 1,
                min_sequence_number: 1,
                added_snapshot_id: 377075049360453639,
                added_files_count: Some(1),
                existing_files_count: Some(0),
                deleted_files_count: Some(0),
                added_rows_count: Some(3),
                existing_rows_count: Some(0),
                deleted_rows_count: Some(0),
                partitions: Some(
                    vec![FieldSummary { contains_null: false, contains_nan: Some(false), lower_bound: Some(Datum::long(1).to_bytes().unwrap()), upper_bound: Some(Datum::long(1).to_bytes().unwrap())}]
                ),
                key_metadata: None,
                first_row_id: Some(10),
            }]
        }.try_into().unwrap();
        let result = serde_json::to_string(&manifest_list).unwrap();
        assert_eq!(
            result,
            r#"[{"manifest_path":"s3a://icebergdata/demo/s1/t1/metadata/05ffe08b-810f-49b3-a8f4-e88fc99b254a-m0.avro","manifest_length":6926,"partition_spec_id":1,"content":0,"sequence_number":1,"min_sequence_number":1,"added_snapshot_id":377075049360453639,"added_files_count":1,"existing_files_count":0,"deleted_files_count":0,"added_rows_count":3,"existing_rows_count":0,"deleted_rows_count":0,"partitions":[{"contains_null":false,"contains_nan":false,"lower_bound":[1,0,0,0,0,0,0,0],"upper_bound":[1,0,0,0,0,0,0,0]}],"key_metadata":null,"first_row_id":10}]"#
        );
    }

    #[tokio::test]
    async fn test_manifest_list_writer_v1() {
        let expected_manifest_list = ManifestList {
            entries: vec![ManifestFile {
                manifest_path: "/opt/bitnami/spark/warehouse/db/table/metadata/10d28031-9739-484c-92db-cdf2975cead4-m0.avro".to_string(),
                manifest_length: 5806,
                partition_spec_id: 1,
                content: ManifestContentType::Data,
                sequence_number: 0,
                min_sequence_number: 0,
                added_snapshot_id: 1646658105718557341,
                added_files_count: Some(3),
                existing_files_count: Some(0),
                deleted_files_count: Some(0),
                added_rows_count: Some(3),
                existing_rows_count: Some(0),
                deleted_rows_count: Some(0),
                partitions: Some(
                    vec![FieldSummary { contains_null: false, contains_nan: Some(false), lower_bound: Some(Datum::long(1).to_bytes().unwrap()), upper_bound: Some(Datum::long(1).to_bytes().unwrap())}],
                ),
                key_metadata: None,
                first_row_id: None,
            }]
        };

        let temp_dir = TempDir::new().unwrap();
        let path = temp_dir.path().join("manifest_list_v1.avro");
        let io = FileIOBuilder::new_fs_io().build().unwrap();
        let output_file = io.new_output(path.to_str().unwrap()).unwrap();

        let mut writer = ManifestListWriter::v1(output_file, 1646658105718557341, Some(0));
        writer
            .add_manifests(expected_manifest_list.entries.clone().into_iter())
            .unwrap();
        writer.close().await.unwrap();

        let bs = fs::read(path).unwrap();

        let manifest_list =
            ManifestList::parse_with_version(&bs, crate::spec::FormatVersion::V1).unwrap();
        assert_eq!(manifest_list, expected_manifest_list);

        temp_dir.close().unwrap();
    }

    #[tokio::test]
    async fn test_manifest_list_writer_v2() {
        let snapshot_id = 377075049360453639;
        let seq_num = 1;
        let mut expected_manifest_list = ManifestList {
            entries: vec![ManifestFile {
                manifest_path: "s3a://icebergdata/demo/s1/t1/metadata/05ffe08b-810f-49b3-a8f4-e88fc99b254a-m0.avro".to_string(),
                manifest_length: 6926,
                partition_spec_id: 1,
                content: ManifestContentType::Data,
                sequence_number: UNASSIGNED_SEQUENCE_NUMBER,
                min_sequence_number: UNASSIGNED_SEQUENCE_NUMBER,
                added_snapshot_id: snapshot_id,
                added_files_count: Some(1),
                existing_files_count: Some(0),
                deleted_files_count: Some(0),
                added_rows_count: Some(3),
                existing_rows_count: Some(0),
                deleted_rows_count: Some(0),
                partitions: Some(
                    vec![FieldSummary { contains_null: false, contains_nan: Some(false), lower_bound: Some(Datum::long(1).to_bytes().unwrap()), upper_bound: Some(Datum::long(1).to_bytes().unwrap())}]
                ),
                key_metadata: None,
                first_row_id: None,
            }]
        };

        let temp_dir = TempDir::new().unwrap();
        let path = temp_dir.path().join("manifest_list_v2.avro");
        let io = FileIOBuilder::new_fs_io().build().unwrap();
        let output_file = io.new_output(path.to_str().unwrap()).unwrap();

        let mut writer = ManifestListWriter::v2(output_file, snapshot_id, Some(0), seq_num);
        writer
            .add_manifests(expected_manifest_list.entries.clone().into_iter())
            .unwrap();
        writer.close().await.unwrap();

        let bs = fs::read(path).unwrap();
        let manifest_list =
            ManifestList::parse_with_version(&bs, crate::spec::FormatVersion::V2).unwrap();
        expected_manifest_list.entries[0].sequence_number = seq_num;
        expected_manifest_list.entries[0].min_sequence_number = seq_num;
        assert_eq!(manifest_list, expected_manifest_list);

        temp_dir.close().unwrap();
    }

    #[tokio::test]
    async fn test_manifest_list_writer_v3() {
        let snapshot_id = 377075049360453639;
        let seq_num = 1;
        let mut expected_manifest_list = ManifestList {
            entries: vec![ManifestFile {
                manifest_path: "s3a://icebergdata/demo/s1/t1/metadata/05ffe08b-810f-49b3-a8f4-e88fc99b254a-m0.avro".to_string(),
                manifest_length: 6926,
                partition_spec_id: 1,
                content: ManifestContentType::Data,
                sequence_number: UNASSIGNED_SEQUENCE_NUMBER,
                min_sequence_number: UNASSIGNED_SEQUENCE_NUMBER,
                added_snapshot_id: snapshot_id,
                added_files_count: Some(1),
                existing_files_count: Some(0),
                deleted_files_count: Some(0),
                added_rows_count: Some(3),
                existing_rows_count: Some(0),
                deleted_rows_count: Some(0),
                partitions: Some(
                    vec![FieldSummary { contains_null: false, contains_nan: Some(false), lower_bound: Some(Datum::long(1).to_bytes().unwrap()), upper_bound: Some(Datum::long(1).to_bytes().unwrap())}]
                ),
                key_metadata: None,
                first_row_id: Some(10),
            }]
        };

        let temp_dir = TempDir::new().unwrap();
        let path = temp_dir.path().join("manifest_list_v2.avro");
        let io = FileIOBuilder::new_fs_io().build().unwrap();
        let output_file = io.new_output(path.to_str().unwrap()).unwrap();

        let mut writer =
            ManifestListWriter::v3(output_file, snapshot_id, Some(0), seq_num, Some(10));
        writer
            .add_manifests(expected_manifest_list.entries.clone().into_iter())
            .unwrap();
        writer.close().await.unwrap();

        let bs = fs::read(path).unwrap();
        let manifest_list =
            ManifestList::parse_with_version(&bs, crate::spec::FormatVersion::V3).unwrap();
        expected_manifest_list.entries[0].sequence_number = seq_num;
        expected_manifest_list.entries[0].min_sequence_number = seq_num;
        expected_manifest_list.entries[0].first_row_id = Some(10);
        assert_eq!(manifest_list, expected_manifest_list);

        temp_dir.close().unwrap();
    }

    #[tokio::test]
    async fn test_manifest_list_writer_v1_as_v2() {
        let expected_manifest_list = ManifestList {
            entries: vec![ManifestFile {
                manifest_path: "/opt/bitnami/spark/warehouse/db/table/metadata/10d28031-9739-484c-92db-cdf2975cead4-m0.avro".to_string(),
                manifest_length: 5806,
                partition_spec_id: 1,
                content: ManifestContentType::Data,
                sequence_number: 0,
                min_sequence_number: 0,
                added_snapshot_id: 1646658105718557341,
                added_files_count: Some(3),
                existing_files_count: Some(0),
                deleted_files_count: Some(0),
                added_rows_count: Some(3),
                existing_rows_count: Some(0),
                deleted_rows_count: Some(0),
                partitions: Some(
                    vec![FieldSummary { contains_null: false, contains_nan: Some(false), lower_bound: Some(Datum::long(1).to_bytes().unwrap()), upper_bound: Some(Datum::long(1).to_bytes().unwrap())}]
                ),
                key_metadata: None,
                first_row_id: None,
            }]
        };

        let temp_dir = TempDir::new().unwrap();
        let path = temp_dir.path().join("manifest_list_v1.avro");
        let io = FileIOBuilder::new_fs_io().build().unwrap();
        let output_file = io.new_output(path.to_str().unwrap()).unwrap();

        let mut writer = ManifestListWriter::v1(output_file, 1646658105718557341, Some(0));
        writer
            .add_manifests(expected_manifest_list.entries.clone().into_iter())
            .unwrap();
        writer.close().await.unwrap();

        let bs = fs::read(path).unwrap();

        let manifest_list =
            ManifestList::parse_with_version(&bs, crate::spec::FormatVersion::V2).unwrap();
        assert_eq!(manifest_list, expected_manifest_list);

        temp_dir.close().unwrap();
    }

    #[tokio::test]
    async fn test_manifest_list_writer_v1_as_v3() {
        let expected_manifest_list = ManifestList {
            entries: vec![ManifestFile {
                manifest_path: "/opt/bitnami/spark/warehouse/db/table/metadata/10d28031-9739-484c-92db-cdf2975cead4-m0.avro".to_string(),
                manifest_length: 5806,
                partition_spec_id: 1,
                content: ManifestContentType::Data,
                sequence_number: 0,
                min_sequence_number: 0,
                added_snapshot_id: 1646658105718557341,
                added_files_count: Some(3),
                existing_files_count: Some(0),
                deleted_files_count: Some(0),
                added_rows_count: Some(3),
                existing_rows_count: Some(0),
                deleted_rows_count: Some(0),
                partitions: Some(
                    vec![FieldSummary { contains_null: false, contains_nan: Some(false), lower_bound: Some(Datum::long(1).to_bytes().unwrap()), upper_bound: Some(Datum::long(1).to_bytes().unwrap())}]
                ),
                key_metadata: None,
                first_row_id: None,
            }]
        };

        let temp_dir = TempDir::new().unwrap();
        let path = temp_dir.path().join("manifest_list_v1.avro");
        let io = FileIOBuilder::new_fs_io().build().unwrap();
        let output_file = io.new_output(path.to_str().unwrap()).unwrap();

        let mut writer = ManifestListWriter::v1(output_file, 1646658105718557341, Some(0));
        writer
            .add_manifests(expected_manifest_list.entries.clone().into_iter())
            .unwrap();
        writer.close().await.unwrap();

        let bs = fs::read(path).unwrap();

        let manifest_list =
            ManifestList::parse_with_version(&bs, crate::spec::FormatVersion::V3).unwrap();
        assert_eq!(manifest_list, expected_manifest_list);

        temp_dir.close().unwrap();
    }

    #[tokio::test]
    async fn test_manifest_list_writer_v2_as_v3() {
        let snapshot_id = 377075049360453639;
        let seq_num = 1;
        let mut expected_manifest_list = ManifestList {
            entries: vec![ManifestFile {
                manifest_path: "s3a://icebergdata/demo/s1/t1/metadata/05ffe08b-810f-49b3-a8f4-e88fc99b254a-m0.avro".to_string(),
                manifest_length: 6926,
                partition_spec_id: 1,
                content: ManifestContentType::Data,
                sequence_number: UNASSIGNED_SEQUENCE_NUMBER,
                min_sequence_number: UNASSIGNED_SEQUENCE_NUMBER,
                added_snapshot_id: snapshot_id,
                added_files_count: Some(1),
                existing_files_count: Some(0),
                deleted_files_count: Some(0),
                added_rows_count: Some(3),
                existing_rows_count: Some(0),
                deleted_rows_count: Some(0),
                partitions: Some(
                    vec![FieldSummary { contains_null: false, contains_nan: Some(false), lower_bound: Some(Datum::long(1).to_bytes().unwrap()), upper_bound: Some(Datum::long(1).to_bytes().unwrap())}]
                ),
                key_metadata: None,
                first_row_id: None,
            }]
        };

        let temp_dir = TempDir::new().unwrap();
        let path = temp_dir.path().join("manifest_list_v2.avro");
        let io = FileIOBuilder::new_fs_io().build().unwrap();
        let output_file = io.new_output(path.to_str().unwrap()).unwrap();

        let mut writer = ManifestListWriter::v2(output_file, snapshot_id, Some(0), seq_num);
        writer
            .add_manifests(expected_manifest_list.entries.clone().into_iter())
            .unwrap();
        writer.close().await.unwrap();

        let bs = fs::read(path).unwrap();

        let manifest_list =
            ManifestList::parse_with_version(&bs, crate::spec::FormatVersion::V3).unwrap();
        expected_manifest_list.entries[0].sequence_number = seq_num;
        expected_manifest_list.entries[0].min_sequence_number = seq_num;
        assert_eq!(manifest_list, expected_manifest_list);

        temp_dir.close().unwrap();
    }

    #[tokio::test]
    async fn test_manifest_list_v2_deserializer_aliases() {
        // reading avro manifest file generated by iceberg 1.4.0
        let avro_1_path = "testdata/manifests_lists/manifest-list-v2-1.avro";
        let bs_1 = fs::read(avro_1_path).unwrap();
        let avro_1_fields = read_avro_schema_fields_as_str(bs_1.clone()).await;
        assert_eq!(
            avro_1_fields,
            "manifest_path, manifest_length, partition_spec_id, content, sequence_number, min_sequence_number, added_snapshot_id, added_data_files_count, existing_data_files_count, deleted_data_files_count, added_rows_count, existing_rows_count, deleted_rows_count, partitions"
        );
        // reading avro manifest file generated by iceberg 1.5.0
        let avro_2_path = "testdata/manifests_lists/manifest-list-v2-2.avro";
        let bs_2 = fs::read(avro_2_path).unwrap();
        let avro_2_fields = read_avro_schema_fields_as_str(bs_2.clone()).await;
        assert_eq!(
            avro_2_fields,
            "manifest_path, manifest_length, partition_spec_id, content, sequence_number, min_sequence_number, added_snapshot_id, added_files_count, existing_files_count, deleted_files_count, added_rows_count, existing_rows_count, deleted_rows_count, partitions"
        );
        // deserializing both files to ManifestList struct
        let _manifest_list_1 =
            ManifestList::parse_with_version(&bs_1, crate::spec::FormatVersion::V2).unwrap();
        let _manifest_list_2 =
            ManifestList::parse_with_version(&bs_2, crate::spec::FormatVersion::V2).unwrap();
    }

    async fn read_avro_schema_fields_as_str(bs: Vec<u8>) -> String {
        let reader = Reader::new(&bs[..]).unwrap();
        let schema = reader.writer_schema();
        let fields: String = match schema {
            Schema::Record(record) => record
                .fields
                .iter()
                .map(|field| field.name.clone())
                .collect::<Vec<String>>()
                .join(", "),
            _ => "".to_string(),
        };
        fields
    }

    #[test]
    fn test_manifest_content_type_default() {
        assert_eq!(ManifestContentType::default(), ManifestContentType::Data);
    }

    #[test]
    fn test_manifest_content_type_default_value() {
        assert_eq!(ManifestContentType::default() as i32, 0);
    }

    #[test]
    fn test_manifest_file_v1_to_v2_projection() {
        use crate::spec::manifest_list::_serde::ManifestFileV1;

        // Create a V1 manifest file object (without V2 fields)
        let v1_manifest = ManifestFileV1 {
            manifest_path: "/test/manifest.avro".to_string(),
            manifest_length: 5806,
            partition_spec_id: 0,
            added_snapshot_id: 1646658105718557341,
            added_data_files_count: Some(3),
            existing_data_files_count: Some(0),
            deleted_data_files_count: Some(0),
            added_rows_count: Some(3),
            existing_rows_count: Some(0),
            deleted_rows_count: Some(0),
            partitions: None,
            key_metadata: None,
        };

        // Convert V1 to V2 - this should apply defaults for missing V2 fields
        let v2_manifest: ManifestFile = v1_manifest.try_into().unwrap();

        // Verify V1→V2 projection defaults are applied correctly
        assert_eq!(
            v2_manifest.content,
            ManifestContentType::Data,
            "V1 manifest content should default to Data (0)"
        );
        assert_eq!(
            v2_manifest.sequence_number, 0,
            "V1 manifest sequence_number should default to 0"
        );
        assert_eq!(
            v2_manifest.min_sequence_number, 0,
            "V1 manifest min_sequence_number should default to 0"
        );

        // Verify other fields are preserved correctly
        assert_eq!(v2_manifest.manifest_path, "/test/manifest.avro");
        assert_eq!(v2_manifest.manifest_length, 5806);
        assert_eq!(v2_manifest.partition_spec_id, 0);
        assert_eq!(v2_manifest.added_snapshot_id, 1646658105718557341);
        assert_eq!(v2_manifest.added_files_count, Some(3));
        assert_eq!(v2_manifest.existing_files_count, Some(0));
        assert_eq!(v2_manifest.deleted_files_count, Some(0));
        assert_eq!(v2_manifest.added_rows_count, Some(3));
        assert_eq!(v2_manifest.existing_rows_count, Some(0));
        assert_eq!(v2_manifest.deleted_rows_count, Some(0));
        assert_eq!(v2_manifest.partitions, None);
        assert_eq!(v2_manifest.key_metadata, None);
    }
}
