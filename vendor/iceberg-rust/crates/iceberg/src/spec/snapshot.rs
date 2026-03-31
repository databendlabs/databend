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

/*!
 * Snapshots
 */
use std::collections::HashMap;
use std::sync::Arc;

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use typed_builder::TypedBuilder;

use super::table_metadata::SnapshotLog;
use crate::error::{Result, timestamp_ms_to_utc};
use crate::io::FileIO;
use crate::spec::{ManifestList, SchemaId, SchemaRef, TableMetadata};
use crate::{Error, ErrorKind};

/// The ref name of the main branch of the table.
pub const MAIN_BRANCH: &str = "main";
/// Placeholder for snapshot ID. The field with this value must be replaced with the actual snapshot ID before it is committed.
pub const UNASSIGNED_SNAPSHOT_ID: i64 = -1;

/// Reference to [`Snapshot`].
pub type SnapshotRef = Arc<Snapshot>;
#[derive(Debug, Serialize, Deserialize, PartialEq, Eq, Clone)]
#[serde(rename_all = "lowercase")]
/// The operation field is used by some operations, like snapshot expiration, to skip processing certain snapshots.
pub enum Operation {
    /// Only data files were added and no files were removed.
    Append,
    /// Data and delete files were added and removed without changing table data;
    /// i.e., compaction, changing the data file format, or relocating data files.
    Replace,
    /// Data and delete files were added and removed in a logical overwrite operation.
    Overwrite,
    /// Data files were removed and their contents logically deleted and/or delete files were added to delete rows.
    Delete,
}

impl Operation {
    /// Returns the string representation (lowercase) of the operation.
    pub fn as_str(&self) -> &str {
        match self {
            Operation::Append => "append",
            Operation::Replace => "replace",
            Operation::Overwrite => "overwrite",
            Operation::Delete => "delete",
        }
    }
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Eq, Clone)]
/// Summarises the changes in the snapshot.
pub struct Summary {
    /// The type of operation in the snapshot
    pub operation: Operation,
    /// Other summary data.
    #[serde(flatten)]
    pub additional_properties: HashMap<String, String>,
}

impl Default for Operation {
    fn default() -> Operation {
        Self::Append
    }
}

#[derive(Debug, PartialEq, Eq, Clone)]
/// Row range of a snapshot, contains first_row_id and added_rows_count.
pub struct SnapshotRowRange {
    /// The first _row_id assigned to the first row in the first data file in the first manifest.
    pub first_row_id: u64,
    /// The upper bound of the number of rows with assigned row IDs
    pub added_rows: u64,
}

#[derive(Debug, PartialEq, Eq, Clone, TypedBuilder)]
#[builder(field_defaults(setter(prefix = "with_")))]
/// A snapshot represents the state of a table at some time and is used to access the complete set of data files in the table.
pub struct Snapshot {
    /// A unique long ID
    pub(crate) snapshot_id: i64,
    /// The snapshot ID of the snapshot’s parent.
    /// Omitted for any snapshot with no parent
    #[builder(default = None)]
    pub(crate) parent_snapshot_id: Option<i64>,
    /// A monotonically increasing long that tracks the order of
    /// changes to a table.
    pub(crate) sequence_number: i64,
    /// A timestamp when the snapshot was created, used for garbage
    /// collection and table inspection
    pub(crate) timestamp_ms: i64,
    /// The location of a manifest list for this snapshot that
    /// tracks manifest files with additional metadata.
    /// Currently we only support manifest list file, and manifest files are not supported.
    #[builder(setter(into))]
    pub(crate) manifest_list: String,
    /// A string map that summarizes the snapshot changes, including operation.
    pub(crate) summary: Summary,
    /// ID of the table’s current schema when the snapshot was created.
    #[builder(setter(strip_option(fallback = schema_id_opt)), default = None)]
    pub(crate) schema_id: Option<SchemaId>,
    /// Encryption Key ID
    #[builder(default)]
    pub(crate) encryption_key_id: Option<String>,
    /// Row range of this snapshot, required when the table version supports row lineage.
    /// Specify as a tuple of (first_row_id, added_rows_count)
    #[builder(default, setter(!strip_option, transform = |first_row_id: u64, added_rows: u64| Some(SnapshotRowRange { first_row_id, added_rows })))]
    // This is specified as a struct instead of two separate fields to ensure that both fields are either set or not set.
    // The java implementations uses two separate fields, then sets `added_row_counts` to Null if `first_row_id` is set to Null.
    // It throws an error if `added_row_counts` is set but `first_row_id` is not set, or if either of the two is negative.
    // We handle all cases infallible using the rust type system.
    pub(crate) row_range: Option<SnapshotRowRange>,
}

impl Snapshot {
    /// Get the id of the snapshot
    #[inline]
    pub fn snapshot_id(&self) -> i64 {
        self.snapshot_id
    }

    /// Get parent snapshot id.
    #[inline]
    pub fn parent_snapshot_id(&self) -> Option<i64> {
        self.parent_snapshot_id
    }

    /// Get sequence_number of the snapshot. Is 0 for Iceberg V1 tables.
    #[inline]
    pub fn sequence_number(&self) -> i64 {
        self.sequence_number
    }
    /// Get location of manifest_list file
    #[inline]
    pub fn manifest_list(&self) -> &str {
        &self.manifest_list
    }

    /// Get summary of the snapshot
    #[inline]
    pub fn summary(&self) -> &Summary {
        &self.summary
    }
    /// Get the timestamp of when the snapshot was created
    #[inline]
    pub fn timestamp(&self) -> Result<DateTime<Utc>> {
        timestamp_ms_to_utc(self.timestamp_ms)
    }

    /// Get the timestamp of when the snapshot was created in milliseconds
    #[inline]
    pub fn timestamp_ms(&self) -> i64 {
        self.timestamp_ms
    }

    /// Get the schema id of this snapshot.
    #[inline]
    pub fn schema_id(&self) -> Option<SchemaId> {
        self.schema_id
    }

    /// Get the schema of this snapshot.
    pub fn schema(&self, table_metadata: &TableMetadata) -> Result<SchemaRef> {
        Ok(match self.schema_id() {
            Some(schema_id) => table_metadata
                .schema_by_id(schema_id)
                .ok_or_else(|| {
                    Error::new(
                        ErrorKind::DataInvalid,
                        format!("Schema with id {schema_id} not found"),
                    )
                })?
                .clone(),
            None => table_metadata.current_schema().clone(),
        })
    }

    /// Get parent snapshot.
    #[cfg(test)]
    pub(crate) fn parent_snapshot(&self, table_metadata: &TableMetadata) -> Option<SnapshotRef> {
        match self.parent_snapshot_id {
            Some(id) => table_metadata.snapshot_by_id(id).cloned(),
            None => None,
        }
    }

    /// Load manifest list.
    pub async fn load_manifest_list(
        &self,
        file_io: &FileIO,
        table_metadata: &TableMetadata,
    ) -> Result<ManifestList> {
        let manifest_list_content = file_io.new_input(&self.manifest_list)?.read().await?;
        ManifestList::parse_with_version(
            &manifest_list_content,
            // TODO: You don't really need the version since you could just project any Avro in
            // the version that you'd like to get (probably always the latest)
            table_metadata.format_version(),
        )
    }

    #[allow(dead_code)]
    pub(crate) fn log(&self) -> SnapshotLog {
        SnapshotLog {
            timestamp_ms: self.timestamp_ms,
            snapshot_id: self.snapshot_id,
        }
    }

    /// The row-id of the first newly added row in this snapshot. All rows added in this snapshot will
    /// have a row-id assigned to them greater than this value. All rows with a row-id less than this
    /// value were created in a snapshot that was added to the table (but not necessarily committed to
    /// this branch) in the past.
    ///
    /// This field is optional but is required when the table version supports row lineage.
    pub fn first_row_id(&self) -> Option<u64> {
        self.row_range.as_ref().map(|r| r.first_row_id)
    }

    /// The total number of newly added rows in this snapshot. It should be the summation of {@link
    /// ManifestFile#ADDED_ROWS_COUNT} for every manifest added in this snapshot.
    ///
    /// This field is optional but is required when the table version supports row lineage.
    pub fn added_rows_count(&self) -> Option<u64> {
        self.row_range.as_ref().map(|r| r.added_rows)
    }

    /// Returns the row range of this snapshot, if available.
    /// This is a tuple containing (first_row_id, added_rows_count).
    pub fn row_range(&self) -> Option<(u64, u64)> {
        self.row_range
            .as_ref()
            .map(|r| (r.first_row_id, r.added_rows))
    }

    /// Get encryption key id, if available.
    pub fn encryption_key_id(&self) -> Option<&str> {
        self.encryption_key_id.as_deref()
    }
}

pub(super) mod _serde {
    /// This is a helper module that defines types to help with serialization/deserialization.
    /// For deserialization the input first gets read into either the [SnapshotV1] or [SnapshotV2] struct
    /// and then converted into the [Snapshot] struct. Serialization works the other way around.
    /// [SnapshotV1] and [SnapshotV2] are internal struct that are only used for serialization and deserialization.
    use std::collections::HashMap;

    use serde::{Deserialize, Serialize};

    use super::{Operation, Snapshot, Summary};
    use crate::spec::SchemaId;
    use crate::spec::snapshot::SnapshotRowRange;
    use crate::{Error, ErrorKind};

    #[derive(Debug, Serialize, Deserialize, PartialEq, Eq)]
    #[serde(rename_all = "kebab-case")]
    /// Defines the structure of a v2 snapshot for serialization/deserialization
    pub(crate) struct SnapshotV3 {
        pub snapshot_id: i64,
        #[serde(skip_serializing_if = "Option::is_none")]
        pub parent_snapshot_id: Option<i64>,
        pub sequence_number: i64,
        pub timestamp_ms: i64,
        pub manifest_list: String,
        pub summary: Summary,
        #[serde(skip_serializing_if = "Option::is_none")]
        pub schema_id: Option<SchemaId>,
        pub first_row_id: u64,
        pub added_rows: u64,
        #[serde(skip_serializing_if = "Option::is_none")]
        pub key_id: Option<String>,
    }

    #[derive(Debug, Serialize, Deserialize, PartialEq, Eq)]
    #[serde(rename_all = "kebab-case")]
    /// Defines the structure of a v2 snapshot for serialization/deserialization
    pub(crate) struct SnapshotV2 {
        pub snapshot_id: i64,
        #[serde(skip_serializing_if = "Option::is_none")]
        pub parent_snapshot_id: Option<i64>,
        pub sequence_number: i64,
        pub timestamp_ms: i64,
        pub manifest_list: String,
        pub summary: Summary,
        #[serde(skip_serializing_if = "Option::is_none")]
        pub schema_id: Option<SchemaId>,
    }

    #[derive(Debug, Serialize, Deserialize, PartialEq, Eq)]
    #[serde(rename_all = "kebab-case")]
    /// Defines the structure of a v1 snapshot for serialization/deserialization
    pub(crate) struct SnapshotV1 {
        pub snapshot_id: i64,
        #[serde(skip_serializing_if = "Option::is_none")]
        pub parent_snapshot_id: Option<i64>,
        pub timestamp_ms: i64,
        #[serde(skip_serializing_if = "Option::is_none")]
        pub manifest_list: Option<String>,
        #[serde(skip_serializing_if = "Option::is_none")]
        pub manifests: Option<Vec<String>>,
        #[serde(skip_serializing_if = "Option::is_none")]
        pub summary: Option<Summary>,
        #[serde(skip_serializing_if = "Option::is_none")]
        pub schema_id: Option<SchemaId>,
    }

    impl From<SnapshotV3> for Snapshot {
        fn from(s: SnapshotV3) -> Self {
            Snapshot {
                snapshot_id: s.snapshot_id,
                parent_snapshot_id: s.parent_snapshot_id,
                sequence_number: s.sequence_number,
                timestamp_ms: s.timestamp_ms,
                manifest_list: s.manifest_list,
                summary: s.summary,
                schema_id: s.schema_id,
                encryption_key_id: s.key_id,
                row_range: Some(SnapshotRowRange {
                    first_row_id: s.first_row_id,
                    added_rows: s.added_rows,
                }),
            }
        }
    }

    impl TryFrom<Snapshot> for SnapshotV3 {
        type Error = Error;

        fn try_from(s: Snapshot) -> Result<Self, Self::Error> {
            let row_range = s.row_range.ok_or_else(|| {
                Error::new(
                    crate::ErrorKind::DataInvalid,
                    "v3 Snapshots must have first-row-id and rows-added fields set.".to_string(),
                )
            })?;

            Ok(SnapshotV3 {
                snapshot_id: s.snapshot_id,
                parent_snapshot_id: s.parent_snapshot_id,
                sequence_number: s.sequence_number,
                timestamp_ms: s.timestamp_ms,
                manifest_list: s.manifest_list,
                summary: s.summary,
                schema_id: s.schema_id,
                first_row_id: row_range.first_row_id,
                added_rows: row_range.added_rows,
                key_id: s.encryption_key_id,
            })
        }
    }

    impl From<SnapshotV2> for Snapshot {
        fn from(v2: SnapshotV2) -> Self {
            Snapshot {
                snapshot_id: v2.snapshot_id,
                parent_snapshot_id: v2.parent_snapshot_id,
                sequence_number: v2.sequence_number,
                timestamp_ms: v2.timestamp_ms,
                manifest_list: v2.manifest_list,
                summary: v2.summary,
                schema_id: v2.schema_id,
                encryption_key_id: None,
                row_range: None,
            }
        }
    }

    impl From<Snapshot> for SnapshotV2 {
        fn from(v2: Snapshot) -> Self {
            SnapshotV2 {
                snapshot_id: v2.snapshot_id,
                parent_snapshot_id: v2.parent_snapshot_id,
                sequence_number: v2.sequence_number,
                timestamp_ms: v2.timestamp_ms,
                manifest_list: v2.manifest_list,
                summary: v2.summary,
                schema_id: v2.schema_id,
            }
        }
    }

    impl TryFrom<SnapshotV1> for Snapshot {
        type Error = Error;

        fn try_from(v1: SnapshotV1) -> Result<Self, Self::Error> {
            Ok(Snapshot {
                snapshot_id: v1.snapshot_id,
                parent_snapshot_id: v1.parent_snapshot_id,
                sequence_number: 0,
                timestamp_ms: v1.timestamp_ms,
                manifest_list: match (v1.manifest_list, v1.manifests) {
                    (Some(file), None) => file,
                    (Some(_), Some(_)) => {
                        return Err(Error::new(
                            ErrorKind::DataInvalid,
                            "Invalid v1 snapshot, when manifest list provided, manifest files should be omitted",
                        ));
                    }
                    (None, _) => {
                        return Err(Error::new(
                            ErrorKind::DataInvalid,
                            "Unsupported v1 snapshot, only manifest list is supported",
                        ));
                    }
                },
                summary: v1.summary.unwrap_or(Summary {
                    operation: Operation::default(),
                    additional_properties: HashMap::new(),
                }),
                schema_id: v1.schema_id,
                encryption_key_id: None,
                row_range: None,
            })
        }
    }

    impl From<Snapshot> for SnapshotV1 {
        fn from(v2: Snapshot) -> Self {
            SnapshotV1 {
                snapshot_id: v2.snapshot_id,
                parent_snapshot_id: v2.parent_snapshot_id,
                timestamp_ms: v2.timestamp_ms,
                manifest_list: Some(v2.manifest_list),
                summary: Some(v2.summary),
                schema_id: v2.schema_id,
                manifests: None,
            }
        }
    }
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Eq, Clone)]
#[serde(rename_all = "kebab-case")]
/// Iceberg tables keep track of branches and tags using snapshot references.
pub struct SnapshotReference {
    /// A reference’s snapshot ID. The tagged snapshot or latest snapshot of a branch.
    pub snapshot_id: i64,
    #[serde(flatten)]
    /// Snapshot retention policy
    pub retention: SnapshotRetention,
}

impl SnapshotReference {
    /// Returns true if the snapshot reference is a branch.
    pub fn is_branch(&self) -> bool {
        matches!(self.retention, SnapshotRetention::Branch { .. })
    }
}

impl SnapshotReference {
    /// Create new snapshot reference
    pub fn new(snapshot_id: i64, retention: SnapshotRetention) -> Self {
        SnapshotReference {
            snapshot_id,
            retention,
        }
    }
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Eq, Clone)]
#[serde(rename_all = "lowercase", tag = "type")]
/// The snapshot expiration procedure removes snapshots from table metadata and applies the table’s retention policy.
pub enum SnapshotRetention {
    #[serde(rename_all = "kebab-case")]
    /// Branches are mutable named references that can be updated by committing a new snapshot as
    /// the branch’s referenced snapshot using the Commit Conflict Resolution and Retry procedures.
    Branch {
        /// A positive number for the minimum number of snapshots to keep in a branch while expiring snapshots.
        /// Defaults to table property history.expire.min-snapshots-to-keep.
        #[serde(skip_serializing_if = "Option::is_none")]
        min_snapshots_to_keep: Option<i32>,
        /// A positive number for the max age of snapshots to keep when expiring, including the latest snapshot.
        /// Defaults to table property history.expire.max-snapshot-age-ms.
        #[serde(skip_serializing_if = "Option::is_none")]
        max_snapshot_age_ms: Option<i64>,
        /// For snapshot references except the main branch, a positive number for the max age of the snapshot reference to keep while expiring snapshots.
        /// Defaults to table property history.expire.max-ref-age-ms. The main branch never expires.
        #[serde(skip_serializing_if = "Option::is_none")]
        max_ref_age_ms: Option<i64>,
    },
    #[serde(rename_all = "kebab-case")]
    /// Tags are labels for individual snapshots.
    Tag {
        /// For snapshot references except the main branch, a positive number for the max age of the snapshot reference to keep while expiring snapshots.
        /// Defaults to table property history.expire.max-ref-age-ms. The main branch never expires.
        #[serde(skip_serializing_if = "Option::is_none")]
        max_ref_age_ms: Option<i64>,
    },
}

impl SnapshotRetention {
    /// Create a new branch retention policy
    pub fn branch(
        min_snapshots_to_keep: Option<i32>,
        max_snapshot_age_ms: Option<i64>,
        max_ref_age_ms: Option<i64>,
    ) -> Self {
        SnapshotRetention::Branch {
            min_snapshots_to_keep,
            max_snapshot_age_ms,
            max_ref_age_ms,
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use chrono::{TimeZone, Utc};

    use crate::spec::TableMetadata;
    use crate::spec::snapshot::_serde::SnapshotV1;
    use crate::spec::snapshot::{Operation, Snapshot, Summary};

    #[test]
    fn schema() {
        let record = r#"
        {
            "snapshot-id": 3051729675574597004,
            "timestamp-ms": 1515100955770,
            "summary": {
                "operation": "append"
            },
            "manifest-list": "s3://b/wh/.../s1.avro",
            "schema-id": 0
        }
        "#;

        let result: Snapshot = serde_json::from_str::<SnapshotV1>(record)
            .unwrap()
            .try_into()
            .unwrap();
        assert_eq!(3051729675574597004, result.snapshot_id());
        assert_eq!(
            Utc.timestamp_millis_opt(1515100955770).unwrap(),
            result.timestamp().unwrap()
        );
        assert_eq!(1515100955770, result.timestamp_ms());
        assert_eq!(
            Summary {
                operation: Operation::Append,
                additional_properties: HashMap::new()
            },
            *result.summary()
        );
        assert_eq!("s3://b/wh/.../s1.avro".to_string(), *result.manifest_list());
    }

    #[test]
    fn test_snapshot_v1_to_v2_projection() {
        use crate::spec::snapshot::_serde::SnapshotV1;

        // Create a V1 snapshot (without sequence-number field)
        let v1_snapshot = SnapshotV1 {
            snapshot_id: 1234567890,
            parent_snapshot_id: Some(987654321),
            timestamp_ms: 1515100955770,
            manifest_list: Some("s3://bucket/manifest-list.avro".to_string()),
            manifests: None, // V1 can have either manifest_list or manifests, but not both
            summary: Some(Summary {
                operation: Operation::Append,
                additional_properties: HashMap::from([
                    ("added-files".to_string(), "5".to_string()),
                    ("added-records".to_string(), "100".to_string()),
                ]),
            }),
            schema_id: Some(1),
        };

        // Convert V1 to V2 - this should apply defaults for missing V2 fields
        let v2_snapshot: Snapshot = v1_snapshot.try_into().unwrap();

        // Verify V1→V2 projection defaults are applied correctly
        assert_eq!(
            v2_snapshot.sequence_number(),
            0,
            "V1 snapshot sequence_number should default to 0"
        );

        // Verify other fields are preserved correctly during conversion
        assert_eq!(v2_snapshot.snapshot_id(), 1234567890);
        assert_eq!(v2_snapshot.parent_snapshot_id(), Some(987654321));
        assert_eq!(v2_snapshot.timestamp_ms(), 1515100955770);
        assert_eq!(
            v2_snapshot.manifest_list(),
            "s3://bucket/manifest-list.avro"
        );
        assert_eq!(v2_snapshot.schema_id(), Some(1));
        assert_eq!(v2_snapshot.summary().operation, Operation::Append);
        assert_eq!(
            v2_snapshot
                .summary()
                .additional_properties
                .get("added-files"),
            Some(&"5".to_string())
        );
    }

    #[test]
    fn test_v1_snapshot_with_manifest_list_and_manifests() {
        {
            let metadata = r#"
    {
        "format-version": 1,
        "table-uuid": "d20125c8-7284-442c-9aea-15fee620737c",
        "location": "s3://bucket/test/location",
        "last-updated-ms": 1700000000000,
        "last-column-id": 1,
        "schema": {
            "type": "struct",
            "fields": [
                {"id": 1, "name": "x", "required": true, "type": "long"}
            ]
        },
        "partition-spec": [],
        "properties": {},
        "current-snapshot-id": 111111111,
        "snapshots": [
            {
                "snapshot-id": 111111111,
                "timestamp-ms": 1600000000000,
                "summary": {"operation": "append"},
                "manifest-list": "s3://bucket/metadata/snap-123.avro",
                "manifests": ["s3://bucket/metadata/manifest-1.avro"]
            }
        ]
    }
    "#;

            let result_both_manifest_list_and_manifest_set =
                serde_json::from_str::<TableMetadata>(metadata);
            assert!(result_both_manifest_list_and_manifest_set.is_err());
            assert_eq!(
                result_both_manifest_list_and_manifest_set
                    .unwrap_err()
                    .to_string(),
                "DataInvalid => Invalid v1 snapshot, when manifest list provided, manifest files should be omitted"
            )
        }

        {
            let metadata = r#"
    {
        "format-version": 1,
        "table-uuid": "d20125c8-7284-442c-9aea-15fee620737c",
        "location": "s3://bucket/test/location",
        "last-updated-ms": 1700000000000,
        "last-column-id": 1,
        "schema": {
            "type": "struct",
            "fields": [
                {"id": 1, "name": "x", "required": true, "type": "long"}
            ]
        },
        "partition-spec": [],
        "properties": {},
        "current-snapshot-id": 111111111,
        "snapshots": [
            {
                "snapshot-id": 111111111,
                "timestamp-ms": 1600000000000,
                "summary": {"operation": "append"},
                "manifests": ["s3://bucket/metadata/manifest-1.avro"]
            }
        ]
    }
    "#;
            let result_missing_manifest_list = serde_json::from_str::<TableMetadata>(metadata);
            assert!(result_missing_manifest_list.is_err());
            assert_eq!(
                result_missing_manifest_list.unwrap_err().to_string(),
                "DataInvalid => Unsupported v1 snapshot, only manifest list is supported"
            )
        }
    }

    #[test]
    fn test_snapshot_v1_to_v2_with_missing_summary() {
        use crate::spec::snapshot::_serde::SnapshotV1;

        // Create a V1 snapshot without summary (should get default)
        let v1_snapshot = SnapshotV1 {
            snapshot_id: 1111111111,
            parent_snapshot_id: None,
            timestamp_ms: 1515100955770,
            manifest_list: Some("s3://bucket/manifest-list.avro".to_string()),
            manifests: None,
            summary: None, // V1 summary is optional
            schema_id: None,
        };

        // Convert V1 to V2 - this should apply default summary
        let v2_snapshot: Snapshot = v1_snapshot.try_into().unwrap();

        // Verify defaults are applied correctly
        assert_eq!(
            v2_snapshot.sequence_number(),
            0,
            "V1 snapshot sequence_number should default to 0"
        );
        assert_eq!(
            v2_snapshot.summary().operation,
            Operation::Append,
            "Missing V1 summary should default to Append operation"
        );
        assert!(
            v2_snapshot.summary().additional_properties.is_empty(),
            "Default summary should have empty additional_properties"
        );

        // Verify other fields
        assert_eq!(v2_snapshot.snapshot_id(), 1111111111);
        assert_eq!(v2_snapshot.parent_snapshot_id(), None);
        assert_eq!(v2_snapshot.schema_id(), None);
    }
}
