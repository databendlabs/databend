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

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use uuid::Uuid;

use super::{
    DEFAULT_PARTITION_SPEC_ID, DEFAULT_SCHEMA_ID, FormatVersion, MAIN_BRANCH, MetadataLog,
    ONE_MINUTE_MS, PartitionSpec, PartitionSpecBuilder, PartitionStatisticsFile, Schema, SchemaRef,
    Snapshot, SnapshotLog, SnapshotReference, SnapshotRetention, SortOrder, SortOrderRef,
    StatisticsFile, StructType, TableMetadata, TableProperties, UNPARTITIONED_LAST_ASSIGNED_ID,
    UnboundPartitionSpec,
};
use crate::error::{Error, ErrorKind, Result};
use crate::spec::{EncryptedKey, INITIAL_ROW_ID, MIN_FORMAT_VERSION_ROW_LINEAGE};
use crate::{TableCreation, TableUpdate};

pub(crate) const FIRST_FIELD_ID: i32 = 1;

/// Manipulating table metadata.
///
/// For this builder the order of called functions matters. Functions are applied in-order.
/// All operations applied to the `TableMetadata` are tracked in `changes` as  a chronologically
/// ordered vec of `TableUpdate`.
/// If an operation does not lead to a change of the `TableMetadata`, the corresponding update
/// is omitted from `changes`.
///
/// Unlike a typical builder pattern, the order of function calls matters.
/// Some basic rules:
/// - `add_schema` must be called before `set_current_schema`.
/// - If a new partition spec and schema are added, the schema should be added first.
#[derive(Debug, Clone)]
pub struct TableMetadataBuilder {
    metadata: TableMetadata,
    changes: Vec<TableUpdate>,
    last_added_schema_id: Option<i32>,
    last_added_spec_id: Option<i32>,
    last_added_order_id: Option<i64>,
    // None if this is a new table (from_metadata) method not used
    previous_history_entry: Option<MetadataLog>,
    last_updated_ms: Option<i64>,
}

#[derive(Debug, Clone, PartialEq)]
/// Result of modifying or creating a `TableMetadata`.
pub struct TableMetadataBuildResult {
    /// The new `TableMetadata`.
    pub metadata: TableMetadata,
    /// The changes that were applied to the metadata.
    pub changes: Vec<TableUpdate>,
    /// Expired metadata logs
    pub expired_metadata_logs: Vec<MetadataLog>,
}

impl TableMetadataBuilder {
    /// Proxy id for "last added" items, including schema, partition spec, sort order.
    pub const LAST_ADDED: i32 = -1;

    /// Create a `TableMetadata` object from scratch.
    ///
    /// This method re-assign ids of fields in the schema, schema.id, sort_order.id and
    /// spec.id. It should only be used to create new table metadata from scratch.
    pub fn new(
        schema: Schema,
        spec: impl Into<UnboundPartitionSpec>,
        sort_order: SortOrder,
        location: String,
        format_version: FormatVersion,
        properties: HashMap<String, String>,
    ) -> Result<Self> {
        // Re-assign field_ids, schema.id, sort_order.id and spec.id for a new table.
        let (fresh_schema, fresh_spec, fresh_sort_order) =
            Self::reassign_ids(schema, spec.into(), sort_order)?;
        let schema_id = fresh_schema.schema_id();

        let builder = Self {
            metadata: TableMetadata {
                format_version,
                table_uuid: Uuid::now_v7(),
                location: "".to_string(), // Overwritten immediately by set_location
                last_sequence_number: 0,
                last_updated_ms: 0,    // Overwritten by build() if not set before
                last_column_id: -1,    // Overwritten immediately by add_current_schema
                current_schema_id: -1, // Overwritten immediately by add_current_schema
                schemas: HashMap::new(),
                partition_specs: HashMap::new(),
                default_spec: Arc::new(
                    // The spec id (-1) is just a proxy value and can be any negative number.
                    // 0 would lead to wrong changes in the builder if the provided spec by the user is
                    // also unpartitioned.
                    // The `default_spec` value is always replaced at the end of this method by he `add_default_partition_spec`
                    // method.
                    PartitionSpec::unpartition_spec().with_spec_id(-1),
                ), // Overwritten immediately by add_default_partition_spec
                default_partition_type: StructType::new(vec![]),
                last_partition_id: UNPARTITIONED_LAST_ASSIGNED_ID,
                properties: HashMap::new(),
                current_snapshot_id: None,
                snapshots: HashMap::new(),
                snapshot_log: vec![],
                sort_orders: HashMap::new(),
                metadata_log: vec![],
                default_sort_order_id: -1, // Overwritten immediately by add_default_sort_order
                refs: HashMap::default(),
                statistics: HashMap::new(),
                partition_statistics: HashMap::new(),
                encryption_keys: HashMap::new(),
                next_row_id: INITIAL_ROW_ID,
            },
            last_updated_ms: None,
            changes: vec![],
            last_added_schema_id: Some(schema_id),
            last_added_spec_id: None,
            last_added_order_id: None,
            previous_history_entry: None,
        };

        builder
            .set_location(location)
            .add_current_schema(fresh_schema)?
            .add_default_partition_spec(fresh_spec.into_unbound())?
            .add_default_sort_order(fresh_sort_order)?
            .set_properties(properties)
    }

    /// Creates a new table metadata builder from the given metadata to modify it.
    /// `current_file_location` is the location where the current version
    /// of the metadata file is stored. This is used to update the metadata log.
    /// If `current_file_location` is `None`, the metadata log will not be updated.
    /// This should only be used to stage-create tables.
    #[must_use]
    pub fn new_from_metadata(
        previous: TableMetadata,
        current_file_location: Option<String>,
    ) -> Self {
        Self {
            previous_history_entry: current_file_location.map(|l| MetadataLog {
                metadata_file: l,
                timestamp_ms: previous.last_updated_ms,
            }),
            metadata: previous,
            changes: Vec::default(),
            last_added_schema_id: None,
            last_added_spec_id: None,
            last_added_order_id: None,
            last_updated_ms: None,
        }
    }

    /// Creates a new table metadata builder from the given table creation.
    pub fn from_table_creation(table_creation: TableCreation) -> Result<Self> {
        let TableCreation {
            name: _,
            location,
            schema,
            partition_spec,
            sort_order,
            properties,
            format_version,
        } = table_creation;

        let location = location.ok_or_else(|| {
            Error::new(
                ErrorKind::DataInvalid,
                "Can't create table without location",
            )
        })?;
        let partition_spec = partition_spec.unwrap_or(UnboundPartitionSpec {
            spec_id: None,
            fields: vec![],
        });

        Self::new(
            schema,
            partition_spec,
            sort_order.unwrap_or(SortOrder::unsorted_order()),
            location,
            format_version,
            properties,
        )
    }

    /// Changes uuid of table metadata.
    pub fn assign_uuid(mut self, uuid: Uuid) -> Self {
        if self.metadata.table_uuid != uuid {
            self.metadata.table_uuid = uuid;
            self.changes.push(TableUpdate::AssignUuid { uuid });
        }

        self
    }

    /// Upgrade `FormatVersion`. Downgrades are not allowed.
    ///
    /// # Errors
    /// - Cannot downgrade to older format versions.
    pub fn upgrade_format_version(mut self, format_version: FormatVersion) -> Result<Self> {
        if format_version < self.metadata.format_version {
            return Err(Error::new(
                ErrorKind::DataInvalid,
                format!(
                    "Cannot downgrade FormatVersion from {} to {}",
                    self.metadata.format_version, format_version
                ),
            ));
        }

        if format_version != self.metadata.format_version {
            match format_version {
                FormatVersion::V1 => {
                    // No changes needed for V1
                }
                FormatVersion::V2 => {
                    self.metadata.format_version = format_version;
                    self.changes
                        .push(TableUpdate::UpgradeFormatVersion { format_version });
                }
                FormatVersion::V3 => {
                    self.metadata.format_version = format_version;
                    self.changes
                        .push(TableUpdate::UpgradeFormatVersion { format_version });
                }
            }
        }

        Ok(self)
    }

    /// Set properties. If a property already exists, it will be overwritten.
    ///
    /// If a reserved property is set, the corresponding action is performed and the property is not persisted.
    /// Currently the following reserved properties are supported:
    /// * format-version: Set the format version of the table.
    ///
    /// # Errors
    /// - If properties contains a reserved property
    pub fn set_properties(mut self, properties: HashMap<String, String>) -> Result<Self> {
        // List of specified properties that are RESERVED and should not be persisted.
        let reserved_properties = properties
            .keys()
            .filter(|key| TableProperties::RESERVED_PROPERTIES.contains(&key.as_str()))
            .map(ToString::to_string)
            .collect::<Vec<_>>();

        if !reserved_properties.is_empty() {
            return Err(Error::new(
                ErrorKind::DataInvalid,
                format!(
                    "Table properties should not contain reserved properties, but got: [{}]",
                    reserved_properties.join(", ")
                ),
            ));
        }

        if properties.is_empty() {
            return Ok(self);
        }

        self.metadata.properties.extend(properties.clone());
        self.changes.push(TableUpdate::SetProperties {
            updates: properties,
        });

        Ok(self)
    }

    /// Remove properties from the table metadata.
    /// Does nothing if the key is not present.
    ///
    /// # Errors
    /// - If properties to remove contains a reserved property
    pub fn remove_properties(mut self, properties: &[String]) -> Result<Self> {
        // remove duplicates
        let properties = properties.iter().cloned().collect::<HashSet<_>>();

        // disallow removal of reserved properties
        let reserved_properties = properties
            .iter()
            .filter(|key| TableProperties::RESERVED_PROPERTIES.contains(&key.as_str()))
            .map(ToString::to_string)
            .collect::<Vec<_>>();

        if !reserved_properties.is_empty() {
            return Err(Error::new(
                ErrorKind::DataInvalid,
                format!(
                    "Table properties to remove contain reserved properties: [{}]",
                    reserved_properties.join(", ")
                ),
            ));
        }

        for property in &properties {
            self.metadata.properties.remove(property);
        }

        if !properties.is_empty() {
            self.changes.push(TableUpdate::RemoveProperties {
                removals: properties.into_iter().collect(),
            });
        }

        Ok(self)
    }

    /// Set the location of the table, stripping any trailing slashes.
    pub fn set_location(mut self, location: String) -> Self {
        let location = location.trim_end_matches('/').to_string();
        if self.metadata.location != location {
            self.changes.push(TableUpdate::SetLocation {
                location: location.clone(),
            });
            self.metadata.location = location;
        }

        self
    }

    /// Add a snapshot to the table metadata.
    ///
    /// # Errors
    /// - Snapshot id already exists.
    /// - For format version > 1: the sequence number of the snapshot is lower than the highest sequence number specified so far.
    /// - For format version >= 3: the first-row-id of the snapshot is lower than the next-row-id of the table.
    /// - For format version >= 3: added-rows is null or first-row-id is null.
    /// - For format version >= 3: next-row-id would overflow when adding added-rows.
    pub fn add_snapshot(mut self, snapshot: Snapshot) -> Result<Self> {
        if self
            .metadata
            .snapshots
            .contains_key(&snapshot.snapshot_id())
        {
            return Err(Error::new(
                ErrorKind::DataInvalid,
                format!("Snapshot already exists for: '{}'", snapshot.snapshot_id()),
            ));
        }

        if self.metadata.format_version != FormatVersion::V1
            && snapshot.sequence_number() <= self.metadata.last_sequence_number
            && snapshot.parent_snapshot_id().is_some()
        {
            return Err(Error::new(
                ErrorKind::DataInvalid,
                format!(
                    "Cannot add snapshot with sequence number {} older than last sequence number {}",
                    snapshot.sequence_number(),
                    self.metadata.last_sequence_number
                ),
            ));
        }

        if let Some(last) = self.metadata.snapshot_log.last() {
            // commits can happen concurrently from different machines.
            // A tolerance helps us avoid failure for small clock skew
            if snapshot.timestamp_ms() - last.timestamp_ms < -ONE_MINUTE_MS {
                return Err(Error::new(
                    ErrorKind::DataInvalid,
                    format!(
                        "Invalid snapshot timestamp {}: before last snapshot timestamp {}",
                        snapshot.timestamp_ms(),
                        last.timestamp_ms
                    ),
                ));
            }
        }

        let max_last_updated = self
            .last_updated_ms
            .unwrap_or_default()
            .max(self.metadata.last_updated_ms);
        if snapshot.timestamp_ms() - max_last_updated < -ONE_MINUTE_MS {
            return Err(Error::new(
                ErrorKind::DataInvalid,
                format!(
                    "Invalid snapshot timestamp {}: before last updated timestamp {}",
                    snapshot.timestamp_ms(),
                    max_last_updated
                ),
            ));
        }

        let mut added_rows = None;
        if self.metadata.format_version >= MIN_FORMAT_VERSION_ROW_LINEAGE {
            if let Some((first_row_id, added_rows_count)) = snapshot.row_range() {
                if first_row_id < self.metadata.next_row_id {
                    return Err(Error::new(
                        ErrorKind::DataInvalid,
                        format!(
                            "Cannot add a snapshot, first-row-id is behind table next-row-id: {first_row_id} < {}",
                            self.metadata.next_row_id
                        ),
                    ));
                }

                added_rows = Some(added_rows_count);
            } else {
                return Err(Error::new(
                    ErrorKind::DataInvalid,
                    format!(
                        "Cannot add a snapshot: first-row-id is null. first-row-id must be set for format version >= {MIN_FORMAT_VERSION_ROW_LINEAGE}",
                    ),
                ));
            }
        }

        if let Some(added_rows) = added_rows {
            self.metadata.next_row_id = self
                .metadata
                .next_row_id
                .checked_add(added_rows)
                .ok_or_else(|| {
                    Error::new(
                        ErrorKind::DataInvalid,
                        "Cannot add snapshot: next-row-id overflowed when adding added-rows",
                    )
                })?;
        }

        // Mutation happens in next line - must be infallible from here
        self.changes.push(TableUpdate::AddSnapshot {
            snapshot: snapshot.clone(),
        });

        self.last_updated_ms = Some(snapshot.timestamp_ms());
        self.metadata.last_sequence_number = snapshot.sequence_number();
        self.metadata
            .snapshots
            .insert(snapshot.snapshot_id(), snapshot.into());

        Ok(self)
    }

    /// Append a snapshot to the specified branch.
    /// Retention settings from the `branch` are re-used.
    ///
    /// # Errors
    /// - Any of the preconditions of `self.add_snapshot` are not met.
    pub fn set_branch_snapshot(self, snapshot: Snapshot, branch: &str) -> Result<Self> {
        let reference = self.metadata.refs.get(branch).cloned();

        let reference = if let Some(mut reference) = reference {
            if !reference.is_branch() {
                return Err(Error::new(
                    ErrorKind::DataInvalid,
                    format!("Cannot append snapshot to non-branch reference '{branch}'",),
                ));
            }

            reference.snapshot_id = snapshot.snapshot_id();
            reference
        } else {
            SnapshotReference {
                snapshot_id: snapshot.snapshot_id(),
                retention: SnapshotRetention::Branch {
                    min_snapshots_to_keep: None,
                    max_snapshot_age_ms: None,
                    max_ref_age_ms: None,
                },
            }
        };

        self.add_snapshot(snapshot)?.set_ref(branch, reference)
    }

    /// Remove snapshots by its ids from the table metadata.
    /// Does nothing if a snapshot id is not present.
    /// Keeps as changes only the snapshots that were actually removed.
    pub fn remove_snapshots(mut self, snapshot_ids: &[i64]) -> Self {
        let mut removed_snapshots = Vec::with_capacity(snapshot_ids.len());

        self.metadata.snapshots.retain(|k, _| {
            if snapshot_ids.contains(k) {
                removed_snapshots.push(*k);
                false
            } else {
                true
            }
        });

        if !removed_snapshots.is_empty() {
            self.changes.push(TableUpdate::RemoveSnapshots {
                snapshot_ids: removed_snapshots,
            });
        }

        // Remove refs that are no longer valid
        self.metadata
            .refs
            .retain(|_, v| self.metadata.snapshots.contains_key(&v.snapshot_id));

        self
    }

    /// Set a reference to a snapshot.
    ///
    /// # Errors
    /// - The snapshot id is unknown.
    pub fn set_ref(mut self, ref_name: &str, reference: SnapshotReference) -> Result<Self> {
        if self
            .metadata
            .refs
            .get(ref_name)
            .is_some_and(|snap_ref| snap_ref.eq(&reference))
        {
            return Ok(self);
        }

        let Some(snapshot) = self.metadata.snapshots.get(&reference.snapshot_id) else {
            return Err(Error::new(
                ErrorKind::DataInvalid,
                format!(
                    "Cannot set '{ref_name}' to unknown snapshot: '{}'",
                    reference.snapshot_id
                ),
            ));
        };

        // Update last_updated_ms to the exact timestamp of the snapshot if it was added in this commit
        let is_added_snapshot = self.changes.iter().any(|update| {
            matches!(update, TableUpdate::AddSnapshot { snapshot: snap } if snap.snapshot_id() == snapshot.snapshot_id())
        });
        if is_added_snapshot {
            self.last_updated_ms = Some(snapshot.timestamp_ms());
        }

        // Current snapshot id is set only for the main branch
        if ref_name == MAIN_BRANCH {
            self.metadata.current_snapshot_id = Some(snapshot.snapshot_id());
            let timestamp_ms = if let Some(last_updated_ms) = self.last_updated_ms {
                last_updated_ms
            } else {
                let last_updated_ms = chrono::Utc::now().timestamp_millis();
                self.last_updated_ms = Some(last_updated_ms);
                last_updated_ms
            };

            self.metadata.snapshot_log.push(SnapshotLog {
                snapshot_id: snapshot.snapshot_id(),
                timestamp_ms,
            });
        }

        self.changes.push(TableUpdate::SetSnapshotRef {
            ref_name: ref_name.to_string(),
            reference: reference.clone(),
        });
        self.metadata.refs.insert(ref_name.to_string(), reference);

        Ok(self)
    }

    /// Remove a reference
    ///
    /// If `ref_name='main'` the current snapshot id is set to -1.
    pub fn remove_ref(mut self, ref_name: &str) -> Self {
        if ref_name == MAIN_BRANCH {
            self.metadata.current_snapshot_id = None;
        }

        if self.metadata.refs.remove(ref_name).is_some() || ref_name == MAIN_BRANCH {
            self.changes.push(TableUpdate::RemoveSnapshotRef {
                ref_name: ref_name.to_string(),
            });
        }

        self
    }

    /// Set statistics for a snapshot
    pub fn set_statistics(mut self, statistics: StatisticsFile) -> Self {
        self.metadata
            .statistics
            .insert(statistics.snapshot_id, statistics.clone());
        self.changes.push(TableUpdate::SetStatistics {
            statistics: statistics.clone(),
        });
        self
    }

    /// Remove statistics for a snapshot
    pub fn remove_statistics(mut self, snapshot_id: i64) -> Self {
        let previous = self.metadata.statistics.remove(&snapshot_id);
        if previous.is_some() {
            self.changes
                .push(TableUpdate::RemoveStatistics { snapshot_id });
        }
        self
    }

    /// Set partition statistics
    pub fn set_partition_statistics(
        mut self,
        partition_statistics_file: PartitionStatisticsFile,
    ) -> Self {
        self.metadata.partition_statistics.insert(
            partition_statistics_file.snapshot_id,
            partition_statistics_file.clone(),
        );
        self.changes.push(TableUpdate::SetPartitionStatistics {
            partition_statistics: partition_statistics_file,
        });
        self
    }

    /// Remove partition statistics
    pub fn remove_partition_statistics(mut self, snapshot_id: i64) -> Self {
        let previous = self.metadata.partition_statistics.remove(&snapshot_id);
        if previous.is_some() {
            self.changes
                .push(TableUpdate::RemovePartitionStatistics { snapshot_id });
        }
        self
    }

    /// Add a schema to the table metadata.
    ///
    /// The provided `schema.schema_id` may not be used.
    ///
    /// Important: Use this method with caution. The builder does not check
    /// if the added schema is compatible with the current schema.
    pub fn add_schema(mut self, schema: Schema) -> Result<Self> {
        // Validate that new schema fields don't conflict with existing partition field names
        self.validate_schema_field_names(&schema)?;

        let new_schema_id = self.reuse_or_create_new_schema_id(&schema);
        let schema_found = self.metadata.schemas.contains_key(&new_schema_id);

        if schema_found {
            if self.last_added_schema_id != Some(new_schema_id) {
                self.changes.push(TableUpdate::AddSchema {
                    schema: schema.clone(),
                });
                self.last_added_schema_id = Some(new_schema_id);
            }

            return Ok(self);
        }

        // New schemas might contain only old columns. In this case last_column_id should not be
        // reduced.
        self.metadata.last_column_id =
            std::cmp::max(self.metadata.last_column_id, schema.highest_field_id());

        // Set schema-id
        let schema = match new_schema_id == schema.schema_id() {
            true => schema,
            false => schema.with_schema_id(new_schema_id),
        };

        self.metadata
            .schemas
            .insert(new_schema_id, schema.clone().into());

        self.changes.push(TableUpdate::AddSchema { schema });

        self.last_added_schema_id = Some(new_schema_id);

        Ok(self)
    }

    /// Set the current schema id.
    ///
    /// If `schema_id` is -1, the last added schema is set as the current schema.
    ///
    /// Errors:
    /// - provided `schema_id` is -1 but no schema has been added via `add_schema`.
    /// - No schema with the provided `schema_id` exists.
    pub fn set_current_schema(mut self, mut schema_id: i32) -> Result<Self> {
        if schema_id == Self::LAST_ADDED {
            schema_id = self.last_added_schema_id.ok_or_else(|| {
                Error::new(
                    ErrorKind::DataInvalid,
                    "Cannot set current schema to last added schema: no schema has been added.",
                )
            })?;
        };
        let schema_id = schema_id; // Make immutable

        if schema_id == self.metadata.current_schema_id {
            return Ok(self);
        }

        let _schema = self.metadata.schemas.get(&schema_id).ok_or_else(|| {
            Error::new(
                ErrorKind::DataInvalid,
                format!("Cannot set current schema to unknown schema with id: '{schema_id}'"),
            )
        })?;

        // Old partition specs and sort-orders should be preserved even if they are not compatible with the new schema,
        // so that older metadata can still be interpreted.
        // Default partition spec and sort order are checked in the build() method
        // which allows other default partition specs and sort orders to be set before the build.

        self.metadata.current_schema_id = schema_id;

        if self.last_added_schema_id == Some(schema_id) {
            self.changes.push(TableUpdate::SetCurrentSchema {
                schema_id: Self::LAST_ADDED,
            });
        } else {
            self.changes
                .push(TableUpdate::SetCurrentSchema { schema_id });
        }

        Ok(self)
    }

    /// Add a schema and set it as the current schema.
    pub fn add_current_schema(self, schema: Schema) -> Result<Self> {
        self.add_schema(schema)?
            .set_current_schema(Self::LAST_ADDED)
    }

    /// Validate schema field names against partition field names across all historical schemas.
    ///
    /// Due to Iceberg's multi-version property, this check ignores existing schema fields
    /// that match partition names (schema evolution allows re-adding previously removed fields).
    /// Only NEW field names that conflict with partition names are rejected.
    ///
    /// # Errors
    /// - Schema field name conflicts with partition field name but doesn't exist in any historical schema.
    fn validate_schema_field_names(&self, schema: &Schema) -> Result<()> {
        if self.metadata.schemas.is_empty() {
            return Ok(());
        }

        for field_name in schema.field_id_to_name_map().values() {
            let has_partition_conflict = self.metadata.partition_name_exists(field_name);
            let is_new_field = !self.metadata.name_exists_in_any_schema(field_name);

            if has_partition_conflict && is_new_field {
                return Err(Error::new(
                    ErrorKind::DataInvalid,
                    format!(
                        "Cannot add schema field '{field_name}' because it conflicts with existing partition field name. \
                         Schema evolution cannot introduce field names that match existing partition field names."
                    ),
                ));
            }
        }

        Ok(())
    }

    /// Validate partition field names against schema field names across all historical schemas.
    ///
    /// Due to Iceberg's multi-version property, partition fields can share names with schema fields
    /// if they meet specific requirements (identity transform + matching source field ID).
    /// This validation enforces those rules across all historical schema versions.
    ///
    /// # Errors
    /// - Partition field name conflicts with schema field name but doesn't use identity transform.
    /// - Partition field uses identity transform but references wrong source field ID.
    fn validate_partition_field_names(&self, unbound_spec: &UnboundPartitionSpec) -> Result<()> {
        if self.metadata.schemas.is_empty() {
            return Ok(());
        }

        let current_schema = self.get_current_schema()?;
        for partition_field in unbound_spec.fields() {
            let exists_in_any_schema = self
                .metadata
                .name_exists_in_any_schema(&partition_field.name);

            // Skip if partition field name doesn't conflict with any schema field
            if !exists_in_any_schema {
                continue;
            }

            // If name exists in schemas, validate against current schema rules
            if let Some(schema_field) = current_schema.field_by_name(&partition_field.name) {
                let is_identity_transform =
                    partition_field.transform == crate::spec::Transform::Identity;
                let has_matching_source_id = schema_field.id == partition_field.source_id;

                if !is_identity_transform {
                    return Err(Error::new(
                        ErrorKind::DataInvalid,
                        format!(
                            "Cannot create partition with name '{}' that conflicts with schema field and is not an identity transform.",
                            partition_field.name
                        ),
                    ));
                }

                if !has_matching_source_id {
                    return Err(Error::new(
                        ErrorKind::DataInvalid,
                        format!(
                            "Cannot create identity partition sourced from different field in schema. \
                             Field name '{}' has id `{}` in schema but partition source id is `{}`",
                            partition_field.name, schema_field.id, partition_field.source_id
                        ),
                    ));
                }
            }
        }

        Ok(())
    }

    /// Add a partition spec to the table metadata.
    ///
    /// The spec is bound eagerly to the current schema.
    /// If a schema is added in the same set of changes, the schema should be added first.
    ///
    /// Even if `unbound_spec.spec_id` is provided as `Some`, it may not be used.
    ///
    /// # Errors
    /// - The partition spec cannot be bound to the current schema.
    /// - The partition spec has non-sequential field ids and the table format version is 1.
    pub fn add_partition_spec(mut self, unbound_spec: UnboundPartitionSpec) -> Result<Self> {
        let schema = self.get_current_schema()?.clone();

        // Check if partition field names conflict with schema field names across all schemas
        self.validate_partition_field_names(&unbound_spec)?;

        let spec = PartitionSpecBuilder::new_from_unbound(unbound_spec.clone(), schema)?
            .with_last_assigned_field_id(self.metadata.last_partition_id)
            .build()?;

        let new_spec_id = self.reuse_or_create_new_spec_id(&spec);
        let spec_found = self.metadata.partition_specs.contains_key(&new_spec_id);
        let spec = spec.with_spec_id(new_spec_id);
        let unbound_spec = unbound_spec.with_spec_id(new_spec_id);

        if spec_found {
            if self.last_added_spec_id != Some(new_spec_id) {
                self.changes
                    .push(TableUpdate::AddSpec { spec: unbound_spec });
                self.last_added_spec_id = Some(new_spec_id);
            }

            return Ok(self);
        }

        if self.metadata.format_version <= FormatVersion::V1 && !spec.has_sequential_ids() {
            return Err(Error::new(
                ErrorKind::DataInvalid,
                "Cannot add partition spec with non-sequential field ids to format version 1 table",
            ));
        }

        let highest_field_id = spec
            .highest_field_id()
            .unwrap_or(UNPARTITIONED_LAST_ASSIGNED_ID);
        self.metadata
            .partition_specs
            .insert(new_spec_id, Arc::new(spec));
        self.changes
            .push(TableUpdate::AddSpec { spec: unbound_spec });

        self.last_added_spec_id = Some(new_spec_id);
        self.metadata.last_partition_id =
            std::cmp::max(self.metadata.last_partition_id, highest_field_id);

        Ok(self)
    }

    /// Set the default partition spec.
    ///
    /// # Errors
    /// - spec_id is -1 but no spec has been added via `add_partition_spec`.
    /// - No partition spec with the provided `spec_id` exists.
    pub fn set_default_partition_spec(mut self, mut spec_id: i32) -> Result<Self> {
        if spec_id == Self::LAST_ADDED {
            spec_id = self.last_added_spec_id.ok_or_else(|| {
                Error::new(
                    ErrorKind::DataInvalid,
                    "Cannot set default partition spec to last added spec: no spec has been added.",
                )
            })?;
        }

        if self.metadata.default_spec.spec_id() == spec_id {
            return Ok(self);
        }

        if !self.metadata.partition_specs.contains_key(&spec_id) {
            return Err(Error::new(
                ErrorKind::DataInvalid,
                format!("Cannot set default partition spec to unknown spec with id: '{spec_id}'",),
            ));
        }

        let schemaless_spec = self
            .metadata
            .partition_specs
            .get(&spec_id)
            .ok_or_else(|| {
                Error::new(
                    ErrorKind::DataInvalid,
                    format!(
                        "Cannot set default partition spec to unknown spec with id: '{spec_id}'",
                    ),
                )
            })?
            .clone();
        let spec = Arc::unwrap_or_clone(schemaless_spec);
        let spec_type = spec.partition_type(self.get_current_schema()?)?;
        self.metadata.default_spec = Arc::new(spec);
        self.metadata.default_partition_type = spec_type;

        if self.last_added_spec_id == Some(spec_id) {
            self.changes.push(TableUpdate::SetDefaultSpec {
                spec_id: Self::LAST_ADDED,
            });
        } else {
            self.changes.push(TableUpdate::SetDefaultSpec { spec_id });
        }

        Ok(self)
    }

    /// Add a partition spec and set it as the default
    pub fn add_default_partition_spec(self, unbound_spec: UnboundPartitionSpec) -> Result<Self> {
        self.add_partition_spec(unbound_spec)?
            .set_default_partition_spec(Self::LAST_ADDED)
    }

    /// Remove partition specs by their ids from the table metadata.
    /// Does nothing if a spec id is not present. Active partition specs
    /// should not be removed.
    ///
    /// # Errors
    /// - Cannot remove the default partition spec.
    pub fn remove_partition_specs(mut self, spec_ids: &[i32]) -> Result<Self> {
        if spec_ids.contains(&self.metadata.default_spec.spec_id()) {
            return Err(Error::new(
                ErrorKind::DataInvalid,
                "Cannot remove default partition spec",
            ));
        }

        let mut removed_specs = Vec::with_capacity(spec_ids.len());
        spec_ids.iter().for_each(|id| {
            if self.metadata.partition_specs.remove(id).is_some() {
                removed_specs.push(*id);
            }
        });

        if !removed_specs.is_empty() {
            self.changes.push(TableUpdate::RemovePartitionSpecs {
                spec_ids: removed_specs,
            });
        }

        Ok(self)
    }

    /// Add a sort order to the table metadata.
    ///
    /// The spec is bound eagerly to the current schema and must be valid for it.
    /// If a schema is added in the same set of changes, the schema should be added first.
    ///
    /// Even if `sort_order.order_id` is provided, it may not be used.
    ///
    /// # Errors
    /// - Sort order id to add already exists.
    /// - Sort order is incompatible with the current schema.
    pub fn add_sort_order(mut self, sort_order: SortOrder) -> Result<Self> {
        let new_order_id = self.reuse_or_create_new_sort_id(&sort_order);
        let sort_order_found = self.metadata.sort_orders.contains_key(&new_order_id);

        if sort_order_found {
            if self.last_added_order_id != Some(new_order_id) {
                self.changes.push(TableUpdate::AddSortOrder {
                    sort_order: sort_order.clone().with_order_id(new_order_id),
                });
                self.last_added_order_id = Some(new_order_id);
            }

            return Ok(self);
        }

        let schema = self.get_current_schema()?.clone().as_ref().clone();
        let sort_order = SortOrder::builder()
            .with_order_id(new_order_id)
            .with_fields(sort_order.fields)
            .build(&schema)
            .map_err(|e| {
                Error::new(
                    ErrorKind::DataInvalid,
                    format!("Sort order to add is incompatible with current schema: {e}"),
                )
                .with_source(e)
            })?;

        self.last_added_order_id = Some(new_order_id);
        self.metadata
            .sort_orders
            .insert(new_order_id, sort_order.clone().into());
        self.changes.push(TableUpdate::AddSortOrder { sort_order });

        Ok(self)
    }

    /// Set the default sort order. If `sort_order_id` is -1, the last added sort order is set as default.
    ///
    /// # Errors
    /// - sort_order_id is -1 but no sort order has been added via `add_sort_order`.
    /// - No sort order with the provided `sort_order_id` exists.
    pub fn set_default_sort_order(mut self, mut sort_order_id: i64) -> Result<Self> {
        if sort_order_id == Self::LAST_ADDED as i64 {
            sort_order_id = self.last_added_order_id.ok_or_else(|| {
                Error::new(
                    ErrorKind::DataInvalid,
                    "Cannot set default sort order to last added order: no order has been added.",
                )
            })?;
        }

        if self.metadata.default_sort_order_id == sort_order_id {
            return Ok(self);
        }

        if !self.metadata.sort_orders.contains_key(&sort_order_id) {
            return Err(Error::new(
                ErrorKind::DataInvalid,
                format!(
                    "Cannot set default sort order to unknown order with id: '{sort_order_id}'"
                ),
            ));
        }

        self.metadata.default_sort_order_id = sort_order_id;

        if self.last_added_order_id == Some(sort_order_id) {
            self.changes.push(TableUpdate::SetDefaultSortOrder {
                sort_order_id: Self::LAST_ADDED as i64,
            });
        } else {
            self.changes
                .push(TableUpdate::SetDefaultSortOrder { sort_order_id });
        }

        Ok(self)
    }

    /// Add a sort order and set it as the default
    fn add_default_sort_order(self, sort_order: SortOrder) -> Result<Self> {
        self.add_sort_order(sort_order)?
            .set_default_sort_order(Self::LAST_ADDED as i64)
    }

    /// Add an encryption key to the table metadata.
    pub fn add_encryption_key(mut self, key: EncryptedKey) -> Self {
        let key_id = key.key_id().to_string();
        if self.metadata.encryption_keys.contains_key(&key_id) {
            // already exists
            return self;
        }

        self.metadata.encryption_keys.insert(key_id, key.clone());
        self.changes.push(TableUpdate::AddEncryptionKey {
            encryption_key: key,
        });
        self
    }

    /// Remove an encryption key from the table metadata.
    pub fn remove_encryption_key(mut self, key_id: &str) -> Self {
        if self.metadata.encryption_keys.remove(key_id).is_some() {
            self.changes.push(TableUpdate::RemoveEncryptionKey {
                key_id: key_id.to_string(),
            });
        }
        self
    }

    /// Build the table metadata.
    pub fn build(mut self) -> Result<TableMetadataBuildResult> {
        self.metadata.last_updated_ms = self
            .last_updated_ms
            .unwrap_or_else(|| chrono::Utc::now().timestamp_millis());

        // Check compatibility of the current schema to the default partition spec and sort order.
        // We use the `get_xxx` methods from the builder to avoid using the panicking
        // `TableMetadata.default_partition_spec` etc. methods.
        let schema = self.get_current_schema()?.clone();
        let sort_order = Arc::unwrap_or_clone(self.get_default_sort_order()?);

        self.metadata.default_spec = Arc::new(
            Arc::unwrap_or_clone(self.metadata.default_spec)
                .into_unbound()
                .bind(schema.clone())?,
        );
        self.metadata.default_partition_type =
            self.metadata.default_spec.partition_type(&schema)?;
        SortOrder::builder()
            .with_fields(sort_order.fields)
            .build(&schema)?;

        self.update_snapshot_log()?;
        self.metadata.try_normalize()?;

        if let Some(hist_entry) = self.previous_history_entry.take() {
            self.metadata.metadata_log.push(hist_entry);
        }
        let expired_metadata_logs = self.expire_metadata_log();

        Ok(TableMetadataBuildResult {
            metadata: self.metadata,
            changes: self.changes,
            expired_metadata_logs,
        })
    }

    fn expire_metadata_log(&mut self) -> Vec<MetadataLog> {
        let max_size = self
            .metadata
            .properties
            .get(TableProperties::PROPERTY_METADATA_PREVIOUS_VERSIONS_MAX)
            .and_then(|v| v.parse::<usize>().ok())
            .unwrap_or(TableProperties::PROPERTY_METADATA_PREVIOUS_VERSIONS_MAX_DEFAULT)
            .max(1);

        if self.metadata.metadata_log.len() > max_size {
            self.metadata
                .metadata_log
                .drain(0..self.metadata.metadata_log.len() - max_size)
                .collect()
        } else {
            Vec::new()
        }
    }

    fn update_snapshot_log(&mut self) -> Result<()> {
        let intermediate_snapshots = self.get_intermediate_snapshots();
        let has_removed_snapshots = self
            .changes
            .iter()
            .any(|update| matches!(update, TableUpdate::RemoveSnapshots { .. }));

        if intermediate_snapshots.is_empty() && !has_removed_snapshots {
            return Ok(());
        }

        let mut new_snapshot_log = Vec::new();
        for log_entry in &self.metadata.snapshot_log {
            let snapshot_id = log_entry.snapshot_id;
            if self.metadata.snapshots.contains_key(&snapshot_id) {
                if !intermediate_snapshots.contains(&snapshot_id) {
                    new_snapshot_log.push(log_entry.clone());
                }
            } else if has_removed_snapshots {
                // any invalid entry causes the history before it to be removed. otherwise, there could be
                // history gaps that cause time-travel queries to produce incorrect results. for example,
                // if history is [(t1, s1), (t2, s2), (t3, s3)] and s2 is removed, the history cannot be
                // [(t1, s1), (t3, s3)] because it appears that s3 was current during the time between t2
                // and t3 when in fact s2 was the current snapshot.
                new_snapshot_log.clear();
            }
        }

        if let Some(current_snapshot_id) = self.metadata.current_snapshot_id {
            let last_id = new_snapshot_log.last().map(|entry| entry.snapshot_id);
            if last_id != Some(current_snapshot_id) {
                return Err(Error::new(
                    ErrorKind::DataInvalid,
                    "Cannot set invalid snapshot log: latest entry is not the current snapshot",
                ));
            }
        };

        self.metadata.snapshot_log = new_snapshot_log;
        Ok(())
    }

    /// Finds intermediate snapshots that have not been committed as the current snapshot.
    ///
    /// Transactions can create snapshots that are never the current snapshot because several
    /// changes are combined by the transaction into one table metadata update. when each
    /// intermediate snapshot is added to table metadata, it is added to the snapshot log, assuming
    /// that it will be the current snapshot. when there are multiple snapshot updates, the log must
    /// be corrected by suppressing the intermediate snapshot entries.
    ///     
    /// A snapshot is an intermediate snapshot if it was added but is not the current snapshot.
    fn get_intermediate_snapshots(&self) -> HashSet<i64> {
        let added_snapshot_ids = self
            .changes
            .iter()
            .filter_map(|update| match update {
                TableUpdate::AddSnapshot { snapshot } => Some(snapshot.snapshot_id()),
                _ => None,
            })
            .collect::<HashSet<_>>();

        self.changes
            .iter()
            .filter_map(|update| match update {
                TableUpdate::SetSnapshotRef {
                    ref_name,
                    reference,
                } => {
                    if added_snapshot_ids.contains(&reference.snapshot_id)
                        && ref_name == MAIN_BRANCH
                        && reference.snapshot_id
                            != self
                                .metadata
                                .current_snapshot_id
                                .unwrap_or(i64::from(Self::LAST_ADDED))
                    {
                        Some(reference.snapshot_id)
                    } else {
                        None
                    }
                }
                _ => None,
            })
            .collect()
    }

    fn reassign_ids(
        schema: Schema,
        spec: UnboundPartitionSpec,
        sort_order: SortOrder,
    ) -> Result<(Schema, PartitionSpec, SortOrder)> {
        // Re-assign field ids and schema ids for a new table.
        let previous_id_to_name = schema.field_id_to_name_map().clone();
        let fresh_schema = schema
            .into_builder()
            .with_schema_id(DEFAULT_SCHEMA_ID)
            .with_reassigned_field_ids(FIRST_FIELD_ID)
            .build()?;

        // Re-build partition spec with new ids
        let mut fresh_spec = PartitionSpecBuilder::new(fresh_schema.clone());
        for field in spec.fields() {
            let source_field_name = previous_id_to_name.get(&field.source_id).ok_or_else(|| {
                Error::new(
                    ErrorKind::DataInvalid,
                    format!(
                        "Cannot find source column with id {} for partition column {} in schema.",
                        field.source_id, field.name
                    ),
                )
            })?;
            fresh_spec =
                fresh_spec.add_partition_field(source_field_name, &field.name, field.transform)?;
        }
        let fresh_spec = fresh_spec.build()?;

        // Re-build sort order with new ids
        let mut fresh_order = SortOrder::builder();
        for mut field in sort_order.fields {
            let source_field_name = previous_id_to_name.get(&field.source_id).ok_or_else(|| {
                Error::new(
                    ErrorKind::DataInvalid,
                    format!(
                        "Cannot find source column with id {} for sort column in schema.",
                        field.source_id
                    ),
                )
            })?;
            let new_field_id = fresh_schema
                       .field_by_name(source_field_name)
                       .ok_or_else(|| {
                           Error::new(
                               ErrorKind::Unexpected,
                               format!(
                                   "Cannot find source column with name {source_field_name} for sort column in re-assigned schema."
                               ),
                           )
                       })?.id;
            field.source_id = new_field_id;
            fresh_order.with_sort_field(field);
        }
        let fresh_sort_order = fresh_order.build(&fresh_schema)?;

        Ok((fresh_schema, fresh_spec, fresh_sort_order))
    }

    fn reuse_or_create_new_schema_id(&self, new_schema: &Schema) -> i32 {
        self.metadata
            .schemas
            .iter()
            .find_map(|(id, schema)| new_schema.is_same_schema(schema).then_some(*id))
            .unwrap_or_else(|| self.get_highest_schema_id() + 1)
    }

    fn get_highest_schema_id(&self) -> i32 {
        *self
            .metadata
            .schemas
            .keys()
            .max()
            .unwrap_or(&self.metadata.current_schema_id)
    }

    fn get_current_schema(&self) -> Result<&SchemaRef> {
        self.metadata
            .schemas
            .get(&self.metadata.current_schema_id)
            .ok_or_else(|| {
                Error::new(
                    ErrorKind::DataInvalid,
                    format!(
                        "Current schema with id '{}' not found in table metadata.",
                        self.metadata.current_schema_id
                    ),
                )
            })
    }

    fn get_default_sort_order(&self) -> Result<SortOrderRef> {
        self.metadata
            .sort_orders
            .get(&self.metadata.default_sort_order_id)
            .cloned()
            .ok_or_else(|| {
                Error::new(
                    ErrorKind::DataInvalid,
                    format!(
                        "Default sort order with id '{}' not found in table metadata.",
                        self.metadata.default_sort_order_id
                    ),
                )
            })
    }

    /// If a compatible spec already exists, use the same ID. Otherwise, use 1 more than the highest ID.
    fn reuse_or_create_new_spec_id(&self, new_spec: &PartitionSpec) -> i32 {
        self.metadata
            .partition_specs
            .iter()
            .find_map(|(id, old_spec)| new_spec.is_compatible_with(old_spec).then_some(*id))
            .unwrap_or_else(|| {
                self.get_highest_spec_id()
                    .map(|id| id + 1)
                    .unwrap_or(DEFAULT_PARTITION_SPEC_ID)
            })
    }

    fn get_highest_spec_id(&self) -> Option<i32> {
        self.metadata.partition_specs.keys().max().copied()
    }

    /// If a compatible sort-order already exists, use the same ID. Otherwise, use 1 more than the highest ID.
    fn reuse_or_create_new_sort_id(&self, new_sort_order: &SortOrder) -> i64 {
        if new_sort_order.is_unsorted() {
            return SortOrder::unsorted_order().order_id;
        }

        self.metadata
            .sort_orders
            .iter()
            .find_map(|(id, sort_order)| {
                sort_order.fields.eq(&new_sort_order.fields).then_some(*id)
            })
            .unwrap_or_else(|| {
                self.highest_sort_order_id()
                    .unwrap_or(SortOrder::unsorted_order().order_id)
                    + 1
            })
    }

    fn highest_sort_order_id(&self) -> Option<i64> {
        self.metadata.sort_orders.keys().max().copied()
    }

    /// Remove schemas by their ids from the table metadata.
    /// Does nothing if a schema id is not present. Active schemas should not be removed.
    pub fn remove_schemas(mut self, schema_id_to_remove: &[i32]) -> Result<Self> {
        if schema_id_to_remove.contains(&self.metadata.current_schema_id) {
            return Err(Error::new(
                ErrorKind::DataInvalid,
                "Cannot remove current schema",
            ));
        }

        if schema_id_to_remove.is_empty() {
            return Ok(self);
        }

        let mut removed_schemas = Vec::with_capacity(schema_id_to_remove.len());
        self.metadata.schemas.retain(|id, _schema| {
            if schema_id_to_remove.contains(id) {
                removed_schemas.push(*id);
                false
            } else {
                true
            }
        });

        self.changes.push(TableUpdate::RemoveSchemas {
            schema_ids: removed_schemas,
        });

        Ok(self)
    }
}

impl From<TableMetadataBuildResult> for TableMetadata {
    fn from(result: TableMetadataBuildResult) -> Self {
        result.metadata
    }
}

#[cfg(test)]
mod tests {
    use std::fs::File;
    use std::io::BufReader;
    use std::thread::sleep;

    use super::*;
    use crate::TableIdent;
    use crate::io::FileIOBuilder;
    use crate::spec::{
        BlobMetadata, NestedField, NullOrder, Operation, PartitionSpec, PrimitiveType, Schema,
        SnapshotRetention, SortDirection, SortField, StructType, Summary, TableProperties,
        Transform, Type, UnboundPartitionField,
    };
    use crate::table::Table;

    const TEST_LOCATION: &str = "s3://bucket/test/location";
    const LAST_ASSIGNED_COLUMN_ID: i32 = 3;

    fn schema() -> Schema {
        Schema::builder()
            .with_fields(vec![
                NestedField::required(1, "x", Type::Primitive(PrimitiveType::Long)).into(),
                NestedField::required(2, "y", Type::Primitive(PrimitiveType::Long)).into(),
                NestedField::required(3, "z", Type::Primitive(PrimitiveType::Long)).into(),
            ])
            .build()
            .unwrap()
    }

    fn sort_order() -> SortOrder {
        let schema = schema();
        SortOrder::builder()
            .with_order_id(1)
            .with_sort_field(SortField {
                source_id: 3,
                transform: Transform::Bucket(4),
                direction: SortDirection::Descending,
                null_order: NullOrder::First,
            })
            .build(&schema)
            .unwrap()
    }

    fn partition_spec() -> UnboundPartitionSpec {
        UnboundPartitionSpec::builder()
            .with_spec_id(0)
            .add_partition_field(2, "y", Transform::Identity)
            .unwrap()
            .build()
    }

    fn builder_without_changes(format_version: FormatVersion) -> TableMetadataBuilder {
        TableMetadataBuilder::new(
            schema(),
            partition_spec(),
            sort_order(),
            TEST_LOCATION.to_string(),
            format_version,
            HashMap::new(),
        )
        .unwrap()
        .build()
        .unwrap()
        .metadata
        .into_builder(Some(
            "s3://bucket/test/location/metadata/metadata1.json".to_string(),
        ))
    }

    #[test]
    fn test_minimal_build() {
        let metadata = TableMetadataBuilder::new(
            schema(),
            partition_spec(),
            sort_order(),
            TEST_LOCATION.to_string(),
            FormatVersion::V1,
            HashMap::new(),
        )
        .unwrap()
        .build()
        .unwrap()
        .metadata;

        assert_eq!(metadata.format_version, FormatVersion::V1);
        assert_eq!(metadata.location, TEST_LOCATION);
        assert_eq!(metadata.current_schema_id, 0);
        assert_eq!(metadata.default_spec.spec_id(), 0);
        assert_eq!(metadata.default_sort_order_id, 1);
        assert_eq!(metadata.last_partition_id, 1000);
        assert_eq!(metadata.last_column_id, 3);
        assert_eq!(metadata.snapshots.len(), 0);
        assert_eq!(metadata.current_snapshot_id, None);
        assert_eq!(metadata.refs.len(), 0);
        assert_eq!(metadata.properties.len(), 0);
        assert_eq!(metadata.metadata_log.len(), 0);
        assert_eq!(metadata.last_sequence_number, 0);
        assert_eq!(metadata.last_column_id, LAST_ASSIGNED_COLUMN_ID);

        // Test can serialize v1
        let _ = serde_json::to_string(&metadata).unwrap();

        // Test can serialize v2
        let metadata = metadata
            .into_builder(Some(
                "s3://bucket/test/location/metadata/metadata1.json".to_string(),
            ))
            .upgrade_format_version(FormatVersion::V2)
            .unwrap()
            .build()
            .unwrap()
            .metadata;

        assert_eq!(metadata.format_version, FormatVersion::V2);
        let _ = serde_json::to_string(&metadata).unwrap();
    }

    #[test]
    fn test_build_unpartitioned_unsorted() {
        let schema = Schema::builder().build().unwrap();
        let metadata = TableMetadataBuilder::new(
            schema.clone(),
            PartitionSpec::unpartition_spec(),
            SortOrder::unsorted_order(),
            TEST_LOCATION.to_string(),
            FormatVersion::V2,
            HashMap::new(),
        )
        .unwrap()
        .build()
        .unwrap()
        .metadata;

        assert_eq!(metadata.format_version, FormatVersion::V2);
        assert_eq!(metadata.location, TEST_LOCATION);
        assert_eq!(metadata.current_schema_id, 0);
        assert_eq!(metadata.default_spec.spec_id(), 0);
        assert_eq!(metadata.default_sort_order_id, 0);
        assert_eq!(metadata.last_partition_id, UNPARTITIONED_LAST_ASSIGNED_ID);
        assert_eq!(metadata.last_column_id, 0);
        assert_eq!(metadata.snapshots.len(), 0);
        assert_eq!(metadata.current_snapshot_id, None);
        assert_eq!(metadata.refs.len(), 0);
        assert_eq!(metadata.properties.len(), 0);
        assert_eq!(metadata.metadata_log.len(), 0);
        assert_eq!(metadata.last_sequence_number, 0);
    }

    #[test]
    fn test_reassigns_ids() {
        let schema = Schema::builder()
            .with_schema_id(10)
            .with_fields(vec![
                NestedField::required(11, "a", Type::Primitive(PrimitiveType::Long)).into(),
                NestedField::required(12, "b", Type::Primitive(PrimitiveType::Long)).into(),
                NestedField::required(
                    13,
                    "struct",
                    Type::Struct(StructType::new(vec![
                        NestedField::required(14, "nested", Type::Primitive(PrimitiveType::Long))
                            .into(),
                    ])),
                )
                .into(),
                NestedField::required(15, "c", Type::Primitive(PrimitiveType::Long)).into(),
            ])
            .build()
            .unwrap();
        let spec = PartitionSpec::builder(schema.clone())
            .with_spec_id(20)
            .add_partition_field("a", "a", Transform::Identity)
            .unwrap()
            .add_partition_field("struct.nested", "nested_partition", Transform::Identity)
            .unwrap()
            .build()
            .unwrap();
        let sort_order = SortOrder::builder()
            .with_fields(vec![SortField {
                source_id: 11,
                transform: Transform::Identity,
                direction: SortDirection::Ascending,
                null_order: NullOrder::First,
            }])
            .with_order_id(10)
            .build(&schema)
            .unwrap();

        let (fresh_schema, fresh_spec, fresh_sort_order) =
            TableMetadataBuilder::reassign_ids(schema, spec.into_unbound(), sort_order).unwrap();

        let expected_schema = Schema::builder()
            .with_fields(vec![
                NestedField::required(1, "a", Type::Primitive(PrimitiveType::Long)).into(),
                NestedField::required(2, "b", Type::Primitive(PrimitiveType::Long)).into(),
                NestedField::required(
                    3,
                    "struct",
                    Type::Struct(StructType::new(vec![
                        NestedField::required(5, "nested", Type::Primitive(PrimitiveType::Long))
                            .into(),
                    ])),
                )
                .into(),
                NestedField::required(4, "c", Type::Primitive(PrimitiveType::Long)).into(),
            ])
            .build()
            .unwrap();

        let expected_spec = PartitionSpec::builder(expected_schema.clone())
            .with_spec_id(0)
            .add_partition_field("a", "a", Transform::Identity)
            .unwrap()
            .add_partition_field("struct.nested", "nested_partition", Transform::Identity)
            .unwrap()
            .build()
            .unwrap();

        let expected_sort_order = SortOrder::builder()
            .with_fields(vec![SortField {
                source_id: 1,
                transform: Transform::Identity,
                direction: SortDirection::Ascending,
                null_order: NullOrder::First,
            }])
            .with_order_id(1)
            .build(&expected_schema)
            .unwrap();

        assert_eq!(fresh_schema, expected_schema);
        assert_eq!(fresh_spec, expected_spec);
        assert_eq!(fresh_sort_order, expected_sort_order);
    }

    #[test]
    fn test_ids_are_reassigned_for_new_metadata() {
        let schema = schema().into_builder().with_schema_id(10).build().unwrap();

        let metadata = TableMetadataBuilder::new(
            schema,
            partition_spec(),
            sort_order(),
            TEST_LOCATION.to_string(),
            FormatVersion::V1,
            HashMap::new(),
        )
        .unwrap()
        .build()
        .unwrap()
        .metadata;

        assert_eq!(metadata.current_schema_id, 0);
        assert_eq!(metadata.current_schema().schema_id(), 0);
    }

    #[test]
    fn test_new_metadata_changes() {
        let changes = TableMetadataBuilder::new(
            schema(),
            partition_spec(),
            sort_order(),
            TEST_LOCATION.to_string(),
            FormatVersion::V1,
            HashMap::from_iter(vec![("property 1".to_string(), "value 1".to_string())]),
        )
        .unwrap()
        .build()
        .unwrap()
        .changes;

        pretty_assertions::assert_eq!(changes, vec![
            TableUpdate::SetLocation {
                location: TEST_LOCATION.to_string()
            },
            TableUpdate::AddSchema { schema: schema() },
            TableUpdate::SetCurrentSchema { schema_id: -1 },
            TableUpdate::AddSpec {
                // Because this is a new tables, field-ids are assigned
                // partition_spec() has None set for field-id
                spec: PartitionSpec::builder(schema())
                    .with_spec_id(0)
                    .add_unbound_field(UnboundPartitionField {
                        name: "y".to_string(),
                        transform: Transform::Identity,
                        source_id: 2,
                        field_id: Some(1000)
                    })
                    .unwrap()
                    .build()
                    .unwrap()
                    .into_unbound(),
            },
            TableUpdate::SetDefaultSpec { spec_id: -1 },
            TableUpdate::AddSortOrder {
                sort_order: sort_order(),
            },
            TableUpdate::SetDefaultSortOrder { sort_order_id: -1 },
            TableUpdate::SetProperties {
                updates: HashMap::from_iter(vec![(
                    "property 1".to_string(),
                    "value 1".to_string()
                )]),
            }
        ]);
    }

    #[test]
    fn test_new_metadata_changes_unpartitioned_unsorted() {
        let schema = Schema::builder().build().unwrap();
        let changes = TableMetadataBuilder::new(
            schema.clone(),
            PartitionSpec::unpartition_spec().into_unbound(),
            SortOrder::unsorted_order(),
            TEST_LOCATION.to_string(),
            FormatVersion::V1,
            HashMap::new(),
        )
        .unwrap()
        .build()
        .unwrap()
        .changes;

        pretty_assertions::assert_eq!(changes, vec![
            TableUpdate::SetLocation {
                location: TEST_LOCATION.to_string()
            },
            TableUpdate::AddSchema {
                schema: Schema::builder().build().unwrap(),
            },
            TableUpdate::SetCurrentSchema { schema_id: -1 },
            TableUpdate::AddSpec {
                // Because this is a new tables, field-ids are assigned
                // partition_spec() has None set for field-id
                spec: PartitionSpec::builder(schema)
                    .with_spec_id(0)
                    .build()
                    .unwrap()
                    .into_unbound(),
            },
            TableUpdate::SetDefaultSpec { spec_id: -1 },
            TableUpdate::AddSortOrder {
                sort_order: SortOrder::unsorted_order(),
            },
            TableUpdate::SetDefaultSortOrder { sort_order_id: -1 },
        ]);
    }

    #[test]
    fn test_add_partition_spec() {
        let builder = builder_without_changes(FormatVersion::V2);

        let added_spec = UnboundPartitionSpec::builder()
            .with_spec_id(10)
            .add_partition_fields(vec![
                UnboundPartitionField {
                    // The previous field - has field_id set
                    name: "y".to_string(),
                    transform: Transform::Identity,
                    source_id: 2,
                    field_id: Some(1000),
                },
                UnboundPartitionField {
                    // A new field without field id - should still be without field id in changes
                    name: "z".to_string(),
                    transform: Transform::Identity,
                    source_id: 3,
                    field_id: None,
                },
            ])
            .unwrap()
            .build();

        let build_result = builder
            .add_partition_spec(added_spec.clone())
            .unwrap()
            .build()
            .unwrap();

        // Spec id should be re-assigned
        let expected_change = added_spec.with_spec_id(1);
        let expected_spec = PartitionSpec::builder(schema())
            .with_spec_id(1)
            .add_unbound_field(UnboundPartitionField {
                name: "y".to_string(),
                transform: Transform::Identity,
                source_id: 2,
                field_id: Some(1000),
            })
            .unwrap()
            .add_unbound_field(UnboundPartitionField {
                name: "z".to_string(),
                transform: Transform::Identity,
                source_id: 3,
                field_id: Some(1001),
            })
            .unwrap()
            .build()
            .unwrap();

        assert_eq!(build_result.changes.len(), 1);
        assert_eq!(
            build_result.metadata.partition_spec_by_id(1),
            Some(&Arc::new(expected_spec))
        );
        assert_eq!(build_result.metadata.default_spec.spec_id(), 0);
        assert_eq!(build_result.metadata.last_partition_id, 1001);
        pretty_assertions::assert_eq!(build_result.changes[0], TableUpdate::AddSpec {
            spec: expected_change
        });

        // Remove the spec
        let build_result = build_result
            .metadata
            .into_builder(Some(
                "s3://bucket/test/location/metadata/metadata1.json".to_string(),
            ))
            .remove_partition_specs(&[1])
            .unwrap()
            .build()
            .unwrap();

        assert_eq!(build_result.changes.len(), 1);
        assert_eq!(build_result.metadata.partition_specs.len(), 1);
        assert!(build_result.metadata.partition_spec_by_id(1).is_none());
    }

    #[test]
    fn test_set_default_partition_spec() {
        let builder = builder_without_changes(FormatVersion::V2);
        let schema = builder.get_current_schema().unwrap().clone();
        let added_spec = UnboundPartitionSpec::builder()
            .with_spec_id(10)
            .add_partition_field(1, "y_bucket[2]", Transform::Bucket(2))
            .unwrap()
            .build();

        let build_result = builder
            .add_partition_spec(added_spec.clone())
            .unwrap()
            .set_default_partition_spec(-1)
            .unwrap()
            .build()
            .unwrap();

        let expected_spec = PartitionSpec::builder(schema)
            .with_spec_id(1)
            .add_unbound_field(UnboundPartitionField {
                name: "y_bucket[2]".to_string(),
                transform: Transform::Bucket(2),
                source_id: 1,
                field_id: Some(1001),
            })
            .unwrap()
            .build()
            .unwrap();

        assert_eq!(build_result.changes.len(), 2);
        assert_eq!(build_result.metadata.default_spec, Arc::new(expected_spec));
        assert_eq!(build_result.changes, vec![
            TableUpdate::AddSpec {
                // Should contain the actual ID that was used
                spec: added_spec.with_spec_id(1)
            },
            TableUpdate::SetDefaultSpec { spec_id: -1 }
        ]);
    }

    #[test]
    fn test_set_existing_default_partition_spec() {
        let builder = builder_without_changes(FormatVersion::V2);
        // Add and set an unbound spec as current
        let unbound_spec = UnboundPartitionSpec::builder().with_spec_id(1).build();
        let build_result = builder
            .add_partition_spec(unbound_spec.clone())
            .unwrap()
            .set_default_partition_spec(-1)
            .unwrap()
            .build()
            .unwrap();

        assert_eq!(build_result.changes.len(), 2);
        assert_eq!(build_result.changes[0], TableUpdate::AddSpec {
            spec: unbound_spec.clone()
        });
        assert_eq!(build_result.changes[1], TableUpdate::SetDefaultSpec {
            spec_id: -1
        });
        assert_eq!(
            build_result.metadata.default_spec,
            Arc::new(
                unbound_spec
                    .bind(build_result.metadata.current_schema().clone())
                    .unwrap()
            )
        );

        // Set old spec again
        let build_result = build_result
            .metadata
            .into_builder(Some(
                "s3://bucket/test/location/metadata/metadata1.json".to_string(),
            ))
            .set_default_partition_spec(0)
            .unwrap()
            .build()
            .unwrap();

        assert_eq!(build_result.changes.len(), 1);
        assert_eq!(build_result.changes[0], TableUpdate::SetDefaultSpec {
            spec_id: 0
        });
        assert_eq!(
            build_result.metadata.default_spec,
            Arc::new(
                partition_spec()
                    .bind(build_result.metadata.current_schema().clone())
                    .unwrap()
            )
        );
    }

    #[test]
    fn test_add_sort_order() {
        let builder = builder_without_changes(FormatVersion::V2);

        let added_sort_order = SortOrder::builder()
            .with_order_id(10)
            .with_fields(vec![SortField {
                source_id: 1,
                transform: Transform::Identity,
                direction: SortDirection::Ascending,
                null_order: NullOrder::First,
            }])
            .build(&schema())
            .unwrap();

        let build_result = builder
            .add_sort_order(added_sort_order.clone())
            .unwrap()
            .build()
            .unwrap();

        let expected_sort_order = added_sort_order.with_order_id(2);

        assert_eq!(build_result.changes.len(), 1);
        assert_eq!(build_result.metadata.sort_orders.keys().max(), Some(&2));
        pretty_assertions::assert_eq!(
            build_result.metadata.sort_order_by_id(2),
            Some(&Arc::new(expected_sort_order.clone()))
        );
        pretty_assertions::assert_eq!(build_result.changes[0], TableUpdate::AddSortOrder {
            sort_order: expected_sort_order
        });
    }

    #[test]
    fn test_add_compatible_schema() {
        let builder = builder_without_changes(FormatVersion::V2);

        let added_schema = Schema::builder()
            .with_schema_id(1)
            .with_fields(vec![
                NestedField::required(1, "x", Type::Primitive(PrimitiveType::Long)).into(),
                NestedField::required(2, "y", Type::Primitive(PrimitiveType::Long)).into(),
                NestedField::required(3, "z", Type::Primitive(PrimitiveType::Long)).into(),
                NestedField::required(4, "a", Type::Primitive(PrimitiveType::Long)).into(),
            ])
            .build()
            .unwrap();

        let build_result = builder
            .add_current_schema(added_schema.clone())
            .unwrap()
            .build()
            .unwrap();

        assert_eq!(build_result.changes.len(), 2);
        assert_eq!(build_result.metadata.schemas.keys().max(), Some(&1));
        pretty_assertions::assert_eq!(
            build_result.metadata.schema_by_id(1),
            Some(&Arc::new(added_schema.clone()))
        );
        pretty_assertions::assert_eq!(build_result.changes[0], TableUpdate::AddSchema {
            schema: added_schema
        });
        assert_eq!(build_result.changes[1], TableUpdate::SetCurrentSchema {
            schema_id: -1
        });
    }

    #[test]
    fn test_set_current_schema_change_is_minus_one_if_schema_was_added_in_this_change() {
        let builder = builder_without_changes(FormatVersion::V2);

        let added_schema = Schema::builder()
            .with_schema_id(1)
            .with_fields(vec![
                NestedField::required(1, "x", Type::Primitive(PrimitiveType::Long)).into(),
                NestedField::required(2, "y", Type::Primitive(PrimitiveType::Long)).into(),
                NestedField::required(3, "z", Type::Primitive(PrimitiveType::Long)).into(),
                NestedField::required(4, "a", Type::Primitive(PrimitiveType::Long)).into(),
            ])
            .build()
            .unwrap();

        let build_result = builder
            .add_schema(added_schema.clone())
            .unwrap()
            .set_current_schema(1)
            .unwrap()
            .build()
            .unwrap();

        assert_eq!(build_result.changes.len(), 2);
        assert_eq!(build_result.changes[1], TableUpdate::SetCurrentSchema {
            schema_id: -1
        });
    }

    #[test]
    fn test_no_metadata_log_for_create_table() {
        let build_result = TableMetadataBuilder::new(
            schema(),
            partition_spec(),
            sort_order(),
            TEST_LOCATION.to_string(),
            FormatVersion::V2,
            HashMap::new(),
        )
        .unwrap()
        .build()
        .unwrap();

        assert_eq!(build_result.metadata.metadata_log.len(), 0);
    }

    #[test]
    fn test_no_metadata_log_entry_for_no_previous_location() {
        // Used for first commit after stage-creation of tables
        let metadata = builder_without_changes(FormatVersion::V2)
            .build()
            .unwrap()
            .metadata;
        assert_eq!(metadata.metadata_log.len(), 1);

        let build_result = metadata
            .into_builder(None)
            .set_properties(HashMap::from_iter(vec![(
                "foo".to_string(),
                "bar".to_string(),
            )]))
            .unwrap()
            .build()
            .unwrap();

        assert_eq!(build_result.metadata.metadata_log.len(), 1);
    }

    #[test]
    fn test_from_metadata_generates_metadata_log() {
        let metadata_path = "s3://bucket/test/location/metadata/metadata1.json";
        let builder = TableMetadataBuilder::new(
            schema(),
            partition_spec(),
            sort_order(),
            TEST_LOCATION.to_string(),
            FormatVersion::V2,
            HashMap::new(),
        )
        .unwrap()
        .build()
        .unwrap()
        .metadata
        .into_builder(Some(metadata_path.to_string()));

        let builder = builder
            .add_default_sort_order(SortOrder::unsorted_order())
            .unwrap();

        let build_result = builder.build().unwrap();

        assert_eq!(build_result.metadata.metadata_log.len(), 1);
        assert_eq!(
            build_result.metadata.metadata_log[0].metadata_file,
            metadata_path
        );
    }

    #[test]
    fn test_set_ref() {
        let builder = builder_without_changes(FormatVersion::V2);

        let snapshot = Snapshot::builder()
            .with_snapshot_id(1)
            .with_timestamp_ms(builder.metadata.last_updated_ms + 1)
            .with_sequence_number(0)
            .with_schema_id(0)
            .with_manifest_list("/snap-1.avro")
            .with_summary(Summary {
                operation: Operation::Append,
                additional_properties: HashMap::from_iter(vec![
                    (
                        "spark.app.id".to_string(),
                        "local-1662532784305".to_string(),
                    ),
                    ("added-data-files".to_string(), "4".to_string()),
                    ("added-records".to_string(), "4".to_string()),
                    ("added-files-size".to_string(), "6001".to_string()),
                ]),
            })
            .build();

        let builder = builder.add_snapshot(snapshot.clone()).unwrap();

        assert!(
            builder
                .clone()
                .set_ref(MAIN_BRANCH, SnapshotReference {
                    snapshot_id: 10,
                    retention: SnapshotRetention::Branch {
                        min_snapshots_to_keep: Some(10),
                        max_snapshot_age_ms: None,
                        max_ref_age_ms: None,
                    },
                })
                .unwrap_err()
                .to_string()
                .contains("Cannot set 'main' to unknown snapshot: '10'")
        );

        let build_result = builder
            .set_ref(MAIN_BRANCH, SnapshotReference {
                snapshot_id: 1,
                retention: SnapshotRetention::Branch {
                    min_snapshots_to_keep: Some(10),
                    max_snapshot_age_ms: None,
                    max_ref_age_ms: None,
                },
            })
            .unwrap()
            .build()
            .unwrap();
        assert_eq!(build_result.metadata.snapshots.len(), 1);
        assert_eq!(
            build_result.metadata.snapshot_by_id(1),
            Some(&Arc::new(snapshot.clone()))
        );
        assert_eq!(build_result.metadata.snapshot_log, vec![SnapshotLog {
            snapshot_id: 1,
            timestamp_ms: snapshot.timestamp_ms()
        }])
    }

    #[test]
    fn test_snapshot_log_skips_intermediates() {
        let builder = builder_without_changes(FormatVersion::V2);

        let snapshot_1 = Snapshot::builder()
            .with_snapshot_id(1)
            .with_timestamp_ms(builder.metadata.last_updated_ms + 1)
            .with_sequence_number(0)
            .with_schema_id(0)
            .with_manifest_list("/snap-1.avro")
            .with_summary(Summary {
                operation: Operation::Append,
                additional_properties: HashMap::from_iter(vec![
                    (
                        "spark.app.id".to_string(),
                        "local-1662532784305".to_string(),
                    ),
                    ("added-data-files".to_string(), "4".to_string()),
                    ("added-records".to_string(), "4".to_string()),
                    ("added-files-size".to_string(), "6001".to_string()),
                ]),
            })
            .build();

        let snapshot_2 = Snapshot::builder()
            .with_snapshot_id(2)
            .with_timestamp_ms(builder.metadata.last_updated_ms + 1)
            .with_sequence_number(0)
            .with_schema_id(0)
            .with_manifest_list("/snap-1.avro")
            .with_summary(Summary {
                operation: Operation::Append,
                additional_properties: HashMap::from_iter(vec![
                    (
                        "spark.app.id".to_string(),
                        "local-1662532784305".to_string(),
                    ),
                    ("added-data-files".to_string(), "4".to_string()),
                    ("added-records".to_string(), "4".to_string()),
                    ("added-files-size".to_string(), "6001".to_string()),
                ]),
            })
            .build();

        let result = builder
            .add_snapshot(snapshot_1)
            .unwrap()
            .set_ref(MAIN_BRANCH, SnapshotReference {
                snapshot_id: 1,
                retention: SnapshotRetention::Branch {
                    min_snapshots_to_keep: Some(10),
                    max_snapshot_age_ms: None,
                    max_ref_age_ms: None,
                },
            })
            .unwrap()
            .set_branch_snapshot(snapshot_2.clone(), MAIN_BRANCH)
            .unwrap()
            .build()
            .unwrap();

        assert_eq!(result.metadata.snapshot_log, vec![SnapshotLog {
            snapshot_id: 2,
            timestamp_ms: snapshot_2.timestamp_ms()
        }]);
        assert_eq!(result.metadata.current_snapshot().unwrap().snapshot_id(), 2);
    }

    #[test]
    fn test_remove_main_ref_keeps_snapshot_log() {
        let builder = builder_without_changes(FormatVersion::V2);

        let snapshot = Snapshot::builder()
            .with_snapshot_id(1)
            .with_timestamp_ms(builder.metadata.last_updated_ms + 1)
            .with_sequence_number(0)
            .with_schema_id(0)
            .with_manifest_list("/snap-1.avro")
            .with_summary(Summary {
                operation: Operation::Append,
                additional_properties: HashMap::from_iter(vec![
                    (
                        "spark.app.id".to_string(),
                        "local-1662532784305".to_string(),
                    ),
                    ("added-data-files".to_string(), "4".to_string()),
                    ("added-records".to_string(), "4".to_string()),
                    ("added-files-size".to_string(), "6001".to_string()),
                ]),
            })
            .build();

        let result = builder
            .add_snapshot(snapshot.clone())
            .unwrap()
            .set_ref(MAIN_BRANCH, SnapshotReference {
                snapshot_id: 1,
                retention: SnapshotRetention::Branch {
                    min_snapshots_to_keep: Some(10),
                    max_snapshot_age_ms: None,
                    max_ref_age_ms: None,
                },
            })
            .unwrap()
            .build()
            .unwrap();

        // Verify snapshot log was created
        assert_eq!(result.metadata.snapshot_log.len(), 1);
        assert_eq!(result.metadata.snapshot_log[0].snapshot_id, 1);
        assert_eq!(result.metadata.current_snapshot_id, Some(1));

        // Remove the main ref
        let result_after_remove = result
            .metadata
            .into_builder(Some(
                "s3://bucket/test/location/metadata/metadata2.json".to_string(),
            ))
            .remove_ref(MAIN_BRANCH)
            .build()
            .unwrap();

        // Verify snapshot log is kept even after removing main ref
        assert_eq!(result_after_remove.metadata.snapshot_log.len(), 1);
        assert_eq!(result_after_remove.metadata.snapshot_log[0].snapshot_id, 1);
        assert_eq!(result_after_remove.metadata.current_snapshot_id, None);
        assert_eq!(result_after_remove.changes.len(), 1);
        assert_eq!(
            result_after_remove.changes[0],
            TableUpdate::RemoveSnapshotRef {
                ref_name: MAIN_BRANCH.to_string()
            }
        );
    }

    #[test]
    fn test_set_branch_snapshot_creates_branch_if_not_exists() {
        let builder = builder_without_changes(FormatVersion::V2);

        let snapshot = Snapshot::builder()
            .with_snapshot_id(2)
            .with_timestamp_ms(builder.metadata.last_updated_ms + 1)
            .with_sequence_number(0)
            .with_schema_id(0)
            .with_manifest_list("/snap-1.avro")
            .with_summary(Summary {
                operation: Operation::Append,
                additional_properties: HashMap::new(),
            })
            .build();

        let build_result = builder
            .set_branch_snapshot(snapshot.clone(), "new_branch")
            .unwrap()
            .build()
            .unwrap();

        let reference = SnapshotReference {
            snapshot_id: 2,
            retention: SnapshotRetention::Branch {
                min_snapshots_to_keep: None,
                max_snapshot_age_ms: None,
                max_ref_age_ms: None,
            },
        };

        assert_eq!(build_result.metadata.refs.len(), 1);
        assert_eq!(
            build_result.metadata.refs.get("new_branch"),
            Some(&reference)
        );
        assert_eq!(build_result.changes, vec![
            TableUpdate::AddSnapshot { snapshot },
            TableUpdate::SetSnapshotRef {
                ref_name: "new_branch".to_string(),
                reference
            }
        ]);
    }

    #[test]
    fn test_cannot_add_duplicate_snapshot_id() {
        let builder = builder_without_changes(FormatVersion::V2);

        let snapshot = Snapshot::builder()
            .with_snapshot_id(2)
            .with_timestamp_ms(builder.metadata.last_updated_ms + 1)
            .with_sequence_number(0)
            .with_schema_id(0)
            .with_manifest_list("/snap-1.avro")
            .with_summary(Summary {
                operation: Operation::Append,
                additional_properties: HashMap::from_iter(vec![
                    (
                        "spark.app.id".to_string(),
                        "local-1662532784305".to_string(),
                    ),
                    ("added-data-files".to_string(), "4".to_string()),
                    ("added-records".to_string(), "4".to_string()),
                    ("added-files-size".to_string(), "6001".to_string()),
                ]),
            })
            .build();

        let builder = builder.add_snapshot(snapshot.clone()).unwrap();
        builder.add_snapshot(snapshot).unwrap_err();
    }

    #[test]
    fn test_add_incompatible_current_schema_fails() {
        let builder = builder_without_changes(FormatVersion::V2);

        let added_schema = Schema::builder()
            .with_schema_id(1)
            .with_fields(vec![])
            .build()
            .unwrap();

        let err = builder
            .add_current_schema(added_schema)
            .unwrap()
            .build()
            .unwrap_err();

        assert!(
            err.to_string()
                .contains("Cannot find partition source field")
        );
    }

    #[test]
    fn test_add_partition_spec_for_v1_requires_sequential_ids() {
        let builder = builder_without_changes(FormatVersion::V1);

        let added_spec = UnboundPartitionSpec::builder()
            .with_spec_id(10)
            .add_partition_fields(vec![
                UnboundPartitionField {
                    name: "y".to_string(),
                    transform: Transform::Identity,
                    source_id: 2,
                    field_id: Some(1000),
                },
                UnboundPartitionField {
                    name: "z".to_string(),
                    transform: Transform::Identity,
                    source_id: 3,
                    field_id: Some(1002),
                },
            ])
            .unwrap()
            .build();

        let err = builder.add_partition_spec(added_spec).unwrap_err();
        assert!(err.to_string().contains(
            "Cannot add partition spec with non-sequential field ids to format version 1 table"
        ));
    }

    #[test]
    fn test_expire_metadata_log() {
        let builder = builder_without_changes(FormatVersion::V2);
        let metadata = builder
            .set_properties(HashMap::from_iter(vec![(
                TableProperties::PROPERTY_METADATA_PREVIOUS_VERSIONS_MAX.to_string(),
                "2".to_string(),
            )]))
            .unwrap()
            .build()
            .unwrap();
        assert_eq!(metadata.metadata.metadata_log.len(), 1);
        assert_eq!(metadata.expired_metadata_logs.len(), 0);

        let metadata = metadata
            .metadata
            .into_builder(Some("path2".to_string()))
            .set_properties(HashMap::from_iter(vec![(
                "change_nr".to_string(),
                "1".to_string(),
            )]))
            .unwrap()
            .build()
            .unwrap();

        assert_eq!(metadata.metadata.metadata_log.len(), 2);
        assert_eq!(metadata.expired_metadata_logs.len(), 0);

        let metadata = metadata
            .metadata
            .into_builder(Some("path2".to_string()))
            .set_properties(HashMap::from_iter(vec![(
                "change_nr".to_string(),
                "2".to_string(),
            )]))
            .unwrap()
            .build()
            .unwrap();
        assert_eq!(metadata.metadata.metadata_log.len(), 2);
        assert_eq!(metadata.expired_metadata_logs.len(), 1);
    }

    #[test]
    fn test_v2_sequence_number_cannot_decrease() {
        let builder = builder_without_changes(FormatVersion::V2);

        let snapshot = Snapshot::builder()
            .with_snapshot_id(1)
            .with_timestamp_ms(builder.metadata.last_updated_ms + 1)
            .with_sequence_number(1)
            .with_schema_id(0)
            .with_manifest_list("/snap-1")
            .with_summary(Summary {
                operation: Operation::Append,
                additional_properties: HashMap::new(),
            })
            .build();

        let builder = builder
            .add_snapshot(snapshot.clone())
            .unwrap()
            .set_ref(MAIN_BRANCH, SnapshotReference {
                snapshot_id: 1,
                retention: SnapshotRetention::Branch {
                    min_snapshots_to_keep: Some(10),
                    max_snapshot_age_ms: None,
                    max_ref_age_ms: None,
                },
            })
            .unwrap();

        let snapshot = Snapshot::builder()
            .with_snapshot_id(2)
            .with_timestamp_ms(builder.metadata.last_updated_ms + 1)
            .with_sequence_number(0)
            .with_schema_id(0)
            .with_manifest_list("/snap-0")
            .with_parent_snapshot_id(Some(1))
            .with_summary(Summary {
                operation: Operation::Append,
                additional_properties: HashMap::new(),
            })
            .build();

        let err = builder
            .set_branch_snapshot(snapshot, MAIN_BRANCH)
            .unwrap_err();
        assert!(
            err.to_string()
                .contains("Cannot add snapshot with sequence number")
        );
    }

    #[test]
    fn test_default_spec_cannot_be_removed() {
        let builder = builder_without_changes(FormatVersion::V2);

        builder.remove_partition_specs(&[0]).unwrap_err();
    }

    #[test]
    fn test_statistics() {
        let builder = builder_without_changes(FormatVersion::V2);

        let statistics = StatisticsFile {
            snapshot_id: 3055729675574597004,
            statistics_path: "s3://a/b/stats.puffin".to_string(),
            file_size_in_bytes: 413,
            file_footer_size_in_bytes: 42,
            key_metadata: None,
            blob_metadata: vec![BlobMetadata {
                snapshot_id: 3055729675574597004,
                sequence_number: 1,
                fields: vec![1],
                r#type: "ndv".to_string(),
                properties: HashMap::new(),
            }],
        };
        let build_result = builder.set_statistics(statistics.clone()).build().unwrap();

        assert_eq!(
            build_result.metadata.statistics,
            HashMap::from_iter(vec![(3055729675574597004, statistics.clone())])
        );
        assert_eq!(build_result.changes, vec![TableUpdate::SetStatistics {
            statistics: statistics.clone()
        }]);

        // Remove
        let builder = build_result.metadata.into_builder(None);
        let build_result = builder
            .remove_statistics(statistics.snapshot_id)
            .build()
            .unwrap();

        assert_eq!(build_result.metadata.statistics.len(), 0);
        assert_eq!(build_result.changes, vec![TableUpdate::RemoveStatistics {
            snapshot_id: statistics.snapshot_id
        }]);

        // Remove again yields no changes
        let builder = build_result.metadata.into_builder(None);
        let build_result = builder
            .remove_statistics(statistics.snapshot_id)
            .build()
            .unwrap();
        assert_eq!(build_result.metadata.statistics.len(), 0);
        assert_eq!(build_result.changes.len(), 0);
    }

    #[test]
    fn test_add_partition_statistics() {
        let builder = builder_without_changes(FormatVersion::V2);

        let statistics = PartitionStatisticsFile {
            snapshot_id: 3055729675574597004,
            statistics_path: "s3://a/b/partition-stats.parquet".to_string(),
            file_size_in_bytes: 43,
        };

        let build_result = builder
            .set_partition_statistics(statistics.clone())
            .build()
            .unwrap();
        assert_eq!(
            build_result.metadata.partition_statistics,
            HashMap::from_iter(vec![(3055729675574597004, statistics.clone())])
        );
        assert_eq!(build_result.changes, vec![
            TableUpdate::SetPartitionStatistics {
                partition_statistics: statistics.clone()
            }
        ]);

        // Remove
        let builder = build_result.metadata.into_builder(None);
        let build_result = builder
            .remove_partition_statistics(statistics.snapshot_id)
            .build()
            .unwrap();
        assert_eq!(build_result.metadata.partition_statistics.len(), 0);
        assert_eq!(build_result.changes, vec![
            TableUpdate::RemovePartitionStatistics {
                snapshot_id: statistics.snapshot_id
            }
        ]);

        // Remove again yields no changes
        let builder = build_result.metadata.into_builder(None);
        let build_result = builder
            .remove_partition_statistics(statistics.snapshot_id)
            .build()
            .unwrap();
        assert_eq!(build_result.metadata.partition_statistics.len(), 0);
        assert_eq!(build_result.changes.len(), 0);
    }

    #[test]
    fn last_update_increased_for_property_only_update() {
        let builder = builder_without_changes(FormatVersion::V2);

        let metadata = builder.build().unwrap().metadata;
        let last_updated_ms = metadata.last_updated_ms;
        sleep(std::time::Duration::from_millis(2));

        let build_result = metadata
            .into_builder(Some(
                "s3://bucket/test/location/metadata/metadata1.json".to_string(),
            ))
            .set_properties(HashMap::from_iter(vec![(
                "foo".to_string(),
                "bar".to_string(),
            )]))
            .unwrap()
            .build()
            .unwrap();

        assert!(
            build_result.metadata.last_updated_ms > last_updated_ms,
            "{} > {}",
            build_result.metadata.last_updated_ms,
            last_updated_ms
        );
    }

    #[test]
    fn test_construct_default_main_branch() {
        // Load the table without ref
        let file = File::open(format!(
            "{}/testdata/table_metadata/{}",
            env!("CARGO_MANIFEST_DIR"),
            "TableMetadataV2Valid.json"
        ))
        .unwrap();
        let reader = BufReader::new(file);
        let resp = serde_json::from_reader::<_, TableMetadata>(reader).unwrap();

        let table = Table::builder()
            .metadata(resp)
            .metadata_location("s3://bucket/test/location/metadata/v1.json".to_string())
            .identifier(TableIdent::from_strs(["ns1", "test1"]).unwrap())
            .file_io(FileIOBuilder::new("memory").build().unwrap())
            .build()
            .unwrap();

        assert_eq!(
            table.metadata().refs.get(MAIN_BRANCH).unwrap().snapshot_id,
            table.metadata().current_snapshot_id().unwrap()
        );
    }

    #[test]
    fn test_active_schema_cannot_be_removed() {
        let builder = builder_without_changes(FormatVersion::V2);
        builder.remove_schemas(&[0]).unwrap_err();
    }

    #[test]
    fn test_remove_schemas() {
        let file = File::open(format!(
            "{}/testdata/table_metadata/{}",
            env!("CARGO_MANIFEST_DIR"),
            "TableMetadataV2Valid.json"
        ))
        .unwrap();
        let reader = BufReader::new(file);
        let resp = serde_json::from_reader::<_, TableMetadata>(reader).unwrap();

        let table = Table::builder()
            .metadata(resp)
            .metadata_location("s3://bucket/test/location/metadata/v1.json".to_string())
            .identifier(TableIdent::from_strs(["ns1", "test1"]).unwrap())
            .file_io(FileIOBuilder::new("memory").build().unwrap())
            .build()
            .unwrap();

        assert_eq!(2, table.metadata().schemas.len());

        {
            // can not remove active schema
            let meta_data_builder = table.metadata().clone().into_builder(None);
            meta_data_builder.remove_schemas(&[1]).unwrap_err();
        }

        let mut meta_data_builder = table.metadata().clone().into_builder(None);
        meta_data_builder = meta_data_builder.remove_schemas(&[0]).unwrap();
        let build_result = meta_data_builder.build().unwrap();
        assert_eq!(1, build_result.metadata.schemas.len());
        assert_eq!(1, build_result.metadata.current_schema_id);
        assert_eq!(1, build_result.metadata.current_schema().schema_id());
        assert_eq!(1, build_result.changes.len());

        let remove_schema_ids =
            if let TableUpdate::RemoveSchemas { schema_ids } = &build_result.changes[0] {
                schema_ids
            } else {
                unreachable!("Expected RemoveSchema change")
            };
        assert_eq!(remove_schema_ids, &[0]);
    }

    #[test]
    fn test_schema_evolution_now_correctly_validates_partition_field_name_conflicts() {
        let initial_schema = Schema::builder()
            .with_fields(vec![
                NestedField::required(1, "data", Type::Primitive(PrimitiveType::String)).into(),
            ])
            .build()
            .unwrap();

        let partition_spec_with_bucket = UnboundPartitionSpec::builder()
            .with_spec_id(0)
            .add_partition_field(1, "bucket_data", Transform::Bucket(16))
            .unwrap()
            .build();

        let metadata = TableMetadataBuilder::new(
            initial_schema,
            partition_spec_with_bucket,
            SortOrder::unsorted_order(),
            TEST_LOCATION.to_string(),
            FormatVersion::V2,
            HashMap::new(),
        )
        .unwrap()
        .build()
        .unwrap()
        .metadata;

        let partition_field_names: Vec<String> = metadata
            .default_partition_spec()
            .fields()
            .iter()
            .map(|f| f.name.clone())
            .collect();
        assert!(partition_field_names.contains(&"bucket_data".to_string()));

        let evolved_schema = Schema::builder()
            .with_fields(vec![
                NestedField::required(1, "data", Type::Primitive(PrimitiveType::String)).into(),
                // Adding a schema field with the same name as an existing partition field
                NestedField::required(2, "bucket_data", Type::Primitive(PrimitiveType::Int)).into(),
            ])
            .build()
            .unwrap();

        let builder = metadata.into_builder(Some(
            "s3://bucket/test/location/metadata/metadata1.json".to_string(),
        ));

        // Try to add the evolved schema - this should now fail immediately with a clear error
        let result = builder.add_current_schema(evolved_schema);

        assert!(result.is_err());
        let error = result.unwrap_err();
        let error_message = error.message();
        assert!(error_message.contains("Cannot add schema field 'bucket_data' because it conflicts with existing partition field name"));
        assert!(error_message.contains("Schema evolution cannot introduce field names that match existing partition field names"));
    }

    #[test]
    fn test_schema_evolution_should_validate_on_schema_add_not_metadata_build() {
        let initial_schema = Schema::builder()
            .with_fields(vec![
                NestedField::required(1, "data", Type::Primitive(PrimitiveType::String)).into(),
            ])
            .build()
            .unwrap();

        let partition_spec = UnboundPartitionSpec::builder()
            .with_spec_id(0)
            .add_partition_field(1, "partition_col", Transform::Bucket(16))
            .unwrap()
            .build();

        let metadata = TableMetadataBuilder::new(
            initial_schema,
            partition_spec,
            SortOrder::unsorted_order(),
            TEST_LOCATION.to_string(),
            FormatVersion::V2,
            HashMap::new(),
        )
        .unwrap()
        .build()
        .unwrap()
        .metadata;

        let non_conflicting_schema = Schema::builder()
            .with_fields(vec![
                NestedField::required(1, "data", Type::Primitive(PrimitiveType::String)).into(),
                NestedField::required(2, "new_field", Type::Primitive(PrimitiveType::Int)).into(),
            ])
            .build()
            .unwrap();

        // This should succeed since there's no name conflict
        let result = metadata
            .clone()
            .into_builder(Some("test_location".to_string()))
            .add_current_schema(non_conflicting_schema)
            .unwrap()
            .build();

        assert!(result.is_ok());
    }

    #[test]
    fn test_partition_spec_evolution_validates_schema_field_name_conflicts() {
        let initial_schema = Schema::builder()
            .with_fields(vec![
                NestedField::required(1, "data", Type::Primitive(PrimitiveType::String)).into(),
                NestedField::required(2, "existing_field", Type::Primitive(PrimitiveType::Int))
                    .into(),
            ])
            .build()
            .unwrap();

        let partition_spec = UnboundPartitionSpec::builder()
            .with_spec_id(0)
            .add_partition_field(1, "data_bucket", Transform::Bucket(16))
            .unwrap()
            .build();

        let metadata = TableMetadataBuilder::new(
            initial_schema,
            partition_spec,
            SortOrder::unsorted_order(),
            TEST_LOCATION.to_string(),
            FormatVersion::V2,
            HashMap::new(),
        )
        .unwrap()
        .build()
        .unwrap()
        .metadata;

        let builder = metadata.into_builder(Some(
            "s3://bucket/test/location/metadata/metadata1.json".to_string(),
        ));

        let conflicting_partition_spec = UnboundPartitionSpec::builder()
            .with_spec_id(1)
            .add_partition_field(1, "existing_field", Transform::Bucket(8))
            .unwrap()
            .build();

        let result = builder.add_partition_spec(conflicting_partition_spec);

        assert!(result.is_err());
        let error = result.unwrap_err();
        let error_message = error.message();
        // The error comes from our multi-version validation
        assert!(error_message.contains(
            "Cannot create partition with name 'existing_field' that conflicts with schema field"
        ));
        assert!(error_message.contains("and is not an identity transform"));
    }

    #[test]
    fn test_schema_evolution_validates_against_all_historical_schemas() {
        // Create a table with an initial schema that has a field "existing_field"
        let initial_schema = Schema::builder()
            .with_fields(vec![
                NestedField::required(1, "data", Type::Primitive(PrimitiveType::String)).into(),
                NestedField::required(2, "existing_field", Type::Primitive(PrimitiveType::Int))
                    .into(),
            ])
            .build()
            .unwrap();

        let partition_spec = UnboundPartitionSpec::builder()
            .with_spec_id(0)
            .add_partition_field(1, "bucket_data", Transform::Bucket(16))
            .unwrap()
            .build();

        let metadata = TableMetadataBuilder::new(
            initial_schema,
            partition_spec,
            SortOrder::unsorted_order(),
            TEST_LOCATION.to_string(),
            FormatVersion::V2,
            HashMap::new(),
        )
        .unwrap()
        .build()
        .unwrap()
        .metadata;

        // Add a second schema that removes the existing_field but keeps the data field
        let second_schema = Schema::builder()
            .with_fields(vec![
                NestedField::required(1, "data", Type::Primitive(PrimitiveType::String)).into(),
                NestedField::required(3, "new_field", Type::Primitive(PrimitiveType::String))
                    .into(),
            ])
            .build()
            .unwrap();

        let metadata = metadata
            .into_builder(Some("test_location".to_string()))
            .add_current_schema(second_schema)
            .unwrap()
            .build()
            .unwrap()
            .metadata;

        // Now try to add a third schema that reintroduces "existing_field"
        // This should succeed because "existing_field" exists in a historical schema,
        // even though there's a partition field named "bucket_data"
        let third_schema = Schema::builder()
            .with_fields(vec![
                NestedField::required(1, "data", Type::Primitive(PrimitiveType::String)).into(),
                NestedField::required(3, "new_field", Type::Primitive(PrimitiveType::String))
                    .into(),
                NestedField::required(4, "existing_field", Type::Primitive(PrimitiveType::Int))
                    .into(),
            ])
            .build()
            .unwrap();

        let builder = metadata
            .clone()
            .into_builder(Some("test_location".to_string()));

        // This should succeed because "existing_field" exists in a historical schema
        let result = builder.add_current_schema(third_schema);
        assert!(result.is_ok());

        // However, trying to add a schema field that conflicts with the partition field
        // and doesn't exist in any historical schema should fail
        let conflicting_schema = Schema::builder()
            .with_fields(vec![
                NestedField::required(1, "data", Type::Primitive(PrimitiveType::String)).into(),
                NestedField::required(3, "new_field", Type::Primitive(PrimitiveType::String))
                    .into(),
                NestedField::required(4, "existing_field", Type::Primitive(PrimitiveType::Int))
                    .into(),
                NestedField::required(5, "bucket_data", Type::Primitive(PrimitiveType::String))
                    .into(), // conflicts with partition field
            ])
            .build()
            .unwrap();

        let builder2 = metadata.into_builder(Some("test_location".to_string()));
        let result2 = builder2.add_current_schema(conflicting_schema);

        // This should fail because "bucket_data" conflicts with partition field name
        // and doesn't exist in any historical schema
        assert!(result2.is_err());
        let error = result2.unwrap_err();
        assert!(error.message().contains("Cannot add schema field 'bucket_data' because it conflicts with existing partition field name"));
    }

    #[test]
    fn test_schema_evolution_allows_existing_partition_field_if_exists_in_historical_schema() {
        // Create initial schema with a field
        let initial_schema = Schema::builder()
            .with_fields(vec![
                NestedField::required(1, "data", Type::Primitive(PrimitiveType::String)).into(),
                NestedField::required(2, "partition_data", Type::Primitive(PrimitiveType::Int))
                    .into(),
            ])
            .build()
            .unwrap();

        let partition_spec = UnboundPartitionSpec::builder()
            .with_spec_id(0)
            .add_partition_field(2, "partition_data", Transform::Identity)
            .unwrap()
            .build();

        let metadata = TableMetadataBuilder::new(
            initial_schema,
            partition_spec,
            SortOrder::unsorted_order(),
            TEST_LOCATION.to_string(),
            FormatVersion::V2,
            HashMap::new(),
        )
        .unwrap()
        .build()
        .unwrap()
        .metadata;

        // Add a new schema that still contains the partition_data field
        let evolved_schema = Schema::builder()
            .with_fields(vec![
                NestedField::required(1, "data", Type::Primitive(PrimitiveType::String)).into(),
                NestedField::required(2, "partition_data", Type::Primitive(PrimitiveType::Int))
                    .into(),
                NestedField::required(3, "new_field", Type::Primitive(PrimitiveType::String))
                    .into(),
            ])
            .build()
            .unwrap();

        // This should succeed because partition_data exists in historical schemas
        let result = metadata
            .into_builder(Some("test_location".to_string()))
            .add_current_schema(evolved_schema);

        assert!(result.is_ok());
    }

    #[test]
    fn test_schema_evolution_prevents_new_field_conflicting_with_partition_field() {
        // Create initial schema WITHOUT the conflicting field
        let initial_schema = Schema::builder()
            .with_fields(vec![
                NestedField::required(1, "data", Type::Primitive(PrimitiveType::String)).into(),
            ])
            .build()
            .unwrap();

        let partition_spec = UnboundPartitionSpec::builder()
            .with_spec_id(0)
            .add_partition_field(1, "bucket_data", Transform::Bucket(16))
            .unwrap()
            .build();

        let metadata = TableMetadataBuilder::new(
            initial_schema,
            partition_spec,
            SortOrder::unsorted_order(),
            TEST_LOCATION.to_string(),
            FormatVersion::V2,
            HashMap::new(),
        )
        .unwrap()
        .build()
        .unwrap()
        .metadata;

        // Try to add a schema with a field that conflicts with partition field name
        let conflicting_schema = Schema::builder()
            .with_fields(vec![
                NestedField::required(1, "data", Type::Primitive(PrimitiveType::String)).into(),
                // This field name conflicts with the partition field "bucket_data"
                NestedField::required(2, "bucket_data", Type::Primitive(PrimitiveType::Int)).into(),
            ])
            .build()
            .unwrap();

        let builder = metadata.into_builder(Some("test_location".to_string()));
        let result = builder.add_current_schema(conflicting_schema);

        // This should fail because "bucket_data" conflicts with partition field name
        // and doesn't exist in any historical schema
        assert!(result.is_err());
        let error = result.unwrap_err();
        assert!(error.message().contains("Cannot add schema field 'bucket_data' because it conflicts with existing partition field name"));
    }

    #[test]
    fn test_partition_spec_evolution_allows_non_conflicting_names() {
        let initial_schema = Schema::builder()
            .with_fields(vec![
                NestedField::required(1, "data", Type::Primitive(PrimitiveType::String)).into(),
                NestedField::required(2, "existing_field", Type::Primitive(PrimitiveType::Int))
                    .into(),
            ])
            .build()
            .unwrap();

        let partition_spec = UnboundPartitionSpec::builder()
            .with_spec_id(0)
            .add_partition_field(1, "data_bucket", Transform::Bucket(16))
            .unwrap()
            .build();

        let metadata = TableMetadataBuilder::new(
            initial_schema,
            partition_spec,
            SortOrder::unsorted_order(),
            TEST_LOCATION.to_string(),
            FormatVersion::V2,
            HashMap::new(),
        )
        .unwrap()
        .build()
        .unwrap()
        .metadata;

        let builder = metadata.into_builder(Some(
            "s3://bucket/test/location/metadata/metadata1.json".to_string(),
        ));

        // Try to add a partition spec with a field name that does NOT conflict with existing schema fields
        let non_conflicting_partition_spec = UnboundPartitionSpec::builder()
            .with_spec_id(1)
            .add_partition_field(2, "new_partition_field", Transform::Bucket(8))
            .unwrap()
            .build();

        let result = builder.add_partition_spec(non_conflicting_partition_spec);

        assert!(result.is_ok());
    }

    #[test]
    fn test_row_lineage_addition() {
        let new_rows = 30;
        let base = builder_without_changes(FormatVersion::V3)
            .build()
            .unwrap()
            .metadata;
        let add_rows = Snapshot::builder()
            .with_snapshot_id(0)
            .with_timestamp_ms(base.last_updated_ms + 1)
            .with_sequence_number(0)
            .with_schema_id(0)
            .with_manifest_list("foo")
            .with_parent_snapshot_id(None)
            .with_summary(Summary {
                operation: Operation::Append,
                additional_properties: HashMap::new(),
            })
            .with_row_range(base.next_row_id(), new_rows)
            .build();

        let first_addition = base
            .into_builder(None)
            .add_snapshot(add_rows.clone())
            .unwrap()
            .build()
            .unwrap()
            .metadata;

        assert_eq!(first_addition.next_row_id(), new_rows);

        let add_more_rows = Snapshot::builder()
            .with_snapshot_id(1)
            .with_timestamp_ms(first_addition.last_updated_ms + 1)
            .with_sequence_number(1)
            .with_schema_id(0)
            .with_manifest_list("foo")
            .with_parent_snapshot_id(Some(0))
            .with_summary(Summary {
                operation: Operation::Append,
                additional_properties: HashMap::new(),
            })
            .with_row_range(first_addition.next_row_id(), new_rows)
            .build();

        let second_addition = first_addition
            .into_builder(None)
            .add_snapshot(add_more_rows)
            .unwrap()
            .build()
            .unwrap()
            .metadata;
        assert_eq!(second_addition.next_row_id(), new_rows * 2);
    }

    #[test]
    fn test_row_lineage_invalid_snapshot() {
        let new_rows = 30;
        let base = builder_without_changes(FormatVersion::V3)
            .build()
            .unwrap()
            .metadata;

        // add rows to check TableMetadata validation; Snapshot rejects negative next-row-id
        let add_rows = Snapshot::builder()
            .with_snapshot_id(0)
            .with_timestamp_ms(base.last_updated_ms + 1)
            .with_sequence_number(0)
            .with_schema_id(0)
            .with_manifest_list("foo")
            .with_parent_snapshot_id(None)
            .with_summary(Summary {
                operation: Operation::Append,
                additional_properties: HashMap::new(),
            })
            .with_row_range(base.next_row_id(), new_rows)
            .build();

        let added = base
            .into_builder(None)
            .add_snapshot(add_rows)
            .unwrap()
            .build()
            .unwrap()
            .metadata;

        let invalid_new_rows = Snapshot::builder()
            .with_snapshot_id(1)
            .with_timestamp_ms(added.last_updated_ms + 1)
            .with_sequence_number(1)
            .with_schema_id(0)
            .with_manifest_list("foo")
            .with_parent_snapshot_id(Some(0))
            .with_summary(Summary {
                operation: Operation::Append,
                additional_properties: HashMap::new(),
            })
            // first_row_id is behind table next_row_id
            .with_row_range(added.next_row_id() - 1, 10)
            .build();

        let err = added
            .into_builder(None)
            .add_snapshot(invalid_new_rows)
            .unwrap_err();
        assert!(
            err.to_string().contains(
                "Cannot add a snapshot, first-row-id is behind table next-row-id: 29 < 30"
            )
        );
    }

    #[test]
    fn test_row_lineage_append_branch() {
        // Appends to a branch should still change last-row-id even if not on main, these changes
        // should also affect commits to main

        let branch = "some_branch";

        // Start with V3 metadata to support row lineage
        let base = builder_without_changes(FormatVersion::V3)
            .build()
            .unwrap()
            .metadata;

        // Initial next_row_id should be 0
        assert_eq!(base.next_row_id(), 0);

        // Write to Branch - append 30 rows
        let branch_snapshot_1 = Snapshot::builder()
            .with_snapshot_id(1)
            .with_timestamp_ms(base.last_updated_ms + 1)
            .with_sequence_number(0)
            .with_schema_id(0)
            .with_manifest_list("foo")
            .with_parent_snapshot_id(None)
            .with_summary(Summary {
                operation: Operation::Append,
                additional_properties: HashMap::new(),
            })
            .with_row_range(base.next_row_id(), 30)
            .build();

        let table_after_branch_1 = base
            .into_builder(None)
            .set_branch_snapshot(branch_snapshot_1.clone(), branch)
            .unwrap()
            .build()
            .unwrap()
            .metadata;

        // Current snapshot should be null (no main branch snapshot yet)
        assert!(table_after_branch_1.current_snapshot().is_none());

        // Branch snapshot should have first_row_id = 0
        let branch_ref = table_after_branch_1.refs.get(branch).unwrap();
        let branch_snap_1 = table_after_branch_1
            .snapshots
            .get(&branch_ref.snapshot_id)
            .unwrap();
        assert_eq!(branch_snap_1.first_row_id(), Some(0));

        // Next row id should be 30
        assert_eq!(table_after_branch_1.next_row_id(), 30);

        // Write to Main - append 28 rows
        let main_snapshot = Snapshot::builder()
            .with_snapshot_id(2)
            .with_timestamp_ms(table_after_branch_1.last_updated_ms + 1)
            .with_sequence_number(1)
            .with_schema_id(0)
            .with_manifest_list("bar")
            .with_parent_snapshot_id(None)
            .with_summary(Summary {
                operation: Operation::Append,
                additional_properties: HashMap::new(),
            })
            .with_row_range(table_after_branch_1.next_row_id(), 28)
            .build();

        let table_after_main = table_after_branch_1
            .into_builder(None)
            .add_snapshot(main_snapshot.clone())
            .unwrap()
            .set_ref(MAIN_BRANCH, SnapshotReference {
                snapshot_id: main_snapshot.snapshot_id(),
                retention: SnapshotRetention::Branch {
                    min_snapshots_to_keep: None,
                    max_snapshot_age_ms: None,
                    max_ref_age_ms: None,
                },
            })
            .unwrap()
            .build()
            .unwrap()
            .metadata;

        // Main snapshot should have first_row_id = 30
        let current_snapshot = table_after_main.current_snapshot().unwrap();
        assert_eq!(current_snapshot.first_row_id(), Some(30));

        // Next row id should be 58 (30 + 28)
        assert_eq!(table_after_main.next_row_id(), 58);

        // Write again to branch - append 21 rows
        let branch_snapshot_2 = Snapshot::builder()
            .with_snapshot_id(3)
            .with_timestamp_ms(table_after_main.last_updated_ms + 1)
            .with_sequence_number(2)
            .with_schema_id(0)
            .with_manifest_list("baz")
            .with_parent_snapshot_id(Some(branch_snapshot_1.snapshot_id()))
            .with_summary(Summary {
                operation: Operation::Append,
                additional_properties: HashMap::new(),
            })
            .with_row_range(table_after_main.next_row_id(), 21)
            .build();

        let table_after_branch_2 = table_after_main
            .into_builder(None)
            .set_branch_snapshot(branch_snapshot_2.clone(), branch)
            .unwrap()
            .build()
            .unwrap()
            .metadata;

        // Branch snapshot should have first_row_id = 58 (30 + 28)
        let branch_ref_2 = table_after_branch_2.refs.get(branch).unwrap();
        let branch_snap_2 = table_after_branch_2
            .snapshots
            .get(&branch_ref_2.snapshot_id)
            .unwrap();
        assert_eq!(branch_snap_2.first_row_id(), Some(58));

        // Next row id should be 79 (30 + 28 + 21)
        assert_eq!(table_after_branch_2.next_row_id(), 79);
    }

    #[test]
    fn test_encryption_keys() {
        let builder = builder_without_changes(FormatVersion::V2);

        // Create test encryption keys
        let encryption_key_1 = EncryptedKey::builder()
            .key_id("key-1")
            .encrypted_key_metadata(vec![1, 2, 3, 4])
            .encrypted_by_id("encryption-service-1")
            .properties(HashMap::from_iter(vec![(
                "algorithm".to_string(),
                "AES-256".to_string(),
            )]))
            .build();

        let encryption_key_2 = EncryptedKey::builder()
            .key_id("key-2")
            .encrypted_key_metadata(vec![5, 6, 7, 8])
            .encrypted_by_id("encryption-service-2")
            .properties(HashMap::new())
            .build();

        // Add first encryption key
        let build_result = builder
            .add_encryption_key(encryption_key_1.clone())
            .build()
            .unwrap();

        assert_eq!(build_result.changes.len(), 1);
        assert_eq!(build_result.metadata.encryption_keys.len(), 1);
        assert_eq!(
            build_result.metadata.encryption_key("key-1"),
            Some(&encryption_key_1)
        );
        assert_eq!(build_result.changes[0], TableUpdate::AddEncryptionKey {
            encryption_key: encryption_key_1.clone()
        });

        // Add second encryption key
        let build_result = build_result
            .metadata
            .into_builder(Some(
                "s3://bucket/test/location/metadata/metadata1.json".to_string(),
            ))
            .add_encryption_key(encryption_key_2.clone())
            .build()
            .unwrap();

        assert_eq!(build_result.changes.len(), 1);
        assert_eq!(build_result.metadata.encryption_keys.len(), 2);
        assert_eq!(
            build_result.metadata.encryption_key("key-1"),
            Some(&encryption_key_1)
        );
        assert_eq!(
            build_result.metadata.encryption_key("key-2"),
            Some(&encryption_key_2)
        );
        assert_eq!(build_result.changes[0], TableUpdate::AddEncryptionKey {
            encryption_key: encryption_key_2.clone()
        });

        // Try to add duplicate key - should not create a change
        let build_result = build_result
            .metadata
            .into_builder(Some(
                "s3://bucket/test/location/metadata/metadata2.json".to_string(),
            ))
            .add_encryption_key(encryption_key_1.clone())
            .build()
            .unwrap();

        assert_eq!(build_result.changes.len(), 0);
        assert_eq!(build_result.metadata.encryption_keys.len(), 2);

        // Remove first encryption key
        let build_result = build_result
            .metadata
            .into_builder(Some(
                "s3://bucket/test/location/metadata/metadata3.json".to_string(),
            ))
            .remove_encryption_key("key-1")
            .build()
            .unwrap();

        assert_eq!(build_result.changes.len(), 1);
        assert_eq!(build_result.metadata.encryption_keys.len(), 1);
        assert_eq!(build_result.metadata.encryption_key("key-1"), None);
        assert_eq!(
            build_result.metadata.encryption_key("key-2"),
            Some(&encryption_key_2)
        );
        assert_eq!(build_result.changes[0], TableUpdate::RemoveEncryptionKey {
            key_id: "key-1".to_string()
        });

        // Try to remove non-existent key - should not create a change
        let build_result = build_result
            .metadata
            .into_builder(Some(
                "s3://bucket/test/location/metadata/metadata4.json".to_string(),
            ))
            .remove_encryption_key("non-existent-key")
            .build()
            .unwrap();

        assert_eq!(build_result.changes.len(), 0);
        assert_eq!(build_result.metadata.encryption_keys.len(), 1);

        // Test encryption_keys_iter()
        let keys = build_result
            .metadata
            .encryption_keys_iter()
            .collect::<Vec<_>>();
        assert_eq!(keys.len(), 1);
        assert_eq!(keys[0], &encryption_key_2);

        // Remove last encryption key
        let build_result = build_result
            .metadata
            .into_builder(Some(
                "s3://bucket/test/location/metadata/metadata5.json".to_string(),
            ))
            .remove_encryption_key("key-2")
            .build()
            .unwrap();

        assert_eq!(build_result.changes.len(), 1);
        assert_eq!(build_result.metadata.encryption_keys.len(), 0);
        assert_eq!(build_result.metadata.encryption_key("key-2"), None);
        assert_eq!(build_result.changes[0], TableUpdate::RemoveEncryptionKey {
            key_id: "key-2".to_string()
        });

        // Verify empty encryption_keys_iter()
        let keys = build_result.metadata.encryption_keys_iter();
        assert_eq!(keys.len(), 0);
    }
}
