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

//! Catalog API for Apache Iceberg

pub mod memory;
mod metadata_location;

use std::collections::HashMap;
use std::fmt::{Debug, Display};
use std::future::Future;
use std::mem::take;
use std::ops::Deref;
use std::str::FromStr;
use std::sync::Arc;

use _serde::{deserialize_snapshot, serialize_snapshot};
use async_trait::async_trait;
pub use memory::MemoryCatalog;
pub use metadata_location::*;
#[cfg(test)]
use mockall::automock;
use serde_derive::{Deserialize, Serialize};
use typed_builder::TypedBuilder;
use uuid::Uuid;

use crate::spec::{
    EncryptedKey, FormatVersion, PartitionStatisticsFile, Schema, SchemaId, Snapshot,
    SnapshotReference, SortOrder, StatisticsFile, TableMetadata, TableMetadataBuilder,
    UnboundPartitionSpec, ViewFormatVersion, ViewRepresentations, ViewVersion,
};
use crate::table::Table;
use crate::{Error, ErrorKind, Result};

/// The catalog API for Iceberg Rust.
#[async_trait]
#[cfg_attr(test, automock)]
pub trait Catalog: Debug + Sync + Send {
    /// List namespaces inside the catalog.
    async fn list_namespaces(&self, parent: Option<&NamespaceIdent>)
    -> Result<Vec<NamespaceIdent>>;

    /// Create a new namespace inside the catalog.
    async fn create_namespace(
        &self,
        namespace: &NamespaceIdent,
        properties: HashMap<String, String>,
    ) -> Result<Namespace>;

    /// Get a namespace information from the catalog.
    async fn get_namespace(&self, namespace: &NamespaceIdent) -> Result<Namespace>;

    /// Check if namespace exists in catalog.
    async fn namespace_exists(&self, namespace: &NamespaceIdent) -> Result<bool>;

    /// Update a namespace inside the catalog.
    ///
    /// # Behavior
    ///
    /// The properties must be the full set of namespace.
    async fn update_namespace(
        &self,
        namespace: &NamespaceIdent,
        properties: HashMap<String, String>,
    ) -> Result<()>;

    /// Drop a namespace from the catalog, or returns error if it doesn't exist.
    async fn drop_namespace(&self, namespace: &NamespaceIdent) -> Result<()>;

    /// List tables from namespace.
    async fn list_tables(&self, namespace: &NamespaceIdent) -> Result<Vec<TableIdent>>;

    /// Create a new table inside the namespace.
    async fn create_table(
        &self,
        namespace: &NamespaceIdent,
        creation: TableCreation,
    ) -> Result<Table>;

    /// Load table from the catalog.
    async fn load_table(&self, table: &TableIdent) -> Result<Table>;

    /// Drop a table from the catalog, or returns error if it doesn't exist.
    async fn drop_table(&self, table: &TableIdent) -> Result<()>;

    /// Check if a table exists in the catalog.
    async fn table_exists(&self, table: &TableIdent) -> Result<bool>;

    /// Rename a table in the catalog.
    async fn rename_table(&self, src: &TableIdent, dest: &TableIdent) -> Result<()>;

    /// Register an existing table to the catalog.
    async fn register_table(&self, table: &TableIdent, metadata_location: String) -> Result<Table>;

    /// Update a table to the catalog.
    async fn update_table(&self, commit: TableCommit) -> Result<Table>;
}

/// Common interface for all catalog builders.
pub trait CatalogBuilder: Default + Debug + Send + Sync {
    /// The catalog type that this builder creates.
    type C: Catalog;
    /// Create a new catalog instance.
    fn load(
        self,
        name: impl Into<String>,
        props: HashMap<String, String>,
    ) -> impl Future<Output = Result<Self::C>> + Send;
}

/// NamespaceIdent represents the identifier of a namespace in the catalog.
///
/// The namespace identifier is a list of strings, where each string is a
/// component of the namespace. It's the catalog implementer's responsibility to
/// handle the namespace identifier correctly.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct NamespaceIdent(Vec<String>);

impl NamespaceIdent {
    /// Create a new namespace identifier with only one level.
    pub fn new(name: String) -> Self {
        Self(vec![name])
    }

    /// Create a multi-level namespace identifier from vector.
    pub fn from_vec(names: Vec<String>) -> Result<Self> {
        if names.is_empty() {
            return Err(Error::new(
                ErrorKind::DataInvalid,
                "Namespace identifier can't be empty!",
            ));
        }
        Ok(Self(names))
    }

    /// Try to create namespace identifier from an iterator of string.
    pub fn from_strs(iter: impl IntoIterator<Item = impl ToString>) -> Result<Self> {
        Self::from_vec(iter.into_iter().map(|s| s.to_string()).collect())
    }

    /// Returns a string for used in url.
    pub fn to_url_string(&self) -> String {
        self.as_ref().join("\u{001f}")
    }

    /// Returns inner strings.
    pub fn inner(self) -> Vec<String> {
        self.0
    }

    /// Get the parent of this namespace.
    /// Returns None if this namespace only has a single element and thus has no parent.
    pub fn parent(&self) -> Option<Self> {
        self.0.split_last().and_then(|(_, parent)| {
            if parent.is_empty() {
                None
            } else {
                Some(Self(parent.to_vec()))
            }
        })
    }
}

impl AsRef<Vec<String>> for NamespaceIdent {
    fn as_ref(&self) -> &Vec<String> {
        &self.0
    }
}

impl Deref for NamespaceIdent {
    type Target = [String];

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

/// Namespace represents a namespace in the catalog.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Namespace {
    name: NamespaceIdent,
    properties: HashMap<String, String>,
}

impl Namespace {
    /// Create a new namespace.
    pub fn new(name: NamespaceIdent) -> Self {
        Self::with_properties(name, HashMap::default())
    }

    /// Create a new namespace with properties.
    pub fn with_properties(name: NamespaceIdent, properties: HashMap<String, String>) -> Self {
        Self { name, properties }
    }

    /// Get the name of the namespace.
    pub fn name(&self) -> &NamespaceIdent {
        &self.name
    }

    /// Get the properties of the namespace.
    pub fn properties(&self) -> &HashMap<String, String> {
        &self.properties
    }
}

impl Display for NamespaceIdent {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0.join("."))
    }
}

/// TableIdent represents the identifier of a table in the catalog.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct TableIdent {
    /// Namespace of the table.
    pub namespace: NamespaceIdent,
    /// Table name.
    pub name: String,
}

impl TableIdent {
    /// Create a new table identifier.
    pub fn new(namespace: NamespaceIdent, name: String) -> Self {
        Self { namespace, name }
    }

    /// Get the namespace of the table.
    pub fn namespace(&self) -> &NamespaceIdent {
        &self.namespace
    }

    /// Get the name of the table.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Try to create table identifier from an iterator of string.
    pub fn from_strs(iter: impl IntoIterator<Item = impl ToString>) -> Result<Self> {
        let mut vec: Vec<String> = iter.into_iter().map(|s| s.to_string()).collect();
        let table_name = vec.pop().ok_or_else(|| {
            Error::new(ErrorKind::DataInvalid, "Table identifier can't be empty!")
        })?;
        let namespace_ident = NamespaceIdent::from_vec(vec)?;

        Ok(Self {
            namespace: namespace_ident,
            name: table_name,
        })
    }
}

impl Display for TableIdent {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}.{}", self.namespace, self.name)
    }
}

/// TableCreation represents the creation of a table in the catalog.
#[derive(Debug, TypedBuilder)]
pub struct TableCreation {
    /// The name of the table.
    pub name: String,
    /// The location of the table.
    #[builder(default, setter(strip_option(fallback = location_opt)))]
    pub location: Option<String>,
    /// The schema of the table.
    pub schema: Schema,
    /// The partition spec of the table, could be None.
    #[builder(default, setter(strip_option(fallback = partition_spec_opt), into))]
    pub partition_spec: Option<UnboundPartitionSpec>,
    /// The sort order of the table.
    #[builder(default, setter(strip_option(fallback = sort_order_opt)))]
    pub sort_order: Option<SortOrder>,
    /// The properties of the table.
    #[builder(default, setter(transform = |props: impl IntoIterator<Item=(String, String)>| {
        props.into_iter().collect()
    }))]
    pub properties: HashMap<String, String>,
    /// Format version of the table. Defaults to V2.
    #[builder(default = FormatVersion::V2)]
    pub format_version: FormatVersion,
}

/// TableCommit represents the commit of a table in the catalog.
///
/// The builder is marked as private since it's dangerous and error-prone to construct
/// [`TableCommit`] directly.
/// Users are supposed to use [`crate::transaction::Transaction`] to update table.
#[derive(Debug, TypedBuilder)]
#[builder(build_method(vis = "pub(crate)"))]
pub struct TableCommit {
    /// The table ident.
    ident: TableIdent,
    /// The requirements of the table.
    ///
    /// Commit will fail if the requirements are not met.
    requirements: Vec<TableRequirement>,
    /// The updates of the table.
    updates: Vec<TableUpdate>,
}

impl TableCommit {
    /// Return the table identifier.
    pub fn identifier(&self) -> &TableIdent {
        &self.ident
    }

    /// Take all requirements.
    pub fn take_requirements(&mut self) -> Vec<TableRequirement> {
        take(&mut self.requirements)
    }

    /// Take all updates.
    pub fn take_updates(&mut self) -> Vec<TableUpdate> {
        take(&mut self.updates)
    }

    /// Applies this [`TableCommit`] to the given [`Table`] as part of a catalog update.
    /// Typically used by [`Catalog::update_table`] to validate requirements and apply metadata updates.
    ///
    /// Returns a new [`Table`] with updated metadata,
    /// or an error if validation or application fails.
    pub fn apply(self, table: Table) -> Result<Table> {
        // check requirements
        for requirement in self.requirements {
            requirement.check(Some(table.metadata()))?;
        }

        // get current metadata location
        let current_metadata_location = table.metadata_location_result()?;

        // apply updates to metadata builder
        let mut metadata_builder = table
            .metadata()
            .clone()
            .into_builder(Some(current_metadata_location.to_string()));
        for update in self.updates {
            metadata_builder = update.apply(metadata_builder)?;
        }

        // Bump the version of metadata
        let new_metadata_location = MetadataLocation::from_str(current_metadata_location)?
            .with_next_version()
            .to_string();

        Ok(table
            .with_metadata(Arc::new(metadata_builder.build()?.metadata))
            .with_metadata_location(new_metadata_location))
    }
}

/// TableRequirement represents a requirement for a table in the catalog.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
#[serde(tag = "type")]
pub enum TableRequirement {
    /// The table must not already exist; used for create transactions
    #[serde(rename = "assert-create")]
    NotExist,
    /// The table UUID must match the requirement.
    #[serde(rename = "assert-table-uuid")]
    UuidMatch {
        /// Uuid of original table.
        uuid: Uuid,
    },
    /// The table branch or tag identified by the requirement's `reference` must
    /// reference the requirement's `snapshot-id`.
    #[serde(rename = "assert-ref-snapshot-id")]
    RefSnapshotIdMatch {
        /// The reference of the table to assert.
        r#ref: String,
        /// The snapshot id of the table to assert.
        /// If the id is `None`, the ref must not already exist.
        #[serde(rename = "snapshot-id")]
        snapshot_id: Option<i64>,
    },
    /// The table's last assigned column id must match the requirement.
    #[serde(rename = "assert-last-assigned-field-id")]
    LastAssignedFieldIdMatch {
        /// The last assigned field id of the table to assert.
        #[serde(rename = "last-assigned-field-id")]
        last_assigned_field_id: i32,
    },
    /// The table's current schema id must match the requirement.
    #[serde(rename = "assert-current-schema-id")]
    CurrentSchemaIdMatch {
        /// Current schema id of the table to assert.
        #[serde(rename = "current-schema-id")]
        current_schema_id: SchemaId,
    },
    /// The table's last assigned partition id must match the
    /// requirement.
    #[serde(rename = "assert-last-assigned-partition-id")]
    LastAssignedPartitionIdMatch {
        /// Last assigned partition id of the table to assert.
        #[serde(rename = "last-assigned-partition-id")]
        last_assigned_partition_id: i32,
    },
    /// The table's default spec id must match the requirement.
    #[serde(rename = "assert-default-spec-id")]
    DefaultSpecIdMatch {
        /// Default spec id of the table to assert.
        #[serde(rename = "default-spec-id")]
        default_spec_id: i32,
    },
    /// The table's default sort order id must match the requirement.
    #[serde(rename = "assert-default-sort-order-id")]
    DefaultSortOrderIdMatch {
        /// Default sort order id of the table to assert.
        #[serde(rename = "default-sort-order-id")]
        default_sort_order_id: i64,
    },
}

/// TableUpdate represents an update to a table in the catalog.
#[derive(Debug, Serialize, Deserialize, PartialEq, Clone)]
#[serde(tag = "action", rename_all = "kebab-case")]
#[allow(clippy::large_enum_variant)]
pub enum TableUpdate {
    /// Upgrade table's format version
    #[serde(rename_all = "kebab-case")]
    UpgradeFormatVersion {
        /// Target format upgrade to.
        format_version: FormatVersion,
    },
    /// Assign a new UUID to the table
    #[serde(rename_all = "kebab-case")]
    AssignUuid {
        /// The new UUID to assign.
        uuid: Uuid,
    },
    /// Add a new schema to the table
    #[serde(rename_all = "kebab-case")]
    AddSchema {
        /// The schema to add.
        schema: Schema,
    },
    /// Set table's current schema
    #[serde(rename_all = "kebab-case")]
    SetCurrentSchema {
        /// Schema ID to set as current, or -1 to set last added schema
        schema_id: i32,
    },
    /// Add a new partition spec to the table
    AddSpec {
        /// The partition spec to add.
        spec: UnboundPartitionSpec,
    },
    /// Set table's default spec
    #[serde(rename_all = "kebab-case")]
    SetDefaultSpec {
        /// Partition spec ID to set as the default, or -1 to set last added spec
        spec_id: i32,
    },
    /// Add sort order to table.
    #[serde(rename_all = "kebab-case")]
    AddSortOrder {
        /// Sort order to add.
        sort_order: SortOrder,
    },
    /// Set table's default sort order
    #[serde(rename_all = "kebab-case")]
    SetDefaultSortOrder {
        /// Sort order ID to set as the default, or -1 to set last added sort order
        sort_order_id: i64,
    },
    /// Add snapshot to table.
    #[serde(rename_all = "kebab-case")]
    AddSnapshot {
        /// Snapshot to add.
        #[serde(
            deserialize_with = "deserialize_snapshot",
            serialize_with = "serialize_snapshot"
        )]
        snapshot: Snapshot,
    },
    /// Set table's snapshot ref.
    #[serde(rename_all = "kebab-case")]
    SetSnapshotRef {
        /// Name of snapshot reference to set.
        ref_name: String,
        /// Snapshot reference to set.
        #[serde(flatten)]
        reference: SnapshotReference,
    },
    /// Remove table's snapshots
    #[serde(rename_all = "kebab-case")]
    RemoveSnapshots {
        /// Snapshot ids to remove.
        snapshot_ids: Vec<i64>,
    },
    /// Remove snapshot reference
    #[serde(rename_all = "kebab-case")]
    RemoveSnapshotRef {
        /// Name of snapshot reference to remove.
        ref_name: String,
    },
    /// Update table's location
    SetLocation {
        /// New location for table.
        location: String,
    },
    /// Update table's properties
    SetProperties {
        /// Properties to update for table.
        updates: HashMap<String, String>,
    },
    /// Remove table's properties
    RemoveProperties {
        /// Properties to remove
        removals: Vec<String>,
    },
    /// Remove partition specs
    #[serde(rename_all = "kebab-case")]
    RemovePartitionSpecs {
        /// Partition spec ids to remove.
        spec_ids: Vec<i32>,
    },
    /// Set statistics for a snapshot
    #[serde(with = "_serde_set_statistics")]
    SetStatistics {
        /// File containing the statistics
        statistics: StatisticsFile,
    },
    /// Remove statistics for a snapshot
    #[serde(rename_all = "kebab-case")]
    RemoveStatistics {
        /// Snapshot id to remove statistics for.
        snapshot_id: i64,
    },
    /// Set partition statistics for a snapshot
    #[serde(rename_all = "kebab-case")]
    SetPartitionStatistics {
        /// File containing the partition statistics
        partition_statistics: PartitionStatisticsFile,
    },
    /// Remove partition statistics for a snapshot
    #[serde(rename_all = "kebab-case")]
    RemovePartitionStatistics {
        /// Snapshot id to remove partition statistics for.
        snapshot_id: i64,
    },
    /// Remove schemas
    #[serde(rename_all = "kebab-case")]
    RemoveSchemas {
        /// Schema IDs to remove.
        schema_ids: Vec<i32>,
    },
    /// Add an encryption key
    #[serde(rename_all = "kebab-case")]
    AddEncryptionKey {
        /// The encryption key to add.
        encryption_key: EncryptedKey,
    },
    /// Remove an encryption key
    #[serde(rename_all = "kebab-case")]
    RemoveEncryptionKey {
        /// The id of the encryption key to remove.
        key_id: String,
    },
}

impl TableUpdate {
    /// Applies the update to the table metadata builder.
    pub fn apply(self, builder: TableMetadataBuilder) -> Result<TableMetadataBuilder> {
        match self {
            TableUpdate::AssignUuid { uuid } => Ok(builder.assign_uuid(uuid)),
            TableUpdate::AddSchema { schema, .. } => Ok(builder.add_schema(schema)?),
            TableUpdate::SetCurrentSchema { schema_id } => builder.set_current_schema(schema_id),
            TableUpdate::AddSpec { spec } => builder.add_partition_spec(spec),
            TableUpdate::SetDefaultSpec { spec_id } => builder.set_default_partition_spec(spec_id),
            TableUpdate::AddSortOrder { sort_order } => builder.add_sort_order(sort_order),
            TableUpdate::SetDefaultSortOrder { sort_order_id } => {
                builder.set_default_sort_order(sort_order_id)
            }
            TableUpdate::AddSnapshot { snapshot } => builder.add_snapshot(snapshot),
            TableUpdate::SetSnapshotRef {
                ref_name,
                reference,
            } => builder.set_ref(&ref_name, reference),
            TableUpdate::RemoveSnapshots { snapshot_ids } => {
                Ok(builder.remove_snapshots(&snapshot_ids))
            }
            TableUpdate::RemoveSnapshotRef { ref_name } => Ok(builder.remove_ref(&ref_name)),
            TableUpdate::SetLocation { location } => Ok(builder.set_location(location)),
            TableUpdate::SetProperties { updates } => builder.set_properties(updates),
            TableUpdate::RemoveProperties { removals } => builder.remove_properties(&removals),
            TableUpdate::UpgradeFormatVersion { format_version } => {
                builder.upgrade_format_version(format_version)
            }
            TableUpdate::RemovePartitionSpecs { spec_ids } => {
                builder.remove_partition_specs(&spec_ids)
            }
            TableUpdate::SetStatistics { statistics } => Ok(builder.set_statistics(statistics)),
            TableUpdate::RemoveStatistics { snapshot_id } => {
                Ok(builder.remove_statistics(snapshot_id))
            }
            TableUpdate::SetPartitionStatistics {
                partition_statistics,
            } => Ok(builder.set_partition_statistics(partition_statistics)),
            TableUpdate::RemovePartitionStatistics { snapshot_id } => {
                Ok(builder.remove_partition_statistics(snapshot_id))
            }
            TableUpdate::RemoveSchemas { schema_ids } => builder.remove_schemas(&schema_ids),
            TableUpdate::AddEncryptionKey { encryption_key } => {
                Ok(builder.add_encryption_key(encryption_key))
            }
            TableUpdate::RemoveEncryptionKey { key_id } => {
                Ok(builder.remove_encryption_key(&key_id))
            }
        }
    }
}

impl TableRequirement {
    /// Check that the requirement is met by the table metadata.
    /// If the requirement is not met, an appropriate error is returned.
    ///
    /// Provide metadata as `None` if the table does not exist.
    pub fn check(&self, metadata: Option<&TableMetadata>) -> Result<()> {
        if let Some(metadata) = metadata {
            match self {
                TableRequirement::NotExist => {
                    return Err(Error::new(
                        ErrorKind::CatalogCommitConflicts,
                        format!(
                            "Requirement failed: Table with id {} already exists",
                            metadata.uuid()
                        ),
                    )
                    .with_retryable(true));
                }
                TableRequirement::UuidMatch { uuid } => {
                    if &metadata.uuid() != uuid {
                        return Err(Error::new(
                            ErrorKind::CatalogCommitConflicts,
                            "Requirement failed: Table UUID does not match",
                        )
                        .with_context("expected", *uuid)
                        .with_context("found", metadata.uuid())
                        .with_retryable(true));
                    }
                }
                TableRequirement::CurrentSchemaIdMatch { current_schema_id } => {
                    // ToDo: Harmonize the types of current_schema_id
                    if metadata.current_schema_id != *current_schema_id {
                        return Err(Error::new(
                            ErrorKind::CatalogCommitConflicts,
                            "Requirement failed: Current schema id does not match",
                        )
                        .with_context("expected", current_schema_id.to_string())
                        .with_context("found", metadata.current_schema_id.to_string())
                        .with_retryable(true));
                    }
                }
                TableRequirement::DefaultSortOrderIdMatch {
                    default_sort_order_id,
                } => {
                    if metadata.default_sort_order().order_id != *default_sort_order_id {
                        return Err(Error::new(
                            ErrorKind::CatalogCommitConflicts,
                            "Requirement failed: Default sort order id does not match",
                        )
                        .with_context("expected", default_sort_order_id.to_string())
                        .with_context("found", metadata.default_sort_order().order_id.to_string())
                        .with_retryable(true));
                    }
                }
                TableRequirement::RefSnapshotIdMatch { r#ref, snapshot_id } => {
                    let snapshot_ref = metadata.snapshot_for_ref(r#ref);
                    if let Some(snapshot_id) = snapshot_id {
                        let snapshot_ref = snapshot_ref.ok_or(
                            Error::new(
                                ErrorKind::CatalogCommitConflicts,
                                format!("Requirement failed: Branch or tag `{ref}` not found"),
                            )
                            .with_retryable(true),
                        )?;
                        if snapshot_ref.snapshot_id() != *snapshot_id {
                            return Err(Error::new(
                                ErrorKind::CatalogCommitConflicts,
                                format!(
                                    "Requirement failed: Branch or tag `{ref}`'s snapshot has changed"
                                ),
                            )
                            .with_context("expected", snapshot_id.to_string())
                            .with_context("found", snapshot_ref.snapshot_id().to_string())
                            .with_retryable(true));
                        }
                    } else if snapshot_ref.is_some() {
                        // a null snapshot ID means the ref should not exist already
                        return Err(Error::new(
                            ErrorKind::CatalogCommitConflicts,
                            format!("Requirement failed: Branch or tag `{ref}` already exists"),
                        )
                        .with_retryable(true));
                    }
                }
                TableRequirement::DefaultSpecIdMatch { default_spec_id } => {
                    // ToDo: Harmonize the types of default_spec_id
                    if metadata.default_partition_spec_id() != *default_spec_id {
                        return Err(Error::new(
                            ErrorKind::CatalogCommitConflicts,
                            "Requirement failed: Default partition spec id does not match",
                        )
                        .with_context("expected", default_spec_id.to_string())
                        .with_context("found", metadata.default_partition_spec_id().to_string())
                        .with_retryable(true));
                    }
                }
                TableRequirement::LastAssignedPartitionIdMatch {
                    last_assigned_partition_id,
                } => {
                    if metadata.last_partition_id != *last_assigned_partition_id {
                        return Err(Error::new(
                            ErrorKind::CatalogCommitConflicts,
                            "Requirement failed: Last assigned partition id does not match",
                        )
                        .with_context("expected", last_assigned_partition_id.to_string())
                        .with_context("found", metadata.last_partition_id.to_string())
                        .with_retryable(true));
                    }
                }
                TableRequirement::LastAssignedFieldIdMatch {
                    last_assigned_field_id,
                } => {
                    if &metadata.last_column_id != last_assigned_field_id {
                        return Err(Error::new(
                            ErrorKind::CatalogCommitConflicts,
                            "Requirement failed: Last assigned field id does not match",
                        )
                        .with_context("expected", last_assigned_field_id.to_string())
                        .with_context("found", metadata.last_column_id.to_string())
                        .with_retryable(true));
                    }
                }
            };
        } else {
            match self {
                TableRequirement::NotExist => {}
                _ => {
                    return Err(Error::new(
                        ErrorKind::TableNotFound,
                        "Requirement failed: Table does not exist",
                    ));
                }
            }
        }

        Ok(())
    }
}

pub(super) mod _serde {
    use serde::{Deserialize as _, Deserializer, Serialize as _};

    use super::*;
    use crate::spec::{SchemaId, Summary};

    pub(super) fn deserialize_snapshot<'de, D>(
        deserializer: D,
    ) -> std::result::Result<Snapshot, D::Error>
    where D: Deserializer<'de> {
        let buf = CatalogSnapshot::deserialize(deserializer)?;
        Ok(buf.into())
    }

    pub(super) fn serialize_snapshot<S>(
        snapshot: &Snapshot,
        serializer: S,
    ) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let buf: CatalogSnapshot = snapshot.clone().into();
        buf.serialize(serializer)
    }

    #[derive(Debug, Serialize, Deserialize, PartialEq, Eq)]
    #[serde(rename_all = "kebab-case")]
    /// Defines the structure of a v2 snapshot for the catalog.
    /// Main difference to SnapshotV2 is that sequence-number is optional
    /// in the rest catalog spec to allow for backwards compatibility with v1.
    struct CatalogSnapshot {
        snapshot_id: i64,
        #[serde(skip_serializing_if = "Option::is_none")]
        parent_snapshot_id: Option<i64>,
        #[serde(default)]
        sequence_number: i64,
        timestamp_ms: i64,
        manifest_list: String,
        summary: Summary,
        #[serde(skip_serializing_if = "Option::is_none")]
        schema_id: Option<SchemaId>,
        #[serde(skip_serializing_if = "Option::is_none")]
        first_row_id: Option<u64>,
        #[serde(skip_serializing_if = "Option::is_none")]
        added_rows: Option<u64>,
        #[serde(skip_serializing_if = "Option::is_none")]
        key_id: Option<String>,
    }

    impl From<CatalogSnapshot> for Snapshot {
        fn from(snapshot: CatalogSnapshot) -> Self {
            let CatalogSnapshot {
                snapshot_id,
                parent_snapshot_id,
                sequence_number,
                timestamp_ms,
                manifest_list,
                schema_id,
                summary,
                first_row_id,
                added_rows,
                key_id,
            } = snapshot;
            let builder = Snapshot::builder()
                .with_snapshot_id(snapshot_id)
                .with_parent_snapshot_id(parent_snapshot_id)
                .with_sequence_number(sequence_number)
                .with_timestamp_ms(timestamp_ms)
                .with_manifest_list(manifest_list)
                .with_summary(summary)
                .with_encryption_key_id(key_id);
            let row_range = first_row_id.zip(added_rows);
            match (schema_id, row_range) {
                (None, None) => builder.build(),
                (Some(schema_id), None) => builder.with_schema_id(schema_id).build(),
                (None, Some((first_row_id, last_row_id))) => {
                    builder.with_row_range(first_row_id, last_row_id).build()
                }
                (Some(schema_id), Some((first_row_id, last_row_id))) => builder
                    .with_schema_id(schema_id)
                    .with_row_range(first_row_id, last_row_id)
                    .build(),
            }
        }
    }

    impl From<Snapshot> for CatalogSnapshot {
        fn from(snapshot: Snapshot) -> Self {
            let first_row_id = snapshot.first_row_id();
            let added_rows = snapshot.added_rows_count();
            let Snapshot {
                snapshot_id,
                parent_snapshot_id,
                sequence_number,
                timestamp_ms,
                manifest_list,
                summary,
                schema_id,
                row_range: _,
                encryption_key_id: key_id,
            } = snapshot;
            CatalogSnapshot {
                snapshot_id,
                parent_snapshot_id,
                sequence_number,
                timestamp_ms,
                manifest_list,
                summary,
                schema_id,
                first_row_id,
                added_rows,
                key_id,
            }
        }
    }
}

/// ViewCreation represents the creation of a view in the catalog.
#[derive(Debug, TypedBuilder)]
pub struct ViewCreation {
    /// The name of the view.
    pub name: String,
    /// The view's base location; used to create metadata file locations
    pub location: String,
    /// Representations for the view.
    pub representations: ViewRepresentations,
    /// The schema of the view.
    pub schema: Schema,
    /// The properties of the view.
    #[builder(default)]
    pub properties: HashMap<String, String>,
    /// The default namespace to use when a reference in the SELECT is a single identifier
    pub default_namespace: NamespaceIdent,
    /// Default catalog to use when a reference in the SELECT does not contain a catalog
    #[builder(default)]
    pub default_catalog: Option<String>,
    /// A string to string map of summary metadata about the version
    /// Typical keys are "engine-name" and "engine-version"
    #[builder(default)]
    pub summary: HashMap<String, String>,
}

/// ViewUpdate represents an update to a view in the catalog.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(tag = "action", rename_all = "kebab-case")]
#[allow(clippy::large_enum_variant)]
pub enum ViewUpdate {
    /// Assign a new UUID to the view
    #[serde(rename_all = "kebab-case")]
    AssignUuid {
        /// The new UUID to assign.
        uuid: Uuid,
    },
    /// Upgrade view's format version
    #[serde(rename_all = "kebab-case")]
    UpgradeFormatVersion {
        /// Target format upgrade to.
        format_version: ViewFormatVersion,
    },
    /// Add a new schema to the view
    #[serde(rename_all = "kebab-case")]
    AddSchema {
        /// The schema to add.
        schema: Schema,
        /// The last column id of the view.
        last_column_id: Option<i32>,
    },
    /// Set view's current schema
    #[serde(rename_all = "kebab-case")]
    SetLocation {
        /// New location for view.
        location: String,
    },
    /// Set view's properties
    ///
    /// Matching keys are updated, and non-matching keys are left unchanged.
    #[serde(rename_all = "kebab-case")]
    SetProperties {
        /// Properties to update for view.
        updates: HashMap<String, String>,
    },
    /// Remove view's properties
    #[serde(rename_all = "kebab-case")]
    RemoveProperties {
        /// Properties to remove
        removals: Vec<String>,
    },
    /// Add a new version to the view
    #[serde(rename_all = "kebab-case")]
    AddViewVersion {
        /// The view version to add.
        view_version: ViewVersion,
    },
    /// Set view's current version
    #[serde(rename_all = "kebab-case")]
    SetCurrentViewVersion {
        /// View version id to set as current, or -1 to set last added version
        view_version_id: i32,
    },
}

mod _serde_set_statistics {
    // The rest spec requires an additional field `snapshot-id`
    // that is redundant with the `snapshot_id` field in the statistics file.
    use serde::{Deserialize, Deserializer, Serialize, Serializer};

    use super::*;

    #[derive(Debug, Serialize, Deserialize)]
    #[serde(rename_all = "kebab-case")]
    struct SetStatistics {
        snapshot_id: Option<i64>,
        statistics: StatisticsFile,
    }

    pub fn serialize<S>(
        value: &StatisticsFile,
        serializer: S,
    ) -> std::result::Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        SetStatistics {
            snapshot_id: Some(value.snapshot_id),
            statistics: value.clone(),
        }
        .serialize(serializer)
    }

    pub fn deserialize<'de, D>(deserializer: D) -> std::result::Result<StatisticsFile, D::Error>
    where D: Deserializer<'de> {
        let SetStatistics {
            snapshot_id,
            statistics,
        } = SetStatistics::deserialize(deserializer)?;
        if let Some(snapshot_id) = snapshot_id
            && snapshot_id != statistics.snapshot_id
        {
            return Err(serde::de::Error::custom(format!(
                "Snapshot id to set {snapshot_id} does not match the statistics file snapshot id {}",
                statistics.snapshot_id
            )));
        }

        Ok(statistics)
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::fmt::Debug;
    use std::fs::File;
    use std::io::BufReader;

    use base64::Engine as _;
    use serde::Serialize;
    use serde::de::DeserializeOwned;
    use uuid::uuid;

    use super::ViewUpdate;
    use crate::io::FileIOBuilder;
    use crate::spec::{
        BlobMetadata, EncryptedKey, FormatVersion, MAIN_BRANCH, NestedField, NullOrder, Operation,
        PartitionStatisticsFile, PrimitiveType, Schema, Snapshot, SnapshotReference,
        SnapshotRetention, SortDirection, SortField, SortOrder, SqlViewRepresentation,
        StatisticsFile, Summary, TableMetadata, TableMetadataBuilder, Transform, Type,
        UnboundPartitionSpec, ViewFormatVersion, ViewRepresentation, ViewRepresentations,
        ViewVersion,
    };
    use crate::table::Table;
    use crate::{
        NamespaceIdent, TableCommit, TableCreation, TableIdent, TableRequirement, TableUpdate,
    };

    #[test]
    fn test_parent_namespace() {
        let ns1 = NamespaceIdent::from_strs(vec!["ns1"]).unwrap();
        let ns2 = NamespaceIdent::from_strs(vec!["ns1", "ns2"]).unwrap();
        let ns3 = NamespaceIdent::from_strs(vec!["ns1", "ns2", "ns3"]).unwrap();

        assert_eq!(ns1.parent(), None);
        assert_eq!(ns2.parent(), Some(ns1.clone()));
        assert_eq!(ns3.parent(), Some(ns2.clone()));
    }

    #[test]
    fn test_create_table_id() {
        let table_id = TableIdent {
            namespace: NamespaceIdent::from_strs(vec!["ns1"]).unwrap(),
            name: "t1".to_string(),
        };

        assert_eq!(table_id, TableIdent::from_strs(vec!["ns1", "t1"]).unwrap());
    }

    #[test]
    fn test_table_creation_iterator_properties() {
        let builder = TableCreation::builder()
            .name("table".to_string())
            .schema(Schema::builder().build().unwrap());

        fn s(k: &str, v: &str) -> (String, String) {
            (k.to_string(), v.to_string())
        }

        let table_creation = builder
            .properties([s("key", "value"), s("foo", "bar")])
            .build();

        assert_eq!(
            HashMap::from([s("key", "value"), s("foo", "bar")]),
            table_creation.properties
        );
    }

    fn test_serde_json<T: Serialize + DeserializeOwned + PartialEq + Debug>(
        json: impl ToString,
        expected: T,
    ) {
        let json_str = json.to_string();
        let actual: T = serde_json::from_str(&json_str).expect("Failed to parse from json");
        assert_eq!(actual, expected, "Parsed value is not equal to expected");

        let restored: T = serde_json::from_str(
            &serde_json::to_string(&actual).expect("Failed to serialize to json"),
        )
        .expect("Failed to parse from serialized json");

        assert_eq!(
            restored, expected,
            "Parsed restored value is not equal to expected"
        );
    }

    fn metadata() -> TableMetadata {
        let tbl_creation = TableCreation::builder()
            .name("table".to_string())
            .location("/path/to/table".to_string())
            .schema(Schema::builder().build().unwrap())
            .build();

        TableMetadataBuilder::from_table_creation(tbl_creation)
            .unwrap()
            .assign_uuid(uuid::Uuid::nil())
            .build()
            .unwrap()
            .metadata
    }

    #[test]
    fn test_check_requirement_not_exist() {
        let metadata = metadata();
        let requirement = TableRequirement::NotExist;

        assert!(requirement.check(Some(&metadata)).is_err());
        assert!(requirement.check(None).is_ok());
    }

    #[test]
    fn test_check_table_uuid() {
        let metadata = metadata();

        let requirement = TableRequirement::UuidMatch {
            uuid: uuid::Uuid::now_v7(),
        };
        assert!(requirement.check(Some(&metadata)).is_err());

        let requirement = TableRequirement::UuidMatch {
            uuid: uuid::Uuid::nil(),
        };
        assert!(requirement.check(Some(&metadata)).is_ok());
    }

    #[test]
    fn test_check_ref_snapshot_id() {
        let metadata = metadata();

        // Ref does not exist but should
        let requirement = TableRequirement::RefSnapshotIdMatch {
            r#ref: "my_branch".to_string(),
            snapshot_id: Some(1),
        };
        assert!(requirement.check(Some(&metadata)).is_err());

        // Ref does not exist and should not
        let requirement = TableRequirement::RefSnapshotIdMatch {
            r#ref: "my_branch".to_string(),
            snapshot_id: None,
        };
        assert!(requirement.check(Some(&metadata)).is_ok());

        // Add snapshot
        let snapshot = Snapshot::builder()
            .with_snapshot_id(3051729675574597004)
            .with_sequence_number(10)
            .with_timestamp_ms(9992191116217)
            .with_manifest_list("s3://b/wh/.../s1.avro".to_string())
            .with_schema_id(0)
            .with_summary(Summary {
                operation: Operation::Append,
                additional_properties: HashMap::new(),
            })
            .build();

        let builder = metadata.into_builder(None);
        let builder = TableUpdate::AddSnapshot {
            snapshot: snapshot.clone(),
        }
        .apply(builder)
        .unwrap();
        let metadata = TableUpdate::SetSnapshotRef {
            ref_name: MAIN_BRANCH.to_string(),
            reference: SnapshotReference {
                snapshot_id: snapshot.snapshot_id(),
                retention: SnapshotRetention::Branch {
                    min_snapshots_to_keep: Some(10),
                    max_snapshot_age_ms: None,
                    max_ref_age_ms: None,
                },
            },
        }
        .apply(builder)
        .unwrap()
        .build()
        .unwrap()
        .metadata;

        // Ref exists and should match
        let requirement = TableRequirement::RefSnapshotIdMatch {
            r#ref: "main".to_string(),
            snapshot_id: Some(3051729675574597004),
        };
        assert!(requirement.check(Some(&metadata)).is_ok());

        // Ref exists but does not match
        let requirement = TableRequirement::RefSnapshotIdMatch {
            r#ref: "main".to_string(),
            snapshot_id: Some(1),
        };
        assert!(requirement.check(Some(&metadata)).is_err());
    }

    #[test]
    fn test_check_last_assigned_field_id() {
        let metadata = metadata();

        let requirement = TableRequirement::LastAssignedFieldIdMatch {
            last_assigned_field_id: 1,
        };
        assert!(requirement.check(Some(&metadata)).is_err());

        let requirement = TableRequirement::LastAssignedFieldIdMatch {
            last_assigned_field_id: 0,
        };
        assert!(requirement.check(Some(&metadata)).is_ok());
    }

    #[test]
    fn test_check_current_schema_id() {
        let metadata = metadata();

        let requirement = TableRequirement::CurrentSchemaIdMatch {
            current_schema_id: 1,
        };
        assert!(requirement.check(Some(&metadata)).is_err());

        let requirement = TableRequirement::CurrentSchemaIdMatch {
            current_schema_id: 0,
        };
        assert!(requirement.check(Some(&metadata)).is_ok());
    }

    #[test]
    fn test_check_last_assigned_partition_id() {
        let metadata = metadata();
        let requirement = TableRequirement::LastAssignedPartitionIdMatch {
            last_assigned_partition_id: 0,
        };
        assert!(requirement.check(Some(&metadata)).is_err());

        let requirement = TableRequirement::LastAssignedPartitionIdMatch {
            last_assigned_partition_id: 999,
        };
        assert!(requirement.check(Some(&metadata)).is_ok());
    }

    #[test]
    fn test_check_default_spec_id() {
        let metadata = metadata();

        let requirement = TableRequirement::DefaultSpecIdMatch { default_spec_id: 1 };
        assert!(requirement.check(Some(&metadata)).is_err());

        let requirement = TableRequirement::DefaultSpecIdMatch { default_spec_id: 0 };
        assert!(requirement.check(Some(&metadata)).is_ok());
    }

    #[test]
    fn test_check_default_sort_order_id() {
        let metadata = metadata();

        let requirement = TableRequirement::DefaultSortOrderIdMatch {
            default_sort_order_id: 1,
        };
        assert!(requirement.check(Some(&metadata)).is_err());

        let requirement = TableRequirement::DefaultSortOrderIdMatch {
            default_sort_order_id: 0,
        };
        assert!(requirement.check(Some(&metadata)).is_ok());
    }

    #[test]
    fn test_table_uuid() {
        test_serde_json(
            r#"
{
    "type": "assert-table-uuid",
    "uuid": "2cc52516-5e73-41f2-b139-545d41a4e151"
}
        "#,
            TableRequirement::UuidMatch {
                uuid: uuid!("2cc52516-5e73-41f2-b139-545d41a4e151"),
            },
        );
    }

    #[test]
    fn test_assert_table_not_exists() {
        test_serde_json(
            r#"
{
    "type": "assert-create"
}
        "#,
            TableRequirement::NotExist,
        );
    }

    #[test]
    fn test_assert_ref_snapshot_id() {
        test_serde_json(
            r#"
{
    "type": "assert-ref-snapshot-id",
    "ref": "snapshot-name",
    "snapshot-id": null
}
        "#,
            TableRequirement::RefSnapshotIdMatch {
                r#ref: "snapshot-name".to_string(),
                snapshot_id: None,
            },
        );

        test_serde_json(
            r#"
{
    "type": "assert-ref-snapshot-id",
    "ref": "snapshot-name",
    "snapshot-id": 1
}
        "#,
            TableRequirement::RefSnapshotIdMatch {
                r#ref: "snapshot-name".to_string(),
                snapshot_id: Some(1),
            },
        );
    }

    #[test]
    fn test_assert_last_assigned_field_id() {
        test_serde_json(
            r#"
{
    "type": "assert-last-assigned-field-id",
    "last-assigned-field-id": 12
}
        "#,
            TableRequirement::LastAssignedFieldIdMatch {
                last_assigned_field_id: 12,
            },
        );
    }

    #[test]
    fn test_assert_current_schema_id() {
        test_serde_json(
            r#"
{
    "type": "assert-current-schema-id",
    "current-schema-id": 4
}
        "#,
            TableRequirement::CurrentSchemaIdMatch {
                current_schema_id: 4,
            },
        );
    }

    #[test]
    fn test_assert_last_assigned_partition_id() {
        test_serde_json(
            r#"
{
    "type": "assert-last-assigned-partition-id",
    "last-assigned-partition-id": 1004
}
        "#,
            TableRequirement::LastAssignedPartitionIdMatch {
                last_assigned_partition_id: 1004,
            },
        );
    }

    #[test]
    fn test_assert_default_spec_id() {
        test_serde_json(
            r#"
{
    "type": "assert-default-spec-id",
    "default-spec-id": 5
}
        "#,
            TableRequirement::DefaultSpecIdMatch { default_spec_id: 5 },
        );
    }

    #[test]
    fn test_assert_default_sort_order() {
        let json = r#"
{
    "type": "assert-default-sort-order-id",
    "default-sort-order-id": 10
}
        "#;

        let update = TableRequirement::DefaultSortOrderIdMatch {
            default_sort_order_id: 10,
        };

        test_serde_json(json, update);
    }

    #[test]
    fn test_parse_assert_invalid() {
        assert!(
            serde_json::from_str::<TableRequirement>(
                r#"
{
    "default-sort-order-id": 10
}
"#
            )
            .is_err(),
            "Table requirements should not be parsed without type."
        );
    }

    #[test]
    fn test_assign_uuid() {
        test_serde_json(
            r#"
{
    "action": "assign-uuid",
    "uuid": "2cc52516-5e73-41f2-b139-545d41a4e151"
}
        "#,
            TableUpdate::AssignUuid {
                uuid: uuid!("2cc52516-5e73-41f2-b139-545d41a4e151"),
            },
        );
    }

    #[test]
    fn test_upgrade_format_version() {
        test_serde_json(
            r#"
{
    "action": "upgrade-format-version",
    "format-version": 2
}
        "#,
            TableUpdate::UpgradeFormatVersion {
                format_version: FormatVersion::V2,
            },
        );
    }

    #[test]
    fn test_add_schema() {
        let test_schema = Schema::builder()
            .with_schema_id(1)
            .with_identifier_field_ids(vec![2])
            .with_fields(vec![
                NestedField::optional(1, "foo", Type::Primitive(PrimitiveType::String)).into(),
                NestedField::required(2, "bar", Type::Primitive(PrimitiveType::Int)).into(),
                NestedField::optional(3, "baz", Type::Primitive(PrimitiveType::Boolean)).into(),
            ])
            .build()
            .unwrap();
        test_serde_json(
            r#"
{
    "action": "add-schema",
    "schema": {
        "type": "struct",
        "schema-id": 1,
        "fields": [
            {
                "id": 1,
                "name": "foo",
                "required": false,
                "type": "string"
            },
            {
                "id": 2,
                "name": "bar",
                "required": true,
                "type": "int"
            },
            {
                "id": 3,
                "name": "baz",
                "required": false,
                "type": "boolean"
            }
        ],
        "identifier-field-ids": [
            2
        ]
    },
    "last-column-id": 3
}
        "#,
            TableUpdate::AddSchema {
                schema: test_schema.clone(),
            },
        );

        test_serde_json(
            r#"
{
    "action": "add-schema",
    "schema": {
        "type": "struct",
        "schema-id": 1,
        "fields": [
            {
                "id": 1,
                "name": "foo",
                "required": false,
                "type": "string"
            },
            {
                "id": 2,
                "name": "bar",
                "required": true,
                "type": "int"
            },
            {
                "id": 3,
                "name": "baz",
                "required": false,
                "type": "boolean"
            }
        ],
        "identifier-field-ids": [
            2
        ]
    }
}
        "#,
            TableUpdate::AddSchema {
                schema: test_schema.clone(),
            },
        );
    }

    #[test]
    fn test_set_current_schema() {
        test_serde_json(
            r#"
{
   "action": "set-current-schema",
   "schema-id": 23
}
        "#,
            TableUpdate::SetCurrentSchema { schema_id: 23 },
        );
    }

    #[test]
    fn test_add_spec() {
        test_serde_json(
            r#"
{
    "action": "add-spec",
    "spec": {
        "fields": [
            {
                "source-id": 4,
                "name": "ts_day",
                "transform": "day"
            },
            {
                "source-id": 1,
                "name": "id_bucket",
                "transform": "bucket[16]"
            },
            {
                "source-id": 2,
                "name": "id_truncate",
                "transform": "truncate[4]"
            }
        ]
    }
}
        "#,
            TableUpdate::AddSpec {
                spec: UnboundPartitionSpec::builder()
                    .add_partition_field(4, "ts_day".to_string(), Transform::Day)
                    .unwrap()
                    .add_partition_field(1, "id_bucket".to_string(), Transform::Bucket(16))
                    .unwrap()
                    .add_partition_field(2, "id_truncate".to_string(), Transform::Truncate(4))
                    .unwrap()
                    .build(),
            },
        );
    }

    #[test]
    fn test_set_default_spec() {
        test_serde_json(
            r#"
{
    "action": "set-default-spec",
    "spec-id": 1
}
        "#,
            TableUpdate::SetDefaultSpec { spec_id: 1 },
        )
    }

    #[test]
    fn test_add_sort_order() {
        let json = r#"
{
    "action": "add-sort-order",
    "sort-order": {
        "order-id": 1,
        "fields": [
            {
                "transform": "identity",
                "source-id": 2,
                "direction": "asc",
                "null-order": "nulls-first"
            },
            {
                "transform": "bucket[4]",
                "source-id": 3,
                "direction": "desc",
                "null-order": "nulls-last"
            }
        ]
    }
}
        "#;

        let update = TableUpdate::AddSortOrder {
            sort_order: SortOrder::builder()
                .with_order_id(1)
                .with_sort_field(
                    SortField::builder()
                        .source_id(2)
                        .direction(SortDirection::Ascending)
                        .null_order(NullOrder::First)
                        .transform(Transform::Identity)
                        .build(),
                )
                .with_sort_field(
                    SortField::builder()
                        .source_id(3)
                        .direction(SortDirection::Descending)
                        .null_order(NullOrder::Last)
                        .transform(Transform::Bucket(4))
                        .build(),
                )
                .build_unbound()
                .unwrap(),
        };

        test_serde_json(json, update);
    }

    #[test]
    fn test_set_default_order() {
        let json = r#"
{
    "action": "set-default-sort-order",
    "sort-order-id": 2
}
        "#;
        let update = TableUpdate::SetDefaultSortOrder { sort_order_id: 2 };

        test_serde_json(json, update);
    }

    #[test]
    fn test_add_snapshot() {
        let json = r#"
{
    "action": "add-snapshot",
    "snapshot": {
        "snapshot-id": 3055729675574597000,
        "parent-snapshot-id": 3051729675574597000,
        "timestamp-ms": 1555100955770,
        "sequence-number": 1,
        "summary": {
            "operation": "append"
        },
        "manifest-list": "s3://a/b/2.avro",
        "schema-id": 1
    }
}
        "#;

        let update = TableUpdate::AddSnapshot {
            snapshot: Snapshot::builder()
                .with_snapshot_id(3055729675574597000)
                .with_parent_snapshot_id(Some(3051729675574597000))
                .with_timestamp_ms(1555100955770)
                .with_sequence_number(1)
                .with_manifest_list("s3://a/b/2.avro")
                .with_schema_id(1)
                .with_summary(Summary {
                    operation: Operation::Append,
                    additional_properties: HashMap::default(),
                })
                .build(),
        };

        test_serde_json(json, update);
    }

    #[test]
    fn test_add_snapshot_v1() {
        let json = r#"
{
    "action": "add-snapshot",
    "snapshot": {
        "snapshot-id": 3055729675574597000,
        "parent-snapshot-id": 3051729675574597000,
        "timestamp-ms": 1555100955770,
        "summary": {
            "operation": "append"
        },
        "manifest-list": "s3://a/b/2.avro"
    }
}
    "#;

        let update = TableUpdate::AddSnapshot {
            snapshot: Snapshot::builder()
                .with_snapshot_id(3055729675574597000)
                .with_parent_snapshot_id(Some(3051729675574597000))
                .with_timestamp_ms(1555100955770)
                .with_sequence_number(0)
                .with_manifest_list("s3://a/b/2.avro")
                .with_summary(Summary {
                    operation: Operation::Append,
                    additional_properties: HashMap::default(),
                })
                .build(),
        };

        let actual: TableUpdate = serde_json::from_str(json).expect("Failed to parse from json");
        assert_eq!(actual, update, "Parsed value is not equal to expected");
    }

    #[test]
    fn test_add_snapshot_v3() {
        let json = serde_json::json!(
        {
            "action": "add-snapshot",
            "snapshot": {
                "snapshot-id": 3055729675574597000i64,
                "parent-snapshot-id": 3051729675574597000i64,
                "timestamp-ms": 1555100955770i64,
                "first-row-id":0,
                "added-rows":2,
                "key-id":"key123",
                "summary": {
                    "operation": "append"
                },
                "manifest-list": "s3://a/b/2.avro"
            }
        });

        let update = TableUpdate::AddSnapshot {
            snapshot: Snapshot::builder()
                .with_snapshot_id(3055729675574597000)
                .with_parent_snapshot_id(Some(3051729675574597000))
                .with_timestamp_ms(1555100955770)
                .with_sequence_number(0)
                .with_manifest_list("s3://a/b/2.avro")
                .with_row_range(0, 2)
                .with_encryption_key_id(Some("key123".to_string()))
                .with_summary(Summary {
                    operation: Operation::Append,
                    additional_properties: HashMap::default(),
                })
                .build(),
        };

        let actual: TableUpdate = serde_json::from_value(json).expect("Failed to parse from json");
        assert_eq!(actual, update, "Parsed value is not equal to expected");
        let restored: TableUpdate = serde_json::from_str(
            &serde_json::to_string(&actual).expect("Failed to serialize to json"),
        )
        .expect("Failed to parse from serialized json");
        assert_eq!(restored, update);
    }

    #[test]
    fn test_remove_snapshots() {
        let json = r#"
{
    "action": "remove-snapshots",
    "snapshot-ids": [
        1,
        2
    ]
}
        "#;

        let update = TableUpdate::RemoveSnapshots {
            snapshot_ids: vec![1, 2],
        };
        test_serde_json(json, update);
    }

    #[test]
    fn test_remove_snapshot_ref() {
        let json = r#"
{
    "action": "remove-snapshot-ref",
    "ref-name": "snapshot-ref"
}
        "#;

        let update = TableUpdate::RemoveSnapshotRef {
            ref_name: "snapshot-ref".to_string(),
        };
        test_serde_json(json, update);
    }

    #[test]
    fn test_set_snapshot_ref_tag() {
        let json = r#"
{
    "action": "set-snapshot-ref",
    "type": "tag",
    "ref-name": "hank",
    "snapshot-id": 1,
    "max-ref-age-ms": 1
}
        "#;

        let update = TableUpdate::SetSnapshotRef {
            ref_name: "hank".to_string(),
            reference: SnapshotReference {
                snapshot_id: 1,
                retention: SnapshotRetention::Tag {
                    max_ref_age_ms: Some(1),
                },
            },
        };

        test_serde_json(json, update);
    }

    #[test]
    fn test_set_snapshot_ref_branch() {
        let json = r#"
{
    "action": "set-snapshot-ref",
    "type": "branch",
    "ref-name": "hank",
    "snapshot-id": 1,
    "min-snapshots-to-keep": 2,
    "max-snapshot-age-ms": 3,
    "max-ref-age-ms": 4
}
        "#;

        let update = TableUpdate::SetSnapshotRef {
            ref_name: "hank".to_string(),
            reference: SnapshotReference {
                snapshot_id: 1,
                retention: SnapshotRetention::Branch {
                    min_snapshots_to_keep: Some(2),
                    max_snapshot_age_ms: Some(3),
                    max_ref_age_ms: Some(4),
                },
            },
        };

        test_serde_json(json, update);
    }

    #[test]
    fn test_set_properties() {
        let json = r#"
{
    "action": "set-properties",
    "updates": {
        "prop1": "v1",
        "prop2": "v2"
    }
}
        "#;

        let update = TableUpdate::SetProperties {
            updates: vec![
                ("prop1".to_string(), "v1".to_string()),
                ("prop2".to_string(), "v2".to_string()),
            ]
            .into_iter()
            .collect(),
        };

        test_serde_json(json, update);
    }

    #[test]
    fn test_remove_properties() {
        let json = r#"
{
    "action": "remove-properties",
    "removals": [
        "prop1",
        "prop2"
    ]
}
        "#;

        let update = TableUpdate::RemoveProperties {
            removals: vec!["prop1".to_string(), "prop2".to_string()],
        };

        test_serde_json(json, update);
    }

    #[test]
    fn test_set_location() {
        let json = r#"
{
    "action": "set-location",
    "location": "s3://bucket/warehouse/tbl_location"
}
    "#;

        let update = TableUpdate::SetLocation {
            location: "s3://bucket/warehouse/tbl_location".to_string(),
        };

        test_serde_json(json, update);
    }

    #[test]
    fn test_table_update_apply() {
        let table_creation = TableCreation::builder()
            .location("s3://db/table".to_string())
            .name("table".to_string())
            .properties(HashMap::new())
            .schema(Schema::builder().build().unwrap())
            .build();
        let table_metadata = TableMetadataBuilder::from_table_creation(table_creation)
            .unwrap()
            .build()
            .unwrap()
            .metadata;
        let table_metadata_builder = TableMetadataBuilder::new_from_metadata(
            table_metadata,
            Some("s3://db/table/metadata/metadata1.gz.json".to_string()),
        );

        let uuid = uuid::Uuid::new_v4();
        let update = TableUpdate::AssignUuid { uuid };
        let updated_metadata = update
            .apply(table_metadata_builder)
            .unwrap()
            .build()
            .unwrap()
            .metadata;
        assert_eq!(updated_metadata.uuid(), uuid);
    }

    #[test]
    fn test_view_assign_uuid() {
        test_serde_json(
            r#"
{
    "action": "assign-uuid",
    "uuid": "2cc52516-5e73-41f2-b139-545d41a4e151"
}
        "#,
            ViewUpdate::AssignUuid {
                uuid: uuid!("2cc52516-5e73-41f2-b139-545d41a4e151"),
            },
        );
    }

    #[test]
    fn test_view_upgrade_format_version() {
        test_serde_json(
            r#"
{
    "action": "upgrade-format-version",
    "format-version": 1
}
        "#,
            ViewUpdate::UpgradeFormatVersion {
                format_version: ViewFormatVersion::V1,
            },
        );
    }

    #[test]
    fn test_view_add_schema() {
        let test_schema = Schema::builder()
            .with_schema_id(1)
            .with_identifier_field_ids(vec![2])
            .with_fields(vec![
                NestedField::optional(1, "foo", Type::Primitive(PrimitiveType::String)).into(),
                NestedField::required(2, "bar", Type::Primitive(PrimitiveType::Int)).into(),
                NestedField::optional(3, "baz", Type::Primitive(PrimitiveType::Boolean)).into(),
            ])
            .build()
            .unwrap();
        test_serde_json(
            r#"
{
    "action": "add-schema",
    "schema": {
        "type": "struct",
        "schema-id": 1,
        "fields": [
            {
                "id": 1,
                "name": "foo",
                "required": false,
                "type": "string"
            },
            {
                "id": 2,
                "name": "bar",
                "required": true,
                "type": "int"
            },
            {
                "id": 3,
                "name": "baz",
                "required": false,
                "type": "boolean"
            }
        ],
        "identifier-field-ids": [
            2
        ]
    },
    "last-column-id": 3
}
        "#,
            ViewUpdate::AddSchema {
                schema: test_schema.clone(),
                last_column_id: Some(3),
            },
        );
    }

    #[test]
    fn test_view_set_location() {
        test_serde_json(
            r#"
{
    "action": "set-location",
    "location": "s3://db/view"
}
        "#,
            ViewUpdate::SetLocation {
                location: "s3://db/view".to_string(),
            },
        );
    }

    #[test]
    fn test_view_set_properties() {
        test_serde_json(
            r#"
{
    "action": "set-properties",
    "updates": {
        "prop1": "v1",
        "prop2": "v2"
    }
}
        "#,
            ViewUpdate::SetProperties {
                updates: vec![
                    ("prop1".to_string(), "v1".to_string()),
                    ("prop2".to_string(), "v2".to_string()),
                ]
                .into_iter()
                .collect(),
            },
        );
    }

    #[test]
    fn test_view_remove_properties() {
        test_serde_json(
            r#"
{
    "action": "remove-properties",
    "removals": [
        "prop1",
        "prop2"
    ]
}
        "#,
            ViewUpdate::RemoveProperties {
                removals: vec!["prop1".to_string(), "prop2".to_string()],
            },
        );
    }

    #[test]
    fn test_view_add_view_version() {
        test_serde_json(
            r#"
{
    "action": "add-view-version",
    "view-version": {
            "version-id" : 1,
            "timestamp-ms" : 1573518431292,
            "schema-id" : 1,
            "default-catalog" : "prod",
            "default-namespace" : [ "default" ],
            "summary" : {
              "engine-name" : "Spark"
            },
            "representations" : [ {
              "type" : "sql",
              "sql" : "SELECT\n    COUNT(1), CAST(event_ts AS DATE)\nFROM events\nGROUP BY 2",
              "dialect" : "spark"
            } ]
    }
}
        "#,
            ViewUpdate::AddViewVersion {
                view_version: ViewVersion::builder()
                    .with_version_id(1)
                    .with_timestamp_ms(1573518431292)
                    .with_schema_id(1)
                    .with_default_catalog(Some("prod".to_string()))
                    .with_default_namespace(NamespaceIdent::from_strs(vec!["default"]).unwrap())
                    .with_summary(
                        vec![("engine-name".to_string(), "Spark".to_string())]
                            .into_iter()
                            .collect(),
                    )
                    .with_representations(ViewRepresentations(vec![ViewRepresentation::Sql(SqlViewRepresentation {
                        sql: "SELECT\n    COUNT(1), CAST(event_ts AS DATE)\nFROM events\nGROUP BY 2".to_string(),
                        dialect: "spark".to_string(),
                    })]))
                    .build(),
            },
        );
    }

    #[test]
    fn test_view_set_current_view_version() {
        test_serde_json(
            r#"
{
    "action": "set-current-view-version",
    "view-version-id": 1
}
        "#,
            ViewUpdate::SetCurrentViewVersion { view_version_id: 1 },
        );
    }

    #[test]
    fn test_remove_partition_specs_update() {
        test_serde_json(
            r#"
{
    "action": "remove-partition-specs",
    "spec-ids": [1, 2]
}
        "#,
            TableUpdate::RemovePartitionSpecs {
                spec_ids: vec![1, 2],
            },
        );
    }

    #[test]
    fn test_set_statistics_file() {
        test_serde_json(
            r#"
        {
                "action": "set-statistics",
                "snapshot-id": 1940541653261589030,
                "statistics": {
                        "snapshot-id": 1940541653261589030,
                        "statistics-path": "s3://bucket/warehouse/stats.puffin",
                        "file-size-in-bytes": 124,
                        "file-footer-size-in-bytes": 27,
                        "blob-metadata": [
                                {
                                        "type": "boring-type",
                                        "snapshot-id": 1940541653261589030,
                                        "sequence-number": 2,
                                        "fields": [
                                                1
                                        ],
                                        "properties": {
                                                "prop-key": "prop-value"
                                        }
                                }
                        ]
                }
        }
        "#,
            TableUpdate::SetStatistics {
                statistics: StatisticsFile {
                    snapshot_id: 1940541653261589030,
                    statistics_path: "s3://bucket/warehouse/stats.puffin".to_string(),
                    file_size_in_bytes: 124,
                    file_footer_size_in_bytes: 27,
                    key_metadata: None,
                    blob_metadata: vec![BlobMetadata {
                        r#type: "boring-type".to_string(),
                        snapshot_id: 1940541653261589030,
                        sequence_number: 2,
                        fields: vec![1],
                        properties: vec![("prop-key".to_string(), "prop-value".to_string())]
                            .into_iter()
                            .collect(),
                    }],
                },
            },
        );
    }

    #[test]
    fn test_remove_statistics_file() {
        test_serde_json(
            r#"
        {
                "action": "remove-statistics",
                "snapshot-id": 1940541653261589030
        }
        "#,
            TableUpdate::RemoveStatistics {
                snapshot_id: 1940541653261589030,
            },
        );
    }

    #[test]
    fn test_set_partition_statistics_file() {
        test_serde_json(
            r#"
            {
                "action": "set-partition-statistics",
                "partition-statistics": {
                    "snapshot-id": 1940541653261589030,
                    "statistics-path": "s3://bucket/warehouse/stats1.parquet",
                    "file-size-in-bytes": 43
                }
            }
            "#,
            TableUpdate::SetPartitionStatistics {
                partition_statistics: PartitionStatisticsFile {
                    snapshot_id: 1940541653261589030,
                    statistics_path: "s3://bucket/warehouse/stats1.parquet".to_string(),
                    file_size_in_bytes: 43,
                },
            },
        )
    }

    #[test]
    fn test_remove_partition_statistics_file() {
        test_serde_json(
            r#"
            {
                "action": "remove-partition-statistics",
                "snapshot-id": 1940541653261589030
            }
            "#,
            TableUpdate::RemovePartitionStatistics {
                snapshot_id: 1940541653261589030,
            },
        )
    }

    #[test]
    fn test_remove_schema_update() {
        test_serde_json(
            r#"
                {
                    "action": "remove-schemas",
                    "schema-ids": [1, 2]
                }        
            "#,
            TableUpdate::RemoveSchemas {
                schema_ids: vec![1, 2],
            },
        );
    }

    #[test]
    fn test_add_encryption_key() {
        let key_bytes = "key".as_bytes();
        let encoded_key = base64::engine::general_purpose::STANDARD.encode(key_bytes);
        test_serde_json(
            format!(
                r#"
                {{
                    "action": "add-encryption-key",
                    "encryption-key": {{
                        "key-id": "a",
                        "encrypted-key-metadata": "{encoded_key}",
                        "encrypted-by-id": "b"
                    }}
                }}        
            "#
            ),
            TableUpdate::AddEncryptionKey {
                encryption_key: EncryptedKey::builder()
                    .key_id("a")
                    .encrypted_key_metadata(key_bytes.to_vec())
                    .encrypted_by_id("b")
                    .build(),
            },
        );
    }

    #[test]
    fn test_remove_encryption_key() {
        test_serde_json(
            r#"
                {
                    "action": "remove-encryption-key",
                    "key-id": "a"
                }        
            "#,
            TableUpdate::RemoveEncryptionKey {
                key_id: "a".to_string(),
            },
        );
    }

    #[test]
    fn test_table_commit() {
        let table = {
            let file = File::open(format!(
                "{}/testdata/table_metadata/{}",
                env!("CARGO_MANIFEST_DIR"),
                "TableMetadataV2Valid.json"
            ))
            .unwrap();
            let reader = BufReader::new(file);
            let resp = serde_json::from_reader::<_, TableMetadata>(reader).unwrap();

            Table::builder()
                .metadata(resp)
                .metadata_location("s3://bucket/test/location/metadata/00000-8a62c37d-4573-4021-952a-c0baef7d21d0.metadata.json".to_string())
                .identifier(TableIdent::from_strs(["ns1", "test1"]).unwrap())
                .file_io(FileIOBuilder::new("memory").build().unwrap())
                .build()
                .unwrap()
        };

        let updates = vec![
            TableUpdate::SetLocation {
                location: "s3://bucket/test/new_location/data".to_string(),
            },
            TableUpdate::SetProperties {
                updates: vec![
                    ("prop1".to_string(), "v1".to_string()),
                    ("prop2".to_string(), "v2".to_string()),
                ]
                .into_iter()
                .collect(),
            },
        ];

        let requirements = vec![TableRequirement::UuidMatch {
            uuid: table.metadata().table_uuid,
        }];

        let table_commit = TableCommit::builder()
            .ident(table.identifier().to_owned())
            .updates(updates)
            .requirements(requirements)
            .build();

        let updated_table = table_commit.apply(table).unwrap();

        assert_eq!(
            updated_table.metadata().properties.get("prop1").unwrap(),
            "v1"
        );
        assert_eq!(
            updated_table.metadata().properties.get("prop2").unwrap(),
            "v2"
        );

        // metadata version should be bumped
        assert!(
            updated_table
                .metadata_location()
                .unwrap()
                .starts_with("s3://bucket/test/location/metadata/00001-")
        );

        assert_eq!(
            updated_table.metadata().location,
            "s3://bucket/test/new_location/data",
        );
    }
}
