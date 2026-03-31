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

use chrono::Utc;
use itertools::Itertools;
use uuid::Uuid;

use super::{
    DEFAULT_SCHEMA_ID, INITIAL_VIEW_VERSION_ID, ONE_MINUTE_MS, Schema, SchemaId,
    TableMetadataBuilder, VIEW_PROPERTY_REPLACE_DROP_DIALECT_ALLOWED,
    VIEW_PROPERTY_REPLACE_DROP_DIALECT_ALLOWED_DEFAULT, VIEW_PROPERTY_VERSION_HISTORY_SIZE,
    VIEW_PROPERTY_VERSION_HISTORY_SIZE_DEFAULT, ViewFormatVersion, ViewMetadata,
    ViewRepresentation, ViewVersion, ViewVersionLog, ViewVersionRef,
};
use crate::ViewCreation;
use crate::catalog::ViewUpdate;
use crate::error::{Error, ErrorKind, Result};
use crate::io::is_truthy;

/// Manipulating view metadata.
///
/// For this builder the order of called functions matters.
/// All operations applied to the `ViewMetadata` are tracked in `changes` as  a chronologically
/// ordered vec of `ViewUpdate`.
/// If an operation does not lead to a change of the `ViewMetadata`, the corresponding update
/// is omitted from `changes`.
#[derive(Debug, Clone)]
pub struct ViewMetadataBuilder {
    metadata: ViewMetadata,
    changes: Vec<ViewUpdate>,
    last_added_schema_id: Option<SchemaId>,
    last_added_version_id: Option<SchemaId>,
    history_entry: Option<ViewVersionLog>,
    // Previous view version is only used during build to check
    // weather dialects are dropped or not.
    previous_view_version: Option<ViewVersionRef>,
}

#[derive(Debug, Clone, PartialEq)]
/// Result of modifying or creating a `ViewMetadata`.
pub struct ViewMetadataBuildResult {
    /// The new `ViewMetadata`.
    pub metadata: ViewMetadata,
    /// The changes that were applied to the metadata.
    pub changes: Vec<ViewUpdate>,
}

impl ViewMetadataBuilder {
    const LAST_ADDED: i32 = TableMetadataBuilder::LAST_ADDED;

    /// Creates a new view metadata builder.
    pub fn new(
        location: String,
        schema: Schema,
        view_version: ViewVersion,
        format_version: ViewFormatVersion,
        properties: HashMap<String, String>,
    ) -> Result<Self> {
        let builder = Self {
            metadata: ViewMetadata {
                format_version,
                view_uuid: Uuid::now_v7(),
                location: "".to_string(), // Overwritten immediately by set_location
                current_version_id: -1,   // Overwritten immediately by set_current_version,
                versions: HashMap::new(), // Overwritten immediately by set_current_version
                version_log: Vec::new(),
                schemas: HashMap::new(), // Overwritten immediately by set_current_version
                properties: HashMap::new(), // Overwritten immediately by set_properties
            },
            changes: vec![],
            last_added_schema_id: None, // Overwritten immediately by set_current_version
            last_added_version_id: None, // Overwritten immediately by set_current_version
            history_entry: None,
            previous_view_version: None, // This is a new view
        };

        builder
            .set_location(location)
            .set_current_version(view_version, schema)?
            .set_properties(properties)
    }

    /// Creates a new view metadata builder from the given metadata to modify it.
    #[must_use]
    pub fn new_from_metadata(previous: ViewMetadata) -> Self {
        let previous_view_version = previous.current_version().clone();
        Self {
            metadata: previous,
            changes: Vec::default(),
            last_added_schema_id: None,
            last_added_version_id: None,
            history_entry: None,
            previous_view_version: Some(previous_view_version),
        }
    }

    /// Creates a new view metadata builder from the given view creation.
    pub fn from_view_creation(view_creation: ViewCreation) -> Result<Self> {
        let ViewCreation {
            location,
            schema,
            properties,
            name: _,
            representations,
            default_catalog,
            default_namespace,
            summary,
        } = view_creation;
        let version = ViewVersion::builder()
            .with_default_catalog(default_catalog)
            .with_default_namespace(default_namespace)
            .with_representations(representations)
            .with_schema_id(schema.schema_id())
            .with_summary(summary)
            .with_timestamp_ms(Utc::now().timestamp_millis())
            .with_version_id(INITIAL_VIEW_VERSION_ID)
            .build();

        Self::new(location, schema, version, ViewFormatVersion::V1, properties)
    }

    /// Upgrade `FormatVersion`. Downgrades are not allowed.
    ///
    /// # Errors
    /// - Cannot downgrade to older format versions.
    pub fn upgrade_format_version(self, format_version: ViewFormatVersion) -> Result<Self> {
        if format_version < self.metadata.format_version {
            return Err(Error::new(
                ErrorKind::DataInvalid,
                format!(
                    "Cannot downgrade ViewFormatVersion from {} to {}",
                    self.metadata.format_version, format_version
                ),
            ));
        }

        if format_version != self.metadata.format_version {
            match format_version {
                ViewFormatVersion::V1 => {
                    // No changes needed for V1
                }
            }
        }

        Ok(self)
    }

    /// Set the location of the view, stripping any trailing slashes.
    pub fn set_location(mut self, location: String) -> Self {
        let location = location.trim_end_matches('/').to_string();
        if self.metadata.location != location {
            self.changes.push(ViewUpdate::SetLocation {
                location: location.clone(),
            });
            self.metadata.location = location;
        }

        self
    }

    /// Set an existing view version as the current version.
    ///
    /// # Errors
    /// - The specified `version_id` does not exist.
    /// - The specified `version_id` is `-1` but no version has been added.
    pub fn set_current_version_id(mut self, mut version_id: i32) -> Result<Self> {
        if version_id == Self::LAST_ADDED {
            let Some(last_added_id) = self.last_added_version_id else {
                return Err(Error::new(
                    ErrorKind::DataInvalid,
                    "Cannot set current version id to last added version: no version has been added.",
                ));
            };
            version_id = last_added_id;
        }

        let version_id = version_id; // make immutable

        if version_id == self.metadata.current_version_id {
            return Ok(self);
        }

        let version = self.metadata.versions.get(&version_id).ok_or_else(|| {
            Error::new(
                ErrorKind::DataInvalid,
                format!("Cannot set current version to unknown version with id: {version_id}"),
            )
        })?;

        self.metadata.current_version_id = version_id;

        if self.last_added_version_id == Some(version_id) {
            self.changes.push(ViewUpdate::SetCurrentViewVersion {
                view_version_id: Self::LAST_ADDED,
            });
        } else {
            self.changes.push(ViewUpdate::SetCurrentViewVersion {
                view_version_id: version_id,
            });
        }

        // Use the timestamp of the snapshot if it was added in this set of changes,
        // otherwise use a current timestamp for the log. The view version was added
        // by a past transaction.
        let version_added_in_this_changes = self
            .changes
            .iter()
            .any(|update| matches!(update, ViewUpdate::AddViewVersion { view_version } if view_version.version_id() == version_id));

        let mut log = version.log();
        if !version_added_in_this_changes {
            log.set_timestamp_ms(Utc::now().timestamp_millis());
        }

        self.history_entry = Some(log);

        Ok(self)
    }

    /// Add a new view version and set it as current.
    pub fn set_current_version(
        mut self,
        view_version: ViewVersion,
        schema: Schema,
    ) -> Result<Self> {
        let schema_id = self.add_schema_internal(schema);
        let view_version = view_version.with_schema_id(schema_id);
        let view_version_id = self.add_version_internal(view_version)?;
        self.set_current_version_id(view_version_id)
    }

    /// Add a new version to the view.
    ///
    /// # Errors
    /// - The schema ID of the version is set to `-1`, but no schema has been added.
    /// - The schema ID of the specified version is unknown.
    /// - Multiple queries for the same dialect are added.
    pub fn add_version(mut self, view_version: ViewVersion) -> Result<Self> {
        self.add_version_internal(view_version)?;

        Ok(self)
    }

    fn add_version_internal(&mut self, view_version: ViewVersion) -> Result<i32> {
        let version_id = self.reuse_or_create_new_view_version_id(&view_version);
        let view_version = view_version.with_version_id(version_id);

        if self.metadata.versions.contains_key(&version_id) {
            // ToDo Discuss: Similar to TableMetadata sort-order, Java does not add changes
            // in this case. I prefer to add changes as the state of the builder is
            // potentially mutated (`last_added_version_id`), thus we should record the change.
            if self.last_added_version_id != Some(version_id) {
                self.changes
                    .push(ViewUpdate::AddViewVersion { view_version });
                self.last_added_version_id = Some(version_id);
            }
            return Ok(version_id);
        }

        let view_version = if view_version.schema_id() == Self::LAST_ADDED {
            let last_added_schema_id = self.last_added_schema_id.ok_or_else(|| {
                Error::new(
                    ErrorKind::DataInvalid,
                    "Cannot set last added schema: no schema has been added",
                )
            })?;
            view_version.with_schema_id(last_added_schema_id)
        } else {
            view_version
        };

        if !self
            .metadata
            .schemas
            .contains_key(&view_version.schema_id())
        {
            return Err(Error::new(
                ErrorKind::DataInvalid,
                format!(
                    "Cannot add version with unknown schema: {}",
                    view_version.schema_id()
                ),
            ));
        }

        require_unique_dialects(&view_version)?;

        // The `TableMetadataBuilder` uses these checks in multiple places - also in Java.
        // If we think delayed requests are a problem, I think we should also add it here.
        if let Some(last) = self.metadata.version_log.last() {
            // commits can happen concurrently from different machines.
            // A tolerance helps us avoid failure for small clock skew
            if view_version.timestamp_ms() - last.timestamp_ms() < -ONE_MINUTE_MS {
                return Err(Error::new(
                    ErrorKind::DataInvalid,
                    format!(
                        "Invalid snapshot timestamp {}: before last snapshot timestamp {}",
                        view_version.timestamp_ms(),
                        last.timestamp_ms()
                    ),
                ));
            }
        }

        self.metadata
            .versions
            .insert(version_id, Arc::new(view_version.clone()));

        let view_version = if let Some(last_added_schema_id) = self.last_added_schema_id {
            if view_version.schema_id() == last_added_schema_id {
                view_version.with_schema_id(Self::LAST_ADDED)
            } else {
                view_version
            }
        } else {
            view_version
        };
        self.changes
            .push(ViewUpdate::AddViewVersion { view_version });

        self.last_added_version_id = Some(version_id);

        Ok(version_id)
    }

    fn reuse_or_create_new_view_version_id(&self, new_view_version: &ViewVersion) -> i32 {
        self.metadata
            .versions
            .iter()
            .find_map(|(id, other_version)| {
                new_view_version
                    .behaves_identical_to(other_version)
                    .then_some(*id)
            })
            .unwrap_or_else(|| {
                self.get_highest_view_version_id()
                    .map(|id| id + 1)
                    .unwrap_or(INITIAL_VIEW_VERSION_ID)
            })
    }

    fn get_highest_view_version_id(&self) -> Option<i32> {
        self.metadata.versions.keys().max().copied()
    }

    /// Add a new schema to the view.
    pub fn add_schema(mut self, schema: Schema) -> Self {
        self.add_schema_internal(schema);

        self
    }

    fn add_schema_internal(&mut self, schema: Schema) -> SchemaId {
        let schema_id = self.reuse_or_create_new_schema_id(&schema);

        if self.metadata.schemas.contains_key(&schema_id) {
            // ToDo Discuss: Java does not add changes in this case. I prefer to add changes
            // as the state of the builder is potentially mutated (`last_added_schema_id`),
            // thus we should record the change.
            if self.last_added_schema_id != Some(schema_id) {
                self.changes.push(ViewUpdate::AddSchema {
                    schema: schema.clone().with_schema_id(schema_id),
                    last_column_id: None,
                });
                self.last_added_schema_id = Some(schema_id);
            }
            return schema_id;
        }

        let schema = schema.with_schema_id(schema_id);

        self.metadata
            .schemas
            .insert(schema_id, Arc::new(schema.clone()));
        let last_column_id = schema.highest_field_id();
        self.changes.push(ViewUpdate::AddSchema {
            schema,
            last_column_id: Some(last_column_id),
        });

        self.last_added_schema_id = Some(schema_id);

        schema_id
    }

    fn reuse_or_create_new_schema_id(&self, new_schema: &Schema) -> SchemaId {
        self.metadata
            .schemas
            .iter()
            .find_map(|(id, schema)| new_schema.is_same_schema(schema).then_some(*id))
            .unwrap_or_else(|| {
                self.get_highest_schema_id()
                    .map(|id| id + 1)
                    .unwrap_or(DEFAULT_SCHEMA_ID)
            })
    }

    fn get_highest_schema_id(&self) -> Option<SchemaId> {
        self.metadata.schemas.keys().max().copied()
    }

    /// Update properties of the view.
    pub fn set_properties(mut self, updates: HashMap<String, String>) -> Result<Self> {
        if updates.is_empty() {
            return Ok(self);
        }

        let num_versions_to_keep = updates
            .get(VIEW_PROPERTY_VERSION_HISTORY_SIZE)
            .and_then(|v| v.parse::<i64>().ok())
            .unwrap_or(1);
        if num_versions_to_keep < 0 {
            return Err(Error::new(
                ErrorKind::DataInvalid,
                format!(
                    "{VIEW_PROPERTY_VERSION_HISTORY_SIZE} must be positive but was {num_versions_to_keep}"
                ),
            ));
        }

        self.metadata.properties.extend(updates.clone());
        self.changes.push(ViewUpdate::SetProperties { updates });

        Ok(self)
    }

    /// Remove properties from the view
    pub fn remove_properties(mut self, removals: &[String]) -> Self {
        if removals.is_empty() {
            return self;
        }

        for property in removals {
            self.metadata.properties.remove(property);
        }

        self.changes.push(ViewUpdate::RemoveProperties {
            removals: removals.to_vec(),
        });

        self
    }

    /// Assign a new UUID to the view.
    pub fn assign_uuid(mut self, uuid: Uuid) -> Self {
        if self.metadata.view_uuid != uuid {
            self.metadata.view_uuid = uuid;
            self.changes.push(ViewUpdate::AssignUuid { uuid });
        }

        self
    }

    /// Build the `ViewMetadata` from the changes.
    pub fn build(mut self) -> Result<ViewMetadataBuildResult> {
        if let Some(history_entry) = self.history_entry.take() {
            self.metadata.version_log.push(history_entry);
        }

        // We should run validate before `self.metadata.current_version()` below,
        // as it might panic if the metadata is invalid.
        self.metadata.validate()?;

        if let Some(previous) = self.previous_view_version.take()
            && !allow_replace_drop_dialects(&self.metadata.properties)
        {
            require_no_dialect_dropped(&previous, self.metadata.current_version())?;
        }

        let _expired_versions = self.expire_versions();
        self.metadata.version_log = update_version_log(
            self.metadata.version_log,
            self.metadata.versions.keys().copied().collect(),
        );

        Ok(ViewMetadataBuildResult {
            metadata: self.metadata,
            changes: self.changes,
        })
    }

    /// Removes expired versions from the view and returns them.
    fn expire_versions(&mut self) -> Vec<ViewVersionRef> {
        let num_versions_to_keep = self
            .metadata
            .properties
            .get(VIEW_PROPERTY_VERSION_HISTORY_SIZE)
            .and_then(|v| v.parse::<usize>().ok())
            .unwrap_or(VIEW_PROPERTY_VERSION_HISTORY_SIZE_DEFAULT)
            .max(1);

        // expire old versions, but keep at least the versions added in this builder
        let num_added_versions = self
            .changes
            .iter()
            .filter(|update| matches!(update, ViewUpdate::AddViewVersion { .. }))
            .count();
        let num_versions_to_keep = num_added_versions.max(num_versions_to_keep);

        if self.metadata.versions.len() > num_versions_to_keep {
            // version ids are assigned sequentially. keep the latest versions by ID.
            let mut versions_to_keep = self
                .metadata
                .versions
                .keys()
                .copied()
                .sorted()
                .rev()
                .take(num_versions_to_keep)
                .collect::<HashSet<_>>();

            // always retain current version
            if !versions_to_keep.contains(&self.metadata.current_version_id) {
                // Remove the lowest ID
                if num_versions_to_keep > num_added_versions {
                    let lowest_id = versions_to_keep.iter().min().copied();
                    lowest_id.map(|id| versions_to_keep.remove(&id));
                }
                // Add the current version ID
                versions_to_keep.insert(self.metadata.current_version_id);
            }

            let mut expired_versions = Vec::new();
            // remove all versions which are not in versions_to_keep from the metadata
            // and add them to the expired_versions list
            self.metadata.versions.retain(|id, version| {
                if versions_to_keep.contains(id) {
                    true
                } else {
                    expired_versions.push(version.clone());
                    false
                }
            });

            expired_versions
        } else {
            Vec::new()
        }
    }
}

/// Expire version log entries that are no longer relevant.
/// Returns the history entries to retain.
fn update_version_log(
    version_log: Vec<ViewVersionLog>,
    ids_to_keep: HashSet<i32>,
) -> Vec<ViewVersionLog> {
    let mut retained_history = Vec::new();
    for log_entry in version_log {
        if ids_to_keep.contains(&log_entry.version_id()) {
            retained_history.push(log_entry);
        } else {
            retained_history.clear();
        }
    }
    retained_history
}

fn allow_replace_drop_dialects(properties: &HashMap<String, String>) -> bool {
    properties
        .get(VIEW_PROPERTY_REPLACE_DROP_DIALECT_ALLOWED)
        .map_or(
            VIEW_PROPERTY_REPLACE_DROP_DIALECT_ALLOWED_DEFAULT,
            |value| is_truthy(value),
        )
}

fn require_no_dialect_dropped(previous: &ViewVersion, current: &ViewVersion) -> Result<()> {
    let base_dialects = lowercase_sql_dialects_for(previous);
    let updated_dialects = lowercase_sql_dialects_for(current);

    if !updated_dialects.is_superset(&base_dialects) {
        return Err(Error::new(
            ErrorKind::DataInvalid,
            format!(
                "Cannot replace view due to loss of view dialects: \nPrevious dialects: {:?}\nNew dialects: {:?}\nSet {} to true to allow dropping dialects.",
                Vec::from_iter(base_dialects),
                Vec::from_iter(updated_dialects),
                VIEW_PROPERTY_REPLACE_DROP_DIALECT_ALLOWED
            ),
        ));
    }

    Ok(())
}

fn lowercase_sql_dialects_for(view_version: &ViewVersion) -> HashSet<String> {
    view_version
        .representations()
        .iter()
        .map(|repr| match repr {
            ViewRepresentation::Sql(sql_repr) => sql_repr.dialect.to_lowercase(),
        })
        .collect()
}

pub(super) fn require_unique_dialects(view_version: &ViewVersion) -> Result<()> {
    let mut seen_dialects = HashSet::with_capacity(view_version.representations().len());
    for repr in view_version.representations().iter() {
        match repr {
            ViewRepresentation::Sql(sql_repr) => {
                if !seen_dialects.insert(sql_repr.dialect.to_lowercase()) {
                    return Err(Error::new(
                        ErrorKind::DataInvalid,
                        format!(
                            "Invalid view version: Cannot add multiple queries for dialect {}",
                            sql_repr.dialect
                        ),
                    ));
                }
            }
        }
    }
    Ok(())
}

#[cfg(test)]
mod test {
    use super::super::view_metadata::tests::get_test_view_metadata;
    use super::*;
    use crate::NamespaceIdent;
    use crate::spec::{
        NestedField, PrimitiveType, SqlViewRepresentation, Type, ViewRepresentations,
    };

    fn new_view_version(id: usize, schema_id: SchemaId, sql: &str) -> ViewVersion {
        new_view_version_with_dialect(id, schema_id, sql, vec!["spark"])
    }

    fn new_view_version_with_dialect(
        id: usize,
        schema_id: SchemaId,
        sql: &str,
        dialects: Vec<&str>,
    ) -> ViewVersion {
        ViewVersion::builder()
            .with_version_id(id as i32)
            .with_schema_id(schema_id)
            .with_timestamp_ms(1573518431300)
            .with_default_catalog(Some("prod".to_string()))
            .with_summary(HashMap::from_iter(vec![(
                "user".to_string(),
                "some-user".to_string(),
            )]))
            .with_representations(ViewRepresentations(
                dialects
                    .iter()
                    .map(|dialect| {
                        ViewRepresentation::Sql(SqlViewRepresentation {
                            dialect: dialect.to_string(),
                            sql: sql.to_string(),
                        })
                    })
                    .collect(),
            ))
            .with_default_namespace(NamespaceIdent::new("default".to_string()))
            .build()
    }

    fn builder_without_changes() -> ViewMetadataBuilder {
        ViewMetadataBuilder::new_from_metadata(get_test_view_metadata("ViewMetadataV1Valid.json"))
    }

    #[test]
    fn test_minimal_builder() {
        let location = "s3://bucket/table".to_string();
        let schema = Schema::builder()
            .with_schema_id(1)
            .with_fields(vec![])
            .build()
            .unwrap();
        // Version ID and schema should be re-assigned
        let version = new_view_version(20, 21, "select 1 as count");
        let format_version = ViewFormatVersion::V1;
        let properties = HashMap::from_iter(vec![("key".to_string(), "value".to_string())]);

        let build_result = ViewMetadataBuilder::new(
            location.clone(),
            schema.clone(),
            version.clone(),
            format_version,
            properties.clone(),
        )
        .unwrap()
        .build()
        .unwrap();

        let metadata = build_result.metadata;
        assert_eq!(metadata.location, location);
        assert_eq!(metadata.current_version_id, INITIAL_VIEW_VERSION_ID);
        assert_eq!(metadata.format_version, format_version);
        assert_eq!(metadata.properties, properties);
        assert_eq!(metadata.versions.len(), 1);
        assert_eq!(metadata.schemas.len(), 1);
        assert_eq!(metadata.version_log.len(), 1);
        assert_eq!(
            Arc::unwrap_or_clone(metadata.versions[&INITIAL_VIEW_VERSION_ID].clone()),
            version
                .clone()
                .with_version_id(INITIAL_VIEW_VERSION_ID)
                .with_schema_id(0)
        );

        let changes = build_result.changes;
        assert_eq!(changes.len(), 5);
        assert!(changes.contains(&ViewUpdate::SetLocation { location }));
        assert!(
            changes.contains(&ViewUpdate::AddViewVersion {
                view_version: version
                    .with_version_id(INITIAL_VIEW_VERSION_ID)
                    .with_schema_id(-1)
            })
        );
        assert!(changes.contains(&ViewUpdate::SetCurrentViewVersion {
            view_version_id: -1
        }));
        assert!(changes.contains(&ViewUpdate::AddSchema {
            schema: schema.clone().with_schema_id(0),
            last_column_id: Some(0)
        }));
        assert!(changes.contains(&ViewUpdate::SetProperties {
            updates: properties
        }));
    }

    #[test]
    fn test_version_expiration() {
        let v1 = new_view_version(0, 1, "select 1 as count");
        let v2 = new_view_version(0, 1, "select count(1) as count from t2");
        let v3 = new_view_version(0, 1, "select count from t1");

        let builder = builder_without_changes()
            .add_version(v1)
            .unwrap()
            .add_version(v2)
            .unwrap()
            .add_version(v3)
            .unwrap();
        let builder_without_changes = builder.clone().build().unwrap().metadata.into_builder();

        // No limit on versions
        let metadata = builder.clone().build().unwrap().metadata;
        assert_eq!(
            metadata.versions.keys().cloned().collect::<HashSet<_>>(),
            HashSet::from_iter(vec![1, 2, 3, 4])
        );

        // Limit to 2 versions, we still want to keep 3 versions as 3 where added during this build
        // Plus the current version
        let metadata = builder
            .clone()
            .set_properties(HashMap::from_iter(vec![(
                VIEW_PROPERTY_VERSION_HISTORY_SIZE.to_string(),
                "2".to_string(),
            )]))
            .unwrap()
            .build()
            .unwrap()
            .metadata;
        assert_eq!(
            metadata.versions.keys().cloned().collect::<HashSet<_>>(),
            HashSet::from_iter(vec![1, 2, 3, 4])
        );
        assert_eq!(metadata.version_log.len(), 1);

        // Limit to 2 versions in new build, only keep 2.
        // One of them should be the current
        let metadata = builder_without_changes
            .clone()
            .set_properties(HashMap::from_iter(vec![(
                VIEW_PROPERTY_VERSION_HISTORY_SIZE.to_string(),
                "2".to_string(),
            )]))
            .unwrap()
            .build()
            .unwrap()
            .metadata;
        assert_eq!(
            metadata.versions.keys().cloned().collect::<HashSet<_>>(),
            HashSet::from_iter(vec![1, 4])
        );

        // Keep at least 1 version irrespective of the limit.
        // This is the current version
        let metadata = builder_without_changes
            .set_properties(HashMap::from_iter(vec![(
                VIEW_PROPERTY_VERSION_HISTORY_SIZE.to_string(),
                "0".to_string(),
            )]))
            .unwrap()
            .build()
            .unwrap()
            .metadata;
        assert_eq!(
            metadata.versions.keys().cloned().collect::<HashSet<_>>(),
            HashSet::from_iter(vec![1])
        );
    }

    #[test]
    fn test_update_version_log() {
        let v1 = new_view_version(1, 1, "select 1 as count");
        let v2 = new_view_version(2, 1, "select count(1) as count from t2");
        let v3 = new_view_version(3, 1, "select count from t1");

        let one = ViewVersionLog::new(1, v1.timestamp_ms());
        let two = ViewVersionLog::new(2, v2.timestamp_ms());
        let three = ViewVersionLog::new(3, v3.timestamp_ms());

        assert_eq!(
            update_version_log(
                vec![one.clone(), two.clone(), three.clone()],
                HashSet::from_iter(vec![1, 2, 3])
            ),
            vec![one.clone(), two.clone(), three.clone()]
        );

        // one was an invalid entry in the history, so all previous elements are removed
        assert_eq!(
            update_version_log(
                vec![
                    three.clone(),
                    two.clone(),
                    one.clone(),
                    two.clone(),
                    three.clone()
                ],
                HashSet::from_iter(vec![2, 3])
            ),
            vec![two.clone(), three.clone()]
        );

        // two was an invalid entry in the history, so all previous elements are removed
        assert_eq!(
            update_version_log(
                vec![
                    one.clone(),
                    two.clone(),
                    three.clone(),
                    one.clone(),
                    three.clone()
                ],
                HashSet::from_iter(vec![1, 3])
            ),
            vec![three.clone(), one.clone(), three.clone()]
        );
    }

    #[test]
    fn test_use_previously_added_version() {
        let v2 = new_view_version(2, 1, "select 1 as count");
        let v3 = new_view_version(3, 1, "select count(1) as count from t2");
        let schema = Schema::builder().build().unwrap();

        let log_v2 = ViewVersionLog::new(2, v2.timestamp_ms());
        let log_v3 = ViewVersionLog::new(3, v3.timestamp_ms());

        let metadata_v2 = builder_without_changes()
            .set_current_version(v2.clone(), schema.clone())
            .unwrap()
            .build()
            .unwrap()
            .metadata;

        // Log should use the exact timestamp of v1
        assert_eq!(metadata_v2.version_log.last().unwrap(), &log_v2);

        // Add second version, should use exact timestamp of v2
        let metadata_v3 = metadata_v2
            .into_builder()
            .set_current_version(v3.clone(), schema)
            .unwrap()
            .build()
            .unwrap()
            .metadata;

        assert_eq!(metadata_v3.version_log[1..], vec![
            log_v2.clone(),
            log_v3.clone()
        ]);

        // Re-use Version 1, add a new log entry with a new timestamp
        let metadata_v4 = metadata_v3
            .into_builder()
            .set_current_version_id(2)
            .unwrap()
            .build()
            .unwrap()
            .metadata;

        // Last entry should be equal to v2 but with an updated timestamp
        let entry = metadata_v4.version_log.last().unwrap();
        assert_eq!(entry.version_id(), 2);
        assert!(entry.timestamp_ms() > v2.timestamp_ms());
    }

    #[test]
    fn test_assign_uuid() {
        let builder = builder_without_changes();
        let uuid = Uuid::now_v7();
        let build_result = builder.clone().assign_uuid(uuid).build().unwrap();
        assert_eq!(build_result.metadata.view_uuid, uuid);
        assert_eq!(build_result.changes, vec![ViewUpdate::AssignUuid { uuid }]);
    }

    #[test]
    fn test_set_location() {
        let builder = builder_without_changes();
        let location = "s3://bucket/table".to_string();
        let build_result = builder
            .clone()
            .set_location(location.clone())
            .build()
            .unwrap();
        assert_eq!(build_result.metadata.location, location);
        assert_eq!(build_result.changes, vec![ViewUpdate::SetLocation {
            location
        }]);
    }

    #[test]
    fn test_set_and_remove_properties() {
        let builder = builder_without_changes();
        let properties = HashMap::from_iter(vec![
            ("key1".to_string(), "value1".to_string()),
            ("key2".to_string(), "value2".to_string()),
        ]);
        let build_result = builder
            .clone()
            .set_properties(properties.clone())
            .unwrap()
            .remove_properties(&["key2".to_string(), "key3".to_string()])
            .build()
            .unwrap();
        assert_eq!(
            build_result.metadata.properties.get("key1"),
            Some(&"value1".to_string())
        );
        assert_eq!(build_result.metadata.properties.get("key2"), None);
        assert_eq!(build_result.changes, vec![
            ViewUpdate::SetProperties {
                updates: properties
            },
            ViewUpdate::RemoveProperties {
                removals: vec!["key2".to_string(), "key3".to_string()]
            }
        ]);
    }

    #[test]
    fn test_add_schema() {
        let builder = builder_without_changes();
        let schema = Schema::builder()
            .with_schema_id(1)
            .with_fields(vec![])
            .build()
            .unwrap();
        let build_result = builder.clone().add_schema(schema.clone()).build().unwrap();
        assert_eq!(build_result.metadata.schemas.len(), 2);
        assert_eq!(build_result.changes, vec![ViewUpdate::AddSchema {
            schema: schema.clone().with_schema_id(2),
            last_column_id: Some(0)
        }]);

        // Add schema again - id is reused
        let build_result = builder.clone().add_schema(schema.clone()).build().unwrap();
        assert_eq!(build_result.metadata.schemas.len(), 2);
        assert_eq!(build_result.changes, vec![ViewUpdate::AddSchema {
            schema: schema.clone().with_schema_id(2),
            last_column_id: Some(0)
        }]);
    }

    #[test]
    fn test_add_and_set_current_version() {
        let builder = builder_without_changes();
        let v1 = new_view_version(2, 1, "select 1 as count");
        let v2 = new_view_version(3, 2, "select count(1) as count from t2");
        let v2_schema = Schema::builder()
            .with_schema_id(2)
            .with_fields(vec![])
            .build()
            .unwrap();

        let build_result = builder
            .clone()
            .add_version(v1.clone())
            .unwrap()
            .add_schema(v2_schema.clone())
            .add_version(v2.clone())
            .unwrap()
            .set_current_version_id(3)
            .unwrap()
            .build()
            .unwrap();

        assert_eq!(build_result.metadata.current_version_id, 3);
        assert_eq!(build_result.metadata.versions.len(), 3);
        assert_eq!(build_result.metadata.schemas.len(), 2);
        assert_eq!(build_result.metadata.version_log.len(), 2);
        assert_eq!(
            Arc::unwrap_or_clone(build_result.metadata.versions[&2].clone()),
            v1.clone().with_version_id(2).with_schema_id(1)
        );
        assert_eq!(
            Arc::unwrap_or_clone(build_result.metadata.versions[&3].clone()),
            v2.clone().with_version_id(3).with_schema_id(2)
        );
        assert_eq!(build_result.changes.len(), 4);
        assert_eq!(build_result.changes, vec![
            ViewUpdate::AddViewVersion {
                view_version: v1.clone().with_version_id(2).with_schema_id(1)
            },
            ViewUpdate::AddSchema {
                schema: v2_schema.clone().with_schema_id(2),
                last_column_id: Some(0)
            },
            ViewUpdate::AddViewVersion {
                view_version: v2.clone().with_version_id(3).with_schema_id(-1)
            },
            ViewUpdate::SetCurrentViewVersion {
                view_version_id: -1
            }
        ]);
        assert_eq!(
            build_result
                .metadata
                .version_log
                .iter()
                .map(|v| v.version_id())
                .collect::<Vec<_>>(),
            vec![1, 3]
        );
    }

    #[test]
    fn test_schema_and_version_id_reassignment() {
        let builder = builder_without_changes();
        let v1 = new_view_version(0, 1, "select 1 as count");
        let v2 = new_view_version(0, 2, "select count(1) as count from t2");
        let v2_schema = Schema::builder()
            .with_schema_id(0)
            .with_fields(vec![])
            .build()
            .unwrap();

        let build_result = builder
            .clone()
            .add_version(v1.clone())
            .unwrap()
            .set_current_version(v2.clone(), v2_schema.clone())
            .unwrap()
            .build()
            .unwrap();

        assert_eq!(build_result.metadata.current_version_id, 3);
        assert_eq!(build_result.metadata.versions.len(), 3);
        assert_eq!(build_result.metadata.schemas.len(), 2);
        assert_eq!(build_result.metadata.version_log.len(), 2);
        assert_eq!(
            Arc::unwrap_or_clone(build_result.metadata.versions[&2].clone()),
            v1.clone().with_version_id(2).with_schema_id(1)
        );
        assert_eq!(
            Arc::unwrap_or_clone(build_result.metadata.versions[&3].clone()),
            v2.clone().with_version_id(3).with_schema_id(2)
        );
        assert_eq!(build_result.changes.len(), 4);
        assert_eq!(build_result.changes, vec![
            ViewUpdate::AddViewVersion {
                view_version: v1.clone().with_version_id(2).with_schema_id(1)
            },
            ViewUpdate::AddSchema {
                schema: v2_schema.clone().with_schema_id(2),
                last_column_id: Some(0)
            },
            ViewUpdate::AddViewVersion {
                view_version: v2.clone().with_version_id(3).with_schema_id(-1)
            },
            ViewUpdate::SetCurrentViewVersion {
                view_version_id: -1
            }
        ]);
        assert_eq!(
            build_result
                .metadata
                .version_log
                .iter()
                .map(|v| v.version_id())
                .collect::<Vec<_>>(),
            vec![1, 3]
        );
    }

    #[test]
    fn test_view_version_deduplication() {
        let builder = builder_without_changes();
        let v1 = new_view_version(0, 1, "select * from ns.tbl");

        assert_eq!(builder.metadata.versions.len(), 1);
        let build_result = builder
            .clone()
            .add_version(v1.clone())
            .unwrap()
            .add_version(v1)
            .unwrap()
            .build()
            .unwrap();

        assert_eq!(build_result.metadata.versions.len(), 2);
        assert_eq!(build_result.metadata.schemas.len(), 1);
    }

    #[test]
    fn test_view_version_and_schema_deduplication() {
        let schema_one = Schema::builder()
            .with_schema_id(5)
            .with_fields(vec![
                NestedField::required(1, "x", Type::Primitive(PrimitiveType::Long)).into(),
            ])
            .build()
            .unwrap();
        let schema_two = Schema::builder()
            .with_schema_id(7)
            .with_fields(vec![
                NestedField::required(1, "y", Type::Primitive(PrimitiveType::Long)).into(),
            ])
            .build()
            .unwrap();
        let schema_three = Schema::builder()
            .with_schema_id(9)
            .with_fields(vec![
                NestedField::required(1, "z", Type::Primitive(PrimitiveType::Long)).into(),
            ])
            .build()
            .unwrap();

        let v1 = new_view_version(1, 5, "select * from ns.tbl");
        let v2 = new_view_version(1, 7, "select count(*) from ns.tbl");
        let v3 = new_view_version(1, 9, "select count(*) as count from ns.tbl");

        let build_result = builder_without_changes()
            .add_schema(schema_one.clone())
            .add_schema(schema_two.clone())
            .add_schema(schema_three.clone())
            .set_current_version(v1.clone(), schema_one.clone())
            .unwrap()
            .set_current_version(v2.clone(), schema_two.clone())
            .unwrap()
            .set_current_version(v3.clone(), schema_three.clone())
            .unwrap()
            .set_current_version(v3.clone(), schema_three.clone())
            .unwrap()
            .set_current_version(v2.clone(), schema_two.clone())
            .unwrap()
            .set_current_version(v1.clone(), schema_one.clone())
            .unwrap()
            .build()
            .unwrap();

        assert_eq!(
            Arc::unwrap_or_clone(build_result.metadata.current_version().clone()),
            v1.clone().with_version_id(2).with_schema_id(2)
        );
        assert_eq!(build_result.metadata.versions.len(), 4);
        assert_eq!(
            build_result.metadata.versions[&2],
            Arc::new(v1.clone().with_version_id(2).with_schema_id(2))
        );
        assert_eq!(
            build_result.metadata.versions[&3],
            Arc::new(v2.clone().with_version_id(3).with_schema_id(3))
        );
        assert_eq!(
            build_result.metadata.versions[&4],
            Arc::new(v3.clone().with_version_id(4).with_schema_id(4))
        );
        assert_eq!(
            // Remove schema_id 1 and get struct only
            build_result
                .metadata
                .schemas_iter()
                .filter(|s| s.schema_id() != 1)
                .sorted_by_key(|s| s.schema_id())
                .map(|s| s.as_struct())
                .collect::<Vec<_>>(),
            vec![
                schema_one.as_struct(),
                schema_two.as_struct(),
                schema_three.as_struct()
            ]
        )
    }

    #[test]
    fn test_error_on_missing_schema() {
        let builder = builder_without_changes();
        // Missing schema
        assert!(
            builder
                .clone()
                .add_version(new_view_version(0, 10, "SELECT * FROM foo"))
                .unwrap_err()
                .to_string()
                .contains("Cannot add version with unknown schema: 10")
        );

        // Missing last added schema
        assert!(
            builder
                .clone()
                .add_version(new_view_version(0, -1, "SELECT * FROM foo"))
                .unwrap_err()
                .to_string()
                .contains("Cannot set last added schema: no schema has been added")
        );
    }

    #[test]
    fn test_error_on_missing_current_version() {
        let builder = builder_without_changes();
        assert!(builder
            .clone()
            .set_current_version_id(-1)
            .unwrap_err()
            .to_string()
            .contains(
                "Cannot set current version id to last added version: no version has been added."
            ));
        assert!(
            builder
                .clone()
                .set_current_version_id(10)
                .unwrap_err()
                .to_string()
                .contains("Cannot set current version to unknown version with id: 10")
        );
    }

    #[test]
    fn test_set_current_version_to_last_added() {
        let builder = builder_without_changes();
        let v1 = new_view_version(2, 1, "select * from ns.tbl");
        let v2 = new_view_version(3, 1, "select a,b from ns.tbl");
        let meta = builder
            .clone()
            .add_version(v1)
            .unwrap()
            .add_version(v2)
            .unwrap()
            .set_current_version_id(-1)
            .unwrap()
            .build()
            .unwrap();
        assert_eq!(meta.metadata.current_version_id, 3);
    }

    #[test]
    fn test_error_when_setting_negative_version_history_size() {
        let builder = builder_without_changes();
        assert!(
            builder
                .clone()
                .set_properties(HashMap::from_iter(vec![(
                    VIEW_PROPERTY_VERSION_HISTORY_SIZE.to_string(),
                    "-1".to_string(),
                )]))
                .unwrap_err()
                .to_string()
                .contains("version.history.num-entries must be positive but was -1")
        );
    }

    #[test]
    fn test_view_version_changes() {
        let builder = builder_without_changes();

        let v1 = new_view_version(2, 1, "select 1 as count");
        let v2 = new_view_version(3, 1, "select count(1) as count from t2");

        let changes = builder
            .clone()
            .add_version(v1.clone())
            .unwrap()
            .add_version(v2.clone())
            .unwrap()
            .build()
            .unwrap()
            .changes;

        assert_eq!(changes.len(), 2);
        assert_eq!(changes, vec![
            ViewUpdate::AddViewVersion {
                view_version: v1.clone()
            },
            ViewUpdate::AddViewVersion {
                view_version: v2.clone()
            }
        ]);
    }

    #[test]
    fn test_dropping_dialect_fails_by_default() {
        let builder = builder_without_changes();

        let spark = new_view_version_with_dialect(0, 0, "SELECT * FROM foo", vec!["spark"]);
        let spark_trino =
            new_view_version_with_dialect(0, 0, "SELECT * FROM foo", vec!["spark", "trino"]);
        let schema = Schema::builder()
            .with_schema_id(0)
            .with_fields(vec![])
            .build()
            .unwrap();

        let err = builder
            .set_current_version(spark_trino, schema.clone())
            .unwrap()
            .build()
            .unwrap()
            .metadata
            .into_builder()
            .set_current_version(spark, schema)
            .unwrap()
            .build()
            .unwrap_err();

        assert!(
            err.to_string()
                .contains("Cannot replace view due to loss of view dialects")
        );
    }

    #[test]
    fn test_dropping_dialects_does_not_fail_when_allowed() {
        let builder = builder_without_changes();

        let spark = new_view_version_with_dialect(0, 0, "SELECT * FROM foo", vec!["spark"]);
        let spark_trino =
            new_view_version_with_dialect(0, 0, "SELECT * FROM foo", vec!["spark", "trino"]);
        let schema = Schema::builder()
            .with_schema_id(0)
            .with_fields(vec![])
            .build()
            .unwrap();

        let build_result = builder
            .set_properties(HashMap::from_iter(vec![(
                VIEW_PROPERTY_REPLACE_DROP_DIALECT_ALLOWED.to_string(),
                "true".to_string(),
            )]))
            .unwrap()
            .set_current_version(spark_trino, schema.clone())
            .unwrap()
            .build()
            .unwrap()
            .metadata
            .into_builder()
            .set_current_version(spark.clone(), schema)
            .unwrap()
            .build()
            .unwrap();

        assert_eq!(
            Arc::unwrap_or_clone(build_result.metadata.current_version().clone()),
            spark.with_version_id(3).with_schema_id(2)
        );
    }

    #[test]
    fn test_can_add_dialects_by_default() {
        let builder = builder_without_changes();

        let spark = new_view_version_with_dialect(0, 0, "SELECT * FROM foo", vec!["spark"]);
        let spark_trino =
            new_view_version_with_dialect(0, 0, "SELECT * FROM foo", vec!["spark", "trino"]);

        let schema = Schema::builder()
            .with_schema_id(0)
            .with_fields(vec![])
            .build()
            .unwrap();

        let build_result = builder
            .set_current_version(spark.clone(), schema.clone())
            .unwrap()
            .build()
            .unwrap()
            .metadata
            .into_builder()
            .set_current_version(spark_trino.clone(), schema.clone())
            .unwrap()
            .build()
            .unwrap();

        assert_eq!(
            Arc::unwrap_or_clone(build_result.metadata.current_version().clone()),
            spark_trino.with_version_id(3).with_schema_id(2)
        );
    }

    #[test]
    fn test_can_update_dialect_by_default() {
        let builder = builder_without_changes();

        let spark_v1 = new_view_version_with_dialect(0, 0, "SELECT * FROM foo", vec!["spark"]);
        let spark_v2 = new_view_version_with_dialect(0, 0, "SELECT * FROM bar", vec!["spark"]);

        let schema = Schema::builder()
            .with_schema_id(0)
            .with_fields(vec![])
            .build()
            .unwrap();

        let build_result = builder
            .set_current_version(spark_v1.clone(), schema.clone())
            .unwrap()
            .build()
            .unwrap()
            .metadata
            .into_builder()
            .set_current_version(spark_v2.clone(), schema.clone())
            .unwrap()
            .build()
            .unwrap();

        assert_eq!(
            Arc::unwrap_or_clone(build_result.metadata.current_version().clone()),
            spark_v2.with_version_id(3).with_schema_id(2)
        );
    }

    #[test]
    fn test_dropping_dialects_allowed_and_then_disallowed() {
        let builder = builder_without_changes();

        let spark = new_view_version_with_dialect(0, 0, "SELECT * FROM foo", vec!["spark"]);
        let trino = new_view_version_with_dialect(0, 0, "SELECT * FROM foo", vec!["trino"]);

        let schema = Schema::builder()
            .with_schema_id(0)
            .with_fields(vec![])
            .build()
            .unwrap();

        let updated = builder
            .set_current_version(spark.clone(), schema.clone())
            .unwrap()
            .build()
            .unwrap()
            .metadata
            .into_builder()
            .set_current_version(trino.clone(), schema.clone())
            .unwrap()
            .set_properties(HashMap::from_iter(vec![(
                VIEW_PROPERTY_REPLACE_DROP_DIALECT_ALLOWED.to_string(),
                "true".to_string(),
            )]))
            .unwrap()
            .build()
            .unwrap();

        assert_eq!(
            Arc::unwrap_or_clone(updated.metadata.current_version().clone()),
            trino.with_version_id(3).with_schema_id(2)
        );

        let err = updated
            .metadata
            .into_builder()
            .set_current_version(spark.clone(), schema.clone())
            .unwrap()
            .set_properties(HashMap::from_iter(vec![(
                VIEW_PROPERTY_REPLACE_DROP_DIALECT_ALLOWED.to_string(),
                "false".to_string(),
            )]))
            .unwrap()
            .build()
            .unwrap_err();

        assert!(
            err.to_string()
                .contains("Cannot replace view due to loss of view dialects")
        );
    }

    #[test]
    fn test_require_no_dialect_dropped() {
        let previous = ViewVersion::builder()
            .with_version_id(0)
            .with_schema_id(0)
            .with_timestamp_ms(0)
            .with_representations(ViewRepresentations(vec![
                ViewRepresentation::Sql(SqlViewRepresentation {
                    dialect: "trino".to_string(),
                    sql: "SELECT * FROM foo".to_string(),
                }),
                ViewRepresentation::Sql(SqlViewRepresentation {
                    dialect: "spark".to_string(),
                    sql: "SELECT * FROM bar".to_string(),
                }),
            ]))
            .with_default_namespace(NamespaceIdent::new("default".to_string()))
            .build();

        let current = ViewVersion::builder()
            .with_version_id(0)
            .with_schema_id(0)
            .with_timestamp_ms(0)
            .with_representations(ViewRepresentations(vec![ViewRepresentation::Sql(
                SqlViewRepresentation {
                    dialect: "trino".to_string(),
                    sql: "SELECT * FROM foo".to_string(),
                },
            )]))
            .with_default_namespace(NamespaceIdent::new("default".to_string()))
            .build();

        assert!(require_no_dialect_dropped(&previous, &current).is_err());

        let current = ViewVersion::builder()
            .with_version_id(0)
            .with_schema_id(0)
            .with_timestamp_ms(0)
            .with_representations(ViewRepresentations(vec![
                ViewRepresentation::Sql(SqlViewRepresentation {
                    dialect: "spark".to_string(),
                    sql: "SELECT * FROM bar".to_string(),
                }),
                ViewRepresentation::Sql(SqlViewRepresentation {
                    dialect: "trino".to_string(),
                    sql: "SELECT * FROM foo".to_string(),
                }),
            ]))
            .with_default_namespace(NamespaceIdent::new("default".to_string()))
            .build();

        assert!(require_no_dialect_dropped(&previous, &current).is_ok());
    }

    #[test]
    fn test_allow_replace_drop_dialects() {
        use std::collections::HashMap;

        use super::allow_replace_drop_dialects;

        let mut properties = HashMap::new();
        assert!(!allow_replace_drop_dialects(&properties));

        properties.insert(
            "replace.drop-dialect.allowed".to_string(),
            "true".to_string(),
        );
        assert!(allow_replace_drop_dialects(&properties));

        properties.insert(
            "replace.drop-dialect.allowed".to_string(),
            "false".to_string(),
        );
        assert!(!allow_replace_drop_dialects(&properties));

        properties.insert(
            "replace.drop-dialect.allowed".to_string(),
            "TRUE".to_string(),
        );
        assert!(allow_replace_drop_dialects(&properties));

        properties.insert(
            "replace.drop-dialect.allowed".to_string(),
            "FALSE".to_string(),
        );
        assert!(!allow_replace_drop_dialects(&properties));
    }

    #[test]
    fn test_lowercase_sql_dialects_for() {
        let view_version = ViewVersion::builder()
            .with_version_id(0)
            .with_schema_id(0)
            .with_timestamp_ms(0)
            .with_representations(ViewRepresentations(vec![
                ViewRepresentation::Sql(SqlViewRepresentation {
                    dialect: "STARROCKS".to_string(),
                    sql: "SELECT * FROM foo".to_string(),
                }),
                ViewRepresentation::Sql(SqlViewRepresentation {
                    dialect: "trino".to_string(),
                    sql: "SELECT * FROM bar".to_string(),
                }),
                ViewRepresentation::Sql(SqlViewRepresentation {
                    dialect: "Spark".to_string(),
                    sql: "SELECT * FROM bar".to_string(),
                }),
            ]))
            .with_default_namespace(NamespaceIdent::new("default".to_string()))
            .build();

        let dialects = lowercase_sql_dialects_for(&view_version);
        assert_eq!(dialects.len(), 3);
        assert!(dialects.contains("trino"));
        assert!(dialects.contains("spark"));
        assert!(dialects.contains("starrocks"));
    }

    #[test]
    fn test_require_unique_dialects() {
        let view_version = ViewVersion::builder()
            .with_version_id(0)
            .with_schema_id(0)
            .with_timestamp_ms(0)
            .with_representations(ViewRepresentations(vec![
                ViewRepresentation::Sql(SqlViewRepresentation {
                    dialect: "trino".to_string(),
                    sql: "SELECT * FROM foo".to_string(),
                }),
                ViewRepresentation::Sql(SqlViewRepresentation {
                    dialect: "trino".to_string(),
                    sql: "SELECT * FROM bar".to_string(),
                }),
            ]))
            .with_default_namespace(NamespaceIdent::new("default".to_string()))
            .build();

        assert!(require_unique_dialects(&view_version).is_err());

        let view_version = ViewVersion::builder()
            .with_version_id(0)
            .with_schema_id(0)
            .with_timestamp_ms(0)
            .with_representations(ViewRepresentations(vec![
                ViewRepresentation::Sql(SqlViewRepresentation {
                    dialect: "trino".to_string(),
                    sql: "SELECT * FROM foo".to_string(),
                }),
                ViewRepresentation::Sql(SqlViewRepresentation {
                    dialect: "spark".to_string(),
                    sql: "SELECT * FROM bar".to_string(),
                }),
            ]))
            .with_default_namespace(NamespaceIdent::new("default".to_string()))
            .build();

        assert!(require_unique_dialects(&view_version).is_ok());
    }
}
