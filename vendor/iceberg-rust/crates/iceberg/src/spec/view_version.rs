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
 * View Versions!
 */
use std::collections::HashMap;
use std::sync::Arc;

use _serde::ViewVersionV1;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use typed_builder::TypedBuilder;

use super::INITIAL_VIEW_VERSION_ID;
use super::view_metadata::ViewVersionLog;
use crate::catalog::NamespaceIdent;
use crate::error::{Result, timestamp_ms_to_utc};
use crate::spec::{SchemaId, SchemaRef, ViewMetadata};
use crate::{Error, ErrorKind};

/// Reference to [`ViewVersion`].
pub type ViewVersionRef = Arc<ViewVersion>;

/// Alias for the integer type used for view version ids.
pub type ViewVersionId = i32;

#[derive(Debug, PartialEq, Eq, Clone, Serialize, Deserialize, TypedBuilder)]
#[serde(from = "ViewVersionV1", into = "ViewVersionV1")]
#[builder(field_defaults(setter(prefix = "with_")))]
/// A view versions represents the definition of a view at a specific point in time.
pub struct ViewVersion {
    /// A unique long ID
    #[builder(default = INITIAL_VIEW_VERSION_ID)]
    version_id: ViewVersionId,
    /// ID of the schema for the view version
    schema_id: SchemaId,
    /// Timestamp when the version was created (ms from epoch)
    timestamp_ms: i64,
    /// A string to string map of summary metadata about the version
    #[builder(default = HashMap::new())]
    summary: HashMap<String, String>,
    /// A list of representations for the view definition.
    representations: ViewRepresentations,
    /// Catalog name to use when a reference in the SELECT does not contain a catalog
    #[builder(default = None)]
    default_catalog: Option<String>,
    /// Namespace to use when a reference in the SELECT is a single identifier
    default_namespace: NamespaceIdent,
}

impl ViewVersion {
    /// Get the version id of this view version.
    #[inline]
    pub fn version_id(&self) -> ViewVersionId {
        self.version_id
    }

    /// Get the schema id of this view version.
    #[inline]
    pub fn schema_id(&self) -> SchemaId {
        self.schema_id
    }

    /// Get the timestamp of when the view version was created
    #[inline]
    pub fn timestamp(&self) -> Result<DateTime<Utc>> {
        timestamp_ms_to_utc(self.timestamp_ms)
    }

    /// Get the timestamp of when the view version was created in milliseconds since epoch
    #[inline]
    pub fn timestamp_ms(&self) -> i64 {
        self.timestamp_ms
    }

    /// Get summary of the view version
    #[inline]
    pub fn summary(&self) -> &HashMap<String, String> {
        &self.summary
    }

    /// Get this views representations
    #[inline]
    pub fn representations(&self) -> &ViewRepresentations {
        &self.representations
    }

    /// Get the default catalog for this view version
    #[inline]
    pub fn default_catalog(&self) -> Option<&String> {
        self.default_catalog.as_ref()
    }

    /// Get the default namespace to use when a reference in the SELECT is a single identifier
    #[inline]
    pub fn default_namespace(&self) -> &NamespaceIdent {
        &self.default_namespace
    }

    /// Get the schema of this snapshot.
    pub fn schema(&self, view_metadata: &ViewMetadata) -> Result<SchemaRef> {
        view_metadata
            .schema_by_id(self.schema_id())
            .ok_or_else(|| {
                Error::new(
                    ErrorKind::DataInvalid,
                    format!("Schema with id {} not found", self.schema_id()),
                )
            })
            .cloned()
    }

    /// Retrieve the history log entry for this view version.
    pub(crate) fn log(&self) -> ViewVersionLog {
        ViewVersionLog::new(self.version_id, self.timestamp_ms)
    }

    /// Check if this view version behaves the same as another view spec.
    ///
    /// Returns true if the view version is equal to the other view version
    /// with `timestamp_ms` and `version_id` ignored. The following must be identical:
    /// * Summary (all of them)
    /// * Representations
    /// * Default Catalog
    /// * Default Namespace
    /// * The Schema ID
    pub(crate) fn behaves_identical_to(&self, other: &Self) -> bool {
        self.summary == other.summary
            && self.representations == other.representations
            && self.default_catalog == other.default_catalog
            && self.default_namespace == other.default_namespace
            && self.schema_id == other.schema_id
    }

    /// Change the version id of this view version.
    pub fn with_version_id(self, version_id: i32) -> Self {
        Self { version_id, ..self }
    }

    /// Change the schema id of this view version.
    pub fn with_schema_id(self, schema_id: SchemaId) -> Self {
        Self { schema_id, ..self }
    }
}

/// A list of view representations.
#[derive(Debug, PartialEq, Eq, Clone, Serialize, Deserialize)]
pub struct ViewRepresentations(pub(crate) Vec<ViewRepresentation>);

impl ViewRepresentations {
    #[inline]
    /// Get the number of representations
    pub fn len(&self) -> usize {
        self.0.len()
    }

    #[inline]
    /// Check if there are no representations
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    /// Get an iterator over the representations
    pub fn iter(&self) -> impl ExactSizeIterator<Item = &'_ ViewRepresentation> {
        self.0.iter()
    }
}

// Iterator for ViewRepresentations
impl IntoIterator for ViewRepresentations {
    type Item = ViewRepresentation;
    type IntoIter = std::vec::IntoIter<Self::Item>;

    fn into_iter(self) -> Self::IntoIter {
        self.0.into_iter()
    }
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Eq, Clone)]
#[serde(tag = "type")]
/// View definitions can be represented in multiple ways.
/// Representations are documented ways to express a view definition.
// ToDo: Make unique per Dialect
pub enum ViewRepresentation {
    #[serde(rename = "sql")]
    /// The SQL representation stores the view definition as a SQL SELECT,
    Sql(SqlViewRepresentation),
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Eq, Clone)]
#[serde(rename_all = "kebab-case")]
/// The SQL representation stores the view definition as a SQL SELECT,
/// with metadata such as the SQL dialect.
pub struct SqlViewRepresentation {
    #[serde(rename = "sql")]
    /// The SQL SELECT statement that defines the view.
    pub sql: String,
    #[serde(rename = "dialect")]
    /// The dialect of the sql SELECT statement (e.g., "trino" or "spark")
    pub dialect: String,
}

pub(super) mod _serde {
    /// This is a helper module that defines types to help with serialization/deserialization.
    /// For deserialization the input first gets read into the [`ViewVersionV1`] struct.
    /// and then converted into the [Snapshot] struct. Serialization works the other way around.
    /// [ViewVersionV1] are internal struct that are only used for serialization and deserialization.
    use serde::{Deserialize, Serialize};

    use super::{ViewRepresentation, ViewRepresentations, ViewVersion};
    use crate::catalog::NamespaceIdent;

    #[derive(Debug, Serialize, Deserialize, PartialEq, Eq)]
    #[serde(rename_all = "kebab-case")]
    /// Defines the structure of a v1 view version for serialization/deserialization
    pub(crate) struct ViewVersionV1 {
        pub version_id: i32,
        pub schema_id: i32,
        pub timestamp_ms: i64,
        pub summary: std::collections::HashMap<String, String>,
        pub representations: Vec<ViewRepresentation>,
        #[serde(skip_serializing_if = "Option::is_none")]
        pub default_catalog: Option<String>,
        pub default_namespace: NamespaceIdent,
    }

    impl From<ViewVersionV1> for ViewVersion {
        fn from(v1: ViewVersionV1) -> Self {
            ViewVersion {
                version_id: v1.version_id,
                schema_id: v1.schema_id,
                timestamp_ms: v1.timestamp_ms,
                summary: v1.summary,
                representations: ViewRepresentations(v1.representations),
                default_catalog: v1.default_catalog,
                default_namespace: v1.default_namespace,
            }
        }
    }

    impl From<ViewVersion> for ViewVersionV1 {
        fn from(v1: ViewVersion) -> Self {
            ViewVersionV1 {
                version_id: v1.version_id,
                schema_id: v1.schema_id,
                timestamp_ms: v1.timestamp_ms,
                summary: v1.summary,
                representations: v1.representations.0,
                default_catalog: v1.default_catalog,
                default_namespace: v1.default_namespace,
            }
        }
    }
}

impl From<SqlViewRepresentation> for ViewRepresentation {
    fn from(sql: SqlViewRepresentation) -> Self {
        ViewRepresentation::Sql(sql)
    }
}

#[cfg(test)]
mod tests {
    use chrono::{TimeZone, Utc};

    use crate::NamespaceIdent;
    use crate::spec::ViewRepresentations;
    use crate::spec::view_version::_serde::ViewVersionV1;
    use crate::spec::view_version::ViewVersion;

    #[test]
    fn view_version() {
        let record = serde_json::json!(
        {
            "version-id" : 1,
            "timestamp-ms" : 1573518431292i64,
            "schema-id" : 1,
            "default-catalog" : "prod",
            "default-namespace" : [ "default" ],
            "summary" : {
              "engine-name" : "Spark",
              "engineVersion" : "3.3.2"
            },
            "representations" : [ {
              "type" : "sql",
              "sql" : "SELECT\n    COUNT(1), CAST(event_ts AS DATE)\nFROM events\nGROUP BY 2",
              "dialect" : "spark"
            } ]
          }
        );

        let result: ViewVersion = serde_json::from_value::<ViewVersionV1>(record.clone())
            .unwrap()
            .into();

        // Roundtrip
        assert_eq!(serde_json::to_value(result.clone()).unwrap(), record);

        assert_eq!(result.version_id(), 1);
        assert_eq!(
            result.timestamp().unwrap(),
            Utc.timestamp_millis_opt(1573518431292).unwrap()
        );
        assert_eq!(result.schema_id(), 1);
        assert_eq!(result.default_catalog, Some("prod".to_string()));
        assert_eq!(result.summary(), &{
            let mut map = std::collections::HashMap::new();
            map.insert("engine-name".to_string(), "Spark".to_string());
            map.insert("engineVersion".to_string(), "3.3.2".to_string());
            map
        });
        assert_eq!(
            result.representations().to_owned(),
            ViewRepresentations(vec![super::ViewRepresentation::Sql(
                super::SqlViewRepresentation {
                    sql: "SELECT\n    COUNT(1), CAST(event_ts AS DATE)\nFROM events\nGROUP BY 2"
                        .to_string(),
                    dialect: "spark".to_string(),
                },
            )])
        );
        assert_eq!(result.default_namespace.inner(), vec![
            "default".to_string()
        ]);
    }

    #[test]
    fn test_behaves_identical_to() {
        let view_version = ViewVersion::builder()
            .with_version_id(1)
            .with_schema_id(1)
            .with_timestamp_ms(1573518431292)
            .with_summary({
                let mut map = std::collections::HashMap::new();
                map.insert("engine-name".to_string(), "Spark".to_string());
                map.insert("engineVersion".to_string(), "3.3.2".to_string());
                map
            })
            .with_representations(ViewRepresentations(vec![super::ViewRepresentation::Sql(
                super::SqlViewRepresentation {
                    sql: "SELECT\n    COUNT(1), CAST(event_ts AS DATE)\nFROM events\nGROUP BY 2"
                        .to_string(),
                    dialect: "spark".to_string(),
                },
            )]))
            .with_default_catalog(Some("prod".to_string()))
            .with_default_namespace(NamespaceIdent::new("default".to_string()))
            .build();

        let mut identical_view_version = view_version.clone();
        identical_view_version.version_id = 2;
        identical_view_version.timestamp_ms = 1573518431293;

        let different_view_version = ViewVersion::builder()
            .with_version_id(view_version.version_id())
            .with_schema_id(view_version.schema_id())
            .with_timestamp_ms(view_version.timestamp_ms())
            .with_summary(view_version.summary().clone())
            .with_representations(ViewRepresentations(vec![super::ViewRepresentation::Sql(
                super::SqlViewRepresentation {
                    sql: "SELECT * from events".to_string(),
                    dialect: "spark".to_string(),
                },
            )]))
            .with_default_catalog(view_version.default_catalog().cloned())
            .with_default_namespace(view_version.default_namespace().clone())
            .build();

        assert!(view_version.behaves_identical_to(&identical_view_version));
        assert!(!view_version.behaves_identical_to(&different_view_version));
    }
}
