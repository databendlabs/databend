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

use std::collections::BTreeSet;

use databend_common_expression::TableField;
use databend_common_expression::TableSchema;
use databend_meta_client::types::SeqV;

use super::TableMeta;
use crate::app_error::AppError;
use crate::app_error::InvalidMaterializedView;

mod mv_definition_ident;
mod mv_source_binding_version_ident;
mod source_table_mv_ident;

pub use mv_definition_ident::MVDefinitionIdent;
pub use mv_definition_ident::MVDefinitionResource;
pub use mv_source_binding_version_ident::LegacyMVSourceBindingVersionIdent;
pub use mv_source_binding_version_ident::LegacyMVSourceBindingVersionResource;
pub use source_table_mv_ident::MVSourceBinding;
pub use source_table_mv_ident::SourceTableMV;
pub use source_table_mv_ident::SourceTableMVIdent;
pub use source_table_mv_ident::SourceTableMVResource;

pub const MATERIALIZED_VIEW_ENGINE: &str = "MATERIALIZED_VIEW";
/// Internal table option containing the source table ID of a materialized view.
pub const OPT_KEY_MATERIALIZED_VIEW_SOURCE_TABLE_ID: &str = "materialized_view_source_table_id";
/// Internal table option containing the source table sequence captured when the MV was created
/// or last refreshed.
pub const OPT_KEY_MATERIALIZED_VIEW_SOURCE_TABLE_SEQ: &str = "materialized_view_source_table_seq";
/// Internal table option holding why the MV is invalid; absent while it is valid. Never cleared.
pub const OPT_KEY_MATERIALIZED_VIEW_INVALID_REASON: &str = "materialized_view_invalid_reason";
/// Internal table option holding the source columns the MV reads, as JSON.
pub const OPT_KEY_MATERIALIZED_VIEW_SOURCE_COLUMNS: &str = "materialized_view_source_columns";
/// Hidden physical column used to match source DELETE/UPDATE rows during refresh.
pub const MATERIALIZED_VIEW_SOURCE_ROW_ID_COLUMN: &str = "_mv_source_row_id";

pub fn is_materialized_view_engine(engine: &str) -> bool {
    engine == MATERIALIZED_VIEW_ENGINE
}

/// A source column the MV reads, as recorded at CREATE.
#[derive(Clone, Debug, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct MVSourceColumn {
    pub id: u32,
    pub name: String,
    #[serde(rename = "type")]
    pub data_type: String,
}

impl MVSourceColumn {
    pub fn from_field(field: &TableField) -> Self {
        MVSourceColumn {
            id: field.column_id,
            name: field.name.clone(),
            data_type: field.data_type.sql_name_explicit_null(),
        }
    }

    /// All columns of `schema`, for an MV of an older version.
    pub fn from_schema(schema: &TableSchema) -> Vec<Self> {
        schema.fields().iter().map(Self::from_field).collect()
    }

    pub fn encode(columns: &[Self]) -> String {
        serde_json::to_string(columns).expect("serializing MV source columns cannot fail")
    }

    /// Whether a recorded column is missing or changed in `schema`.
    pub fn changed(recorded: &[Self], schema: &TableSchema) -> bool {
        recorded.iter().any(|column| {
            schema
                .fields()
                .iter()
                .find(|field| field.column_id == column.id)
                .is_none_or(|field| &Self::from_field(field) != column)
        })
    }
}

impl TableMeta {
    /// Return the source table ID required by a materialized view.
    pub fn materialized_view_source_table_id(&self) -> Result<u64, AppError> {
        let source_table_id = self
            .options
            .get(OPT_KEY_MATERIALIZED_VIEW_SOURCE_TABLE_ID)
            .ok_or_else(|| {
                AppError::InvalidMaterializedView(InvalidMaterializedView::new(format!(
                    "missing required table option {OPT_KEY_MATERIALIZED_VIEW_SOURCE_TABLE_ID}"
                )))
            })?;

        source_table_id.parse::<u64>().map_err(|_| {
            AppError::InvalidMaterializedView(InvalidMaterializedView::new(format!(
                "invalid table option {OPT_KEY_MATERIALIZED_VIEW_SOURCE_TABLE_ID}: '{source_table_id}'"
            )))
        })
    }

    /// `None` for an MV of an older version.
    pub fn materialized_view_source_columns(
        &self,
    ) -> Result<Option<Vec<MVSourceColumn>>, AppError> {
        let Some(columns) = self.options.get(OPT_KEY_MATERIALIZED_VIEW_SOURCE_COLUMNS) else {
            return Ok(None);
        };
        serde_json::from_str(columns).map(Some).map_err(|error| {
            AppError::InvalidMaterializedView(InvalidMaterializedView::new(format!(
                "invalid table option {OPT_KEY_MATERIALIZED_VIEW_SOURCE_COLUMNS}: {error}"
            )))
        })
    }

    /// `false` when the MV records no source columns.
    pub fn is_materialized_view_source_schema_changed(
        &self,
        schema: &TableSchema,
    ) -> Result<bool, AppError> {
        Ok(self
            .materialized_view_source_columns()?
            .is_some_and(|recorded| MVSourceColumn::changed(&recorded, schema)))
    }

    /// Return the source table sequence recorded by a materialized view.
    pub fn materialized_view_source_table_seq(&self) -> Result<u64, AppError> {
        let source_table_seq = self
            .options
            .get(OPT_KEY_MATERIALIZED_VIEW_SOURCE_TABLE_SEQ)
            .ok_or_else(|| {
                AppError::InvalidMaterializedView(InvalidMaterializedView::new(format!(
                    "missing required table option {OPT_KEY_MATERIALIZED_VIEW_SOURCE_TABLE_SEQ}"
                )))
            })?;

        source_table_seq.parse::<u64>().map_err(|_| {
            AppError::InvalidMaterializedView(InvalidMaterializedView::new(format!(
                "invalid table option {OPT_KEY_MATERIALIZED_VIEW_SOURCE_TABLE_SEQ}: '{source_table_seq}'"
            )))
        })
    }

    /// Why the MV is invalid; `None` while it is valid.
    pub fn materialized_view_invalid_reason(&self) -> Option<&str> {
        self.options
            .get(OPT_KEY_MATERIALIZED_VIEW_INVALID_REASON)
            .map(String::as_str)
    }

    pub fn is_valid_materialized_view(&self) -> bool {
        self.materialized_view_invalid_reason().is_none()
    }
}

/// Definition associated with a materialized-view table.
///
/// A materialized view reuses table metadata and storage for its materialized
/// data, and its table ID is also its materialized view ID. [`TableMeta`]
/// describes how the data is stored, while this record stores the defining
/// query and the columns returned to users under the same table ID.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MVDefinition {
    pub original_query: String,
    pub query: String,
    pub logical_schema: TableSchema,
    pub sync_creation: bool,
}

/// Materialized-view metadata supplied only while creating a table.
///
/// `definition` is persisted as [`MVDefinition`]. `create_table` reads the
/// current source `TableMeta` itself to reject a missing or dropped source.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CreateMaterializedViewMeta {
    pub definition: MVDefinition,
}

/// Complete metadata needed to use one materialized view.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MVInfo {
    pub mv_id: u64,
    pub definition: SeqV<MVDefinition>,
    pub table_meta: SeqV<TableMeta>,
}

/// Safe equality constraints accepted by the materialized-view listing API.
///
/// `None` means that the caller did not extract a necessary constraint for that field. The API
/// applies these constraints only to prune candidates; SQL callers must still evaluate their
/// complete filter expression.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct MaterializedViewListFilter {
    pub materialized_view_ids: Option<BTreeSet<u64>>,
    pub database_names: Option<BTreeSet<String>>,
    pub names: Option<BTreeSet<String>>,
    pub source_table_ids: Option<BTreeSet<u64>>,
}

/// Source metadata collected for one listed materialized view.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ListedMVSource {
    pub table_id: u64,
    /// Current database identity resolved from the source table's ID-to-name mapping.
    pub database_id: Option<u64>,
    pub database_name: Option<String>,
    pub table_name: Option<String>,
    /// Whether source `TableMeta` exists and is not soft-dropped, and its database is not dropped.
    pub is_active: bool,
    /// Whether the source's current database is dropped. `is_active` is false in that case too;
    /// this flag only tells management paths which of the two was dropped.
    pub database_dropped: bool,
}

/// Complete metadata needed by management and system-table listing paths.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ListedMaterializedView {
    pub mv: MVInfo,
    pub database_id: u64,
    pub database_name: String,
    pub name: String,
    pub source: ListedMVSource,
}

#[cfg(test)]
mod tests {
    use super::OPT_KEY_MATERIALIZED_VIEW_SOURCE_TABLE_ID;
    use super::OPT_KEY_MATERIALIZED_VIEW_SOURCE_TABLE_SEQ;
    use crate::schema::TableMeta;

    #[test]
    fn test_materialized_view_source_table_id() {
        let mut table_meta = TableMeta::default();

        assert!(table_meta.materialized_view_source_table_id().is_err());

        table_meta.options.insert(
            OPT_KEY_MATERIALIZED_VIEW_SOURCE_TABLE_ID.to_string(),
            "invalid".to_string(),
        );
        assert!(table_meta.materialized_view_source_table_id().is_err());

        table_meta.options.insert(
            OPT_KEY_MATERIALIZED_VIEW_SOURCE_TABLE_ID.to_string(),
            "42".to_string(),
        );
        assert_eq!(table_meta.materialized_view_source_table_id().unwrap(), 42);
    }

    #[test]
    fn test_materialized_view_source_table_seq() {
        let mut table_meta = TableMeta::default();

        assert!(table_meta.materialized_view_source_table_seq().is_err());

        table_meta.options.insert(
            OPT_KEY_MATERIALIZED_VIEW_SOURCE_TABLE_SEQ.to_string(),
            "invalid".to_string(),
        );
        assert!(table_meta.materialized_view_source_table_seq().is_err());

        table_meta.options.insert(
            OPT_KEY_MATERIALIZED_VIEW_SOURCE_TABLE_SEQ.to_string(),
            "42".to_string(),
        );
        assert_eq!(table_meta.materialized_view_source_table_seq().unwrap(), 42);
    }
}
