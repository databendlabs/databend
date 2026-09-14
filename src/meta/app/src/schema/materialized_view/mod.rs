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
pub use mv_source_binding_version_ident::MVSourceBindingVersion;
pub use mv_source_binding_version_ident::MVSourceBindingVersionIdent;
pub use mv_source_binding_version_ident::MVSourceBindingVersionResource;
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
/// Hidden physical column used to match source DELETE/UPDATE rows during refresh.
pub const MATERIALIZED_VIEW_SOURCE_ROW_ID_COLUMN: &str = "_mv_source_row_id";

pub fn is_materialized_view_engine(engine: &str) -> bool {
    engine == MATERIALIZED_VIEW_ENGINE
}

pub fn invalidates_mv_source_bindings(old_meta: &TableMeta, new_meta: &TableMeta) -> bool {
    if old_meta.schema == new_meta.schema {
        return false;
    }

    // Match each old column by ID, then compare the complete TableField. A missing or changed
    // old field invalidates the binding; fields that exist only in the new schema (ADD COLUMN)
    // are intentionally ignored.
    old_meta.schema.fields().iter().any(|old_field| {
        new_meta
            .schema
            .fields()
            .iter()
            .find(|new_field| new_field.column_id == old_field.column_id)
            != Some(old_field)
    })
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
/// `definition` is persisted as [`MVDefinition`].
/// `expected_source_generation` binds that definition to the source metadata
/// observed while binding. `create_table` compares it with the current semantic
/// generation, then uses the freshly read version-key KV sequence as its
/// transaction condition. It is not persisted in the MV `TableMeta`.
/// A source `TableMeta` sequence is intentionally not carried here because
/// ordinary source-table writes advance it without invalidating the bound
/// schema. `create_table` reads the current source `TableMeta` itself to reject
/// a missing or dropped source.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CreateMaterializedViewMeta {
    pub definition: MVDefinition,
    /// Semantic source binding generation observed while binding.
    ///
    /// A missing version key is generation 0. MV-invalidating source DDL
    /// increments the stored generation, rejecting a CREATE bound before that
    /// DDL. The version key's KV sequence is intentionally kept inside the
    /// Meta API as a transaction CAS token.
    pub expected_source_generation: u64,
}

/// A point-in-time view of one materialized-view definition and its source generation.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MVDefinitionSnapshot {
    pub definition: Option<SeqV<MVDefinition>>,
    pub bound_source_generation: Option<u64>,
    pub current_source_generation: Option<u64>,
}

/// Complete metadata needed to use one materialized view.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MVInfo {
    pub mv_id: u64,
    pub definition: SeqV<MVDefinition>,
    pub table_meta: SeqV<TableMeta>,
}

/// A consistent view of the active MV bindings for one source table.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MVSourceBindingSnapshot {
    /// Binding generation at which `materialized_views` was collected.
    pub generation: u64,
    /// Empty when the generation changed while MV metadata was being collected.
    pub materialized_views: Vec<MVInfo>,
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
