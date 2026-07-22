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

//! Materialized view metadata.

use databend_common_expression::TableSchema;
use databend_meta_client::types::SeqV;

use super::TableMeta;
use crate::app_error::AppError;
use crate::app_error::InvalidMaterializedView;

mod mv_definition_ident;
mod source_table_mv_ids_ident;

pub use mv_definition_ident::MVDefinitionIdent;
pub use mv_definition_ident::MVDefinitionResource;
pub use source_table_mv_ids_ident::SourceTableMVIdsIdent;
pub use source_table_mv_ids_ident::SourceTableMVIdsResource;

pub const MATERIALIZED_VIEW_ENGINE: &str = "MATERIALIZED_VIEW";
/// Internal table option set when a hidden materialized view is created.
pub const OPT_KEY_MATERIALIZED_VIEW_SOURCE_TABLE_ID: &str = "materialized_view_source_table_id";

pub fn is_materialized_view_engine(engine: &str) -> bool {
    engine == MATERIALIZED_VIEW_ENGINE
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
}

/// Definition associated with a materialized-view table.
///
/// A materialized view reuses table metadata and storage for its materialized
/// data, and its table ID is also its materialized view ID. [`TableMeta`]
/// describes the physical storage, while this record stores the defining query
/// and externally visible logical schema under the same table ID.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MVDefinition {
    pub original_query: String,
    pub query: String,
    /// Logical schema of the user's defining query, used for the externally
    /// visible column types. `TableMeta::schema` stores the rewritten physical
    /// schema that Fuse uses to read and write the materialized data.
    pub schema: TableSchema,
}

/// Complete metadata needed to use one materialized view.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MVInfo {
    pub mv_id: u64,
    pub definition: SeqV<MVDefinition>,
    pub table_meta: SeqV<TableMeta>,
}

/// Materialized views associated with one source table.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SourceTableMVs {
    /// Sequence of the [`SourceTableMVIds`] KV, or 0 if it does not exist.
    pub source_table_mvs_index_seq: u64,
    pub mvs: Vec<MVInfo>,
}

/// Reverse index from a source table to its dependent materialized-view table IDs.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct SourceTableMVIds {
    mv_ids: Vec<u64>,
}

impl SourceTableMVIds {
    pub fn from_ids(mv_ids: Vec<u64>) -> Self {
        let mut source_mv_ids = Self::default();
        for mv_id in mv_ids {
            source_mv_ids.add(mv_id);
        }
        source_mv_ids
    }

    pub fn mv_ids(&self) -> &[u64] {
        &self.mv_ids
    }

    pub fn add(&mut self, mv_id: u64) {
        if self.mv_ids.contains(&mv_id) {
            return;
        }

        self.mv_ids.push(mv_id);
    }

    pub fn remove(&mut self, mv_id: u64) {
        self.mv_ids.retain(|id| *id != mv_id);
    }
}

#[cfg(test)]
mod tests {
    use super::OPT_KEY_MATERIALIZED_VIEW_SOURCE_TABLE_ID;
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
}
