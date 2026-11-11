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

use std::collections::HashMap;

use databend_common_catalog::table::Table;
use databend_common_catalog::table::TableExt;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::TableSchema;
use databend_common_license::license::Feature;
use databend_common_license::license_manager::LicenseManagerSwitch;
use databend_common_meta_api::MaterializedViewApi;
use databend_common_meta_api::TableApi;
use databend_common_meta_app::schema::MVSourceColumn;
use databend_common_meta_app::schema::OPT_KEY_MATERIALIZED_VIEW_INVALID_REASON;
use databend_common_meta_app::schema::OPT_KEY_MATERIALIZED_VIEW_SOURCE_COLUMNS;
use databend_common_meta_app::schema::TableMeta;
use databend_common_meta_app::schema::UpsertTableOptionReq;
use databend_common_meta_app::schema::is_materialized_view_engine;
use databend_common_sql::plans::MaintenanceTarget;
use databend_common_users::UserApiProvider;
use databend_meta_client::types::MatchSeq;
use databend_storages_common_table_meta::table::MATERIALIZED_VIEW_SOURCE_SCHEMA_CHANGED_REASON;
use databend_storages_common_table_meta::table::MATERIALIZED_VIEW_SOURCE_SWAPPED_REASON;
use log::info;
use log::warn;

use crate::meta_service_error;
use crate::sessions::QueryContext;
use crate::sessions::TableContextLicense;
use crate::sessions::TableContextTableAccess;

pub fn check_materialized_view_license(ctx: &QueryContext, engine: &str) -> Result<()> {
    if is_materialized_view_engine(engine) {
        LicenseManagerSwitch::instance()
            .check_enterprise_enabled(ctx.get_license_key(), Feature::MaterializedView)?;
    }
    Ok(())
}

/// Reject attaching a row access or masking policy to a materialized-view source.
///
/// MV storage is computed without reader policies, so a source with any dependent MV, including
/// an invalid one, cannot get a policy. Drop the MVs first.
pub async fn check_policy_target_without_materialized_view(
    ctx: &QueryContext,
    table: &dyn Table,
) -> Result<()> {
    if !UserApiProvider::instance()
        .get_meta_store_client()
        .list_mvs_by_source_table_id(&ctx.get_tenant(), table.get_id())
        .await
        .map_err(meta_service_error)?
        .is_empty()
    {
        return Err(ErrorCode::AlterTableError(format!(
            "Cannot attach a row access or masking policy to table '{}' because one or more materialized views depend on it; drop the materialized views first",
            table.name()
        )));
    }
    Ok(())
}

/// What a source DDL did to a table, as the interpreter that ran it knows it. The invalid reasons
/// of the materialized views of the table follow from it; both are final.
///
/// A drop, undrop, or rename of the table or of its database, and CREATE OR REPLACE, which drops
/// the table it replaces, are not listed: they leave the MVs alone. Readers bind the definition
/// by name, so the MVs are rejected while the source is dropped or elsewhere, and usable again
/// once it is back.
pub enum MVSourceChange<'a> {
    /// ALTER TABLE ... SWAP WITH exchanged the table with another. Final.
    Swapped,
    /// A column change from `previous_schema` to `schema`. Final when it reaches a column an MV
    /// reads.
    SchemaChanged {
        previous_schema: &'a TableSchema,
        schema: &'a TableSchema,
    },
}

/// The options to change on the MV with `table_meta` after `change`: the invalid reason, and,
/// for an MV of an older version that records no source columns, all the columns of the schema
/// before a column change. Every reason is final: an MV that has one is left alone.
fn mv_option_changes(
    mv_id: u64,
    table_meta: &TableMeta,
    change: &MVSourceChange,
) -> HashMap<String, Option<String>> {
    let mut changes = HashMap::new();
    if !table_meta.is_valid_materialized_view() {
        return changes;
    }
    let reason = match change {
        MVSourceChange::Swapped => Some(MATERIALIZED_VIEW_SOURCE_SWAPPED_REASON),
        MVSourceChange::SchemaChanged {
            previous_schema,
            schema,
        } => {
            let changed = match table_meta.materialized_view_source_columns() {
                Ok(Some(columns)) => MVSourceColumn::changed(&columns, schema),
                Ok(None) => {
                    let columns = MVSourceColumn::from_schema(previous_schema);
                    changes.insert(
                        OPT_KEY_MATERIALIZED_VIEW_SOURCE_COLUMNS.to_string(),
                        Some(MVSourceColumn::encode(&columns)),
                    );
                    MVSourceColumn::changed(&columns, schema)
                }
                Err(error) => {
                    warn!(
                        "materialized view {} has unreadable source columns, marked invalid: {}",
                        mv_id, error
                    );
                    true
                }
            };
            changed.then_some(MATERIALIZED_VIEW_SOURCE_SCHEMA_CHANGED_REASON)
        }
    };
    if let Some(reason) = reason {
        changes.insert(
            OPT_KEY_MATERIALIZED_VIEW_INVALID_REASON.to_string(),
            Some(reason.to_string()),
        );
    }
    changes
}

/// Best effort, after a DDL of table `source_table_id` that `change` describes: set the invalid
/// reason of its materialized views, and record the source columns of those created by
/// an older version from the schema before a column change. No MV of an older version was
/// invalidated by a schema change of its source, so those are the columns it was created
/// against, or extend them by ADD COLUMN.
///
/// The reason is not written in the DDL's transaction, so that the DDL does not grow with the
/// MVs of its table. Each MV is updated through the table option API, which changes only these
/// options on its latest `TableMeta`, so a concurrent REFRESH commit is kept. A failure is
/// logged and does not fail the DDL: MV readers and REFRESH check the source identity and
/// schema themselves.
pub async fn update_materialized_view_validity(
    ctx: &QueryContext,
    source_table_id: u64,
    change: MVSourceChange<'_>,
) {
    let meta = UserApiProvider::instance().get_meta_store_client();
    let mvs = match meta
        .list_mvs_by_source_table_id(&ctx.get_tenant(), source_table_id)
        .await
    {
        Ok(mvs) => mvs,
        Err(error) => {
            warn!(
                "failed to list the materialized views of source table {}, their invalid reason may be stale: {}",
                source_table_id, error
            );
            return;
        }
    };
    for mv in mvs {
        let options = mv_option_changes(mv.mv_id, &mv.table_meta.data, &change);
        if options.is_empty() {
            continue;
        }
        info!(
            "update materialized view {} after DDL of source table {}: {:?}",
            mv.mv_id, source_table_id, options
        );
        if let Err(error) = meta
            .upsert_table_option(UpsertTableOptionReq {
                table_id: mv.mv_id,
                seq: MatchSeq::Any,
                options,
            })
            .await
        {
            warn!(
                "failed to update materialized view {} after DDL of source table {}, its invalid reason may be stale: {}",
                mv.mv_id, source_table_id, error
            );
        }
    }
}

pub fn check_maintenance_target(table: &dyn Table, target: &MaintenanceTarget) -> Result<()> {
    match target {
        MaintenanceTarget::Table => table.check_mutable(),
        MaintenanceTarget::MaterializedView { table_id } => {
            if table.get_id() != *table_id || !is_materialized_view_engine(table.engine()) {
                return Err(ErrorCode::InvalidOperation(format!(
                    "Materialized view maintenance target changed: expected table id {}, found '{}'(id {}, engine {})",
                    table_id,
                    table.name(),
                    table.get_id(),
                    table.engine()
                )));
            }
            Ok(())
        }
    }
}
