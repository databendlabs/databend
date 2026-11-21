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

pub(crate) mod analyze_hook;
pub(crate) mod compact_hook;
pub(crate) mod recluster_hook;
pub(crate) mod refresh_hook;
mod table_hook_task;
pub(crate) mod vacuum_hook;

#[allow(clippy::module_inception)]
mod hook;

use std::sync::Arc;

use databend_common_catalog::catalog::CATALOG_DEFAULT;
use databend_common_catalog::table::Table;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_meta_app::principal::SENSITIVE_SYSTEM_RESOURCE;
pub use hook::HookOperator;
pub(crate) use hook::execute_deferred_table_hooks;
use log::info;
use log::warn;

use crate::interpreters::common::check_expected_table_id;
use crate::sessions::QueryContext;
use crate::sessions::TableContextTableAccess;
use crate::sessions::TableContextTableManagement;
use crate::table_maintenance::scheduler::TaskKind;

#[derive(Clone)]
pub(crate) struct TableHookTarget {
    pub catalog: String,
    pub database: String,
    pub table: String,
    pub table_id: u64,
}

impl TableHookTarget {
    /// Each operation resolves by identity afresh, following renames but not replacements.
    pub(crate) async fn resolve(
        &self,
        ctx: &Arc<QueryContext>,
        hook_name: &str,
    ) -> Result<Option<Self>> {
        let Some((database, table)) = resolve_current_table_name_by_id(
            ctx,
            hook_name,
            &self.catalog,
            &self.database,
            &self.table,
            self.table_id,
        )
        .await?
        else {
            return Ok(None);
        };
        Ok(Some(Self {
            catalog: self.catalog.clone(),
            database,
            table,
            table_id: self.table_id,
        }))
    }

    fn is_system_history(&self) -> bool {
        self.catalog == CATALOG_DEFAULT && self.database == SENSITIVE_SYSTEM_RESOURCE
    }

    /// A queued notification can outlive a rename, including a move into system_history.
    async fn resolve_background(
        &self,
        ctx: &Arc<QueryContext>,
        kind: TaskKind,
    ) -> Result<Option<(Self, Arc<dyn Table>)>> {
        let Some(desc) = self.resolve(ctx, kind.name()).await? else {
            return Ok(None);
        };
        let table = ctx
            .get_table(&desc.catalog, &desc.database, &desc.table)
            .await?;
        if !desc.matches_id(kind.name(), table.get_id())
            || !hook::HookMode::for_table(&desc, table.as_ref()).queues(kind)
        {
            return Ok(None);
        }
        Ok(Some((desc, table)))
    }

    fn matches_id(&self, hook_name: &str, actual_table_id: u64) -> bool {
        if self.table_id == actual_table_id {
            return true;
        }
        warn!(
            "Skip {hook_name} hook because table id mismatches target, expected_table_id={}, actual_table_id={actual_table_id}, catalog={}, database={}, table={}",
            self.table_id, self.catalog, self.database, self.table,
        );
        false
    }
}

async fn resolve_current_table_name_by_id(
    ctx: &Arc<QueryContext>,
    hook_name: &str,
    catalog_name: &str,
    database: &str,
    table: &str,
    table_id: u64,
) -> Result<Option<(String, String)>> {
    // Most hooks still target the original name. Refresh and verify it first so the
    // common path needs no ID-to-name lookup and leaves a current table in the cache.
    if table_id_exists_in_database(ctx, catalog_name, database, table, table, table_id).await? {
        return Ok(Some((database.to_string(), table.to_string())));
    }

    let catalog = ctx.get_catalog(catalog_name).await?;
    let Some(current_table_name) = catalog.get_table_name_by_id(table_id).await? else {
        warn!(
            "Skip {} hook because table id cannot be resolved, table_id={}, catalog={}, database={}, table={}",
            hook_name, table_id, catalog_name, database, table,
        );
        return Ok(None);
    };

    if let Some(database_name) = resolve_table_database(
        ctx,
        catalog_name,
        database,
        table,
        &current_table_name,
        table_id,
    )
    .await?
    {
        if database_name != database || current_table_name != table {
            info!(
                "Resolved {} hook target by table id, table_id={}, old={}.{}.{}, current={}.{}.{}",
                hook_name,
                table_id,
                catalog_name,
                database,
                table,
                catalog_name,
                database_name,
                current_table_name,
            );
        }
        return Ok(Some((database_name, current_table_name)));
    }

    warn!(
        "Skip {} hook because table id cannot be verified by current name, table_id={}, catalog={}, database={}, table={}, current_table={}",
        hook_name, table_id, catalog_name, database, table, current_table_name,
    );
    Ok(None)
}

async fn resolve_table_database(
    ctx: &Arc<QueryContext>,
    catalog_name: &str,
    original_database: &str,
    original_table: &str,
    current_table_name: &str,
    table_id: u64,
) -> Result<Option<String>> {
    if table_id_exists_in_database(
        ctx,
        catalog_name,
        original_database,
        original_table,
        current_table_name,
        table_id,
    )
    .await?
    {
        return Ok(Some(original_database.to_string()));
    }

    let catalog = ctx.get_catalog(catalog_name).await?;
    for database in catalog.list_databases(&ctx.get_tenant()).await? {
        let database_name = database.name().to_string();
        if database_name == original_database {
            continue;
        }

        if table_id_exists_in_database(
            ctx,
            catalog_name,
            &database_name,
            original_table,
            current_table_name,
            table_id,
        )
        .await?
        {
            return Ok(Some(database_name));
        }
    }

    Ok(None)
}

async fn table_id_exists_in_database(
    ctx: &Arc<QueryContext>,
    catalog_name: &str,
    database: &str,
    original_table: &str,
    current_table_name: &str,
    expected_table_id: u64,
) -> Result<bool> {
    ctx.evict_table_from_cache(catalog_name, database, original_table)?;
    if current_table_name != original_table {
        ctx.evict_table_from_cache(catalog_name, database, current_table_name)?;
    }
    ctx.clear_table_meta_timestamps_cache();

    match ctx
        .get_table(catalog_name, database, current_table_name)
        .await
    {
        // A mismatch is a failed name candidate, not a resolver error.
        Ok(table) => Ok(check_expected_table_id(table.get_id(), Some(expected_table_id)).is_ok()),
        Err(error)
            if matches!(
                error.code(),
                ErrorCode::UNKNOWN_TABLE | ErrorCode::UNKNOWN_DATABASE
            ) =>
        {
            Ok(false)
        }
        Err(error) => Err(error),
    }
}
