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
pub(crate) mod refresh_hook;
pub(crate) mod table_hook_scheduler;
pub(crate) mod vacuum_hook;

#[allow(clippy::module_inception)]
mod hook;

use std::sync::Arc;

use databend_common_exception::Result;
pub use hook::HookOperator;
use log::info;
use log::warn;
pub use table_hook_scheduler::TableHookScheduler;

use crate::sessions::QueryContext;
use crate::sessions::TableContextTableAccess;
use crate::sessions::TableContextTableManagement;

pub(crate) fn table_id_matches_target(
    hook_name: &str,
    expected_table_id: Option<u64>,
    actual_table_id: u64,
    catalog: &str,
    database: &str,
    table: &str,
) -> bool {
    let Some(expected_table_id) = expected_table_id else {
        return true;
    };

    if expected_table_id == actual_table_id {
        return true;
    }

    log::warn!(
        "Skip {} hook because table id mismatches target, expected_table_id={}, actual_table_id={}, catalog={}, database={}, table={}",
        hook_name,
        expected_table_id,
        actual_table_id,
        catalog,
        database,
        table,
    );
    false
}

pub(crate) async fn resolve_current_table_name_by_id(
    ctx: &Arc<QueryContext>,
    hook_name: &str,
    catalog_name: &str,
    database: &str,
    table: &str,
    expected_table_id: Option<u64>,
) -> Result<Option<(String, String)>> {
    let Some(table_id) = expected_table_id else {
        return Ok(Some((database.to_string(), table.to_string())));
    };

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
    ctx.evict_table_from_cache(catalog_name, database, current_table_name)?;
    ctx.clear_table_meta_timestamps_cache();

    match ctx
        .get_table(catalog_name, database, current_table_name)
        .await
    {
        Ok(table) => Ok(table.get_id() == expected_table_id),
        Err(_) => Ok(false),
    }
}
