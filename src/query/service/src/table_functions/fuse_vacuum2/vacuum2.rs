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

use std::sync::Arc;

use databend_common_catalog::catalog::Catalog;
use databend_common_catalog::table::Table;
use databend_common_catalog::table::TableExt;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_storages_fuse::FuseTable;
use databend_enterprise_vacuum_handler::get_vacuum_handler;
use log::info;
use log::warn;

use crate::sessions::TableContext;

pub(crate) async fn vacuum_table(
    ctx: &Arc<dyn TableContext>,
    catalog: &dyn Catalog,
    database_name: &str,
    table_name: &str,
    respect_flash_back: bool,
) -> Result<Vec<String>> {
    let table = catalog
        .get_table(&ctx.get_tenant(), database_name, table_name)
        .await?;
    let table = FuseTable::try_from_table(table.as_ref()).map_err(|_| {
        ErrorCode::StorageOther("Invalid table engine, only fuse table is supported")
    })?;

    table.check_mutable()?;
    get_vacuum_handler()
        .do_vacuum2(table, ctx.clone(), respect_flash_back)
        .await
}

pub(crate) async fn vacuum_tables(
    ctx: &Arc<dyn TableContext>,
    catalog: &dyn Catalog,
    database_name: Option<&str>,
) -> Result<()> {
    if let Some(database_name) = database_name {
        vacuum_database(ctx, catalog, database_name).await?;
        return Ok(());
    }

    let tenant = ctx.get_tenant();
    let databases = catalog.list_databases(&tenant).await?;
    let num_databases = databases.len();

    for (index, database) in databases.iter().enumerate() {
        if database.engine().eq_ignore_ascii_case("SYSTEM") {
            info!("Bypass system database [{}]", database.name());
            continue;
        }

        info!(
            "Processing db {}, progress: {}/{}",
            database.name(),
            index + 1,
            num_databases
        );
        vacuum_database(ctx, catalog, database.name()).await?;
    }

    Ok(())
}

async fn vacuum_database(
    ctx: &Arc<dyn TableContext>,
    catalog: &dyn Catalog,
    database_name: &str,
) -> Result<()> {
    let tenant = ctx.get_tenant();
    let tables = catalog.list_tables(&tenant, database_name).await?;
    info!("Found {} tables in db {}", tables.len(), database_name);

    let num_tables = tables.len();
    let handler = get_vacuum_handler();
    for (index, table) in tables.iter().enumerate() {
        let table_name = &table.get_table_info().name;
        info!(
            "Processing table {}.{}, db level progress: {}/{}",
            database_name,
            table_name,
            index + 1,
            num_tables
        );

        let Ok(table) = FuseTable::try_from_table(table.as_ref()) else {
            info!("Bypass non-fuse table {}.{}", database_name, table_name);
            continue;
        };

        if table.is_read_only() {
            info!("Bypass read only table {}.{}", database_name, table_name);
            continue;
        }

        if let Err(error) = handler.do_vacuum2(table, ctx.clone(), false).await {
            warn!(
                "vacuum2 table {}.{} failed: {}",
                database_name, table_name, error
            );
        }
    }

    Ok(())
}
