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

// src/query/storages/fuse/src/vacuum/mod.rs

use std::sync::Arc;

use databend_common_catalog::table::{Table, TableExt};
use databend_common_catalog::table_context::TableContext;
use databend_common_exception::Result;
use databend_common_meta_app::schema::TableInfo;
use databend_enterprise_vacuum_handler::VacuumHandlerWrapper;
use log::info;
use log::warn;
use databend_common_catalog::catalog::Catalog;
use crate::FuseTable;

pub async fn vacuum_table(
    fuse_table: &FuseTable,
    ctx: Arc<dyn TableContext>,
    vacuum_handler: &VacuumHandlerWrapper,
    respect_flash_back: bool,
) {
    warn!(
        "[VACUUM] Vacuuming table: {}, ident: {}",
        fuse_table.table_info.name, fuse_table.table_info.ident
    );

    if let Err(e) = vacuum_handler
        .do_vacuum2(fuse_table, ctx, respect_flash_back)
        .await
    {
        // Vacuum in a best-effort manner, errors are ignored
        warn!(
            "[VACUUM] Vacuum table {} failed : {}",
            fuse_table.table_info.name, e
        );
    } else {
        info!("[VACUUM] Vacuum table {} done", fuse_table.table_info.name);
    }
}

pub async fn vacuum_tables_from_info(
    table_infos: Vec<TableInfo>,
    ctx: Arc<dyn TableContext>,
    vacuum_handler: Arc<VacuumHandlerWrapper>,

) -> Result<()> {
    for table_info in table_infos {
        let table = FuseTable::do_create(table_info)?
            .refresh(ctx.as_ref())
            .await?;
        let fuse_table = FuseTable::try_from_table(table.as_ref())?;
        vacuum_table(fuse_table, ctx.clone(), &vacuum_handler, true).await;
    }

    Ok(())
}

pub async fn vacuum_all_tables(
    ctx: &Arc<dyn TableContext>,
    handler: & VacuumHandlerWrapper,
    catalog: &dyn Catalog,
) -> Result<Vec<String>> {
    let tenant_id = ctx.get_tenant();
    let dbs = catalog.list_databases(&tenant_id).await?;
    let num_db = dbs.len();

    for (idx_db, db) in dbs.iter().enumerate() {
        if db.engine().to_uppercase() == "SYSTEM" {
            info!("Bypass system database [{}]", db.name());
            continue;
        }

        info!(
                "Processing db {}, progress: {}/{}",
                db.name(),
                idx_db + 1,
                num_db
            );
        let tables = catalog.list_tables(&tenant_id, db.name()).await?;
        info!("Found {} tables in db {}", tables.len(), db.name());

        let num_tbl = tables.len();
        for (idx_tbl, table) in tables.iter().enumerate() {
            info!(
                    "Processing table {}.{}, db level progress: {}/{}",
                    db.name(),
                    table.get_table_info().name,
                    idx_tbl + 1,
                    num_tbl
                );

            let Ok(tbl) = FuseTable::try_from_table(table.as_ref()) else {
                info!(
                        "Bypass non-fuse table {}.{}",
                        db.name(),
                        table.get_table_info().name
                    );
                continue;
            };

            if tbl.is_read_only() {
                info!(
                        "Bypass read only table {}.{}",
                        db.name(),
                        table.get_table_info().name
                    );
                continue;
            }

            let res = handler.do_vacuum2(tbl, ctx.clone(), false).await;

            if let Err(e) = res {
                warn!(
                        "vacuum2 table {}.{} failed: {}",
                        db.name(),
                        table.get_table_info().name,
                        e
                    );
            };
        }
    }

    Ok(vec![])
}
