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

use databend_common_catalog::table::TableExt;
use databend_common_catalog::table_context::TableContext;
use databend_common_exception::Result;
use databend_common_meta_app::schema::TableInfo;
use databend_enterprise_vacuum_handler::VacuumHandlerWrapper;
use log::info;
use log::warn;

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
        let table = FuseTable::create_without_refresh_table_info(
            table_info,
            ctx.get_settings().get_s3_storage_class()?,
        )?
        .refresh(ctx.as_ref())
        .await?;
        let fuse_table = FuseTable::try_from_table(table.as_ref())?;
        vacuum_table(fuse_table, ctx.clone(), &vacuum_handler, true).await;
    }

    Ok(())
}
