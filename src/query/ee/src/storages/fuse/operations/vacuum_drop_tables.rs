// Copyright 2023 Databend Cloud
//
// Licensed under the Elastic License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.elastic.co/licensing/elastic-license
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::collections::HashSet;
use std::sync::Arc;
use std::time::Instant;

use databend_common_base::runtime::execute_futures_in_parallel;
use databend_common_catalog::catalog::Catalog;
use databend_common_exception::Result;
use databend_common_meta_app::schema::DBIdTableName;
use databend_common_meta_app::schema::DroppedId;
use databend_common_meta_app::schema::GcDroppedTableReq;
use databend_common_meta_app::schema::TableId;
use databend_common_meta_app::schema::TableInfo;
use databend_common_meta_app::tenant::Tenant;
use databend_common_storages_fuse::FuseTable;
use databend_enterprise_vacuum_handler::vacuum_handler::DroppedTableDesc;
use databend_enterprise_vacuum_handler::vacuum_handler::VacuumDropFileInfo;
use databend_enterprise_vacuum_handler::vacuum_handler::VacuumDropTablesResult;
use databend_enterprise_vacuum_handler::vacuum_handler::VacuumDroppedTablesCtx;
use futures_util::TryStreamExt;
use log::error;
use log::info;
use opendal::EntryMode;
use opendal::Operator;

#[async_backtrace::framed]
pub async fn do_vacuum_drop_table(
    catalog: Arc<dyn Catalog>,
    tenant: Tenant,
    tables: Vec<DroppedTableDesc>,
    dry_run_limit: Option<usize>,
) -> VacuumDropTablesResult {
    let is_dry_run = dry_run_limit.is_some();

    let mut list_files = vec![];
    let mut failed_tables = HashSet::new();
    for table_desc in tables {
        let table = &table_desc.table;
        let table_info = table.get_table_info();

        if !table.is_read_only() {
            if let Ok(fuse_table) = FuseTable::try_from_table(table.as_ref()) {
                let operator = fuse_table.get_operator();

                let result =
                    vacuum_drop_single_table(table_info, operator, dry_run_limit, &mut list_files)
                        .await;

                if result.is_err() {
                    let table_id = table_info.ident.table_id;
                    failed_tables.insert(table_id);
                    continue;
                }
            } else {
                info!(
                    "table {} is not of engine {}, it's data will NOT be cleaned.",
                    table.get_table_info().name,
                    table.engine()
                );
            };
        } else {
            info!(
                "table {} is read-only, it's data will NOT be cleaned.",
                table.get_table_info().name,
            );
        }

        if !is_dry_run {
            let req = GcDroppedTableReq {
                tenant: tenant.clone(),
                catalog: catalog.name(),
                drop_ids: vec![DroppedId::Table {
                    name: DBIdTableName {
                        db_id: table_desc.db_id,
                        table_name: table_desc.table_name.clone(),
                    },
                    id: TableId::new(table.get_table_info().ident.table_id),
                }],
            };

            let result = catalog.gc_drop_tables(req).await;

            if result.is_err() {
                let table_id = table_info.ident.table_id;
                failed_tables.insert(table_id);
                info!(
                    "failed to drop table table db_id: {}, table {}[{:}]",
                    table_desc.db_id, table_desc.table_name, table_id
                );
                continue;
            }
        }
    }
    Ok(if dry_run_limit.is_some() {
        (Some(list_files), failed_tables)
    } else {
        (None, failed_tables)
    })
}

// public for IT
pub async fn vacuum_drop_single_table(
    table_info: &TableInfo,
    operator: Operator,
    dry_run_limit: Option<usize>,
    list_files: &mut Vec<VacuumDropFileInfo>,
) -> Result<()> {
    let dir = format!(
        "{}/",
        FuseTable::parse_storage_prefix_from_table_info(table_info)?
    );

    info!(
        "vacuum drop table {:?} dir {:?}, is_external_table:{:?}",
        table_info.name,
        dir,
        table_info.meta.storage_params.is_some()
    );

    let start = Instant::now();

    match dry_run_limit {
        None => {
            let result = operator.remove_all(&dir).await;
            if let Err(ref err) = result {
                error!("failed to remove all in directory {}: {}", dir, err);
            }
            result?;
        }
        Some(dry_run_limit) => {
            let mut ds = operator.lister_with(&dir).recursive(true).await?;

            loop {
                let entry = ds.try_next().await;
                match entry {
                    Ok(Some(de)) => {
                        let meta = de.metadata();
                        if EntryMode::FILE == meta.mode() {
                            let mut content_length = meta.content_length();
                            if content_length == 0 {
                                content_length = operator.stat(de.path()).await?.content_length();
                            }

                            list_files.push((
                                table_info.name.clone(),
                                de.name().to_string(),
                                content_length,
                            ));
                            if list_files.len() >= dry_run_limit {
                                break;
                            }
                        }
                    }
                    Ok(None) => break,
                    Err(e) => {
                        if e.kind() == opendal::ErrorKind::NotFound {
                            info!("target not found, ignored. {}", e);
                            continue;
                        } else {
                            return Err(e.into());
                        }
                    }
                }
            }
        }
    };

    info!(
        "vacuum drop table {:?} dir {:?}, cost:{:?}",
        table_info.name,
        dir,
        start.elapsed()
    );
    Ok(())
}

#[async_backtrace::framed]
pub async fn vacuum_drop_tables_by_table_info(
    dropped_tables: VacuumDroppedTablesCtx,
) -> VacuumDropTablesResult {
    let VacuumDroppedTablesCtx {
        tenant,
        catalog,
        threads_nums,
        tables,
        dry_run_limit,
    } = dropped_tables;

    let start = Instant::now();
    let num_tables = tables.len();

    let num_threads = std::cmp::min(threads_nums, 3);
    let batch_size = (num_tables / num_threads).clamp(1, 50);

    info!(
        "vacuum dropped tables, number of tables: {}, batch_size: {}, parallelism degree: {}",
        num_tables, batch_size, num_threads
    );

    let result = if batch_size >= tables.len() {
        do_vacuum_drop_table(catalog, tenant, tables, dry_run_limit).await?
    } else {
        let mut chunks = tables.chunks(batch_size);
        let dry_run_limit = dry_run_limit
            .map(|dry_run_limit| (dry_run_limit / num_threads).min(dry_run_limit).max(1));
        let tasks = std::iter::from_fn(move || {
            chunks.next().map(|tables| {
                let catalog = catalog.clone();
                let tenant = tenant.clone();
                do_vacuum_drop_table(catalog, tenant, tables.to_vec(), dry_run_limit)
            })
        });

        let result = execute_futures_in_parallel(
            tasks,
            num_threads,
            num_threads * 2,
            "batch-vacuum-drop-tables-worker".to_owned(),
        )
        .await?;

        // Note that Errs should NOT be swallowed if any target is not successfully deleted.
        // Otherwise, the caller site may proceed to purge meta-data from meta-server with
        // some table data have not been cleaned completely, and the `vacuum` action of those
        // dropped tables can no longer be roll-forward.
        if dry_run_limit.is_some() {
            let mut ret_files = vec![];
            for res in result {
                if let Some(files) = res?.0 {
                    ret_files.extend(files);
                }
            }
            (Some(ret_files), HashSet::new())
        } else {
            let mut failed_tables = HashSet::new();
            for res in result {
                let (_, tbl) = res?;
                failed_tables.extend(tbl);
            }
            (None, failed_tables)
        }
    };

    let (_, failed_tables) = &result;
    let (success_count, failed_count) = (num_tables - failed_tables.len(), failed_tables.len());
    info!(
        "vacuum {} dropped tables completed - success: {}, failed: {}, total_cost: {:?}",
        num_tables,
        success_count,
        failed_count,
        start.elapsed()
    );

    Ok(result)
}

// TODO remove this func
#[async_backtrace::framed]
pub async fn vacuum_drop_tables(dropped_tables: VacuumDroppedTablesCtx) -> VacuumDropTablesResult {
    vacuum_drop_tables_by_table_info(dropped_tables).await
}
