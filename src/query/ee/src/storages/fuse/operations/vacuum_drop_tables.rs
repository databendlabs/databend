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

use std::sync::Arc;
use std::time::Instant;

use databend_common_base::runtime::execute_futures_in_parallel;
use databend_common_catalog::table::Table;
use databend_common_exception::Result;
use databend_common_meta_app::schema::TableInfo;
use databend_common_storages_fuse::FuseTable;
use databend_enterprise_vacuum_handler::vacuum_handler::VacuumDropFileInfo;
use futures_util::TryStreamExt;
use log::error;
use log::info;
use opendal::EntryMode;
use opendal::Metakey;
use opendal::Operator;

#[async_backtrace::framed]
pub async fn do_vacuum_drop_table(
    tables: Vec<(TableInfo, Operator)>,
    dry_run_limit: Option<usize>,
) -> Result<Option<Vec<VacuumDropFileInfo>>> {
    let mut list_files = vec![];
    for (table_info, operator) in tables {
        let dir = format!("{}/", FuseTable::parse_storage_prefix(&table_info)?);

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
                let mut ds = operator
                    .lister_with(&dir)
                    .recursive(true)
                    .metakey(Metakey::Mode)
                    .metakey(Metakey::ContentLength)
                    .await?;

                while let Some(de) = ds.try_next().await? {
                    let meta = de.metadata();
                    if EntryMode::FILE == meta.mode() {
                        list_files.push((
                            table_info.name.clone(),
                            de.name().to_string(),
                            meta.content_length(),
                        ));
                        if list_files.len() >= dry_run_limit {
                            break;
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
    }
    Ok(if dry_run_limit.is_some() {
        Some(list_files)
    } else {
        None
    })
}

#[async_backtrace::framed]
pub async fn vacuum_drop_tables_by_table_info(
    num_threads: usize,
    table_infos: Vec<(TableInfo, Operator)>,
    dry_run_limit: Option<usize>,
) -> Result<Option<Vec<VacuumDropFileInfo>>> {
    let start = Instant::now();
    let num_tables = table_infos.len();

    // - for each vacuum task, the tables passed to it will be processed sequentially
    // - while removing one table's data, at most 1000 objects will be deleted (in batch)
    // - let's assume that the rate limit is 3500 (individual) objects per second:
    //   A parallelism degree of up to 3 appears to be safe.
    let num_threads = std::cmp::min(num_threads, 3);

    let batch_size = (num_tables / num_threads).clamp(1, 50);

    info!(
        "vacuum dropped tables, number of tables: {}, batch_size: {}, parallelism degree: {}",
        num_tables, batch_size, num_threads
    );

    let result = if batch_size >= table_infos.len() {
        do_vacuum_drop_table(table_infos, dry_run_limit).await?
    } else {
        let mut chunks = table_infos.chunks(batch_size);
        let dry_run_limit = dry_run_limit
            .map(|dry_run_limit| (dry_run_limit / num_threads).min(dry_run_limit).max(1));
        let tasks = std::iter::from_fn(move || {
            chunks
                .next()
                .map(|tables| do_vacuum_drop_table(tables.to_vec(), dry_run_limit))
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
        // some table data un-vacuumed, and the `vacuum` action of those dropped tables can no
        // longer be roll-forward.
        if dry_run_limit.is_some() {
            let mut ret_files = vec![];
            for file in result {
                if let Some(files) = file? {
                    ret_files.extend(files);
                }
            }
            Some(ret_files)
        } else {
            for file in result {
                let _ = file?;
            }
            None
        }
    };

    info!(
        "vacuum {} dropped tables, cost:{:?}",
        num_tables,
        start.elapsed()
    );

    Ok(result)
}

#[async_backtrace::framed]
pub async fn vacuum_drop_tables(
    threads_nums: usize,
    tables: Vec<Arc<dyn Table>>,
    dry_run_limit: Option<usize>,
) -> Result<Option<Vec<VacuumDropFileInfo>>> {
    let num_tables = tables.len();
    info!("vacuum_drop_tables {} tables", num_tables);

    let mut table_infos = Vec::with_capacity(num_tables);
    for table in tables {
        let (table_info, operator) =
            if let Ok(fuse_table) = FuseTable::try_from_table(table.as_ref()) {
                (fuse_table.get_table_info(), fuse_table.get_operator())
            } else {
                info!(
                    "ignore table {}, which is not of FUSE engine. Table engine {}",
                    table.get_table_info().name,
                    table.engine()
                );
                continue;
            };

        table_infos.push((table_info.clone(), operator));
    }

    vacuum_drop_tables_by_table_info(threads_nums, table_infos, dry_run_limit).await
}
