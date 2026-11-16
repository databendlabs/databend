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
use std::time::Instant;

use databend_common_base::runtime::execute_futures_in_parallel;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_meta_app::schema::TableInfo;
use databend_common_meta_app::schema::parse_clone_group_id;
use databend_common_storages_fuse::FuseTable;
use databend_enterprise_vacuum_handler::vacuum_handler::VacuumDropTablesResult;
use databend_storages_common_table_meta::table::is_fuse_backed_engine;
use log::error;
use log::info;
use opendal::Operator;

#[async_backtrace::framed]
pub async fn do_vacuum_drop_table(tables: Vec<(TableInfo, Operator)>) -> VacuumDropTablesResult {
    let mut failed_tables = HashSet::new();
    for (table_info, operator) in tables {
        let result = vacuum_drop_single_table(&table_info, operator).await;
        if result.is_err() {
            let table_id = table_info.ident.table_id;
            failed_tables.insert(table_id);
        }
    }
    Ok(failed_tables)
}

async fn vacuum_drop_single_table(table_info: &TableInfo, operator: Operator) -> Result<()> {
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

    operator.remove_all(&dir).await.inspect_err(|err| {
        error!("failed to remove all in directory {}: {}", dir, err);
    })?;

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
    num_threads: usize,
    table_infos: Vec<(TableInfo, Operator)>,
) -> VacuumDropTablesResult {
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

    let failed_tables = if batch_size >= table_infos.len() {
        do_vacuum_drop_table(table_infos).await?
    } else {
        let mut chunks = table_infos.chunks(batch_size);
        let tasks = std::iter::from_fn(move || {
            chunks
                .next()
                .map(|tables| do_vacuum_drop_table(tables.to_vec()))
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
        let mut failed_tables = HashSet::new();
        for res in result {
            failed_tables.extend(res?);
        }
        failed_tables
    };

    let (success_count, failed_count) = (num_tables - failed_tables.len(), failed_tables.len());
    info!(
        "vacuum {} dropped tables completed - success: {}, failed: {}, total_cost: {:?}",
        num_tables,
        success_count,
        failed_count,
        start.elapsed()
    );

    Ok(failed_tables)
}

/// Vacuum dropped table directories, allowing a clone-group member only when the caller has
/// verified that no existing group member directly depends on it.
///
/// Callers must compute `safe_clone_table_ids` from the current clone lineage. Passing an empty
/// set is safe but defers every clone-group member indefinitely, so it must be a deliberate
/// choice rather than a default.
#[async_backtrace::framed]
pub async fn vacuum_drop_tables(
    threads_nums: usize,
    tables: Vec<TableInfo>,
    safe_clone_table_ids: HashSet<u64>,
) -> VacuumDropTablesResult {
    let num_tables = tables.len();
    info!("vacuum_drop_tables {} tables", num_tables);

    let mut table_infos = Vec::with_capacity(num_tables);
    let mut failed_tables = HashSet::new();
    for table_info in tables {
        // Attached/shared tables do not own their physical data. Materialized
        // views and dynamic tables do, even though they reject user mutations.
        if table_info.is_shared()
            || (table_info.meta.storage_params.is_some()
                && FuseTable::is_table_attached(table_info.options()))
        {
            continue;
        }
        match parse_clone_group_id(&table_info.meta.options) {
            Err(error) => {
                error!(
                    "defer vacuum of table {} with invalid clone_group_id: {}",
                    table_info.ident.table_id, error
                );
                failed_tables.insert(table_info.ident.table_id);
                continue;
            }
            Ok(Some(_)) if !safe_clone_table_ids.contains(&table_info.ident.table_id) => {
                info!(
                    "defer vacuum of clone-group table {} until it has no existing clone child",
                    table_info.ident.table_id
                );
                failed_tables.insert(table_info.ident.table_id);
                continue;
            }
            Ok(_) => {}
        }
        let operator = if is_fuse_backed_engine(table_info.engine()) {
            FuseTable::create_storage_operator(&table_info, None)
        } else {
            Err(ErrorCode::UnknownTableEngine(format!(
                "Cannot vacuum physical data for table engine {}",
                table_info.engine()
            )))
        };
        match operator {
            Ok(operator) => table_infos.push((table_info, operator)),
            Err(err) => {
                error!(
                    "failed to initialize storage for dropped table {} (id:{}): {}",
                    table_info.desc, table_info.ident.table_id, err
                );
                failed_tables.insert(table_info.ident.table_id);
            }
        }
    }

    let failed = vacuum_drop_tables_by_table_info(threads_nums, table_infos).await?;
    failed_tables.extend(failed);
    Ok(failed_tables)
}
