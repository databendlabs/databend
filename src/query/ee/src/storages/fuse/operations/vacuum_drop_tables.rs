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
                operator.remove_all(&dir).await?;
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
pub async fn do_vacuum_drop_tables(
    threads_nums: usize,
    tables: Vec<Arc<dyn Table>>,
    dry_run_limit: Option<usize>,
) -> Result<Option<Vec<VacuumDropFileInfo>>> {
    let start = Instant::now();
    let tables_len = tables.len();
    info!("do_vacuum_drop_tables {} tables", tables_len);

    let batch_size = (tables_len / threads_nums).min(50).max(1);

    let mut table_vecs = Vec::with_capacity(tables.len());
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

        table_vecs.push((table_info.clone(), operator));
    }

    let result = if batch_size >= table_vecs.len() {
        do_vacuum_drop_table(table_vecs, dry_run_limit).await?
    } else {
        let mut chunks = table_vecs.chunks(batch_size);
        let dry_run_limit = dry_run_limit
            .map(|dry_run_limit| (dry_run_limit / threads_nums).min(dry_run_limit).max(1));
        let tasks = std::iter::from_fn(move || {
            chunks
                .next()
                .map(|tables| do_vacuum_drop_table(tables.to_vec(), dry_run_limit))
        });

        let result = execute_futures_in_parallel(
            tasks,
            threads_nums,
            threads_nums * 2,
            "batch-vacuum-drop-tables-worker".to_owned(),
        )
        .await?;
        if dry_run_limit.is_some() {
            let mut ret_files = vec![];
            for file in result {
                // return error if any errors happens during `do_vacuum_drop_table`
                if let Some(files) = file? {
                    ret_files.extend(files);
                }
            }
            Some(ret_files)
        } else {
            None
        }
    };

    info!(
        "do_vacuum_drop_tables {} tables, cost:{:?}",
        tables_len,
        start.elapsed()
    );

    Ok(result)
}
