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
    table_info: &TableInfo,
    operator: &Operator,
    dry_run_limit: Option<usize>,
) -> Result<Option<Vec<VacuumDropFileInfo>>> {
    let dir = format!("{}/", FuseTable::parse_storage_prefix(table_info)?);

    info!(
        "vacuum drop table {:?} dir {:?}, is_external_table:{:?}",
        table_info.name,
        dir,
        table_info.meta.storage_params.is_some()
    );

    let start = Instant::now();

    let ret = match dry_run_limit {
        None => {
            operator.remove_all(&dir).await?;
            Ok(None)
        }
        Some(dry_run_limit) => {
            let mut ds = operator
                .lister_with(&dir)
                .recursive(true)
                .metakey(Metakey::Mode)
                .metakey(Metakey::ContentLength)
                .await?;
            let mut list_files = Vec::new();
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

            Ok(Some(list_files))
        }
    };

    info!(
        "vacuum drop table {:?} dir {:?}, cost:{} sec",
        table_info.name,
        dir,
        start.elapsed().as_secs()
    );
    ret
}

#[async_backtrace::framed]
pub async fn do_vacuum_drop_tables(
    tables: Vec<Arc<dyn Table>>,
    dry_run_limit: Option<usize>,
) -> Result<Option<Vec<VacuumDropFileInfo>>> {
    let start = Instant::now();
    let tables_len = tables.len();
    info!("do_vacuum_drop_tables {} tables", tables_len);
    let mut list_files = Vec::new();
    let mut left_limit = dry_run_limit;
    for table in tables {
        // only operate fuse table
        let ret = if let Ok(fuse_table) = FuseTable::try_from_table(table.as_ref()) {
            let table_info = table.get_table_info();
            let operator = fuse_table.get_operator_ref();
            do_vacuum_drop_table(table_info, operator, left_limit).await?
        } else {
            info!(
                "ignore table {}, which is not of FUSE engine. Table engine {}",
                table.get_table_info().name,
                table.engine()
            );
            continue;
        };
        if let Some(ret) = ret {
            list_files.extend(ret);
            if list_files.len() >= dry_run_limit.unwrap() {
                info!(
                    "do_vacuum_drop_tables {} tables, cost:{} sec",
                    tables_len,
                    start.elapsed().as_secs()
                );
                return Ok(Some(list_files));
            } else {
                left_limit = Some(dry_run_limit.unwrap() - list_files.len());
            }
        }
    }
    info!(
        "do_vacuum_drop_tables {} tables, cost:{} sec",
        tables_len,
        start.elapsed().as_secs()
    );

    Ok(if dry_run_limit.is_some() {
        Some(list_files)
    } else {
        None
    })
}
