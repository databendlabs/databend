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

use common_base::base::uuid::Uuid;
use common_catalog::table::Table;
use common_exception::Result;
use common_storages_fuse::FuseTable;
use futures_util::TryStreamExt;
use opendal::EntryMode;
use opendal::Metakey;
use storages_common_cache::CacheAccessor;
use storages_common_cache_manager::CacheManager;
use storages_common_table_meta::meta::TableSnapshot;
use tracing::info;

#[async_backtrace::framed]
async fn do_vacuum_drop_table(
    table: Arc<dyn Table>,
    dry_run_limit: Option<usize>,
) -> Result<Option<Vec<(String, String)>>> {
    // storage_params is_some means it is an external table, ignore
    if table.get_table_info().meta.storage_params.is_some() {
        info!("ignore external table {}", table.get_table_info().name);
        return Ok(None);
    }
    let fuse_table = FuseTable::try_from_table(table.as_ref())?;

    let operator = fuse_table.get_operator_ref();

    let dir = format!("{}/", fuse_table.meta_location_generator().prefix(),);
    info!("vacuum drop table {:?} dir {:?}", table.name(), dir);
    let start = Instant::now();

    match dry_run_limit {
        None => {
            let _ = operator.remove_all(&dir).await;
            // write an empty snapshot as last snapshot
            if let Ok(Some(snapshot_loc)) = fuse_table.snapshot_loc().await {
                if let Ok(Some(prev)) = fuse_table.read_table_snapshot().await {
                    let uuid = Uuid::new_v4();
                    let current = TableSnapshot::new(
                        uuid,
                        &None,
                        prev.prev_snapshot_id,
                        prev.schema.clone(),
                        Default::default(),
                        vec![],
                        None,
                        None,
                    );

                    if let Ok(bytes) = current.to_bytes() {
                        let _ = operator.write(&snapshot_loc, bytes).await;
                    }

                    if let Some(snapshot_cache) =
                        CacheManager::instance().get_table_snapshot_cache()
                    {
                        snapshot_cache.evict(&snapshot_loc);
                    }
                }
            }
            info!(
                "vacuum drop table {:?} dir {:?}, cost:{} sec",
                table.name(),
                dir,
                start.elapsed().as_secs()
            );
            Ok(None)
        }
        Some(dry_run_limit) => {
            let mut ds = operator.list_with(&dir).delimiter("").await?;
            let mut list_files = Vec::new();
            while let Some(de) = ds.try_next().await? {
                let meta = operator.metadata(&de, Metakey::Mode).await?;
                if EntryMode::FILE == meta.mode() {
                    list_files.push((fuse_table.name().to_string(), de.name().to_string()));
                    if list_files.len() >= dry_run_limit {
                        break;
                    }
                }
            }

            info!(
                "vacuum table {:?} dir {:?}, cost:{} sec",
                table.name(),
                dir,
                start.elapsed().as_secs()
            );
            Ok(Some(list_files))
        }
    }
}

#[async_backtrace::framed]
pub async fn do_vacuum_drop_tables(
    tables: Vec<Arc<dyn Table>>,
    dry_run_limit: Option<usize>,
) -> Result<Option<Vec<(String, String)>>> {
    let start = Instant::now();
    let tables_len = tables.len();
    info!("do_vacuum_drop_tables {} tables", tables_len);
    let mut list_files = Vec::new();
    let mut left_limit = dry_run_limit;
    for table in tables {
        let ret = do_vacuum_drop_table(table, left_limit).await?;
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
