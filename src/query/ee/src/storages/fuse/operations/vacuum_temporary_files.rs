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

use std::time::Duration;
use std::time::Instant;
use std::time::SystemTime;
use std::time::UNIX_EPOCH;

use databend_common_exception::Result;
use databend_common_meta_app::schema::TableInfo;
use databend_common_storage::DataOperator;
use databend_common_storages_fuse::FuseTable;
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
) -> Result<Option<Vec<(String, String)>>> {
    // storage_params is_some means it is an external table, ignore
    if table_info.meta.storage_params.is_some() {
        info!("ignore external table {}", table_info.name);
        return Ok(None);
    }

    let dir = format!("{}/", FuseTable::parse_storage_prefix(table_info)?);
    info!("vacuum drop table {:?} dir {:?}", table_info.name, dir);
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
                .await?;
            let mut list_files = Vec::new();
            while let Some(de) = ds.try_next().await? {
                let meta = de.metadata();
                if EntryMode::FILE == meta.mode() {
                    list_files.push((table_info.name.clone(), de.name().to_string()));
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
pub async fn do_vacuum_temporary_files(
    temporary_dir: String,
    limit: Option<usize>,
) -> Result<Vec<String>> {
    let operator = DataOperator::instance().operator();

    let mut ds = operator
        .lister_with(&temporary_dir)
        .recursive(true)
        .metakey(Metakey::LastModified)
        .await?;

    let limit = limit.unwrap_or(usize::MAX);
    let expire_time = Duration::from_secs(60 * 60 * 24 * 3).as_millis() as i64;
    let timestamp = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis() as i64;

    let mut remove_temp_files_name = Vec::new();
    while let Some(de) = ds.try_next().await? {
        let meta = de.metadata();

        if let Some(modified) = meta.last_modified() {
            if timestamp - modified.timestamp() >= expire_time {
                operator.delete(de.path()).await?;
                remove_temp_files_name.push(de.name().to_string());
            }

            if remove_temp_files_name.len() >= limit {
                break;
            }
        }
    }

    Ok(remove_temp_files_name)
}

pub fn query_spill_prefix(tenant: &str) -> String {
    format!("_query_spill/{}", tenant)
}
