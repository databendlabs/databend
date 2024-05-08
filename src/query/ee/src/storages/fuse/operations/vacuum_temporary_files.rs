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
use std::time::Duration;
use std::time::SystemTime;
use std::time::UNIX_EPOCH;

use databend_common_catalog::table_context::TableContext;
use databend_common_exception::Result;
use databend_common_storage::DataOperator;
use databend_storages_common_io::Files;
use futures_util::TryStreamExt;
use log::info;
use opendal::Metakey;

const BATCH_SIZE: usize = 10_000;

#[async_backtrace::framed]
pub async fn do_vacuum_temporary_files(
    ctx: Arc<dyn TableContext>,
    temporary_dir: String,
    retain: Option<Duration>,
    limit: Option<usize>,
) -> Result<Vec<String>> {
    let operator = DataOperator::instance().operator();
    let files = Files::create(ctx, operator.clone());

    let mut ds = operator
        .lister_with(&temporary_dir)
        .recursive(true)
        .metakey(Metakey::LastModified)
        .await?;

    let limit = limit.unwrap_or(usize::MAX);
    let expire_time = retain.map(|x| x.as_millis()).unwrap_or(60 * 60 * 24 * 3) as i64;
    let timestamp = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis() as i64;

    let mut remove_temp_files_name = Vec::new();
    let mut expired_files = Vec::with_capacity(BATCH_SIZE);
    while let Some(de) = ds.try_next().await? {
        let meta = de.metadata();

        if let Some(modified) = meta.last_modified() {
            if timestamp - modified.timestamp_millis() >= expire_time {
                expired_files.push(de.path().to_string());
                remove_temp_files_name.push(de.name().to_string());
            }

            // If the batch size is reached, remove the files in batch.
            if expired_files.len() >= BATCH_SIZE {
                info!("Removing {} expired files in batch", expired_files.len());
                files.remove_file_in_batch(expired_files.clone()).await?;
                expired_files.clear();
            }

            if remove_temp_files_name.len() >= limit {
                break;
            }
        }
    }

    if !expired_files.is_empty() {
        info!(
            "Removing the remaining {} expired files",
            expired_files.len()
        );
        files.remove_file_in_batch(expired_files).await?;
    }

    info!(
        "Finished vacuuming temporary files, removed {} files in total",
        remove_temp_files_name.len()
    );

    Ok(remove_temp_files_name)
}
