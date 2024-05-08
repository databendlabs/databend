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
use std::time::SystemTime;
use std::time::UNIX_EPOCH;

use databend_common_exception::Result;
use databend_common_storage::DataOperator;
use futures_util::stream;
use futures_util::TryStreamExt;
use opendal::Entry;
use opendal::EntryMode;
use opendal::Metakey;

#[async_backtrace::framed]
pub async fn do_vacuum_temporary_files(
    temporary_dir: String,
    retain: Option<Duration>,
    limit: Option<usize>,
) -> Result<usize> {
    let limit = limit.unwrap_or(usize::MAX);
    let expire_time = retain
        .map(|x| x.as_millis())
        .unwrap_or(1000 * 60 * 60 * 24 * 3) as i64;
    let timestamp = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis() as i64;

    let operator = DataOperator::instance().operator();

    let meta_key = Metakey::Mode | Metakey::LastModified;
    let mut ds = operator
        .lister_with(&temporary_dir)
        .metakey(meta_key)
        .await?;

    let mut removed_temp_files = 0;

    while removed_temp_files < limit {
        let mut end_of_stream = true;
        let mut remove_temp_files_path = Vec::with_capacity(1000);

        while let Some(de) = ds.try_next().await? {
            let meta = de.metadata();

            match meta.mode() {
                EntryMode::DIR => {
                    let life_mills =
                        match operator.is_exist(&format!("{}finished", de.path())).await? {
                            true => 0,
                            false => expire_time,
                        };

                    vacuum_finished_query(
                        &mut removed_temp_files,
                        &de,
                        limit,
                        timestamp,
                        life_mills,
                    )
                    .await?;

                    if removed_temp_files >= limit {
                        end_of_stream = false;
                        break;
                    }
                }
                EntryMode::FILE => {
                    if let Some(modified) = meta.last_modified() {
                        if timestamp - modified.timestamp_millis() >= expire_time {
                            removed_temp_files += 1;
                            remove_temp_files_path.push(de.path().to_string());

                            if removed_temp_files >= limit || remove_temp_files_path.len() >= 1000 {
                                end_of_stream = false;
                                break;
                            }
                        }
                    }
                }
                EntryMode::Unknown => unreachable!(),
            }
        }

        if !remove_temp_files_path.is_empty() {
            operator
                .remove_via(stream::iter(remove_temp_files_path))
                .await?;
        }

        if end_of_stream {
            break;
        }
    }

    Ok(removed_temp_files)
}

async fn vacuum_finished_query(
    removed_temp_files: &mut usize,
    de: &Entry,
    limit: usize,
    timestamp: i64,
    life_mills: i64,
) -> Result<()> {
    let operator = DataOperator::instance().operator();

    let mut all_files_removed = true;
    let mut ds = operator
        .lister_with(de.path())
        .metakey(Metakey::Mode | Metakey::LastModified)
        .await?;

    while *removed_temp_files < limit {
        let mut end_of_stream = true;
        let mut all_each_files_removed = true;
        let mut remove_temp_files_path = Vec::with_capacity(1001);

        while let Some(de) = ds.try_next().await? {
            let meta = de.metadata();
            if meta.is_file() {
                if de.name() == "finished" {
                    continue;
                }

                if let Some(modified) = meta.last_modified() {
                    if timestamp - modified.timestamp_millis() >= life_mills {
                        *removed_temp_files += 1;
                        remove_temp_files_path.push(de.path().to_string());

                        if *removed_temp_files >= limit || remove_temp_files_path.len() >= 1000 {
                            end_of_stream = false;
                            break;
                        }

                        continue;
                    }
                }
            }

            all_each_files_removed = false;
        }

        all_files_removed &= all_each_files_removed;

        if !remove_temp_files_path.is_empty() {
            operator
                .remove_via(stream::iter(remove_temp_files_path))
                .await?;
        }

        if end_of_stream {
            break;
        }
    }

    if all_files_removed {
        operator.delete(&format!("{}finished", de.path())).await?;
        operator.delete(de.path()).await?;
    }

    Ok(())
}
