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

use anyhow::Result;
use databend_common_version::BUILD_INFO;
use futures::SinkExt;
use futures::TryStreamExt;
use log::info;
use opendal::Operator;

use crate::storage::init_query;
use crate::storage::load_bendsave_storage;
use crate::storage::load_databend_meta;
use crate::storage::load_query_config;
use crate::storage::load_query_storage;
use crate::storage::verify_query_license;
use crate::utils::DATABEND_META_BACKUP_PATH;
use crate::utils::storage_copy;

pub async fn backup(from: &str, to: &str) -> Result<()> {
    let query_cfg = load_query_config(from)?;
    init_query(&query_cfg)?;
    verify_query_license(&query_cfg, &BUILD_INFO).await?;
    let databend_storage = load_query_storage(&query_cfg)?;

    let bendsave_storage = load_bendsave_storage(to).await?;

    // backup metadata first.
    backup_meta(bendsave_storage.clone()).await?;
    storage_copy(databend_storage, bendsave_storage).await?;

    info!("databend backup has been finished");
    Ok(())
}

/// Backup the entire databend meta to epochfs.
pub async fn backup_meta(op: Operator) -> Result<()> {
    let (_client_handle, stream) = load_databend_meta().await?;
    let mut stream = stream.map_err(std::io::Error::other);

    let mut file = op
        .writer_with(DATABEND_META_BACKUP_PATH)
        .chunk(8 * 1024 * 1024)
        .await?
        .into_bytes_sink();
    file.send_all(&mut stream).await?;
    file.close().await?;
    info!("databend meta has been backed up");
    Ok(())
}
