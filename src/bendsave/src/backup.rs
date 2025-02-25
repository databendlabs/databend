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

use anyhow::anyhow;
use anyhow::Result;
use futures::StreamExt;
use futures::TryStreamExt;
use log::info;
use opendal::Buffer;
use opendal::Operator;

use crate::storage::load_databend_meta;
use crate::storage::load_epochfs_storage;
use crate::storage::load_query_storage;
use crate::utils::DATABEND_META_BACKUP_PATH;

pub async fn backup(from: &str, to: &str) -> Result<()> {
    let databend_storage = load_query_storage(from)?;
    let epochfs_op = load_epochfs_storage(to).await?;
    let epochfs_storage = epochfs::Fs::new(epochfs_op).await?;

    // backup metadata first.
    backup_meta(&epochfs_storage).await?;
    backup_query(databend_storage, &epochfs_storage).await?;

    let checkpoint = epochfs_storage.commit().await?;
    info!("backup to checkpoint: {checkpoint}");
    Ok(())
}

/// Backup the entire databend meta to epochfs.
pub async fn backup_meta(efs: &epochfs::Fs) -> Result<()> {
    let (_client_handle, stream) = load_databend_meta().await?;
    let stream = stream.map_ok(Buffer::from);

    let mut file = efs.create_file(DATABEND_META_BACKUP_PATH).await?;
    file.sink(stream).await?;
    file.commit().await?;
    info!("databend meta has been backed up");
    Ok(())
}

/// Backup the entire databend query data to epochfs.
pub async fn backup_query(dstore: Operator, efs: &epochfs::Fs) -> Result<()> {
    let mut list = dstore.lister_with("/").recursive(true).await?;
    while let Some(entry) = list.next().await.transpose()? {
        if entry.metadata().is_dir() {
            continue;
        }

        let stream = dstore
            .reader_with(entry.path())
            .chunk(8 * 1024 * 1024)
            .await?
            .into_bytes_stream(..)
            .await?
            .map_ok(Buffer::from)
            .map_err(|err| anyhow!("read databend query data failed: {err:?}"));

        let mut file = efs.create_file(entry.path()).await?;
        file.sink(stream).await?;
        file.commit().await?;
        info!("databend query file {} has been backed up", entry.path());
    }
    info!("databend query has been backed up");
    Ok(())
}
