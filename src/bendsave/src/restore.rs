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

use std::pin::pin;

use anyhow::anyhow;
use anyhow::Result;
use databend_common_meta_control::args::ImportArgs;
use databend_common_meta_control::import::import_data;
use futures::StreamExt;
use log::info;
use opendal::Operator;
use tokio::io::AsyncWriteExt;

use crate::storage::load_epochfs_storage;
use crate::storage::load_meta_config;
use crate::storage::load_query_storage;
use crate::utils::DATABEND_META_BACKUP_PATH;

pub async fn restore(from: &str, checkpoint: &str, to_query: &str, to_meta: &str) -> Result<()> {
    let epochfs_op = load_epochfs_storage(from).await?;
    let epochfs_storage = epochfs::Fs::new(epochfs_op).await?;
    epochfs_storage.load(checkpoint).await?;
    info!("load from checkpoint: {checkpoint}");

    let databend_storage = load_query_storage(to_query)?;
    let meta_config = load_meta_config(to_meta)?;

    restore_query(&epochfs_storage, databend_storage).await?;
    restore_meta(&epochfs_storage, &meta_config).await?;
    Ok(())
}

pub async fn restore_meta(
    efs: &epochfs::Fs,
    meta_cfg: &databend_meta::configs::Config,
) -> Result<()> {
    let Some(file) = efs.open_file(DATABEND_META_BACKUP_PATH).await? else {
        return Err(anyhow!(
            "meta backup file not found, is the backup succeed?"
        ));
    };
    let stream = file.stream().await?;
    let mut stream = pin!(stream);

    // Write databend meta backup to local fs.
    let mut local_file = tokio::fs::File::create_new(DATABEND_META_BACKUP_PATH).await?;
    while let Some(buf) = stream.next().await {
        let mut buf = buf?;
        local_file.write_all_buf(&mut buf).await?;
    }
    local_file.sync_all().await?;
    local_file.shutdown().await?;

    let import_args = ImportArgs {
        raft_dir: Some(meta_cfg.raft_config.raft_dir.to_string()),
        db: DATABEND_META_BACKUP_PATH.to_string(),
        initial_cluster: vec![],
        id: meta_cfg.raft_config.id,
    };
    import_data(&import_args).await?;

    info!("databend meta has been restored");
    Ok(())
}

pub async fn restore_query(efs: &epochfs::Fs, dstore: Operator) -> Result<()> {
    let files = efs.list_files();
    let mut files = pin!(files);

    while let Some(file) = files.next().await.transpose()? {
        let reader = file.stream().await?;
        let mut reader = pin!(reader);

        let mut writer = dstore.writer(file.path()).await?;
        while let Some(buffer) = reader.next().await {
            let buffer = buffer?;
            writer.write(buffer).await?;
        }
        writer.close().await?;
        info!("databend query file {} has been restored", file.path());
    }
    info!("databend query has been restored");
    Ok(())
}
