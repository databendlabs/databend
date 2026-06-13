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
use databend_common_meta_control::args::ImportArgs;
use databend_common_meta_control::import::import_data;
use databend_meta::configs::MetaServiceConfig;
use databend_meta_runtime::DatabendRuntime;
use futures::StreamExt;
use log::info;
use opendal::Operator;
use tokio::io::AsyncWriteExt;

use crate::storage::init_query;
use crate::storage::load_bendsave_storage;
use crate::storage::load_meta_config;
use crate::storage::load_query_config;
use crate::storage::load_query_storage;
use crate::utils::DATABEND_META_BACKUP_PATH;
use crate::utils::storage_copy;

pub async fn restore(from: &str, to_query: &str, to_meta: &str) -> Result<()> {
    let bendsave_storage = load_bendsave_storage(from).await?;

    let query_cfg = load_query_config(to_query)?;
    init_query(&query_cfg)?;
    let databend_storage = load_query_storage(&query_cfg)?;

    let meta_config = load_meta_config(to_meta)?;

    storage_copy(bendsave_storage.clone(), databend_storage).await?;
    restore_meta(bendsave_storage, &meta_config).await?;
    Ok(())
}

pub async fn restore_meta(efs: Operator, meta_cfg: &MetaServiceConfig) -> Result<()> {
    let mut stream = efs
        .reader(DATABEND_META_BACKUP_PATH)
        .await?
        .into_bytes_stream(..)
        .await?;

    // Write databend meta backup to local fs.
    let mut local_file = tokio::fs::File::create_new(DATABEND_META_BACKUP_PATH).await?;
    while let Some(buf) = stream.next().await {
        let mut buf = buf?;
        local_file.write_all_buf(&mut buf).await?;
    }
    local_file.sync_all().await?;
    local_file.shutdown().await?;

    let id = meta_cfg.raft_config.id;

    let import_args = ImportArgs {
        raft_dir: Some(meta_cfg.raft_config.raft_dir.to_string()),
        db: DATABEND_META_BACKUP_PATH.to_string(),

        // when restoring, create a single node cluster, so that it will serve at once.
        // And the address in the following will be replaced with the content in the config upon `databend-meta` startup.
        initial_cluster: vec![format!("{id}=127.0.0.1:28004")],
        id,
    };
    import_data::<DatabendRuntime>(&import_args).await?;

    info!("databend meta has been restored");
    Ok(())
}
