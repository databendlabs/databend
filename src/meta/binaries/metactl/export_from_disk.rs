// Copyright 2021 Datafuse Labs
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::fs::File;
use std::io::Write;
use std::sync::Arc;

use databend_common_meta_raft_store::config::RaftConfig;
use databend_meta::store::StoreInner;
use futures::TryStreamExt;

use crate::upgrade;
use crate::Config;

/// Print the entire sled db.
///
/// The output encodes every key-value into one line:
/// `[sled_tree_name, {key_space: {key, value}}]`
/// E.g.:
/// `["state_machine/0",{"GenericKV":{"key":"wow","value":{"seq":3,"meta":null,"data":[119,111,119]}}}`
pub async fn export_from_dir(config: &Config) -> anyhow::Result<()> {
    upgrade::upgrade(config).await?;

    let raft_config: RaftConfig = config.clone().into();

    let sto_inn = StoreInner::open_create(&raft_config, Some(()), None).await?;
    let mut lines = Arc::new(sto_inn).export();

    eprintln!("    From: {}", raft_config.raft_dir);

    let file: Option<File> = if !config.db.is_empty() {
        eprintln!("    To:   File: {}", config.db);
        Some((File::create(&config.db))?)
    } else {
        eprintln!("    To:   <stdout>");
        None
    };

    let mut cnt = 0;

    while let Some(line) = lines.try_next().await? {
        cnt += 1;

        if file.as_ref().is_none() {
            println!("{}", line);
        } else {
            file.as_ref()
                .unwrap()
                .write_all(format!("{}\n", line).as_bytes())?;
        }
    }

    if file.is_some() {
        file.as_ref().unwrap().sync_all()?
    }

    eprintln!("Exported {} records", cnt);

    Ok(())
}
