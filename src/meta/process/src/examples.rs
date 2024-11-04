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

use std::collections::BTreeMap;

use clap::Parser;
use databend_common_meta_raft_store::key_spaces::RaftStoreEntry;
use databend_common_meta_sled_store::init_sled_db;
use databend_common_tracing::init_logging;
use databend_common_tracing::Config as LogConfig;
use serde::Deserialize;
use serde::Serialize;

use crate::kv_processor::GenericKVProcessor;
use crate::process::Process;
use crate::process_meta_dir;
use crate::rewrite_kv;

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq, Parser)]
#[clap(about, author)]
pub struct Config {
    #[clap(long, default_value = "INFO")]
    pub log_level: String,

    #[clap(long, default_value = "")]
    pub cmd: String,

    #[clap(flatten)]
    pub raft_config: RaftConfig,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq, Parser)]
#[clap(about, version, author)]
#[serde(default)]
pub struct RaftConfig {
    /// The dir to store persisted meta state, including raft logs, state machine etc.
    #[clap(long, default_value = "./_meta")]
    #[serde(alias = "kvsrv_raft_dir")]
    pub raft_dir: String,
}

impl Default for RaftConfig {
    fn default() -> Self {
        Self {
            raft_dir: "./_meta".to_string(),
        }
    }
}

/// An example of rewriting all TableMeta records in meta-service
#[allow(dead_code)]
async fn upgrade_09() -> anyhow::Result<()> {
    let config = Config::parse();

    let _guards = init_logging(
        "databend-meta-upgrade-09",
        &LogConfig::default(),
        BTreeMap::new(),
    );

    eprintln!("config: {}", pretty(&config)?);

    match config.cmd.as_str() {
        "print" => {
            print_table_meta(&config)?;
        }
        "upgrade" => {
            let p = GenericKVProcessor::new(rewrite_kv::upgrade_table_meta);
            process_meta_dir::process_sled_db(&config, |x| p.process(x))?;
        }
        _ => {
            return Err(anyhow::anyhow!("invalid cmd: {:?}", config.cmd));
        }
    }

    Ok(())
}

/// It does not update any data but just print TableMeta in protobuf message format
/// that are found in log or state machine.
pub fn print_table_meta(config: &Config) -> anyhow::Result<()> {
    let p = GenericKVProcessor::new(rewrite_kv::print_table_meta);

    let raft_config = &config.raft_config;

    init_sled_db(raft_config.raft_dir.clone(), 64 * 1024 * 1024 * 1024);

    for tree_iter_res in databend_common_meta_sled_store::iter::<Vec<u8>>() {
        let (_tree_name, item_iter) = tree_iter_res?;

        for item_res in item_iter {
            let (k, v) = item_res?;
            let v1_ent = RaftStoreEntry::deserialize(&k, &v)?;

            p.process(v1_ent)?;
        }
    }

    Ok(())
}

fn pretty<T>(v: &T) -> Result<String, serde_json::Error>
where T: Serialize {
    serde_json::to_string_pretty(v)
}
