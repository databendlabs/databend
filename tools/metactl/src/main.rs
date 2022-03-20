// Copyright 2021 Datafuse Labs.
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

#![feature(stdin_forwarders)]

mod grpc;

use std::collections::BTreeMap;
use std::io;
use std::net::SocketAddr;

use clap::Parser;
use common_base::tokio;
use common_meta_raft_store::config::RaftConfig;
use common_meta_raft_store::sled_key_spaces::KeySpaceKV;
use common_meta_sled_store::get_sled_db;
use common_meta_sled_store::init_sled_db;
use common_tracing::init_global_tracing;
use databend_meta::configs::config::METASRV_GRPC_API_ADDRESS;
use databend_meta::export::deserialize_to_kv_variant;
use databend_meta::export::serialize_kv_variant;
use serde::Deserialize;
use serde::Serialize;
use tokio::net::TcpSocket;

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Parser)]
#[clap(about, version, author)]
struct Config {
    #[clap(long, default_value = "INFO")]
    pub log_level: String,

    #[clap(long)]
    pub import: bool,

    #[clap(long)]
    pub export: bool,

    #[clap(long, env = METASRV_GRPC_API_ADDRESS, default_value = "127.0.0.1:9191")]
    pub grpc_api_address: String,

    #[clap(flatten)]
    pub raft_config: RaftConfig,
}

/// Usage:
/// - To dump a sled db: `$0 --raft-dir ./_your_meta_dir/`:
///   ```
///   ["global-local-kvstate_machine/0",7,"sledks::Sequences","generic-kv",13]
///   ["global-local-kvstate_machine/0",7,"sledks::Sequences","table-lookup",1]
///   ["global-local-kvstate_machine/0",7,"sledks::Sequences","table_id",1]
///   ["global-local-kvstate_machine/0",7,"sledks::Sequences","tables",1]
///   ["global-local-kvstate_machine/0",8,"sledks::Databases",1,{"seq":1,"meta":null,"data":{"engine":"","engine_options":{},"options":{},"created_on":"2022-02-16T03:20:26.007286Z"}}]
///   ```
#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let config = Config::parse();
    let raft_config = &config.raft_config;

    let _guards = init_global_tracing("metactl", "./_metactl_log", &config.log_level);

    eprintln!();
    eprintln!("███╗   ███╗███████╗████████╗ █████╗        ██████╗████████╗██╗     ");
    eprintln!("████╗ ████║██╔════╝╚══██╔══╝██╔══██╗      ██╔════╝╚══██╔══╝██║     ");
    eprintln!("██╔████╔██║█████╗     ██║   ███████║█████╗██║        ██║   ██║     ");
    eprintln!("██║╚██╔╝██║██╔══╝     ██║   ██╔══██║╚════╝██║        ██║   ██║     ");
    eprintln!("██║ ╚═╝ ██║███████╗   ██║   ██║  ██║      ╚██████╗   ██║   ███████╗");
    eprintln!("╚═╝     ╚═╝╚══════╝   ╚═╝   ╚═╝  ╚═╝       ╚═════╝   ╚═╝   ╚══════╝");
    eprintln!();

    eprintln!("raft_config: {}", pretty(raft_config)?);

    // export from grpc api if metasrv is running
    if config.export && !config.grpc_api_address.is_empty() {
        let grpc_api_addr = match config.grpc_api_address.parse() {
            Ok(addr) => addr,
            Err(e) => {
                eprintln!(
                    "ERROR: grpc api address is invalid: {}",
                    &config.grpc_api_address
                );
                return Err(e)?;
            }
        };
        if service_is_running(grpc_api_addr).await? {
            eprintln!("export meta from: {}", &config.grpc_api_address);
            grpc::export_meta(&config.grpc_api_address).await?;
            return Ok(());
        }
    }

    init_sled_db(raft_config.raft_dir.clone());

    if config.export {
        eprintln!("export meta dir from: {}", raft_config.raft_dir);
        print_meta()?;
    } else if config.import {
        eprintln!("import meta dir into: {}", raft_config.raft_dir);
        clear()?;
        import_from_stdin()?;
    }

    Ok(())
}

fn pretty<T>(v: &T) -> Result<String, serde_json::Error>
where T: Serialize {
    serde_json::to_string_pretty(v)
}

fn clear() -> anyhow::Result<()> {
    let db = get_sled_db();

    let tree_names = db.tree_names();
    for n in tree_names.iter() {
        let name = String::from_utf8(n.to_vec())?;
        let tree = db.open_tree(&name)?;
        tree.clear()?;
        eprintln!("Clear sled tree {} Done", name);
    }

    Ok(())
}

/// Read every line from stdin, deserialize it into tree_name, key and value. Insert them into sled db and flush.
fn import_from_stdin() -> anyhow::Result<()> {
    let db = get_sled_db();

    let mut trees = BTreeMap::new();

    let lines = io::stdin().lines();
    let mut n = 0;
    for line in lines {
        let l = line?;
        let (tree_name, kv_variant): (String, KeySpaceKV) = serde_json::from_str(&l)?;
        // eprintln!("line: {}", l);

        if !trees.contains_key(&tree_name) {
            let tree = db.open_tree(&tree_name)?;
            trees.insert(tree_name.clone(), tree);
        }

        let tree = trees.get(&tree_name).unwrap();

        let (k, v) = serialize_kv_variant(&kv_variant)?;

        tree.insert(k, v)?;
        n += 1;
    }

    for tree in trees.values() {
        tree.flush()?;
    }

    eprintln!("Imported {} records", n);

    Ok(())
}

/// Print the entire sled db.
///
/// The output encodes every key-value into one line:
/// `[sled_tree_name, {key_space: {key, value}}]`
/// E.g.:
/// `["test-29000-state_machine/0",{"GenericKV":{"key":"wow","value":{"seq":3,"meta":null,"data":[119,111,119]}}}`
fn print_meta() -> anyhow::Result<()> {
    let db = get_sled_db();

    let tree_names = db.tree_names();
    for n in tree_names.iter() {
        let name = String::from_utf8(n.to_vec())?;

        let tree = db.open_tree(&name)?;
        for x in tree.iter() {
            let kv = x?;
            let kv = vec![kv.0.to_vec(), kv.1.to_vec()];

            let kv_variant = deserialize_to_kv_variant(&kv)?;
            let tree_kv = (name.clone(), kv_variant);

            let line = serde_json::to_string(&tree_kv)?;

            println!("{}", line);
        }
    }

    Ok(())
}

// if port is open, service is running
async fn service_is_running(addr: SocketAddr) -> Result<bool, io::Error> {
    let socket = TcpSocket::new_v4()?;
    let stream = socket.connect(addr).await;

    Ok(stream.is_ok())
}
