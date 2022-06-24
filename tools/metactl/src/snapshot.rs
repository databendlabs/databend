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

use std::collections::BTreeMap;
use std::fs::File;
use std::io;
use std::io::BufRead;
use std::io::BufReader;
use std::io::Lines;
use std::io::Write;
use std::net::SocketAddr;

use anyhow::anyhow;
use common_base::base::tokio;
use common_meta_raft_store::sled_key_spaces::KeySpaceKV;
use common_meta_sled_store::get_sled_db;
use common_meta_sled_store::init_cluster;
use common_meta_sled_store::init_sled_db;
use databend_meta::export::deserialize_to_kv_variant;
use databend_meta::export::serialize_kv_variant;
use tokio::net::TcpSocket;

use crate::export_meta;
use crate::Config;

pub async fn export_data(config: &Config) -> anyhow::Result<()> {
    let raft_config = &config.raft_config;

    // export from grpc api if metasrv is running
    if config.grpc_api_address.is_empty() {
        eprintln!("export meta dir from: {}", raft_config.raft_dir);

        init_sled_db(raft_config.raft_dir.clone());
        export_from_dir(config.db.clone())?;
    } else {
        export_from_running_node(&config).await?;
    }

    return Ok(());
}

pub fn import_data(config: &Config) -> anyhow::Result<()> {
    let raft_config = &config.raft_config;
    eprintln!("import meta dir into: {}", raft_config.raft_dir);

    init_sled_db(raft_config.raft_dir.clone());

    clear()?;
    import_from(config.db.clone())?;

    return Ok(());
}

fn import_lines<B: BufRead>(lines: Lines<B>) -> anyhow::Result<()> {
    let db = get_sled_db();
    let mut trees = BTreeMap::new();
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

/// Read every line from stdin or restore file, deserialize it into tree_name, key and value. Insert them into sled db and flush.
fn import_from(restore: String) -> anyhow::Result<()> {
    if restore.is_empty() {
        let lines = io::stdin().lines();
        import_lines(lines)?;
    } else {
        let file = File::open(restore).unwrap();
        let reader = BufReader::new(file);
        let lines = reader.lines();
        import_lines(lines)?;
    }

    Ok(())
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

/// Print the entire sled db.
///
/// The output encodes every key-value into one line:
/// `[sled_tree_name, {key_space: {key, value}}]`
/// E.g.:
/// `["test-29000-state_machine/0",{"GenericKV":{"key":"wow","value":{"seq":3,"meta":null,"data":[119,111,119]}}}`
fn export_from_dir(save: String) -> anyhow::Result<()> {
    let db = get_sled_db();

    let file: Option<File> = if !save.is_empty() {
        Some(File::create(&save)?)
    } else {
        None
    };

    let mut tree_names = db.tree_names();
    tree_names.sort();
    for n in tree_names.iter() {
        let name = String::from_utf8(n.to_vec())?;

        let tree = db.open_tree(&name)?;
        for x in tree.iter() {
            let kv = x?;
            let kv = vec![kv.0.to_vec(), kv.1.to_vec()];

            let kv_variant = deserialize_to_kv_variant(&kv)?;
            let tree_kv = (name.clone(), kv_variant);

            let line = serde_json::to_string(&tree_kv)?;

            if file.as_ref().is_none() {
                println!("{}", line);
            } else {
                file.as_ref()
                    .unwrap()
                    .write(format!("{}\n", line).as_bytes())?;
            }
        }
    }

    if file.as_ref().is_some() {
        let _ = file.as_ref().unwrap().sync_all()?;
    }

    Ok(())
}

/// Dump metasrv data, raft-log, state machine etc in json to stdout.
async fn export_from_running_node(config: &Config) -> Result<(), anyhow::Error> {
    eprintln!("export meta dir from remote: {}", config.grpc_api_address);

    let grpc_api_addr: SocketAddr = match config.grpc_api_address.parse() {
        Ok(addr) => addr,
        Err(e) => {
            eprintln!(
                "ERROR: grpc api address is invalid: {}",
                &config.grpc_api_address
            );
            return Err(anyhow!(e));
        }
    };

    if service_is_running(grpc_api_addr).await? {
        export_meta(&config.grpc_api_address, config.db.clone()).await?;
        return Ok(());
    }
    Ok(())
}

// if port is open, service is running
async fn service_is_running(addr: SocketAddr) -> Result<bool, io::Error> {
    let socket = TcpSocket::new_v4()?;
    let stream = socket.connect(addr).await;

    Ok(stream.is_ok())
}
