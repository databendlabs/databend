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
use std::collections::BTreeSet;
use std::fs::File;
use std::io;
use std::io::BufRead;
use std::io::BufReader;
use std::io::Lines;
use std::io::Write;
use std::net::SocketAddr;
use std::str::FromStr;

use anyhow::anyhow;
use common_base::base::tokio;
use common_meta_raft_store::config::RaftConfig;
use common_meta_raft_store::log::RaftLog;
use common_meta_raft_store::sled_key_spaces::KeySpaceKV;
use common_meta_raft_store::state::RaftState;
use common_meta_raft_store::state_machine::StateMachine;
use common_meta_sled_store::get_sled_db;
use common_meta_sled_store::init_sled_db;
use common_meta_types::anyerror::AnyError;
use common_meta_types::Cmd;
use common_meta_types::Endpoint;
use common_meta_types::LogEntry;
use common_meta_types::LogId;
use common_meta_types::MetaStorageError;
use common_meta_types::Node;
use databend_meta::export::deserialize_to_kv_variant;
use databend_meta::export::serialize_kv_variant;
use openraft::raft::Entry;
use openraft::raft::EntryPayload;
use openraft::Membership;
use tokio::net::TcpSocket;
use url::Url;

use crate::export_meta;
use crate::Config;

pub async fn export_data(config: &Config) -> anyhow::Result<()> {
    let raft_config = &config.raft_config;

    // export from grpc api if metasrv is running
    if config.grpc_api_address.is_empty() {
        init_sled_db(raft_config.raft_dir.clone());
        export_from_dir(config)?;
    } else {
        export_from_running_node(config).await?;
    }

    Ok(())
}

pub async fn import_data(config: &Config) -> anyhow::Result<()> {
    let raft_config = &config.raft_config;
    eprintln!("import meta dir into: {}", raft_config.raft_dir);

    init_sled_db(raft_config.raft_dir.clone());

    clear()?;
    let max_log_id = import_from(config.db.clone())?;

    if config.initial_cluster.is_empty() {
        return Ok(());
    }
    init_new_cluster(
        config.initial_cluster.clone(),
        max_log_id,
        config.raft_config.id,
    )
    .await?;
    Ok(())
}

// return the max log id
fn import_lines<B: BufRead>(lines: Lines<B>) -> anyhow::Result<Option<LogId>> {
    let db = get_sled_db();
    let mut trees = BTreeMap::new();
    let mut n = 0;
    let mut max_log_id: Option<LogId> = None;
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

        if let KeySpaceKV::Logs { key: _, value } = kv_variant {
            match max_log_id {
                Some(log_id) => {
                    if value.log_id > log_id {
                        max_log_id = Some(value.log_id);
                    }
                }
                None => max_log_id = Some(value.log_id),
            };
        };
    }
    for tree in trees.values() {
        tree.flush()?;
    }

    eprintln!("Imported {} records", n);
    Ok(max_log_id)
}

/// Read every line from stdin or restore file, deserialize it into tree_name, key and value.
/// Insert them into sled db and flush.
fn import_from(restore: String) -> anyhow::Result<Option<LogId>> {
    if restore.is_empty() {
        let lines = io::stdin().lines();
        import_lines(lines)
    } else {
        match File::open(restore) {
            Ok(file) => {
                let reader = BufReader::new(file);
                let lines = reader.lines();
                import_lines(lines)
            }
            Err(e) => Err(anyhow::Error::new(e)),
        }
    }
}

// initial_cluster format: node_id=endpoint,grpc_api_addr;
async fn init_new_cluster(
    initial_cluster: Vec<String>,
    max_log_id: Option<LogId>,
    id: u64,
) -> anyhow::Result<()> {
    println!("init-cluster: {:?}", initial_cluster);

    let mut node_ids = BTreeSet::new();
    let mut nodes = BTreeMap::new();
    for peer in initial_cluster {
        println!("peer:{}", peer);
        let node_info: Vec<&str> = peer.split('=').collect();
        if node_info.len() != 2 {
            return Err(anyhow::anyhow!("invalid peer str: {}", peer));
        }
        let id = u64::from_str(node_info[0])?;
        node_ids.insert(id);

        let addrs: Vec<&str> = node_info[1].split(',').collect();
        if addrs.len() != 2 {
            return Err(anyhow::anyhow!("invalid peer str: {}", peer));
        }
        let url = Url::parse(&format!("http://{}", addrs[0]))?;
        if url.host_str().is_none() || url.port().is_none() {
            return Err(anyhow::anyhow!("invalid peer raft addr: {}", addrs[0]));
        }
        let endpoint = Endpoint {
            addr: url.host_str().unwrap().to_string(),
            port: url.port().unwrap() as u32,
        };
        let node = Node {
            name: id.to_string(),
            endpoint: endpoint.clone(),
            grpc_api_addr: Some(addrs[1].to_string()),
        };
        println!("new cluster node:{}", node);
        nodes.insert(id, node);
    }

    let db = get_sled_db();
    let config = RaftConfig {
        ..Default::default()
    };
    let log = RaftLog::open(&db, &config).await?;
    let raft_state = RaftState::open_create(&db, &config, Some(()), None).await?;
    let (sm_id, _prev_sm_id) = raft_state.read_state_machine_id()?;

    let sm = StateMachine::open(&config, sm_id).await?;

    let mut log_id: LogId = match max_log_id {
        Some(max_log_id) => max_log_id,
        None => match sm.get_last_applied()? {
            Some(last_applied) => last_applied,
            None => {
                return Err(anyhow::Error::new(MetaStorageError::SledError(
                    AnyError::error("cannot find last applied log id"),
                )));
            }
        },
    };

    // construct Membership log entry
    {
        // insert last membership log
        log_id.index += 1;
        let membership = Membership::new_single(node_ids);
        let entry: Entry<LogEntry> = Entry::<LogEntry> {
            log_id,
            payload: EntryPayload::Membership(membership),
        };

        log.insert(&entry).await?;
    }

    // construct AddNode log entries
    {
        // first clear all the nodes info
        sm.nodes().clear()?;

        for node in nodes {
            sm.add_node(node.0, &node.1).await?;
            log_id.index += 1;
            let cmd: Cmd = Cmd::AddNode {
                node_id: node.0,
                node: node.1,
            };

            let entry: Entry<LogEntry> = Entry::<LogEntry> {
                log_id,
                payload: EntryPayload::Normal(LogEntry {
                    txid: None,
                    time_ms: None,
                    cmd,
                }),
            };

            log.insert(&entry).await?;
        }
    }

    raft_state.set_node_id(id).await?;

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
fn export_from_dir(config: &Config) -> anyhow::Result<()> {
    let db = get_sled_db();

    let file: Option<File> = if !config.db.is_empty() {
        eprintln!(
            "export meta dir from: {} to {}",
            config.raft_config.raft_dir, config.db
        );
        Some((File::create(&config.db))?)
    } else {
        eprintln!("export meta dir from: {}", config.raft_config.raft_dir);
        None
    };

    let mut cnt = 0;
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
            cnt += 1;

            if file.as_ref().is_none() {
                println!("{}", line);
            } else {
                file.as_ref()
                    .unwrap()
                    .write_all(format!("{}\n", line).as_bytes())?;
            }
        }
    }

    if file.as_ref().is_some() {
        file.as_ref().unwrap().sync_all()?
    }

    eprintln!("export {} records", cnt);

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
