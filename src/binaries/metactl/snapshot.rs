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
use std::collections::BTreeSet;
use std::fs::File;
use std::io;
use std::io::BufRead;
use std::io::BufReader;
use std::io::Lines;
use std::io::Write;
use std::net::SocketAddr;
use std::net::ToSocketAddrs;
use std::str::FromStr;

use anyhow::anyhow;
use common_base::base::tokio;
use common_meta_raft_store::config::RaftConfig;
use common_meta_raft_store::key_spaces::RaftStoreEntry;
use common_meta_raft_store::key_spaces::RaftStoreEntryCompat;
use common_meta_raft_store::log::RaftLog;
use common_meta_raft_store::log::TREE_RAFT_LOG;
use common_meta_raft_store::ondisk::DataVersion;
use common_meta_raft_store::ondisk::DATA_VERSION;
use common_meta_raft_store::ondisk::TREE_HEADER;
use common_meta_raft_store::state::RaftState;
use common_meta_raft_store::state::TREE_RAFT_STATE;
use common_meta_raft_store::state_machine::StateMachine;
use common_meta_sled_store::get_sled_db;
use common_meta_sled_store::init_sled_db;
use common_meta_sled_store::openraft::compat::Upgrade;
use common_meta_stoerr::MetaStorageError;
use common_meta_types::anyerror::AnyError;
use common_meta_types::Cmd;
use common_meta_types::Endpoint;
use common_meta_types::Entry;
use common_meta_types::EntryPayload;
use common_meta_types::LogEntry;
use common_meta_types::LogId;
use common_meta_types::Membership;
use common_meta_types::Node;
use common_meta_types::NodeId;
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
    eprintln!("    Into Meta Dir: '{}'", raft_config.raft_dir);

    let nodes = build_nodes(config.initial_cluster.clone(), raft_config.id)?;

    init_sled_db(raft_config.raft_dir.clone());

    clear()?;
    let max_log_id = import_from(config.db.clone())?;

    if config.initial_cluster.is_empty() {
        return Ok(());
    }

    init_new_cluster(nodes, max_log_id, config.raft_config.id).await?;
    Ok(())
}

// return the max log id
fn import_lines<B: BufRead>(lines: Lines<B>) -> anyhow::Result<Option<LogId>> {
    let db = get_sled_db();
    let mut trees = BTreeMap::new();
    let mut n = 0;
    let mut max_log_id: Option<LogId> = None;

    #[allow(clippy::useless_conversion)]
    let mut it = lines.into_iter();

    // First line is the data header that containing version.
    {
        let first_line = it
            .next()
            .ok_or_else(|| anyhow::anyhow!("no data to import"))??;

        let (tree_name, kv_entry): (String, RaftStoreEntryCompat) =
            serde_json::from_str(&first_line)?;

        let kv_entry = kv_entry.upgrade();

        let version = if tree_name == TREE_HEADER {
            // There is a explicit header.
            if let RaftStoreEntry::DataHeader { key, value } = &kv_entry {
                assert_eq!(key, "header", "The key can only be 'header'");
                value.version
            } else {
                unreachable!("The header tree can only contain DataHeader");
            }
        } else {
            // Without header, the data version is V0 by default.
            DataVersion::V0
        };

        if !DATA_VERSION.is_compatible(version) {
            return Err(anyhow!(
                "invalid data version: {:?}, This program version is {:?}; The latest compatible program version is: {:?}",
                version,
                DATA_VERSION,
                version.max_compatible_working_version(),
            ));
        }

        let (k, v) = RaftStoreEntry::serialize(&kv_entry)?;

        let tree = db.open_tree(&tree_name)?;
        tree.insert(k, v)?;
        tree.flush()?;
    }

    for line in it {
        let l = line?;
        let (tree_name, kv_entry): (String, RaftStoreEntryCompat) = serde_json::from_str(&l)?;
        let kv_entry = kv_entry.upgrade();

        // eprintln!("line: {}", l);

        if !trees.contains_key(&tree_name) {
            let tree = db.open_tree(&tree_name)?;
            trees.insert(tree_name.clone(), tree);
        }

        let tree = trees.get(&tree_name).unwrap();

        let (k, v) = RaftStoreEntry::serialize(&kv_entry)?;

        tree.insert(k, v)?;
        n += 1;

        if let RaftStoreEntry::Logs { key: _, value } = kv_entry {
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

/// Build `Node` for cluster with new addresses configured.
///
/// Raw config is: `<NodeId>=<raft-api-host>:<raft-api-port>[,...]`, e.g. `1=localhost:29103` or `1=localhost:29103,0.0.0.0:19191`
/// The second part is obsolete grpc api address and will be just ignored. Databend-meta loads Grpc address from config file when starting up.
fn build_nodes(initial_cluster: Vec<String>, id: u64) -> anyhow::Result<BTreeMap<NodeId, Node>> {
    eprintln!("Initialize Cluster: id={}, {:?}", id, initial_cluster);

    let mut nodes = BTreeMap::new();
    for peer in initial_cluster {
        eprintln!("peer:{}", peer);

        let id_addrs: Vec<&str> = peer.split('=').collect();
        if id_addrs.len() != 2 {
            return Err(anyhow::anyhow!("invalid peer str: {}", peer));
        }
        let id = u64::from_str(id_addrs[0])?;

        let addrs: Vec<&str> = id_addrs[1].split(',').collect();
        if addrs.len() > 2 || addrs.is_empty() {
            return Err(anyhow::anyhow!(
                "require 1 or 2 addresses in peer str: {}",
                peer
            ));
        }
        let url = Url::parse(&format!("http://{}", addrs[0]))?;
        let endpoint = match (url.host_str(), url.port()) {
            (Some(addr), Some(port)) => Endpoint {
                addr: addr.to_string(),
                port: port as u32,
            },
            _ => {
                return Err(anyhow::anyhow!("invalid peer raft addr: {}", addrs[0]));
            }
        };

        let node = Node::new(id, endpoint.clone());
        eprintln!("new cluster node:{}", node);

        nodes.insert(id, node);
    }

    if nodes.is_empty() {
        return Ok(nodes);
    }

    if !nodes.contains_key(&id) {
        return Err(anyhow::anyhow!(
            "node id ({}) has to be one of cluster member({:?})",
            id,
            nodes.keys().collect::<Vec<_>>()
        ));
    }

    Ok(nodes)
}

// initial_cluster format: node_id=endpoint,grpc_api_addr;
async fn init_new_cluster(
    nodes: BTreeMap<NodeId, Node>,
    max_log_id: Option<LogId>,
    id: u64,
) -> anyhow::Result<()> {
    eprintln!("Initialize Cluster with: {:?}", nodes);

    let node_ids = nodes.keys().copied().collect::<BTreeSet<_>>();

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
        None => sm
            .get_last_applied()?
            .ok_or(anyhow::Error::new(MetaStorageError::SledError(
                AnyError::error("cannot find last applied log id"),
            )))?,
    };

    // construct Membership log entry
    {
        // insert last membership log
        log_id.index += 1;
        let membership = Membership::new(vec![node_ids], ());
        let entry: Entry = Entry {
            log_id,
            payload: EntryPayload::Membership(membership),
        };

        log.append([entry]).await?;
    }

    // construct AddNode log entries
    {
        // first clear all the nodes info
        sm.nodes().range_remove(.., true).await?;

        for node in nodes {
            sm.add_node(node.0, &node.1).await?;
            log_id.index += 1;
            let cmd: Cmd = Cmd::AddNode {
                node_id: node.0,
                node: node.1,
                overriding: true,
            };

            let entry: Entry = Entry {
                log_id,
                payload: EntryPayload::Normal(LogEntry {
                    txid: None,
                    time_ms: None,
                    cmd,
                }),
            };

            log.append([entry]).await?;
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

    eprintln!("    From: {}", config.raft_config.raft_dir);

    let file: Option<File> = if !config.db.is_empty() {
        eprintln!("    To:   File: {}", config.db);
        Some((File::create(&config.db))?)
    } else {
        eprintln!("    To:   <stdout>");
        None
    };

    let mut cnt = 0;
    let mut present_tree_names = {
        let mut tree_names = BTreeSet::new();
        for n in db.tree_names() {
            let name = String::from_utf8(n.to_vec())?;
            tree_names.insert(name);
        }
        tree_names
    };

    // Export in header, raft_state, log and other order.
    let mut tree_names = vec![];

    for name in [TREE_HEADER, TREE_RAFT_STATE, TREE_RAFT_LOG] {
        if present_tree_names.remove(name) {
            tree_names.push(name.to_string());
        } else {
            eprintln!("tree {} not found", name);
        }
    }
    tree_names.extend(present_tree_names.into_iter().collect::<Vec<_>>());

    for tree_name in tree_names.iter() {
        eprintln!("Exporting: sled tree: '{}'...", tree_name);

        let tree = db.open_tree(tree_name)?;
        for ivec_pair_res in tree.iter() {
            let kv = ivec_pair_res?;
            let k = kv.0.to_vec();
            let v = kv.1.to_vec();

            let kv_entry = RaftStoreEntry::deserialize(&k, &v)?;
            let tree_kv = (tree_name.clone(), kv_entry);

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

    eprintln!("Exported {} records", cnt);

    Ok(())
}

/// Dump metasrv data, raft-log, state machine etc in json to stdout.
async fn export_from_running_node(config: &Config) -> Result<(), anyhow::Error> {
    eprintln!("    From: online meta-service: {}", config.grpc_api_address);

    let grpc_api_addr = get_available_socket_addr(&config.grpc_api_address).await?;

    export_meta(grpc_api_addr.to_string().as_str(), config.db.clone()).await?;
    Ok(())
}

// if port is open, service is running
async fn is_service_running(addr: SocketAddr) -> Result<bool, io::Error> {
    let socket = TcpSocket::new_v4()?;
    let stream = socket.connect(addr).await;

    Ok(stream.is_ok())
}

// try to get available grpc api socket address
async fn get_available_socket_addr(endpoint: &str) -> Result<SocketAddr, anyhow::Error> {
    let addrs_iter = endpoint.to_socket_addrs()?;
    for addr in addrs_iter {
        if is_service_running(addr).await? {
            return Ok(addr);
        }
        eprintln!("WARN: {} is not available", addr);
    }
    Err(anyhow!("no metasrv running on: {}", endpoint))
}
