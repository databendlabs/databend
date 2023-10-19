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
use std::fs::remove_dir_all;
use std::fs::File;
use std::io;
use std::io::BufRead;
use std::io::BufReader;
use std::io::Lines;
use std::io::Write;
use std::net::SocketAddr;
use std::net::ToSocketAddrs;
use std::path::Path;
use std::str::FromStr;
use std::sync::Arc;

use anyhow::anyhow;
use common_base::base::tokio;
use common_meta_raft_store::config::RaftConfig;
use common_meta_raft_store::key_spaces::RaftStoreEntry;
use common_meta_raft_store::key_spaces::RaftStoreEntryCompat;
use common_meta_raft_store::ondisk::DataVersion;
use common_meta_raft_store::ondisk::OnDisk;
use common_meta_raft_store::ondisk::DATA_VERSION;
use common_meta_raft_store::ondisk::TREE_HEADER;
use common_meta_raft_store::sm_v002::SnapshotStoreV002;
use common_meta_raft_store::state::RaftState;
use common_meta_sled_store::get_sled_db;
use common_meta_sled_store::init_sled_db;
use common_meta_sled_store::openraft::compat::Upgrade;
use common_meta_sled_store::openraft::RaftSnapshotBuilder;
use common_meta_sled_store::openraft::RaftStorage;
use common_meta_types::Cmd;
use common_meta_types::CommittedLeaderId;
use common_meta_types::Endpoint;
use common_meta_types::Entry;
use common_meta_types::EntryPayload;
use common_meta_types::LogEntry;
use common_meta_types::LogId;
use common_meta_types::Membership;
use common_meta_types::Node;
use common_meta_types::NodeId;
use common_meta_types::StoredMembership;
use databend_meta::store::RaftStore;
use databend_meta::store::StoreInner;
use futures::TryStreamExt;
use tokio::net::TcpSocket;
use url::Url;

use crate::export_meta;
use crate::Config;

pub async fn export_data(config: &Config) -> anyhow::Result<()> {
    match config.raft_dir {
        None => export_from_running_node(config).await?,
        Some(ref dir) => {
            init_sled_db(dir.clone());
            export_from_dir(config).await?;
        }
    }
    Ok(())
}

pub async fn import_data(config: &Config) -> anyhow::Result<()> {
    let raft_dir = config.raft_dir.clone().unwrap_or_default();
    eprintln!("    Into Meta Dir: '{}'", raft_dir);

    let nodes = build_nodes(config.initial_cluster.clone(), config.id)?;

    init_sled_db(raft_dir.clone());

    clear(config)?;
    let max_log_id = import_from_stdin_or_file(config).await?;

    if config.initial_cluster.is_empty() {
        return Ok(());
    }

    init_new_cluster(config, nodes, max_log_id, config.id).await?;
    Ok(())
}

/// Import from lines of exported data and Return the max log id that is found.
async fn import_lines<B: BufRead + 'static>(
    config: &Config,
    lines: Lines<B>,
) -> anyhow::Result<Option<LogId>> {
    #[allow(clippy::useless_conversion)]
    let mut it = lines.into_iter().peekable();
    let first = it
        .peek()
        .ok_or_else(|| anyhow::anyhow!("no data to import"))?;

    let first_line = match first {
        Ok(l) => l,
        Err(e) => {
            return Err(anyhow::anyhow!("{}", e));
        }
    };

    // First line is the data header that containing version.
    let version = read_version(first_line)?;

    if !DATA_VERSION.is_compatible(version) {
        return Err(anyhow!(
            "invalid data version: {:?}, This program version is {:?}; The latest compatible program version is: {:?}",
            version,
            DATA_VERSION,
            version.max_compatible_working_version(),
        ));
    }

    let max_log_id = match version {
        DataVersion::V0 => import_v0_or_v001(config, it)?,
        DataVersion::V001 => import_v0_or_v001(config, it)?,
        DataVersion::V002 => import_v002(config, it).await?,
    };

    Ok(max_log_id)
}

fn read_version(first_line: &str) -> anyhow::Result<DataVersion> {
    let (tree_name, kv_entry): (String, RaftStoreEntryCompat) = serde_json::from_str(first_line)?;

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

    Ok(version)
}

/// Import serialized lines for `DataVersion::V0` and `DataVersion::V001`
///
/// While importing, the max log id is also returned.
fn import_v0_or_v001(
    _config: &Config,
    lines: impl IntoIterator<Item = Result<String, io::Error>>,
) -> anyhow::Result<Option<LogId>> {
    let db = get_sled_db();
    let mut n = 0;
    let mut max_log_id: Option<LogId> = None;
    let mut trees = BTreeMap::new();

    for line in lines {
        let l = line?;
        let (tree_name, kv_entry): (String, RaftStoreEntryCompat) = serde_json::from_str(&l)?;
        let kv_entry = kv_entry.upgrade();

        if !trees.contains_key(&tree_name) {
            let tree = db.open_tree(&tree_name)?;
            trees.insert(tree_name.clone(), tree);
        }

        let tree = trees.get(&tree_name).unwrap();

        let (k, v) = RaftStoreEntry::serialize(&kv_entry)?;

        tree.insert(k, v)?;
        n += 1;

        if let RaftStoreEntry::Logs { key: _, value } = kv_entry {
            max_log_id = std::cmp::max(max_log_id, Some(value.log_id));
        };
    }

    for tree in trees.values() {
        tree.flush()?;
    }

    eprintln!("Imported {} records", n);
    Ok(max_log_id)
}

/// Import serialized lines for `DataVersion::V002`
///
/// While importing, the max log id is also returned.
///
/// It write logs and related entries to sled trees, and state_machine entries to a snapshot.
async fn import_v002(
    config: &Config,
    lines: impl IntoIterator<Item = Result<String, io::Error>>,
) -> anyhow::Result<Option<LogId>> {
    let raft_config: RaftConfig = config.clone().into();

    let db = get_sled_db();

    let mut n = 0;
    let mut max_log_id: Option<LogId> = None;
    let mut trees = BTreeMap::new();

    let mut snapshot_store = SnapshotStoreV002::new(DataVersion::V002, raft_config);
    let mut writer = snapshot_store.new_writer()?;

    for line in lines {
        let l = line?;
        let (tree_name, kv_entry): (String, RaftStoreEntryCompat) = serde_json::from_str(&l)?;
        let kv_entry = kv_entry.upgrade();

        if tree_name.starts_with("state_machine/") {
            // Write to snapshot
            writer
                .write_entries::<io::Error>(futures::stream::iter([kv_entry]))
                .await?;
        } else {
            // Write to sled tree
            if !trees.contains_key(&tree_name) {
                let tree = db.open_tree(&tree_name)?;
                trees.insert(tree_name.clone(), tree);
            }

            let tree = trees.get(&tree_name).unwrap();

            let (k, v) = RaftStoreEntry::serialize(&kv_entry)?;

            tree.insert(k, v)?;

            if let RaftStoreEntry::Logs { key: _, value } = kv_entry {
                max_log_id = std::cmp::max(max_log_id, Some(value.log_id));
            };
        }

        n += 1;
    }

    for tree in trees.values() {
        tree.flush()?;
    }
    let (snapshot_id, snapshot_size) = writer.commit(None)?;

    eprintln!(
        "Imported {} records, snapshot id: {}; snapshot size: {}",
        n,
        snapshot_id.to_string(),
        snapshot_size
    );
    Ok(max_log_id)
}

/// Read every line from stdin or restore file, deserialize it into tree_name, key and value.
/// Insert them into sled db and flush.
///
/// Finally upgrade the data in raft_dir to the latest version.
async fn import_from_stdin_or_file(config: &Config) -> anyhow::Result<Option<LogId>> {
    let restore = config.db.clone();

    let max_log_id = if restore.is_empty() {
        let lines = io::stdin().lines();

        import_lines(config, lines).await?
    } else {
        let file = File::open(restore)?;
        let reader = BufReader::new(file);
        let lines = reader.lines();

        import_lines(config, lines).await?
    };

    upgrade(config).await?;

    Ok(max_log_id)
}

/// Upgrade the data in raft_dir to the latest version.
async fn upgrade(config: &Config) -> anyhow::Result<()> {
    let raft_config: RaftConfig = config.clone().into();

    let db = get_sled_db();

    let mut on_disk = OnDisk::open(&db, &raft_config).await?;
    on_disk.log_stderr(true);
    on_disk.upgrade().await?;

    Ok(())
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
    config: &Config,
    nodes: BTreeMap<NodeId, Node>,
    max_log_id: Option<LogId>,
    id: u64,
) -> anyhow::Result<()> {
    eprintln!("Initialize Cluster with: {:?}", nodes);

    let db = get_sled_db();
    let raft_config: RaftConfig = config.clone().into();

    let mut sto = RaftStore::open_create(&raft_config, Some(()), None).await?;

    let last_applied = {
        let sm2 = sto.get_state_machine().await;
        *sm2.last_applied_ref()
    };

    let last_log_id = std::cmp::max(last_applied, max_log_id);
    let mut log_id = last_log_id.unwrap_or(LogId::new(CommittedLeaderId::new(0, 0), 0));

    let node_ids = nodes.keys().copied().collect::<BTreeSet<_>>();
    let membership = Membership::new(vec![node_ids], ());

    // Update snapshot: Replace nodes set and membership config.
    {
        let mut sm2 = sto.get_state_machine().await;

        *sm2.nodes_mut() = nodes.clone();

        // It must set membership to state machine because
        // the snapshot may contain more logs than the last_log_id.
        // In which case, logs will be purged upon startup.
        *sm2.last_membership_mut() = StoredMembership::new(last_applied, membership.clone());
    }

    // Build snapshot to persist state machine.
    sto.build_snapshot().await?;

    // Update logs: add nodes and override membership
    {
        // insert membership log
        log_id.index += 1;

        let entry: Entry = Entry {
            log_id,
            payload: EntryPayload::Membership(membership),
        };

        sto.append_to_log([entry]).await?;

        // insert AddNodes logs
        for (node_id, node) in nodes {
            log_id.index += 1;

            let cmd: Cmd = Cmd::AddNode {
                node_id,
                node,
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

            sto.append_to_log([entry]).await?;
        }
    }

    // Reset node id
    let raft_state = RaftState::open_create(&db, &raft_config, Some(()), None).await?;
    raft_state.set_node_id(id).await?;

    Ok(())
}

fn clear(config: &Config) -> anyhow::Result<()> {
    let db = get_sled_db();

    let tree_names = db.tree_names();
    for n in tree_names.iter() {
        let name = String::from_utf8(n.to_vec())?;
        let tree = db.open_tree(&name)?;
        tree.clear()?;
        eprintln!("Clear sled tree {} Done", name);
    }

    let df_meta_path = format!("{}/df_meta", config.raft_dir.clone().unwrap_or_default());
    if Path::new(&df_meta_path).exists() {
        remove_dir_all(&df_meta_path)?;
    }

    Ok(())
}

/// Print the entire sled db.
///
/// The output encodes every key-value into one line:
/// `[sled_tree_name, {key_space: {key, value}}]`
/// E.g.:
/// `["state_machine/0",{"GenericKV":{"key":"wow","value":{"seq":3,"meta":null,"data":[119,111,119]}}}`
async fn export_from_dir(config: &Config) -> anyhow::Result<()> {
    upgrade(config).await?;

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
