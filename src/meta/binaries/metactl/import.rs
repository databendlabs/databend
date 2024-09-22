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
use std::path::Path;
use std::str::FromStr;
use std::sync::Arc;
use std::sync::Mutex;

use databend_common_meta_raft_store::config::RaftConfig;
use databend_common_meta_raft_store::key_spaces::RaftStoreEntry;
use databend_common_meta_raft_store::key_spaces::SMEntry;
use databend_common_meta_raft_store::ondisk::DataVersion;
use databend_common_meta_raft_store::sm_v003::adapter::SnapshotUpgradeV002ToV003;
use databend_common_meta_raft_store::sm_v003::write_entry::WriteEntry;
use databend_common_meta_raft_store::sm_v003::SnapshotStoreV003;
use databend_common_meta_raft_store::state::RaftState;
use databend_common_meta_raft_store::state_machine::MetaSnapshotId;
use databend_common_meta_sled_store::get_sled_db;
use databend_common_meta_sled_store::init_sled_db;
use databend_common_meta_sled_store::openraft::storage::RaftLogStorageExt;
use databend_common_meta_sled_store::openraft::RaftSnapshotBuilder;
use databend_common_meta_types::sys_data::SysData;
use databend_common_meta_types::Cmd;
use databend_common_meta_types::CommittedLeaderId;
use databend_common_meta_types::Endpoint;
use databend_common_meta_types::Entry;
use databend_common_meta_types::EntryPayload;
use databend_common_meta_types::LogEntry;
use databend_common_meta_types::LogId;
use databend_common_meta_types::Membership;
use databend_common_meta_types::Node;
use databend_common_meta_types::NodeId;
use databend_common_meta_types::StoredMembership;
use databend_meta::store::RaftStore;
use url::Url;

use crate::reading;
use crate::upgrade;
use crate::ImportArgs;

pub async fn import_data(args: &ImportArgs) -> anyhow::Result<()> {
    let raft_dir = args.raft_dir.clone().unwrap_or_default();

    eprintln!();
    eprintln!("Import:");
    eprintln!("    Into Meta Dir: '{}'", raft_dir);
    eprintln!("    Initialize Cluster with Id: {}, cluster: {{", args.id);
    for peer in args.initial_cluster.clone() {
        eprintln!("        Peer: {}", peer);
    }
    eprintln!("    }}");

    let nodes = build_nodes(args.initial_cluster.clone(), args.id)?;

    init_sled_db(raft_dir.clone(), 64 * 1024 * 1024 * 1024);

    clear(args)?;
    let max_log_id = import_from_stdin_or_file(args).await?;

    if args.initial_cluster.is_empty() {
        return Ok(());
    }

    init_new_cluster(args, nodes, max_log_id).await?;
    Ok(())
}

/// Import from lines of exported data and Return the max log id that is found.
async fn import_lines<B: BufRead + 'static>(
    raft_config: RaftConfig,
    lines: Lines<B>,
) -> anyhow::Result<Option<LogId>> {
    #[allow(clippy::useless_conversion)]
    let mut it = lines.into_iter().peekable();
    let version = reading::validate_version(&mut it)?;

    let max_log_id = match version {
        DataVersion::V0 => {
            return Err(anyhow::anyhow!(
                "importing from V0 is not supported since 2024-03-01,
                 please use an older version databend-metactl to import from V0"
            ));
        }
        DataVersion::V001 => {
            return Err(anyhow::anyhow!(
                "importing from V001 is not supported since 2024-06-12,
                 please use an older version databend-metactl to import from V001"
            ));
        }
        DataVersion::V002 => import_v002(raft_config, it).await?,
        DataVersion::V003 => import_v003(raft_config, it).await?,
    };

    Ok(max_log_id)
}

/// Import serialized lines for `DataVersion::V002`
///
/// While importing, the max log id is also returned.
///
/// It write logs and related entries to sled trees, and state_machine entries to a snapshot.
async fn import_v002(
    raft_config: RaftConfig,
    lines: impl IntoIterator<Item = Result<String, io::Error>>,
) -> anyhow::Result<Option<LogId>> {
    // v002 and v003 share the same exported data format.
    import_v003(raft_config, lines).await
}

/// Import serialized lines for `DataVersion::V003`
///
/// While importing, the max log id is also returned.
///
/// It write logs and related entries to sled trees, and state_machine entries to a snapshot.
async fn import_v003(
    raft_config: RaftConfig,
    lines: impl IntoIterator<Item = Result<String, io::Error>>,
) -> anyhow::Result<Option<LogId>> {
    let db = get_sled_db();

    let mut n = 0;
    let mut max_log_id: Option<LogId> = None;
    let mut trees = BTreeMap::new();

    let sys_data = Arc::new(Mutex::new(SysData::default()));

    let snapshot_store = SnapshotStoreV003::new(raft_config);
    let writer = snapshot_store.new_writer()?;
    let (tx, join_handle) = writer.spawn_writer_thread("import_v003");

    let mut converter = SnapshotUpgradeV002ToV003 {
        sys_data: sys_data.clone(),
    };

    for line in lines {
        let l = line?;
        let (tree_name, kv_entry): (String, RaftStoreEntry) = serde_json::from_str(&l)?;

        if tree_name.starts_with("state_machine/") {
            // Write to snapshot
            let sm_entry: SMEntry = kv_entry.try_into().map_err(|err_str| {
                anyhow::anyhow!("Failed to convert RaftStoreEntry to SMEntry: {}", err_str)
            })?;

            let kv = converter.sm_entry_to_rotbl_kv(sm_entry)?;
            if let Some(kv) = kv {
                tx.send(WriteEntry::Data(kv)).await?;
            }
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

    let s = {
        let r = sys_data.lock().unwrap();
        r.clone()
    };

    tx.send(WriteEntry::Finish(s)).await?;
    let temp_snapshot_data = join_handle.await??;

    let last_applied = {
        let r = sys_data.lock().unwrap();
        *r.last_applied_ref()
    };
    let snapshot_id = MetaSnapshotId::new_with_epoch(last_applied);
    let db = temp_snapshot_data.move_to_final_path(snapshot_id.to_string())?;

    eprintln!(
        "Imported {} records, snapshot: {}; snapshot_path: {}; snapshot_stat: {}",
        n,
        snapshot_id,
        db.path(),
        db.stat()
    );
    Ok(max_log_id)
}

/// Read every line from stdin or restore file, deserialize it into tree_name, key and value.
/// Insert them into sled db and flush.
///
/// Finally upgrade the data in raft_dir to the latest version.
async fn import_from_stdin_or_file(args: &ImportArgs) -> anyhow::Result<Option<LogId>> {
    let restore = args.db.clone();

    let raft_config: RaftConfig = args.clone().into();
    let max_log_id = if restore.is_empty() {
        eprintln!("    From: <stdin>");
        let lines = io::stdin().lines();

        import_lines(raft_config, lines).await?
    } else {
        eprintln!("    From: {}", args.db);
        let file = File::open(restore)?;
        let reader = BufReader::new(file);
        let lines = reader.lines();

        import_lines(raft_config, lines).await?
    };

    let raft_config: RaftConfig = args.clone().into();
    upgrade::upgrade(&raft_config).await?;

    Ok(max_log_id)
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
            (Some(addr), Some(port)) => Endpoint::new(addr, port),
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
    args: &ImportArgs,
    nodes: BTreeMap<NodeId, Node>,
    max_log_id: Option<LogId>,
) -> anyhow::Result<()> {
    eprintln!();
    eprintln!("Initialize Cluster with: {:?}", nodes);

    let db = get_sled_db();
    let raft_config: RaftConfig = args.clone().into();

    let mut sto = RaftStore::open_create(&raft_config, Some(()), None).await?;

    let last_applied = {
        let sm2 = sto.get_state_machine().await;
        *sm2.sys_data_ref().last_applied_ref()
    };

    let last_log_id = std::cmp::max(last_applied, max_log_id);
    let mut log_id = last_log_id.unwrap_or(LogId::new(CommittedLeaderId::new(0, 0), 0));

    let node_ids = nodes.keys().copied().collect::<BTreeSet<_>>();
    let membership = Membership::new(vec![node_ids], ());

    // Update snapshot: Replace nodes set and membership config.
    {
        let mut sm2 = sto.get_state_machine().await;

        *sm2.sys_data_mut().nodes_mut() = nodes.clone();

        // It must set membership to state machine because
        // the snapshot may contain more logs than the last_log_id.
        // In which case, logs will be purged upon startup.
        *sm2.sys_data_mut().last_membership_mut() =
            StoredMembership::new(last_applied, membership.clone());
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

        sto.blocking_append([entry]).await?;

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

            sto.blocking_append([entry]).await?;
        }
    }

    // Reset node id
    let raft_state = RaftState::open_create(&db, &raft_config, Some(()), None).await?;
    raft_state.set_node_id(args.id).await?;

    Ok(())
}

/// Clear all sled data and on-disk snapshot.
fn clear(args: &ImportArgs) -> anyhow::Result<()> {
    eprintln!();
    eprintln!("Clear All Sled Trees Before Import:");
    let db = get_sled_db();

    let tree_names = db.tree_names();
    for n in tree_names.iter() {
        let name = String::from_utf8(n.to_vec())?;
        let tree = db.open_tree(&name)?;
        tree.clear()?;
        eprintln!("    Cleared sled tree: {}", name);
    }

    let df_meta_path = format!("{}/df_meta", args.raft_dir.clone().unwrap_or_default());
    if Path::new(&df_meta_path).exists() {
        remove_dir_all(&df_meta_path)?;
    }

    Ok(())
}
