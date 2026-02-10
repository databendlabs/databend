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
use std::fs::remove_dir_all;
use std::io;
use std::io::BufRead;
use std::io::BufReader;
use std::io::Lines;
use std::path::Path;
use std::str::FromStr;

use databend_meta::store::RaftStore;
use databend_meta_raft_store::config::RaftConfig;
use databend_meta_raft_store::ondisk::DataVersion;
use databend_meta_raft_store::raft_log::api::raft_log_writer::RaftLogWriter;
use databend_meta_raft_store::raft_log_v004;
use databend_meta_runtime_api::SpawnApi;
use databend_meta_sled_store::init_get_sled_db;
use databend_meta_sled_store::openraft::RaftSnapshotBuilder;
use databend_meta_sled_store::openraft::storage::RaftLogStorageExt;
use databend_meta_types::Cmd;
use databend_meta_types::Endpoint;
use databend_meta_types::LogEntry;
use databend_meta_types::node::Node;
use databend_meta_types::raft_types::Entry;
use databend_meta_types::raft_types::EntryPayload;
use databend_meta_types::raft_types::LogId;
use databend_meta_types::raft_types::Membership;
use databend_meta_types::raft_types::NodeId;
use databend_meta_types::raft_types::StoredMembership;
use databend_meta_types::raft_types::new_log_id;
use display_more::display_option::DisplayOptionExt;
use url::Url;

use crate::args::ImportArgs;
use crate::reading;
use crate::upgrade;

pub async fn import_data<SP: SpawnApi>(args: &ImportArgs) -> anyhow::Result<()> {
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

    clear(args)?;
    let max_log_id = import_from_stdin_or_file::<SP>(args).await?;

    if args.initial_cluster.is_empty() {
        return Ok(());
    }

    init_new_cluster::<SP>(args, nodes, max_log_id).await?;
    Ok(())
}

/// Import from lines of exported data and Return the max log id that is found.
async fn import_lines<SP: SpawnApi, B: BufRead + 'static>(
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
        // v002 v003 v004 share the same exported data format.
        DataVersion::V002 => crate::import_v004::import_v004::<SP>(raft_config, it).await?,
        DataVersion::V003 => crate::import_v004::import_v004::<SP>(raft_config, it).await?,
        DataVersion::V004 => crate::import_v004::import_v004::<SP>(raft_config, it).await?,
    };

    Ok(max_log_id)
}

/// Read every line from stdin or restore file, deserialize it into tree_name, key and value.
/// Insert them into sled db and flush.
///
/// Finally upgrade the data in raft_dir to the latest version.
async fn import_from_stdin_or_file<SP: SpawnApi>(
    args: &ImportArgs,
) -> anyhow::Result<Option<LogId>> {
    let restore = args.db.clone();

    let raft_config: RaftConfig = args.clone().into();
    let max_log_id = if restore.is_empty() {
        eprintln!("    From: <stdin>");
        let lines = io::stdin().lines();

        import_lines::<SP, _>(raft_config, lines).await?
    } else {
        eprintln!("    From: {}", args.db);
        let file = File::open(restore)?;
        let reader = BufReader::new(file);
        let lines = reader.lines();

        import_lines::<SP, _>(raft_config, lines).await?
    };

    let raft_config: RaftConfig = args.clone().into();
    upgrade::upgrade::<SP>(&raft_config).await?;

    eprintln!("Finished import, max_log_id: '{}'", max_log_id.display());

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

    eprintln!("new cluster: {:?}", nodes);

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
async fn init_new_cluster<SP: SpawnApi>(
    args: &ImportArgs,
    nodes: BTreeMap<NodeId, Node>,
    max_log_id: Option<LogId>,
) -> anyhow::Result<()> {
    eprintln!();
    eprintln!("Initialize Cluster with: {:?}", nodes);

    let raft_config: RaftConfig = args.clone().into();

    let sto = RaftStore::<SP>::open(&raft_config).await?;

    let last_applied = {
        let sm2 = sto.get_sm_v003();
        *sm2.sys_data().last_applied_ref()
    };

    let last_log_id = std::cmp::max(last_applied, max_log_id);

    let node_ids = nodes.keys().copied().collect::<BTreeSet<_>>();
    let membership = Membership::new_with_defaults(vec![node_ids], []);

    // Update snapshot: Replace nodes set and membership config.
    {
        let sm2 = sto.get_sm_v003();

        // It must set membership to state machine because
        // the snapshot may contain more logs than the last_log_id.
        // In which case, logs will be purged upon startup.
        sm2.with_sys_data(|s| {
            *s.nodes_mut() = nodes.clone();
            *s.last_membership_mut() = StoredMembership::new(last_applied, membership.clone());
        });
    }

    // Build snapshot to persist state machine.
    sto.state_machine().clone().build_snapshot().await?;

    // Update logs: add nodes and override membership
    {
        // insert membership log
        let mut log_id = last_log_id
            .map(|mut log_id| {
                log_id.index += 1;
                log_id
            })
            .unwrap_or(new_log_id(0, 0, 0));

        let entry: Entry = Entry {
            log_id,
            payload: EntryPayload::Membership(membership),
        };

        let mut log = sto.log().clone();
        log.blocking_append([entry]).await?;

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
                payload: EntryPayload::Normal(LogEntry::new(cmd)),
            };

            log.blocking_append([entry]).await?;
        }
    }

    // Reset node id
    {
        let mut log = sto.log().write().await;
        log.save_user_data(Some(raft_log_v004::LogStoreMeta {
            node_id: Some(args.id),
        }))?;
        raft_log_v004::util::blocking_flush(&mut log).await?;
    }

    Ok(())
}

/// Clear all sled data and on-disk snapshot.
fn clear(args: &ImportArgs) -> anyhow::Result<()> {
    eprintln!();
    eprintln!("Clear All Sled Trees Before Import:");
    let db = init_get_sled_db(args.raft_dir.clone().unwrap(), 1024 * 1024 * 1024);

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
