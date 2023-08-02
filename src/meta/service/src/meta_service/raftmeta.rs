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
use std::fmt::Debug;
use std::net::Ipv4Addr;
use std::sync::atomic::AtomicI32;
use std::sync::Arc;
use std::time::Duration;

use anyerror::AnyError;
use common_base::base::tokio;
use common_base::base::tokio::sync::watch;
use common_base::base::tokio::sync::watch::error::RecvError;
use common_base::base::tokio::sync::Mutex;
use common_base::base::tokio::task::JoinHandle;
use common_base::base::tokio::time::sleep;
use common_base::base::tokio::time::Instant;
use common_grpc::ConnectionFactory;
use common_grpc::DNSResolver;
use common_meta_client::reply_to_api_result;
use common_meta_raft_store::config::RaftConfig;
use common_meta_raft_store::key_spaces::GenericKV;
use common_meta_raft_store::ondisk::DataVersion;
use common_meta_raft_store::ondisk::DATA_VERSION;
use common_meta_sled_store::openraft;
use common_meta_sled_store::openraft::storage::Adaptor;
use common_meta_sled_store::openraft::ChangeMembers;
use common_meta_sled_store::SledKeySpace;
use common_meta_stoerr::MetaStorageError;
use common_meta_types::protobuf::raft_service_client::RaftServiceClient;
use common_meta_types::protobuf::raft_service_server::RaftServiceServer;
use common_meta_types::protobuf::WatchRequest;
use common_meta_types::AppliedState;
use common_meta_types::Cmd;
use common_meta_types::CommittedLeaderId;
use common_meta_types::ConnectionError;
use common_meta_types::Endpoint;
use common_meta_types::ForwardRPCError;
use common_meta_types::ForwardToLeader;
use common_meta_types::GrpcConfig;
use common_meta_types::InvalidReply;
use common_meta_types::LogEntry;
use common_meta_types::LogId;
use common_meta_types::MembershipNode;
use common_meta_types::MetaAPIError;
use common_meta_types::MetaError;
use common_meta_types::MetaManagementError;
use common_meta_types::MetaNetworkError;
use common_meta_types::MetaOperationError;
use common_meta_types::MetaStartupError;
use common_meta_types::Node;
use common_meta_types::NodeId;
use common_meta_types::RaftMetrics;
use common_meta_types::TypeConfig;
use futures::channel::oneshot;
use itertools::Itertools;
use log::as_debug;
use log::as_display;
use log::debug;
use log::error;
use log::info;
use log::warn;
use maplit::btreemap;
use minitrace::prelude::*;
use openraft::Config;
use openraft::Raft;
use openraft::ServerState;
use openraft::SnapshotPolicy;

use crate::configs::Config as MetaConfig;
use crate::message::ForwardRequest;
use crate::message::ForwardRequestBody;
use crate::message::ForwardResponse;
use crate::message::JoinRequest;
use crate::message::LeaveRequest;
use crate::meta_service::errors::grpc_error_to_network_err;
use crate::meta_service::meta_leader::MetaLeader;
use crate::meta_service::RaftServiceImpl;
use crate::metrics::server_metrics;
use crate::network::Network;
use crate::store::RaftStore;
use crate::version::METASRV_COMMIT_VERSION;
use crate::watcher::DispatcherSender;
use crate::watcher::EventDispatcher;
use crate::watcher::EventDispatcherHandle;
use crate::watcher::Watcher;
use crate::watcher::WatcherSender;
use crate::Opened;

#[derive(serde::Serialize)]
pub struct MetaNodeStatus {
    pub id: NodeId,

    /// The build version of meta-service binary.
    pub binary_version: String,

    /// The version of the data this meta-service is serving.
    pub data_version: DataVersion,

    /// The raft service endpoint for internal communication
    pub endpoint: String,

    /// The size in bytes of the on disk data.
    pub db_size: u64,

    /// Server state, one of "Follower", "Learner", "Candidate", "Leader".
    pub state: String,

    /// Is this node a leader.
    pub is_leader: bool,

    /// Current term.
    pub current_term: u64,

    /// Last received log index
    pub last_log_index: u64,

    /// Last log id that has been committed and applied to state machine.
    pub last_applied: LogId,

    /// The last known leader node.
    pub leader: Option<Node>,

    /// The replication state of all nodes.
    ///
    /// Only leader node has non-None data for this field, i.e., `is_leader` is true.
    pub replication: Option<BTreeMap<NodeId, Option<LogId>>>,

    /// Nodes that can vote in election can grant replication.
    pub voters: Vec<Node>,

    /// Also known as `learner`s.
    pub non_voters: Vec<Node>,

    /// The last `seq` used by GenericKV sub tree.
    ///
    /// `seq` is a monotonically incremental integer for every value that is inserted or updated.
    pub last_seq: u64,
}

pub type LogStore = Adaptor<TypeConfig, RaftStore>;
pub type SMStore = Adaptor<TypeConfig, RaftStore>;

/// MetaRaft is a implementation of the generic Raft handling meta data R/W.
pub type MetaRaft = Raft<TypeConfig, Network, LogStore, SMStore>;

/// MetaNode is the container of meta data related components and threads, such as storage, the raft node and a raft-state monitor.
pub struct MetaNode {
    pub sto: RaftStore,
    pub dispatcher_handle: EventDispatcherHandle,
    pub raft: MetaRaft,
    pub running_tx: watch::Sender<()>,
    pub running_rx: watch::Receiver<()>,
    pub join_handles: Mutex<Vec<JoinHandle<Result<(), AnyError>>>>,
    pub joined_tasks: AtomicI32,
}

impl Opened for MetaNode {
    fn is_opened(&self) -> bool {
        self.sto.is_opened()
    }
}

pub struct MetaNodeBuilder {
    node_id: Option<NodeId>,
    raft_config: Option<Config>,
    sto: Option<RaftStore>,
    monitor_metrics: bool,
    endpoint: Option<Endpoint>,
}

impl MetaNodeBuilder {
    pub async fn build(mut self) -> Result<Arc<MetaNode>, MetaStartupError> {
        let node_id = self
            .node_id
            .ok_or_else(|| MetaStartupError::InvalidConfig(String::from("node_id is not set")))?;

        let config = self
            .raft_config
            .take()
            .ok_or_else(|| MetaStartupError::InvalidConfig(String::from("config is not set")))?;

        let sto = self
            .sto
            .take()
            .ok_or_else(|| MetaStartupError::InvalidConfig(String::from("sto is not set")))?;

        let net = Network::new(sto.clone());

        let (log_store, sm_store) = Adaptor::new(sto.clone());

        let raft = MetaRaft::new(node_id, Arc::new(config), net, log_store, sm_store)
            .await
            .map_err(|e| MetaStartupError::MetaServiceError(e.to_string()))?;
        let metrics_rx = raft.metrics();

        let (tx, rx) = watch::channel::<()>(());

        let dispatcher_tx = EventDispatcher::spawn();

        sto.get_state_machine()
            .await
            .set_subscriber(Box::new(DispatcherSender(dispatcher_tx.clone())));

        let mn = Arc::new(MetaNode {
            sto: sto.clone(),
            dispatcher_handle: EventDispatcherHandle::new(dispatcher_tx),
            raft,
            running_tx: tx,
            running_rx: rx,
            join_handles: Mutex::new(Vec::new()),
            joined_tasks: AtomicI32::new(1),
        });

        if self.monitor_metrics {
            info!("about to subscribe raft metrics");
            MetaNode::subscribe_metrics(mn.clone(), metrics_rx).await;
        }

        let endpoint = if let Some(a) = self.endpoint.take() {
            a
        } else {
            sto.get_node_endpoint(&node_id).await.map_err(|e| {
                MetaStartupError::InvalidConfig(format!(
                    "endpoint of node: {} is not configured and is not in store, error: {}",
                    node_id, e,
                ))
            })?
        };

        info!("about to start raft grpc on endpoint {}", endpoint);

        MetaNode::start_grpc(mn.clone(), &endpoint.addr, endpoint.port).await?;

        Ok(mn)
    }

    #[must_use]
    pub fn node_id(mut self, node_id: NodeId) -> Self {
        self.node_id = Some(node_id);
        self
    }

    #[must_use]
    pub fn sto(mut self, sto: RaftStore) -> Self {
        self.sto = Some(sto);
        self
    }

    #[must_use]
    pub fn endpoint(mut self, a: Endpoint) -> Self {
        self.endpoint = Some(a);
        self
    }

    #[must_use]
    pub fn monitor_metrics(mut self, b: bool) -> Self {
        self.monitor_metrics = b;
        self
    }
}

impl MetaNode {
    pub fn builder(config: &RaftConfig) -> MetaNodeBuilder {
        let raft_config = MetaNode::new_raft_config(config);

        MetaNodeBuilder {
            node_id: None,
            raft_config: Some(raft_config),
            sto: None,
            monitor_metrics: true,
            endpoint: None,
        }
    }

    pub fn new_raft_config(config: &RaftConfig) -> Config {
        let hb = config.heartbeat_interval;

        let election_timeouts = config.election_timeout();

        Config {
            cluster_name: config.cluster_name.clone(),
            heartbeat_interval: hb,
            election_timeout_min: election_timeouts.0,
            election_timeout_max: election_timeouts.1,
            install_snapshot_timeout: config.install_snapshot_timeout,
            snapshot_policy: SnapshotPolicy::LogsSinceLast(config.snapshot_logs_since_last),
            max_in_snapshot_log_to_keep: config.max_applied_log_to_keep,
            ..Default::default()
        }
        .validate()
        .expect("building raft Config from databend-metasrv config")
    }

    /// Start the grpc service for raft communication and meta operation API.
    #[minitrace::trace]
    pub async fn start_grpc(
        mn: Arc<MetaNode>,
        host: &str,
        port: u32,
    ) -> Result<(), MetaNetworkError> {
        let mut rx = mn.running_rx.clone();

        let meta_srv_impl = RaftServiceImpl::create(mn.clone());
        let meta_srv = RaftServiceServer::new(meta_srv_impl)
            .max_decoding_message_size(GrpcConfig::MAX_DECODING_SIZE)
            .max_encoding_message_size(GrpcConfig::MAX_ENCODING_SIZE);

        let ipv4_addr = host.parse::<Ipv4Addr>();
        let addr = match ipv4_addr {
            Ok(addr) => format!("{}:{}", addr, port),
            Err(_) => {
                let resolver = DNSResolver::instance().map_err(|e| {
                    MetaNetworkError::DnsParseError(format!(
                        "get dns resolver instance error: {}",
                        e
                    ))
                })?;
                let ip_addrs = resolver.resolve(host).await.map_err(|e| {
                    MetaNetworkError::GetNodeAddrError(format!(
                        "resolve addr {} error: {}",
                        host, e
                    ))
                })?;
                format!("{}:{}", ip_addrs[0], port)
            }
        };

        info!("about to start raft grpc on resolved addr {}", addr);

        let addr_str = addr.to_string();
        let ret = addr.parse::<std::net::SocketAddr>();
        let addr = match ret {
            Ok(addr) => addr,
            Err(e) => {
                return Err(e.into());
            }
        };
        let node_id = mn.sto.id;

        let srv = tonic::transport::Server::builder().add_service(meta_srv);

        let h = tokio::spawn(async move {
            srv.serve_with_shutdown(addr, async move {
                let _ = rx.changed().await;
                info!(
                    "signal received, shutting down: id={} {} ",
                    node_id, addr_str
                );
            })
            .await
            .map_err(|e| {
                AnyError::new(&e).add_context(|| "when serving meta-service grpc service")
            })?;

            Ok::<(), AnyError>(())
        });

        let mut jh = mn.join_handles.lock().await;
        jh.push(h);
        Ok(())
    }

    /// Open or create a meta node.
    /// 1. If `open` is `Some`, try to open an existent one.
    /// 2. If `create` is `Some`, try to create an one in non-voter mode.
    #[minitrace::trace]
    pub async fn open_create(
        config: &RaftConfig,
        open: Option<()>,
        create: Option<()>,
    ) -> Result<Arc<MetaNode>, MetaStartupError> {
        info!(
            "open_create_boot, config: {:?}, open: {:?}, create: {:?}",
            config, open, create
        );

        let mut config = config.clone();

        // Always disable fsync on mac.
        // Because there are some integration tests running on mac VM.
        //
        // On mac File::sync_all() takes 10 ms ~ 30 ms, 500 ms at worst, which very likely to fail a test.
        if cfg!(target_os = "macos") {
            warn!("Disabled fsync for meta data tests. fsync on mac is quite slow");
            config.no_sync = true;
        }

        let sto = RaftStore::open_create(&config, open, create).await?;

        // config.id only used for the first time
        let self_node_id = if sto.is_opened() { sto.id } else { config.id };

        let builder = MetaNode::builder(&config)
            .sto(sto.clone())
            .node_id(self_node_id)
            .endpoint(config.raft_api_listen_host_endpoint());
        let mn = builder.build().await?;

        info!("MetaNode started: {:?}", config);

        Ok(mn)
    }

    /// Open or create a metasrv node.
    /// Optionally boot a single node cluster.
    /// 1. If `open` is `Some`, try to open an existent one.
    /// 2. If `create` is `Some`, try to create an one in non-voter mode.
    #[minitrace::trace]
    pub async fn open_create_boot(
        config: &RaftConfig,
        open: Option<()>,
        create: Option<()>,
        initialize_cluster: Option<Node>,
    ) -> Result<Arc<MetaNode>, MetaStartupError> {
        let mn = Self::open_create(config, open, create).await?;

        if let Some(node) = initialize_cluster {
            mn.init_cluster(node).await?;
        }
        Ok(mn)
    }

    #[minitrace::trace]
    pub async fn stop(&self) -> Result<i32, MetaError> {
        let mut rx = self.raft.metrics();

        let res = self.raft.shutdown().await;
        info!("raft shutdown res: {:?}", res);

        // The returned error does not mean this function call failed.
        // Do not need to return this error. Keep shutting down other tasks.
        if let Err(ref e) = res {
            error!("raft shutdown error: {:?}", e);
        }

        // safe unwrap: receiver wait for change.
        self.running_tx.send(()).unwrap();

        // wait for raft to close the metrics tx
        loop {
            let r = rx.changed().await;
            if r.is_err() {
                break;
            }
            info!("waiting for raft to shutdown, metrics: {:?}", rx.borrow());
        }
        info!("shutdown raft");

        for j in self.join_handles.lock().await.iter_mut() {
            let res = j.await;
            info!("task quit res: {:?}", res);

            // The returned error does not mean this function call failed.
            // Do not need to return this error. Keep shutting down other tasks.
            if let Err(ref e) = res {
                error!("task quit with error: {:?}", e);
            }

            self.joined_tasks
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        }

        info!("shutdown: id={}", self.sto.id);
        let joined = self.joined_tasks.load(std::sync::atomic::Ordering::Relaxed);
        Ok(joined)
    }

    /// Spawn a monitor to watch raft state changes and report metrics changes.
    pub async fn subscribe_metrics(mn: Arc<Self>, mut metrics_rx: watch::Receiver<RaftMetrics>) {
        let meta_node = mn.clone();

        let fut = async move {
            let mut last_leader: Option<u64> = None;

            loop {
                let changed = metrics_rx.changed().await;

                if let Err(changed_err) = changed {
                    // Shutting down.
                    error!("{} when watching metrics_rx", changed_err);
                    break;
                }

                let mm = metrics_rx.borrow().clone();

                // Report metrics about server state and role.

                server_metrics::set_node_is_health(
                    mm.state == ServerState::Follower || mm.state == ServerState::Leader,
                );

                if mm.current_leader.is_some() && mm.current_leader != last_leader {
                    server_metrics::incr_leader_change();
                }
                server_metrics::set_current_leader(mm.current_leader.unwrap_or_default());
                server_metrics::set_is_leader(mm.current_leader == Some(meta_node.sto.id));

                // metrics about raft log and state machine.

                server_metrics::set_current_term(mm.current_term);
                server_metrics::set_last_log_index(mm.last_log_index.unwrap_or_default());
                server_metrics::set_proposals_applied(mm.last_applied.unwrap_or_default().index);
                server_metrics::set_last_seq(
                    meta_node
                        .get_last_seq()
                        .await
                        .map_err(|e| AnyError::new(&e))?,
                );

                last_leader = mm.current_leader;
            }

            Ok::<(), AnyError>(())
        };
        let h = tokio::task::spawn(fut.in_span(Span::enter_with_local_parent("watch-metrics")));

        {
            let mut jh = mn.join_handles.lock().await;
            jh.push(h);
        }
    }

    /// Start MetaNode in either `boot`, `single`, `join` or `open` mode,
    /// according to config.
    #[minitrace::trace]
    pub async fn start(config: &MetaConfig) -> Result<Arc<MetaNode>, MetaStartupError> {
        info!(config = as_debug!(config); "start()");
        let mn = Self::do_start(config).await?;
        info!("Done starting MetaNode: {:?}", config);
        Ok(mn)
    }

    /// Leave the cluster if `--leave` is specified.
    ///
    /// Return whether it has left the cluster.
    #[minitrace::trace]
    pub async fn leave_cluster(conf: &RaftConfig) -> Result<bool, MetaManagementError> {
        if conf.leave_via.is_empty() {
            info!("'--leave-via' is empty, do not need to leave cluster");
            return Ok(false);
        }

        let leave_id = if let Some(id) = conf.leave_id {
            id
        } else {
            info!("'--leave-id' is None, do not need to leave cluster");
            return Ok(false);
        };

        let mut errors = vec![];
        let addrs = &conf.leave_via;
        info!("node-{} about to leave cluster via {:?}", leave_id, addrs);

        #[allow(clippy::never_loop)]
        for addr in addrs {
            info!("leave cluster via {}...", addr);

            let conn_res = RaftServiceClient::connect(format!("http://{}", addr)).await;
            let mut raft_client = match conn_res {
                Ok(c) => c,
                Err(e) => {
                    error!(
                        "fail connecting to {} while leaving cluster, err: {:?}",
                        addr, e
                    );
                    errors.push(
                        AnyError::new(&e)
                            .add_context(|| format!("leave {} via: {}", leave_id, addr.clone())),
                    );
                    continue;
                }
            };

            let req = ForwardRequest {
                forward_to_leader: 1,
                body: ForwardRequestBody::Leave(LeaveRequest { node_id: leave_id }),
            };

            let leave_res = raft_client.forward(req).await;
            match leave_res {
                Ok(resp) => {
                    let reply = resp.into_inner();

                    if !reply.data.is_empty() {
                        info!("Done leaving cluster via {} reply: {:?}", addr, reply.data);
                        return Ok(true);
                    } else {
                        error!("leaving cluster via {} fail: {:?}", addr, reply.error);
                    }
                }
                Err(s) => {
                    error!("leaving cluster via {} fail: {:?}", addr, s);
                    errors.push(
                        AnyError::new(&s)
                            .add_context(|| format!("leave {} via: {}", leave_id, addr.clone())),
                    );
                }
            };
        }
        Err(MetaManagementError::Leave(AnyError::error(format!(
            "fail to leave {} cluster via {:?}, caused by errors: {}",
            leave_id,
            addrs,
            errors.into_iter().map(|e| e.to_string()).join(", ")
        ))))
    }

    /// Join to an existent cluster if:
    /// - `--join` is specified
    /// - and this node is not in a cluster.
    #[minitrace::trace]
    pub async fn join_cluster(
        &self,
        conf: &RaftConfig,
        grpc_api_advertise_address: Option<String>,
    ) -> Result<Result<(), String>, MetaManagementError> {
        if conf.join.is_empty() {
            info!("'--join' is empty, do not need joining cluster");
            return Ok(Err("Did not join: --join is empty".to_string()));
        }

        // Try to join a cluster only when this node has no log.
        // Joining a node with log has risk messing up the data in this node and in the target cluster.
        let in_cluster = self
            .is_in_cluster()
            .await
            .map_err(|e| MetaManagementError::Join(AnyError::new(&e)))?;

        if let Ok(reason) = in_cluster {
            info!("skip joining, because: {}", reason);
            return Ok(Err(format!("Did not join: {}", reason)));
        }

        self.do_join_cluster(conf, grpc_api_advertise_address)
            .await?;
        Ok(Ok(()))
    }

    #[minitrace::trace]
    async fn do_join_cluster(
        &self,
        conf: &RaftConfig,
        grpc_api_advertise_address: Option<String>,
    ) -> Result<(), MetaManagementError> {
        let mut errors = vec![];
        let addrs = &conf.join;

        #[allow(clippy::never_loop)]
        for addr in addrs {
            if addr == &conf.raft_api_advertise_host_string() {
                info!("avoid join via self: {}", addr);
                continue;
            }

            for _i in 0..3 {
                let res = self.join_via(conf, &grpc_api_advertise_address, addr).await;
                match res {
                    Ok(x) => return Ok(x),
                    Err(api_err) => {
                        warn!("{} while joining cluster via {}", api_err, addr);
                        let can_retry = api_err.is_retryable();

                        if can_retry {
                            sleep(Duration::from_millis(1_000)).await;
                            continue;
                        } else {
                            errors.push(api_err);
                            break;
                        }
                    }
                }
            }
        }

        Err(MetaManagementError::Join(AnyError::error(format!(
            "fail to join {} cluster via {:?}, caused by errors: {}",
            self.sto.id,
            addrs,
            errors.into_iter().map(|e| e.to_string()).join(", ")
        ))))
    }

    #[minitrace::trace]
    async fn join_via(
        &self,
        conf: &RaftConfig,
        grpc_api_advertise_address: &Option<String>,
        addr: &String,
    ) -> Result<(), MetaAPIError> {
        // Joining cluster has to use advertise host instead of listen host.
        let advertise_endpoint = conf.raft_api_advertise_host_endpoint();

        let timeout = Some(Duration::from_millis(10_000));
        info!(
            "try to join cluster via {}, timeout: {:?}...",
            addr, timeout
        );

        let chan_res = ConnectionFactory::create_rpc_channel(addr, timeout, None).await;
        let chan = match chan_res {
            Ok(c) => c,
            Err(e) => {
                error!("connect to {} join cluster fail: {:?}", addr, e);
                let net_err = grpc_error_to_network_err(e);
                return Err(MetaAPIError::NetworkError(net_err));
            }
        };
        let mut raft_client = RaftServiceClient::new(chan);

        let req = ForwardRequest {
            forward_to_leader: 1,
            body: ForwardRequestBody::Join(JoinRequest::new(
                conf.id,
                advertise_endpoint.clone(),
                grpc_api_advertise_address.clone(),
            )),
        };

        let join_res = raft_client.forward(req.clone()).await;
        info!("join cluster result: {:?}", join_res);

        match join_res {
            Ok(r) => {
                let reply = r.into_inner();

                let res: Result<ForwardResponse, MetaAPIError> = reply_to_api_result(reply);
                match res {
                    Ok(v) => {
                        info!("join cluster via {} success: {:?}", addr, v);
                        Ok(())
                    }
                    Err(e) => {
                        error!("join cluster via {} fail: {}", addr, e.to_string());
                        Err(e)
                    }
                }
            }
            Err(s) => {
                error!("join cluster via {} fail: {:?}", addr, s);
                let net_err = MetaNetworkError::from(s);
                Err(MetaAPIError::NetworkError(net_err))
            }
        }
    }

    /// Check meta-node state to see if it's appropriate to join to a cluster.
    ///
    /// If there is no StorageError, it returns a `Result`: `Ok` indicates this node is already in a cluster.
    /// `Err` explains the reason why it is not in cluster.
    ///
    /// Meta node should decide whether to execute `join_cluster()`:
    ///
    /// - It can not rely on if there are logs.
    ///   It's possible the leader has setup a replication to this new
    ///   node but not yet added it as a **voter**. In such a case, this node will
    ///   never be added into the cluster automatically.
    ///
    /// - It must detect if there is a committed **membership** config
    ///   that includes this node. Thus only when a node has already joined to a
    ///   cluster(leader committed the membership and has replicated it to this node),
    ///   it skips the join process.
    ///
    ///   Why skip checking membership in raft logs:
    ///
    ///   A leader may have replicated **non-committed** membership to this node and the crashed.
    ///   Then the next leader does not know about this new node.
    ///
    ///   Only when the membership is committed, this node can be sure it is in a cluster.
    async fn is_in_cluster(&self) -> Result<Result<String, String>, MetaStorageError> {
        let m = {
            let sm = self.sto.get_state_machine().await;
            sm.get_membership()?
        };
        info!("is_in_cluster: membership: {:?}", m);

        let membership = match m {
            None => {
                return Ok(Err(format!("node {} has empty membership", self.sto.id)));
            }
            Some(x) => x,
        };

        let voter_ids = membership.membership().voter_ids().collect::<BTreeSet<_>>();

        if voter_ids.contains(&self.sto.id) {
            return Ok(Ok(format!("node {} already in cluster", self.sto.id)));
        }

        Ok(Err(format!(
            "node {} has membership but not in it",
            self.sto.id
        )))
    }

    async fn do_start(conf: &MetaConfig) -> Result<Arc<MetaNode>, MetaStartupError> {
        let raft_conf = &conf.raft_config;

        if raft_conf.single {
            let mn = MetaNode::open_create(raft_conf, Some(()), Some(())).await?;
            mn.init_cluster(conf.get_node()).await?;
            return Ok(mn);
        }

        if !raft_conf.join.is_empty() {
            // Bring up a new node, join it into a cluster

            let mn = MetaNode::open_create(raft_conf, Some(()), Some(())).await?;
            return Ok(mn);
        }
        // open mode

        let mn = MetaNode::open_create(raft_conf, Some(()), None).await?;
        Ok(mn)
    }

    /// Boot up the first node to create a cluster.
    /// For every cluster this func should be called exactly once.
    #[minitrace::trace]
    pub async fn boot(config: &MetaConfig) -> Result<Arc<MetaNode>, MetaStartupError> {
        let mn = Self::open_create(&config.raft_config, None, Some(())).await?;
        mn.init_cluster(config.get_node()).await?;
        Ok(mn)
    }

    /// Initialized a single node cluster if this node is just created:
    /// - Initializing raft membership.
    /// - Adding current node into the meta data.
    #[minitrace::trace]
    pub async fn init_cluster(&self, node: Node) -> Result<(), MetaStartupError> {
        if self.is_opened() {
            info!("It is opened, skip initializing cluster");
            return Ok(());
        }

        let node_id = self.sto.id;

        let mut cluster_node_ids = BTreeSet::new();
        cluster_node_ids.insert(node_id);

        // TODO(1): initialize() and add_node() are not done atomically.
        //          There is an issue that just after initializing the cluster, the node will be used but no node info is found.
        //          To address it, upgrade to membership with embedded Node.
        self.raft.initialize(cluster_node_ids).await?;

        info!("initialized cluster");

        self.add_node(node_id, node)
            .await
            .map_err(|e| MetaStartupError::AddNodeError {
                source: AnyError::new(&e),
            })?;

        Ok(())
    }

    #[minitrace::trace]
    pub async fn get_node(&self, node_id: &NodeId) -> Result<Option<Node>, MetaStorageError> {
        // inconsistent get: from local state machine

        let sm = self.sto.state_machine.read().await;
        let n = sm.get_node(node_id)?;
        Ok(n)
    }

    #[minitrace::trace]
    pub async fn get_nodes(&self) -> Result<Vec<Node>, MetaStorageError> {
        // inconsistent get: from local state machine

        let sm = self.sto.state_machine.read().await;
        let nodes = sm.get_nodes()?;
        Ok(nodes)
    }

    pub async fn get_status(&self) -> Result<MetaNodeStatus, MetaError> {
        let voters = self
            .sto
            .get_nodes(|ms| ms.voter_ids().collect::<Vec<_>>())
            .await?;

        let learners = self
            .sto
            .get_nodes(|ms| ms.learner_ids().collect::<Vec<_>>())
            .await?;

        let endpoint = self.sto.get_node_endpoint(&self.sto.id).await?;

        let db_size = self.sto.db.size_on_disk().map_err(|e| {
            let se = MetaStorageError::SledError(AnyError::new(&e).add_context(|| "get db_size"));
            MetaError::StorageError(se)
        })?;

        let metrics = self.raft.metrics().borrow().clone();

        let leader = if let Some(leader_id) = metrics.current_leader {
            self.get_node(&leader_id).await?
        } else {
            None
        };

        let last_seq = self.get_last_seq().await?;

        Ok(MetaNodeStatus {
            id: self.sto.id,
            binary_version: METASRV_COMMIT_VERSION.as_str().to_string(),
            data_version: DATA_VERSION,
            endpoint: endpoint.to_string(),
            db_size,
            state: format!("{:?}", metrics.state),
            is_leader: metrics.state == openraft::ServerState::Leader,
            current_term: metrics.current_term,
            last_log_index: metrics.last_log_index.unwrap_or(0),
            last_applied: metrics
                .last_applied
                .unwrap_or(LogId::new(CommittedLeaderId::new(0, 0), 0)),
            leader,
            replication: metrics.replication,
            voters,
            non_voters: learners,
            last_seq,
        })
    }

    pub(crate) async fn get_last_seq(&self) -> Result<u64, MetaStorageError> {
        let sm = self.sto.state_machine.read().await;
        let last_seq = sm.sequences().get(&GenericKV::NAME.to_string())?;

        Ok(last_seq.unwrap_or_default().0)
    }

    #[minitrace::trace]
    pub async fn get_grpc_advertise_addrs(&self) -> Result<Vec<String>, MetaStorageError> {
        // inconsistent get: from local state machine

        let nodes = {
            let sm = self.sto.state_machine.read().await;
            sm.get_nodes()?
        };

        let endpoints: Vec<String> = nodes
            .iter()
            .map(|n: &Node| {
                if let Some(addr) = n.grpc_api_advertise_address.clone() {
                    addr
                } else {
                    // for compatibility with old version that not have grpc_api_addr in NodeInfo.
                    "".to_string()
                }
            })
            .collect();
        Ok(endpoints)
    }

    #[minitrace::trace]
    pub async fn consistent_read<Request, Reply>(&self, req: Request) -> Result<Reply, MetaAPIError>
    where
        Request: Into<ForwardRequestBody> + Debug,
        ForwardResponse: TryInto<Reply>,
        <ForwardResponse as TryInto<Reply>>::Error: std::fmt::Display,
    {
        let res = self
            .handle_forwardable_request(ForwardRequest {
                forward_to_leader: 1,
                body: req.into(),
            })
            .await;

        match res {
            Err(e) => {
                server_metrics::incr_read_failed();
                Err(e)
            }
            Ok(res) => {
                let res: Reply = res.try_into().map_err(|e| {
                    server_metrics::incr_read_failed();
                    let invalid_reply = InvalidReply::new(
                        format!("expect reply type to be {}", std::any::type_name::<Reply>(),),
                        &AnyError::error(e),
                    );
                    MetaNetworkError::from(invalid_reply)
                })?;

                Ok(res)
            }
        }
    }

    #[minitrace::trace]
    pub async fn handle_forwardable_request(
        &self,
        req: ForwardRequest,
    ) -> Result<ForwardResponse, MetaAPIError> {
        debug!(target = as_display!(&req.forward_to_leader), req = as_debug!(&req); "handle_forwardable_request");

        let forward = req.forward_to_leader;

        let mut n_retry = 17;
        let mut slp = Duration::from_millis(200);

        loop {
            let assume_leader_res = self.assume_leader().await;
            debug!("assume_leader: is_err: {}", assume_leader_res.is_err());

            // Handle the request locally or return a ForwardToLeader error
            let op_err = match assume_leader_res {
                Ok(leader) => {
                    let res = leader.handle_request(req.clone()).await;
                    match res {
                        Ok(x) => return Ok(x),
                        Err(e) => e,
                    }
                }
                Err(e) => MetaOperationError::ForwardToLeader(e),
            };

            // If needs to forward, deal with it. Otherwise return the unhandlable error.
            let to_leader = match op_err {
                MetaOperationError::ForwardToLeader(err) => err,
                MetaOperationError::DataError(d_err) => {
                    return Err(d_err.into());
                }
            };

            if forward == 0 {
                return Err(MetaAPIError::CanNotForward(AnyError::error(
                    "max number of forward reached",
                )));
            }

            let leader_id = to_leader.leader_id.ok_or_else(|| {
                MetaAPIError::CanNotForward(AnyError::error("need to forward but no known leader"))
            })?;

            let mut req_cloned = req.clone();
            // Avoid infinite forward
            req_cloned.decr_forward();

            let res = self.forward_to(&leader_id, req_cloned).await;
            let forward_err = match res {
                Ok(x) => {
                    return Ok(x);
                }
                Err(forward_err) => forward_err,
            };

            match forward_err {
                ForwardRPCError::NetworkError(ref net_err) => {
                    warn!(
                        "{} retries left, sleep time: {:?}; forward_to {} failed: {}",
                        n_retry, slp, leader_id, net_err
                    );

                    n_retry -= 1;
                    if n_retry == 0 {
                        error!("no more retry for forward_to {}", leader_id);
                        return Err(MetaAPIError::from(forward_err));
                    } else {
                        tokio::time::sleep(slp).await;
                        slp = std::cmp::min(slp * 2, Duration::from_secs(1));
                        continue;
                    }
                }
                ForwardRPCError::RemoteError(_) => {
                    return Err(MetaAPIError::from(forward_err));
                }
            }
        }
    }

    /// Return a MetaLeader if `self` believes it is the leader.
    ///
    /// Otherwise it returns the leader in a ForwardToLeader error.
    #[minitrace::trace]
    pub async fn assume_leader(&self) -> Result<MetaLeader<'_>, ForwardToLeader> {
        let leader_id = self.get_leader().await.map_err(|e| {
            error!("raft metrics rx closed: {}", e);
            ForwardToLeader {
                leader_id: None,
                leader_node: None,
            }
        })?;

        debug!("curr_leader_id: {:?}", leader_id);

        if leader_id == Some(self.sto.id) {
            return Ok(MetaLeader::new(self));
        }

        Err(ForwardToLeader {
            leader_id,
            leader_node: None,
        })
    }

    /// Add a new node into this cluster.
    /// The node info is committed with raft, thus it must be called on an initialized node.
    pub async fn add_node(
        &self,
        node_id: NodeId,
        node: Node,
    ) -> Result<AppliedState, MetaAPIError> {
        // TODO: use txid?
        let cmd = Cmd::AddNode {
            node_id,
            node,
            overriding: false,
        };
        let resp = self.write(LogEntry::new(cmd)).await?;

        self.raft
            .change_membership(
                ChangeMembers::AddNodes(btreemap! {node_id => MembershipNode{}}),
                true,
            )
            .await?;

        Ok(resp)
    }

    /// Submit a write request to the known leader. Returns the response after applying the request.
    #[minitrace::trace]
    pub async fn write(&self, req: LogEntry) -> Result<AppliedState, MetaAPIError> {
        debug!("req: {:?}", req);

        let res = self
            .handle_forwardable_request(ForwardRequest {
                forward_to_leader: 1,
                body: ForwardRequestBody::Write(req.clone()),
            })
            .await?;

        let res: AppliedState = res.try_into().expect("expect AppliedState");

        Ok(res)
    }

    /// Try to get the leader from the latest metrics of the local raft node.
    /// If leader is absent, wait for an metrics update in which a leader is set.
    #[minitrace::trace]
    pub async fn get_leader(&self) -> Result<Option<NodeId>, RecvError> {
        let mut rx = self.raft.metrics();

        let mut expire_at: Option<Instant> = None;

        loop {
            if let Some(l) = rx.borrow().current_leader {
                return Ok(Some(l));
            }

            if expire_at.is_none() {
                let timeout = Duration::from_millis(2_000);
                expire_at = Some(Instant::now() + timeout);
            }
            if Some(Instant::now()) > expire_at {
                warn!("timeout waiting for a leader");
                return Ok(None);
            }

            // Wait for metrics to change and re-fetch the leader id.
            //
            // Note that when it returns, `changed()` will mark the most recent value as **seen**.
            rx.changed().await?;
        }
    }

    #[minitrace::trace]
    pub async fn forward_to(
        &self,
        node_id: &NodeId,
        req: ForwardRequest,
    ) -> Result<ForwardResponse, ForwardRPCError> {
        let endpoint = self
            .sto
            .get_node_endpoint(node_id)
            .await
            .map_err(|e| MetaNetworkError::GetNodeAddrError(e.to_string()))?;

        let client = RaftServiceClient::connect(format!("http://{}", endpoint))
            .await
            .map_err(|e| {
                MetaNetworkError::ConnectionError(ConnectionError::new(
                    e,
                    format!("address: {}", endpoint),
                ))
            })?;

        let mut client = client
            .max_decoding_message_size(GrpcConfig::MAX_DECODING_SIZE)
            .max_encoding_message_size(GrpcConfig::MAX_ENCODING_SIZE);

        let resp = client.forward(req).await.map_err(|e| {
            MetaNetworkError::from(e)
                .add_context(format!("target: {}, endpoint: {}", node_id, endpoint))
        })?;
        let raft_mes = resp.into_inner();

        let res: Result<ForwardResponse, MetaAPIError> = reply_to_api_result(raft_mes);
        let resp = res?;
        Ok(resp)
    }

    pub(crate) async fn add_watcher(
        &self,
        request: WatchRequest,
        tx: WatcherSender,
    ) -> Result<Watcher, &'static str> {
        let (resp_tx, resp_rx) = oneshot::channel();

        self.dispatcher_handle.request(|d: &mut EventDispatcher| {
            let add_res = d.add_watcher(request, tx);
            let _ = resp_tx.send(add_res);
        });

        let recv_res = resp_rx.await;
        match recv_res {
            Ok(add_res) => add_res,
            Err(_e) => Err("dispatcher closed"),
        }
    }
}
