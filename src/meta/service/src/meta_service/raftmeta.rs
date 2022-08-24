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

use std::collections::BTreeSet;
use std::fmt::Debug;
use std::net::Ipv4Addr;
use std::sync::atomic::AtomicI32;
use std::sync::Arc;
use std::time::Duration;

use common_base::base::tokio;
use common_base::base::tokio::sync::watch;
use common_base::base::tokio::sync::Mutex;
use common_base::base::tokio::sync::RwLockReadGuard;
use common_base::base::tokio::task::JoinHandle;
use common_grpc::ConnectionFactory;
use common_grpc::DNSResolver;
use common_meta_raft_store::config::RaftConfig;
use common_meta_raft_store::sled_key_spaces::GenericKV;
use common_meta_raft_store::state_machine::StateMachine;
use common_meta_sled_store::openraft;
use common_meta_sled_store::openraft::DefensiveCheck;
use common_meta_sled_store::openraft::StoreExt;
use common_meta_sled_store::SledKeySpace;
use common_meta_types::protobuf::raft_service_client::RaftServiceClient;
use common_meta_types::protobuf::raft_service_server::RaftServiceServer;
use common_meta_types::protobuf::WatchRequest;
use common_meta_types::AppliedState;
use common_meta_types::Cmd;
use common_meta_types::ConnectionError;
use common_meta_types::Endpoint;
use common_meta_types::ForwardRequest;
use common_meta_types::ForwardResponse;
use common_meta_types::ForwardToLeader;
use common_meta_types::LeaveRequest;
use common_meta_types::LogEntry;
use common_meta_types::MetaError;
use common_meta_types::MetaNetworkError;
use common_meta_types::MetaNetworkResult;
use common_meta_types::MetaRaftError;
use common_meta_types::MetaResult;
use common_meta_types::Node;
use common_meta_types::NodeId;
use common_meta_types::ToMetaError;
use openraft::Config;
use openraft::LogId;
use openraft::Raft;
use openraft::RaftMetrics;
use openraft::SnapshotPolicy;
use openraft::State;
use tracing::debug;
use tracing::error;
use tracing::info;
use tracing::warn;
use tracing::Instrument;

use crate::configs::Config as MetaConfig;
use crate::meta_service::meta_leader::MetaLeader;
use crate::meta_service::ForwardRequestBody;
use crate::meta_service::JoinRequest;
use crate::meta_service::RaftServiceImpl;
use crate::metrics::incr_meta_metrics_leader_change;
use crate::metrics::incr_meta_metrics_read_failed;
use crate::metrics::set_meta_metrics_current_leader;
use crate::metrics::set_meta_metrics_current_term;
use crate::metrics::set_meta_metrics_is_leader;
use crate::metrics::set_meta_metrics_last_log_index;
use crate::metrics::set_meta_metrics_node_is_health;
use crate::metrics::set_meta_metrics_proposals_applied;
use crate::network::Network;
use crate::store::RaftStore;
use crate::store::RaftStoreBare;
use crate::watcher::WatcherManager;
use crate::watcher::WatcherStreamSender;
use crate::Opened;

#[derive(serde::Serialize)]
pub struct MetaNodeStatus {
    pub id: NodeId,

    pub endpoint: String,

    pub db_size: u64,

    pub state: String,

    pub is_leader: bool,

    pub current_term: u64,

    pub last_log_index: u64,

    pub last_applied: LogId,

    pub leader: Option<Node>,

    pub voters: Vec<Node>,

    pub non_voters: Vec<Node>,

    /// The last `seq` used by GenericKV sub tree.
    ///
    /// `seq` is a monotonically incremental integer for every value that is inserted or updated.
    pub last_seq: u64,
}

// MetaRaft is a impl of the generic Raft handling meta data R/W.
pub type MetaRaft = Raft<LogEntry, AppliedState, Network, RaftStore>;

// MetaNode is the container of meta data related components and threads, such as storage, the raft node and a raft-state monitor.
pub struct MetaNode {
    pub sto: Arc<RaftStore>,
    pub watcher: WatcherManager,
    pub raft: MetaRaft,
    pub running_tx: watch::Sender<()>,
    pub running_rx: watch::Receiver<()>,
    pub join_handles: Mutex<Vec<JoinHandle<MetaResult<()>>>>,
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
    sto: Option<Arc<RaftStore>>,
    monitor_metrics: bool,
    endpoint: Option<Endpoint>,
}

impl MetaNodeBuilder {
    pub async fn build(mut self) -> MetaResult<Arc<MetaNode>> {
        let node_id = self
            .node_id
            .ok_or_else(|| MetaError::InvalidConfig(String::from("node_id is not set")))?;

        let config = self
            .raft_config
            .take()
            .ok_or_else(|| MetaError::InvalidConfig(String::from("config is not set")))?;

        let sto = self
            .sto
            .take()
            .ok_or_else(|| MetaError::InvalidConfig(String::from("sto is not set")))?;

        let net = Network::new(sto.clone());

        let raft = MetaRaft::new(node_id, Arc::new(config), Arc::new(net), sto.clone());
        let metrics_rx = raft.metrics();

        let (tx, rx) = watch::channel::<()>(());

        let watcher = WatcherManager::create();

        sto.get_state_machine()
            .await
            .set_subscriber(Box::new(watcher.subscriber.clone()));

        let mn = Arc::new(MetaNode {
            sto: sto.clone(),
            watcher,
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
            sto.get_node_endpoint(&node_id).await?
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
    pub fn sto(mut self, sto: Arc<RaftStore>) -> Self {
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

        Config {
            cluster_name: config.cluster_name.clone(),
            heartbeat_interval: hb,
            election_timeout_min: hb * 8,
            election_timeout_max: hb * 12,
            install_snapshot_timeout: config.install_snapshot_timeout,
            snapshot_policy: SnapshotPolicy::LogsSinceLast(config.snapshot_logs_since_last),
            max_applied_log_to_keep: config.max_applied_log_to_keep,
            ..Default::default()
        }
        .validate()
        .expect("building raft Config from databend-metasrv config")
    }

    /// Start the grpc service for raft communication and meta operation API.
    #[tracing::instrument(level = "debug", skip(mn))]
    pub async fn start_grpc(mn: Arc<MetaNode>, host: &str, port: u32) -> MetaNetworkResult<()> {
        let mut rx = mn.running_rx.clone();

        let meta_srv_impl = RaftServiceImpl::create(mn.clone());
        let meta_srv = RaftServiceServer::new(meta_srv_impl);

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
            .map_error_to_meta_error(MetaError::MetaServiceError, || "fail to serve")?;

            Ok::<(), MetaError>(())
        });

        let mut jh = mn.join_handles.lock().await;
        jh.push(h);
        Ok(())
    }

    /// Open or create a metasrv node.
    /// Optionally boot a single node cluster.
    /// 1. If `open` is `Some`, try to open an existent one.
    /// 2. If `create` is `Some`, try to create an one in non-voter mode.
    #[tracing::instrument(level = "debug", skip_all)]
    pub async fn open_create_boot(
        config: &RaftConfig,
        open: Option<()>,
        create: Option<()>,
        initialize_cluster: Option<Node>,
    ) -> MetaResult<Arc<MetaNode>> {
        info!(
            "open_create_boot, config: {:?}, open: {:?}, create: {:?}, initialize_cluster: {:?}",
            config, open, create, initialize_cluster
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

        let sto = RaftStoreBare::open_create(&config, open, create).await?;
        let sto = Arc::new(StoreExt::new(sto));
        sto.set_defensive(true);

        // config.id only used for the first time
        let self_node_id = if sto.is_opened() { sto.id } else { config.id };

        let builder = MetaNode::builder(&config)
            .sto(sto.clone())
            .node_id(self_node_id)
            .endpoint(config.raft_api_listen_host_endpoint());
        let mn = builder.build().await?;

        info!("MetaNode started: {:?}", config);

        // init_cluster with advertise_host other than listen_host
        if mn.is_opened() {
            return Ok(mn);
        }

        if let Some(node) = initialize_cluster {
            mn.init_cluster(node).await?;
        }
        Ok(mn)
    }

    #[tracing::instrument(level = "debug", skip(self))]
    pub async fn stop(&self) -> MetaResult<i32> {
        let mut rx = self.raft.metrics();

        self.raft
            .shutdown()
            .await
            .map_error_to_meta_error(MetaError::MetaServiceError, || "fail to stop raft")?;
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
            let _rst = j
                .await
                .map_error_to_meta_error(MetaError::MetaServiceError, || "fail to join")?;
            // TODO(luhuanbing): Add joined node information to enrich debugging information
            self.joined_tasks
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        }

        info!("shutdown: id={}", self.sto.id);
        let joined = self.joined_tasks.load(std::sync::atomic::Ordering::Relaxed);
        Ok(joined)
    }

    // spawn a monitor to watch raft state changes such as leader changes,
    // and manually add non-voter to cluster so that non-voter receives raft logs.
    pub async fn subscribe_metrics(mn: Arc<Self>, mut metrics_rx: watch::Receiver<RaftMetrics>) {
        // TODO(luhuanbing): every state change triggers add_non_voter is not very reasonable
        let mut running_rx = mn.running_rx.clone();
        let mut jh = mn.join_handles.lock().await;
        let mut current_leader: Option<u64> = None;

        let mn = mn.clone();

        let span = tracing::span!(tracing::Level::INFO, "watch-metrics");

        let h = tokio::task::spawn(
            {
                async move {
                    loop {
                        let changed = tokio::select! {
                            _ = running_rx.changed() => {
                               return Ok::<(), MetaError>(());
                            }
                            changed = metrics_rx.changed() => {
                                changed
                            }
                        };
                        if changed.is_ok() {
                            let mm = metrics_rx.borrow().clone();
                            set_meta_metrics_node_is_health(
                                mm.state == State::Follower || mm.state == State::Leader,
                            );

                            if let Some(cur) = mm.current_leader {
                                // if current leader has changed?
                                if let Some(leader) = current_leader {
                                    if cur != leader {
                                        current_leader = Some(cur);
                                        incr_meta_metrics_leader_change();
                                    }
                                } else {
                                    current_leader = Some(cur);
                                    incr_meta_metrics_leader_change();
                                }

                                if cur == mn.sto.id {
                                    let _rst = mn.add_configured_non_voters().await;

                                    if _rst.is_err() {
                                        warn!(
                                            "fail to add non-voter: my id={}, rst:{:?}",
                                            mn.sto.id, _rst
                                        );
                                    }
                                    set_meta_metrics_is_leader(true);
                                } else {
                                    set_meta_metrics_is_leader(false);
                                }
                                set_meta_metrics_current_leader(cur);
                            } else {
                                set_meta_metrics_current_leader(0);
                                set_meta_metrics_is_leader(false);
                            }
                            if let Some(last_applied) = mm.last_applied {
                                set_meta_metrics_proposals_applied(last_applied.index);
                            }
                            if let Some(last_log_index) = mm.last_log_index {
                                set_meta_metrics_last_log_index(last_log_index);
                            }
                            set_meta_metrics_current_term(mm.current_term);
                        } else {
                            // shutting down
                            break;
                        }
                    }

                    Ok::<(), MetaError>(())
                }
            }
            .instrument(span),
        );
        jh.push(h);
    }

    /// Start MetaNode in either `boot`, `single`, `join` or `open` mode,
    /// according to config.
    #[tracing::instrument(level = "debug", skip(config))]
    pub async fn start(config: &MetaConfig) -> Result<Arc<MetaNode>, MetaError> {
        info!(?config, "start()");
        let mn = Self::do_start(config).await?;
        info!("Done starting MetaNode: {:?}", config);
        Ok(mn)
    }

    /// Leave the cluster if `--leave` is specified.
    ///
    /// Return whether it has left the cluster.
    #[tracing::instrument(level = "info", skip_all)]
    pub async fn leave_cluster(conf: &RaftConfig) -> Result<bool, MetaError> {
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

        let addrs = &conf.leave_via;
        info!("node-{} about to leave cluster via {:?}", leave_id, addrs);

        #[allow(clippy::never_loop)]
        for addr in addrs {
            info!("leave cluster via {}...", addr);

            let conn_res = RaftServiceClient::connect(format!("http://{}", addr)).await;
            let mut raft_client = match conn_res {
                Ok(c) => c,
                Err(err) => {
                    error!(
                        "fail connecting to {} while leaving cluster, err: {:?}",
                        addr, err
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
                }
            };
        }
        Err(MetaError::MetaServiceError(format!(
            "leave cluster via {:?} failed",
            addrs
        )))
    }

    /// Join an existent cluster if `--join` is specified and this meta node is just created, i.e., not opening an already initialized store.
    #[tracing::instrument(level = "info", skip(conf, self))]
    pub async fn join_cluster(&self, conf: &RaftConfig, grpc_api_addr: String) -> MetaResult<()> {
        if conf.join.is_empty() {
            info!("'--join' is empty, do not need joining cluster");
            return Ok(());
        }

        // Try to join a cluster only when this node is just created.
        // Joining a node with log has risk messing up the data in this node and in the target cluster.
        if self.is_opened() {
            info!("meta node is already initialized, skip joining it to a cluster");
            return Ok(());
        }

        let addrs = &conf.join;

        // Joining cluster has to use advertise host instead of listen host.
        let advertise_endpoint = conf.raft_api_advertise_host_endpoint();
        #[allow(clippy::never_loop)]
        for addr in addrs {
            let timeout = Some(Duration::from_millis(3_000));
            info!(
                "try to join cluster via {}, timeout: {:?}...",
                addr, timeout
            );

            if addr == &conf.raft_api_advertise_host_string() {
                info!("avoid join via self: {}", addr);
                continue;
            }

            let chan_res = ConnectionFactory::create_rpc_channel(addr, timeout, None).await;
            let chan = match chan_res {
                Ok(c) => c,
                Err(e) => {
                    error!("connect to {} join cluster fail: {:?}", addr, e);
                    continue;
                }
            };
            let mut raft_client = RaftServiceClient::new(chan);

            let req = ForwardRequest {
                forward_to_leader: 1,
                body: ForwardRequestBody::Join(JoinRequest {
                    node_id: conf.id,
                    endpoint: advertise_endpoint.clone(),
                    grpc_api_addr: grpc_api_addr.clone(),
                }),
            };

            let join_res = raft_client.forward(req.clone()).await;
            info!("join cluster result: {:?}", join_res);

            match join_res {
                Ok(r) => {
                    let reply = r.into_inner();
                    if !reply.data.is_empty() {
                        info!("join cluster via {} success: {:?}", addr, reply.data);
                        return Ok(());
                    } else {
                        error!("join cluster via {} fail: {:?}", addr, reply.error);
                    }
                }
                Err(s) => {
                    error!("join cluster via {} fail: {:?}", addr, s);
                }
            };
        }
        Err(MetaError::MetaServiceError(format!(
            "join cluster via {:?} fail",
            addrs
        )))
    }

    async fn do_start(conf: &MetaConfig) -> Result<Arc<MetaNode>, MetaError> {
        let raft_conf = &conf.raft_config;

        let initialize_cluster = if raft_conf.single {
            Some(conf.get_node())
        } else {
            None
        };

        if raft_conf.single {
            let mn = MetaNode::open_create_boot(raft_conf, Some(()), Some(()), initialize_cluster)
                .await?;
            return Ok(mn);
        }

        if !raft_conf.join.is_empty() {
            // Bring up a new node, join it into a cluster

            let mn = MetaNode::open_create_boot(raft_conf, Some(()), Some(()), initialize_cluster)
                .await?;
            return Ok(mn);
        }
        // open mode

        let mn = MetaNode::open_create_boot(raft_conf, Some(()), None, initialize_cluster).await?;
        Ok(mn)
    }

    /// Boot up the first node to create a cluster.
    /// For every cluster this func should be called exactly once.
    #[tracing::instrument(level = "debug", skip(config), fields(config_id=config.raft_config.config_id.as_str()))]
    pub async fn boot(config: &MetaConfig) -> MetaResult<Arc<MetaNode>> {
        let mn =
            Self::open_create_boot(&config.raft_config, None, Some(()), Some(config.get_node()))
                .await?;

        Ok(mn)
    }

    // Initialized a single node cluster by:
    // - Initializing raft membership.
    // - Adding current node into the meta data.
    #[tracing::instrument(level = "debug", skip(self))]
    pub async fn init_cluster(&self, node: Node) -> MetaResult<()> {
        let node_id = self.sto.id;

        let mut cluster_node_ids = BTreeSet::new();
        cluster_node_ids.insert(node_id);

        self.raft
            .initialize(cluster_node_ids)
            .await
            .map_err(|x| MetaError::MetaServiceError(format!("{:?}", x)))?;

        info!("initialized cluster");

        self.add_node(node).await?;

        Ok(())
    }

    /// When a leader is established, it is the leader's responsibility to setup replication from itself to non-voters, AKA learners.
    /// openraft does not persist the node set of non-voters, thus we need to do it manually.
    /// This fn should be called once a node found it becomes leader.
    #[tracing::instrument(level = "debug", skip(self))]
    pub async fn add_configured_non_voters(&self) -> MetaResult<()> {
        let node_ids = self.sto.list_non_voters().await;
        for i in node_ids.iter() {
            let x = self.raft.add_learner(*i, true).await;

            info!("add_non_voter result: {:?}", x);
            if x.is_ok() {
                info!("non-voter is added: {}", i);
            } else {
                info!("non-voter already exist: {}", i);
            }
        }
        Ok(())
    }

    #[tracing::instrument(level = "debug", skip(self))]
    pub async fn get_node(&self, node_id: &NodeId) -> MetaResult<Option<Node>> {
        // inconsistent get: from local state machine

        let sm = self.sto.state_machine.read().await;
        sm.get_node(node_id)
    }

    #[tracing::instrument(level = "debug", skip(self))]
    pub async fn get_nodes(&self) -> MetaResult<Vec<Node>> {
        // inconsistent get: from local state machine
        let sm = self.sto.state_machine.read().await;
        sm.get_nodes()
    }

    fn get_state_string(state: openraft::State) -> String {
        match state {
            openraft::State::Learner => "Learner".to_string(),
            openraft::State::Follower => "Follower".to_string(),
            openraft::State::Candidate => "Candidate".to_string(),
            openraft::State::Leader => "Leader".to_string(),
            openraft::State::Shutdown => "Shutdown".to_string(),
        }
    }

    pub async fn get_status(&self) -> MetaResult<MetaNodeStatus> {
        let voters = self.get_voters().await?;
        let non_voters = self.get_non_voters().await?;

        let endpoint = self
            .sto
            .get_node_endpoint(&self.sto.id)
            .await
            .map_err(|e| MetaError::MetaServiceError(format!("get self endpoint failed: {}", e)))?;

        let db_size = self
            .sto
            .db
            .size_on_disk()
            .map_err(|_| MetaError::MetaServiceError("get db_size failed".to_string()))?;

        let metrics = self.raft.metrics().borrow().clone();

        let leader = if let Some(leader_id) = metrics.current_leader {
            self.get_node(&leader_id).await?
        } else {
            None
        };

        let last_seq = {
            let sm = self.sto.state_machine.read().await;
            let last_seq = sm.sequences().get(&GenericKV::NAME.to_string())?;
            last_seq.unwrap_or_default().0
        };

        Ok(MetaNodeStatus {
            id: self.sto.id,
            endpoint: endpoint.to_string(),
            db_size,
            state: MetaNode::get_state_string(metrics.state),
            is_leader: metrics.state == openraft::State::Leader,
            current_term: metrics.current_term,
            last_log_index: metrics.last_log_index.unwrap_or(0),
            last_applied: match metrics.last_applied {
                Some(id) => id,
                None => LogId::new(0, 0),
            },
            leader,
            voters,
            non_voters,
            last_seq,
        })
    }

    #[tracing::instrument(level = "debug", skip(self))]
    pub async fn get_voters(&self) -> MetaResult<Vec<Node>> {
        // inconsistent get: from local state machine
        self.sto.get_voters().await
    }

    #[tracing::instrument(level = "debug", skip(self))]
    pub async fn get_non_voters(&self) -> MetaResult<Vec<Node>> {
        // inconsistent get: from local state machine
        self.sto.get_non_voters().await
    }

    #[tracing::instrument(level = "debug", skip(self))]
    pub async fn get_meta_addrs(&self) -> MetaResult<Vec<String>> {
        // inconsistent get: from local state machine
        let sm = self.sto.state_machine.read().await;
        let nodes = sm.get_nodes()?;
        let endpoints: Vec<String> = nodes
            .iter()
            .map(|n| {
                if let Some(addr) = n.grpc_api_addr.clone() {
                    addr
                } else {
                    // for compatibility with old version that not have grpc_api_addr in NodeInfo.
                    "".to_string()
                }
            })
            .collect();
        Ok(endpoints)
    }

    #[tracing::instrument(level = "debug", skip(self))]
    pub async fn consistent_read<Request, Reply>(&self, req: Request) -> Result<Reply, MetaError>
    where
        Request: Into<ForwardRequestBody> + Debug,
        ForwardResponse: TryInto<Reply>,
        <ForwardResponse as TryInto<Reply>>::Error: std::fmt::Display,
    {
        match self
            .handle_forwardable_request(ForwardRequest {
                forward_to_leader: 1,
                body: req.into(),
            })
            .await
        {
            Err(e) => {
                incr_meta_metrics_read_failed();
                Err(e)
            }
            Ok(res) => {
                let res: Reply = res.try_into().map_err(|e| {
                    incr_meta_metrics_read_failed();
                    MetaRaftError::ConsistentReadError(format!(
                        "consistent read recv invalid reply: {}",
                        e
                    ))
                })?;

                Ok(res)
            }
        }
    }

    #[tracing::instrument(level = "debug", skip(self, req), fields(target=%req.forward_to_leader))]
    pub async fn handle_forwardable_request(
        &self,
        req: ForwardRequest,
    ) -> Result<ForwardResponse, MetaError> {
        debug!("handle_forwardable_request: {:?}", req);

        let forward = req.forward_to_leader;

        let l = self.as_leader().await;
        debug!("as_leader: is_err: {}", l.is_err());
        let res = match l {
            Ok(l) => l.handle_forwardable_req(req.clone()).await,
            Err(e) => Err(MetaRaftError::ForwardToLeader(e).into()),
        };

        let e = match res {
            Ok(x) => return Ok(x),
            Err(e) => e,
        };

        let e = match e {
            MetaError::MetaRaftError(err) => match err {
                MetaRaftError::ForwardToLeader(err) => err,
                _ => return Err(err.into()),
            },
            _ => return Err(e),
        };

        if forward == 0 {
            return Err(MetaRaftError::RequestNotForwardToLeaderError(
                "req not forward to leader".to_string(),
            )
            .into());
        }

        let leader_id = e.leader_id.ok_or_else(|| {
            MetaRaftError::NoLeaderError("need to forward req but no leader".to_string())
        })?;

        let mut r2 = req.clone();
        // Avoid infinite forward
        r2.decr_forward();

        let res: ForwardResponse = self.forward(&leader_id, r2).await?;

        Ok(res)
    }

    /// Return a MetaLeader if `self` believes it is the leader.
    ///
    /// Otherwise it returns the leader in a ForwardToLeader error.
    #[tracing::instrument(level = "debug", skip(self))]
    pub async fn as_leader(&self) -> Result<MetaLeader<'_>, ForwardToLeader> {
        let curr_leader = self.get_leader().await;
        debug!("curr_leader: {:?}", curr_leader);
        if curr_leader == self.sto.id {
            return Ok(MetaLeader::new(self));
        }
        Err(ForwardToLeader {
            leader_id: Some(curr_leader),
        })
    }

    /// Add a new node into this cluster.
    /// The node info is committed with raft, thus it must be called on an initialized node.
    pub async fn add_node(&self, node: Node) -> Result<AppliedState, MetaError> {
        // TODO: use txid?
        let node_id = node.name.parse::<u64>().map_err(|e| {
            MetaError::MetaServiceError(format!("parse node_id error: {:?}", e.to_string()))
        })?;
        let resp = self
            .write(LogEntry {
                txid: None,
                cmd: Cmd::AddNode {
                    node_id: node_id as NodeId,
                    node,
                },
            })
            .await?;
        Ok(resp)
    }

    /// Remove a node from this cluster.
    #[tracing::instrument(level = "debug", skip(self))]
    pub async fn remove_node(&self, node_id: NodeId) -> Result<AppliedState, MetaError> {
        let resp = self
            .write(LogEntry {
                txid: None,
                cmd: Cmd::RemoveNode { node_id },
            })
            .await?;
        Ok(resp)
    }

    pub async fn get_state_machine(&self) -> RwLockReadGuard<'_, StateMachine> {
        self.sto.state_machine.read().await
    }

    /// Submit a write request to the known leader. Returns the response after applying the request.
    #[tracing::instrument(level = "debug", skip(self, req))]
    pub async fn write(&self, req: LogEntry) -> Result<AppliedState, MetaError> {
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
    #[tracing::instrument(level = "debug", skip(self))]
    pub async fn get_leader(&self) -> NodeId {
        // fast path: there is a known leader

        if let Some(l) = self.raft.metrics().borrow().current_leader {
            return l;
        }

        // slow path: wait loop

        // Need to clone before calling changed() on it.
        // Otherwise other thread waiting on changed() may not receive the change event.
        let mut rx = self.raft.metrics();

        loop {
            // NOTE:
            // The metrics may have already changed before we cloning it.
            // Thus we need to re-check the cloned rx.
            if let Some(l) = rx.borrow().current_leader {
                return l;
            }

            let changed = rx.changed().await;
            if changed.is_err() {
                info!("raft metrics tx closed");
                return 0;
            }
        }
    }

    #[tracing::instrument(level = "debug", skip(self))]
    pub async fn forward(
        &self,
        node_id: &NodeId,
        req: ForwardRequest,
    ) -> Result<ForwardResponse, MetaError> {
        let endpoint = self
            .sto
            .get_node_endpoint(node_id)
            .await
            .map_err(|e| MetaNetworkError::GetNodeAddrError(e.to_string()))?;

        let mut client = RaftServiceClient::connect(format!("http://{}", endpoint))
            .await
            .map_err(|e| {
                MetaNetworkError::ConnectionError(ConnectionError::new(
                    e,
                    format!("address: {}", endpoint),
                ))
            })?;

        let resp = client.forward(req).await.map_err(|e| {
            MetaRaftError::ForwardRequestError(format!("{} while forward to {}", e, endpoint))
        })?;
        let raft_mes = resp.into_inner();

        let res: Result<ForwardResponse, MetaError> = raft_mes.into();
        res
    }

    pub fn create_watcher_stream(&self, request: WatchRequest, tx: WatcherStreamSender) {
        self.watcher.create_watcher_stream(request, tx)
    }
}
