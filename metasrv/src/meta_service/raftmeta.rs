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
use std::sync::Arc;

use common_base::tokio;
use common_base::tokio::sync::watch;
use common_base::tokio::sync::Mutex;
use common_base::tokio::sync::RwLockReadGuard;
use common_base::tokio::task::JoinHandle;
use common_grpc::DNSResolver;
use common_meta_api::MetaApi;
use common_meta_raft_store::config::RaftConfig;
use common_meta_raft_store::state_machine::StateMachine;
use common_meta_raft_store::state_machine::TableLookupKey;
use common_meta_raft_store::state_machine::TableLookupValue;
use common_meta_sled_store::openraft;
use common_meta_types::protobuf::raft_service_client::RaftServiceClient;
use common_meta_types::protobuf::raft_service_server::RaftServiceServer;
use common_meta_types::protobuf::RaftReply;
use common_meta_types::AppliedState;
use common_meta_types::Cmd;
use common_meta_types::ConnectionError;
use common_meta_types::Endpoint;
use common_meta_types::ForwardRequest;
use common_meta_types::ForwardResponse;
use common_meta_types::ForwardToLeader;
use common_meta_types::ListTableReq;
use common_meta_types::LogEntry;
use common_meta_types::MetaError;
use common_meta_types::MetaNetworkError;
use common_meta_types::MetaNetworkResult;
use common_meta_types::MetaRaftError;
use common_meta_types::MetaResult;
use common_meta_types::Node;
use common_meta_types::NodeId;
use common_meta_types::SeqV;
use common_meta_types::TableInfo;
use common_meta_types::TableMeta;
use common_meta_types::ToMetaError;
use common_tracing::tracing;
use common_tracing::tracing::Instrument;
use openraft::Config;
use openraft::Raft;
use openraft::RaftMetrics;
use openraft::SnapshotPolicy;
use tonic::Status;

use crate::meta_service::meta_leader::MetaLeader;
use crate::meta_service::ForwardRequestBody;
use crate::meta_service::JoinRequest;
use crate::meta_service::RaftServiceImpl;
use crate::network::Network;
use crate::store::MetaRaftStore;
use crate::Opened;

// MetaRaft is a impl of the generic Raft handling meta data R/W.
pub type MetaRaft = Raft<LogEntry, AppliedState, Network, MetaRaftStore>;

// MetaNode is the container of meta data related components and threads, such as storage, the raft node and a raft-state monitor.
pub struct MetaNode {
    pub sto: Arc<MetaRaftStore>,
    pub raft: MetaRaft,
    pub running_tx: watch::Sender<()>,
    pub running_rx: watch::Receiver<()>,
    pub join_handles: Mutex<Vec<JoinHandle<MetaResult<()>>>>,
}

impl Opened for MetaNode {
    fn is_opened(&self) -> bool {
        self.sto.is_opened()
    }
}

pub struct MetaNodeBuilder {
    node_id: Option<NodeId>,
    raft_config: Option<Config>,
    sto: Option<Arc<MetaRaftStore>>,
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

        let mn = Arc::new(MetaNode {
            sto: sto.clone(),
            raft,
            running_tx: tx,
            running_rx: rx,
            join_handles: Mutex::new(Vec::new()),
        });

        if self.monitor_metrics {
            tracing::info!("about to subscribe raft metrics");
            MetaNode::subscribe_metrics(mn.clone(), metrics_rx).await;
        }

        let endpoint = if let Some(a) = self.endpoint.take() {
            a
        } else {
            sto.get_node_endpoint(&node_id).await?
        };

        tracing::info!("about to start raft grpc on endpoint {}", endpoint);

        MetaNode::start_grpc(mn.clone(), &endpoint.addr, endpoint.port).await?;

        Ok(mn)
    }

    #[must_use]
    pub fn node_id(mut self, node_id: NodeId) -> Self {
        self.node_id = Some(node_id);
        self
    }

    #[must_use]
    pub fn sto(mut self, sto: Arc<MetaRaftStore>) -> Self {
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
        // TODO(xp): configure cluster name.

        let hb = config.heartbeat_interval;

        Config {
            cluster_name: "foo_cluster".to_string(),
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
    #[tracing::instrument(level = "info", skip(mn))]
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

        tracing::info!("about to start raft grpc on resolved addr {}", addr);

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
                tracing::info!(
                    "signal received, shutting down: id={} {} ",
                    node_id,
                    addr_str
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
    /// 3. If `init_cluster` is `Some` and it is just created, try to initialize a single-node cluster.
    ///
    /// TODO(xp): `init_cluster`: pass in a Map<id, address> to initialize the cluster.
    #[tracing::instrument(level = "info", skip(config), fields(config_id=config.config_id.as_str()))]
    pub async fn open_create_boot(
        config: &RaftConfig,
        open: Option<()>,
        create: Option<()>,
        init_cluster: Option<Vec<String>>,
    ) -> MetaResult<Arc<MetaNode>> {
        let mut config = config.clone();

        // Always disable fsync on mac.
        // Because there are some integration tests running on mac VM.
        //
        // On mac File::sync_all() takes 10 ms ~ 30 ms, 500 ms at worst, which very likely to fail a test.
        if cfg!(target_os = "macos") {
            tracing::warn!("Disabled fsync for meta data tests. fsync on mac is quite slow");
            config.no_sync = true;
        }

        let sto = MetaRaftStore::open_create(&config, open, create).await?;
        let is_open = sto.is_opened();
        let sto = Arc::new(sto);

        let mut builder = MetaNode::builder(&config).sto(sto.clone());
        // config.id only used for the first time
        let self_node_id = if is_open { sto.id } else { config.id };

        // use ip:port to start grpc listening
        builder = builder
            .node_id(self_node_id)
            .endpoint(config.raft_api_listen_host_endpoint());
        let mn = builder.build().await?;

        tracing::info!("MetaNode started: {:?}", config);

        // init_cluster with advertise_host other than listen_host
        if !is_open {
            if let Some(_addrs) = init_cluster {
                mn.init_cluster(config.raft_api_advertise_host_endpoint())
                    .await?;
            }
        }
        Ok(mn)
    }

    #[tracing::instrument(level = "info", skip(self))]
    pub async fn stop(&self) -> MetaResult<i32> {
        // TODO(xp): need to be reentrant.

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
            tracing::info!("waiting for raft to shutdown, metrics: {:?}", rx.borrow());
        }
        tracing::info!("shutdown raft");

        // raft counts 1
        let mut joined = 1;
        for j in self.join_handles.lock().await.iter_mut() {
            let _rst = j
                .await
                .map_error_to_meta_error(MetaError::MetaServiceError, || "fail to join")?;
            joined += 1;
        }

        tracing::info!("shutdown: id={}", self.sto.id);
        Ok(joined)
    }

    // spawn a monitor to watch raft state changes such as leader changes,
    // and manually add non-voter to cluster so that non-voter receives raft logs.
    pub async fn subscribe_metrics(mn: Arc<Self>, mut metrics_rx: watch::Receiver<RaftMetrics>) {
        //TODO: return a handle for join
        // TODO: every state change triggers add_non_voter!!!
        let mut running_rx = mn.running_rx.clone();
        let mut jh = mn.join_handles.lock().await;

        // TODO: reduce dependency: it does not need all of the fields in MetaNode
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
                            if let Some(cur) = mm.current_leader {
                                if cur == mn.sto.id {
                                    // TODO: check result
                                    let _rst = mn.add_configured_non_voters().await;

                                    if _rst.is_err() {
                                        tracing::info!(
                                            "fail to add non-voter: my id={}, rst:{:?}",
                                            mn.sto.id,
                                            _rst
                                        );
                                    }
                                }
                            }
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
    #[tracing::instrument(level = "info", skip(config))]
    pub async fn start(config: &RaftConfig) -> Result<Arc<MetaNode>, MetaError> {
        tracing::info!(?config, "start()");
        let mn = Self::do_start(config).await?;
        tracing::info!("Done starting MetaNode: {:?}", config);
        Ok(mn)
    }

    #[tracing::instrument(level = "info", skip(conf, self))]
    pub async fn join_cluster(&self, conf: &RaftConfig) -> MetaResult<()> {
        if conf.join.is_empty() {
            tracing::info!("--join config is empty");
            return Ok(());
        }

        // Try to join a cluster only when this node is just created.
        // Joining a node with log has risk messing up the data in this node and in the target cluster.
        if self.is_opened() {
            tracing::info!("has opened");
            return Ok(());
        }

        let addrs = &conf.join;
        // Join cluster use advertise host instead of listen host
        let raft_api_advertise_host_endpoint = conf.raft_api_advertise_host_endpoint();
        #[allow(clippy::never_loop)]
        for addr in addrs {
            tracing::info!("try to join cluster accross {}...", addr);

            let mut client = match RaftServiceClient::connect(format!("http://{}", addr)).await {
                Ok(c) => c,
                Err(e) => {
                    tracing::error!("connect to {} join cluster fail: {:?}", addr, e);
                    continue;
                }
            };

            let admin_req = ForwardRequest {
                forward_to_leader: 1,
                body: ForwardRequestBody::Join(JoinRequest {
                    node_id: conf.id,
                    endpoint: raft_api_advertise_host_endpoint.clone(),
                }),
            };

            let result: std::result::Result<RaftReply, Status> =
                match client.forward(admin_req.clone()).await {
                    Ok(r) => Ok(r.into_inner()),
                    Err(s) => {
                        tracing::error!("join cluster accross {} fail: {:?}", addr, s);
                        continue;
                    }
                };

            match result {
                Ok(reply) => {
                    if !reply.data.is_empty() {
                        tracing::info!("join cluster accross {} success: {:?}", addr, reply.data);
                        return Ok(());
                    } else {
                        tracing::error!("join cluster accross {} fail: {:?}", addr, reply.error);
                    }
                }
                Err(s) => {
                    tracing::error!("join cluster accross {} fail: {:?}", addr, s);
                }
            }
        }
        Err(
            MetaRaftError::JoinClusterFail(format!("join cluster accross addrs {:?} fail", addrs))
                .into(),
        )
    }

    async fn do_start(conf: &RaftConfig) -> Result<Arc<MetaNode>, MetaError> {
        if conf.single {
            let mn = MetaNode::open_create_boot(conf, Some(()), Some(()), Some(vec![])).await?;
            return Ok(mn);
        }

        if !conf.join.is_empty() {
            // Bring up a new node, join it into a cluster
            let mn = MetaNode::open_create_boot(conf, Some(()), Some(()), None).await?;

            if mn.is_opened() {
                return Ok(mn);
            }
            return Ok(mn);
        }
        // open mode
        let mn = MetaNode::open_create_boot(conf, Some(()), None, None).await?;
        Ok(mn)
    }

    /// Boot up the first node to create a cluster.
    /// For every cluster this func should be called exactly once.
    #[tracing::instrument(level = "info", skip(config), fields(config_id=config.config_id.as_str()))]
    pub async fn boot(config: &RaftConfig) -> MetaResult<Arc<MetaNode>> {
        // 1. Bring a node up as non voter, start the grpc service for raft communication.
        // 2. Initialize itself as leader, because it is the only one in the new cluster.
        // 3. Add itself to the cluster storage by committing an `add-node` log so that the cluster members(only this node) is persisted.

        let mn = Self::open_create_boot(config, None, Some(()), Some(vec![])).await?;

        Ok(mn)
    }

    // Initialized a single node cluster by:
    // - Initializing raft membership.
    // - Adding current node into the meta data.
    #[tracing::instrument(level = "info", skip(self))]
    pub async fn init_cluster(&self, endpoint: Endpoint) -> MetaResult<()> {
        let node_id = self.sto.id;

        let mut cluster_node_ids = BTreeSet::new();
        cluster_node_ids.insert(node_id);

        let rst = self
            .raft
            .initialize(cluster_node_ids)
            .await
            .map_err(|x| MetaError::MetaServiceError(format!("{:?}", x)))?;

        tracing::info!("initialized cluster, rst: {:?}", rst);

        self.add_node(node_id, endpoint).await?;

        Ok(())
    }

    /// When a leader is established, it is the leader's responsibility to setup replication from itself to non-voters, AKA learners.
    /// openraft does not persist the node set of non-voters, thus we need to do it manually.
    /// This fn should be called once a node found it becomes leader.
    #[tracing::instrument(level = "info", skip(self))]
    pub async fn add_configured_non_voters(&self) -> MetaResult<()> {
        // TODO after leader established, add non-voter through apis
        let node_ids = self.sto.list_non_voters().await;
        for i in node_ids.iter() {
            let x = self.raft.add_learner(*i, true).await;

            tracing::info!("add_non_voter result: {:?}", x);
            if x.is_ok() {
                tracing::info!("non-voter is added: {}", i);
            } else {
                tracing::info!("non-voter already exist: {}", i);
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
    pub async fn consistent_read<Request, Reply>(&self, req: Request) -> Result<Reply, MetaError>
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
            .await?;

        let res: Reply = res.try_into().map_err(|e| {
            MetaRaftError::ConsistentReadError(format!("consistent read recv invalid reply: {}", e))
        })?;

        Ok(res)
    }

    #[tracing::instrument(level = "debug", skip(self, req), fields(target=%req.forward_to_leader))]
    pub async fn handle_forwardable_request(
        &self,
        req: ForwardRequest,
    ) -> Result<ForwardResponse, MetaError> {
        tracing::debug!("handle_forwardable_request: {:?}", req);

        let forward = req.forward_to_leader;

        let l = self.as_leader().await;
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
        if curr_leader == self.sto.id {
            return Ok(MetaLeader::new(self));
        }
        Err(ForwardToLeader {
            leader_id: Some(curr_leader),
        })
    }

    /// Add a new node into this cluster.
    /// The node info is committed with raft, thus it must be called on an initialized node.
    #[tracing::instrument(level = "debug", skip(self))]
    pub async fn add_node(
        &self,
        node_id: NodeId,
        endpoint: Endpoint,
    ) -> Result<AppliedState, MetaError> {
        // TODO: use txid?
        let resp = self
            .write(LogEntry {
                txid: None,
                cmd: Cmd::AddNode {
                    node_id,
                    node: Node {
                        name: node_id.to_string(),
                        endpoint,
                    },
                },
            })
            .await?;
        Ok(resp)
    }

    pub async fn get_state_machine(&self) -> RwLockReadGuard<'_, StateMachine> {
        self.sto.state_machine.read().await
    }

    #[tracing::instrument(level = "debug", skip(self))]
    pub async fn lookup_table_id(
        &self,
        db_id: u64,
        name: &str,
    ) -> Result<Option<SeqV<TableLookupValue>>, MetaError> {
        // inconsistent get: from local state machine

        let sm = self.sto.state_machine.read().await;
        match sm.table_lookup().get(
            &(TableLookupKey {
                database_id: db_id,
                table_name: name.to_string(),
            }),
        ) {
            Ok(e) => Ok(e),
            Err(e) => Err(e.into()),
        }
    }

    #[tracing::instrument(level = "debug", skip(self))]
    pub async fn list_tables(&self, req: ListTableReq) -> Result<Vec<Arc<TableInfo>>, MetaError> {
        // inconsistent get: from local state machine

        let sm = self.sto.state_machine.read().await;
        sm.list_tables(req).await
    }

    #[tracing::instrument(level = "debug", skip(self))]
    pub async fn get_table_by_id(&self, tid: &u64) -> Result<Option<SeqV<TableMeta>>, MetaError> {
        // inconsistent get: from local state machine

        let sm = self.sto.state_machine.read().await;
        sm.get_table_meta_by_id(tid)
    }

    /// Submit a write request to the known leader. Returns the response after applying the request.
    #[tracing::instrument(level = "info", skip(self, req))]
    pub async fn write(&self, req: LogEntry) -> Result<AppliedState, MetaError> {
        tracing::debug!("req: {:?}", req);

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
    #[tracing::instrument(level = "info", skip(self))]
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
                tracing::info!("raft metrics tx closed");
                return 0;
            }
        }
    }

    #[tracing::instrument(level = "info", skip(self))]
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

        let resp = client
            .forward(req)
            .await
            .map_err(|e| MetaRaftError::ForwardRequestError(e.to_string()))?;
        let raft_mes = resp.into_inner();

        let res: Result<ForwardResponse, MetaError> = raft_mes.into();
        res
    }
}
