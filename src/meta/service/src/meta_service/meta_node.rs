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

use std::collections::BTreeSet;
use std::net::Ipv4Addr;
use std::sync::atomic::AtomicI32;
use std::sync::Arc;
use std::time::Duration;

use anyerror::AnyError;
use databend_common_base::base::tokio;
use databend_common_base::base::tokio::sync::watch;
use databend_common_base::base::tokio::sync::watch::error::RecvError;
use databend_common_base::base::tokio::sync::Mutex;
use databend_common_base::base::tokio::task::JoinHandle;
use databend_common_base::base::tokio::time::sleep;
use databend_common_base::base::tokio::time::Instant;
use databend_common_grpc::ConnectionFactory;
use databend_common_grpc::DNSResolver;
use databend_common_meta_client::reply_to_api_result;
use databend_common_meta_client::RequestFor;
use databend_common_meta_raft_store::config::RaftConfig;
use databend_common_meta_raft_store::ondisk::DATA_VERSION;
use databend_common_meta_raft_store::raft_log_v004::RaftLogStat;
use databend_common_meta_sled_store::openraft;
use databend_common_meta_sled_store::openraft::error::RaftError;
use databend_common_meta_sled_store::openraft::ChangeMembers;
use databend_common_meta_stoerr::MetaStorageError;
use databend_common_meta_types::protobuf::raft_service_client::RaftServiceClient;
use databend_common_meta_types::protobuf::raft_service_server::RaftServiceServer;
use databend_common_meta_types::protobuf::WatchRequest;
use databend_common_meta_types::protobuf::WatchResponse;
use databend_common_meta_types::raft_types::CommittedLeaderId;
use databend_common_meta_types::raft_types::ForwardToLeader;
use databend_common_meta_types::raft_types::InitializeError;
use databend_common_meta_types::raft_types::LogId;
use databend_common_meta_types::raft_types::MembershipNode;
use databend_common_meta_types::raft_types::NodeId;
use databend_common_meta_types::raft_types::RaftMetrics;
use databend_common_meta_types::raft_types::TypeConfig;
use databend_common_meta_types::AppliedState;
use databend_common_meta_types::Cmd;
use databend_common_meta_types::Endpoint;
use databend_common_meta_types::ForwardRPCError;
use databend_common_meta_types::GrpcConfig;
use databend_common_meta_types::LogEntry;
use databend_common_meta_types::MetaAPIError;
use databend_common_meta_types::MetaError;
use databend_common_meta_types::MetaManagementError;
use databend_common_meta_types::MetaNetworkError;
use databend_common_meta_types::MetaOperationError;
use databend_common_meta_types::MetaStartupError;
use databend_common_meta_types::Node;
use fastrace::func_name;
use fastrace::prelude::*;
use itertools::Itertools;
use log::debug;
use log::error;
use log::info;
use log::warn;
use maplit::btreemap;
use openraft::Config;
use openraft::Raft;
use openraft::ServerState;
use openraft::SnapshotPolicy;
use tokio::sync::mpsc;
use tonic::Status;

use crate::configs::Config as MetaConfig;
use crate::message::ForwardRequest;
use crate::message::ForwardRequestBody;
use crate::message::ForwardResponse;
use crate::message::JoinRequest;
use crate::message::LeaveRequest;
use crate::meta_service::errors::grpc_error_to_network_err;
use crate::meta_service::forwarder::MetaForwarder;
use crate::meta_service::meta_leader::MetaLeader;
use crate::meta_service::meta_node_status::MetaNodeStatus;
use crate::meta_service::RaftServiceImpl;
use crate::metrics::server_metrics;
use crate::network::NetworkFactory;
use crate::request_handling::Forwarder;
use crate::request_handling::Handler;
use crate::store::RaftStore;
use crate::version::METASRV_COMMIT_VERSION;
use crate::watcher::dispatch::Dispatcher;
use crate::watcher::dispatch::DispatcherHandle;
use crate::watcher::watch_stream::WatchStreamSender;

pub type LogStore = RaftStore;
pub type SMStore = RaftStore;

/// MetaRaft is an implementation of the generic Raft handling metadata R/W.
pub type MetaRaft = Raft<TypeConfig>;

/// MetaNode is the container of metadata related components and threads, such as storage, the raft node and a raft-state monitor.
pub struct MetaNode {
    pub raft_store: RaftStore,
    pub dispatcher_handle: DispatcherHandle,
    pub raft: MetaRaft,
    pub running_tx: watch::Sender<()>,
    pub running_rx: watch::Receiver<()>,
    pub join_handles: Mutex<Vec<JoinHandle<Result<(), AnyError>>>>,
    pub joined_tasks: AtomicI32,
}

impl Drop for MetaNode {
    fn drop(&mut self) {
        info!(
            "MetaNode(id={}, raft={}) is dropping",
            self.raft_store.id,
            self.raft_store.config.raft_api_advertise_host_string()
        );
    }
}

pub struct MetaNodeBuilder {
    node_id: Option<NodeId>,
    raft_config: Option<Config>,
    sto: Option<RaftStore>,
    raft_service_endpoint: Option<Endpoint>,
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

        let net = NetworkFactory::new(sto.clone());

        let log_store = sto.clone();
        let sm_store = sto.clone();

        let raft = MetaRaft::new(node_id, Arc::new(config), net, log_store, sm_store)
            .await
            .map_err(|e| MetaStartupError::MetaServiceError(e.to_string()))?;

        let (tx, rx) = watch::channel::<()>(());

        let handle = Dispatcher::spawn();

        sto.get_state_machine()
            .await
            .set_event_sender(Box::new(handle.clone()));

        let meta_node = Arc::new(MetaNode {
            raft_store: sto.clone(),
            dispatcher_handle: handle,
            raft: raft.clone(),
            running_tx: tx,
            running_rx: rx,
            join_handles: Mutex::new(Vec::new()),
            joined_tasks: AtomicI32::new(1),
        });

        MetaNode::subscribe_metrics(meta_node.clone(), raft.metrics()).await;

        let endpoint = if let Some(a) = self.raft_service_endpoint.take() {
            a
        } else {
            sto.get_node_raft_endpoint(&node_id).await.map_err(|e| {
                MetaStartupError::InvalidConfig(format!(
                    "endpoint of node: {} is not configured and is not in store, error: {}",
                    node_id, e,
                ))
            })?
        };

        MetaNode::start_raft_service(meta_node.clone(), &endpoint).await?;

        Ok(meta_node)
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
    pub fn raft_service_endpoint(mut self, endpoint: Endpoint) -> Self {
        self.raft_service_endpoint = Some(endpoint);
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
            raft_service_endpoint: None,
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
            snapshot_max_chunk_size: config.snapshot_chunk_size,
            ..Default::default()
        }
        .validate()
        .expect("building raft Config from databend-metasrv config")
    }

    /// Start the grpc service for raft communication and meta operation API.
    #[fastrace::trace]
    pub async fn start_raft_service(
        meta_node: Arc<MetaNode>,
        endpoint: &Endpoint,
    ) -> Result<(), MetaNetworkError> {
        info!("Start raft service listening on: {}", endpoint);

        let host = endpoint.addr();
        let port = endpoint.port();

        let mut running_rx = meta_node.running_rx.clone();

        let raft_service_impl = RaftServiceImpl::create(meta_node.clone());
        let raft_server = RaftServiceServer::new(raft_service_impl)
            .max_decoding_message_size(GrpcConfig::MAX_DECODING_SIZE)
            .max_encoding_message_size(GrpcConfig::MAX_ENCODING_SIZE);

        let ipv4_addr = host.parse::<Ipv4Addr>();
        let ip_port = match ipv4_addr {
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

        info!("about to start raft grpc on: {}", ip_port);

        let socket_addr = ip_port.parse::<std::net::SocketAddr>()?;
        let node_id = meta_node.raft_store.id;

        let srv = tonic::transport::Server::builder().add_service(raft_server);

        let h = databend_common_base::runtime::spawn(async move {
            srv.serve_with_shutdown(socket_addr, async move {
                let _ = running_rx.changed().await;
                info!(
                    "running_rx for Raft server received, shutting down: id={} {} ",
                    node_id, ip_port
                );
            })
            .await
            .map_err(|e| {
                AnyError::new(&e).add_context(|| "when serving meta-service raft service")
            })?;

            Ok::<(), AnyError>(())
        });

        let mut jh = meta_node.join_handles.lock().await;
        jh.push(h);
        Ok(())
    }

    /// Open or create a meta node.
    #[fastrace::trace]
    pub async fn open(config: &RaftConfig) -> Result<Arc<MetaNode>, MetaStartupError> {
        info!("MetaNode::open, config: {:?}", config);

        let mut config = config.clone();

        // Always disable fsync on mac.
        // Because there are some integration tests running on mac VM.
        //
        // On mac File::sync_all() takes 10 ms ~ 30 ms, 500 ms at worst, which very likely to fail a test.
        if cfg!(target_os = "macos") {
            warn!("Disabled fsync for meta data tests. fsync on mac is quite slow");
            config.no_sync = true;
        }

        let log_store = RaftStore::open(&config).await?;

        // config.id only used for the first time
        let self_node_id = if log_store.is_opened {
            log_store.id
        } else {
            config.id
        };

        let builder = MetaNode::builder(&config)
            .sto(log_store.clone())
            .node_id(self_node_id)
            .raft_service_endpoint(config.raft_api_listen_host_endpoint());
        let mn = builder.build().await?;

        info!("MetaNode started: {:?}", config);

        Ok(mn)
    }

    /// Open or create a metasrv node.
    ///
    /// Optionally boot a single node cluster.
    /// If `initialize_cluster` is `Some`, initialize the cluster as a single-node cluster.
    #[fastrace::trace]
    pub async fn open_boot(
        config: &RaftConfig,
        initialize_cluster: Option<Node>,
    ) -> Result<Arc<MetaNode>, MetaStartupError> {
        let mn = Self::open(config).await?;

        if let Some(node) = initialize_cluster {
            mn.init_cluster(node).await?;
        }
        Ok(mn)
    }

    #[fastrace::trace]
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

        info!("shutdown: id={}", self.raft_store.id);
        let joined = self.joined_tasks.load(std::sync::atomic::Ordering::Relaxed);
        Ok(joined)
    }

    /// Spawn a monitor to watch raft state changes and report metrics changes.
    pub async fn subscribe_metrics(mn: Arc<Self>, mut metrics_rx: watch::Receiver<RaftMetrics>) {
        info!("Start a task subscribing raft metrics and forward to metrics API");
        let meta_node = mn.clone();

        let fut = async move {
            let mut last_leader: Option<u64> = None;

            loop {
                let changed = metrics_rx.changed().await;

                if let Err(changed_err) = changed {
                    // Shutting down.
                    info!(
                        "{}; when:(watching metrics_rx); quit subscribe_metrics() loop",
                        changed_err
                    );
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
                server_metrics::set_is_leader(mm.current_leader == Some(meta_node.raft_store.id));

                // metrics about raft log and state machine.
                server_metrics::set_current_term(mm.current_term);
                server_metrics::set_last_log_index(mm.last_log_index.unwrap_or_default());
                server_metrics::set_proposals_applied(mm.last_applied.unwrap_or_default().index);
                server_metrics::set_last_seq(meta_node.get_last_seq().await);

                {
                    let st = meta_node.get_raft_log_stat().await;
                    server_metrics::set_raft_log_stat(st);
                }

                // metrics about server storage
                server_metrics::set_raft_log_size(meta_node.get_raft_log_size().await);
                server_metrics::set_snapshot_key_count(meta_node.get_snapshot_key_count().await);

                last_leader = mm.current_leader;
            }

            Ok::<(), AnyError>(())
        };
        let h = databend_common_base::runtime::spawn(
            fut.in_span(Span::enter_with_local_parent("watch-metrics")),
        );

        {
            let mut jh = mn.join_handles.lock().await;
            jh.push(h);
        }
    }

    /// Start MetaNode in either `boot`, `single`, `join` or `open` mode,
    /// according to config.
    #[fastrace::trace]
    pub async fn start(config: &MetaConfig) -> Result<Arc<MetaNode>, MetaStartupError> {
        info!(config :? =(config); "start()");
        let mn = Self::do_start(config).await?;
        info!("Done starting MetaNode: {:?}", config);
        Ok(mn)
    }

    /// Leave the cluster if `--leave` is specified.
    ///
    /// Return whether it has left the cluster.
    #[fastrace::trace]
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
                        errors.push(
                            AnyError::error(reply.error).add_context(|| {
                                format!("leave {} via: {}", leave_id, addr.clone())
                            }),
                        );
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
    #[fastrace::trace]
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

    #[fastrace::trace]
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
            "fail to join node-{} to cluster via {:?}, errors: {}",
            self.raft_store.id,
            addrs,
            errors.into_iter().map(|e| e.to_string()).join(", ")
        ))))
    }

    #[fastrace::trace]
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
        let membership = {
            let sm = self.raft_store.get_state_machine().await;
            sm.sys_data_ref().last_membership_ref().membership().clone()
        };
        info!("is_in_cluster: membership: {:?}", membership);

        let voter_ids = membership.voter_ids().collect::<BTreeSet<_>>();

        if voter_ids.contains(&self.raft_store.id) {
            return Ok(Ok(format!(
                "node {} already in cluster",
                self.raft_store.id
            )));
        }

        Ok(Err(format!(
            "node {} has membership but not in it",
            self.raft_store.id
        )))
    }

    async fn do_start(conf: &MetaConfig) -> Result<Arc<MetaNode>, MetaStartupError> {
        let raft_conf = &conf.raft_config;

        if raft_conf.single {
            let mn = MetaNode::open(raft_conf).await?;
            mn.init_cluster(conf.get_node()).await?;
            return Ok(mn);
        }

        let mn = MetaNode::open(raft_conf).await?;
        Ok(mn)
    }

    /// Boot up the first node to create a cluster.
    /// For every cluster this func should be called exactly once.
    #[fastrace::trace]
    pub async fn boot(config: &MetaConfig) -> Result<Arc<MetaNode>, MetaStartupError> {
        let mn = Self::open(&config.raft_config).await?;
        mn.init_cluster(config.get_node()).await?;
        Ok(mn)
    }

    /// Initialized a single node cluster if this node is just created:
    /// - Initializing raft membership.
    /// - Adding current node into the meta data.
    #[fastrace::trace]
    pub async fn init_cluster(&self, node: Node) -> Result<(), MetaStartupError> {
        info!("Initialize node as single node cluster: {:?}", node);

        let node_id = self.raft_store.id;

        let mut cluster_node_ids = BTreeSet::new();
        cluster_node_ids.insert(node_id);

        // initialize() and add_node() are not done atomically.
        // There is an issue that just after initializing the cluster,
        // the node will be used but no node info is found.
        // Thus, meta-server can only be initialized with a single node.
        //
        // We do not store node info in membership config,
        // because every start a meta-server node updates its latest configured address.
        let res = self.raft.initialize(cluster_node_ids.clone()).await;
        match res {
            Ok(_) => {
                info!("Initialized with: {:?}", cluster_node_ids);
            }
            Err(e) => match e {
                RaftError::APIError(e) => match e {
                    InitializeError::NotAllowed(e) => {
                        info!("Already initialized: {}", e);
                    }
                    InitializeError::NotInMembers(e) => {
                        return Err(MetaStartupError::InvalidConfig(e.to_string()));
                    }
                },
                RaftError::Fatal(fatal) => {
                    return Err(MetaStartupError::MetaServiceError(fatal.to_string()));
                }
            },
        }

        if self.get_node(&node_id).await.is_none() {
            info!(
                "This node not found in state-machine; add node: {}:{:?}",
                node_id, node
            );
            self.add_node(node_id, node.clone()).await.map_err(|e| {
                MetaStartupError::AddNodeError {
                    source: AnyError::new(&e),
                }
            })?;
        } else {
            info!("This node already in state-machine; No need to add");
        }

        info!("Done initializing node as single node cluster: {:?}", node);

        Ok(())
    }

    #[fastrace::trace]
    pub async fn get_node(&self, node_id: &NodeId) -> Option<Node> {
        // inconsistent get: from local state machine

        let sm = self.raft_store.state_machine.read().await;
        let n = sm.sys_data_ref().nodes_ref().get(node_id).cloned();
        n
    }

    #[fastrace::trace]
    pub async fn get_nodes(&self) -> Vec<Node> {
        // inconsistent get: from local state machine

        let sm = self.raft_store.state_machine.read().await;
        let nodes = sm
            .sys_data_ref()
            .nodes_ref()
            .values()
            .cloned()
            .collect::<Vec<_>>();
        nodes
    }

    /// Get the size in bytes of the on disk files of the raft log storage.
    async fn get_raft_log_size(&self) -> u64 {
        self.raft_store.log.read().await.on_disk_size()
    }

    async fn get_raft_log_stat(&self) -> RaftLogStat {
        self.raft_store.log.read().await.stat()
    }

    async fn get_snapshot_key_count(&self) -> u64 {
        self.raft_store
            .try_get_snapshot_key_count()
            .await
            .unwrap_or_default()
    }

    pub async fn get_status(&self) -> Result<MetaNodeStatus, MetaError> {
        let voters = self
            .raft_store
            .get_nodes(|ms| ms.voter_ids().collect::<Vec<_>>())
            .await;

        let learners = self
            .raft_store
            .get_nodes(|ms| ms.learner_ids().collect::<Vec<_>>())
            .await;

        let endpoint = self
            .raft_store
            .get_node_raft_endpoint(&self.raft_store.id)
            .await?;

        let raft_log_status = self.get_raft_log_stat().await.into();
        let snapshot_key_count = self.get_snapshot_key_count().await;

        let metrics = self.raft.metrics().borrow().clone();

        let leader = if let Some(leader_id) = metrics.current_leader {
            self.get_node(&leader_id).await
        } else {
            None
        };

        let last_seq = self.get_last_seq().await;

        Ok(MetaNodeStatus {
            id: self.raft_store.id,
            binary_version: METASRV_COMMIT_VERSION.as_str().to_string(),
            data_version: DATA_VERSION,
            endpoint: endpoint.to_string(),
            raft_log: raft_log_status,
            snapshot_key_count,
            state: format!("{:?}", metrics.state),
            is_leader: metrics.state == openraft::ServerState::Leader,
            current_term: metrics.current_term,
            last_log_index: metrics.last_log_index.unwrap_or(0),
            last_applied: metrics
                .last_applied
                .unwrap_or(LogId::new(CommittedLeaderId::new(0, 0), 0)),
            snapshot_last_log_id: metrics.snapshot,
            purged: metrics.purged,
            leader,
            replication: metrics.replication,
            voters,
            non_voters: learners,
            last_seq,
        })
    }

    pub(crate) async fn get_last_seq(&self) -> u64 {
        let sm = self.raft_store.state_machine.read().await;
        sm.sys_data_ref().curr_seq()
    }

    #[fastrace::trace]
    pub async fn get_grpc_advertise_addrs(&self) -> Vec<String> {
        // Maybe stale get: from local state machine

        let nodes = {
            let sm = self.raft_store.state_machine.read().await;
            sm.sys_data_ref()
                .nodes_ref()
                .values()
                .cloned()
                .collect::<Vec<_>>()
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
        endpoints
    }

    #[fastrace::trace]
    pub async fn handle_forwardable_request<Req>(
        &self,
        req: ForwardRequest<Req>,
    ) -> Result<(Option<Endpoint>, Req::Reply), MetaAPIError>
    where
        Req: RequestFor,
        for<'a> MetaLeader<'a>: Handler<Req>,
        for<'a> MetaForwarder<'a>: Forwarder<Req>,
    {
        debug!(target :% =(&req.forward_to_leader),
               req :? =(&req);
               "handle_forwardable_request");

        let mut n_retry = 20;
        let mut slp = Duration::from_millis(200);

        loop {
            let assume_leader_res = self.assume_leader().await;
            debug!("assume_leader: is_err: {}", assume_leader_res.is_err());

            // Handle the request locally or return a ForwardToLeader error
            let op_err = match assume_leader_res {
                Ok(leader) => {
                    let res = leader.handle(req.clone()).await;
                    match res {
                        Ok(x) => return Ok((None, x)),
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

            let leader_id = to_leader.leader_id.ok_or_else(|| {
                MetaAPIError::CanNotForward(AnyError::error("need to forward but no known leader"))
            })?;

            let req_cloned = req.next()?;

            let f = MetaForwarder::new(self);
            let res = f.forward(leader_id, req_cloned).await;

            let forward_err = match res {
                Ok((_leader_raft_endpoint, reply)) => {
                    let leader_grpc_endpoint = self
                        .get_node(&leader_id)
                        .await
                        .and_then(|node| node.grpc_api_advertise_address)
                        .and_then(|leader_grpc_address| {
                            let endpoint_res = Endpoint::parse(&leader_grpc_address);

                            match endpoint_res {
                                Ok(o) => Some(o),
                                Err(e) => {
                                    error!(
                                        "fail to parse leader_grpc_address: {}; error: {}",
                                        &leader_grpc_address, e
                                    );
                                    None
                                }
                            }
                        });

                    return Ok((leader_grpc_endpoint, reply));
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
    #[fastrace::trace]
    pub async fn assume_leader(&self) -> Result<MetaLeader<'_>, ForwardToLeader> {
        let leader_id = self.get_leader().await.map_err(|e| {
            error!("raft metrics rx closed: {}", e);
            ForwardToLeader {
                leader_id: None,
                leader_node: None,
            }
        })?;

        debug!("curr_leader_id: {:?}", leader_id);

        if leader_id == Some(self.raft_store.id) {
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
    #[fastrace::trace]
    pub async fn write(&self, req: LogEntry) -> Result<AppliedState, MetaAPIError> {
        debug!("{} req: {:?}", func_name!(), req);

        // TODO: enable returning endpoint
        let (_endpoint, res) = self
            .handle_forwardable_request(ForwardRequest::new(
                1,
                ForwardRequestBody::Write(req.clone()),
            ))
            .await?;

        let res: AppliedState = res.try_into().expect("expect AppliedState");

        Ok(res)
    }

    /// Try to get the leader from the latest metrics of the local raft node.
    /// If leader is absent, wait for an metrics update in which a leader is set.
    #[fastrace::trace]
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

    pub(crate) async fn add_watcher(
        &self,
        request: WatchRequest,
        tx: mpsc::Sender<Result<WatchResponse, Status>>,
    ) -> Result<Arc<WatchStreamSender>, Status> {
        let stream_sender = self
            .dispatcher_handle
            .request_blocking(|d: &mut Dispatcher| d.add_watcher(request, tx))
            .await
            .map_err(|_e| Status::internal("watch-event-Dispatcher closed"))?
            .map_err(Status::invalid_argument)?;

        Ok(stream_sender)
    }
}
