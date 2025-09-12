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
use std::net::Ipv4Addr;
use std::sync::atomic::AtomicI32;
use std::sync::Arc;
use std::sync::Weak;
use std::time::Duration;

use anyerror::AnyError;
use databend_common_base::base::tokio;
use databend_common_base::base::tokio::sync::watch;
use databend_common_base::base::tokio::sync::watch::error::RecvError;
use databend_common_base::base::tokio::sync::Mutex;
use databend_common_base::base::tokio::task::JoinHandle;
use databend_common_base::base::tokio::time::sleep;
use databend_common_base::base::tokio::time::Instant;
use databend_common_base::base::BuildInfoRef;
use databend_common_grpc::ConnectionFactory;
use databend_common_grpc::DNSResolver;
use databend_common_meta_client::reply_to_api_result;
use databend_common_meta_client::RequestFor;
use databend_common_meta_raft_store::config::RaftConfig;
use databend_common_meta_raft_store::ondisk::DATA_VERSION;
use databend_common_meta_raft_store::raft_log_v004::RaftLogStat;
use databend_common_meta_raft_store::StateMachineFeature;
use databend_common_meta_sled_store::openraft;
use databend_common_meta_sled_store::openraft::error::RaftError;
use databend_common_meta_sled_store::openraft::ChangeMembers;
use databend_common_meta_stoerr::MetaStorageError;
use databend_common_meta_types::node::Node;
use databend_common_meta_types::protobuf::raft_service_client::RaftServiceClient;
use databend_common_meta_types::protobuf::raft_service_server::RaftServiceServer;
use databend_common_meta_types::protobuf::watch_request::FilterType;
use databend_common_meta_types::protobuf::WatchRequest;
use databend_common_meta_types::protobuf::WatchResponse;
use databend_common_meta_types::raft_types::new_log_id;
use databend_common_meta_types::raft_types::ForwardToLeader;
use databend_common_meta_types::raft_types::InitializeError;
use databend_common_meta_types::raft_types::MembershipNode;
use databend_common_meta_types::raft_types::NodeId;
use databend_common_meta_types::raft_types::RaftMetrics;
use databend_common_meta_types::raft_types::TypeConfig;
use databend_common_meta_types::snapshot_db::DBStat;
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
use watcher::dispatch::Dispatcher;
use watcher::key_range::build_key_range;
use watcher::watch_stream::WatchStreamSender;
use watcher::EventFilter;

use crate::configs::Config as MetaConfig;
use crate::message::ForwardRequest;
use crate::message::ForwardRequestBody;
use crate::message::ForwardResponse;
use crate::message::JoinRequest;
use crate::message::LeaveRequest;
use crate::meta_service::errors::grpc_error_to_network_err;
use crate::meta_service::forwarder::MetaForwarder;
use crate::meta_service::meta_leader::MetaLeader;
use crate::meta_service::meta_node_kv_api_impl::MetaKVApi;
use crate::meta_service::meta_node_kv_api_impl::MetaKVApiOwned;
use crate::meta_service::meta_node_status::MetaNodeStatus;
use crate::meta_service::runtime_config::RuntimeConfig;
use crate::meta_service::watcher::DispatcherHandle;
use crate::meta_service::watcher::WatchTypes;
use crate::meta_service::RaftServiceImpl;
use crate::metrics::server_metrics;
use crate::network::NetworkFactory;
use crate::request_handling::Forwarder;
use crate::request_handling::Handler;
use crate::store::RaftStore;

pub type LogStore = RaftStore;
pub type SMStore = RaftStore;

/// MetaRaft is an implementation of the generic Raft handling metadata R/W.
pub type MetaRaft = Raft<TypeConfig>;

/// MetaNode is the container of metadata related components and threads, such as storage, the raft node and a raft-state monitor.
pub struct MetaNode {
    pub raft_store: RaftStore,
    /// MetaNode hold a strong reference to the dispatcher handle.
    ///
    /// Other components should keep a weak one.
    pub dispatcher_handle: Arc<DispatcherHandle>,
    pub raft: MetaRaft,
    pub runtime_config: RuntimeConfig,
    pub running_tx: watch::Sender<()>,
    pub running_rx: watch::Receiver<()>,
    pub join_handles: Mutex<Vec<JoinHandle<Result<(), AnyError>>>>,
    pub joined_tasks: AtomicI32,
    pub version: BuildInfoRef,
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
    version: Option<BuildInfoRef>,
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

        let version = self
            .version
            .ok_or_else(|| MetaStartupError::InvalidConfig(String::from("version is not set")))?;

        let net = NetworkFactory::new(sto.clone());

        let log_store = sto.clone();
        let sm_store = sto.clone();

        let raft = MetaRaft::new(node_id, Arc::new(config), net, log_store, sm_store)
            .await
            .map_err(|e| MetaStartupError::MetaServiceError(e.to_string()))?;

        let runtime_config = RuntimeConfig::default();

        let (tx, rx) = watch::channel::<()>(());

        let handle = Dispatcher::spawn();
        let handle = DispatcherHandle::new(handle, node_id);
        let handle = Arc::new(handle);

        let on_change_applied = {
            let h = handle.clone();
            let broadcast = runtime_config.broadcast_state_machine_changes.clone();
            move |change| {
                if broadcast.load(std::sync::atomic::Ordering::Relaxed) {
                    h.send_change(change)
                } else {
                    info!(
                        "broadcast_state_machine_changes is disabled, ignoring change: {:?}",
                        change
                    );
                }
            }
        };

        sto.state_machine()
            .set_on_change_applied(Box::new(on_change_applied));

        let meta_node = Arc::new(MetaNode {
            raft_store: sto.clone(),
            dispatcher_handle: handle,
            raft: raft.clone(),
            runtime_config,
            running_tx: tx,
            running_rx: rx,
            join_handles: Mutex::new(Vec::new()),
            joined_tasks: AtomicI32::new(1),
            version,
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

    #[must_use]
    pub fn version(mut self, version: BuildInfoRef) -> Self {
        self.version = Some(version);
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
            version: None,
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
            // Allow Leader to reset replication if a follower clears its log.
            // Usefull in a testing environment.
            allow_log_reversion: Some(true),
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
    pub async fn open(
        config: &RaftConfig,
        version: BuildInfoRef,
    ) -> Result<Arc<MetaNode>, MetaStartupError> {
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
            .raft_service_endpoint(config.raft_api_listen_host_endpoint())
            .version(version);
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
        version: BuildInfoRef,
    ) -> Result<Arc<MetaNode>, MetaStartupError> {
        let mn = Self::open(config, version).await?;

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
    pub async fn subscribe_metrics(mn: Arc<Self>, metrics_rx: watch::Receiver<RaftMetrics>) {
        info!("Start a task subscribing raft metrics and forward to metrics API");

        let fut = Self::report_metrics_loop(mn.clone(), metrics_rx);

        let h = databend_common_base::runtime::spawn(
            fut.in_span(Span::enter_with_local_parent("watch-metrics")),
        );

        {
            let mut jh = mn.join_handles.lock().await;
            jh.push(h);
        }
    }

    /// Report metrics changes periodically.
    async fn report_metrics_loop(
        meta_node: Arc<Self>,
        mut metrics_rx: watch::Receiver<RaftMetrics>,
    ) -> Result<(), AnyError> {
        const RATE_LIMIT_INTERVAL: Duration = Duration::from_millis(200);
        let mut last_leader: Option<u64> = None;

        loop {
            let loop_start = Instant::now();

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
            {
                let stat = meta_node.get_snapshot_key_space_stat().await;

                server_metrics::set_snapshot_primary_index_count(
                    stat.get("kv--").copied().unwrap_or_default(),
                );

                server_metrics::set_snapshot_expire_index_count(
                    stat.get("exp-").copied().unwrap_or_default(),
                )
            }

            {
                let db_stat = meta_node.get_snapshot_db_stat().await;
                let snapshot = server_metrics::snapshot();
                snapshot.block_count.set(db_stat.block_num as i64);
                snapshot.data_size.set(db_stat.data_size as i64);
                snapshot.index_size.set(db_stat.index_size as i64);
                snapshot.avg_block_size.set(db_stat.avg_block_size as i64);
                snapshot
                    .avg_keys_per_block
                    .set(db_stat.avg_keys_per_block as i64);
                snapshot.read_block.set(db_stat.read_block as i64);
                snapshot
                    .read_block_from_cache
                    .set(db_stat.read_block_from_cache as i64);
                snapshot
                    .read_block_from_disk
                    .set(db_stat.read_block_from_disk as i64);
            }

            last_leader = mm.current_leader;

            let metrics_str = crate::metrics::meta_metrics_to_prometheus_string();
            let parsed_metrics = Self::parse_metrics_to_json(&metrics_str);
            info!("metrics: {}", parsed_metrics);

            let elapsed = loop_start.elapsed();
            if elapsed < RATE_LIMIT_INTERVAL {
                let sleep_duration = RATE_LIMIT_INTERVAL - elapsed;
                sleep(sleep_duration).await;
            }
        }

        Ok(())
    }

    /// Parse metrics string and return structured JSON value.
    fn parse_metrics_to_json(metrics_str: &str) -> serde_json::Value {
        use std::collections::BTreeMap;

        use serde_json::Map;
        use serde_json::Value;

        let mut categories: BTreeMap<String, Map<String, Value>> = BTreeMap::new();
        let mut histograms: BTreeMap<String, BTreeMap<String, f64>> = BTreeMap::new();

        for line in metrics_str.lines() {
            let line = line.trim();

            if line.is_empty() || line.starts_with('#') {
                continue;
            }

            if line.starts_with("metasrv_meta_network_rpc_delay_seconds_bucket") {
                continue;
            }

            let line = if let Some(stripped) = line.strip_prefix("metasrv_") {
                stripped
            } else {
                continue;
            };

            let parts: Vec<&str> = line.splitn(2, ' ').collect();
            if parts.len() != 2 {
                continue;
            }

            let (metric_name, value_str) = (parts[0], parts[1]);
            let value: f64 = value_str.parse().unwrap_or(0.0);

            let (category, clean_name) =
                if let Some(stripped) = metric_name.strip_prefix("meta_network_") {
                    ("meta_network", stripped)
                } else if let Some(stripped) = metric_name.strip_prefix("raft_network_") {
                    ("raft_network", stripped)
                } else if let Some(stripped) = metric_name.strip_prefix("raft_storage_") {
                    ("raft_storage", stripped)
                } else if let Some(stripped) = metric_name.strip_prefix("server_") {
                    ("server", stripped)
                } else {
                    continue;
                };

            if clean_name.contains("_bucket") {
                if let Some((hist_key, le_value)) =
                    Self::parse_histogram_bucket(category, clean_name)
                {
                    histograms
                        .entry(hist_key)
                        .or_default()
                        .insert(le_value, value);
                }
                continue;
            }

            // Handle labeled and unlabeled metrics
            Self::handle_labeled_metric(category, clean_name, value, &mut categories);
        }

        // Process histogram buckets and convert them to percentiles
        // NOTE: hist_key format is "category.metric_name|sub_key" where:
        // - "category.metric_name" is the base histogram identifier
        // - "sub_key" is either "_default" (no labels) or "label1=value1,label2=value2" (with labels)
        // This encoding allows us to group buckets by their labels before converting to percentiles
        for (hist_key, buckets) in histograms {
            // Split the encoded key back into main key and sub-key
            // Example: "raft_network.append_sent_seconds|to=1" -> ("raft_network.append_sent_seconds", "to=1")
            if let Some((main_key, sub_key)) = hist_key.split_once('|') {
                // Parse the main key to extract category and metric name
                // Example: "raft_network.append_sent_seconds" -> ["raft_network", "append_sent_seconds"]
                let parts: Vec<&str> = main_key.splitn(2, '.').collect();
                if parts.len() == 2 {
                    let (category, metric_name) = (parts[0], parts[1]);

                    // Convert the histogram buckets to percentile arrays
                    if let Some(percentiles) = Self::convert_histogram_to_percentiles(buckets) {
                        let category_map = categories.entry(category.to_string()).or_default();

                        if sub_key == "_default" {
                            // Histogram has no additional labels (only 'le' labels)
                            // Store directly: raft_network.append_sent_seconds = [["p50", 0.001], ...]
                            category_map.insert(metric_name.to_string(), percentiles);
                        } else {
                            // Histogram has additional labels beyond 'le'
                            // Create nested structure: raft_network.append_sent_seconds.{to=1} = [["p50", 0.001], ...]
                            let metric_map = category_map
                                .entry(metric_name.to_string())
                                .or_insert_with(|| Value::Object(serde_json::Map::new()))
                                .as_object_mut()
                                .unwrap();
                            metric_map.insert(sub_key.to_string(), percentiles);
                        }
                    }
                }
            }
        }

        Value::Object(
            categories
                .into_iter()
                .map(|(k, v)| (k, Value::Object(v)))
                .collect(),
        )
    }

    /// Handle a labeled or unlabeled metric by organizing it into the categories structure.
    fn handle_labeled_metric(
        category: &str,
        clean_name: &str,
        value: f64,
        categories: &mut std::collections::BTreeMap<
            String,
            serde_json::Map<String, serde_json::Value>,
        >,
    ) {
        use serde_json::Value;

        if let Some((name_part, labels_part)) = clean_name.split_once('{') {
            if let Some(labels_str) = labels_part.strip_suffix('}') {
                // Parse labels and create sub-key
                let label_key = Self::parse_label_key(labels_str);

                let category_map = categories.entry(category.to_string()).or_default();
                let metric_map = category_map
                    .entry(name_part.to_string())
                    .or_insert_with(|| Value::Object(serde_json::Map::new()))
                    .as_object_mut()
                    .unwrap();
                metric_map.insert(label_key, Value::from(value));
            } else {
                // Malformed labels, treat as regular metric
                categories
                    .entry(category.to_string())
                    .or_default()
                    .insert(clean_name.to_string(), Value::from(value));
            }
        } else {
            // No labels, regular metric
            categories
                .entry(category.to_string())
                .or_default()
                .insert(clean_name.to_string(), Value::from(value));
        }
    }

    /// Parse label string and create a comma-separated key with quotes stripped.
    fn parse_label_key(labels_str: &str) -> String {
        labels_str
            .split(',')
            .map(|label_pair| {
                let label_pair = label_pair.trim();
                if let Some((key, value)) = label_pair.split_once('=') {
                    let cleaned_value = value
                        .strip_prefix('"')
                        .and_then(|v| v.strip_suffix('"'))
                        .unwrap_or(value);
                    format!("{}={}", key, cleaned_value)
                } else {
                    label_pair.to_string()
                }
            })
            .collect::<Vec<_>>()
            .join(",")
    }

    /// Parse a histogram bucket metric line and extract histogram key and le value.
    /// Returns None if not a valid histogram bucket or if le="+Inf".
    fn parse_histogram_bucket(category: &str, clean_name: &str) -> Option<(String, String)> {
        if let Some((base_name, bucket_part)) = clean_name.split_once("_bucket") {
            // Check if it has labels in curly braces
            if let Some(labels_part) = bucket_part.strip_prefix('{') {
                if let Some(labels_str) = labels_part.strip_suffix('}') {
                    let mut le_value = None;
                    let mut other_labels = Vec::new();

                    // Split labels by comma and extract le value and other labels
                    for label_pair in labels_str.split(',') {
                        let label_pair = label_pair.trim();
                        if let Some((key, value)) = label_pair.split_once('=') {
                            if key == "le" {
                                if let Some(quoted_value) =
                                    value.strip_prefix('"').and_then(|v| v.strip_suffix('"'))
                                {
                                    if quoted_value != "+Inf" {
                                        le_value = Some(quoted_value.to_string());
                                    }
                                }
                            } else {
                                // Strip quotes from other labels too for consistency
                                if let Some((key, value)) = label_pair.split_once('=') {
                                    let cleaned_value = value
                                        .strip_prefix('"')
                                        .and_then(|v| v.strip_suffix('"'))
                                        .unwrap_or(value);
                                    other_labels.push(format!("{}={}", key, cleaned_value));
                                } else {
                                    other_labels.push(label_pair.to_string());
                                }
                            }
                        }
                    }

                    if let Some(le_val) = le_value {
                        let hist_key = if other_labels.is_empty() {
                            format!("{}.{}", category, base_name)
                        } else {
                            // For histograms with other labels, create sub-key structure later
                            format!("{}.{}", category, base_name)
                        };

                        let sub_key = if other_labels.is_empty() {
                            "_default".to_string()
                        } else {
                            Self::parse_label_key(&other_labels.join(","))
                        };

                        return Some((format!("{}|{}", hist_key, sub_key), le_val));
                    }
                }
            }
        }
        None
    }

    /// Convert histogram bucket data to percentiles.
    /// Note: Prometheus histogram buckets contain cumulative counts (not individual bucket counts).
    fn convert_histogram_to_percentiles(
        buckets: std::collections::BTreeMap<String, f64>,
    ) -> Option<serde_json::Value> {
        use serde_json::Value;

        let mut sorted_buckets: Vec<(f64, f64)> = buckets
            .iter()
            .filter_map(|(le, cumulative_count)| {
                le.parse::<f64>()
                    .ok()
                    .map(|le_val| (le_val, *cumulative_count))
            })
            .collect();

        if sorted_buckets.is_empty() {
            return None;
        }

        sorted_buckets.sort_by(|a, b| a.0.partial_cmp(&b.0).unwrap());

        // The total count is the largest cumulative count (highest bucket)
        let total_count = sorted_buckets
            .iter()
            .map(|(_, count)| *count)
            .fold(0.0, f64::max);

        if total_count <= 0.0 {
            return None;
        }

        let mut percentiles = [None; 4];
        let thresholds = [(0.5, 0), (0.9, 1), (0.99, 2), (0.999, 3)];

        // Find the first bucket where cumulative count >= threshold
        for &(le, cumulative_count) in &sorted_buckets {
            for &(threshold_ratio, index) in &thresholds {
                let threshold = total_count * threshold_ratio;
                if cumulative_count >= threshold && percentiles[index].is_none() {
                    percentiles[index] = Some(le);
                }
            }
        }

        // Convert to simple array format: [["p50", 5.0], ["p90", 100.0]...]
        let result_values: Vec<Value> = [
            ("p50", percentiles[0]),
            ("p90", percentiles[1]),
            ("p99", percentiles[2]),
            ("p99.9", percentiles[3]),
        ]
        .iter()
        .filter_map(|(label, value)| value.map(|v| serde_json::json!([label, v])))
        .collect();

        if !result_values.is_empty() {
            Some(Value::Array(result_values))
        } else {
            None
        }
    }

    /// Start MetaNode in either `boot`, `single`, `join` or `open` mode,
    /// according to config.
    #[fastrace::trace]
    pub async fn start(
        config: &MetaConfig,
        version: BuildInfoRef,
    ) -> Result<Arc<MetaNode>, MetaStartupError> {
        info!(config :? =(config); "start()");
        let mn = Self::do_start(config, version).await?;
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
                        error!("join cluster via {} fail: {}", addr, e);
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
            let sm = &self.raft_store.state_machine();
            sm.sys_data().last_membership_ref().membership().clone()
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

    async fn do_start(
        conf: &MetaConfig,
        version: BuildInfoRef,
    ) -> Result<Arc<MetaNode>, MetaStartupError> {
        let raft_conf = &conf.raft_config;

        if raft_conf.single {
            let mn = MetaNode::open(raft_conf, version).await?;
            mn.init_cluster(conf.get_node()).await?;
            return Ok(mn);
        }

        let mn = MetaNode::open(raft_conf, version).await?;
        Ok(mn)
    }

    /// Boot up the first node to create a cluster.
    /// For every cluster this func should be called exactly once.
    #[fastrace::trace]
    pub async fn boot(
        config: &MetaConfig,
        version: BuildInfoRef,
    ) -> Result<Arc<MetaNode>, MetaStartupError> {
        let mn = Self::open(&config.raft_config, version).await?;
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

        let sm = self.raft_store.state_machine();
        let n = sm.sys_data().nodes_ref().get(node_id).cloned();
        n
    }

    #[fastrace::trace]
    pub async fn get_nodes(&self) -> Vec<Node> {
        // inconsistent get: from local state machine

        let sm = self.raft_store.state_machine();
        let nodes = sm
            .sys_data()
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

    async fn get_snapshot_key_space_stat(&self) -> BTreeMap<String, u64> {
        self.raft_store.get_snapshot_key_space_stat().await
    }

    async fn get_snapshot_db_stat(&self) -> DBStat {
        self.raft_store.get_snapshot_db_stat().await
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
        let snapshot_key_space_stat = self.get_snapshot_key_space_stat().await;

        let metrics = self.raft.metrics().borrow().clone();

        let leader = if let Some(leader_id) = metrics.current_leader {
            self.get_node(&leader_id).await
        } else {
            None
        };

        let last_seq = self.get_last_seq().await;

        Ok(MetaNodeStatus {
            id: self.raft_store.id,
            binary_version: self.version.semantic.to_string(),
            data_version: DATA_VERSION,
            endpoint: endpoint.to_string(),
            raft_log: raft_log_status,
            snapshot_key_count,
            snapshot_key_space_stat,
            state: format!("{:?}", metrics.state),
            is_leader: metrics.state == openraft::ServerState::Leader,
            current_term: metrics.current_term,
            last_log_index: metrics.last_log_index.unwrap_or(0),
            last_applied: metrics.last_applied.unwrap_or(new_log_id(0, 0, 0)),
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
        let sm = self.raft_store.state_machine();
        sm.sys_data().curr_seq()
    }

    #[fastrace::trace]
    pub async fn get_grpc_advertise_addrs(&self) -> Vec<String> {
        // Maybe stale get: from local state machine

        let nodes = {
            let sm = self.raft_store.state_machine();
            sm.sys_data()
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
        let mut slp = Duration::from_millis(1_000);

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

            // If it needs to forward, deal with it. Otherwise, return the unhandlable error.
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

    /// Propose a log entry to set a feature.
    pub async fn set_feature(
        &self,
        feature: StateMachineFeature,
        enable: bool,
    ) -> Result<(), MetaAPIError> {
        let cmd = Cmd::SetFeature {
            feature: feature.to_string(),
            enable,
        };

        self.write(LogEntry::new(cmd)).await?;
        Ok(())
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

    pub(crate) fn insert_watch_sender(
        &self,
        sender: Arc<WatchStreamSender<WatchTypes>>,
    ) -> Weak<WatchStreamSender<WatchTypes>> {
        let weak = Arc::downgrade(&sender);

        self.dispatcher_handle
            .request(move |dispatcher: &mut Dispatcher<WatchTypes>| {
                dispatcher.insert_watch_stream_sender(sender);
            });

        weak
    }

    pub(crate) fn new_watch_sender(
        &self,
        request: WatchRequest,
        tx: mpsc::Sender<Result<WatchResponse, Status>>,
    ) -> Result<Arc<WatchStreamSender<WatchTypes>>, Status> {
        let key_range = match build_key_range(&request.key, &request.key_end) {
            Ok(kr) => kr,
            Err(e) => return Err(Status::invalid_argument(e.to_string())),
        };

        let interested = event_filter_from_filter_type(request.filter_type());

        let sender = Dispatcher::new_watch_stream_sender(key_range.clone(), interested, tx);
        Ok(sender)
    }

    /// Get a kvapi::KVApi implementation.
    pub fn kv_api(&self) -> MetaKVApi {
        MetaKVApi::new(self)
    }

    pub fn kv_api_owned(self: &Arc<Self>) -> MetaKVApiOwned {
        MetaKVApiOwned::new(self.clone())
    }

    pub fn runtime_config(&self) -> &RuntimeConfig {
        &self.runtime_config
    }
}

pub(crate) fn event_filter_from_filter_type(filter_type: FilterType) -> EventFilter {
    match filter_type {
        FilterType::All => EventFilter::all(),
        FilterType::Update => EventFilter::update(),
        FilterType::Delete => EventFilter::delete(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_metrics_to_json() {
        let metrics_input = r#"
# This is a comment
metasrv_meta_network_recv_bytes_total 1234
metasrv_meta_network_req_failed_total 0
metasrv_meta_network_req_success_total 5
metasrv_meta_network_rpc_delay_ms_bucket{le="1.0"} 0
metasrv_meta_network_rpc_delay_ms_bucket{le="10.0"} 2
metasrv_meta_network_rpc_delay_ms_bucket{le="100.0"} 4
metasrv_meta_network_rpc_delay_ms_bucket{le="1000.0"} 5
metasrv_meta_network_rpc_delay_ms_bucket{le="+Inf"} 5
metasrv_meta_network_rpc_delay_seconds_bucket{le="1.0"} 0
metasrv_raft_network_active_peers{id="1",addr="127.0.0.1:29003"} 1
metasrv_raft_storage_snapshot_building 0
metasrv_server_current_term 1
metasrv_server_is_leader 1
non_metasrv_metric 123

        "#;

        let result = MetaNode::parse_metrics_to_json(metrics_input);

        // Verify expected categories are present
        assert!(result.get("meta_network").is_some());
        assert!(result.get("raft_network").is_some());
        assert!(result.get("raft_storage").is_some());
        assert!(result.get("server").is_some());

        // Verify some expected metrics
        let meta_network = result.get("meta_network").unwrap();
        assert!(meta_network.get("recv_bytes_total").is_some());
        assert!(meta_network.get("req_success_total").is_some());
        assert!(meta_network.get("rpc_delay_ms").is_some()); // Should be converted to percentiles

        let server = result.get("server").unwrap();
        assert!(server.get("current_term").is_some());
        assert!(server.get("is_leader").is_some());
    }

    #[test]
    fn test_parse_metrics_empty_input() {
        let result = MetaNode::parse_metrics_to_json("");

        // Should return empty JSON object
        assert!(result.is_object());
        assert_eq!(result.as_object().unwrap().len(), 0);
    }

    #[test]
    fn test_parse_metrics_comments_only() {
        let metrics_input = r#"
# Comment line 1
# Comment line 2
        "#;

        let result = MetaNode::parse_metrics_to_json(metrics_input);

        // Should return empty JSON object for comments only
        assert!(result.is_object());
        assert_eq!(result.as_object().unwrap().len(), 0);
    }

    #[test]
    fn test_parse_metrics_malformed_lines() {
        let metrics_input = r#"
metasrv_server_current_term 1
malformed_line_without_value
metasrv_meta_network_invalid_value abc
metasrv_server_is_leader 0
        "#;

        let result = MetaNode::parse_metrics_to_json(metrics_input);

        // Should handle malformed lines gracefully and return valid JSON object
        assert!(result.is_object());

        // Should have parsed the valid metrics
        let server = result.get("server").unwrap();
        assert_eq!(server.get("current_term").unwrap(), 1.0);
        assert_eq!(server.get("is_leader").unwrap(), 0.0);

        // Invalid value should be parsed as 0.0 due to unwrap_or(0.0)
        let meta_network = result.get("meta_network");
        if let Some(meta_net) = meta_network {
            if let Some(invalid_val) = meta_net.get("invalid_value") {
                assert_eq!(invalid_val, 0.0);
            }
        }
    }

    #[test]
    fn test_convert_histogram_to_percentiles() {
        use std::collections::BTreeMap;

        // Typical case: varied latencies
        let mut buckets = BTreeMap::new();
        buckets.insert("1.0".to_string(), 10.0);
        buckets.insert("5.0".to_string(), 50.0);
        buckets.insert("10.0".to_string(), 80.0);
        buckets.insert("100.0".to_string(), 100.0);

        let result = MetaNode::convert_histogram_to_percentiles(buckets).unwrap();
        let array = result.as_array().unwrap();
        assert_eq!(array.len(), 4);
        assert_eq!(array[0][0], "p50");
        assert_eq!(array[0][1], 5.0);
        assert_eq!(array[1][0], "p90");
        assert_eq!(array[1][1], 100.0);

        // Edge case: all requests fast (uniform cumulative count)
        let mut fast_buckets = BTreeMap::new();
        fast_buckets.insert("0.001".to_string(), 9.0);
        fast_buckets.insert("0.01".to_string(), 9.0);
        fast_buckets.insert("1.0".to_string(), 9.0);

        let fast_result = MetaNode::convert_histogram_to_percentiles(fast_buckets).unwrap();
        let fast_array = fast_result.as_array().unwrap();
        assert_eq!(fast_array[0][1], 0.001);
        assert_eq!(fast_array[2][1], 0.001); // p99

        // Edge case: empty or zero counts
        assert!(MetaNode::convert_histogram_to_percentiles(BTreeMap::new()).is_none());

        let mut zero_buckets = BTreeMap::new();
        zero_buckets.insert("1.0".to_string(), 0.0);
        assert!(MetaNode::convert_histogram_to_percentiles(zero_buckets).is_none());
    }

    #[test]
    fn test_parse_metrics_to_json_big() {
        let metrics_input = r#"
metasrv_server_current_leader_id 0
metasrv_server_is_leader 1
metasrv_server_node_is_health 1
metasrv_server_leader_changes_total 1
metasrv_server_applying_snapshot 0
metasrv_server_snapshot_key_count 4024528
metasrv_server_snapshot_primary_index_count 102696918
metasrv_server_snapshot_expire_index_count 78
metasrv_server_snapshot_block_count 504
metasrv_server_snapshot_data_size 642273909
metasrv_server_snapshot_index_size 79306
metasrv_server_snapshot_avg_block_size 1274352
metasrv_server_snapshot_avg_keys_per_block 7985
metasrv_server_snapshot_read_block 28072
metasrv_server_snapshot_read_block_from_cache 27621
metasrv_server_snapshot_read_block_from_disk 451
metasrv_server_raft_log_cache_items 11386
metasrv_server_raft_log_cache_used_size 3887263
metasrv_server_raft_log_wal_open_chunk_size 3881166
metasrv_server_raft_log_wal_offset 1864954656
metasrv_server_raft_log_wal_closed_chunk_count 0
metasrv_server_raft_log_wal_closed_chunk_total_size 0
metasrv_server_raft_log_size 3881166
metasrv_server_proposals_applied 3459805
metasrv_server_last_log_index 3459807
metasrv_server_last_seq 6681352
metasrv_server_current_term 1
metasrv_server_proposals_pending 30
metasrv_server_proposals_failed_total 0
metasrv_server_read_failed_total 0
metasrv_server_watchers 0
metasrv_server_version{component="metasrv",semver="v1.2.794-nightly",sha="4fca845964"} 1
metasrv_meta_network_rpc_delay_seconds_sum 801.2423599059944
metasrv_meta_network_rpc_delay_ms_sum 787255.0
metasrv_meta_network_rpc_delay_ms_count 28019
metasrv_meta_network_rpc_delay_ms_bucket{le="1.0"} 4
metasrv_meta_network_rpc_delay_ms_bucket{le="2.0"} 4
metasrv_meta_network_rpc_delay_ms_bucket{le="5.0"} 378
metasrv_meta_network_rpc_delay_ms_bucket{le="10.0"} 812
metasrv_meta_network_rpc_delay_ms_bucket{le="20.0"} 5523
metasrv_meta_network_rpc_delay_ms_bucket{le="50.0"} 27461
metasrv_meta_network_rpc_delay_ms_bucket{le="100.0"} 27946
metasrv_meta_network_rpc_delay_ms_bucket{le="200.0"} 28019
metasrv_meta_network_rpc_delay_ms_bucket{le="500.0"} 28019
metasrv_meta_network_rpc_delay_ms_bucket{le="1000.0"} 28019
metasrv_meta_network_rpc_delay_ms_bucket{le="2000.0"} 28019
metasrv_meta_network_rpc_delay_ms_bucket{le="5000.0"} 28019
metasrv_meta_network_rpc_delay_ms_bucket{le="10000.0"} 28019
metasrv_meta_network_rpc_delay_ms_bucket{le="30000.0"} 28019
metasrv_meta_network_rpc_delay_ms_bucket{le="60000.0"} 28019
metasrv_meta_network_rpc_delay_ms_bucket{le="+Inf"} 28019
metasrv_meta_network_sent_bytes_total 5346188
metasrv_meta_network_recv_bytes_total 35974092
metasrv_meta_network_req_inflights 30
metasrv_meta_network_req_success_total 520648
metasrv_meta_network_req_failed_total 0
metasrv_meta_network_watch_initialization_total 0
metasrv_meta_network_watch_change_total 0
metasrv_meta_network_stream_get_item_sent_total 0
metasrv_meta_network_stream_mget_item_sent_total 472897
metasrv_meta_network_stream_list_item_sent_total 68422
metasrv_raft_storage_snapshot_building 1
metasrv_raft_storage_snapshot_written_entries_total 80114031

        "#;

        let result = MetaNode::parse_metrics_to_json(metrics_input);

        println!("{}", result);

        // Verify expected categories are present
        assert!(result.get("meta_network").is_some());
        assert!(result.get("raft_storage").is_some());
        assert!(result.get("server").is_some());

        // Verify some expected metrics
        let meta_network = result.get("meta_network").unwrap();
        assert!(meta_network.get("recv_bytes_total").is_some());
        assert!(meta_network.get("req_success_total").is_some());
        assert!(meta_network.get("rpc_delay_ms").is_some()); // Should be converted to percentiles

        let server = result.get("server").unwrap();
        assert!(server.get("current_term").is_some());
        assert!(server.get("is_leader").is_some());
    }

    #[test]
    fn test_parse_metrics_to_json_labeled_buckets() {
        let metrics_input = r#"
metasrv_raft_network_active_peers{id="1",addr="127.0.0.1:29003"} 1
metasrv_raft_network_active_peers{id="2",addr="127.0.0.1:29006"} 1
metasrv_raft_network_append_sent_seconds_bucket{le="+Inf",to="1"} 9
metasrv_raft_network_append_sent_seconds_bucket{le="+Inf",to="2"} 6
metasrv_raft_network_append_sent_seconds_bucket{le="0.001",to="1"} 9
metasrv_raft_network_append_sent_seconds_bucket{le="0.001",to="2"} 6
metasrv_raft_network_append_sent_seconds_bucket{le="0.002",to="1"} 9
metasrv_raft_network_append_sent_seconds_bucket{le="0.002",to="2"} 6
metasrv_raft_network_append_sent_seconds_bucket{le="0.004",to="1"} 9
metasrv_raft_network_append_sent_seconds_bucket{le="0.004",to="2"} 6
metasrv_raft_network_append_sent_seconds_bucket{le="0.008",to="1"} 9
metasrv_raft_network_append_sent_seconds_bucket{le="0.008",to="2"} 6
metasrv_raft_network_append_sent_seconds_bucket{le="0.016",to="1"} 9
metasrv_raft_network_append_sent_seconds_bucket{le="0.016",to="2"} 6
metasrv_raft_network_append_sent_seconds_bucket{le="0.032",to="1"} 9
metasrv_raft_network_append_sent_seconds_bucket{le="0.032",to="2"} 6
metasrv_raft_network_append_sent_seconds_bucket{le="0.064",to="1"} 9
metasrv_raft_network_append_sent_seconds_bucket{le="0.064",to="2"} 6
metasrv_raft_network_append_sent_seconds_bucket{le="0.128",to="1"} 9
metasrv_raft_network_append_sent_seconds_bucket{le="0.128",to="2"} 6
metasrv_raft_network_append_sent_seconds_bucket{le="0.256",to="1"} 9
metasrv_raft_network_append_sent_seconds_bucket{le="0.256",to="2"} 6
metasrv_raft_network_append_sent_seconds_bucket{le="0.512",to="1"} 9
metasrv_raft_network_append_sent_seconds_bucket{le="0.512",to="2"} 6
metasrv_raft_network_append_sent_seconds_bucket{le="1.024",to="1"} 9
metasrv_raft_network_append_sent_seconds_bucket{le="1.024",to="2"} 6
metasrv_raft_network_append_sent_seconds_bucket{le="131.072",to="1"} 9
metasrv_raft_network_append_sent_seconds_bucket{le="131.072",to="2"} 6
metasrv_raft_network_append_sent_seconds_bucket{le="16.384",to="1"} 9
metasrv_raft_network_append_sent_seconds_bucket{le="16.384",to="2"} 6
metasrv_raft_network_append_sent_seconds_bucket{le="2.048",to="1"} 9
metasrv_raft_network_append_sent_seconds_bucket{le="2.048",to="2"} 6
metasrv_raft_network_append_sent_seconds_bucket{le="262.144",to="1"} 9
metasrv_raft_network_append_sent_seconds_bucket{le="262.144",to="2"} 6
metasrv_raft_network_append_sent_seconds_bucket{le="32.768",to="1"} 9
metasrv_raft_network_append_sent_seconds_bucket{le="32.768",to="2"} 6
metasrv_raft_network_append_sent_seconds_bucket{le="4.096",to="1"} 9
metasrv_raft_network_append_sent_seconds_bucket{le="4.096",to="2"} 6
metasrv_raft_network_append_sent_seconds_bucket{le="524.288",to="1"} 9
metasrv_raft_network_append_sent_seconds_bucket{le="524.288",to="2"} 6
metasrv_raft_network_append_sent_seconds_bucket{le="65.536",to="1"} 9
metasrv_raft_network_append_sent_seconds_bucket{le="65.536",to="2"} 6
metasrv_raft_network_append_sent_seconds_bucket{le="8.192",to="1"} 9
metasrv_raft_network_append_sent_seconds_bucket{le="8.192",to="2"} 6
metasrv_raft_network_append_sent_seconds_count{to="1"} 9
metasrv_raft_network_append_sent_seconds_count{to="2"} 6

        "#;

        let result = MetaNode::parse_metrics_to_json(metrics_input);

        println!("{}", result);

        // Verify expected categories are present
        assert!(result.get("raft_network").is_some());

        // Verify labeled histograms use sub-key structure
        let raft_network = result.get("raft_network").unwrap();
        let append_sent_seconds = raft_network.get("append_sent_seconds").unwrap();
        assert!(append_sent_seconds.is_object());

        let hist_obj = append_sent_seconds.as_object().unwrap();
        assert!(hist_obj.get("to=1").is_some());
        assert!(hist_obj.get("to=2").is_some());

        // Verify the histograms contain percentile arrays
        let hist1 = hist_obj.get("to=1").unwrap();
        let hist2 = hist_obj.get("to=2").unwrap();
        assert!(hist1.is_array());
        assert!(hist2.is_array());

        // Verify regular labeled metrics use sub-key structure
        let active_peers = raft_network.get("active_peers").unwrap();
        assert!(active_peers.is_object());
        let peers_obj = active_peers.as_object().unwrap();
        assert!(peers_obj.get("id=1,addr=127.0.0.1:29003").is_some());
        assert!(peers_obj.get("id=2,addr=127.0.0.1:29006").is_some());
    }

    #[test]
    fn test_parse_metrics_label_handling() {
        let metrics_input = r#"
metasrv_server_current_term 1
metasrv_server_version{component="metasrv",semver="v1.2.794"} 1
metasrv_raft_network_active_peers{id="1",addr="127.0.0.1:29003"} 1
metasrv_raft_network_active_peers{id="2",addr="127.0.0.1:29006"} 1
metasrv_meta_network_req_inflights{type="read"} 5
metasrv_meta_network_req_inflights{type="write"} 3
metasrv_server_no_labels 42
        "#;

        let result = MetaNode::parse_metrics_to_json(metrics_input);

        // Verify categories exist
        assert!(result.get("server").is_some());
        assert!(result.get("raft_network").is_some());
        assert!(result.get("meta_network").is_some());

        let server = result.get("server").unwrap();
        let raft_network = result.get("raft_network").unwrap();
        let meta_network = result.get("meta_network").unwrap();

        // Metrics without labels should use simple names
        assert_eq!(server.get("current_term").unwrap(), 1.0);
        assert_eq!(server.get("no_labels").unwrap(), 42.0);

        // Metrics with labels should use sub-key structure
        let version = server.get("version").unwrap().as_object().unwrap();
        assert_eq!(
            version.get("component=metasrv,semver=v1.2.794").unwrap(),
            1.0
        );

        let active_peers = raft_network
            .get("active_peers")
            .unwrap()
            .as_object()
            .unwrap();
        assert_eq!(active_peers.get("id=1,addr=127.0.0.1:29003").unwrap(), 1.0);
        assert_eq!(active_peers.get("id=2,addr=127.0.0.1:29006").unwrap(), 1.0);

        let req_inflights = meta_network
            .get("req_inflights")
            .unwrap()
            .as_object()
            .unwrap();
        assert_eq!(req_inflights.get("type=read").unwrap(), 5.0);
        assert_eq!(req_inflights.get("type=write").unwrap(), 3.0);

        // Verify that labeled metrics exist as objects (not simple values)
        assert!(server.get("version").unwrap().is_object());
        assert!(raft_network.get("active_peers").unwrap().is_object());
        assert!(meta_network.get("req_inflights").unwrap().is_object());
    }
}
