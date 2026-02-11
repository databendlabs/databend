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

use std::collections::HashMap;
use std::collections::hash_map::Entry;
use std::net::SocketAddr;
use std::ops::RangeInclusive;
use std::str::FromStr;
use std::sync::Arc;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;
use std::time::Duration;
use std::time::Instant;

use arrow_flight::flight_service_client::FlightServiceClient;
use databend_base::uniq_id::GlobalUniq;
use databend_common_base::base::BuildInfoRef;
use databend_common_base::base::DummySignalStream;
use databend_common_base::base::GlobalInstance;
use databend_common_base::base::SignalStream;
use databend_common_base::base::SignalType;
use databend_common_cache::Cache;
use databend_common_cache::LruCache;
use databend_common_cache::MemSized;
pub use databend_common_catalog::cluster_info::Cluster;
use databend_common_config::CacheStorageTypeInnerConfig;
use databend_common_config::GlobalConfig;
use databend_common_config::InnerConfig;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_grpc::ConnectionFactory;
use databend_common_license::license::Feature;
use databend_common_license::license_manager::LicenseManagerSwitch;
use databend_common_management::WarehouseApi;
use databend_common_management::WarehouseMgr;
use databend_common_meta_app::tenant::Tenant;
use databend_common_meta_store::MetaStore;
use databend_common_meta_store::MetaStoreProvider;
use databend_common_metrics::cluster::*;
use databend_common_settings::FlightKeepAliveParams;
use databend_common_settings::Settings;
use databend_common_telemetry::report_node_telemetry;
use databend_common_version::DATABEND_TELEMETRY_API_KEY;
use databend_common_version::DATABEND_TELEMETRY_ENDPOINT;
use databend_common_version::DATABEND_TELEMETRY_SOURCE;
use databend_enterprise_resources_management::ResourcesManagement;
use databend_meta_runtime::DatabendRuntime;
use databend_meta_types::NodeInfo;
use databend_meta_types::SeqV;
use futures::Future;
use futures::StreamExt;
use futures::future::Either;
use futures::future::select;
use log::error;
use log::info;
use log::warn;
use rand::Rng;
use rand::thread_rng;
use serde::Deserialize;
use serde::Serialize;
use tokio::sync::Mutex;
use tokio::sync::Notify;
use tokio::task::JoinHandle;
use tokio::time::sleep as tokio_async_sleep;
use tokio::time::sleep;

use crate::servers::flight::FlightClient;
use crate::servers::flight::keep_alive::build_keep_alive_config;

pub struct ClusterDiscovery {
    local_id: String,
    local_secret: String,
    version: BuildInfoRef,
    heartbeat: Mutex<ClusterHeartbeat>,
    warehouse_manager: Arc<dyn WarehouseApi>,
    cluster_id: String,
    tenant_id: String,
    flight_address: String,
    lru_cache: parking_lot::Mutex<LruCache<String, CachedNode>>,
}

// avoid leak FlightClient to common-xxx
#[async_trait::async_trait]
pub trait ClusterHelper {
    fn create(nodes: Vec<Arc<NodeInfo>>, local_id: String) -> Arc<Cluster>;
    fn empty(node: NodeInfo) -> Arc<Cluster>;
    fn is_empty(&self) -> bool;
    fn is_local(&self, node: &NodeInfo) -> bool;
    fn local_id(&self) -> String;
    fn ordered_index(&self) -> usize;
    fn index_of_nodeid(&self, node_id: &str) -> Option<usize>;

    fn get_nodes(&self) -> Vec<Arc<NodeInfo>>;

    async fn do_action<T: Serialize + Send + Clone, Res: for<'de> Deserialize<'de> + Send>(
        &self,
        path: &str,
        message: HashMap<String, T>,
        flight_params: FlightParams,
    ) -> Result<HashMap<String, Res>>;
}

#[async_trait::async_trait]
impl ClusterHelper for Cluster {
    fn create(nodes: Vec<Arc<NodeInfo>>, local_id: String) -> Arc<Cluster> {
        let unassign = nodes.iter().all(|node| !node.assigned_warehouse());
        Arc::new(Cluster {
            unassign,
            local_id,
            nodes,
        })
    }

    fn empty(node: NodeInfo) -> Arc<Cluster> {
        let node_info = Arc::new(node);
        let local_id = node_info.id.clone();
        Cluster::create(vec![node_info], local_id)
    }

    fn is_empty(&self) -> bool {
        self.nodes.len() <= 1
    }

    fn is_local(&self, node: &NodeInfo) -> bool {
        node.id == self.local_id
    }

    fn local_id(&self) -> String {
        self.local_id.clone()
    }

    fn ordered_index(&self) -> usize {
        let mut nodes = self.get_nodes();
        nodes.sort_by(|a, b| a.id.cmp(&b.id));
        nodes
            .iter()
            .position(|x| x.id == self.local_id)
            .unwrap_or(0)
    }

    fn index_of_nodeid(&self, node_id: &str) -> Option<usize> {
        let mut nodes = self.get_nodes();
        nodes.sort_by(|a, b| a.id.cmp(&b.id));
        nodes.iter().position(|x| x.id == node_id)
    }

    fn get_nodes(&self) -> Vec<Arc<NodeInfo>> {
        self.nodes.to_vec()
    }

    async fn do_action<T: Serialize + Send + Clone, Res: for<'de> Deserialize<'de> + Send>(
        &self,
        path: &str,
        message: HashMap<String, T>,
        flight_params: FlightParams,
    ) -> Result<HashMap<String, Res>> {
        fn get_node<'a>(nodes: &'a [Arc<NodeInfo>], id: &str) -> Result<&'a Arc<NodeInfo>> {
            for node in nodes {
                if node.id == id {
                    return Ok(node);
                }
            }

            Err(ErrorCode::NotFoundClusterNode(format!(
                "Not found node {} in cluster",
                id
            )))
        }

        let mut response = HashMap::with_capacity(message.len());
        for (id, message) in message {
            let node = get_node(&self.nodes, &id)?;

            let do_action_with_retry = {
                let config = GlobalConfig::instance();
                let flight_address = node.flight_address.clone();
                let node_secret = node.secret.clone();

                async move {
                    let mut attempt = 0;

                    loop {
                        let mut conn =
                            create_client(&config, &flight_address, flight_params.keep_alive)
                                .await?;
                        match conn
                            .do_action::<_, Res>(
                                path,
                                node_secret.clone(),
                                message.clone(),
                                flight_params.timeout,
                            )
                            .await
                        {
                            Ok(result) => return Ok(result),
                            Err(e)
                                if e.code() == ErrorCode::CANNOT_CONNECT_NODE
                                    && attempt < flight_params.retry_times =>
                            {
                                // only retry when error is network problem
                                info!("retry do_action, attempt: {}", attempt);
                                attempt += 1;
                                sleep(Duration::from_secs(flight_params.retry_interval)).await;
                            }
                            Err(e) => return Err(e),
                        }
                    }
                }
            };

            response.insert(id, do_action_with_retry.await?);
        }

        Ok(response)
    }
}

impl ClusterDiscovery {
    #[async_backtrace::framed]
    pub async fn create_meta_client(
        cfg: &InnerConfig,
        _version: BuildInfoRef,
    ) -> Result<MetaStore> {
        let meta_api_provider = MetaStoreProvider::new(cfg.meta.to_meta_grpc_client_conf());
        match meta_api_provider
            .create_meta_store::<DatabendRuntime>()
            .await
        {
            Ok(meta_store) => Ok(meta_store),
            Err(cause) => Err(ErrorCode::MetaServiceError(format!(
                "Failed to create meta store: {}",
                cause
            ))),
        }
    }

    #[async_backtrace::framed]
    pub async fn init(cfg: &InnerConfig, version: BuildInfoRef) -> Result<()> {
        let metastore = Self::create_meta_client(cfg, version).await?;
        GlobalInstance::set(Self::try_create(cfg, version, metastore).await?);

        Ok(())
    }

    #[async_backtrace::framed]
    pub async fn try_create(
        cfg: &InnerConfig,
        version: BuildInfoRef,
        metastore: MetaStore,
    ) -> Result<Arc<ClusterDiscovery>> {
        let (lift_time, provider) = Self::create_provider(cfg, version, metastore)?;

        Ok(Arc::new(ClusterDiscovery {
            local_id: cfg.query.node_id.clone(),
            local_secret: cfg.query.node_secret.clone(),
            version,
            warehouse_manager: provider.clone(),
            heartbeat: Mutex::new(ClusterHeartbeat::create(
                lift_time,
                provider,
                cfg.query.common.cluster_id.clone(),
                cfg.query.tenant_id.tenant_name().to_string(),
            )),
            cluster_id: cfg.query.common.cluster_id.clone(),
            tenant_id: cfg.query.tenant_id.tenant_name().to_string(),
            flight_address: cfg.query.common.flight_api_address.clone(),
            lru_cache: parking_lot::Mutex::new(LruCache::with_items_capacity(100)),
        }))
    }

    pub fn instance() -> Arc<ClusterDiscovery> {
        GlobalInstance::get()
    }

    fn create_provider(
        cfg: &InnerConfig,
        version: BuildInfoRef,
        metastore: MetaStore,
    ) -> Result<(Duration, Arc<dyn WarehouseApi>)> {
        // TODO: generate if tenant or cluster id is empty
        let tenant_id = &cfg.query.tenant_id;
        let lift_time = Duration::from_secs(60);
        let cluster_manager =
            WarehouseMgr::create(metastore, tenant_id.tenant_name(), lift_time, version)?;

        Ok((lift_time, Arc::new(cluster_manager)))
    }

    async fn create_cluster_with_try_connect(
        &self,
        config: &InnerConfig,
        nodes: Result<Vec<NodeInfo>>,
    ) -> Result<Arc<Cluster>> {
        match nodes {
            Err(cause) => {
                metric_incr_cluster_error_count(
                    &self.local_id,
                    "discover",
                    &self.cluster_id,
                    &self.tenant_id,
                    &self.flight_address,
                );
                Err(cause.add_message_back("(while cluster api get_nodes)."))
            }
            Ok(cluster_nodes) => {
                let mut has_local_node = false;
                let mut res = Vec::with_capacity(cluster_nodes.len());
                for node in &cluster_nodes {
                    if node.id == self.local_id {
                        has_local_node = true;
                    }

                    if config.query.check_connection_before_schedule && node.id != self.local_id {
                        let start_at = Instant::now();
                        if let Err(cause) = create_client(
                            config,
                            &node.flight_address,
                            FlightKeepAliveParams::default(),
                        )
                        .await
                        {
                            warn!(
                                "Cannot connect node [{:?}] after {:?}s, remove it in query. cause: {:?}",
                                node.flight_address,
                                start_at.elapsed().as_secs_f32(),
                                cause
                            );

                            continue;
                        }
                    }

                    res.push(Arc::new(node.clone()));
                }

                // When this node loses heartbeat with the meta node but receives an SQL request from the client, we should attempt to use this node.
                if !has_local_node && !res.is_empty() {
                    let mut local_node_info = NodeInfo::create(
                        config.query.node_id.clone(),
                        config.query.node_secret.clone(),
                        format!(
                            "{}:{}",
                            config.query.common.http_handler_host,
                            config.query.common.http_handler_port
                        ),
                        config.query.common.flight_api_address.clone(),
                        config.query.common.discovery_address.clone(),
                        String::new(),
                        String::new(),
                    );

                    local_node_info.cluster_id = res[0].cluster_id.clone();
                    local_node_info.warehouse_id = res[0].warehouse_id.clone();
                    res.push(Arc::new(local_node_info));
                }

                metrics_gauge_discovered_nodes(
                    &self.local_id,
                    &self.cluster_id,
                    &self.tenant_id,
                    &self.flight_address,
                    cluster_nodes.len() as f64,
                );

                // compatibility, for self-managed nodes, we allow queries to continue executing even when the heartbeat fails.
                if cluster_nodes.is_empty() && !config.query.common.cluster_id.is_empty() {
                    let mut node_info = NodeInfo::create(
                        config.query.node_id.clone(),
                        config.query.node_secret.clone(),
                        format!(
                            "{}:{}",
                            config.query.common.http_handler_host,
                            config.query.common.http_handler_port
                        ),
                        config.query.common.flight_api_address.clone(),
                        config.query.common.discovery_address.clone(),
                        String::new(),
                        String::new(),
                    );

                    node_info.cluster_id = config.query.common.cluster_id.clone();
                    node_info.warehouse_id = config.query.common.warehouse_id.clone();
                    return Ok(Cluster::empty(node_info));
                }

                Ok(Cluster::create(res, self.local_id.clone()))
            }
        }
    }

    pub async fn discover_warehouse_nodes(&self, config: &InnerConfig) -> Result<Arc<Cluster>> {
        let nodes = match config.query.common.warehouse_id.is_empty() {
            true => {
                self.warehouse_manager
                    .discover_warehouse_nodes(&config.query.node_id)
                    .await
            }
            false => {
                self.warehouse_manager
                    .list_warehouse_nodes(config.query.common.warehouse_id.clone())
                    .await
            }
        };

        self.create_cluster_with_try_connect(config, nodes).await
    }

    #[async_backtrace::framed]
    pub async fn discover(&self, config: &InnerConfig) -> Result<Arc<Cluster>> {
        let nodes = match config.query.common.cluster_id.is_empty() {
            true => self.warehouse_manager.discover(&config.query.node_id).await,
            false => {
                self.warehouse_manager
                    .list_warehouse_cluster_nodes(
                        &config.query.common.warehouse_id,
                        &config.query.common.cluster_id,
                    )
                    .await
            }
        };

        self.create_cluster_with_try_connect(config, nodes).await
    }

    pub async fn single_node_cluster(&self, config: &InnerConfig) -> Result<Arc<Cluster>> {
        // For embedded/local mode with empty cluster_id and warehouse_id, skip meta lookup
        let use_embedded_mode = databend_common_config::GlobalConfig::is_embedded_mode();

        let node_info_opt = if use_embedded_mode {
            None // Skip meta lookup for embedded mode
        } else {
            self.warehouse_manager
                .get_node_info(&config.query.node_id)
                .await?
        };

        match node_info_opt {
            None => {
                let mut node_info = NodeInfo::create(
                    config.query.node_id.clone(),
                    config.query.node_secret.clone(),
                    format!(
                        "{}:{}",
                        config.query.common.http_handler_host,
                        config.query.common.http_handler_port
                    ),
                    config.query.common.flight_api_address.clone(),
                    config.query.common.discovery_address.clone(),
                    String::new(),
                    String::new(),
                );

                // For embedded mode, set virtual cluster/warehouse IDs to make node "assigned"
                if use_embedded_mode {
                    node_info.cluster_id = "embedded_cluster".to_string();
                    node_info.warehouse_id = "embedded_warehouse".to_string();
                } else {
                    node_info.cluster_id = config.query.common.cluster_id.clone();
                    node_info.warehouse_id = config.query.common.warehouse_id.clone();
                }
                Ok(Cluster::empty(node_info))
            }
            Some(v) => Ok(Cluster::create(
                vec![Arc::new(v)],
                config.query.node_id.clone(),
            )),
        }
    }

    pub async fn find_node_by_warehouse(
        self: Arc<Self>,
        warehouse: &str,
    ) -> Result<Option<Arc<NodeInfo>>> {
        let nodes = self
            .warehouse_manager
            .list_warehouse_nodes(warehouse.to_string())
            .await?;

        let mut warehouse_clusters_nodes = Vec::new();
        let mut warehouse_clusters_nodes_index = HashMap::new();

        for node in nodes {
            match warehouse_clusters_nodes_index
                .entry((node.version.to_string(), node.cluster_id.clone()))
            {
                Entry::Vacant(v) => {
                    v.insert(warehouse_clusters_nodes.len());
                    warehouse_clusters_nodes.push(vec![Arc::new(node)]);
                }
                Entry::Occupied(v) => {
                    warehouse_clusters_nodes[*v.get()].push(Arc::new(node));
                }
            };
        }

        if warehouse_clusters_nodes.is_empty() {
            return Ok(None);
        }

        let system_time = std::time::SystemTime::now();
        let system_timestamp = system_time
            .duration_since(std::time::UNIX_EPOCH)
            .expect("expect time");

        let millis = system_timestamp.as_millis();
        let cluster_idx = (millis % warehouse_clusters_nodes_index.len() as u128) as usize;
        let pick_cluster_nodes = &warehouse_clusters_nodes[cluster_idx];
        let nodes_idx = (millis % pick_cluster_nodes.len() as u128) as usize;
        Ok(Some(pick_cluster_nodes[nodes_idx].clone()))
    }

    pub async fn find_node_by_id(self: Arc<Self>, id: &str) -> Result<Option<Arc<NodeInfo>>> {
        {
            let mut lru_cache = self.lru_cache.lock();
            if let Some(node_info) = lru_cache.get(id) {
                return Ok(Some(node_info.node.clone()));
            }
        }

        if let Some(node_info) = self.warehouse_manager.get_node_info(id).await? {
            let cache_object = Arc::new(node_info);
            let mut lru_cache = self.lru_cache.lock();
            lru_cache.insert(id.to_string(), CachedNode {
                node: cache_object.clone(),
            });
            return Ok(Some(cache_object));
        }

        Ok(None)
    }

    #[async_backtrace::framed]
    async fn drop_invalid_nodes(self: &Arc<Self>, node_info: &NodeInfo) -> Result<()> {
        let online_nodes = match self.warehouse_manager.list_online_nodes().await {
            Ok(nodes) => nodes,
            Err(cause) => {
                metric_incr_cluster_error_count(
                    &self.local_id,
                    "drop_invalid_ndes.get_nodes",
                    &self.cluster_id,
                    &self.tenant_id,
                    &self.flight_address,
                );
                return Err(cause.add_message_back("(while drop_invalid_nodes)"));
            }
        };

        for before_node in online_nodes {
            // Restart in a very short time(< heartbeat timeout) after abnormal shutdown, Which will
            // lead to some invalid information
            if before_node.flight_address.eq(&node_info.flight_address) {
                let drop_invalid_node = self.warehouse_manager.shutdown_node(before_node.id);
                if let Err(cause) = drop_invalid_node.await {
                    warn!("Drop invalid node failure: {:?}", cause);
                }
            }
        }

        Ok(())
    }

    #[async_backtrace::framed]
    pub async fn unregister_to_metastore(
        self: &Arc<Self>,
        signal: &mut SignalStream,
        cfg: &InnerConfig,
    ) {
        self.report_telemetry_data(cfg, None).await;

        let mut heartbeat = self.heartbeat.lock().await;

        if let Err(shutdown_failure) = heartbeat.shutdown().await {
            warn!(
                "Cannot shutdown cluster heartbeat, cause {:?}",
                shutdown_failure
            );
        }

        let mut mut_signal_pin = signal.as_mut();
        let signal_future = Box::pin(mut_signal_pin.next());
        let drop_node = Box::pin(self.warehouse_manager.shutdown_node(self.local_id.clone()));
        match futures::future::select(drop_node, signal_future).await {
            Either::Left((drop_node_result, _)) => {
                if let Err(drop_node_failure) = drop_node_result {
                    warn!(
                        "Cannot drop cluster node(while shutdown), cause {:?}",
                        drop_node_failure
                    );
                }
            }
            Either::Right((signal_type, _)) => {
                match signal_type {
                    None => *signal = DummySignalStream::create(SignalType::Exit),
                    Some(signal_type) => *signal = DummySignalStream::create(signal_type),
                };
            }
        };
    }

    #[async_backtrace::framed]
    pub async fn register_to_metastore(self: &Arc<Self>, cfg: &InnerConfig) -> Result<()> {
        let mut address = cfg.query.common.flight_api_address.clone();
        let mut http_address = format!(
            "{}:{}",
            cfg.query.common.http_handler_host, cfg.query.common.http_handler_port
        );
        let mut discovery_address = match cfg.query.common.discovery_address.is_empty() {
            true => format!(
                "{}:{}",
                cfg.query.common.http_handler_host, cfg.query.common.http_handler_port
            ),
            false => cfg.query.common.discovery_address.clone(),
        };

        for (lookup_ip, typ) in [
            (&mut address, "flight-api-address"),
            (&mut discovery_address, "discovery-address"),
            (&mut http_address, "http-address"),
        ] {
            if let Ok(socket_addr) = SocketAddr::from_str(lookup_ip) {
                let ip_addr = socket_addr.ip();
                if ip_addr.is_loopback() || ip_addr.is_unspecified() {
                    let local_addr = self.warehouse_manager.get_local_addr().await?;

                    let local_socket_addr = SocketAddr::from_str(&local_addr)?;
                    let new_addr = format!("{}:{}", local_socket_addr.ip(), socket_addr.port());
                    warn!(
                        "Detected loopback or unspecified address as {} endpoint. \
                        We rewrite it(\"{}\" -> \"{}\") for advertising to other nodes. \
                        If there are proxies between nodes, you can specify endpoint with --{}.",
                        typ, lookup_ip, new_addr, typ
                    );

                    *lookup_ip = new_addr;
                }
            }
        }

        let mut cache_id = self.local_id.clone();
        if let CacheStorageTypeInnerConfig::Disk = cfg.cache.data_cache_storage {
            let path = format!("{}/cache_id", cfg.cache.disk_cache_config.path);
            cache_id = match tokio::fs::read_to_string(path.clone()).await {
                Ok(content) => content,
                Err(e) if e.kind() == tokio::io::ErrorKind::NotFound => {
                    let cache_id = GlobalUniq::unique();
                    if let Err(e) = tokio::fs::write(path, cache_id.clone()).await {
                        return Err(ErrorCode::TokioError(format!(
                            "Cannot write cache id file, cause: {:?}",
                            e
                        )));
                    }

                    cache_id
                }
                Err(e) => {
                    return Err(ErrorCode::TokioError(format!(
                        "Cannot read cache id file, cause: {:?}",
                        e
                    )));
                }
            }
        }

        let mut node_info = NodeInfo::create(
            self.local_id.clone(),
            self.local_secret.clone(),
            http_address,
            address,
            discovery_address,
            self.version.commit_detail.clone(),
            cache_id,
        );

        let resources_management = GlobalInstance::get::<Arc<dyn ResourcesManagement>>();
        resources_management.init_node(&mut node_info).await?;

        self.drop_invalid_nodes(&node_info).await?;

        let online_nodes = self.warehouse_manager.list_online_nodes().await?;
        self.check_license_key(online_nodes.clone()).await?;

        match self.warehouse_manager.start_node(node_info.clone()).await {
            Ok(seq_node) => {
                self.start_heartbeat(seq_node).await?;
                self.report_telemetry_data(cfg, Some(online_nodes)).await;
                Ok(())
            }
            Err(cause) => Err(cause.add_message_back("(while cluster api add_node).")),
        }
    }

    #[async_backtrace::framed]
    async fn start_heartbeat(self: &Arc<Self>, seq_node: SeqV<NodeInfo>) -> Result<()> {
        let mut heartbeat = self.heartbeat.lock().await;
        let seq = seq_node.seq;
        let node_info = seq_node.data;
        heartbeat.start(node_info, seq);
        Ok(())
    }

    async fn check_license_key(&self, nodes: Vec<NodeInfo>) -> Result<()> {
        let license_key = self.get_license_key(&self.tenant_id).await?;

        let total_cpu_nums = nodes.iter().map(|x| x.cpu_nums).sum::<u64>();

        info!(
            "[Cluster] current resource status - online nodes: [{}], available CPU cores: [{}] for tenant [{}].",
            nodes.len(),
            total_cpu_nums,
            self.tenant_id
        );

        LicenseManagerSwitch::instance()
            .check_enterprise_enabled(license_key.clone(), Feature::MaxNodeQuota(nodes.len()))?;

        LicenseManagerSwitch::instance()
            .check_enterprise_enabled(license_key, Feature::MaxCpuQuota(total_cpu_nums as usize))
    }

    async fn get_license_key(&self, tenant: &str) -> Result<String> {
        // We must get the license key from settings. It may be in the configuration file.
        let settings = Settings::create(Tenant::new_literal(tenant));
        settings.load_changes().await?;
        Ok(settings.get_enterprise_license(self.version))
    }

    async fn report_telemetry_data(&self, cfg: &InnerConfig, online_nodes: Option<Vec<NodeInfo>>) {
        let start_time = std::time::Instant::now();

        let license_result = self
            .get_license_key(&self.tenant_id)
            .await
            .ok()
            .filter(|key| !key.is_empty())
            .and_then(|key| LicenseManagerSwitch::instance().parse_license(&key).ok());

        if let Some(ref claims) = license_result {
            let license_type = claims.custom.r#type.as_deref().unwrap_or("").to_lowercase();
            if license_type != "trial" && !cfg.telemetry.enabled {
                return;
            }
        }

        let license_info = match license_result {
            Some(claims) => {
                let expires_at = claims.expires_at.map(|d| d.as_secs()).unwrap_or(0);
                serde_json::json!({
                    "has_license": true,
                    "expires_at": expires_at,
                    "license_info": claims.custom
                })
            }
            None => serde_json::json!({
                "has_license": false,
                "license_info": null
            }),
        };

        // Collect system information
        let mut system = sysinfo::System::new();
        system.refresh_memory();
        system.refresh_cpu_all();

        let event_type = if online_nodes.is_none() {
            "node_shutdown"
        } else {
            "node_startup"
        };

        let payload = serde_json::json!({
            "timestamp": std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_secs())
                .unwrap_or(0),
            "event_type": event_type,
            "cluster": {
                "cluster_id": self.cluster_id,
                "tenant_name": self.tenant_id
            },
            "node": {
                "node_id": self.local_id,
                "version": self.version.commit_detail.as_str()
            },
            "system": {
                "os_name": sysinfo::System::name().unwrap_or_else(|| "unknown".to_string()),
                "os_version": sysinfo::System::os_version().unwrap_or_else(|| "unknown".to_string()),
                "kernel_version": sysinfo::System::kernel_version().unwrap_or_else(|| "unknown".to_string()),
                "arch": sysinfo::System::cpu_arch(),
                "cpu_cores": system.cpus().len(),
                "total_memory": system.total_memory(),
                "available_memory": system.available_memory(),
                "used_memory": system.used_memory(),
                "uptime_seconds": sysinfo::System::uptime()
            },
            "cache_strategy": {
                "enable_table_meta_cache": cfg.cache.enable_table_meta_cache,
                "table_meta_snapshot_count": cfg.cache.table_meta_snapshot_count,
                "table_meta_segment_bytes": cfg.cache.table_meta_segment_bytes,
                "block_meta_count": cfg.cache.block_meta_count,
                "segment_block_metas_count": cfg.cache.segment_block_metas_count,
                "table_meta_statistic_count": cfg.cache.table_meta_statistic_count,
                "segment_statistics_bytes": cfg.cache.segment_statistics_bytes,
                "enable_table_index_bloom": cfg.cache.enable_table_index_bloom,
                "table_bloom_index_meta_count": cfg.cache.table_bloom_index_meta_count,
                "table_bloom_index_filter_count": cfg.cache.table_bloom_index_filter_count,
                "table_bloom_index_filter_size": cfg.cache.table_bloom_index_filter_size,
                "table_prune_partitions_count": cfg.cache.table_prune_partitions_count,
                "inverted_index_meta_count": cfg.cache.inverted_index_meta_count,
                "inverted_index_filter_size": cfg.cache.inverted_index_filter_size,
                "vector_index_meta_count": cfg.cache.vector_index_meta_count,
                "vector_index_filter_size": cfg.cache.vector_index_filter_size,
                "data_cache_in_memory_bytes": cfg.cache.data_cache_in_memory_bytes,
                "table_data_cache_population_queue_size": cfg.cache.table_data_cache_population_queue_size,
                "table_data_deserialized_data_bytes": cfg.cache.table_data_deserialized_data_bytes,
                "disk_cache_max_bytes": cfg.cache.disk_cache_config.max_bytes,
                "disk_cache_sync_data": cfg.cache.disk_cache_config.sync_data
            },
            "memory_management": {
                "max_server_memory_usage": cfg.query.common.max_server_memory_usage,
                "max_memory_limit_enabled": cfg.query.common.max_memory_limit_enabled,
                "spill_enabled": cfg.spill.global_bytes_limit > 0,
                "reserved_disk_ratio": cfg.spill.reserved_disk_ratio
            },
            "query_execution": {
                "max_running_queries": cfg.query.common.max_running_queries,
                "global_statement_queue": cfg.query.common.global_statement_queue,
            },
            "storage_engine": {
                "table_engine_memory_enabled": cfg.query.common.table_engine_memory_enabled,
                "storage_type": cfg.storage.params.to_string(),
                "storage_allow_insecure": cfg.storage.allow_insecure
            },
            "meta_config": {
                "meta_embedded": cfg.meta.embedded_dir.is_empty(),
                "meta_client_timeout": cfg.meta.client_timeout_in_second,
                "rpc_client_timeout_secs": cfg.query.common.rpc_client_timeout_secs,
                "endpoints": cfg.meta.endpoints,
            },
            "license": license_info,
            "cluster_nodes": {
                "count": online_nodes.as_ref().map(|nodes| nodes.len()).unwrap_or(0),
                "nodes": online_nodes.as_ref().map(|nodes|
                    nodes.iter().map(|node| serde_json::json!({
                        "id": node.id,
                        "cpu_nums": node.cpu_nums,
                        "version": node.version,
                        "binary_version": node.binary_version,
                        "cluster_id": node.cluster_id,
                        "warehouse_id": node.warehouse_id
                    })).collect::<Vec<_>>()
                ).unwrap_or_default()
            },
            "observability": {
                "collection_duration_ms": start_time.elapsed().as_millis() as u64,
                "report_timestamp": std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .map(|d| d.as_secs())
                    .unwrap_or(0),
                "source": DATABEND_TELEMETRY_SOURCE,
                "schema_version": "1.1"
            }
        });

        let _ = report_node_telemetry(
            payload,
            DATABEND_TELEMETRY_ENDPOINT,
            DATABEND_TELEMETRY_API_KEY,
        )
        .await;
    }
}

struct ClusterHeartbeat {
    timeout: Duration,
    shutdown: Arc<AtomicBool>,
    shutdown_notify: Arc<Notify>,
    cluster_api: Arc<dyn WarehouseApi>,
    shutdown_handler: Option<JoinHandle<()>>,
    cluster_id: String,
    tenant_id: String,
}

impl ClusterHeartbeat {
    pub fn create(
        timeout: Duration,
        cluster_api: Arc<dyn WarehouseApi>,
        cluster_id: String,
        tenant_id: String,
    ) -> ClusterHeartbeat {
        ClusterHeartbeat {
            timeout,
            cluster_api,
            shutdown: Arc::new(AtomicBool::new(false)),
            shutdown_notify: Arc::new(Notify::new()),
            shutdown_handler: None,
            cluster_id,
            tenant_id,
        }
    }

    fn heartbeat_loop(&self, mut node: NodeInfo, seq: u64) -> impl Future<Output = ()> + 'static {
        let shutdown = self.shutdown.clone();
        let shutdown_notify = self.shutdown_notify.clone();
        let cluster_api = self.cluster_api.clone();
        let sleep_range = self.heartbeat_interval(self.timeout);
        let cluster_id = self.cluster_id.clone();
        let tenant_id = self.tenant_id.clone();

        async move {
            let mut shutdown_notified = Box::pin(shutdown_notify.notified());

            let mut match_seq = seq;
            while !shutdown.load(Ordering::Relaxed) {
                let mills = {
                    let mut rng = thread_rng();
                    rng.gen_range(sleep_range.clone())
                };

                let sleep = tokio_async_sleep(Duration::from_millis(mills as u64));

                match select(shutdown_notified, Box::pin(sleep)).await {
                    Either::Left((_, _)) => {
                        break;
                    }
                    Either::Right((_, new_shutdown_notified)) => {
                        shutdown_notified = new_shutdown_notified;
                        let heartbeat = cluster_api.heartbeat_node(&mut node, match_seq);
                        match heartbeat.await {
                            Ok(new_match_seq) => {
                                match_seq = new_match_seq;
                            }
                            Err(failure) => {
                                metric_incr_cluster_heartbeat_count(
                                    &node.id,
                                    &node.flight_address,
                                    &cluster_id,
                                    &tenant_id,
                                    "failure",
                                );
                                error!("Cluster cluster api heartbeat failure: {:?}", failure);
                            }
                        }
                    }
                }
            }
        }
    }

    fn heartbeat_interval(&self, duration: Duration) -> RangeInclusive<u128> {
        (duration / 3).as_millis()..=((duration / 3) * 2).as_millis()
    }

    pub fn start(&mut self, node_info: NodeInfo, seq: u64) {
        self.shutdown_handler = Some(databend_common_base::runtime::spawn(
            self.heartbeat_loop(node_info, seq),
        ));
    }

    #[async_backtrace::framed]
    pub async fn shutdown(&mut self) -> Result<()> {
        if let Some(shutdown_handler) = self.shutdown_handler.take() {
            self.shutdown.store(true, Ordering::Relaxed);
            self.shutdown_notify.notify_waiters();
            if let Err(shutdown_failure) = shutdown_handler.await {
                return Err(ErrorCode::TokioError(format!(
                    "Cannot shutdown cluster heartbeat, cause {:?}",
                    shutdown_failure
                )));
            }
        }
        Ok(())
    }
}

#[async_backtrace::framed]
pub async fn create_client(
    config: &InnerConfig,
    address: &str,
    keep_alive: FlightKeepAliveParams,
) -> Result<FlightClient> {
    let timeout = if config.query.common.rpc_client_timeout_secs > 0 {
        Some(Duration::from_secs(
            config.query.common.rpc_client_timeout_secs,
        ))
    } else {
        None
    };

    let rpc_tls_config = if config.tls_query_cli_enabled() {
        Some(config.query.to_grpc_tls_config())
    } else {
        None
    };
    let keep_alive_config = build_keep_alive_config(keep_alive);

    Ok(FlightClient::new(FlightServiceClient::new(
        ConnectionFactory::create_rpc_channel(
            address.to_owned(),
            timeout,
            rpc_tls_config,
            keep_alive_config,
        )
        .await?,
    )))
}

#[derive(Clone, Copy, Debug)]
pub struct FlightParams {
    pub(crate) timeout: u64,
    pub(crate) retry_times: u64,
    pub(crate) retry_interval: u64,
    pub(crate) keep_alive: FlightKeepAliveParams,
}

#[derive(Clone)]
pub struct CachedNode {
    pub node: Arc<NodeInfo>,
}

impl MemSized for CachedNode {
    fn mem_bytes(&self) -> usize {
        0
    }
}
