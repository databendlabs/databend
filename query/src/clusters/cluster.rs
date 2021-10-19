// Copyright 2020 Datafuse Labs.
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

use std::ops::RangeInclusive;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Duration;

use common_arrow::arrow_format::flight::service::flight_service_client::FlightServiceClient;
use common_base::tokio;
use common_base::tokio::sync::Mutex;
use common_base::tokio::sync::Notify;
use common_base::tokio::task::JoinHandle;
use common_base::tokio::time::sleep as tokio_async_sleep;
use common_base::GlobalUniqName;
use common_exception::ErrorCode;
use common_exception::Result;
use common_flight_rpc::ConnectionFactory;
use common_management::NamespaceApi;
use common_management::NamespaceMgr;
use common_meta_api::KVApi;
use common_meta_types::NodeInfo;
use futures::future::select;
use futures::future::Either;
use futures::Future;
use rand::thread_rng;
use rand::Rng;

use crate::api::FlightClient;
use crate::common::MetaClientProvider;
use crate::configs::Config;

pub type ClusterRef = Arc<Cluster>;
pub type ClusterDiscoveryRef = Arc<ClusterDiscovery>;

pub struct ClusterDiscovery {
    local_id: String,
    heartbeat: Mutex<ClusterHeartbeat>,
    api_provider: Arc<dyn NamespaceApi>,
}

impl ClusterDiscovery {
    async fn create_meta_client(cfg: &Config) -> Result<Arc<dyn KVApi>> {
        let meta_api_provider = MetaClientProvider::new(cfg);
        match meta_api_provider.try_get_kv_client().await {
            Ok(client) => Ok(client),
            Err(cause) => Err(cause.add_message_back("(while create namespace api).")),
        }
    }

    pub async fn create_global(cfg: Config) -> Result<ClusterDiscoveryRef> {
        let local_id = GlobalUniqName::unique();
        let meta_client = ClusterDiscovery::create_meta_client(&cfg).await?;
        let (lift_time, provider) = Self::create_provider(&cfg, meta_client)?;

        Ok(Arc::new(ClusterDiscovery {
            local_id: local_id.clone(),
            api_provider: provider.clone(),
            heartbeat: Mutex::new(ClusterHeartbeat::create(lift_time, provider)),
        }))
    }

    fn create_provider(
        cfg: &Config,
        api: Arc<dyn KVApi>,
    ) -> Result<(Duration, Arc<dyn NamespaceApi>)> {
        // TODO: generate if tenant or namespace is empty
        let tenant = &cfg.query.tenant;
        let namespace = &cfg.query.namespace;
        let lift_time = Duration::from_secs(60);
        let namespace_manager = NamespaceMgr::new(api, tenant, namespace, lift_time)?;

        Ok((lift_time, Arc::new(namespace_manager)))
    }

    pub async fn discover(&self) -> Result<ClusterRef> {
        match self.api_provider.get_nodes().await {
            Err(cause) => Err(cause.add_message_back("(while namespace api get_nodes).")),
            Ok(cluster_nodes) => {
                let mut res = Vec::with_capacity(cluster_nodes.len());

                for node in &cluster_nodes {
                    res.push(Arc::new(node.clone()))
                }

                Ok(Cluster::create(res, self.local_id.clone()))
            }
        }
    }

    async fn drop_invalid_nodes(self: &Arc<Self>, node_info: &NodeInfo) -> Result<()> {
        let current_nodes_info = match self.api_provider.get_nodes().await {
            Ok(nodes) => nodes,
            Err(cause) => {
                return Err(cause.add_message_back("(while drop_invalid_nodes)"));
            }
        };

        for before_node in current_nodes_info {
            // Restart in a very short time(< heartbeat timeout) after abnormal shutdown, Which will
            // lead to some invalid information
            if before_node.flight_address.eq(&node_info.flight_address) {
                let drop_invalid_node = self.api_provider.drop_node(before_node.id, None);
                if let Err(cause) = drop_invalid_node.await {
                    log::warn!("Drop invalid node failure: {:?}", cause);
                }
            }
        }

        Ok(())
    }

    pub async fn unregister_to_metastore(self: &Arc<Self>) {
        let mut heartbeat = self.heartbeat.lock().await;

        if let Err(shutdown_failure) = heartbeat.shutdown().await {
            log::warn!(
                "Cannot shutdown namespace heartbeat, cause {:?}",
                shutdown_failure
            );
        }

        let drop_node = self.api_provider.drop_node(self.local_id.clone(), None);
        if let Err(drop_node_failure) = drop_node.await {
            log::warn!(
                "Cannot drop namespace node(while shutdown), cause {:?}",
                drop_node_failure
            );
        }
    }

    pub async fn register_to_metastore(self: &Arc<Self>, cfg: &Config) -> Result<()> {
        let cpus = cfg.query.num_cpus;
        // TODO: 0.0.0.0 || ::0
        let address = cfg.query.flight_api_address.clone();
        let node_info = NodeInfo::create(self.local_id.clone(), cpus, address);

        self.drop_invalid_nodes(&node_info).await?;
        match self.api_provider.add_node(node_info).await {
            Ok(_) => self.start_heartbeat().await,
            Err(cause) => Err(cause.add_message_back("(while namespace api add_node).")),
        }
    }

    async fn start_heartbeat(self: &Arc<Self>) -> Result<()> {
        let mut heartbeat = self.heartbeat.lock().await;
        heartbeat.start(self.local_id.clone());
        Ok(())
    }
}

pub struct Cluster {
    local_id: String,
    nodes: Vec<Arc<NodeInfo>>,
}

impl Cluster {
    pub fn create(nodes: Vec<Arc<NodeInfo>>, local_id: String) -> ClusterRef {
        Arc::new(Cluster { local_id, nodes })
    }

    pub fn empty() -> ClusterRef {
        Arc::new(Cluster {
            local_id: String::from(""),
            nodes: Vec::new(),
        })
    }

    pub fn is_empty(&self) -> bool {
        self.nodes.len() <= 1
    }

    pub fn is_local(&self, node: &NodeInfo) -> bool {
        node.id == self.local_id
    }

    pub fn local_id(&self) -> String {
        self.local_id.clone()
    }

    pub async fn create_node_conn(&self, name: &str, config: &Config) -> Result<FlightClient> {
        for node in &self.nodes {
            if node.id == name {
                return match config.tls_query_cli_enabled() {
                    true => Ok(FlightClient::new(FlightServiceClient::new(
                        ConnectionFactory::create_flight_channel(
                            node.flight_address.clone(),
                            None,
                            Some(config.tls_query_client_conf()),
                        )?,
                    ))),
                    false => Ok(FlightClient::new(FlightServiceClient::new(
                        ConnectionFactory::create_flight_channel(
                            node.flight_address.clone(),
                            None,
                            None,
                        )?,
                    ))),
                };
            }
        }

        Err(ErrorCode::NotFoundClusterNode(format!(
            "The node \"{}\" not found in the cluster",
            name
        )))
    }

    pub fn get_nodes(&self) -> Vec<Arc<NodeInfo>> {
        self.nodes.to_vec()
    }
}

struct ClusterHeartbeat {
    timeout: Duration,
    shutdown: Arc<AtomicBool>,
    shutdown_notify: Arc<Notify>,
    namespace_api: Arc<dyn NamespaceApi>,
    shutdown_handler: Option<JoinHandle<()>>,
}

impl ClusterHeartbeat {
    pub fn create(timeout: Duration, namespace_api: Arc<dyn NamespaceApi>) -> ClusterHeartbeat {
        ClusterHeartbeat {
            timeout,
            namespace_api,
            shutdown: Arc::new(AtomicBool::new(false)),
            shutdown_notify: Arc::new(Notify::new()),
            shutdown_handler: None,
        }
    }

    fn heartbeat_loop(&self, local_id: String) -> impl Future<Output = ()> + 'static {
        let shutdown = self.shutdown.clone();
        let shutdown_notify = self.shutdown_notify.clone();
        let namespace_api = self.namespace_api.clone();
        let sleep_range = self.heartbeat_interval(self.timeout);

        async move {
            let mut shutdown_notified = Box::pin(shutdown_notify.notified());

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
                        let heartbeat = namespace_api.heartbeat(local_id.clone(), None);
                        if let Err(failure) = heartbeat.await {
                            log::error!("Cluster namespace api heartbeat failure: {:?}", failure);
                        }
                    }
                }
            }
        }
    }

    fn heartbeat_interval(&self, duration: Duration) -> RangeInclusive<u128> {
        (duration / 3).as_millis()..=((duration / 3) * 2).as_millis()
    }

    pub fn start(&mut self, local_id: String) {
        self.shutdown_handler = Some(tokio::spawn(self.heartbeat_loop(local_id)));
    }

    pub async fn shutdown(&mut self) -> Result<()> {
        if let Some(shutdown_handler) = self.shutdown_handler.take() {
            self.shutdown.store(true, Ordering::Relaxed);
            self.shutdown_notify.notify_waiters();
            if let Err(shutdown_failure) = shutdown_handler.await {
                return Err(ErrorCode::TokioError(format!(
                    "Cannot shutdown namespace heartbeat, cause {:?}",
                    shutdown_failure
                )));
            }
        }
        Ok(())
    }
}
