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
use common_base::DummySignalStream;
use common_base::GlobalUniqName;
use common_base::SignalStream;
use common_base::SignalType;
use common_exception::ErrorCode;
use common_exception::Result;
use common_grpc::ConnectionFactory;
use common_management::ClusterApi;
use common_management::ClusterMgr;
use common_meta_api::KVApi;
use common_meta_types::NodeInfo;
use common_tracing::tracing;
use futures::future::select;
use futures::future::Either;
use futures::Future;
use futures::StreamExt;
use rand::thread_rng;
use rand::Rng;

use crate::api::FlightClient;
use crate::common::MetaClientProvider;
use crate::configs::Config;

pub struct ClusterDiscovery {
    local_id: String,
    heartbeat: Mutex<ClusterHeartbeat>,
    api_provider: Arc<dyn ClusterApi>,
}

impl ClusterDiscovery {
    async fn create_meta_client(cfg: &Config) -> Result<Arc<dyn KVApi>> {
        let meta_api_provider = MetaClientProvider::new(cfg.meta.to_grpc_client_config());
        match meta_api_provider.try_get_kv_client().await {
            Ok(client) => Ok(client),
            Err(cause) => Err(cause.add_message_back("(while create cluster api).")),
        }
    }

    pub async fn create_global(cfg: Config) -> Result<Arc<ClusterDiscovery>> {
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
    ) -> Result<(Duration, Arc<dyn ClusterApi>)> {
        // TODO: generate if tenant or cluster id is empty
        let tenant_id = &cfg.query.tenant_id;
        let cluster_id = &cfg.query.cluster_id;
        let lift_time = Duration::from_secs(60);
        let cluster_manager = ClusterMgr::create(api, tenant_id, cluster_id, lift_time)?;

        Ok((lift_time, Arc::new(cluster_manager)))
    }

    pub async fn discover(&self) -> Result<Arc<Cluster>> {
        match self.api_provider.get_nodes().await {
            Err(cause) => Err(cause.add_message_back("(while cluster api get_nodes).")),
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
                    tracing::warn!("Drop invalid node failure: {:?}", cause);
                }
            }
        }

        Ok(())
    }

    pub async fn unregister_to_metastore(self: &Arc<Self>, signal: &mut SignalStream) {
        let mut heartbeat = self.heartbeat.lock().await;

        if let Err(shutdown_failure) = heartbeat.shutdown().await {
            tracing::warn!(
                "Cannot shutdown cluster heartbeat, cause {:?}",
                shutdown_failure
            );
        }

        let mut mut_signal_pin = signal.as_mut();
        let signal_future = Box::pin(mut_signal_pin.next());
        let drop_node = Box::pin(self.api_provider.drop_node(self.local_id.clone(), None));
        match futures::future::select(drop_node, signal_future).await {
            Either::Left((drop_node_result, _)) => {
                if let Err(drop_node_failure) = drop_node_result {
                    tracing::warn!(
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

    pub async fn register_to_metastore(self: &Arc<Self>, cfg: &Config) -> Result<()> {
        let cpus = cfg.query.num_cpus;
        // TODO: 127.0.0.1 || ::0
        let address = cfg.query.flight_api_address.clone();
        let node_info = NodeInfo::create(self.local_id.clone(), cpus, address);

        self.drop_invalid_nodes(&node_info).await?;
        match self.api_provider.add_node(node_info).await {
            Ok(_) => self.start_heartbeat().await,
            Err(cause) => Err(cause.add_message_back("(while cluster api add_node).")),
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
    pub fn create(nodes: Vec<Arc<NodeInfo>>, local_id: String) -> Arc<Cluster> {
        Arc::new(Cluster { local_id, nodes })
    }

    pub fn empty() -> Arc<Cluster> {
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
                        ConnectionFactory::create_rpc_channel(
                            node.flight_address.clone(),
                            None,
                            Some(config.tls_query_client_conf()),
                        )?,
                    ))),
                    false => Ok(FlightClient::new(FlightServiceClient::new(
                        ConnectionFactory::create_rpc_channel(
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
    cluster_api: Arc<dyn ClusterApi>,
    shutdown_handler: Option<JoinHandle<()>>,
}

impl ClusterHeartbeat {
    pub fn create(timeout: Duration, cluster_api: Arc<dyn ClusterApi>) -> ClusterHeartbeat {
        ClusterHeartbeat {
            timeout,
            cluster_api,
            shutdown: Arc::new(AtomicBool::new(false)),
            shutdown_notify: Arc::new(Notify::new()),
            shutdown_handler: None,
        }
    }

    fn heartbeat_loop(&self, local_id: String) -> impl Future<Output = ()> + 'static {
        let shutdown = self.shutdown.clone();
        let shutdown_notify = self.shutdown_notify.clone();
        let cluster_api = self.cluster_api.clone();
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
                        let heartbeat = cluster_api.heartbeat(local_id.clone(), None);
                        if let Err(failure) = heartbeat.await {
                            tracing::error!("Cluster cluster api heartbeat failure: {:?}", failure);
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
                    "Cannot shutdown cluster heartbeat, cause {:?}",
                    shutdown_failure
                )));
            }
        }
        Ok(())
    }
}
