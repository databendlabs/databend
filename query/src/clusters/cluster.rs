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

use std::sync::Arc;
use std::time::Duration;

use common_arrow::arrow_flight::flight_service_client::FlightServiceClient;
use common_exception::ErrorCode;
use common_exception::Result;
use common_management::NamespaceApi;
use common_management::NamespaceMgr;
use common_management::NodeInfo;
use common_runtime::tokio;
use common_runtime::tokio::time::sleep as tokio_async_sleep;
use common_store_api_sdk::ConnectionFactory;
use common_store_api_sdk::KVApi;
use rand::thread_rng;
use rand::Rng;

use crate::api::FlightClient;
use crate::common::StoreApiProvider;
use crate::configs::Config;

pub type ClusterRef = Arc<Cluster>;
pub type ClusterDiscoveryRef = Arc<ClusterDiscovery>;

pub struct ClusterDiscovery {
    local_id: String,
    heartbeat: ClusterHeartbeat,
    api_provider: Arc<dyn NamespaceApi>,
}

impl ClusterDiscovery {
    async fn create_store_client(cfg: &Config) -> Result<Arc<dyn KVApi>> {
        let store_api_provider = StoreApiProvider::new(cfg);
        match store_api_provider.try_get_kv_client().await {
            Ok(client) => Ok(client),
            Err(cause) => Err(cause.add_message_back("(while create namespace api).")),
        }
    }

    pub async fn create_global(cfg: Config) -> Result<ClusterDiscoveryRef> {
        let local_id = global_unique_id();
        let store_client = ClusterDiscovery::create_store_client(&cfg).await?;
        let (lift_time, provider) = Self::create_provider(&cfg, store_client)?;

        Ok(Arc::new(ClusterDiscovery {
            local_id: local_id.clone(),
            api_provider: provider.clone(),
            heartbeat: ClusterHeartbeat::create(lift_time, local_id, provider),
        }))
    }

    fn create_provider(
        cfg: &Config,
        api: Arc<dyn KVApi>,
    ) -> Result<(Duration, Arc<dyn NamespaceApi>)> {
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

                println!("Discover cluster nodes {:?}", res);
                Ok(Cluster::create(res, self.local_id.clone()))
            }
        }
    }

    pub async fn register_to_metastore(self: &Arc<Self>, cfg: &Config) -> Result<()> {
        let cpus = cfg.query.num_cpus;
        let address = cfg.query.flight_api_address.clone();
        let node_info = NodeInfo::create(self.local_id.clone(), cpus, address);

        // TODO: restart node
        match self.api_provider.add_node(node_info).await {
            Ok(_) => self.heartbeat.startup(),
            Err(cause) => Err(cause.add_message_back("(while namespace api add_node).")),
        }
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

fn global_unique_id() -> String {
    let mut uuid = uuid::Uuid::new_v4().as_u128();
    let mut unique_id = Vec::with_capacity(22);

    loop {
        let m = (uuid % 62) as u8;
        uuid /= 62;

        match m as u8 {
            0..=9 => unique_id.push((b'0' + m) as char),
            10..=35 => unique_id.push((b'a' + (m - 10)) as char),
            36..=61 => unique_id.push((b'A' + (m - 36)) as char),
            unreachable => unreachable!("Unreachable branch m = {}", unreachable),
        }

        if uuid == 0 {
            return unique_id.iter().collect();
        }
    }
}

struct ClusterHeartbeat {
    lift_time: Duration,
    local_node_id: String,
    provider: Arc<dyn NamespaceApi>,
}

impl ClusterHeartbeat {
    pub fn create(
        lift_time: Duration,
        local_node_id: String,
        provider: Arc<dyn NamespaceApi>,
    ) -> ClusterHeartbeat {
        ClusterHeartbeat {
            lift_time,
            local_node_id,
            provider,
        }
    }

    pub fn startup(&self) -> Result<()> {
        let sleep_time = self.lift_time;
        let local_node_id = self.local_node_id.clone();
        let provider = self.provider.clone();

        tokio::spawn(async move {
            loop {
                let min_sleep_time = sleep_time / 3;
                let max_sleep_time = min_sleep_time * 2;
                let sleep_range = min_sleep_time.as_millis()..=max_sleep_time.as_millis();

                let mills = {
                    let mut rng = thread_rng();
                    rng.gen_range(sleep_range)
                };

                tokio_async_sleep(Duration::from_millis(mills as u64)).await;

                if let Err(cause) = provider.heartbeat(local_node_id.clone(), None).await {
                    log::error!("Cluster Heartbeat failure: {:?}", cause);
                }
            }
        });

        Ok(())
    }
}
