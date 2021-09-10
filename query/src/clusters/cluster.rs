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

use std::collections::hash_map::Entry::Occupied;
use std::collections::hash_map::Entry::Vacant;
use std::collections::HashMap;
use std::net::IpAddr;
use std::sync::Arc;

use common_exception::ErrorCode;
use common_exception::Result;
use common_flights::{DNSResolver, StoreClient};
use common_infallible::Mutex;

use crate::clusters::address::Address;
use crate::clusters::node::Node;
use crate::configs::Config;
use common_management::{NamespaceApi, NamespaceMgr, LocalKVStore, NodeInfo};

pub type ClusterRef = Arc<Cluster>;

pub struct Cluster {
    local_port: u16,
    nodes: Mutex<HashMap<String, Arc<Node>>>,
    local_id: String,
    provider: Mutex<Box<dyn NamespaceApi + Sync + Send>>,
}

impl Cluster {
    // TODO(Winter): this should be disabled by compile flag
    async fn standalone_without_metastore(cfg: &Config) -> Result<ClusterRef> {
        let local_store = LocalKVStore::new_temp();
        Ok(Arc::new(Cluster {
            local_port: Address::create(&cfg.query.flight_api_address)?.port(),
            nodes: Mutex::new(HashMap::new()),
            local_id: global_unique_id(),
            provider: Mutex::new(Box::new(NamespaceMgr::<LocalKVStore>::new(local_store.await?))),
        }))
    }

    async fn cluster_with_metastore(cfg: &Config) -> Result<ClusterRef> {
        let address = &cfg.meta.meta_address;
        let username = &cfg.meta.meta_username;
        let password = &cfg.meta.meta_password;
        let store_client = StoreClient::try_create(address, username, password);

        Ok(Arc::new(Cluster {
            local_port: Address::create(&cfg.query.flight_api_address)?.port(),
            nodes: Mutex::new(HashMap::new()),
            local_id: global_unique_id(),
            provider: Mutex::new(Box::new(NamespaceMgr::<StoreClient>::new(store_client.await?))),
        }))
    }

    pub async fn create_global(cfg: Config) -> Result<ClusterRef> {
        let cluster = match cfg.meta.meta_address.is_empty() {
            true => Self::standalone_without_metastore(&cfg).await?,
            false => Self::cluster_with_metastore(&cfg).await?,
        };

        cluster.register_to_metastore(&cfg).await;
        Ok(cluster)
    }

    pub async fn empty() -> ClusterRef {
        let local_store = LocalKVStore::new_temp();

        Arc::new(Cluster {
            local_port: 9090,
            nodes: Mutex::new(HashMap::new()),
            local_id: global_unique_id(),
            provider: Mutex::new(Box::new(NamespaceMgr::<LocalKVStore>::new(local_store.await.unwrap()))),
        })
    }

    pub fn is_empty(&self) -> Result<bool> {
        Ok(self.nodes.lock().len() == 0)
    }

    pub async fn add_node(&self, name: &str, priority: u8, address: &str) -> Result<()> {
        let address = Address::create(address)?;
        let address_is_local = is_local(&address, self.local_port).await?;
        let mut nodes = self.nodes.lock();
        let new_node_sequence = nodes.len();

        match nodes.entry(name.to_string()) {
            Occupied(_) => Err(ErrorCode::DuplicateClusterNode(format!(
                "The node \"{}\" already exists in the cluster",
                name
            ))),
            Vacant(entry) => {
                entry.insert(Arc::new(Node::create(
                    name.to_string(),
                    priority,
                    address.clone(),
                    address_is_local,
                    new_node_sequence,
                )?));

                Ok(())
            }
        }
    }

    pub fn remove_node(&self, name: String) -> Result<()> {
        match self.nodes.lock().remove(&*name) {
            Some(_) => Ok(()),
            None => Err(ErrorCode::NotFoundClusterNode(format!(
                "The node \"{}\" not found in the cluster",
                name
            ))),
        }
    }

    pub fn get_node_by_name(&self, name: String) -> Result<Arc<Node>> {
        self.nodes
            .lock()
            .get(&name)
            .map(Clone::clone)
            .ok_or_else(|| {
                ErrorCode::NotFoundClusterNode(format!(
                    "The node \"{}\" not found in the cluster",
                    name
                ))
            })
    }

    pub fn get_nodes(&self) -> Result<Vec<Arc<Node>>> {
        let mut nodes = self
            .nodes
            .lock()
            .iter()
            .map(|(_, node)| node.clone())
            .collect::<Vec<_>>();
        nodes.sort_by(|left, right| left.sequence.cmp(&right.sequence));
        Ok(nodes)
    }

    pub async fn register_to_metastore(&self, cfg: &Config) -> Result<()> {
        let tenant_id = cfg.query.tenant.clone();
        let namespace_id = cfg.query.namespace.clone();
        let mut api_provider = self.provider.lock();

        let cpus = cfg.query.num_cpus;
        let address = cfg.query.flight_api_address.clone();
        let node_info = NodeInfo::create(self.local_id.clone(), cpus, address);
        api_provider.add_node(tenant_id, namespace_id, node_info).await?;
        Ok(())
    }
}

fn global_unique_id() -> String {
    let mut uuid = uuid::Uuid::new_v4().as_u128();
    let mut unique_id = String::from("");

    loop {
        let m = uuid % 36;
        uuid = uuid / 36;

        unique_id.push(std::char::from_digit(m as u32, 36).unwrap());
        if uuid == 0 {
            return unique_id;
        }
    }
}

async fn is_local(address: &Address, expect_port: u16) -> Result<bool> {
    if address.port() != expect_port {
        return Result::Ok(false);
    }

    match address {
        Address::SocketAddress(socket_addr) => is_local_impl(&socket_addr.ip()),
        Address::Named((host, _)) => match DNSResolver::instance()?.resolve(host.as_str()).await {
            Err(error) => Result::Err(ErrorCode::DnsParseError(format!(
                "DNS resolver lookup error: {}",
                error
            ))),
            Ok(resolved_ips) => {
                for resolved_ip in &resolved_ips {
                    if is_local_impl(resolved_ip)? {
                        return Ok(true);
                    }
                }

                Ok(false)
            }
        },
    }
}

fn is_local_impl(address: &IpAddr) -> Result<bool> {
    for network_interface in &pnet::datalink::interfaces() {
        for interface_ip in &network_interface.ips {
            if address == &interface_ip.ip() {
                return Ok(true);
            }
        }
    }

    Ok(false)
}
