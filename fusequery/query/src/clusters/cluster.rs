// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use std::collections::hash_map::Entry::Occupied;
use std::collections::hash_map::Entry::Vacant;
use std::collections::HashMap;
use std::net::IpAddr;
use std::sync::Arc;

use common_exception::ErrorCodes;
use common_exception::Result;
use common_infallible::Mutex;
use trust_dns_resolver::TokioAsyncResolver;

use crate::clusters::address::Address;
use crate::clusters::node::Node;
use crate::configs::Config;

pub type ClusterRef = Arc<Cluster>;

pub struct Cluster {
    local_port: u16,
    nodes: Mutex<HashMap<String, Arc<Node>>>
}

impl Cluster {
    pub fn create_global(cfg: Config) -> Result<ClusterRef> {
        Ok(Arc::new(Cluster {
            nodes: Mutex::new(HashMap::new()),
            local_port: Address::create(&cfg.flight_api_address)?.port()
        }))
    }

    pub fn empty() -> ClusterRef {
        Arc::new(Cluster {
            local_port: 9090,
            nodes: Mutex::new(HashMap::new())
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
            Occupied(_) => Err(ErrorCodes::DuplicateClusterNode(format!(
                "The node \"{}\" already exists in the cluster",
                name
            ))),
            Vacant(entry) => {
                entry.insert(Arc::new(Node::create(
                    name.to_string(),
                    priority,
                    address.clone(),
                    address_is_local,
                    new_node_sequence
                )?));

                Ok(())
            }
        }
    }

    pub fn remove_node(&self, name: String) -> Result<()> {
        match self.nodes.lock().remove(&*name) {
            Some(_) => Ok(()),
            None => Err(ErrorCodes::NotFoundClusterNode(format!(
                "The node \"{}\" not found in the cluster",
                name
            )))
        }
    }

    pub fn get_node_by_name(&self, name: String) -> Result<Arc<Node>> {
        self.nodes
            .lock()
            .get(&name)
            .map(Clone::clone)
            .ok_or_else(|| {
                ErrorCodes::NotFoundClusterNode(format!(
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
}

fn create_dns_resolver() -> Result<TokioAsyncResolver> {
    match TokioAsyncResolver::tokio_from_system_conf() {
        Ok(resolver) => Ok(resolver),
        Err(error) => Result::Err(ErrorCodes::DnsParseError(format!(
            "DNS resolver create error: {}",
            error
        )))
    }
}

async fn is_local(address: &Address, expect_port: u16) -> Result<bool> {
    if address.port() != expect_port {
        return Result::Ok(false);
    }

    match address {
        Address::SocketAddress(socket_addr) => is_local_impl(socket_addr.ip()),
        Address::Named((host, _)) => {
            let dns_resolver = create_dns_resolver()?;
            match dns_resolver.lookup_ip(host.as_str()).await {
                Err(error) => Result::Err(ErrorCodes::DnsParseError(format!(
                    "DNS resolver lookup error: {}",
                    error
                ))),
                Ok(resolved_ip) => match resolved_ip.iter().next() {
                    Some(resolved_ip) => is_local_impl(resolved_ip),
                    None => Result::Err(ErrorCodes::DnsParseError(
                        "Resolved hostname must be IPv4 or IPv6"
                    ))
                }
            }
        }
    }
}

fn is_local_impl(address: IpAddr) -> Result<bool> {
    for network_interface in &pnet::datalink::interfaces() {
        for interface_ip in &network_interface.ips {
            if address == interface_ip.ip() {
                return Ok(true);
            }
        }
    }

    Ok(false)
}
