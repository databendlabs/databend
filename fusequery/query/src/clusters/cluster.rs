// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use std::collections::HashMap;
use std::sync::Arc;

use common_exception::{Result, ErrorCodes};
use common_infallible::Mutex;

use crate::clusters::node::Node;
use crate::configs::Config;
use std::collections::hash_map::Entry::{Occupied, Vacant};
use std::net::{ToSocketAddrs, Ipv4Addr, SocketAddr, IpAddr};
use trust_dns_resolver::{Resolver, TokioAsyncResolver};
use warp::hyper::client::connect::dns::GaiResolver;
use crate::clusters::address::Address;
use pnet::datalink::NetworkInterface;

pub type ClusterRef = Arc<Cluster>;

pub struct Cluster {
    cfg: Config,
    nodes: Mutex<HashMap<String, Arc<Node>>>,
}

impl Cluster {
    pub fn create_global(cfg: Config) -> ClusterRef {
        Arc::new(Cluster {
            cfg,
            nodes: Mutex::new(HashMap::new()),
        })
    }

    pub fn empty() -> ClusterRef {
        Arc::new(Cluster {
            cfg: Config::default(),
            nodes: Mutex::new(HashMap::new()),
        })
    }

    pub fn make_query_cluster(&self) -> ClusterRef {
        Arc::new(Cluster {
            cfg: self.cfg.clone(),
            nodes: Mutex::new(self.nodes.lock().clone()),
        })
    }

    pub fn is_empty(&self) -> Result<bool> {
        Ok(self.nodes.lock().len() == 0)
    }

    pub async fn add_node(&self, name: &String, priority: u8, address: &String) -> Result<()> {
        let address = Address::create(address)?;
        let listener_address = Address::create(&self.cfg.flight_api_address)?;
        let local = is_local(&address, listener_address.port()).await?;

        let mut nodes = self.nodes.lock();
        let new_node_sequence = nodes.len();

        match nodes.entry(name.clone()) {
            Occupied(_) => {
                Err(ErrorCodes::DuplicateClusterNode(format!(
                    "The node \"{}\" already exists in the cluster",
                    name
                )))
            },
            Vacant(entry) => {
                entry.insert(Arc::new(
                    Node {
                        name: name.clone(),
                        priority: priority,
                        address: address.clone(),
                        local: local,
                        sequence: new_node_sequence,
                    }
                ));

                Ok(())
            },
        }
    }

    pub fn remove_node(&self, name: String) -> Result<()> {
        match self.nodes.lock().remove(&*name) {
            Some(_) => Ok(()),
            None => {
                Err(ErrorCodes::NotFoundClusterNode(format!(
                    "The node \"{}\" not found in the cluster",
                    name
                )))
            },
        }
    }

    pub fn get_node_by_name(&self, name: String) -> Result<Arc<Node>> {
        self.nodes.lock().get(&name).map(|x| x.clone()).ok_or_else(|| {
            ErrorCodes::NotFoundClusterNode(format!(
                "The node \"{}\" not found in the cluster",
                name
            ))
        })
    }

    pub fn get_nodes(&self) -> Result<Vec<Arc<Node>>> {
        let mut nodes = self.nodes.lock().iter().map(|(_, node)| node.clone()).collect::<Vec<_>>();
        nodes.sort_by(|left, right| left.sequence.cmp(&right.sequence));
        Ok(nodes)
    }
}

fn create_dns_resolver() -> Result<TokioAsyncResolver> {
    match TokioAsyncResolver::tokio_from_system_conf() {
        Ok(resolver) => Ok(resolver),
        Err(error) => Result::Err(ErrorCodes::DnsParseError("")),
    }
}

async fn is_local(address: &Address, expect_port: u16) -> Result<bool> {
    if address.port() != expect_port {
        return Result::Ok(false);
    }

    match address {
        Address::SocketAddress(socket_addr) => is_local_impl(socket_addr.ip()),
        Address::Named((host, port)) => {
            let dns_resolver = create_dns_resolver()?;
            match dns_resolver.lookup_ip(host.as_str()).await {
                Err(error) => Result::Err(ErrorCodes::DnsParseError("")),
                Ok(resolved_ip) => match resolved_ip.iter().next() {
                    Some(resolved_ip) => is_local_impl(resolved_ip),
                    None => Result::Err(ErrorCodes::DnsParseError("")),
                },
            }
        },
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
