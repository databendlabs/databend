// Copyright 2020-2021 The FuseQuery Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use crate::clusters::Node;
use crate::configs::Config;
use crate::error::FuseQueryResult;

pub type ClusterRef = Arc<Cluster>;

pub struct Cluster {
    cfg: Config,
    nodes: Mutex<HashMap<String, Node>>,
}

impl Cluster {
    pub fn create(cfg: Config) -> ClusterRef {
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

    pub fn is_empty(&self) -> FuseQueryResult<bool> {
        Ok(self.nodes.lock()?.len() == 0)
    }

    pub fn add_node(&self, n: &Node) -> FuseQueryResult<()> {
        let mut node = Node {
            name: n.name.clone(),
            cpus: n.cpus,
            address: n.address.clone(),
            local: false,
        };
        if node.address == self.cfg.rpc_api_address {
            node.local = true;
        }
        self.nodes.lock()?.insert(node.name.clone(), node);
        Ok(())
    }

    pub fn remove_node(&self, id: String) -> FuseQueryResult<()> {
        self.nodes.lock()?.remove(&*id);
        Ok(())
    }

    pub fn get_nodes(&self) -> FuseQueryResult<Vec<Node>> {
        let mut nodes = vec![];

        for (_, node) in self.nodes.lock()?.iter() {
            nodes.push(node.clone());
        }
        Ok(nodes)
    }
}
