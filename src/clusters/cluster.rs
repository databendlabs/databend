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
    nodes: Mutex<HashMap<String, Node>>,
}

impl Cluster {
    pub fn create(_cfg: Config) -> ClusterRef {
        Arc::new(Cluster {
            nodes: Mutex::new(HashMap::new()),
        })
    }

    pub fn empty() -> ClusterRef {
        Arc::new(Cluster {
            nodes: Mutex::new(HashMap::new()),
        })
    }

    pub fn add_node(&self, node: &Node) -> FuseQueryResult<()> {
        self.nodes.lock()?.insert(node.name.clone(), node.clone());
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
