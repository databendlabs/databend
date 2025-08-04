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

use std::net::AddrParseError;
use std::net::SocketAddr;
use std::str::FromStr;

use serde::Deserialize;
use serde::Serialize;

#[derive(Serialize, Deserialize, Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Default)]
pub enum NodeType {
    #[default]
    SelfManaged,
    SystemManaged,
}

/// Query node
#[derive(Serialize, Deserialize, Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Default)]
#[serde(default)]
pub struct NodeInfo {
    pub id: String,
    pub secret: String,
    pub cpu_nums: u64,
    pub version: u32,
    pub http_address: String,
    pub flight_address: String,
    pub discovery_address: String,
    pub binary_version: String,
    pub node_type: NodeType,
    pub node_group: Option<String>,

    #[serde(skip_serializing_if = "String::is_empty")]
    pub cluster_id: String,
    #[serde(skip_serializing_if = "String::is_empty")]
    pub warehouse_id: String,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub runtime_node_group: Option<String>,

    pub cache_id: String,
}

impl NodeInfo {
    pub fn create(
        id: String,
        secret: String,
        http_address: String,
        flight_address: String,
        discovery_address: String,
        binary_version: String,
        cache_id: String,
    ) -> NodeInfo {
        let cpu_nums = num_cpus::get() as u64;
        NodeInfo {
            id,
            secret,
            cpu_nums,
            version: 0,
            http_address,
            flight_address,
            discovery_address,
            binary_version,
            cache_id,
            cluster_id: "".to_string(),
            warehouse_id: "".to_string(),
            node_type: NodeType::SystemManaged,
            node_group: None,
            runtime_node_group: None,
        }
    }

    pub fn ip_port(&self) -> Result<(String, u16), AddrParseError> {
        let addr = SocketAddr::from_str(&self.flight_address)?;

        Ok((addr.ip().to_string(), addr.port()))
    }

    pub fn assigned_warehouse(&self) -> bool {
        !self.warehouse_id.is_empty() && !self.cluster_id.is_empty()
    }

    // Unload the warehouse and cluster from the node.
    // 1. Used when a node is removed from the cluster.
    // 2. For cluster_node_key: node_info, since its warehouse and cluster are already encoded in the key, we do not need to write the warehouse and cluster into its value again.
    pub fn unload_warehouse_info(&self) -> NodeInfo {
        NodeInfo {
            id: self.id.clone(),
            secret: self.secret.clone(),
            cpu_nums: self.cpu_nums,
            version: self.version,
            http_address: self.http_address.clone(),
            flight_address: self.flight_address.clone(),
            discovery_address: self.discovery_address.clone(),
            binary_version: self.binary_version.clone(),
            node_type: self.node_type.clone(),
            node_group: self.node_group.clone(),
            cache_id: self.cache_id.clone(),
            cluster_id: String::new(),
            warehouse_id: String::new(),
            runtime_node_group: self.runtime_node_group.clone(),
        }
    }

    pub fn leave_warehouse(&self) -> NodeInfo {
        NodeInfo {
            id: self.id.clone(),
            secret: self.secret.clone(),
            cpu_nums: self.cpu_nums,
            version: self.version,
            http_address: self.http_address.clone(),
            flight_address: self.flight_address.clone(),
            discovery_address: self.discovery_address.clone(),
            binary_version: self.binary_version.clone(),
            node_type: self.node_type.clone(),
            node_group: self.node_group.clone(),
            cache_id: self.cache_id.clone(),
            cluster_id: String::new(),
            warehouse_id: String::new(),
            runtime_node_group: None,
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::node::Node;

    #[test]
    fn test_serde_node_compatible() -> anyhow::Result<()> {
        // Without `grpc_api_addr`
        let s1 = r#"{"name":"a","endpoint":{"addr":"b","port":3},"grpc_api_advertise_address":"grpc_advertise"}"#;
        let _n1: Node = serde_json::from_str(s1)?;
        // println!("{n:?}");

        // With `grpc_api_addr`
        let s2 = r#"{"name":"a","endpoint":{"addr":"b","port":3},"grpc_api_addr":"grpc_addr","grpc_api_advertise_address":"grpc_advertise"}"#;
        let _n2: Node = serde_json::from_str(s2)?;
        // println!("{n:?}");
        Ok(())
    }
}
