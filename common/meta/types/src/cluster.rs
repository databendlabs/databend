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

use std::convert::TryFrom;
use std::fmt;
use std::net::SocketAddr;
use std::str::FromStr;

use async_raft::NodeId;
use common_exception::exception::ErrorCode;
use common_exception::exception::Result;
use serde::Deserialize;
use serde::Serialize;

/// A slot is a virtual and intermediate allocation unit in a distributed storage.
/// The key of an object is mapped to a slot by some hashing algo.
/// A slot is assigned to several physical servers(normally 3 for durability).
#[derive(Serialize, Deserialize, Debug, Default, Clone)]
pub struct Slot {
    pub node_ids: Vec<NodeId>,
}

#[derive(Serialize, Deserialize, Debug, Default, Clone, PartialEq)]
pub struct Node {
    pub name: String,
    pub address: String,
}

impl fmt::Display for Node {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}={}", self.name, self.address)
    }
}

/// Query node
#[derive(
    serde::Serialize, serde::Deserialize, Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Default,
)]
pub struct NodeInfo {
    #[serde(default)]
    pub id: String,
    #[serde(default)]
    pub cpu_nums: u64,
    #[serde(default)]
    pub version: u32,
    #[serde(default)]
    pub flight_address: String,
}

impl TryFrom<Vec<u8>> for NodeInfo {
    type Error = ErrorCode;

    fn try_from(value: Vec<u8>) -> Result<Self> {
        match serde_json::from_slice(&value) {
            Ok(user_info) => Ok(user_info),
            Err(serialize_error) => Err(ErrorCode::IllegalUserInfoFormat(format!(
                "Cannot deserialize namespace from bytes. cause {}",
                serialize_error
            ))),
        }
    }
}

impl NodeInfo {
    pub fn create(id: String, cpu_nums: u64, flight_address: String) -> NodeInfo {
        NodeInfo {
            id,
            cpu_nums,
            version: 0,
            flight_address,
        }
    }

    pub fn ip_port(&self) -> Result<(String, u16)> {
        let addr = SocketAddr::from_str(&self.flight_address)?;

        Ok((addr.ip().to_string(), addr.port()))
    }
}
