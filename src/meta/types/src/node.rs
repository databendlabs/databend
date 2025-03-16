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

use std::fmt;

use serde::Deserialize;
use serde::Serialize;

use crate::Endpoint;

#[derive(Serialize, Deserialize, Debug, Default, Clone, PartialEq, Eq, deepsize::DeepSizeOf)]
pub struct Node {
    /// Node name for display.
    pub name: String,

    /// Raft service endpoint to connect to.
    pub endpoint: Endpoint,

    /// For backward compatibility, it can not be removed.
    /// 2023-02-09
    //#[deprecated(note = "it is listening addr, not advertise addr")]
    #[serde(skip)]
    pub grpc_api_addr: Option<String>,

    /// The address `ip:port` for a meta-client to connect to.
    pub grpc_api_advertise_address: Option<String>,
}

impl Node {
    pub fn new(name: impl ToString, endpoint: Endpoint) -> Self {
        Self {
            name: name.to_string(),
            endpoint,
            ..Default::default()
        }
    }

    pub fn with_grpc_advertise_address(mut self, g: Option<impl ToString>) -> Self {
        self.grpc_api_advertise_address = g.map(|x| x.to_string());
        self
    }
}

impl fmt::Display for Node {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let grpc_addr_display = if let Some(grpc_addr) = &self.grpc_api_advertise_address {
            grpc_addr.to_string()
        } else {
            "".to_string()
        };
        write!(
            f,
            "id={} raft={} grpc={}",
            self.name, self.endpoint, grpc_addr_display
        )
    }
}
