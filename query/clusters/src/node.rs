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

use common_arrow::arrow_format::flight::service::flight_service_client::FlightServiceClient;
use common_exception::Result;
use common_store_api_sdk::ConnectionFactory;
use serde::de::Error;
use serde::Deserializer;
use serde::Serializer;

use super::address::Address;
use crate::api::FlightClient;
use crate::configs::Config;

#[derive(Debug)]
pub struct Node {
    pub name: String,
    // Node priority is in [0,10]
    // larger value means higher priority
    pub priority: u8,
    pub address: Address,
    pub local: bool,
    pub sequence: usize,
}

impl PartialEq for Node {
    fn eq(&self, other: &Self) -> bool {
        self.name == other.name
            && self.priority == other.priority
            && self.address == other.address
            && self.local == other.local
    }
}

impl Node {
    pub fn create(
        name: String,
        priority: u8,
        address: Address,
        local: bool,
        sequence: usize,
    ) -> Result<Node> {
        Ok(Node {
            name,
            priority,
            address,
            local,
            sequence,
        })
    }

    pub fn is_local(&self) -> bool {
        self.local
    }

    pub async fn get_flight_client(&self, conf: &Config) -> Result<FlightClient> {
        let tls_conf = if conf.tls_query_cli_enabled() {
            Some(conf.tls_query_client_conf())
        } else {
            None
        };

        let channel =
            ConnectionFactory::create_flight_channel(self.address.clone(), None, tls_conf);
        channel.map(|channel| FlightClient::new(FlightServiceClient::new(channel)))
    }
}

impl serde::Serialize for Node {
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where S: Serializer {
        #[derive(serde::Serialize, serde::Deserialize)]
        struct NodeSerializeView {
            name: String,
            priority: u8,
            address: Address,
            local: bool,
            sequence: usize,
        }

        NodeSerializeView::serialize(
            &NodeSerializeView {
                name: self.name.clone(),
                priority: self.priority,
                address: self.address.clone(),
                local: self.local,
                sequence: self.sequence,
            },
            serializer,
        )
    }
}

impl<'de> serde::Deserialize<'de> for Node {
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where D: Deserializer<'de> {
        #[derive(serde::Serialize, serde::Deserialize)]
        struct NodeDeserializeView {
            pub name: String,
            pub priority: u8,
            pub address: Address,
            pub local: bool,
            pub sequence: usize,
        }

        let node_deserialize_view = NodeDeserializeView::deserialize(deserializer)?;
        let deserialize_result = Node::create(
            node_deserialize_view.name.clone(),
            node_deserialize_view.priority,
            node_deserialize_view.address.clone(),
            node_deserialize_view.local,
            node_deserialize_view.sequence,
        );

        match deserialize_result {
            Ok(node) => Ok(node),
            Err(error) => Err(D::Error::custom(error)),
        }
    }
}
