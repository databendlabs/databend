// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use common_arrow::arrow_flight::flight_service_client::FlightServiceClient;
use common_exception::Result;
use common_flights::ConnectionFactory;
use serde::de::Error;
use serde::Deserializer;
use serde::Serializer;

use super::address::Address;
use crate::api::FlightClient;

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

    pub async fn get_flight_client(&self) -> Result<FlightClient> {
        let channel = ConnectionFactory::create_flight_channel(self.address.clone(), None).await;
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
