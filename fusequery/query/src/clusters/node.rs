use std::fmt::Formatter;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use futures::{Future, FutureExt, TryFutureExt};
use futures::stream::Stream;
use serde::{Deserializer, Serializer};
use serde::de::{Error, MapAccess, Unexpected, Visitor};
use serde::ser::SerializeStruct;
use tokio_stream::StreamExt;
use tonic::transport::{Channel, Endpoint, Uri};
use tower::buffer::Buffer;
use tower::util::Either;
use warp::hyper::client::HttpConnector;

use common_arrow::arrow_flight::flight_service_client::FlightServiceClient;
use common_datavalues::DataSchemaRef;
// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.
use common_exception::{ErrorCodes, Result};
use common_infallible::{Mutex, RwLock};
use common_streams::SendableDataBlockStream;

use crate::api::ExecutePlanWithShuffleAction;
use crate::api::FlightClient;

use super::address::Address;

#[derive(Debug)]
pub struct Node {
    pub name: String,
    // Node priority is in [0,10]
    // larger value means higher priority
    pub priority: u8,
    pub address: Address,
    pub local: bool,
    pub sequence: usize,
    channel_generator: Arc<Mutex<ChannelGenerator>>,
}

impl PartialEq for Node {
    fn eq(&self, other: &Self) -> bool {
        self.name == other.name && self.priority == other.priority && self.address == other.address
            && self.local == other.local
    }
}

impl Node {
    pub fn create(name: String, priority: u8, address: Address, local: bool, sequence: usize) -> Result<Node> {
        Ok(Node {
            name,
            priority,
            address: address.clone(),
            local,
            sequence,
            channel_generator: Arc::new(Mutex::new(ChannelGenerator::create(address))),
        })
    }

    pub fn is_local(&self) -> bool {
        self.local
    }

    pub fn get_flight_client(&self) -> Result<FlightClient> {
        let channel = self.channel_generator.lock().generate();
        channel.map(|channel| FlightClient::new(FlightServiceClient::new(channel.clone())))
    }
}

impl serde::Serialize for Node {
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
        where S: Serializer
    {
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
        where D: Deserializer<'de>
    {
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
            Err(error) => Err(D::Error::custom(error))
        }
    }
}

#[derive(Debug, Clone)]
struct ChannelGenerator {
    address: Option<Address>,
    channel: (Option<Channel>, Option<Arc<ErrorCodes>>),
}

impl ChannelGenerator {
    pub fn create(address: Address) -> ChannelGenerator {
        ChannelGenerator {
            address: Some(address),
            channel: (None, None),
        }
    }

    pub fn generate(&mut self) -> Result<Channel> {
        if let Some(address) = self.address.take() {
            let channel = futures::executor::block_on(Self::create_flight_channel(address));

            match channel {
                Ok(channel) => self.channel = (Some(channel), None),
                Err(error) => self.channel = (None, Some(Arc::new(error)))
            }
        }

        match &self.channel {
            (Some(channel), None) => Ok(channel.clone()),
            (None, Some(error)) => Err(ErrorCodes::create(error.code(), error.message(), error.backtrace.clone())),
            _ => Err(ErrorCodes::LogicalError("Logical error: Unreachable code for ChannelGenerator.")),
        }
    }

    async fn create_flight_channel(addr: Address) -> Result<Channel> {
        /// Hack: block with connect
        /// There's no better way. limited by 'connect_with_connector'(no 'lazy_connect_with_connector')
        match format!("http://{}", addr.to_string()).parse::<Uri>() {
            Err(error) => Result::Err(ErrorCodes::BadAddressFormat(format!("Node address format is not parse: {}", error))),
            Ok(uri) => {
                let mut inner_connector = HttpConnector::new();
                inner_connector.set_nodelay(true);
                inner_connector.set_keepalive(None);
                inner_connector.enforce_http(false);

                match Channel::builder(uri).connect_with_connector(inner_connector).await {
                    Ok(channel) => Result::Ok(channel),
                    Err(error) => Result::Err(ErrorCodes::CannotConnectNode(format!("Node address format is not parse: {}", error))),
                }
            }
        }
    }
}


