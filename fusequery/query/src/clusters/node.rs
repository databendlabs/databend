// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.
use common_exception::{Result, ErrorCodes};
use crate::api::FlightClient;
use common_infallible::{RwLock, Mutex};
use std::sync::Arc;
use futures::Future;
use super::address::Address;
use common_streams::SendableDataBlockStream;
use common_datavalues::DataSchemaRef;
use serde::{Serializer, Deserializer};
use serde::ser::SerializeStruct;
use serde::de::{Visitor, Unexpected, Error, MapAccess};
use std::fmt::Formatter;
use crate::api::ExecutePlanWithShuffleAction;
use std::pin::Pin;
use tonic::transport::Channel;
use common_arrow::arrow_flight::flight_service_client::FlightServiceClient;

#[derive(serde::Serialize, serde::Deserialize)]
pub struct Node {
    pub name: String,
    // Node priority is in [0,10]
    // larger value means higher priority
    pub priority: u8,
    pub address: Address,
    pub local: bool,
    pub sequence: usize,

    #[serde(skip)]
    flight_channel: Option<Channel>,
    // flight_client: Mutex<Option<FlightClient>>,
}

impl PartialEq for Node {
    fn eq(&self, other: &Self) -> bool {
        self.name == other.name && self.priority == other.priority && self.address == other.address
            && self.local == other.local
    }
}

type FlightFuture<T> = Pin<Box<dyn Future<Output=Result<T>> + Send>>;

impl Node {
    pub fn create(name: String, priority: u8, address: Address, local: bool, sequence: usize) -> Arc<Node> {
        Arc::new(Node {
            name,
            priority,
            address,
            local,
            sequence,
            flight_channel: None,
        })
    }

    pub fn is_local(&self) -> bool {
        self.local
    }

    pub async fn prepare_query_stage(&self, action: ExecutePlanWithShuffleAction, timeout: u64) -> Result<()> {
        let inner_client = FlightServiceClient::new(self.flight_channel.clone().unwrap());
        let mut flight_client = FlightClient::new(inner_client);
        flight_client.prepare_query_stage(action, timeout).await
    }

    pub async fn fetch_stream(&self, stream_name: String, schema: DataSchemaRef, timeout: u64) -> Result<SendableDataBlockStream>
    {
        let inner_client = FlightServiceClient::new(self.flight_channel.clone().unwrap());
        let mut flight_client = FlightClient::new(inner_client);
        flight_client.fetch_stream(stream_name, schema, timeout).await
    }
}