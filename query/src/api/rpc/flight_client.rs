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

use std::convert::TryInto;
use std::marker::PhantomData;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

use async_channel::{Receiver, Sender, TryRecvError};
use futures_util::StreamExt;
use common_arrow::arrow_format::flight::data::{Action, FlightData};
use common_arrow::arrow_format::flight::service::flight_service_client::FlightServiceClient;
use common_base::base::tokio::time::Duration;
use common_exception::ErrorCode;
use common_exception::Result;
use tonic::metadata::MetadataKey;
use tonic::metadata::MetadataValue;
use tonic::transport::channel::Channel;
use tonic::{Request, Status, Streaming};

use crate::api::rpc::flight_actions::FlightAction;
use crate::api::rpc::packets::DataPacket;
use crate::api::rpc::packets::DataPacketStream;
use crate::api::rpc::request_builder::RequestBuilder;

pub struct FlightClient {
    inner: FlightServiceClient<Channel>,
}

// TODO: Integration testing required
impl FlightClient {
    pub fn new(inner: FlightServiceClient<Channel>) -> FlightClient {
        FlightClient { inner }
    }

    pub async fn execute_action(&mut self, action: FlightAction, timeout: u64) -> Result<()> {
        self.do_action(action, timeout).await?;
        Ok(())
    }

    fn set_metadata<T>(request: &mut Request<T>, name: &'static str, value: &str) -> Result<()> {
        match MetadataValue::try_from(value) {
            Ok(metadata_value) => {
                let metadata_key = MetadataKey::from_static(name);
                request.metadata_mut().insert(metadata_key, metadata_value);
                Ok(())
            }
            Err(cause) => Err(ErrorCode::BadBytes(format!(
                "Cannot parse query id to MetadataValue, {:?}",
                cause
            ))),
        }
    }

    pub async fn do_exchange(&mut self, query_id: &str, source: &str, fragment_id: usize) -> Result<FlightExchange> {
        let (tx, rx) = async_channel::bounded(1);
        Ok(FlightExchange::from_client(
            tx,
            self.inner.do_exchange(
                RequestBuilder::create(Box::pin(rx))
                    .with_metadata("x-source", source)?
                    .with_metadata("x-query-id", query_id)?
                    .with_metadata("x-fragment-id", &fragment_id.to_string())?
                    .build()
            ).await?.into_inner(),
        ))
    }

    pub async fn do_put(
        &mut self,
        query_id: &str,
        source: &str,
        rx: Receiver<DataPacket>,
    ) -> Result<()> {
        let mut request = Request::new(Box::pin(DataPacketStream::create(rx)));

        Self::set_metadata(&mut request, "x-source", source)?;
        Self::set_metadata(&mut request, "x-query-id", query_id)?;
        self.inner.do_put(request).await?;
        Ok(())
    }

    // Execute do_action.
    #[tracing::instrument(level = "debug", skip_all)]
    async fn do_action(&mut self, action: FlightAction, timeout: u64) -> Result<Vec<u8>> {
        let action: Action = action.try_into()?;
        let action_type = action.r#type.clone();
        let request = Request::new(action);
        let mut request = common_tracing::inject_span_to_tonic_request(request);
        request.set_timeout(Duration::from_secs(timeout));

        let response = self.inner.do_action(request).await?;

        match response.into_inner().message().await? {
            Some(response) => Ok(response.body),
            None => Err(ErrorCode::EmptyDataFromServer(format!(
                "Can not receive data from flight server, action: {:?}",
                action_type
            ))),
        }
    }
}

#[derive(Clone)]
pub enum FlightExchange {
    // dummy if localhost
    Dummy,
    Client(ClientFlightExchange),
    Server(ServerFlightExchange),
}

impl FlightExchange {
    pub fn from_server(
        streaming: Request<Streaming<FlightData>>,
        response_tx: Sender<std::result::Result<FlightData, Status>>,
    ) -> FlightExchange {
        let mut streaming = streaming.into_inner();
        let (tx, rx) = async_channel::bounded(1);
        common_base::base::tokio::spawn(async move {
            while let Some(message) = streaming.next().await {
                if let Err(_cause) = tx.send(message).await {
                    break;
                }
            }
        });

        FlightExchange::Server(ServerFlightExchange {
            response_tx,
            request_rx: rx,
            state: Arc::new(ChannelState::create()),
            is_closed_request: AtomicBool::new(false),
            is_closed_response: AtomicBool::new(false),
        })
    }

    pub fn from_client(
        response_tx: Sender<FlightData>,
        mut streaming: Streaming<FlightData>,
    ) -> FlightExchange {
        let (tx, request_rx) = async_channel::bounded(1);
        common_base::base::tokio::spawn(async move {
            while let Some(message) = streaming.next().await {
                if let Err(_cause) = tx.send(message).await {
                    break;
                }
            }
        });

        FlightExchange::Client(ClientFlightExchange {
            request_rx,
            response_tx,
            state: Arc::new(ChannelState::create()),
            is_closed_request: AtomicBool::new(false),
            is_closed_response: AtomicBool::new(false),
        })
    }
}

impl FlightExchange {
    pub async fn send(&self, data: DataPacket) -> Result<()> {
        match self {
            FlightExchange::Dummy => Err(ErrorCode::UnImplement("Unimplemented send in dummy exchange.")),
            FlightExchange::Client(exchange) => exchange.send(data).await,
            FlightExchange::Server(exchange) => exchange.send(data).await,
        }
    }

    pub async fn recv(&self) -> Result<Option<DataPacket>> {
        match self {
            FlightExchange::Client(exchange) => exchange.recv().await,
            FlightExchange::Server(exchange) => exchange.recv().await,
            FlightExchange::Dummy => Ok(None),
        }
    }

    pub fn try_recv(&self) -> Result<Option<DataPacket>> {
        match self {
            FlightExchange::Dummy => Ok(None),
            FlightExchange::Client(exchange) => exchange.try_recv(),
            FlightExchange::Server(exchange) => exchange.try_recv(),
        }
    }

    pub fn close_input(&self) {
        match self {
            FlightExchange::Dummy => { /* do nothing*/ }
            FlightExchange::Client(exchange) => exchange.close_input(),
            FlightExchange::Server(exchange) => exchange.close_input(),
        }
    }

    pub fn close_output(&self) {
        match self {
            FlightExchange::Dummy => { /* do nothing*/ }
            FlightExchange::Client(exchange) => exchange.close_ouput(),
            FlightExchange::Server(exchange) => exchange.close_output(),
        }
    }
}

struct ChannelState {
    request_count: AtomicUsize,
    response_count: AtomicUsize,
}

impl ChannelState {
    pub fn create() -> ChannelState {
        ChannelState {
            request_count: AtomicUsize::new(1),
            response_count: AtomicUsize::new(1),
        }
    }
}

pub struct ClientFlightExchange {
    state: Arc<ChannelState>,
    is_closed_request: AtomicBool,
    is_closed_response: AtomicBool,
    response_tx: Sender<FlightData>,
    request_rx: Receiver<std::result::Result<FlightData, Status>>,
}

impl ClientFlightExchange {
    pub async fn send(&self, data: DataPacket) -> Result<()> {
        if let Err(_cause) = self.response_tx.send(FlightData::from(data)).await {
            return Err(ErrorCode::LogicalError("It's a bug"));
        }

        Ok(())
    }

    pub async fn recv(&self) -> Result<Option<DataPacket>> {
        match self.request_rx.recv().await {
            Err(_) => Ok(None),
            Ok(message) => match message {
                Ok(data) => Ok(Some(DataPacket::try_from(data)?)),
                Err(status) => Err(ErrorCode::from(status))
            }
        }
    }

    pub fn try_recv(&self) -> Result<Option<DataPacket>> {
        match self.request_rx.try_recv() {
            Err(_) => Ok(None),
            Ok(message) => match message {
                Ok(data) => Ok(Some(DataPacket::try_from(data)?)),
                Err(status) => Err(ErrorCode::from(status)),
            }
        }
    }

    pub fn close_input(&self) {
        if !self.is_closed_request.fetch_or(true, Ordering::SeqCst) {
            if self.state.request_count.fetch_sub(1, Ordering::AcqRel) == 1 {
                self.request_rx.close();
            }
        }
    }

    pub fn close_ouput(&self) {
        if !self.is_closed_response.fetch_or(true, Ordering::SeqCst) {
            if self.state.response_count.fetch_sub(1, Ordering::AcqRel) == 1 {
                self.response_tx.close();
            }
        }
    }
}

impl Clone for ClientFlightExchange {
    fn clone(&self) -> Self {
        self.state.request_count.fetch_add(1, Ordering::Relaxed);
        self.state.response_count.fetch_add(1, Ordering::Relaxed);

        ClientFlightExchange {
            state: self.state.clone(),
            request_rx: self.request_rx.clone(),
            response_tx: self.response_tx.clone(),
            is_closed_request: AtomicBool::new(false),
            is_closed_response: AtomicBool::new(false),
        }
    }
}

impl Drop for ClientFlightExchange {
    fn drop(&mut self) {
        if !self.is_closed_request.fetch_or(true, Ordering::SeqCst) {
            if self.state.request_count.fetch_sub(1, Ordering::AcqRel) == 1 {
                self.request_rx.close();
            }
        }

        if !self.is_closed_response.fetch_or(true, Ordering::SeqCst) {
            if self.state.response_count.fetch_sub(1, Ordering::AcqRel) == 1 {
                self.response_tx.close();
            }
        }
    }
}

pub struct ServerFlightExchange {
    state: Arc<ChannelState>,
    is_closed_request: AtomicBool,
    is_closed_response: AtomicBool,
    request_rx: Receiver<std::result::Result<FlightData, Status>>,
    response_tx: Sender<std::result::Result<FlightData, Status>>,
}

impl Clone for ServerFlightExchange {
    fn clone(&self) -> Self {
        self.state.request_count.fetch_add(1, Ordering::Relaxed);
        self.state.response_count.fetch_add(1, Ordering::Relaxed);

        ServerFlightExchange {
            state: self.state.clone(),
            request_rx: self.request_rx.clone(),
            response_tx: self.response_tx.clone(),
            is_closed_request: AtomicBool::new(false),
            is_closed_response: AtomicBool::new(false),
        }
    }
}

impl Drop for ServerFlightExchange {
    fn drop(&mut self) {
        if !self.is_closed_request.fetch_or(true, Ordering::SeqCst) {
            if self.state.request_count.fetch_sub(1, Ordering::AcqRel) == 1 {
                self.request_rx.close();
            }
        }

        if !self.is_closed_response.fetch_or(true, Ordering::SeqCst) {
            if self.state.response_count.fetch_sub(1, Ordering::AcqRel) == 1 {
                self.response_tx.close();
            }
        }
    }
}

impl ServerFlightExchange {
    pub async fn send(&self, data: DataPacket) -> Result<()> {
        if let Err(_cause) = self.response_tx.send(Ok(FlightData::from(data))).await {
            return Err(ErrorCode::LogicalError("It's a bug"));
        }

        Ok(())
    }

    pub async fn recv(&self) -> Result<Option<DataPacket>> {
        match self.request_rx.recv().await {
            Err(_) => Ok(None),
            Ok(message) => match message {
                Ok(data) => Ok(Some(DataPacket::try_from(data)?)),
                Err(status) => Err(ErrorCode::from(status))
            }
        }
    }

    pub fn try_recv(&self) -> Result<Option<DataPacket>> {
        match self.request_rx.try_recv() {
            Err(_) => Ok(None),
            Ok(message) => match message {
                Ok(data) => Ok(Some(DataPacket::try_from(data)?)),
                Err(status) => Err(ErrorCode::from(status)),
            }
        }
    }

    pub fn close_input(&self) {
        if !self.is_closed_request.fetch_or(true, Ordering::SeqCst) {
            if self.state.request_count.fetch_sub(1, Ordering::AcqRel) == 1 {
                self.request_rx.close();
            }
        }
    }

    pub fn close_output(&self) {
        if !self.is_closed_response.fetch_or(true, Ordering::SeqCst) {
            if self.state.response_count.fetch_sub(1, Ordering::AcqRel) == 1 {
                self.response_tx.close();
            }
        }
    }
}
