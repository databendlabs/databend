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
use std::error::Error;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::sync::Arc;

use async_channel::Receiver;
use async_channel::Sender;
use common_arrow::arrow_format::flight::data::Action;
use common_arrow::arrow_format::flight::data::FlightData;
use common_arrow::arrow_format::flight::service::flight_service_client::FlightServiceClient;
use common_base::base::tokio::time::Duration;
use common_exception::ErrorCode;
use common_exception::Result;
use futures_util::StreamExt;
use parking_lot::Mutex;
use tonic::transport::channel::Channel;
use tonic::Request;
use tonic::Status;
use tonic::Streaming;

use crate::api::rpc::flight_actions::FlightAction;
use crate::api::rpc::packets::DataPacket;
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
        if let Err(cause) = self.do_action(action, timeout).await {
            return Err(cause.add_message_back("(while in query flight)"));
        }

        Ok(())
    }

    pub async fn request_server_exchange(&mut self, query_id: &str) -> Result<FlightExchange> {
        let (tx, rx) = async_channel::bounded(8);
        Ok(FlightExchange::from_client(
            tx,
            self.exchange_streaming(
                RequestBuilder::create(Box::pin(rx))
                    .with_metadata("x-type", "request_server_exchange")?
                    .with_metadata("x-query-id", query_id)?
                    .build(),
            )
            .await?,
        ))
    }

    pub async fn do_exchange(
        &mut self,
        query_id: &str,
        source: &str,
        fragment_id: usize,
    ) -> Result<FlightExchange> {
        let (tx, rx) = async_channel::bounded(8);
        Ok(FlightExchange::from_client(
            tx,
            self.exchange_streaming(
                RequestBuilder::create(Box::pin(rx))
                    .with_metadata("x-type", "exchange_fragment")?
                    .with_metadata("x-source", source)?
                    .with_metadata("x-query-id", query_id)?
                    .with_metadata("x-fragment-id", &fragment_id.to_string())?
                    .build(),
            )
            .await?,
        ))
    }

    async fn exchange_streaming(
        &mut self,
        request: impl tonic::IntoStreamingRequest<Message = FlightData>,
    ) -> Result<Streaming<FlightData>> {
        match self.inner.do_exchange(request).await {
            Ok(res) => Ok(res.into_inner()),
            Err(status) => Err(ErrorCode::from(status).add_message_back("(while in query flight)")),
        }
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
        response_tx: Sender<Result<FlightData, Status>>,
    ) -> FlightExchange {
        let mut streaming = streaming.into_inner();
        let state = Arc::new(ChannelState::create());
        let rx = Self::listen_request(state.clone(), response_tx.clone(), streaming);

        FlightExchange::Server(ServerFlightExchange {
            state,
            response_tx,
            request_rx: rx,
            is_closed_request: AtomicBool::new(false),
            is_closed_response: AtomicBool::new(false),
        })
    }

    pub fn from_client(
        response_tx: Sender<FlightData>,
        mut streaming: Streaming<FlightData>,
    ) -> FlightExchange {
        let state = Arc::new(ChannelState::create());
        let rx = Self::listen_request(state.clone(), response_tx.clone(), streaming);

        FlightExchange::Client(ClientFlightExchange {
            state,
            response_tx,
            request_rx: rx,
            is_closed_request: AtomicBool::new(false),
            is_closed_response: AtomicBool::new(false),
        })
    }

    fn listen_request<ResponseT>(
        state: Arc<ChannelState>,
        response: Sender<ResponseT>,
        mut streaming: Streaming<FlightData>,
    ) -> Receiver<Result<FlightData, Status>> {
        let (tx, rx) = async_channel::bounded(1);
        common_base::base::tokio::spawn(async move {
            while let Some(message) = streaming.next().await {
                match message {
                    Ok(message) if DataPacket::is_closing_input(&message) => {
                        response.close();
                        continue;
                    }
                    Ok(message) if DataPacket::is_closing_output(&message) => {
                        // The client guarantees that it will not send message, so not exists die message.
                        return;
                    }
                    other => {
                        if let Some(status) = other {
                            if let Some(error) = match_for_io_error(&status) {
                                {
                                    let may_recv_error = state.may_recv_error.lock();
                                    *may_recv_error = Some(std::io::Error::new(error.kind(), ""));
                                }

                                tx.close();
                                response.close();
                                return;
                            }
                        }

                        // We need to continue consume stream for avoid stream die message blocking io buffer.
                        let _ = tx.send(other).await;
                    }
                }
            }
        });

        rx
    }
}

impl FlightExchange {
    pub async fn send(&self, data: DataPacket) -> Result<()> {
        match self {
            FlightExchange::Dummy => Err(ErrorCode::Unimplemented(
                "Unimplemented send in dummy exchange.",
            )),
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
            FlightExchange::Client(exchange) => exchange.close_output(),
            FlightExchange::Server(exchange) => exchange.close_output(),
        }
    }
}

struct ChannelState {
    request_count: AtomicUsize,
    response_count: AtomicUsize,
    may_recv_error: Mutex<Option<std::io::Error>>,
}

impl ChannelState {
    pub fn create() -> ChannelState {
        ChannelState {
            may_recv_error: Mutex::new(None),
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
    request_rx: Receiver<Result<FlightData, Status>>,
}

impl ClientFlightExchange {
    pub async fn send(&self, data: DataPacket) -> Result<()> {
        if let Err(_cause) = self.response_tx.send(FlightData::from(data)).await {
            let may_recv_error = self.state.may_recv_error.lock();

            // we need to return the error when io error occurs
            if let Some(recv_error) = &*may_recv_error {
                let kind = recv_error.kind();
                let error = std::io::Error::new(kind, "Flight connection failure");
                return Err(ErrorCode::from(error));
            }

            return Err(ErrorCode::AbortedQuery(
                "Aborted query, because the remote flight channel is closed.",
            ));
        }

        Ok(())
    }

    pub async fn recv(&self) -> Result<Option<DataPacket>> {
        match self.request_rx.recv().await {
            Err(_) => {
                let may_recv_error = self.state.may_recv_error.lock();

                // we need to return the error when io error occurs
                if let Some(recv_error) = &*may_recv_error {
                    let kind = recv_error.kind();
                    let error = std::io::Error::new(kind, "Flight connection failure");
                    return Err(ErrorCode::from(error));
                }

                Ok(None)
            }
            Ok(message) => match message {
                Ok(data) => Ok(Some(DataPacket::try_from(data)?)),
                Err(status) => Err(ErrorCode::from(status)),
            },
        }
    }

    pub fn close_input(&self) {
        if !self.is_closed_request.fetch_or(true, Ordering::SeqCst)
            && self.state.request_count.fetch_sub(1, Ordering::AcqRel) == 1
        {
            // Notify remote not to send messages.
            let packet = FlightData::from(DataPacket::ClosingInput);
            let _ = self.response_tx.send_blocking(packet);

            // Close local message channel
            self.request_rx.close();
        }
    }

    pub fn close_output(&self) {
        if !self.is_closed_response.fetch_or(true, Ordering::SeqCst)
            && self.state.response_count.fetch_sub(1, Ordering::AcqRel) == 1
        {
            // Notify remote that no message will be sent.
            let packet = FlightData::from(DataPacket::ClosingOutput);
            let _ = self.response_tx.send_blocking(packet);

            // Close local message channel
            self.response_tx.close();
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
        self.close_input();
        self.close_output();
    }
}

pub struct ServerFlightExchange {
    state: Arc<ChannelState>,
    is_closed_request: AtomicBool,
    is_closed_response: AtomicBool,
    request_rx: Receiver<Result<FlightData, Status>>,
    response_tx: Sender<Result<FlightData, Status>>,
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
        self.close_input();
        self.close_output();
    }
}

impl ServerFlightExchange {
    pub async fn send(&self, data: DataPacket) -> Result<()> {
        if let Err(_cause) = self.response_tx.send(Ok(FlightData::from(data))).await {
            let may_recv_error = self.state.may_recv_error.lock();

            // we need to return the error when io error occurs
            if let Some(recv_error) = &*may_recv_error {
                let kind = recv_error.kind();
                let error = std::io::Error::new(kind, "Flight connection failure");
                return Err(ErrorCode::from(error));
            }

            return Err(ErrorCode::AbortedQuery(
                "Aborted query, because the remote flight channel is closed.",
            ));
        }

        Ok(())
    }

    pub async fn recv(&self) -> Result<Option<DataPacket>> {
        match self.request_rx.recv().await {
            Err(_) => {
                let may_recv_error = self.state.may_recv_error.lock();

                // we need to return the error when io error occurs
                if let Some(recv_error) = &*may_recv_error {
                    let kind = recv_error.kind();
                    let error = std::io::Error::new(kind, "Flight connection failure");
                    return Err(ErrorCode::from(error));
                }

                Ok(None)
            }
            Ok(message) => match message {
                Ok(data) => Ok(Some(DataPacket::try_from(data)?)),
                Err(status) => Err(ErrorCode::from(status)),
            },
        }
    }

    pub fn close_input(&self) {
        if !self.is_closed_request.fetch_or(true, Ordering::SeqCst)
            && self.state.request_count.fetch_sub(1, Ordering::AcqRel) == 1
        {
            // Notify remote not to send messages.
            let packet = FlightData::from(DataPacket::ClosingInput);
            let _ = self.response_tx.send_blocking(Ok(packet));

            // Close local message channel
            self.request_rx.close();
        }
    }

    pub fn close_output(&self) {
        if !self.is_closed_response.fetch_or(true, Ordering::SeqCst)
            && self.state.response_count.fetch_sub(1, Ordering::AcqRel) == 1
        {
            // Notify remote that no message will be sent.
            let packet = FlightData::from(DataPacket::ClosingOutput);
            let _ = self.response_tx.send_blocking(Ok(packet));

            // Close local message channel
            self.response_tx.close();
        }
    }
}

fn match_for_io_error(err_status: &Status) -> Option<&std::io::Error> {
    let mut err: &(dyn Error + 'static) = err_status;

    loop {
        if let Some(io_err) = err.downcast_ref::<std::io::Error>() {
            return Some(io_err);
        }

        // h2::Error do not expose std::io::Error with `source()`
        // https://github.com/hyperium/h2/pull/462
        use h2::Error as h2Error;
        if let Some(h2_err) = err.downcast_ref::<h2Error>() {
            if let Some(io_err) = h2_err.get_io() {
                return Some(io_err);
            }
        }

        err = match err.source() {
            Some(err) => err,
            None => return None,
        };
    }
}
