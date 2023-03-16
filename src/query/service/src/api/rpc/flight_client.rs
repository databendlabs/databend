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
use std::io::ErrorKind;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::sync::Arc;

use async_channel::Receiver;
use async_channel::Sender;
use async_channel::WeakSender;
use common_arrow::arrow_format::flight::data::Action;
use common_arrow::arrow_format::flight::data::FlightData;
use common_arrow::arrow_format::flight::service::flight_service_client::FlightServiceClient;
use common_base::base::tokio::sync::Notify;
use common_base::base::tokio::time::Duration;
use common_base::runtime::GlobalIORuntime;
use common_base::runtime::TrySpawn;
use common_exception::ErrorCode;
use common_exception::Result;
use futures::StreamExt;
use futures_util::future::BoxFuture;
use futures_util::future::Either;
use parking_lot::Mutex;
use tonic::transport::channel::Channel;
use tonic::Request;
use tonic::Status;
use tonic::Streaming;
use tracing::info;

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
            None,
            None,
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
            Some(query_id.to_string()),
            Some(fragment_id),
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
        query_id: Option<String>,
        fragment: Option<usize>,
        streaming: Request<Streaming<FlightData>>,
        response_tx: Sender<Result<FlightData, Status>>,
    ) -> FlightExchange {
        let streaming = streaming.into_inner();
        let state = Arc::new(ChannelState::create());
        let f = |x| Ok(FlightData::from(x));
        let (tx, rx) = Self::listen_request(
            query_id,
            fragment,
            state.clone(),
            response_tx.clone(),
            streaming,
            f,
        );

        FlightExchange::Server(ServerFlightExchange {
            state,
            request_rx: rx,
            response_tx: tx,
            network_tx: response_tx.downgrade(),
        })
    }

    pub fn from_client(
        query_id: Option<String>,
        fragment: Option<usize>,
        response_tx: Sender<FlightData>,
        streaming: Streaming<FlightData>,
    ) -> FlightExchange {
        let state = Arc::new(ChannelState::create());
        let f = FlightData::from;
        let (tx, rx) = Self::listen_request(
            query_id,
            fragment,
            state.clone(),
            response_tx.clone(),
            streaming,
            f,
        );

        FlightExchange::Client(ClientFlightExchange {
            state,
            request_rx: rx,
            response_tx: tx,
            network_tx: response_tx.downgrade(),
        })
    }

    fn listen_request<ResponseT: Send + 'static>(
        query_id: Option<String>,
        fragment: Option<usize>,
        state: Arc<ChannelState>,
        network_tx: Sender<ResponseT>,
        mut streaming: Streaming<FlightData>,
        f: impl Fn(DataPacket) -> ResponseT + Sync + Send + 'static,
    ) -> (Sender<ResponseT>, Receiver<Result<FlightData, Status>>) {
        let (tx, rx) = async_channel::bounded(1);
        let (response_tx, response_rx) = async_channel::bounded(1);

        Self::start_push_worker(
            query_id.clone(),
            fragment,
            network_tx.clone(),
            response_rx,
            state.clone(),
        );

        let f = Arc::new(f);
        GlobalIORuntime::instance().spawn({
            let channel_state = state.clone();
            let response_tx = response_tx.clone();

            async move {
                let mut notified = Box::pin(channel_state.shutdown_notify.notified());
                let mut futures = Vec::<BoxFuture<'static, _>>::new();

                'loop_worker: loop {
                    if channel_state.closed_both() {
                        break 'loop_worker;
                    }

                    match futures::future::select(notified, streaming.next()).await {
                        Either::Left((_, right)) => {
                            debug_assert!(state.closed_both());

                            // break 'loop_worker;
                            tx.close();
                            drop(network_tx);
                            response_tx.close();

                            if let Some(Ok(_message)) = right.await {
                                let _ = StreamExt::count(streaming).await;
                            }

                            return;
                        }
                        Either::Right((None, _notified)) => {
                            if state.acquire_close_input() {
                                futures.push(Box::pin(common_base::base::tokio::spawn({
                                    let f = f.clone();
                                    let network_tx = network_tx.clone();
                                    let channel_state = channel_state.clone();

                                    async move {
                                        let response_t = f(DataPacket::ClosingInput);
                                        let _ = network_tx.send(response_t).await;

                                        if channel_state.close_input() {
                                            channel_state.shutdown_notify.notify_waiters();
                                        }
                                    }
                                })));
                            }

                            if state.acquire_close_output() {
                                futures.push(Box::pin(common_base::base::tokio::spawn({
                                    let f = f.clone();
                                    let response_tx = response_tx.clone();
                                    let channel_state = channel_state.clone();
                                    async move {
                                        let response_t = f(DataPacket::ClosingOutput);
                                        let _ = response_tx.send(response_t).await;
                                        response_tx.close();

                                        if channel_state.close_output() {
                                            channel_state.shutdown_notify.notify_waiters();
                                        }
                                    }
                                })));
                            }

                            break 'loop_worker;
                        }
                        Either::Right((Some(message), left)) => {
                            notified = left;

                            match message {
                                Ok(message) if DataPacket::is_closing_input(&message) => {
                                    if channel_state.acquire_close_output() {
                                        if let Some(query_id) = &query_id {
                                            info!(
                                                "First recv closing input query: {:?}, fragment:{}",
                                                query_id,
                                                fragment.unwrap()
                                            );
                                        }

                                        // create new future send packet to remote for avoid blocking recv data
                                        futures.push(Box::pin(common_base::base::tokio::spawn({
                                            let f = f.clone();
                                            let response_tx = response_tx.clone();
                                            let query_id = query_id.clone();
                                            let fragment = fragment.clone();
                                            let channel_state = channel_state.clone();

                                            async move {
                                                if let Some(query_id) = &query_id {
                                                    info!(
                                                        "Prepare send closing output query: {:?}. fragment: {}",
                                                        query_id,
                                                        fragment.unwrap(),
                                                    );
                                                }

                                                let response_t = f(DataPacket::ClosingOutput);
                                                let res = response_tx.send(response_t).await.is_ok();

                                                response_tx.close();

                                                if channel_state.close_output() {
                                                    channel_state.shutdown_notify.notify_waiters();
                                                }

                                                if let Some(query_id) = &query_id {
                                                    info!(
                                                        "Send closing output query: {:?}. fragment: {}, {}",
                                                        query_id,
                                                        fragment.unwrap(),
                                                        res
                                                    );
                                                }
                                            }
                                        })));
                                    }
                                }
                                Ok(message) if DataPacket::is_closing_output(&message) => {
                                    if !tx.is_closed() {
                                        tx.close();
                                    }

                                    if channel_state.acquire_close_input() {
                                        if let Some(query_id) = &query_id {
                                            info!(
                                                "First recv closing output query: {:?}, fragment:{}",
                                                query_id,
                                                fragment.unwrap()
                                            );
                                        }

                                        // create new future send packet to remote for avoid blocking recv data
                                        futures.push(Box::pin(common_base::base::tokio::spawn({
                                            let f = f.clone();
                                            let network_tx = network_tx.clone();
                                            let query_id = query_id.clone();
                                            let fragment = fragment.clone();
                                            let channel_state = channel_state.clone();

                                            async move {
                                                if let Some(query_id) = &query_id {
                                                    info!(
                                                        "Prepare send closing input query: {:?}. fragment: {}",
                                                        query_id,
                                                        fragment.unwrap(),
                                                    );
                                                }

                                                let response_t = f(DataPacket::ClosingInput);
                                                let res = network_tx.send(response_t).await.is_ok();

                                                if channel_state.close_input() {
                                                    channel_state.shutdown_notify.notify_waiters();
                                                }

                                                if let Some(query_id) = &query_id {
                                                    info!(
                                                        "Send closing input query: {:?}. fragment: {}, {}",
                                                        query_id,
                                                        fragment.unwrap(),
                                                        res
                                                    );
                                                }
                                            }
                                        })));
                                    }
                                }
                                other => {
                                    if let Err(status) = &other {
                                        let mut may_recv_error = state.may_recv_error.lock();
                                        *may_recv_error = Some(match match_for_io_error(status) {
                                            Some(error) => std::io::Error::new(error.kind(), ""),
                                            None => std::io::Error::new(
                                                ErrorKind::Other,
                                                format!("{:?}", status),
                                            ),
                                        });

                                        tx.close();
                                        response_tx.close();
                                        network_tx.close();
                                        return;
                                    }

                                    // We need to continue consume stream for avoid stream die message blocking io buffer.
                                    let _ = tx.send(other).await;
                                }
                            };
                        }
                    }
                }

                if let Some(query_id) = &query_id {
                    info!(
                        "Break flight listener query: {:?}, fragment:{}, {}",
                        query_id,
                        fragment.unwrap(),
                        channel_state.closed_both(),
                    );
                }

                let recv_all = StreamExt::count(streaming);
                let send_all = futures::future::join_all(futures);

                match futures::future::select(send_all, recv_all).await {
                    Either::Left((_, recv_all)) => {
                        tx.close();
                        drop(network_tx);
                        response_tx.close();

                        let _ = recv_all.await;

                        if let Some(query_id) = query_id {
                            info!(
                                "Shutdown flight listener query: {:?}, fragment:{}",
                                query_id,
                                fragment.unwrap()
                            );
                        }
                    }
                    Either::Right((_, send_all)) => {
                        let _ = send_all.await;
                        tx.close();
                        drop(network_tx);
                        response_tx.close();

                        if let Some(query_id) = query_id {
                            info!(
                                "Shutdown flight listener query: {:?}, fragment:{}",
                                query_id,
                                fragment.unwrap()
                            );
                        }
                    }
                };
            }
        });

        (response_tx, rx)
    }

    fn start_push_worker<ResponseT: Send + 'static>(
        query_id: Option<String>,
        fragment: Option<usize>,
        network_tx: Sender<ResponseT>,
        response_rx: Receiver<ResponseT>,
        channel_state: Arc<ChannelState>,
    ) {
        GlobalIORuntime::instance().spawn(async move {
            let mut notified = Box::pin(channel_state.shutdown_notify.notified());

            'publisher_worker: loop {
                if channel_state.closed_both() {
                    break 'publisher_worker;
                }

                match futures::future::select(notified, response_rx.recv()).await {
                    Either::Right((Err(_), _left)) => {
                        break 'publisher_worker;
                    }
                    Either::Left((_, _recv)) => {
                        while let Ok(response) = response_rx.try_recv() {
                            if network_tx.send(response).await.is_err() {
                                break 'publisher_worker;
                            }
                        }

                        break 'publisher_worker;
                    }
                    Either::Right((Ok(response), left)) => {
                        notified = left;

                        if network_tx.send(response).await.is_err() {
                            break 'publisher_worker;
                        }
                    }
                }
            }

            response_rx.close();
            drop(network_tx);
            if let Some(query_id) = query_id {
                info!(
                    "Shutdown flight push worker query: {}, fragment: {}",
                    query_id,
                    fragment.unwrap()
                );
            }
        });
    }
}

impl FlightExchange {
    pub fn get_ref(&self) -> FlightExchangeRef {
        let state = match self {
            FlightExchange::Dummy => Arc::new(ChannelState::create()),
            FlightExchange::Client(exchange) => exchange.state.clone(),
            FlightExchange::Server(exchange) => exchange.state.clone(),
        };

        state.request_count.fetch_add(1, Ordering::SeqCst);
        state.response_count.fetch_add(1, Ordering::SeqCst);

        FlightExchangeRef {
            state,
            inner: self.clone(),
            is_closed_request: AtomicBool::new(false),
            is_closed_response: AtomicBool::new(false),
        }
    }

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

    pub fn is_closed_input(&self) -> bool {
        match self {
            FlightExchange::Dummy => true,
            FlightExchange::Client(exchange) => exchange.request_rx.is_closed(),
            FlightExchange::Server(exchange) => exchange.request_rx.is_closed(),
        }
    }

    pub fn is_closed_output(&self) -> bool {
        match self {
            FlightExchange::Dummy => true,
            FlightExchange::Client(exchange) => exchange.response_tx.is_closed(),
            FlightExchange::Server(exchange) => exchange.response_tx.is_closed(),
        }
    }

    pub async fn close_input(&self) -> bool {
        match self {
            FlightExchange::Dummy => true,
            FlightExchange::Client(exchange) => exchange.close_input().await,
            FlightExchange::Server(exchange) => exchange.close_input().await,
        }
    }

    pub async fn close_output(&self) -> bool {
        match self {
            FlightExchange::Dummy => true,
            FlightExchange::Client(exchange) => exchange.close_output().await,
            FlightExchange::Server(exchange) => exchange.close_output().await,
        }
    }
}

pub struct FlightExchangeRef {
    inner: FlightExchange,
    state: Arc<ChannelState>,
    is_closed_request: AtomicBool,
    is_closed_response: AtomicBool,
}

impl Drop for FlightExchangeRef {
    fn drop(&mut self) {
        // Blocking is ok, because the channel may not be closed when has error in query execution.
        if !self.is_closed_request.load(Ordering::SeqCst)
            || !self.is_closed_response.load(Ordering::SeqCst)
        {
            futures::executor::block_on(async move {
                self.close_input().await;
                self.close_output().await;
            });
        }
    }
}

impl Clone for FlightExchangeRef {
    fn clone(&self) -> Self {
        self.state.request_count.fetch_add(1, Ordering::SeqCst);
        self.state.response_count.fetch_add(1, Ordering::SeqCst);

        FlightExchangeRef {
            state: self.state.clone(),
            inner: self.inner.clone(),
            is_closed_request: AtomicBool::new(false),
            is_closed_response: AtomicBool::new(false),
        }
    }
}

impl FlightExchangeRef {
    pub async fn send(&self, data: DataPacket) -> Result<()> {
        self.inner.send(data).await
    }

    pub async fn recv(&self) -> Result<Option<DataPacket>> {
        self.inner.recv().await
    }

    pub async fn close_input(&self) -> bool {
        if self.is_closed_request.fetch_or(true, Ordering::SeqCst) {
            return false;
        }

        if self.state.request_count.fetch_sub(1, Ordering::SeqCst) == 1 {
            return self.inner.close_input().await;
        }

        false
    }

    pub async fn close_output(&self) -> bool {
        if self.is_closed_response.fetch_or(true, Ordering::SeqCst) {
            return false;
        }

        if self.state.response_count.fetch_sub(1, Ordering::SeqCst) == 1 {
            return self.inner.close_output().await;
        }

        false
    }
}

static SENDING_CLOSING_INPUT: usize = 1;
static SENT_CLOSING_INPUT: usize = 1 << 1;
static SENDING_CLOSING_OUTPUT: usize = 1 << 2;
static SENT_CLOSING_OUTPUT: usize = 1 << 3;

struct ChannelState {
    request_count: AtomicUsize,
    response_count: AtomicUsize,
    flags: AtomicUsize,
    shutdown_notify: Notify,
    may_recv_error: Mutex<Option<std::io::Error>>,
}

impl ChannelState {
    pub fn create() -> ChannelState {
        ChannelState {
            may_recv_error: Mutex::new(None),
            request_count: AtomicUsize::new(0),
            response_count: AtomicUsize::new(0),
            flags: AtomicUsize::new(0),
            shutdown_notify: Notify::new(),
        }
    }

    pub fn closed_both(&self) -> bool {
        let flags = self.flags.load(Ordering::Acquire);
        (flags & SENT_CLOSING_INPUT != 0) && (flags & SENDING_CLOSING_OUTPUT != 0)
    }

    pub fn close_input(&self) -> bool {
        (self.flags.fetch_or(SENT_CLOSING_INPUT, Ordering::SeqCst) & SENT_CLOSING_OUTPUT) != 0
    }

    pub fn close_output(&self) -> bool {
        (self.flags.fetch_or(SENT_CLOSING_OUTPUT, Ordering::SeqCst) & SENT_CLOSING_INPUT) != 0
    }

    pub fn acquire_close_input(&self) -> bool {
        (self.flags.fetch_or(SENDING_CLOSING_INPUT, Ordering::SeqCst) & SENDING_CLOSING_INPUT) == 0
    }

    pub fn acquire_close_output(&self) -> bool {
        (self
            .flags
            .fetch_or(SENDING_CLOSING_OUTPUT, Ordering::SeqCst)
            & SENDING_CLOSING_OUTPUT)
            == 0
    }
}

#[derive(Clone)]
pub struct ClientFlightExchange {
    state: Arc<ChannelState>,
    response_tx: Sender<FlightData>,
    network_tx: WeakSender<FlightData>,
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

    pub async fn close_input(&self) -> bool {
        if self.state.acquire_close_input() {
            // Close local channel first.
            // NOTE: this is very important. When we open the local channel while pushing closing input, it may cause distributed deadlock.
            self.request_rx.close();

            // Notify remote not to send messages.
            // We send it directly to the network channel avoid response channel is closed
            if let Some(network_tx) = self.network_tx.upgrade() {
                let packet = FlightData::from(DataPacket::ClosingInput);
                if network_tx.send(packet).await.is_ok() {
                    if self.state.close_input() {
                        self.state.shutdown_notify.notify_waiters();
                    }

                    return true;
                }
            }
        }

        false
    }

    pub async fn close_output(&self) -> bool {
        if !self.state.acquire_close_output() {
            // Notify remote that no message will be sent.
            let packet = FlightData::from(DataPacket::ClosingOutput);
            if self.response_tx.send(packet).await.is_ok() {
                if self.state.close_output() {
                    self.state.shutdown_notify.notify_waiters();
                }

                return true;
            }
        }

        false
    }
}

#[derive(Clone)]
pub struct ServerFlightExchange {
    state: Arc<ChannelState>,
    network_tx: WeakSender<Result<FlightData, Status>>,
    request_rx: Receiver<Result<FlightData, Status>>,
    response_tx: Sender<Result<FlightData, Status>>,
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

    pub async fn close_input(&self) -> bool {
        if self.state.acquire_close_input() {
            // Close local channel first.
            // NOTE: this is very important. When we open the local channel while pushing closing input, it may cause distributed deadlock.
            self.request_rx.close();

            // Notify remote not to send messages.
            // We send it directly to the network channel avoid response channel is closed
            if let Some(network_tx) = self.network_tx.upgrade() {
                let packet = FlightData::from(DataPacket::ClosingInput);
                if network_tx.send(Ok(packet)).await.is_ok() {
                    if self.state.close_input() {
                        self.state.shutdown_notify.notify_waiters();
                    }

                    return true;
                }
            }
        }

        false
    }

    pub async fn close_output(&self) -> bool {
        if self.state.acquire_close_output() {
            // Notify remote that no message will be sent.
            let packet = FlightData::from(DataPacket::ClosingOutput);
            if self.response_tx.send(Ok(packet)).await.is_ok() {
                if self.state.close_output() {
                    self.state.shutdown_notify.notify_waiters();
                }

                return true;
            }
        }

        false
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
