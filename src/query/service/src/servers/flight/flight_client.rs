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

use std::collections::VecDeque;
use std::str::FromStr;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::sync::Arc;

use async_channel::Receiver;
use async_channel::Sender;
use databend_common_arrow::arrow_format::flight::data::Action;
use databend_common_arrow::arrow_format::flight::data::FlightData;
use databend_common_arrow::arrow_format::flight::data::Ticket;
use databend_common_arrow::arrow_format::flight::service::flight_service_client::FlightServiceClient;
use databend_common_base::base::tokio::time::Duration;
use databend_common_base::runtime::drop_guard;
use databend_common_base::runtime::Runtime;
use databend_common_base::runtime::TrySpawn;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use futures::StreamExt;
use futures_util::future::Either;
use minitrace::full_name;
use minitrace::future::FutureExt;
use minitrace::Span;
use parking_lot::Mutex;
use parking_lot::RwLock;
use serde::Deserialize;
use serde::Serialize;
use tonic::metadata::AsciiMetadataKey;
use tonic::metadata::AsciiMetadataValue;
use tonic::transport::channel::Channel;
use tonic::Request;
use tonic::Status;
use tonic::Streaming;

use crate::pipelines::executor::WatchNotify;
use crate::servers::flight::request_builder::RequestBuilder;
use crate::servers::flight::v1::exchange::DataExchangeManager;
use crate::servers::flight::v1::packets::DataPacket;

pub struct FlightClient {
    inner: FlightServiceClient<Channel>,
}

// TODO: Integration testing required
impl FlightClient {
    pub fn new(mut inner: FlightServiceClient<Channel>) -> FlightClient {
        inner = inner.max_decoding_message_size(usize::MAX);
        inner = inner.max_encoding_message_size(usize::MAX);

        FlightClient { inner }
    }

    #[async_backtrace::framed]
    #[minitrace::trace]
    pub async fn do_action<T, Res>(
        &mut self,
        path: &str,
        secret: String,
        message: T,
        timeout: u64,
    ) -> Result<Res>
    where
        T: Serialize,
        Res: for<'a> Deserialize<'a>,
    {
        let mut body = Vec::with_capacity(512);
        let mut serializer = serde_json::Serializer::new(&mut body);
        let serializer = serde_stacker::Serializer::new(&mut serializer);
        message.serialize(serializer).map_err(|cause| {
            ErrorCode::BadArguments(format!(
                "Request payload serialize error while in {:?}, cause: {}",
                path, cause
            ))
        })?;

        drop(message);
        let mut request =
            databend_common_tracing::inject_span_to_tonic_request(Request::new(Action {
                body,
                r#type: path.to_string(),
            }));

        request.set_timeout(Duration::from_secs(timeout));
        request.metadata_mut().insert(
            AsciiMetadataKey::from_str("secret").unwrap(),
            AsciiMetadataValue::from_str(&secret).unwrap(),
        );

        let response = self.inner.do_action(request).await?;

        match response.into_inner().message().await? {
            Some(response) => {
                let mut deserializer = serde_json::Deserializer::from_slice(&response.body);
                deserializer.disable_recursion_limit();
                let deserializer = serde_stacker::Deserializer::new(&mut deserializer);

                Res::deserialize(deserializer).map_err(|cause| {
                    ErrorCode::BadBytes(format!(
                        "Response payload deserialize error while in {:?}, cause: {}",
                        path, cause
                    ))
                })
            }
            None => Err(ErrorCode::EmptyDataFromServer(format!(
                "Can not receive data from flight server, action: {:?}",
                path
            ))),
        }
    }

    #[async_backtrace::framed]
    pub async fn request_server_exchange(
        &mut self,
        query_id: &str,
        target: &str,
        source_address: &str,
    ) -> Result<FlightExchange> {
        let req = RequestBuilder::create(Ticket::default())
            .with_metadata("x-type", "request_server_exchange")?
            .with_metadata("x-target", target)?
            .with_metadata("x-query-id", query_id)?
            .with_metadata("x-continue-from", "0")?
            .build();
        let streaming = self.get_streaming(req).await?;

        let (notify, rx) = Self::streaming_receiver(streaming);
        Ok(FlightExchange::create_receiver(
            notify,
            rx,
            Some(ConnectionInfo {
                query_id: query_id.to_string(),
                target: target.to_string(),
                fragment: None,
                source_address: source_address.to_string(),
            }),
        ))
    }

    #[async_backtrace::framed]
    #[minitrace::trace]
    pub async fn do_get(
        &mut self,
        query_id: &str,
        target: &str,
        fragment: usize,
        source_address: &str,
    ) -> Result<FlightExchange> {
        let request = RequestBuilder::create(Ticket::default())
            .with_metadata("x-type", "exchange_fragment")?
            .with_metadata("x-target", target)?
            .with_metadata("x-query-id", query_id)?
            .with_metadata("x-fragment-id", &fragment.to_string())?
            .with_metadata("x-continue-from", "0")?
            .build();
        let request = databend_common_tracing::inject_span_to_tonic_request(request);

        let streaming = self.get_streaming(request).await?;

        let (notify, rx) = Self::streaming_receiver(streaming);
        Ok(FlightExchange::create_receiver(
            notify,
            rx,
            Some(ConnectionInfo {
                query_id: query_id.to_string(),
                target: target.to_string(),
                fragment: Some(fragment),
                source_address: source_address.to_string(),
            }),
        ))
    }

    fn streaming_receiver(
        mut streaming: Streaming<FlightData>,
    ) -> (Arc<WatchNotify>, Receiver<Result<FlightData>>) {
        let (tx, rx) = async_channel::bounded(1);
        let notify = Arc::new(WatchNotify::new());
        let fut = {
            let notify = notify.clone();
            async move {
                let mut notified = Box::pin(notify.notified());
                let mut streaming_next = streaming.next();

                loop {
                    match futures::future::select(notified, streaming_next).await {
                        Either::Left((_, _)) | Either::Right((None, _)) => {
                            break;
                        }
                        Either::Right((Some(message), next_notified)) => {
                            notified = next_notified;
                            streaming_next = streaming.next();

                            match message {
                                Ok(message) => {
                                    if tx.send(Ok(message)).await.is_err() {
                                        break;
                                    }
                                }
                                Err(status) => {
                                    let _ = tx.send(Err(ErrorCode::from(status))).await;
                                    break;
                                }
                            }
                        }
                    }
                }

                drop(streaming);
                tx.close();
            }
        }
        .in_span(Span::enter_with_local_parent(full_name!()));

        databend_common_base::runtime::spawn(fut);

        (notify, rx)
    }

    #[async_backtrace::framed]
    async fn get_streaming(&mut self, request: Request<Ticket>) -> Result<Streaming<FlightData>> {
        match self.inner.do_get(request).await {
            Ok(res) => Ok(res.into_inner()),
            Err(status) => Err(ErrorCode::from(status).add_message_back("(while in query flight)")),
        }
    }

    #[async_backtrace::framed]
    async fn reconnect(
        &mut self,
        info: &ConnectionInfo,
        continue_from: usize,
    ) -> Result<(Arc<WatchNotify>, Receiver<Result<FlightData>>)> {
        let request = match info.fragment {
            Some(fragment_id) => RequestBuilder::create(Ticket::default())
                .with_metadata("x-type", "exchange_fragment")?
                .with_metadata("x-target", &info.target)?
                .with_metadata("x-query-id", &info.query_id)?
                .with_metadata("x-fragment-id", &fragment_id.to_string())?
                .with_metadata("x-continue-from", &continue_from.to_string())?
                .build(),
            None => RequestBuilder::create(Ticket::default())
                .with_metadata("x-type", "request_server_exchange")?
                .with_metadata("x-target", &info.target)?
                .with_metadata("x-query-id", &info.query_id)?
                .with_metadata("x-continue-from", &continue_from.to_string())?
                .build(),
        };
        let request = databend_common_tracing::inject_span_to_tonic_request(request);

        let streaming = self.get_streaming(request).await?;

        Ok(Self::streaming_receiver(streaming))
    }
}

pub struct ConnectionInfo {
    pub query_id: String,
    pub target: String,
    pub fragment: Option<usize>,
    pub source_address: String,
}

pub struct FlightReceiver {
    seq_num: AtomicUsize,
    notify: RwLock<Arc<WatchNotify>>,
    rx: RwLock<Receiver<Result<FlightData>>>,
    connection_info: Option<ConnectionInfo>,
}

impl Drop for FlightReceiver {
    fn drop(&mut self) {
        drop_guard(move || {
            self.close();
        })
    }
}

impl FlightReceiver {
    pub fn create(
        rx: Receiver<Result<FlightData>>,
        connection_info: Option<ConnectionInfo>,
    ) -> FlightReceiver {
        FlightReceiver {
            seq_num: AtomicUsize::new(0),
            rx: RwLock::new(rx),
            notify: RwLock::new(Arc::new(WatchNotify::new())),
            connection_info,
        }
    }

    #[async_backtrace::framed]
    pub async fn recv(&self) -> Result<Option<DataPacket>> {
        let receiver = self.rx.read().clone();
        match receiver.recv().await {
            Err(_) => Ok(None),
            Ok(Err(error)) => {
                self.retry().await?;
                Err(error) // TODO
            }
            Ok(Ok(message)) => {
                self.seq_num.fetch_add(1, Ordering::Acquire);
                Ok(Some(DataPacket::try_from(message)?))
            }
        }
    }

    #[async_backtrace::framed]
    pub async fn retry(&self) -> Result<()> {
        if let Some(connection_info) = &self.connection_info {
            let mut flight_client =
                DataExchangeManager::create_client(&connection_info.source_address, true).await?;

            let (notify, receiver) = flight_client
                .reconnect(connection_info, self.seq_num.load(Ordering::Acquire))
                .await?;

            *self.notify.write() = notify;
            *self.rx.write() = receiver;
        }
        Ok(())
    }

    pub fn close(&self) {
        self.rx.read().close();
        self.notify.read().notify_waiters();
    }
}

pub struct FlightSender {
    tx: Mutex<Sender<Result<FlightData, Status>>>,
    seq_num: AtomicUsize,
    buffer: Mutex<VecDeque<FlightData>>,
}

impl FlightSender {
    pub fn create(tx: Sender<Result<FlightData, Status>>) -> FlightSender {
        FlightSender {
            tx: Mutex::new(tx),
            seq_num: AtomicUsize::new(0),
            buffer: Mutex::new(VecDeque::with_capacity(3)),
        }
    }

    pub fn is_closed(&self) -> bool {
        self.tx.lock().is_closed()
    }

    #[async_backtrace::framed]
    pub async fn send(&self, data: DataPacket) -> Result<()> {
        self.seq_num.fetch_add(1, Ordering::Acquire);
        let flight_data = FlightData::try_from(data)?;
        self.update_buffer(flight_data.clone());
        let sender = self.tx.lock().clone();
        if let Err(_cause) = sender.send(Ok(flight_data)).await {
            return Err(ErrorCode::AbortedQuery(
                "Aborted query, because the remote flight channel is closed.",
            ));
        }
        Ok(())
    }

    fn update_buffer(&self, data: FlightData) {
        let mut buffer = self.buffer.lock();
        if buffer.len() == buffer.capacity() {
            buffer.pop_front();
        }
        buffer.push_back(data);
    }

    pub fn replace_tx(
        &self,
        new_tx: Sender<Result<FlightData, Status>>,
        continue_from: usize,
    ) -> Result<()> {
        *self.tx.lock() = new_tx;
        let buffer = self.buffer.lock().clone();
        let sender = self.tx.lock().clone();
        let seq_num = self.seq_num.load(Ordering::Acquire);
        Runtime::with_worker_threads(1, Some(String::from("ReconnectSender")))?.spawn(async move {
            if seq_num - continue_from < buffer.len() {
                for data in buffer.iter().skip(seq_num - continue_from) {
                    if let Err(_cause) = sender.send(Ok(data.clone())).await {
                        break;
                    }
                }
            } else {
                let _res = sender
                    .send(Err(ErrorCode::AbortedQuery(
                        "Aborted query, because the remote flight channel is closed.",
                    )
                    .into()))
                    .await;
            }
        });
        Ok(())
    }

    pub fn close(&self) {
        self.tx.lock().close();
    }
}

pub struct FlightSenderWrapper {
    inner: Arc<FlightSender>,
}

impl FlightSenderWrapper {
    pub fn create(inner: Arc<FlightSender>) -> FlightSenderWrapper {
        FlightSenderWrapper { inner }
    }

    #[async_backtrace::framed]
    pub async fn send(&self, data: DataPacket) -> Result<()> {
        self.inner.send(data).await
    }

    pub fn close(&self) {
        self.inner.close();
    }
}

impl Drop for FlightSenderWrapper {
    fn drop(&mut self) {
        drop_guard(move || {
            self.inner.close();
        })
    }
}

pub enum FlightExchange {
    Dummy,
    Receiver {
        notify: Arc<WatchNotify>,
        receiver: Receiver<Result<FlightData>>,
        // connection_info used for retrying when receiver get error
        connection_info: Option<ConnectionInfo>,
    },
    Sender(Sender<Result<FlightData, Status>>),
}

impl FlightExchange {
    pub fn create_sender(sender: Sender<Result<FlightData, Status>>) -> FlightExchange {
        FlightExchange::Sender(sender)
    }

    pub fn create_receiver(
        notify: Arc<WatchNotify>,
        receiver: Receiver<Result<FlightData>>,
        connection_info: Option<ConnectionInfo>,
    ) -> FlightExchange {
        FlightExchange::Receiver {
            notify,
            receiver,
            connection_info,
        }
    }

    pub fn convert_to_sender(self) -> FlightSender {
        match self {
            FlightExchange::Sender(tx) => FlightSender {
                tx: Mutex::new(tx),
                seq_num: AtomicUsize::new(0),
                buffer: Mutex::new(VecDeque::with_capacity(3)),
            },
            _ => unreachable!(),
        }
    }

    pub fn convert_to_receiver(self) -> FlightReceiver {
        match self {
            FlightExchange::Receiver {
                notify,
                receiver,
                connection_info,
            } => FlightReceiver {
                seq_num: AtomicUsize::new(0),
                notify: RwLock::new(notify),
                rx: RwLock::new(receiver),
                connection_info,
            },
            _ => unreachable!(),
        }
    }
}
