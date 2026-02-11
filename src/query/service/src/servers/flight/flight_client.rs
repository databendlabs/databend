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

use std::str::FromStr;
use std::sync::Arc;

use arrow_flight::Action;
use arrow_flight::FlightData;
use arrow_flight::Ticket;
use arrow_flight::flight_service_client::FlightServiceClient;
use async_channel::Receiver;
use async_channel::Sender;
use databend_common_base::runtime::catch_unwind;
use databend_common_base::runtime::drop_guard;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use fastrace::Span;
use fastrace::func_path;
use fastrace::future::FutureExt;
use futures::StreamExt;
use futures_util::future::Either;
use log::debug;
use log::error;
use serde::Deserialize;
use serde::Serialize;
use tokio::time::Duration;
use tonic::Request;
use tonic::Status;
use tonic::Streaming;
use tonic::metadata::AsciiMetadataKey;
use tonic::metadata::AsciiMetadataValue;
use tonic::transport::channel::Channel;

use crate::pipelines::executor::WatchNotify;
use crate::servers::flight::request_builder::RequestBuilder;
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
    #[fastrace::trace]
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
        let uuid = uuid::Uuid::new_v4().to_string();
        debug!("[{}]FlightClient::do_action: path={:?}", &uuid, &path);
        let message_type = std::any::type_name::<T>();
        let body = match catch_unwind(|| -> std::result::Result<Vec<u8>, serde_json::Error> {
            let mut body = Vec::with_capacity(512);
            let mut serializer = serde_json::Serializer::new(&mut body);
            let serializer = serde_stacker::Serializer::new(&mut serializer);
            message.serialize(serializer)?;
            Ok(body)
        }) {
            Ok(Ok(body)) => body,
            Ok(Err(cause)) => {
                error!("{:?}", &cause);
                return Err(ErrorCode::BadArguments(format!(
                    "Request payload serialize error while in {:?}, type: {}, cause: {}",
                    path, message_type, cause
                )));
            }
            Err(cause) => {
                error!("{:?}", &cause);
                return Err(cause.add_message_back(format!(
                    "(while serializing flight action request: action={:?}, type={})",
                    path, message_type
                )));
            }
        };

        debug!("[{}]FlightClient::serd finish: path={:?}", &uuid, &path);

        drop(message);
        let mut request =
            databend_common_tracing::inject_span_to_tonic_request(Request::new(Action {
                body: body.into(),
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
                let response_type = std::any::type_name::<Res>();
                let response_len = response.body.len();
                debug!(
                    "[{}]FlightClient::receive response: path={:?}",
                    &uuid, &path
                );
                let after_deserd = match catch_unwind(
                    || -> std::result::Result<Res, serde_json::Error> {
                        let mut deserializer = serde_json::Deserializer::from_slice(&response.body);
                        deserializer.disable_recursion_limit();
                        let deserializer = serde_stacker::Deserializer::new(&mut deserializer);
                        Res::deserialize(deserializer)
                    },
                ) {
                    Ok(Ok(res)) => Ok(res),
                    Ok(Err(cause)) => {
                        error!("{:?}", &cause);
                        Err(ErrorCode::BadBytes(format!(
                            "Response payload deserialize error while in {:?}, type: {}, len: {}, cause: {}",
                            path, response_type, response_len, cause
                        )))
                    }
                    Err(cause) => {
                        error!("{:?}", &cause);
                        Err(cause.add_message_back(format!(
                        "(while deserializing flight action response: action={:?}, type={}, len={})",
                        path, response_type, response_len
                    )))
                    }
                };
                debug!("[{}]FlightClient::deserd finish: path={:?}", &uuid, &path);
                after_deserd
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
    ) -> Result<FlightExchange> {
        let streaming = self
            .get_streaming(
                RequestBuilder::create(Ticket::default())
                    .with_metadata("x-type", "request_server_exchange")?
                    .with_metadata("x-target", target)?
                    .with_metadata("x-query-id", query_id)?
                    .build(),
            )
            .await?;

        let (notify, rx) = Self::streaming_receiver(streaming);
        Ok(FlightExchange::create_receiver(notify, rx))
    }

    #[async_backtrace::framed]
    #[fastrace::trace]
    pub async fn do_get(&mut self, query_id: &str, channel_id: &str) -> Result<FlightExchange> {
        let request = RequestBuilder::create(Ticket::default())
            .with_metadata("x-type", "exchange_fragment")?
            .with_metadata("x-query-id", query_id)?
            .with_metadata("x-channel-id", channel_id)?
            .build();
        let request = databend_common_tracing::inject_span_to_tonic_request(request);

        let streaming = self.get_streaming(request).await?;

        let (notify, rx) = Self::streaming_receiver(streaming);
        Ok(FlightExchange::create_receiver(notify, rx))
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
        .in_span(Span::enter_with_local_parent(func_path!()));

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
}

pub struct FlightReceiver {
    notify: Arc<WatchNotify>,
    rx: Receiver<Result<FlightData>>,
}

impl Drop for FlightReceiver {
    fn drop(&mut self) {
        drop_guard(move || {
            self.close();
        })
    }
}

impl FlightReceiver {
    pub fn create(rx: Receiver<Result<FlightData>>) -> FlightReceiver {
        FlightReceiver {
            rx,
            notify: Arc::new(WatchNotify::new()),
        }
    }

    #[async_backtrace::framed]
    pub async fn recv(&self) -> Result<Option<DataPacket>> {
        match self.rx.recv().await {
            Err(_) => Ok(None),
            Ok(Err(error)) => Err(error),
            Ok(Ok(message)) => Ok(Some(DataPacket::try_from(message)?)),
        }
    }

    pub fn close(&self) {
        self.rx.close();
        self.notify.notify_waiters();
    }
}

pub struct FlightSender {
    tx: Sender<std::result::Result<FlightData, Status>>,
}

impl FlightSender {
    pub fn create(tx: Sender<std::result::Result<FlightData, Status>>) -> FlightSender {
        FlightSender { tx }
    }

    pub fn is_closed(&self) -> bool {
        self.tx.is_closed()
    }

    #[async_backtrace::framed]
    pub async fn send(&self, data: DataPacket) -> Result<()> {
        if let Err(_cause) = self.tx.send(Ok(FlightData::try_from(data)?)).await {
            return Err(ErrorCode::AbortedQuery(
                "Aborted query, because the remote flight channel is closed.",
            ));
        }

        Ok(())
    }

    pub fn close(&self) {
        self.tx.close();
    }
}

pub enum FlightExchange {
    Dummy,
    Receiver {
        notify: Arc<WatchNotify>,
        receiver: Receiver<Result<FlightData>>,
    },
    Sender(Sender<std::result::Result<FlightData, Status>>),
}

impl FlightExchange {
    pub fn create_sender(
        sender: Sender<std::result::Result<FlightData, Status>>,
    ) -> FlightExchange {
        FlightExchange::Sender(sender)
    }

    pub fn create_receiver(
        notify: Arc<WatchNotify>,
        receiver: Receiver<Result<FlightData>>,
    ) -> FlightExchange {
        FlightExchange::Receiver { notify, receiver }
    }

    pub fn convert_to_sender(self) -> FlightSender {
        match self {
            FlightExchange::Sender(tx) => FlightSender { tx },
            _ => unreachable!(),
        }
    }

    pub fn convert_to_receiver(self) -> FlightReceiver {
        match self {
            FlightExchange::Receiver { notify, receiver } => FlightReceiver {
                notify,
                rx: receiver,
            },
            _ => unreachable!(),
        }
    }
}
