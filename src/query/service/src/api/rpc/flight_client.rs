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

use std::convert::TryInto;
use std::error::Error;

use async_channel::Receiver;
use async_channel::Sender;
use common_arrow::arrow_format::flight::data::Action;
use common_arrow::arrow_format::flight::data::FlightData;
use common_arrow::arrow_format::flight::data::Ticket;
use common_arrow::arrow_format::flight::service::flight_service_client::FlightServiceClient;
use common_base::base::tokio::time::Duration;
use common_base::runtime::GlobalIORuntime;
use common_base::runtime::TrySpawn;
use common_exception::ErrorCode;
use common_exception::Result;
use futures::StreamExt;
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
    pub fn new(mut inner: FlightServiceClient<Channel>) -> FlightClient {
        inner = inner.max_decoding_message_size(usize::MAX);
        inner = inner.max_encoding_message_size(usize::MAX);

        FlightClient { inner }
    }

    #[async_backtrace::framed]
    pub async fn execute_action(&mut self, action: FlightAction, timeout: u64) -> Result<()> {
        if let Err(cause) = self.do_action(action, timeout).await {
            return Err(cause.add_message_back("(while in query flight)"));
        }

        Ok(())
    }

    #[async_backtrace::framed]
    pub async fn request_server_exchange(
        &mut self,
        query_id: &str,
        target: &str,
    ) -> Result<FlightExchange> {
        let mut streaming = self
            .get_streaming(
                RequestBuilder::create(Ticket::default())
                    .with_metadata("x-type", "request_server_exchange")?
                    .with_metadata("x-target", target)?
                    .with_metadata("x-query-id", query_id)?
                    .build(),
            )
            .await?;

        let (tx, rx) = async_channel::bounded(1);
        GlobalIORuntime::instance().spawn({
            async move {
                while let Some(message) = streaming.next().await {
                    if tx.send(message.map_err(ErrorCode::from)).await.is_err() {
                        break;
                    }
                }

                tx.close();
            }
        });

        Ok(FlightExchange::create_receiver(rx))
    }

    #[async_backtrace::framed]
    pub async fn do_get(
        &mut self,
        query_id: &str,
        target: &str,
        fragment: usize,
    ) -> Result<FlightExchange> {
        let mut streaming = self
            .get_streaming(
                RequestBuilder::create(Ticket::default())
                    .with_metadata("x-type", "exchange_fragment")?
                    .with_metadata("x-target", target)?
                    .with_metadata("x-query-id", query_id)?
                    .with_metadata("x-fragment-id", &fragment.to_string())?
                    .build(),
            )
            .await?;

        let (tx, rx) = async_channel::bounded(1);
        GlobalIORuntime::instance().spawn({
            async move {
                while let Some(message) = streaming.next().await {
                    if tx.send(message.map_err(ErrorCode::from)).await.is_err() {
                        break;
                    }
                }

                tx.close();
            }
        });

        Ok(FlightExchange::create_receiver(rx))
    }

    #[async_backtrace::framed]
    async fn get_streaming(&mut self, request: Request<Ticket>) -> Result<Streaming<FlightData>> {
        match self.inner.do_get(request).await {
            Ok(res) => Ok(res.into_inner()),
            Err(status) => Err(ErrorCode::from(status).add_message_back("(while in query flight)")),
        }
    }

    // Execute do_action.
    #[tracing::instrument(level = "debug", skip_all)]
    #[async_backtrace::framed]
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

pub struct FlightReceiver {
    rx: Receiver<Result<FlightData>>,
}

impl FlightReceiver {
    pub fn create(rx: Receiver<Result<FlightData>>) -> FlightReceiver {
        FlightReceiver { rx }
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
    }
}

pub struct FlightSender {
    tx: Sender<Result<FlightData, Status>>,
}

impl FlightSender {
    pub fn create(tx: Sender<Result<FlightData, Status>>) -> FlightSender {
        FlightSender { tx }
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
    Receiver(Receiver<Result<FlightData>>),
    Sender(Sender<Result<FlightData, Status>>),
}

impl FlightExchange {
    pub fn create_sender(sender: Sender<Result<FlightData, Status>>) -> FlightExchange {
        FlightExchange::Sender(sender)
    }

    pub fn create_receiver(receiver: Receiver<Result<FlightData>>) -> FlightExchange {
        FlightExchange::Receiver(receiver)
    }

    pub fn convert_to_sender(self) -> FlightSender {
        match self {
            FlightExchange::Sender(tx) => FlightSender { tx },
            _ => unreachable!(),
        }
    }

    pub fn convert_to_receiver(self) -> FlightReceiver {
        match self {
            FlightExchange::Receiver(rx) => FlightReceiver { rx },
            _ => unreachable!(),
        }
    }
}

#[allow(dead_code)]
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
