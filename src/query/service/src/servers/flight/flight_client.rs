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
use arrow_flight::flight_service_client::FlightServiceClient;
use async_channel::Receiver;
use databend_common_base::runtime::Runtime;
use databend_common_base::runtime::drop_guard;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use serde::Deserialize;
use serde::Serialize;
use tokio::sync::Semaphore;
use tokio::time::Duration;
use tonic::Request;
use tonic::Streaming;
use tonic::metadata::AsciiMetadataKey;
use tonic::metadata::AsciiMetadataValue;
use tonic::transport::channel::Channel;

use crate::servers::flight::v1::network::NetworkOutbound;
use crate::servers::flight::v1::network::PendingNetworkOutbound;
use crate::servers::flight::v1::network::SendOutcome;
use crate::servers::flight::v1::packets::DataPacket;

#[derive(Clone, Debug, Eq, Hash, PartialEq, Serialize, Deserialize)]
pub enum DoExchangeStream {
    /// Thread topology belongs to the admitted exchange; reconnect only identifies it.
    Blocks {
        exchange_id: String,
    },
    Packets {
        channel_id: String,
    },
    Statistics,
}

/// Parameters for a do_exchange RPC call, serialized as JSON in metadata.
#[derive(Clone, Serialize, Deserialize)]
pub struct DoExchangeParams {
    pub query_id: String,
    pub exchange_session_id: String,
    pub source_id: String,
    pub stream: DoExchangeStream,
}

pub struct FlightClient {
    inner: FlightServiceClient<Channel>,
    info: FlightClientInfo,
}

#[derive(Clone)]
pub(crate) struct FlightClientInfo {
    pub(crate) local_node_id: String,
    pub(crate) remote_node_id: String,
}

#[derive(Clone, Copy)]
pub(crate) enum FlightOperation {
    Connect,
    DoAction,
    DoExchange,
}

impl FlightOperation {
    fn as_str(self) -> &'static str {
        match self {
            FlightOperation::Connect => "connect",
            FlightOperation::DoAction => "do_action",
            FlightOperation::DoExchange => "do_exchange",
        }
    }
}

impl FlightClientInfo {
    pub(crate) fn new(local_node_id: impl Into<String>, remote_node_id: impl Into<String>) -> Self {
        Self {
            local_node_id: local_node_id.into(),
            remote_node_id: remote_node_id.into(),
        }
    }

    pub(crate) fn add_error_context(
        &self,
        error: ErrorCode,
        operation: FlightOperation,
    ) -> ErrorCode {
        error.add_message_back(format!(
            "(flight {}, client={}, service={})",
            operation.as_str(),
            self.local_node_id,
            self.remote_node_id,
        ))
    }
}

// TODO: Integration testing required
impl FlightClient {
    pub fn new(
        inner: FlightServiceClient<Channel>,
        local_node_id: impl Into<String>,
        remote_node_id: impl Into<String>,
    ) -> FlightClient {
        Self::with_info(inner, FlightClientInfo::new(local_node_id, remote_node_id))
    }

    pub(crate) fn with_info(
        mut inner: FlightServiceClient<Channel>,
        info: FlightClientInfo,
    ) -> FlightClient {
        inner = inner.max_decoding_message_size(usize::MAX);
        inner = inner.max_encoding_message_size(usize::MAX);

        FlightClient { inner, info }
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
                body: body.into(),
                r#type: path.to_string(),
            }));

        request.set_timeout(Duration::from_secs(timeout));
        request.metadata_mut().insert(
            AsciiMetadataKey::from_str("secret").unwrap(),
            AsciiMetadataValue::from_str(&secret).unwrap(),
        );

        let response = self.inner.do_action(request).await.map_err(|status| {
            self.info
                .add_error_context(ErrorCode::from(status), FlightOperation::DoAction)
        })?;

        let response = response.into_inner().message().await.map_err(|status| {
            self.info
                .add_error_context(ErrorCode::from(status), FlightOperation::DoAction)
        })?;

        match response {
            Some(response) => {
                let mut deserializer = serde_json::Deserializer::from_slice(&response.body);
                deserializer.disable_recursion_limit();
                let deserializer = serde_stacker::Deserializer::new(&mut deserializer);

                Res::deserialize(deserializer).map_err(|cause| {
                    self.info.add_error_context(
                        ErrorCode::BadBytes(format!(
                            "Response payload deserialize error while in {:?}, cause: {}",
                            path, cause
                        )),
                        FlightOperation::DoAction,
                    )
                })
            }
            None => Err(self.info.add_error_context(
                ErrorCode::EmptyDataFromServer(format!(
                    "Can not receive data from flight server, action: {:?}",
                    path
                )),
                FlightOperation::DoAction,
            )),
        }
    }

    pub fn do_exchange(
        &mut self,
        request_rx: Receiver<FlightData>,
        params: DoExchangeParams,
    ) -> std::pin::Pin<
        Box<dyn std::future::Future<Output = Result<Streaming<FlightData>>> + Send + '_>,
    > {
        Box::pin(async move {
            let mut request = Request::new(request_rx);

            let params_json = serde_json::to_string(&params).map_err(|e| {
                ErrorCode::Internal(format!("Failed to serialize DoExchangeParams: {}", e))
            })?;
            if let Ok(value) = params_json.parse() {
                request.metadata_mut().insert("x-exchange-params", value);
            }

            match self.inner.do_exchange(request).await {
                Ok(response) => Ok(response.into_inner()),
                Err(status) => Err(self
                    .info
                    .add_error_context(ErrorCode::from(status), FlightOperation::DoExchange)),
            }
        })
    }
}

pub struct FlightReceiver {
    rx: Receiver<Result<FlightData>>,
}

impl Drop for FlightReceiver {
    fn drop(&mut self) {
        drop_guard(move || self.close())
    }
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

enum FlightSenderInner {
    Closed,
    Outbound(Arc<NetworkOutbound>),
}

pub struct FlightSender {
    inner: FlightSenderInner,
}

impl FlightSender {
    pub fn create_closed() -> FlightSender {
        FlightSender {
            inner: FlightSenderInner::Closed,
        }
    }

    pub(crate) fn from_pending_outbound(
        outbound: PendingNetworkOutbound,
        runtime: &Runtime,
    ) -> FlightSender {
        let outbound = outbound.start(Arc::new(Semaphore::new(8)), None, runtime);
        FlightSender {
            inner: FlightSenderInner::Outbound(Arc::new(outbound)),
        }
    }

    pub fn is_closed(&self) -> bool {
        match &self.inner {
            FlightSenderInner::Closed => true,
            FlightSenderInner::Outbound(outbound) => outbound.is_closed(),
        }
    }

    #[async_backtrace::framed]
    pub async fn send(&self, data: DataPacket) -> Result<SendOutcome> {
        let data = FlightData::try_from(data)?;
        match &self.inner {
            FlightSenderInner::Closed => Ok(SendOutcome::ReceiverClosed),
            FlightSenderInner::Outbound(outbound) => outbound.send(0, data).await,
        }
    }

    pub async fn finish(&self) -> Result<()> {
        match &self.inner {
            FlightSenderInner::Closed => Ok(()),
            FlightSenderInner::Outbound(outbound) => outbound.finish().await,
        }
    }
}
