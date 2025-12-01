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

use std::pin::Pin;
use std::sync::Arc;

use arrow_array::RecordBatch;
use arrow_array::StringArray;
use arrow_flight::encode::FlightDataEncoderBuilder;
use arrow_flight::flight_service_server::FlightService;
use arrow_flight::flight_service_server::FlightServiceServer;
use arrow_flight::Action;
use arrow_flight::ActionType;
use arrow_flight::Criteria;
use arrow_flight::Empty;
use arrow_flight::FlightData;
use arrow_flight::FlightDescriptor;
use arrow_flight::FlightEndpoint;
use arrow_flight::FlightInfo;
use arrow_flight::HandshakeRequest;
use arrow_flight::HandshakeResponse;
use arrow_flight::PollInfo;
use arrow_flight::PutResult;
use arrow_flight::SchemaResult;
use arrow_flight::Ticket;
use arrow_schema::DataType as ArrowDataType;
use arrow_schema::Field;
use arrow_schema::Schema;
use databend_common_base::base::tokio;
use databend_common_exception::Result;
use databend_common_expression::types::DataType;
use databend_common_expression::udf_client::UDFFlightClient;
use databend_common_expression::BlockEntry;
use databend_common_expression::Column;
use futures::stream;
use futures::Stream;
use futures::StreamExt;
use tokio::time::timeout;
use tokio_stream::wrappers::TcpListenerStream;
use tonic::transport::Server;
use tonic::Request;
use tonic::Response;
use tonic::Status;

#[derive(Clone)]
enum MockMode {
    Transport,
    ServerStatus(String),
    MalformedData,
    SchemaMismatch,
}

struct MockFlightService {
    mode: MockMode,
}

type FlightStream<T> = Pin<Box<dyn Stream<Item = std::result::Result<T, Status>> + Send + 'static>>;

#[tonic::async_trait]
impl FlightService for MockFlightService {
    type HandshakeStream = FlightStream<HandshakeResponse>;

    async fn handshake(
        &self,
        _request: Request<tonic::Streaming<HandshakeRequest>>,
    ) -> std::result::Result<Response<Self::HandshakeStream>, Status> {
        Err(Status::unimplemented("handshake not supported"))
    }

    type ListFlightsStream = FlightStream<FlightInfo>;

    async fn list_flights(
        &self,
        _request: Request<Criteria>,
    ) -> std::result::Result<Response<Self::ListFlightsStream>, Status> {
        Err(Status::unimplemented("list flights not supported"))
    }

    async fn get_flight_info(
        &self,
        request: Request<FlightDescriptor>,
    ) -> std::result::Result<Response<FlightInfo>, Status> {
        if matches!(self.mode, MockMode::SchemaMismatch) {
            let schema = Schema::new(vec![
                Field::new("arg1", ArrowDataType::Null, true),
                Field::new("result", ArrowDataType::Utf8, false),
            ]);
            let descriptor = request.into_inner();
            let info = FlightInfo::new()
                .try_with_schema(&schema)
                .map_err(|e| Status::internal(e.to_string()))?
                .with_descriptor(descriptor)
                .with_endpoint(FlightEndpoint::new());
            Ok(Response::new(info))
        } else {
            Err(Status::unimplemented("get flight info not supported"))
        }
    }

    async fn poll_flight_info(
        &self,
        _request: Request<FlightDescriptor>,
    ) -> std::result::Result<Response<PollInfo>, Status> {
        Err(Status::unimplemented("poll flight info not supported"))
    }

    async fn get_schema(
        &self,
        _request: Request<FlightDescriptor>,
    ) -> std::result::Result<Response<SchemaResult>, Status> {
        match &self.mode {
            MockMode::SchemaMismatch => {
                let err = Status::internal("schema mismatch: expected Int32, got Utf8");
                Err(err)
            }
            _ => Err(Status::unimplemented("get schema not supported")),
        }
    }

    type DoGetStream = FlightStream<FlightData>;

    async fn do_get(
        &self,
        _request: Request<Ticket>,
    ) -> std::result::Result<Response<Self::DoGetStream>, Status> {
        Err(Status::unimplemented("do_get not supported"))
    }

    type DoPutStream = FlightStream<PutResult>;

    async fn do_put(
        &self,
        _request: Request<tonic::Streaming<FlightData>>,
    ) -> std::result::Result<Response<Self::DoPutStream>, Status> {
        Err(Status::unimplemented("do_put not supported"))
    }

    type DoExchangeStream = FlightStream<FlightData>;

    async fn do_exchange(
        &self,
        _request: Request<tonic::Streaming<FlightData>>,
    ) -> std::result::Result<Response<Self::DoExchangeStream>, Status> {
        let stream: FlightStream<FlightData> = match &self.mode {
            MockMode::Transport => Box::pin(stream::once(async {
                Err(Status::internal(
                    "h2 protocol error: error reading a body from connection",
                ))
            })),
            MockMode::ServerStatus(message) => {
                let message = message.clone();
                Box::pin(stream::once(async move {
                    Err(Status::invalid_argument(message))
                }))
            }
            MockMode::MalformedData => {
                let invalid = FlightData::default();
                Box::pin(stream::iter(vec![Ok(invalid)]))
            }
            MockMode::SchemaMismatch => {
                let schema = Arc::new(Schema::new(vec![Field::new(
                    "result",
                    ArrowDataType::Utf8,
                    false,
                )]));
                let batch =
                    RecordBatch::try_new(schema.clone(), vec![Arc::new(StringArray::from(vec![
                        "hello",
                    ]))])
                    .expect("build record batch");
                let stream = stream::iter(vec![Ok(batch)]);
                let encoder = FlightDataEncoderBuilder::new()
                    .with_schema(schema)
                    .build(stream)
                    .map(|res| res.map_err(|e| Status::internal(e.to_string())));
                Box::pin(encoder)
            }
        };
        Ok(Response::new(stream))
    }

    type DoActionStream = FlightStream<arrow_flight::Result>;

    async fn do_action(
        &self,
        _request: Request<Action>,
    ) -> std::result::Result<Response<Self::DoActionStream>, Status> {
        Err(Status::unimplemented("do_action not supported"))
    }

    type ListActionsStream = FlightStream<ActionType>;

    async fn list_actions(
        &self,
        _request: Request<Empty>,
    ) -> std::result::Result<Response<Self::ListActionsStream>, Status> {
        Err(Status::unimplemented("list_actions not supported"))
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn transport_error_returns_friendly_hint() -> Result<()> {
    let message = run_mock_exchange(MockMode::Transport).await?;
    assert!(
        message.contains("stopped responding before it finished"),
        "unexpected transport message: {message}"
    );
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn server_status_error_includes_message() -> Result<()> {
    let message = run_mock_exchange(MockMode::ServerStatus(
        "remote validation failed".to_string(),
    ))
    .await?;
    assert!(
        message.contains("reported an error: remote validation failed"),
        "unexpected server status message: {message}"
    );
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn malformed_data_returns_parse_hint() -> Result<()> {
    let message = run_mock_exchange(MockMode::MalformedData).await?;
    assert!(
        message.contains("could not parse"),
        "unexpected malformed data message: {message}"
    );
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn schema_mismatch_returns_schema_hint() -> Result<()> {
    let message = run_mock_exchange(MockMode::SchemaMismatch).await?;
    assert!(
        message.contains("return incorrect type"),
        "unexpected schema mismatch message: {message}"
    );
    Ok(())
}

async fn run_mock_exchange(mode: MockMode) -> Result<String> {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let address = listener.local_addr()?;
    let incoming = TcpListenerStream::new(listener);

    let service = MockFlightService { mode };
    let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel::<()>();
    let server = databend_common_base::runtime::spawn(async move {
        let _ = Server::builder()
            .add_service(FlightServiceServer::new(service))
            .serve_with_incoming_shutdown(incoming, async move {
                let _ = shutdown_rx.await;
            })
            .await;
    });

    tokio::time::sleep(std::time::Duration::from_millis(50)).await;

    let endpoint =
        UDFFlightClient::build_endpoint(&format!("http://{}", address), 3, 3, "udf-client-test")?;
    let mut client = UDFFlightClient::connect("mock_udf", endpoint, 3, 1024).await?;

    let num_rows = 1;
    let args = vec![BlockEntry::from(Column::Null { len: num_rows })];
    let return_type = DataType::Null;
    let result = timeout(
        std::time::Duration::from_secs(5),
        client.do_exchange(
            "mock_udf",
            "mock_handler",
            Some(num_rows),
            args,
            &return_type,
        ),
    )
    .await
    .expect("do_exchange future timed out");

    let _ = shutdown_tx.send(());
    let _ = timeout(std::time::Duration::from_secs(5), server)
        .await
        .expect("server shutdown timed out");

    let err = result.expect_err("expected failure");
    Ok(err.message().to_string())
}
