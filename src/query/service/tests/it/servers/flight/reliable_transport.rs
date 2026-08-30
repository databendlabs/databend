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
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;
use std::time::Duration;

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
use arrow_flight::flight_service_client::FlightServiceClient;
use arrow_flight::flight_service_server::FlightService;
use arrow_flight::flight_service_server::FlightServiceServer;
use async_channel::Receiver;
use databend_common_base::runtime::Runtime;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_grpc::ConnectionFactory;
use databend_query::servers::flight::v1::transport::DeliveryOutcome;
use databend_query::servers::flight::v1::transport::InboundDelivery;
use databend_query::servers::flight::v1::transport::OutboundStream;
use databend_query::servers::flight::v1::transport::StreamSendOutcome;
use databend_query::servers::flight::v1::transport::reliable::DoExchangeConnector;
use databend_query::servers::flight::v1::transport::reliable::DoExchangeTransport;
use databend_query::servers::flight::v1::transport::reliable::FlightReconnectPolicy;
use databend_query::servers::flight::v1::transport::reliable::PendingReliableOutbound;
use databend_query::servers::flight::v1::transport::reliable::ReliableInboundSource;
use databend_query::servers::flight::v1::transport::reliable::ReliableOutbound;
use futures::Stream;
use futures::StreamExt;
use parking_lot::Mutex;
use socket2::SockRef;
use tokio::io::copy_bidirectional;
use tokio::net::TcpListener;
use tokio::net::TcpStream;
use tokio::sync::Semaphore;
use tokio::sync::oneshot;
use tokio_stream::wrappers::TcpListenerStream;
use tokio_util::sync::CancellationToken;
use tonic::Request;
use tonic::Response;
use tonic::Status;
use tonic::Streaming;
use tonic::transport::Server;

struct TestDelivery {
    sender: async_channel::Sender<FlightData>,
    terminal: Mutex<Option<Option<ErrorCode>>>,
    consumer_closed: CancellationToken,
}

impl TestDelivery {
    fn create() -> (Arc<Self>, Receiver<FlightData>) {
        let (sender, receiver) = async_channel::unbounded();
        (
            Arc::new(Self {
                sender,
                terminal: Mutex::new(None),
                consumer_closed: CancellationToken::new(),
            }),
            receiver,
        )
    }

    fn close_consumer(&self) {
        self.consumer_closed.cancel();
    }

    fn terminal_error(&self) -> Option<ErrorCode> {
        self.terminal.lock().clone().flatten()
    }
}

#[async_trait::async_trait]
impl InboundDelivery for TestDelivery {
    async fn deliver(&self, _lane: usize, data: FlightData) -> Result<DeliveryOutcome> {
        if self.consumer_closed.is_cancelled() {
            return Ok(DeliveryOutcome::ConsumerClosed);
        }
        self.sender
            .send(data)
            .await
            .map(|_| DeliveryOutcome::Accepted)
            .map_err(|_| ErrorCode::AbortedQuery("test delivery closed"))
    }

    fn is_closed(&self) -> bool {
        self.consumer_closed.is_cancelled() || self.sender.is_closed()
    }

    fn consumer_closed(&self) -> Option<futures::future::BoxFuture<'static, ()>> {
        let closed = self.consumer_closed.clone();
        Some(Box::pin(async move { closed.cancelled().await }))
    }

    fn terminate(&self, cause: Option<ErrorCode>) {
        let mut terminal = self.terminal.lock();
        if terminal.is_none() {
            *terminal = Some(cause);
            self.sender.close();
        }
    }
}

type FlightStream<T> = Pin<Box<dyn Stream<Item = std::result::Result<T, Status>> + Send + 'static>>;

#[derive(Clone)]
struct ReliableFlightService {
    source: Arc<ReliableInboundSource>,
    runtime: Arc<Runtime>,
}

#[tonic::async_trait]
impl FlightService for ReliableFlightService {
    type HandshakeStream = FlightStream<HandshakeResponse>;

    async fn handshake(
        &self,
        _: Request<Streaming<HandshakeRequest>>,
    ) -> std::result::Result<Response<Self::HandshakeStream>, Status> {
        Err(Status::unimplemented("handshake"))
    }

    type ListFlightsStream = FlightStream<FlightInfo>;

    async fn list_flights(
        &self,
        _: Request<Criteria>,
    ) -> std::result::Result<Response<Self::ListFlightsStream>, Status> {
        Err(Status::unimplemented("list_flights"))
    }

    async fn get_flight_info(
        &self,
        _: Request<FlightDescriptor>,
    ) -> std::result::Result<Response<FlightInfo>, Status> {
        Ok(Response::new(
            FlightInfo::new().with_endpoint(FlightEndpoint::new()),
        ))
    }

    async fn poll_flight_info(
        &self,
        _: Request<FlightDescriptor>,
    ) -> std::result::Result<Response<PollInfo>, Status> {
        Err(Status::unimplemented("poll_flight_info"))
    }

    async fn get_schema(
        &self,
        _: Request<FlightDescriptor>,
    ) -> std::result::Result<Response<SchemaResult>, Status> {
        Err(Status::unimplemented("get_schema"))
    }

    type DoGetStream = FlightStream<FlightData>;

    async fn do_get(
        &self,
        _: Request<Ticket>,
    ) -> std::result::Result<Response<Self::DoGetStream>, Status> {
        Err(Status::unimplemented("do_get"))
    }

    type DoPutStream = FlightStream<PutResult>;

    async fn do_put(
        &self,
        _: Request<Streaming<FlightData>>,
    ) -> std::result::Result<Response<Self::DoPutStream>, Status> {
        Err(Status::unimplemented("do_put"))
    }

    type DoExchangeStream = FlightStream<FlightData>;

    async fn do_exchange(
        &self,
        request: Request<Streaming<FlightData>>,
    ) -> std::result::Result<Response<Self::DoExchangeStream>, Status> {
        let connection = self.source.connect(
            self.runtime.clone(),
            ErrorCode::CannotConnectNode("TCP attachment disconnected"),
        );
        let (tx, rx) = async_channel::bounded(1);
        let stream = request.into_inner();
        databend_common_base::runtime::spawn(async move {
            connection.serve(stream, tx).await;
        });
        Ok(Response::new(Box::pin(rx)))
    }

    type DoActionStream = FlightStream<arrow_flight::Result>;

    async fn do_action(
        &self,
        _: Request<Action>,
    ) -> std::result::Result<Response<Self::DoActionStream>, Status> {
        Err(Status::unimplemented("do_action"))
    }

    type ListActionsStream = FlightStream<ActionType>;

    async fn list_actions(
        &self,
        _: Request<Empty>,
    ) -> std::result::Result<Response<Self::ListActionsStream>, Status> {
        Err(Status::unimplemented("list_actions"))
    }
}

#[derive(Debug)]
enum ProxyFault {
    Reset,
    ResetAndRejectReconnects,
}

struct ProxyConnection {
    fault: oneshot::Sender<ProxyFault>,
}

struct TransportHarness {
    _runtime: Arc<Runtime>,
    delivery: Arc<TestDelivery>,
    receiver: Receiver<FlightData>,
    source: Arc<ReliableInboundSource>,
    outbound: Arc<ReliableOutbound>,
    connections: Receiver<ProxyConnection>,
    server_shutdown: Option<oneshot::Sender<()>>,
    proxy_task: tokio::task::JoinHandle<()>,
    server_task: tokio::task::JoinHandle<()>,
}

impl TransportHarness {
    async fn create(retry_times: u64, slots: usize) -> Self {
        let runtime = Arc::new(Runtime::with_worker_threads(1, None).unwrap());
        let (delivery, receiver) = TestDelivery::create();
        let source = Arc::new(ReliableInboundSource::new(
            delivery.clone(),
            Duration::from_secs(5),
            "test receiver".to_string(),
        ));

        let backend_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let backend_addr = backend_listener.local_addr().unwrap();
        let (server_shutdown, server_shutdown_rx) = oneshot::channel();
        let service = ReliableFlightService {
            source: source.clone(),
            runtime: runtime.clone(),
        };
        let server_task = databend_common_base::runtime::spawn(async move {
            Server::builder()
                .add_service(FlightServiceServer::new(service))
                .serve_with_incoming_shutdown(
                    TcpListenerStream::new(backend_listener),
                    async move {
                        let _ = server_shutdown_rx.await;
                    },
                )
                .await
                .unwrap();
        });

        let proxy_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let proxy_addr = proxy_listener.local_addr().unwrap();
        let reject_reconnects = Arc::new(AtomicBool::new(false));
        let (connection_tx, connections) = async_channel::unbounded();
        let proxy_task = {
            let reject_reconnects = reject_reconnects.clone();
            databend_common_base::runtime::spawn(async move {
                while let Ok((mut client, _)) = proxy_listener.accept().await {
                    if reject_reconnects.load(Ordering::SeqCst) {
                        let _ = SockRef::from(&client).set_linger(Some(Duration::ZERO));
                        continue;
                    }
                    let mut backend = TcpStream::connect(backend_addr).await.unwrap();
                    let (fault, fault_rx) = oneshot::channel();
                    if connection_tx.send(ProxyConnection { fault }).await.is_err() {
                        return;
                    }
                    let reject_reconnects = reject_reconnects.clone();
                    databend_common_base::runtime::spawn(async move {
                        tokio::select! {
                            _ = copy_bidirectional(&mut client, &mut backend) => {}
                            command = fault_rx => {
                                if let Ok(command) = command {
                                    if matches!(command, ProxyFault::ResetAndRejectReconnects) {
                                        reject_reconnects.store(true, Ordering::SeqCst);
                                    }
                                    let _ = SockRef::from(&client)
                                        .set_linger(Some(Duration::ZERO));
                                    let _ = SockRef::from(&backend)
                                        .set_linger(Some(Duration::ZERO));
                                }
                            }
                        }
                    });
                }
            })
        };

        let connector: DoExchangeConnector = Arc::new(move || {
            Box::pin(async move {
                let channel = ConnectionFactory::create_rpc_channel(
                    proxy_addr,
                    Some(Duration::from_millis(500)),
                    None,
                    None,
                )
                .await
                .map_err(ErrorCode::from)?;
                let mut client = FlightServiceClient::new(channel);
                let (send_tx, send_rx) = async_channel::bounded(1);
                let response = client
                    .do_exchange(Request::new(send_rx))
                    .await
                    .map_err(ErrorCode::from)?;
                Ok(DoExchangeTransport {
                    send_tx,
                    response_stream: response.into_inner().boxed(),
                })
            })
        });
        let reconnect =
            FlightReconnectPolicy::new(retry_times, Duration::ZERO, Duration::from_millis(500));
        let pending = PendingReliableOutbound::connect(
            1,
            connector,
            reconnect,
            "test sender".to_string(),
            "test receiver".to_string(),
        )
        .await
        .unwrap();
        let outbound = Arc::new(pending.start(Arc::new(Semaphore::new(slots)), None, &runtime));

        Self {
            _runtime: runtime,
            delivery,
            receiver,
            source,
            outbound,
            connections,
            server_shutdown: Some(server_shutdown),
            proxy_task,
            server_task,
        }
    }

    fn payload(value: u8) -> FlightData {
        FlightData {
            data_body: vec![value].into(),
            ..Default::default()
        }
    }

    async fn next_connection(&self) -> ProxyConnection {
        tokio::time::timeout(Duration::from_secs(5), self.connections.recv())
            .await
            .expect("timed out waiting for a TCP connection")
            .expect("TCP proxy stopped")
    }
}

impl Drop for TransportHarness {
    fn drop(&mut self) {
        if let Some(shutdown) = self.server_shutdown.take() {
            let _ = shutdown.send(());
        }
        self.proxy_task.abort();
        self.server_task.abort();
    }
}

// Scenario: DATA reaches the consumer and normal producer completion closes both endpoints.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn normal_transfer_completes_both_endpoints() {
    let harness = TransportHarness::create(3, 8).await;

    assert_eq!(
        harness
            .outbound
            .send(0, TransportHarness::payload(1))
            .await
            .unwrap(),
        StreamSendOutcome::Accepted
    );
    harness.outbound.finish().await.unwrap();

    assert_eq!(
        harness.receiver.recv().await.unwrap(),
        TransportHarness::payload(1)
    );
    assert!(harness.receiver.recv().await.is_err());
    assert!(harness.delivery.terminal_error().is_none());
}

// Scenario: An idle downstream consumer can close the logical stream without more DATA.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn idle_consumer_close_reaches_the_sender() {
    let harness = TransportHarness::create(3, 8).await;

    harness.delivery.close_consumer();
    tokio::time::timeout(Duration::from_secs(1), harness.outbound.finish())
        .await
        .expect("consumer close must wake the sender")
        .unwrap();

    assert!(harness.outbound.is_closed());
    assert!(harness.delivery.terminal_error().is_none());
}

// Scenario: An idle receiver pipeline failure reaches the sender with its original cause.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn idle_receiver_failure_reaches_the_sender() {
    let harness = TransportHarness::create(3, 8).await;
    let cause = ErrorCode::AbortedQuery("receiver pipeline failed");

    harness.source.fail(cause.clone());
    let error = tokio::time::timeout(Duration::from_secs(1), harness.outbound.finish())
        .await
        .expect("receiver failure must wake the sender")
        .unwrap_err();

    assert_eq!(error.code(), cause.code());
    assert!(error.message().contains(&cause.message()));
}

// Scenario: A producer failure terminates the receiver with the producer's original error.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn producer_failure_reaches_the_receiver() {
    let harness = TransportHarness::create(3, 8).await;
    let cause = ErrorCode::AbortedQuery("producer serialization failed");

    harness.outbound.fail(cause.clone()).await;

    let error = harness
        .delivery
        .terminal_error()
        .expect("producer failure must terminate the receiver");
    assert_eq!(error.code(), cause.code());
    assert_eq!(error.message(), cause.message());
}

// Scenario: Cancellation releases a producer blocked by transport backpressure.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cancellation_releases_a_backpressured_sender() {
    let harness = TransportHarness::create(3, 0).await;
    let mut send = Box::pin(harness.outbound.send(0, TransportHarness::payload(2)));

    assert!(futures::poll!(&mut send).is_pending());
    harness.outbound.abort();
    let error = tokio::time::timeout(Duration::from_secs(1), send)
        .await
        .expect("cancellation must release the sender")
        .unwrap_err();

    assert_eq!(error.code(), ErrorCode::ABORTED_QUERY);
}

// Scenario: A real TCP reset after DATA is accepted reconnects and delivers it exactly once.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn tcp_reset_during_transfer_recovers_without_duplicate_data() {
    let harness = TransportHarness::create(3, 8).await;
    let first = harness.next_connection().await;

    assert_eq!(
        harness
            .outbound
            .send(0, TransportHarness::payload(3))
            .await
            .unwrap(),
        StreamSendOutcome::Accepted
    );
    first.fault.send(ProxyFault::Reset).unwrap();
    let _replacement = harness.next_connection().await;

    harness.outbound.finish().await.unwrap();
    assert_eq!(
        harness.receiver.recv().await.unwrap(),
        TransportHarness::payload(3)
    );
    assert!(harness.receiver.recv().await.is_err());
}

// Scenario: Reconnect failure stops after the configured retry budget instead of hanging.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn reconnect_budget_exhaustion_returns_an_error() {
    let harness = TransportHarness::create(3, 8).await;
    let first = harness.next_connection().await;

    first
        .fault
        .send(ProxyFault::ResetAndRejectReconnects)
        .unwrap();
    let error = tokio::time::timeout(Duration::from_secs(5), harness.outbound.finish())
        .await
        .expect("reconnect exhaustion must not hang")
        .unwrap_err();

    assert!(!error.message().is_empty());
}
