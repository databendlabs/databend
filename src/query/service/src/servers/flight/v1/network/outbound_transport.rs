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

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;
use std::time::Duration;
use std::time::Instant;

use arrow_flight::FlightData;
use async_channel::Receiver;
use async_channel::Sender;
use async_channel::TrySendError;
use databend_common_base::base::WatchNotify;
use databend_common_base::runtime::Runtime;
use futures::StreamExt;
use futures::future::Either;
use futures::future::select;
use futures::stream::BoxStream;
use log::warn;
use parking_lot::Mutex;
use tokio::task::JoinHandle;
use tonic::Code;
use tonic::Status;

use super::do_exchange_protocol::DoExchangeFrame;

pub const REMOTE_FLIGHT_CHANNEL_CLOSED_MESSAGE: &str =
    "Aborted query, because the remote flight channel is closed.";

pub struct PingPongResponse {
    pub data: Result<FlightData, Status>,
    pub rtt: Duration,
}

pub trait PingPongCallback: Send + Sync + 'static {
    fn has_pending(&self) -> bool;
    fn on_response(&self, response: PingPongResponse);
    fn on_receiver_finished(&self);
}

pub struct DoExchangeConnection {
    pub request_tx: Sender<FlightData>,
    pub response_stream: BoxStream<'static, Result<FlightData, Status>>,
}

pub type DoExchangeConnectFuture =
    Pin<Box<dyn Future<Output = Result<DoExchangeConnection, Status>> + Send + 'static>>;
pub type DoExchangeConnector = Arc<dyn Fn() -> DoExchangeConnectFuture + Send + Sync + 'static>;

#[derive(Clone, Copy)]
pub struct DoExchangeRetryPolicy {
    pub max_retries: u64,
    pub retry_interval: Duration,
}

impl Default for DoExchangeRetryPolicy {
    fn default() -> Self {
        Self {
            max_retries: 0,
            retry_interval: Duration::ZERO,
        }
    }
}

pub struct PingPongExchangeInner {
    in_flight: AtomicBool,
    send_time: Mutex<Option<Instant>>,
    send_tx: Sender<FlightData>,
    finishing: AtomicBool,
    pub shutdown: WatchNotify,
}

pub struct PingPongExchange {
    pub num_threads: usize,
    inner: Arc<PingPongExchangeInner>,
    send_rx: Mutex<Option<Receiver<FlightData>>>,
    initial_connection: Mutex<Option<DoExchangeConnection>>,
    connector: DoExchangeConnector,
    retry_policy: DoExchangeRetryPolicy,
}

impl Drop for PingPongExchange {
    fn drop(&mut self) {
        self.finish();
    }
}

impl std::ops::Deref for PingPongExchange {
    type Target = PingPongExchangeInner;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

impl PingPongExchangeInner {
    pub fn get_rtt(&self) -> Duration {
        self.send_time
            .lock()
            .take()
            .map(|t| t.elapsed())
            .unwrap_or_default()
    }

    pub fn try_send(&self, data: FlightData) -> Result<Option<FlightData>, Status> {
        if self.in_flight.fetch_or(true, Ordering::SeqCst) {
            return Ok(Some(data));
        }

        self.force_send(data)
    }

    pub(crate) fn force_send(&self, data: FlightData) -> Result<Option<FlightData>, Status> {
        *self.send_time.lock() = Some(Instant::now());
        match self.send_tx.try_send(data) {
            Ok(_) => Ok(None),
            Err(TrySendError::Closed(_)) => {
                *self.send_time.lock() = None;
                self.in_flight.store(false, Ordering::SeqCst);
                Err(Status::aborted(REMOTE_FLIGHT_CHANNEL_CLOSED_MESSAGE))
            }
            Err(TrySendError::Full(data)) => {
                *self.send_time.lock() = None;
                self.in_flight.store(false, Ordering::SeqCst);
                Ok(Some(data))
            }
        }
    }

    pub fn ready_send(&self) {
        self.in_flight.store(false, Ordering::SeqCst);
    }
}

impl PingPongExchange {
    pub async fn connect(
        num_threads: usize,
        connector: DoExchangeConnector,
        retry_policy: DoExchangeRetryPolicy,
    ) -> Result<Self, Status> {
        let initial_connection = connect_with_retry(&connector, retry_policy).await?;
        Ok(Self::from_connection(
            num_threads,
            initial_connection,
            connector,
            retry_policy,
        ))
    }

    fn from_connection(
        num_threads: usize,
        initial_connection: DoExchangeConnection,
        connector: DoExchangeConnector,
        retry_policy: DoExchangeRetryPolicy,
    ) -> Self {
        let (send_tx, send_rx) = async_channel::bounded(1);
        let inner = Arc::new(PingPongExchangeInner {
            in_flight: AtomicBool::new(false),
            send_time: Mutex::new(None),
            send_tx,
            finishing: AtomicBool::new(false),
            shutdown: WatchNotify::new(),
        });

        Self {
            inner,
            num_threads,
            send_rx: Mutex::new(Some(send_rx)),
            initial_connection: Mutex::new(Some(initial_connection)),
            connector,
            retry_policy,
        }
    }

    pub fn from_parts(
        num_threads: usize,
        send_tx: Sender<FlightData>,
        response_stream: tonic::Streaming<FlightData>,
    ) -> Self {
        Self::from_stream(num_threads, send_tx, response_stream)
    }

    pub fn from_stream(
        num_threads: usize,
        request_tx: Sender<FlightData>,
        stream: impl futures::Stream<Item = Result<FlightData, Status>> + Send + 'static,
    ) -> Self {
        let mut sequence = 0;
        let stream = stream.map(move |result| match result {
            Ok(response) if DoExchangeFrame::try_from(response.clone()).is_ok() => Ok(response),
            Ok(_) => {
                let response = DoExchangeFrame::Ack { sequence }.into();
                sequence += 1;
                Ok(response)
            }
            Err(status) => Err(status),
        });
        let connector: DoExchangeConnector = Arc::new(|| {
            Box::pin(async { Err(Status::aborted(REMOTE_FLIGHT_CHANNEL_CLOSED_MESSAGE)) })
        });
        Self::from_connection(
            num_threads,
            DoExchangeConnection {
                request_tx,
                response_stream: Box::pin(stream),
            },
            connector,
            DoExchangeRetryPolicy {
                max_retries: 1,
                retry_interval: Duration::ZERO,
            },
        )
    }

    pub fn finish(&self) {
        self.inner.finishing.store(true, Ordering::Release);
        self.inner.shutdown.notify_waiters();
    }

    pub fn start(
        &self,
        callback: Arc<dyn PingPongCallback>,
        runtime: &Runtime,
    ) -> Result<JoinHandle<()>, Status> {
        let Some(send_rx) = self.send_rx.lock().take() else {
            return Err(Status::already_exists("do_exchange sender already started"));
        };
        let Some(initial_connection) = self.initial_connection.lock().take() else {
            return Err(Status::already_exists(
                "do_exchange connection already started",
            ));
        };

        let inner = self.inner.clone();
        let connector = self.connector.clone();
        let retry_policy = self.retry_policy;
        Ok(runtime.spawn(async move {
            run_exchange(
                inner,
                send_rx,
                initial_connection,
                connector,
                retry_policy,
                callback,
            )
            .await;
        }))
    }
}

fn is_retriable(status: &Status) -> bool {
    matches!(
        status.code(),
        Code::Unavailable | Code::Cancelled | Code::DeadlineExceeded | Code::Unknown
    )
}

async fn connect_with_retry(
    connector: &DoExchangeConnector,
    retry_policy: DoExchangeRetryPolicy,
) -> Result<DoExchangeConnection, Status> {
    let mut retries = 0;
    loop {
        match connector().await {
            Ok(connection) => return Ok(connection),
            Err(status) if is_retriable(&status) && retries < retry_policy.max_retries => {
                retries += 1;
                warn!(
                    "do_exchange connect failed, retry {}/{}: {}",
                    retries, retry_policy.max_retries, status
                );
                tokio::time::sleep(retry_policy.retry_interval).await;
            }
            Err(status) => return Err(status),
        }
    }
}

async fn round_trip(
    connection: &mut Option<DoExchangeConnection>,
    connector: &DoExchangeConnector,
    retry_policy: DoExchangeRetryPolicy,
    frame: FlightData,
) -> Result<DoExchangeFrame, Status> {
    let mut retries = 0;

    loop {
        if connection.is_none() {
            match connector().await {
                Ok(new_connection) => {
                    *connection = Some(new_connection);
                }
                Err(status) if is_retriable(&status) && retries < retry_policy.max_retries => {
                    retries += 1;
                    warn!(
                        "do_exchange reconnect failed, retry {}/{}: {}",
                        retries, retry_policy.max_retries, status
                    );
                    tokio::time::sleep(retry_policy.retry_interval).await;
                    continue;
                }
                Err(status) => return Err(status),
            }
        }

        let result = {
            let connection = connection
                .as_mut()
                .expect("do_exchange connection must be initialized");

            match connection.request_tx.send(frame.clone()).await {
                Ok(()) => match connection.response_stream.next().await {
                    Some(Ok(response)) => DoExchangeFrame::try_from(response),
                    Some(Err(status)) => Err(status),
                    None => Err(Status::unavailable(
                        "do_exchange response stream closed without a terminal frame",
                    )),
                },
                Err(_) => Err(Status::unavailable(
                    "do_exchange request stream closed while sending",
                )),
            }
        };

        match result {
            Ok(response) => return Ok(response),
            Err(status) if is_retriable(&status) && retries < retry_policy.max_retries => {
                retries += 1;
                warn!(
                    "do_exchange communication failed, reconnect {}/{}: {}",
                    retries, retry_policy.max_retries, status
                );
                connection.take();
                tokio::time::sleep(retry_policy.retry_interval).await;
            }
            Err(status) => return Err(status),
        }
    }
}

async fn run_exchange(
    inner: Arc<PingPongExchangeInner>,
    send_rx: Receiver<FlightData>,
    initial_connection: DoExchangeConnection,
    connector: DoExchangeConnector,
    retry_policy: DoExchangeRetryPolicy,
    callback: Arc<dyn PingPongCallback>,
) {
    let mut connection = Some(initial_connection);
    let mut next_sequence = 0;
    let mut finishing = false;
    let mut shutdown_fut = Box::pin(inner.shutdown.notified());

    loop {
        finishing |= inner.finishing.load(Ordering::Acquire);
        if finishing && !inner.in_flight.load(Ordering::Acquire) && !callback.has_pending() {
            match round_trip(
                &mut connection,
                &connector,
                retry_policy,
                DoExchangeFrame::Finish {
                    sequence: next_sequence,
                }
                .into(),
            )
            .await
            {
                Ok(DoExchangeFrame::FinishAck { sequence }) if sequence == next_sequence => {}
                Ok(DoExchangeFrame::ReceiverFinished) => callback.on_receiver_finished(),
                Ok(response) => callback.on_response(PingPongResponse {
                    data: Err(Status::failed_precondition(format!(
                        "unexpected do_exchange FINISH response: {:?}",
                        response
                    ))),
                    rtt: Duration::ZERO,
                }),
                Err(status) => callback.on_response(PingPongResponse {
                    data: Err(status),
                    rtt: Duration::ZERO,
                }),
            }
            inner.send_tx.close();
            break;
        }

        let recv_fut = Box::pin(send_rx.recv());
        let data = match select(shutdown_fut, recv_fut).await {
            Either::Left((_, _)) => {
                finishing = true;
                shutdown_fut = Box::pin(inner.shutdown.notified());
                continue;
            }
            Either::Right((Ok(data), next_shutdown)) => {
                shutdown_fut = next_shutdown;
                data
            }
            Either::Right((Err(_), _)) => break,
        };

        let exchange_fut = Box::pin(round_trip(
            &mut connection,
            &connector,
            retry_policy,
            DoExchangeFrame::Data {
                sequence: next_sequence,
                data,
            }
            .into(),
        ));
        let response = if finishing {
            exchange_fut.await
        } else {
            match select(shutdown_fut, exchange_fut).await {
                Either::Left((_, pending_exchange)) => {
                    finishing = true;
                    shutdown_fut = Box::pin(inner.shutdown.notified());
                    pending_exchange.await
                }
                Either::Right((response, next_shutdown)) => {
                    shutdown_fut = next_shutdown;
                    response
                }
            }
        };

        let rtt = inner.get_rtt();
        match response {
            Ok(DoExchangeFrame::Ack { sequence }) if sequence == next_sequence => {
                next_sequence += 1;
                callback.on_response(PingPongResponse {
                    data: Ok(FlightData::default()),
                    rtt,
                });
            }
            Ok(DoExchangeFrame::ReceiverFinished) => {
                callback.on_receiver_finished();
                inner.ready_send();
                inner.send_tx.close();
                break;
            }
            Ok(response) => {
                callback.on_response(PingPongResponse {
                    data: Err(Status::failed_precondition(format!(
                        "unexpected do_exchange DATA response: {:?}",
                        response
                    ))),
                    rtt,
                });
                inner.send_tx.close();
                break;
            }
            Err(status) => {
                callback.on_response(PingPongResponse {
                    data: Err(status),
                    rtt,
                });
                inner.send_tx.close();
                break;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::servers::flight::v1::network::do_exchange_protocol::DoExchangeFrame;

    fn create_mock_exchange(
        num_threads: usize,
    ) -> (
        PingPongExchange,
        async_channel::Receiver<FlightData>,
        async_channel::Sender<Result<FlightData, Status>>,
    ) {
        let (send_tx, send_rx) = async_channel::bounded(1);
        let (pong_tx, pong_rx) = async_channel::unbounded();
        let exchange = PingPongExchange::from_stream(num_threads, send_tx, pong_rx);
        (exchange, send_rx, pong_tx)
    }

    fn make_flight_data(len: usize) -> FlightData {
        FlightData {
            data_body: bytes::Bytes::from(vec![0u8; len]),
            ..Default::default()
        }
    }

    #[test]
    fn test_ping_pong_in_flight_returns_data() {
        let (exchange, _send_rx, _pong_tx) = create_mock_exchange(2);

        assert!(exchange.try_send(make_flight_data(1)).unwrap().is_none());
        let returned = exchange.try_send(make_flight_data(2)).unwrap();
        assert_eq!(returned.unwrap().data_body.len(), 2);
    }

    #[tokio::test]
    async fn test_reconnect_resends_unacknowledged_sequence() {
        let attempts = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let received_sequences = Arc::new(Mutex::new(Vec::new()));

        let connector: DoExchangeConnector = {
            let attempts = attempts.clone();
            let received_sequences = received_sequences.clone();
            Arc::new(move || {
                let attempt = attempts.fetch_add(1, Ordering::SeqCst);
                let received_sequences = received_sequences.clone();
                Box::pin(async move {
                    let (request_tx, request_rx) = async_channel::bounded(1);
                    let (response_tx, response_rx) = async_channel::bounded(1);
                    tokio::spawn(async move {
                        let request = request_rx.recv().await.unwrap();
                        let DoExchangeFrame::Data { sequence, .. } =
                            DoExchangeFrame::try_from(request).unwrap()
                        else {
                            panic!("expected DATA");
                        };
                        received_sequences.lock().push(sequence);

                        if attempt == 0 {
                            response_tx
                                .send(Err(Status::unavailable("connection lost")))
                                .await
                                .unwrap();
                        } else {
                            response_tx
                                .send(Ok(DoExchangeFrame::Ack { sequence }.into()))
                                .await
                                .unwrap();
                        }
                    });

                    Ok(DoExchangeConnection {
                        request_tx,
                        response_stream: Box::pin(response_rx),
                    })
                })
            })
        };

        let exchange = PingPongExchange::connect(1, connector, DoExchangeRetryPolicy {
            max_retries: 1,
            retry_interval: Duration::ZERO,
        })
        .await
        .unwrap();

        struct Callback(Arc<AtomicBool>);
        impl PingPongCallback for Callback {
            fn has_pending(&self) -> bool {
                false
            }

            fn on_response(&self, response: PingPongResponse) {
                assert!(response.data.is_ok());
                self.0.store(true, Ordering::SeqCst);
            }

            fn on_receiver_finished(&self) {
                panic!("receiver must remain active");
            }
        }

        let completed = Arc::new(AtomicBool::new(false));
        let runtime = Runtime::with_worker_threads(2, None).unwrap();
        exchange
            .start(Arc::new(Callback(completed.clone())), &runtime)
            .unwrap();
        exchange.try_send(make_flight_data(1)).unwrap();

        tokio::time::timeout(Duration::from_secs(2), async {
            while !completed.load(Ordering::SeqCst) {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();

        assert_eq!(*received_sequences.lock(), vec![0, 0]);
    }
}
