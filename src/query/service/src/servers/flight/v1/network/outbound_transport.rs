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

use std::pin::pin;
use std::sync::Arc;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;
use std::time::Duration;
use std::time::Instant;

use arrow_flight::FlightData;
use async_channel::Sender;
use async_channel::TrySendError;
use databend_common_base::base::WatchNotify;
use databend_common_base::runtime::Runtime;
use futures::StreamExt;
use futures::future::Either;
use futures::future::select;
use futures::stream::BoxStream;
use parking_lot::Mutex;
use tokio::task::JoinHandle;
use tonic::Status;

/// Response from a ping-pong exchange.
pub struct PingPongResponse {
    pub data: Result<FlightData, Status>,
    pub rtt: Duration,
}

/// Callback trait for handling ping-pong responses.
pub trait PingPongCallback: Send + Sync + 'static {
    fn on_response(&self, response: PingPongResponse);
}

pub struct PingPongExchangeInner {
    in_flight: AtomicBool,
    send_time: Mutex<Option<Instant>>,
    send_tx: Sender<FlightData>,
    shutdown: WatchNotify,
}

/// A non-blocking ping-pong style flight exchange.
///
/// This exchange guarantees that at most one request is in-flight at any time.
/// When a request is sent, subsequent sends will return the data back to the caller
/// until a response is received.
pub struct PingPongExchange {
    pub num_threads: usize,
    inner: Arc<PingPongExchangeInner>,
    response_stream: Mutex<Option<BoxStream<'static, Result<FlightData, Status>>>>,
}

impl Drop for PingPongExchange {
    fn drop(&mut self) {
        self.inner.shutdown.notify_waiters();
    }
}

impl std::ops::Deref for PingPongExchange {
    type Target = PingPongExchangeInner;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

impl PingPongExchangeInner {
    /// Try to send data through the exchange.
    ///
    /// Returns:
    /// - `Ok(None)`: Data was sent successfully
    /// - `Ok(Some(data))`: A request is already in-flight, data is returned to caller
    /// - `Err(status)`: The exchange is closed
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
                Err(Status::aborted("Exchange closed"))
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
    pub fn from_parts(
        num_threads: usize,
        send_tx: async_channel::Sender<FlightData>,
        response_stream: tonic::Streaming<FlightData>,
    ) -> Self {
        Self::from_stream(num_threads, send_tx, response_stream)
    }

    pub fn from_stream(
        num_threads: usize,
        send_tx: async_channel::Sender<FlightData>,
        stream: impl futures::Stream<Item = Result<FlightData, Status>> + Send + 'static,
    ) -> Self {
        let inner = Arc::new(PingPongExchangeInner {
            in_flight: AtomicBool::new(false),
            send_time: Mutex::new(None),
            send_tx,
            shutdown: WatchNotify::new(),
        });

        Self {
            inner,
            num_threads,
            response_stream: Mutex::new(Some(Box::pin(stream))),
        }
    }

    /// Start the receiver with the given callback.
    ///
    /// This should be called before sending data. The callback will be invoked
    /// for each response received from the remote end.
    ///
    /// Returns an error if the receiver has already been started.
    pub fn start(
        &self,
        callback: Arc<dyn PingPongCallback>,
        runtime: &Runtime,
    ) -> Result<JoinHandle<()>, Status> {
        let Some(mut stream) = self.response_stream.lock().take() else {
            return Err(Status::already_exists("Receiver already started"));
        };

        let inner = self.inner.clone();
        Ok(runtime.spawn(async move {
            let mut shutdown_fut = pin!(inner.shutdown.notified());

            loop {
                match select(shutdown_fut, stream.next()).await {
                    Either::Left(_) => {
                        break;
                    }
                    Either::Right((None, _)) => {
                        let rtt = inner
                            .send_time
                            .lock()
                            .take()
                            .map(|t| t.elapsed())
                            .unwrap_or_default();
                        callback.on_response(PingPongResponse {
                            data: Err(Status::ok("Stream ended")),
                            rtt,
                        });
                        break;
                    }
                    Either::Right((Some(Ok(data)), next_shutdown)) => {
                        shutdown_fut = next_shutdown;
                        let rtt = inner
                            .send_time
                            .lock()
                            .take()
                            .map(|t| t.elapsed())
                            .unwrap_or_default();
                        callback.on_response(PingPongResponse {
                            data: Ok(data),
                            rtt,
                        });
                    }
                    Either::Right((Some(Err(status)), _)) => {
                        let rtt = inner
                            .send_time
                            .lock()
                            .take()
                            .map(|t| t.elapsed())
                            .unwrap_or_default();
                        callback.on_response(PingPongResponse {
                            data: Err(status),
                            rtt,
                        });
                        break;
                    }
                }
            }
        }))
    }
}

#[cfg(test)]
mod tests {
    use arrow_flight::FlightData;
    use tonic::Status;

    use super::*;

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

    #[tokio::test]
    async fn test_ping_pong_basic_send_recv() {
        let (exchange, send_rx, _pong_tx) = create_mock_exchange(2);

        // First send should succeed
        assert!(exchange.try_send(make_flight_data(10)).unwrap().is_none());

        // Data should arrive on send_rx
        let received = send_rx.recv().await.unwrap();
        assert_eq!(received.data_body.len(), 10);

        // Simulate pong by clearing in_flight
        exchange.ready_send();

        // Second send should succeed
        assert!(exchange.try_send(make_flight_data(20)).unwrap().is_none());
        let received = send_rx.recv().await.unwrap();
        assert_eq!(received.data_body.len(), 20);
    }

    #[tokio::test]
    async fn test_ping_pong_in_flight_returns_data() {
        let (exchange, send_rx, _pong_tx) = create_mock_exchange(2);

        // First send succeeds
        assert!(exchange.try_send(make_flight_data(1)).unwrap().is_none());

        // Second send returns data back (in-flight)
        let returned = exchange.try_send(make_flight_data(2)).unwrap();
        assert!(returned.is_some());
        assert_eq!(returned.unwrap().data_body.len(), 2);

        // Drain the channel and simulate pong
        let _ = send_rx.recv().await.unwrap();
        exchange.ready_send();

        // Now send should succeed again
        assert!(exchange.try_send(make_flight_data(3)).unwrap().is_none());
    }

    #[tokio::test]
    async fn test_ping_pong_closed_returns_error() {
        let (exchange, send_rx, _pong_tx) = create_mock_exchange(1);

        // Drop the receiver to close the channel
        drop(send_rx);

        let result = exchange.try_send(make_flight_data(1));
        assert!(result.is_err());
    }
}
