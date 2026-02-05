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
use databend_common_base::runtime::GlobalIORuntime;
use futures::StreamExt;
use futures::future::Either;
use futures::future::select;
use parking_lot::Mutex;
use tokio::task::JoinHandle;
use tonic::Status;
use tonic::Streaming;

use crate::servers::flight::FlightClient;

/// Response from a ping-pong exchange.
pub struct PingPongResponse {
    pub data: Result<FlightData, Status>,
    pub rtt: Duration,
}

/// Callback trait for handling ping-pong responses.
pub trait PingPongCallback: Send + Sync + 'static {
    fn on_response(&self, exchange: &PingPongExchangeInner, response: PingPongResponse);
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
    inner: Arc<PingPongExchangeInner>,
    response_stream: Mutex<Option<Streaming<FlightData>>>,
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

    /// Check if the exchange is ready to send.
    pub fn can_send(&self) -> bool {
        !self.in_flight.load(Ordering::SeqCst)
    }
}

impl PingPongExchange {
    /// Create a new ping-pong exchange connection.
    ///
    /// The exchange is created but the receiver is not started yet.
    /// Call `start` to begin receiving responses.
    pub async fn connect(
        client: &mut FlightClient,
        query_id: &str,
        channel_id: &str,
    ) -> Result<Self, Status> {
        let (send_tx, send_rx) = async_channel::bounded(1);

        let response_stream = client.do_exchange(query_id, channel_id, send_rx).await?;

        let inner = Arc::new(PingPongExchangeInner {
            in_flight: AtomicBool::new(false),
            send_time: Mutex::new(None),
            send_tx,
            shutdown: WatchNotify::new(),
        });

        Ok(Self {
            inner,
            response_stream: Mutex::new(Some(response_stream)),
        })
    }

    /// Start the receiver with the given callback.
    ///
    /// This should be called before sending data. The callback will be invoked
    /// for each response received from the remote end.
    ///
    /// Returns an error if the receiver has already been started.
    pub fn start(&self, callback: Arc<dyn PingPongCallback>) -> Result<JoinHandle<()>, Status> {
        let Some(mut stream) = self.response_stream.lock().take() else {
            return Err(Status::already_exists("Receiver already started"));
        };

        let inner = self.inner.clone();
        Ok(GlobalIORuntime::instance().spawn(async move {
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
                        callback.on_response(&inner, PingPongResponse {
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
                        inner.in_flight.store(false, Ordering::SeqCst);
                        callback.on_response(&inner, PingPongResponse {
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
                        callback.on_response(&inner, PingPongResponse {
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
