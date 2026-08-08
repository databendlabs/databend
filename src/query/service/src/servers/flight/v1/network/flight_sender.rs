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
use std::sync::Arc;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;

use arrow_flight::FlightData;
use databend_common_base::runtime::Runtime;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use parking_lot::Mutex;
use tokio::sync::OwnedSemaphorePermit;
use tokio::sync::Semaphore;
use tonic::Status;

use super::PingPongCallback;
use super::PingPongExchange;
use crate::servers::flight::FlightOperation;
use crate::servers::flight::add_flight_error_context;

const PENDING_PACKETS: usize = 8;

struct PendingPacket {
    data: FlightData,
    _permit: OwnedSemaphorePermit,
}

struct SenderState {
    pending: VecDeque<PendingPacket>,
    terminal: Option<Result<()>>,
}

impl SenderState {
    fn finish(&mut self, terminal: Result<()>) {
        if self.terminal.is_none() {
            self.terminal = Some(terminal);
        }
        self.pending.clear();
    }

    fn check_open(&self) -> Result<()> {
        match &self.terminal {
            None => Ok(()),
            Some(Err(cause)) => Err(cause.clone()),
            Some(Ok(())) => Err(ErrorCode::AbortedQuery(
                "Aborted query, because the remote flight channel is closed.",
            )),
        }
    }
}

struct SenderCallback {
    state: Arc<Mutex<SenderState>>,
    // The response task owns this callback. Retaining the exchange here lets a graceful
    // close finish its EOS handshake after the pipeline-side FlightSender is dropped.
    _exchange: Arc<PingPongExchange>,
    local_node_id: String,
    remote_node_id: String,
}

impl PingPongCallback for SenderCallback {
    fn pop_pending(&self) -> Option<FlightData> {
        self.state
            .lock()
            .pending
            .pop_front()
            .map(|packet| packet.data)
    }

    fn on_remote_finished(&self) {
        self.state.lock().finish(Ok(()));
    }

    fn on_error(&self, status: Status) {
        self.state.lock().finish(Err(add_flight_error_context(
            status.into(),
            FlightOperation::DoExchange,
            &self.local_node_id,
            &self.remote_node_id,
        )));
    }
}

pub struct ReconnectableFlightSender {
    exchange: Arc<PingPongExchange>,
    state: Arc<Mutex<SenderState>>,
    pending_slots: Arc<Semaphore>,
    finishing: AtomicBool,
}

impl ReconnectableFlightSender {
    pub fn create(exchange: PingPongExchange, runtime: &Runtime) -> Result<Self> {
        let local_node_id = exchange.local_node_id().to_string();
        let remote_node_id = exchange.remote_node_id().to_string();
        let exchange = Arc::new(exchange);
        let state = Arc::new(Mutex::new(SenderState {
            pending: VecDeque::new(),
            terminal: None,
        }));

        exchange
            .start(
                Arc::new(SenderCallback {
                    state: state.clone(),
                    _exchange: exchange.clone(),
                    local_node_id,
                    remote_node_id,
                }),
                runtime,
            )
            .map_err(ErrorCode::from)?;

        Ok(Self {
            exchange,
            state,
            pending_slots: Arc::new(Semaphore::new(PENDING_PACKETS)),
            finishing: AtomicBool::new(false),
        })
    }

    pub async fn send(&self, data: FlightData) -> Result<()> {
        self.state.lock().check_open()?;

        let data = match self.exchange.try_send(data) {
            Ok(None) => return Ok(()),
            Ok(Some(data)) => data,
            Err(status) => return Err(self.status_to_error(status)),
        };

        let permit = self
            .pending_slots
            .clone()
            .acquire_owned()
            .await
            .map_err(|_| ErrorCode::AbortedQuery("do_exchange sender is closed"))?;
        self.state.lock().check_open()?;

        self.exchange
            .send_or_enqueue(data, |data| {
                let mut state = self.state.lock();
                if state.terminal.is_none() {
                    state.pending.push_back(PendingPacket {
                        data,
                        _permit: permit,
                    });
                }
            })
            .map_err(|status| self.status_to_error(status))
    }

    pub fn finish(&self) {
        if !self.finishing.swap(true, Ordering::AcqRel) {
            self.exchange.request_finish();
        }
    }

    pub fn is_closed(&self) -> bool {
        self.state.lock().terminal.is_some()
    }

    fn status_to_error(&self, status: Status) -> ErrorCode {
        add_flight_error_context(
            status.into(),
            FlightOperation::DoExchange,
            self.exchange.local_node_id(),
            self.exchange.remote_node_id(),
        )
    }
}

impl Drop for ReconnectableFlightSender {
    fn drop(&mut self) {
        if !self.finishing.load(Ordering::Acquire) {
            self.exchange.shutdown.notify_waiters();
        }
    }
}
