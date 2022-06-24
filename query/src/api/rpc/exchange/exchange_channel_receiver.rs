// Copyright 2022 Datafuse Labs.
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

use std::collections::HashMap;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;
use std::sync::atomic::Ordering::Acquire;
use std::sync::Arc;

use async_channel::Receiver;
use common_base::base::tokio::sync::Notify;
use common_base::base::tokio::task::JoinHandle;
use common_base::base::Runtime;
use common_base::base::TrySpawn;
use common_base::infallible::Mutex;
use common_exception::ErrorCode;
use common_exception::Result;
use futures::future::Either;

use crate::api::rpc::exchange::exchange_channel::FragmentReceiver;
use crate::api::rpc::packet::DataPacket;
use crate::sessions::QueryContext;

pub struct ExchangeReceiver {
    query_id: String,
    ctx: Arc<QueryContext>,
    runtime: Arc<Runtime>,
    rx: Mutex<Option<Receiver<Result<DataPacket>>>>,
    fragments_receiver: Mutex<Option<Vec<Option<FragmentReceiver>>>>,

    finished: Arc<AtomicBool>,
    shutdown_notify: Arc<Notify>,
    worker_handler: Mutex<Option<JoinHandle<()>>>,
}

impl ExchangeReceiver {
    pub fn create(
        ctx: Arc<QueryContext>,
        query_id: String,
        runtime: Arc<Runtime>,
        rx: Receiver<Result<DataPacket>>,
        receivers_map: &HashMap<usize, FragmentReceiver>,
    ) -> Arc<ExchangeReceiver> {
        let max_fragments_id = receivers_map.keys().max().cloned().unwrap_or(0);
        let mut receivers_array = vec![None; max_fragments_id + 1];

        for (fragment_id, receiver) in receivers_map {
            receivers_array[*fragment_id] = Some(receiver.clone());
        }

        Arc::new(ExchangeReceiver {
            ctx,
            runtime,
            query_id,
            rx: Mutex::new(Some(rx)),
            worker_handler: Mutex::new(None),
            shutdown_notify: Arc::new(Notify::new()),
            finished: Arc::new(AtomicBool::new(false)),
            fragments_receiver: Mutex::new(Some(receivers_array)),
        })
    }

    pub fn listen(self: &Arc<Self>) -> Result<()> {
        let this = self.clone();
        let rx = self.get_rx()?;
        let shutdown_notify = self.shutdown_notify.clone();
        let mut fragments_receiver = self.get_fragments_receiver()?;

        let worker_handler = self.runtime.spawn(async move {
            let mut notified = Box::pin(shutdown_notify.notified());

            while !this.finished.load(Ordering::Relaxed) {
                match futures::future::select(rx.recv(), notified).await {
                    Either::Left((recv_data, b)) => {
                        notified = b;

                        if let Ok(recv_data) = recv_data {
                            let txs = &mut fragments_receiver;
                            if let Err(cause) = this.on_packet(recv_data, txs).await {
                                if Self::send_error(&mut fragments_receiver, &cause).await {
                                    Self::shutdown_query(&this, cause);
                                }
                                break;
                            }
                        } else {
                            // This ok. we will close the channel when the data transmission is completed.
                            break;
                        }
                    }
                    Either::Right((_notified, _recv)) => {
                        break;
                    }
                };
            }
        });

        let mut handler = self.worker_handler.lock();
        *handler = Some(worker_handler);

        Ok(())
    }

    async fn send_error(receivers: &mut [Option<FragmentReceiver>], cause: &ErrorCode) -> bool {
        let mut need_shutdown = true;
        for (fragment_id, fragment_receiver) in receivers.iter_mut().enumerate() {
            if let Some(fragment_receiver) = fragment_receiver {
                let error_code = cause.clone();
                need_shutdown = false;
                if let Err(_cause) = fragment_receiver.send(Err(error_code)).await {
                    common_tracing::tracing::warn!(
                        "Fragment {} flight channel is closed.",
                        fragment_id
                    );
                }
            }

            // Stop all fragments if it's can be stopped
            if let Some(tx) = fragment_receiver.take() {
                tx.close();
            }
        }

        need_shutdown
    }

    fn shutdown_query(this: &Arc<ExchangeReceiver>, cause: ErrorCode) {
        let query_id = &this.query_id;
        let exchange_manager = this.ctx.get_exchange_manager();
        if let Err(cause) = exchange_manager.shutdown_query(query_id, Some(cause)) {
            common_tracing::tracing::warn!("Cannot shutdown query, cause {:?}", cause);
        }
    }

    pub fn shutdown(self: &Arc<Self>) {
        self.finished.store(true, Ordering::SeqCst);
        self.shutdown_notify.notify_one();
    }

    pub async fn join(self: &Arc<Self>) -> Result<()> {
        match self.get_worker_handler()?.await {
            Ok(_res) => Ok(()),
            Err(maybe_panic_error) => match maybe_panic_error.is_panic() {
                false => Err(ErrorCode::TokioError("Tokio task has canceled.")),
                true => {
                    let panic_error = maybe_panic_error.into_panic();
                    match panic_error.downcast_ref::<&'static str>() {
                        None => match panic_error.downcast_ref::<String>() {
                            None => Err(ErrorCode::PanicError("Sorry, unknown panic message")),
                            Some(message) => Err(ErrorCode::PanicError(message.to_string())),
                        },
                        Some(message) => Err(ErrorCode::PanicError(message.to_string())),
                    }
                }
            },
        }
    }

    fn get_worker_handler(self: &Arc<Self>) -> Result<JoinHandle<()>> {
        match self.worker_handler.lock().take() {
            Some(handler) => Ok(handler),
            None => Err(ErrorCode::LogicalError(
                "Cannot get worker handler. It's a bug.",
            )),
        }
    }

    fn get_rx(self: &Arc<Self>) -> Result<Receiver<Result<DataPacket>>> {
        match self.rx.lock().take() {
            Some(rx) => Ok(rx),
            None => Err(ErrorCode::LogicalError(
                "Cannot get receiver rx. It's a bug.",
            )),
        }
    }

    fn get_fragments_receiver(self: &Arc<Self>) -> Result<Vec<Option<FragmentReceiver>>> {
        let mut fragments_receiver = self.fragments_receiver.lock();

        match fragments_receiver.take() {
            Some(fragments_receiver) => Ok(fragments_receiver),
            None => Err(ErrorCode::LogicalError(
                "Cannot get fragments receiver. It's a bug.",
            )),
        }
    }

    async fn on_packet(
        self: &Arc<Self>,
        packet: Result<DataPacket>,
        fragments_receiver: &mut [Option<FragmentReceiver>],
    ) -> Result<()> {
        match packet? {
            DataPacket::Data(fragment_id, flight_data) => {
                if let Some(tx) = &fragments_receiver[fragment_id] {
                    if let Err(_cause) = tx.send(Ok(flight_data)).await {
                        common_tracing::tracing::warn!(
                            "Fragment {} flight channel is closed.",
                            fragment_id
                        );
                        if let Some(tx) = fragments_receiver[fragment_id].take() {
                            drop(tx);
                            std::sync::atomic::fence(Acquire);
                        }
                    }
                }

                Ok(())
            }
            DataPacket::EndFragment(fragment_id) => {
                if fragment_id < fragments_receiver.len() {
                    if let Some(tx) = fragments_receiver[fragment_id].take() {
                        drop(tx);
                        std::sync::atomic::fence(Acquire);
                    }
                }

                Ok(())
            }
            DataPacket::ErrorCode(error_code) => Err(error_code),
            DataPacket::Progress(ref values) => {
                self.ctx.get_scan_progress().incr(values);
                Ok(())
            }
        }
    }
}
