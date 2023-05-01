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

use std::collections::HashMap;
use std::future::Future;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Duration;

use common_base::base::select3;
use common_base::base::tokio;
use common_base::base::tokio::sync::Notify;
use common_base::base::tokio::task::JoinHandle;
use common_base::base::Select3Output;
use common_base::runtime::Runtime;
use common_base::runtime::TrySpawn;
use common_exception::ErrorCode;
use common_exception::Result;

use crate::api::rpc::flight_client::FlightExchange;
use crate::api::rpc::flight_client::FlightSender;
use crate::api::DataPacket;
use crate::sessions::QueryContext;

pub struct StatisticsReceiver {
    _runtime: Runtime,
    shutdown_flag: Arc<AtomicBool>,
    shutdown_notify: Arc<Notify>,
    exchange_handler: Vec<JoinHandle<Result<()>>>,
}

impl StatisticsReceiver {
    pub fn spawn_receiver(
        ctx: &Arc<QueryContext>,
        statistics_exchanges: HashMap<String, Vec<FlightExchange>>,
    ) -> Result<StatisticsReceiver> {
        let shutdown_notify = Arc::new(Notify::new());
        let shutdown_flag = Arc::new(AtomicBool::new(false));
        let mut exchange_handler = Vec::with_capacity(statistics_exchanges.len());
        let runtime = Runtime::with_worker_threads(2, Some(String::from("StatisticsReceiver")))?;

        for (_source, mut exchanges) in statistics_exchanges.into_iter() {
            debug_assert_eq!(exchanges.len(), 2);

            let (tx, rx) = match (exchanges.remove(0), exchanges.remove(0)) {
                (tx @ FlightExchange::Sender { .. }, rx @ FlightExchange::Receiver { .. }) => {
                    (tx.convert_to_sender(), rx.convert_to_receiver())
                }
                (rx @ FlightExchange::Receiver { .. }, tx @ FlightExchange::Sender { .. }) => {
                    (tx.convert_to_sender(), rx.convert_to_receiver())
                }
                _ => unreachable!(),
            };

            exchange_handler.push(runtime.spawn({
                let ctx = ctx.clone();
                let shutdown_flag = shutdown_flag.clone();
                let shutdown_notify = shutdown_notify.clone();

                async move {
                    let mut recv = Box::pin(rx.recv());
                    let mut notified = Box::pin(shutdown_notify.notified());

                    'worker_loop: while !shutdown_flag.load(Ordering::Relaxed) {
                        let interval = Box::pin(tokio::time::sleep(Duration::from_millis(500)));

                        match select3(interval, notified, recv).await {
                            Select3Output::Left((_res, middle, right)) => {
                                recv = right;
                                notified = middle;

                                if !shutdown_flag.load(Ordering::Relaxed) {
                                    match Self::fetch(&ctx, &tx, recv).await {
                                        Ok(true) => {
                                            return Ok(());
                                        }
                                        Ok(false) => {
                                            recv = Box::pin(rx.recv());
                                        }
                                        Err(cause) => {
                                            ctx.get_current_session()
                                                .force_kill_query(cause.clone());
                                            return Err(cause);
                                        }
                                    };
                                }
                            }
                            Select3Output::Middle((_, _, right)) => {
                                recv = right;
                                break 'worker_loop;
                            }
                            Select3Output::Right((res, _, middle)) => {
                                notified = middle;
                                match StatisticsReceiver::recv_data(&ctx, res) {
                                    Ok(true) => {
                                        return Ok(());
                                    }
                                    Ok(false) => {
                                        recv = Box::pin(rx.recv());
                                    }
                                    Err(cause) => {
                                        ctx.get_current_session().force_kill_query(cause.clone());
                                        return Err(cause);
                                    }
                                };
                            }
                        }
                    }

                    if let Err(cause) = StatisticsReceiver::fetch(&ctx, &tx, recv).await {
                        ctx.get_current_session().force_kill_query(cause.clone());
                        return Err(cause);
                    }

                    Ok(())
                }
            }));
        }

        Ok(StatisticsReceiver {
            shutdown_flag,
            shutdown_notify,
            exchange_handler,
            _runtime: runtime,
        })
    }

    #[async_backtrace::framed]
    async fn fetch(
        ctx: &Arc<QueryContext>,
        tx: &FlightSender,
        recv: impl Future<Output = Result<Option<DataPacket>>>,
    ) -> Result<bool> {
        if let Err(error) = tx.send(DataPacket::FetchProgressAndPrecommit).await {
            // The query is done(in remote).
            return match error.code() == ErrorCode::ABORTED_QUERY {
                true => Ok(true),
                false => Err(error),
            };
        }

        Self::recv_data(ctx, recv.await)
    }

    fn recv_data(ctx: &Arc<QueryContext>, recv_data: Result<Option<DataPacket>>) -> Result<bool> {
        match recv_data {
            Ok(None) => Ok(true),
            Err(transport_error) => Err(transport_error),
            Ok(Some(DataPacket::ErrorCode(error))) => Err(error),
            Ok(Some(DataPacket::Dictionary(_))) => unreachable!(),
            Ok(Some(DataPacket::FragmentData(_))) => unreachable!(),
            Ok(Some(DataPacket::FetchProgressAndPrecommit)) => unreachable!(),
            Ok(Some(DataPacket::ProgressAndPrecommit {
                progress,
                precommit,
            })) => {
                for progress_info in progress {
                    progress_info.inc(ctx);
                }

                for precommit_block in precommit {
                    precommit_block.precommit(ctx);
                }

                Ok(false)
            }
        }
    }

    pub fn shutdown(&mut self) {
        self.shutdown_flag.store(true, Ordering::Release);
        self.shutdown_notify.notify_waiters();
    }

    pub fn wait_shutdown(&mut self) -> Result<()> {
        let mut exchanges_handler = std::mem::take(&mut self.exchange_handler);
        futures::executor::block_on(async move {
            while let Some(exchange_handler) = exchanges_handler.pop() {
                match exchange_handler.await {
                    Ok(Ok(_)) => Ok(()),
                    Ok(Err(cause)) => Err(cause),
                    Err(join_error) => match join_error.is_cancelled() {
                        true => Err(ErrorCode::TokioError("Tokio error is cancelled.")),
                        false => {
                            let panic_error = join_error.into_panic();
                            match panic_error.downcast_ref::<&'static str>() {
                                None => match panic_error.downcast_ref::<String>() {
                                    None => {
                                        Err(ErrorCode::PanicError("Sorry, unknown panic message"))
                                    }
                                    Some(message) => {
                                        Err(ErrorCode::PanicError(message.to_string()))
                                    }
                                },
                                Some(message) => Err(ErrorCode::PanicError(message.to_string())),
                            }
                        }
                    },
                }?;
            }

            Ok(())
        })
    }
}
