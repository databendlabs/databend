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

use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;
use std::sync::Arc;

use common_base::base::tokio;
use common_base::base::tokio::sync::Notify;
use common_base::base::tokio::task::JoinHandle;
use common_exception::ErrorCode;
use common_exception::Result;
use futures_util::future::Either;
use parking_lot::Mutex;

use crate::api::rpc::flight_client::FlightExchange;
use crate::api::rpc::packets::PrecommitBlock;
use crate::api::rpc::packets::ProgressInfo;
use crate::api::DataPacket;
use crate::api::FragmentData;
use crate::sessions::QueryContext;

pub struct StatisticsReceiver {
    ctx: Arc<QueryContext>,
    exchanges: Vec<FlightExchange>,
    shutdown_flag: Arc<AtomicBool>,
    shutdown_notify: Arc<Notify>,
    exchange_handler: Vec<JoinHandle<()>>,
    recv_error: Arc<Mutex<Option<ErrorCode>>>,
}

impl StatisticsReceiver {
    pub fn create(ctx: Arc<QueryContext>, exchanges: Vec<FlightExchange>) -> StatisticsReceiver {
        StatisticsReceiver {
            ctx,
            exchanges,
            shutdown_flag: Arc::new(AtomicBool::new(false)),
            shutdown_notify: Arc::new(Notify::new()),
            exchange_handler: vec![],
            recv_error: Arc::new(Mutex::new(None)),
        }
    }

    pub fn start(&mut self) {
        for flight_exchange in &self.exchanges {
            let ctx = self.ctx.clone();
            let shutdown_flag = self.shutdown_flag.clone();
            let shutdown_notify = self.shutdown_notify.clone();
            let recv_error = self.recv_error.clone();
            let flight_exchange = flight_exchange.clone();

            self.exchange_handler.push(tokio::spawn(async move {
                let mut recv = Box::pin(flight_exchange.recv());
                let mut notified = Box::pin(shutdown_notify.notified());

                'worker_loop: while !shutdown_flag.load(Ordering::Relaxed) {
                    match futures::future::select(recv, notified).await {
                        Either::Left((res, right)) => {
                            notified = right;
                            recv = Box::pin(flight_exchange.recv());

                            if Self::on_data_packet(&ctx, &recv_error, res) {
                                break 'worker_loop;
                            }
                        }
                        Either::Right((_, _)) => {
                            break 'worker_loop;
                        }
                    }
                }
            }));
        }
    }

    fn on_data_packet(
        ctx: &Arc<QueryContext>,
        recv_error: &Mutex<Option<ErrorCode>>,
        res: Result<Option<DataPacket>>,
    ) -> bool {
        match res {
            Ok(None) => true,
            Err(error) => Self::on_recv_error(ctx, recv_error, error),
            Ok(Some(DataPacket::ErrorCode(v))) => Self::on_recv_error(ctx, recv_error, v),
            Ok(Some(DataPacket::Progress(v))) => Self::on_recv_progress(ctx, v),
            Ok(Some(DataPacket::FragmentData(v))) => Self::on_recv_data(v),
            Ok(Some(DataPacket::PrecommitBlock(v))) => Self::on_recv_precommit(ctx, v),
        }
    }

    pub fn shutdown(&mut self) {
        self.shutdown_flag.store(true, Ordering::Release);
        self.shutdown_notify.notify_waiters();
    }

    pub fn wait_shutdown(&mut self) -> Result<()> {
        let exchanges_handler = std::mem::take(&mut self.exchange_handler);
        futures::executor::block_on(async move {
            futures::future::join_all(exchanges_handler.into_iter()).await;
        });

        self.take_receive_error()
    }

    pub fn take_receive_error(&mut self) -> Result<()> {
        match self.recv_error.lock().take() {
            None => Ok(()),
            Some(error) => Err(error),
        }
    }

    fn on_recv_progress(ctx: &Arc<QueryContext>, progress: ProgressInfo) -> bool {
        progress.inc(ctx);
        false
    }

    fn on_recv_data(_fragment_data: FragmentData) -> bool {
        unimplemented!()
    }

    fn on_recv_precommit(ctx: &Arc<QueryContext>, fragment_data: PrecommitBlock) -> bool {
        fragment_data.precommit(ctx);
        false
    }

    fn on_recv_error(
        ctx: &Arc<QueryContext>,
        recv_error: &Mutex<Option<ErrorCode>>,
        cause: ErrorCode,
    ) -> bool {
        // abort query for current session.
        ctx.get_current_session().force_kill_query();
        let mut recv_error = recv_error.lock();
        *recv_error = Some(cause);
        true
    }
}
