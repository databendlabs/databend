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

use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;
use std::sync::Arc;

use async_channel::Sender;
use common_base::base::tokio::task::JoinHandle;
use common_base::runtime::TrySpawn;
use common_catalog::table_context::TableContext;
use common_exception::ErrorCode;
use common_exception::Result;
use futures_util::future::Either;

use crate::api::rpc::flight_client::FlightExchange;
use crate::api::rpc::flight_client::FlightSender;
use crate::api::rpc::packets::PrecommitBlock;
use crate::api::rpc::packets::ProgressInfo;
use crate::api::DataPacket;
use crate::sessions::QueryContext;

pub struct StatisticsSender {
    _spawner: Arc<QueryContext>,
    shutdown_flag: Arc<AtomicBool>,
    shutdown_flag_sender: Sender<Option<ErrorCode>>,
    join_handle: Option<JoinHandle<()>>,
}

impl StatisticsSender {
    pub fn spawn_sender(
        query_id: &str,
        ctx: Arc<QueryContext>,
        mut exchanges: Vec<FlightExchange>,
    ) -> StatisticsSender {
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

        let spawner = ctx.clone();
        let shutdown_flag = Arc::new(AtomicBool::new(false));
        let (shutdown_flag_sender, shutdown_flag_receiver) = async_channel::bounded(1);

        let handle = spawner.spawn({
            let query_id = query_id.to_string();
            let shutdown_flag = shutdown_flag.clone();

            async move {
                let mut recv = Box::pin(rx.recv());
                let mut notified = Box::pin(shutdown_flag_receiver.recv());

                while !shutdown_flag.load(Ordering::Relaxed) {
                    match futures::future::select(recv, notified).await {
                        Either::Right((Ok(None), _))
                        | Either::Right((Err(_), _))
                        | Either::Left((Ok(None), _))
                        | Either::Left((Err(_), _)) => {
                            break;
                        }
                        Either::Right((Ok(Some(error_code)), _recv)) => {
                            let data = DataPacket::ErrorCode(error_code);
                            if let Err(error_code) = tx.send(data).await {
                                tracing::warn!(
                                    "Cannot send data via flight exchange, cause: {:?}",
                                    error_code
                                );
                            }

                            return;
                        }
                        Either::Left((Ok(Some(command)), right)) => {
                            notified = right;
                            recv = Box::pin(rx.recv());

                            if let Err(_cause) =
                                StatisticsSender::on_command(&ctx, command, &tx).await
                            {
                                ctx.get_exchange_manager().shutdown_query(&query_id);
                                return;
                            }
                        }
                    }
                }

                if let Ok(Some(command)) = rx.recv().await {
                    if let Err(error) = Self::on_command(&ctx, command, &tx).await {
                        tracing::warn!("Statistics send has error, cause: {:?}.", error);
                    }
                }
            }
        });

        StatisticsSender {
            _spawner: spawner,
            shutdown_flag,
            shutdown_flag_sender,
            join_handle: Some(handle),
        }
    }

    pub fn shutdown(&mut self, error: Option<ErrorCode>) {
        self.shutdown_flag.store(true, Ordering::Release);
        let shutdown_flag_sender = self.shutdown_flag_sender.clone();

        let join_handle = self.join_handle.take();
        futures::executor::block_on(async move {
            if let Err(error_code) = shutdown_flag_sender.send(error).await {
                tracing::warn!(
                    "Cannot send data via flight exchange, cause: {:?}",
                    error_code
                );
            }

            shutdown_flag_sender.close();

            if let Some(join_handle) = join_handle {
                let _ = join_handle.await;
            }
        });
    }

    #[async_backtrace::framed]
    async fn on_command(
        ctx: &Arc<QueryContext>,
        command: DataPacket,
        flight_sender: &FlightSender,
    ) -> Result<()> {
        match command {
            DataPacket::ErrorCode(_) => unreachable!(),
            DataPacket::Dictionary(_) => unreachable!(),
            DataPacket::FragmentData(_) => unreachable!(),
            DataPacket::ProgressAndPrecommit { .. } => unreachable!(),
            DataPacket::FetchProgressAndPrecommit => {
                flight_sender
                    .send(DataPacket::ProgressAndPrecommit {
                        progress: Self::fetch_progress(ctx).await?,
                        precommit: Self::fetch_precommit(ctx).await?,
                    })
                    .await
            }
        }
    }

    #[async_backtrace::framed]
    async fn fetch_progress(ctx: &Arc<QueryContext>) -> Result<Vec<ProgressInfo>> {
        let mut progress_info = vec![];

        let scan_progress = ctx.get_scan_progress();
        let scan_progress_values = scan_progress.fetch();

        if scan_progress_values.rows != 0 || scan_progress_values.bytes != 0 {
            progress_info.push(ProgressInfo::ScanProgress(scan_progress_values));
        }

        let write_progress = ctx.get_write_progress();
        let write_progress_values = write_progress.fetch();

        if write_progress_values.rows != 0 || write_progress_values.bytes != 0 {
            progress_info.push(ProgressInfo::WriteProgress(write_progress_values));
        }

        let result_progress = ctx.get_result_progress();
        let result_progress_values = result_progress.fetch();

        if result_progress_values.rows != 0 || result_progress_values.bytes != 0 {
            progress_info.push(ProgressInfo::ResultProgress(result_progress_values));
        }

        Ok(progress_info)
    }

    #[async_backtrace::framed]
    async fn fetch_precommit(ctx: &Arc<QueryContext>) -> Result<Vec<PrecommitBlock>> {
        Ok(ctx
            .consume_precommit_blocks()
            .into_iter()
            .map(PrecommitBlock)
            .collect())
    }
}
