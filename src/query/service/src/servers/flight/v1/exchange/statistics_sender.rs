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

use std::sync::Arc;
use std::time::Duration;

use databend_common_base::base::tokio::time::sleep;
use databend_common_base::runtime::defer;
use databend_common_base::runtime::GlobalIORuntime;
use databend_common_base::runtime::TrySpawn;
use databend_common_catalog::table_context::TableContext;
use databend_common_exception::Result;
use databend_common_storage::MutationStatus;
use futures_util::future::Either;
use log::warn;
use parking_lot::Mutex;

use crate::pipelines::executor::PipelineCompleteExecutor;
use crate::pipelines::executor::QueryHandle;
use crate::servers::flight::v1::exchange::DataExchangeManager;
use crate::servers::flight::v1::packets::DataPacket;
use crate::servers::flight::v1::packets::ProgressInfo;
use crate::servers::flight::FlightExchange;
use crate::servers::flight::FlightSender;
use crate::sessions::QueryContext;

pub struct DistributedQueryDaemon {
    query_id: String,
    ctx: Arc<QueryContext>,
}

impl DistributedQueryDaemon {
    pub fn create(query_id: &str, ctx: Arc<QueryContext>) -> Self {
        DistributedQueryDaemon {
            ctx,
            query_id: query_id.to_string(),
        }
    }

    pub fn run(self, exchange: FlightExchange, executor: Arc<PipelineCompleteExecutor>) {
        GlobalIORuntime::instance().spawn(async move {
            let ctx = self.ctx.clone();
            let query_id = self.query_id.clone();
            let tx = exchange.convert_to_sender();

            let statistics_handle = self.ctx.spawn(async move {
                let shutdown_cause = Arc::new(Mutex::new(None));
                let _shutdown_guard = defer({
                    let query_id = query_id.clone();
                    let shutdown_cause = shutdown_cause.clone();

                    move || {
                        let exchange_manager = DataExchangeManager::instance();

                        let shutdown_cause = shutdown_cause.lock().take();
                        exchange_manager.on_finished_query(&query_id, shutdown_cause);
                    }
                });

                if let Err(cause) = executor.execute().await {
                    *shutdown_cause.lock() = Some(cause.clone());

                    let data = DataPacket::ErrorCode(cause);
                    if let Err(error_code) = tx.send(data).await {
                        warn!(
                            "Cannot send data via flight exchange, cause: {:?}",
                            error_code
                        );
                    }

                    return;
                }

                let query_handle = executor.get_handle();

                let mut cnt = 0;
                let mut wait_shutdown = Box::pin(query_handle.wait());
                let mut sleep_future = Box::pin(sleep(Duration::from_millis(100)));

                loop {
                    match futures::future::select(sleep_future, wait_shutdown).await {
                        Either::Right((Ok(_), _)) => {
                            // query completed.
                            break;
                        }
                        Either::Right((Err(cause), _)) => {
                            *shutdown_cause.lock() = Some(cause.clone());
                            let data = DataPacket::ErrorCode(cause);
                            if let Err(error_code) = tx.send(data).await {
                                warn!(
                                    "Cannot send data via flight exchange, cause: {:?}",
                                    error_code
                                );
                            }

                            return;
                        }
                        Either::Left((_, right)) => {
                            wait_shutdown = right;
                            sleep_future = Box::pin(sleep(Duration::from_millis(100)));

                            if let Err(cause) = Self::send_progress(&ctx, &tx).await {
                                ctx.get_exchange_manager()
                                    .shutdown_query(&query_id, Some(cause));
                                return;
                            }

                            cnt += 1;

                            if cnt % 5 == 0 {
                                // send profiles per 500 millis
                                if let Err(error) =
                                    Self::send_profile(&query_handle, &tx, false).await
                                {
                                    warn!("Profiles send has error, cause: {:?}.", error);
                                }
                            }
                        }
                    }
                }

                if let Err(error) = Self::send_profile(&query_handle, &tx, true).await {
                    warn!("Profiles send has error, cause: {:?}.", error);
                }

                if let Err(error) = Self::send_copy_status(&ctx, &tx).await {
                    warn!("CopyStatus send has error, cause: {:?}.", error);
                }

                if let Err(error) = Self::send_mutation_status(&ctx, &tx).await {
                    warn!("MutationStatus send has error, cause: {:?}.", error);
                }

                if let Err(error) = Self::send_progress(&ctx, &tx).await {
                    warn!("Statistics send has error, cause: {:?}.", error);
                }
            });

            if let Err(cause) = statistics_handle.await {
                warn!(
                    "Distributed query statistics handle abort. cause: {:?}",
                    cause
                );
                let exchange_manager = DataExchangeManager::instance();
                exchange_manager.on_finished_query(&self.query_id, Some(cause));
            }
        });
    }

    #[async_backtrace::framed]
    async fn send_progress(ctx: &Arc<QueryContext>, tx: &FlightSender) -> Result<()> {
        let progress = Self::fetch_progress(ctx);
        let data_packet = DataPacket::SerializeProgress(progress);
        tx.send(data_packet).await
    }

    #[async_backtrace::framed]
    async fn send_copy_status(ctx: &Arc<QueryContext>, flight_sender: &FlightSender) -> Result<()> {
        let copy_status = ctx.get_copy_status();
        if !copy_status.files.is_empty() {
            let data_packet = DataPacket::CopyStatus(copy_status.as_ref().to_owned());
            flight_sender.send(data_packet).await?;
        }
        Ok(())
    }

    #[async_backtrace::framed]
    async fn send_mutation_status(
        ctx: &Arc<QueryContext>,
        flight_sender: &FlightSender,
    ) -> Result<()> {
        let mutation_status = {
            let binding = ctx.get_mutation_status();
            let status = binding.read();
            MutationStatus {
                insert_rows: status.insert_rows,
                deleted_rows: status.deleted_rows,
                update_rows: status.update_rows,
            }
        };
        let data_packet = DataPacket::MutationStatus(mutation_status);
        flight_sender.send(data_packet).await?;
        Ok(())
    }

    async fn send_profile(
        query_handle: &Arc<dyn QueryHandle>,
        tx: &FlightSender,
        collect_metrics: bool,
    ) -> Result<()> {
        let plans_profile = query_handle.fetch_profiling(collect_metrics);

        if !plans_profile.is_empty() {
            let data_packet = DataPacket::QueryProfiles(plans_profile);
            tx.send(data_packet).await?;
        }

        Ok(())
    }

    #[async_backtrace::framed]
    async fn send_scan_cache_metrics(
        ctx: &Arc<QueryContext>,
        flight_sender: &FlightSender,
    ) -> Result<()> {
        let data_cache_metrics = ctx.get_data_cache_metrics();
        let data_packet = DataPacket::DataCacheMetrics(data_cache_metrics.as_values());
        flight_sender.send(data_packet).await
    }

    fn fetch_progress(ctx: &Arc<QueryContext>) -> Vec<ProgressInfo> {
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

        let stats = ctx.get_spill_file_stats(None);
        if stats.file_nums != 0 {
            progress_info.push(ProgressInfo::SpillTotalStats(stats))
        }
        progress_info
    }
}
