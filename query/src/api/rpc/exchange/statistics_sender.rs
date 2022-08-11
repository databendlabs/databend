use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;
use futures_util::future::Either;
use common_exception::Result;
use crate::api::rpc::flight_client::FlightExchange;
use common_base::base::tokio;
use common_base::base::tokio::sync::Notify;
use crate::sessions::QueryContext;
use futures::future::select;
use common_catalog::table_context::TableContext;
use crate::api::DataPacket;
use crate::api::rpc::packets::{PrecommitBlock, ProgressInfo};

pub struct StatisticsSender {
    query_id: String,
    ctx: Arc<QueryContext>,
    exchange: FlightExchange,
    shutdown_flag: Arc<AtomicBool>,
    shutdown_notify: Arc<Notify>,
}

impl StatisticsSender {
    pub fn create(query_id: &str, ctx: Arc<QueryContext>, exchange: FlightExchange) -> StatisticsSender {
        StatisticsSender {
            ctx,
            exchange,
            query_id: query_id.to_string(),
            shutdown_flag: Arc::new(AtomicBool::new(false)),
            shutdown_notify: Arc::new(Notify::new()),
        }
    }

    pub fn start(&mut self) {
        let ctx = self.ctx.clone();
        let query_id = self.query_id.clone();
        let exchange = self.exchange.clone();
        let shutdown_flag = self.shutdown_flag.clone();
        let shutdown_notify = self.shutdown_notify.clone();

        tokio::spawn(async move {
            let mut notified = Box::pin(shutdown_notify.notified());

            while !shutdown_flag.load(Ordering::Relaxed) {
                let interval = Box::pin(tokio::time::sleep(Duration::from_millis(500)));

                match futures::future::select(interval, notified).await {
                    Either::Left((_res, right)) => {
                        notified = right;

                        if !shutdown_flag.load(Ordering::Relaxed) {
                            if let Err(_cause) = Self::send_statistics(&ctx, &exchange).await {
                                ctx.get_exchange_manager().shutdown_query(&query_id);
                                break;
                            }
                        }
                    }
                    Either::Right((_, _)) => { break; }
                }
            }

            if let Err(_cause) = Self::send_statistics(&ctx, &exchange).await {
                ctx.get_exchange_manager().shutdown_query(&query_id);
                tracing::error!("Statistics send statistics has error, because exchange channel is closed.");
            }
        });
    }

    pub fn shutdown(&mut self) {
        self.shutdown_flag.store(true, Ordering::Release);
        self.shutdown_notify.notify_one();
    }


    async fn send_statistics(ctx: &Arc<QueryContext>, exchange: &FlightExchange) -> Result<()> {
        Self::send_progress(ctx, exchange).await?;
        Self::send_precommit(ctx, exchange).await
    }

    async fn send_progress(ctx: &Arc<QueryContext>, exchange: &FlightExchange) -> Result<()> {
        let scan_progress = ctx.get_scan_progress();
        let scan_progress_values = scan_progress.fetch();

        if scan_progress_values.rows != 0 || scan_progress_values.bytes != 0 {
            let progress = ProgressInfo::ScanProgress(scan_progress_values);
            exchange.send(DataPacket::Progress(progress)).await?;
        }

        let write_progress = ctx.get_write_progress();
        let write_progress_values = write_progress.fetch();

        if write_progress_values.rows != 0 || write_progress_values.bytes != 0 {
            let progress = ProgressInfo::WriteProgress(write_progress_values);
            exchange.send(DataPacket::Progress(progress)).await?;
        }

        let result_progress = ctx.get_result_progress();
        let result_progress_values = result_progress.fetch();

        if result_progress_values.rows != 0 || result_progress_values.bytes != 0 {
            let progress = ProgressInfo::ResultProgress(result_progress_values);
            exchange.send(DataPacket::Progress(progress)).await?
        }

        Ok(())
    }

    async fn send_precommit(ctx: &Arc<QueryContext>, exchange: &FlightExchange) -> Result<()> {
        let precommit_blocks = ctx.consume_precommit_blocks();

        if !precommit_blocks.is_empty() {
            for precommit_block in precommit_blocks {
                if !precommit_block.is_empty() {
                    let precommit_block = PrecommitBlock(precommit_block);
                    exchange.send(DataPacket::PrecommitBlock(precommit_block)).await?;
                }
            }
        }

        Ok(())
    }
}
