use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Duration;

use common_datablocks::DataBlock;
use common_exception::Result;
use common_runtime::tokio;
use common_runtime::tokio::time::interval;
use futures::channel::mpsc;
use futures::channel::mpsc::Receiver;
use futures::SinkExt;
use futures::StreamExt;
use tokio_stream::wrappers::IntervalStream;

use crate::interpreters::InterpreterFactory;
use crate::servers::clickhouse::writers::QueryWriter;
use crate::sessions::FuseQueryContextRef;
use crate::sql::PlanParser;

pub struct InteractiveWorkerBase;

pub enum BlockItem {
    Block(Result<DataBlock>),
    ProgressTicker,
}

impl InteractiveWorkerBase {
    pub async fn do_query(query: &str, ctx: FuseQueryContextRef) -> Result<Receiver<BlockItem>> {
        log::debug!("{}", query);

        let plan = PlanParser::create(ctx.clone()).build_from_sql(query)?;
        let interpreter = InterpreterFactory::get(ctx.clone(), plan)?;

        let async_data_stream = interpreter.execute();
        let data_stream = async_data_stream.await?;
        let mut abort_stream = ctx.try_create_abortable(data_stream)?;

        let mut interval_stream = IntervalStream::new(interval(Duration::from_millis(30)));
        let (mut tx, mut rx) = mpsc::channel(20);
        let cancel = Arc::new(AtomicBool::new(false));

        let mut tx2 = tx.clone();
        let cancel_clone = cancel.clone();

        tokio::spawn(async move {
            while !cancel.load(Ordering::Relaxed) {
                let _ = interval_stream.next().await;
                tx.send(BlockItem::ProgressTicker).await.ok();
            }
        });

        ctx.execute_task(async move {
            while let Some(block) = abort_stream.next().await {
                tx2.send(BlockItem::Block(block)).await.ok();
            }

            cancel_clone.store(true, Ordering::Relaxed);
        });

        Ok(rx)
    }
}
