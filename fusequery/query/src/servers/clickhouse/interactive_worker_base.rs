// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Duration;

use clickhouse_srv::types::Block as ClickHouseBlock;
use clickhouse_srv::CHContext;
use common_datablocks::DataBlock;
use common_exception::Result;
use common_planners::PlanNode;
use common_runtime::tokio;
use common_runtime::tokio::sync::mpsc::channel;
use common_runtime::tokio::time::interval;
use futures::channel::mpsc;
use futures::channel::mpsc::Receiver;
use futures::SinkExt;
use futures::StreamExt;
use tokio_stream::wrappers::IntervalStream;
use tokio_stream::wrappers::ReceiverStream;

use crate::interpreters::InterpreterFactory;
use crate::sessions::FuseQueryContextRef;
use crate::sql::PlanParser;

pub struct InteractiveWorkerBase;

pub enum BlockItem {
    Block(Result<DataBlock>),
    // for insert prepare, we do not need to send another block again
    InsertSample(DataBlock),
    ProgressTicker,
}

impl InteractiveWorkerBase {
    pub async fn do_query(
        ch_ctx: &mut CHContext,
        ctx: FuseQueryContextRef,
    ) -> Result<Receiver<BlockItem>> {
        let query = &ch_ctx.state.query;
        log::debug!("{}", query);

        let plan = PlanParser::create(ctx.clone()).build_from_sql(query)?;
        if let PlanNode::InsertInto(insert) = &plan {
            let sample_block = DataBlock::empty_with_schema(insert.schema());

            let (sender, rec) = channel(4);
            ch_ctx.state.out = Some(sender);
            tokio::spawn(async move {
                let mut rows = 0;
                let mut stream = ReceiverStream::new(rec);
                while let Some(block) = stream.next().await {
                    rows += block.row_count();
                }
            });

            let (mut tx, rx) = mpsc::channel(3);
            tx.send(BlockItem::InsertSample(sample_block)).await.ok();
            return Ok(rx);
        }

        let interpreter = InterpreterFactory::get(ctx.clone(), plan)?;

        let async_data_stream = interpreter.execute();
        let data_stream = async_data_stream.await?;
        let mut abort_stream = ctx.try_create_abortable(data_stream)?;

        let mut interval_stream = IntervalStream::new(interval(Duration::from_millis(30)));
        let (mut tx, rx) = mpsc::channel(20);
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
        })?;

        Ok(rx)
    }
}
