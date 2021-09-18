// Copyright 2020 Datafuse Labs.
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
use std::time::Duration;
use std::time::Instant;

use common_clickhouse_srv::types::Block as ClickHouseBlock;
use common_clickhouse_srv::CHContext;
use common_datablocks::DataBlock;
use common_datavalues::DataSchemaRef;
use common_exception::Result;
use common_planners::InsertIntoPlan;
use common_planners::PlanNode;
use common_runtime::tokio;
use common_runtime::tokio::sync::mpsc::channel;
use common_runtime::tokio::time::interval;
use futures::channel::mpsc;
use futures::channel::mpsc::Receiver;
use futures::SinkExt;
use futures::StreamExt;
use metrics::histogram;
use tokio_stream::wrappers::IntervalStream;
use tokio_stream::wrappers::ReceiverStream;

use super::writers::from_clickhouse_block;
use crate::interpreters::InterpreterFactory;
use crate::sessions::DatabendQueryContextRef;
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
        ctx: DatabendQueryContextRef,
    ) -> Result<Receiver<BlockItem>> {
        let query = &ch_ctx.state.query;
        log::debug!("{}", query);

        let plan = PlanParser::create(ctx.clone()).build_from_sql(query)?;

        match plan {
            PlanNode::InsertInto(insert) => Self::process_insert_query(insert, ch_ctx, ctx).await,
            _ => {
                let start = Instant::now();
                let interpreter = InterpreterFactory::get(ctx.clone(), plan)?;
                let name = interpreter.name().to_string();
                let async_data_stream = interpreter.execute();
                let mut data_stream = async_data_stream.await?;
                histogram!(
                    super::clickhouse_metrics::METRIC_INTERPRETER_USEDTIME,
                    start.elapsed(),
                    "interpreter" => name
                );
                let mut interval_stream = IntervalStream::new(interval(Duration::from_millis(30)));
                let cancel = Arc::new(AtomicBool::new(false));

                let (mut tx, rx) = mpsc::channel(20);
                let mut tx2 = tx.clone();
                let cancel_clone = cancel.clone();

                tokio::spawn(async move {
                    while !cancel.load(Ordering::Relaxed) {
                        let _ = interval_stream.next().await;
                        tx.send(BlockItem::ProgressTicker).await.ok();
                    }
                });

                ctx.execute_task(async move {
                    while let Some(block) = data_stream.next().await {
                        tx2.send(BlockItem::Block(block)).await.ok();
                    }

                    cancel_clone.store(true, Ordering::Relaxed);
                })?;

                Ok(rx)
            }
        }
    }

    pub async fn process_insert_query(
        insert: InsertIntoPlan,
        ch_ctx: &mut CHContext,
        ctx: DatabendQueryContextRef,
    ) -> Result<Receiver<BlockItem>> {
        let sample_block = DataBlock::empty_with_schema(insert.schema());
        let (sender, rec) = channel(4);
        ch_ctx.state.out = Some(sender);

        let sc = sample_block.schema().clone();
        let stream = ReceiverStream::new(rec);
        let stream = FromClickHouseBlockStream {
            input: stream,
            schema: sc,
        };
        insert.set_input_stream(Box::pin(stream));
        let interpreter = InterpreterFactory::get(ctx.clone(), PlanNode::InsertInto(insert))?;
        let name = interpreter.name().to_string();

        let (mut tx, rx) = mpsc::channel(20);
        tx.send(BlockItem::InsertSample(sample_block)).await.ok();

        // the data is comming in async mode
        let sent_all_data = ch_ctx.state.sent_all_data.clone();
        let start = Instant::now();
        ctx.execute_task(async move {
            interpreter.execute().await.unwrap();
            sent_all_data.notify_one();
        })?;
        histogram!(
            super::clickhouse_metrics::METRIC_INTERPRETER_USEDTIME,
            start.elapsed(),
            "interpreter" => name
        );
        Ok(rx)
    }
}

pub struct FromClickHouseBlockStream {
    input: ReceiverStream<ClickHouseBlock>,
    schema: DataSchemaRef,
}

impl futures::stream::Stream for FromClickHouseBlockStream {
    type Item = DataBlock;

    fn poll_next(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        self.input.poll_next_unpin(cx).map(|x| match x {
            Some(v) => {
                let block = from_clickhouse_block(self.schema.clone(), v);
                match block {
                    Ok(block) => Some(block),
                    Err(e) => {
                        log::error!(
                            "failed to convert ClickHouseBlock to block , breaking out, {:?}",
                            e
                        );
                        None
                    }
                }
            }
            _ => None,
        })
    }
}
