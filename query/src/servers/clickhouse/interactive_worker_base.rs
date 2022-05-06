// Copyright 2021 Datafuse Labs.
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

use common_base::tokio;
use common_base::tokio::sync::mpsc::channel;
use common_base::tokio::time::interval;
use common_base::ProgressValues;
use common_base::TrySpawn;
use common_datablocks::DataBlock;
use common_datavalues::DataSchemaRef;
use common_exception::ErrorCode;
use common_exception::Result;
use common_exception::ToErrorCode;
use common_planners::InsertPlan;
use common_planners::PlanNode;
use common_tracing::tracing;
use futures::channel::mpsc;
use futures::channel::mpsc::Receiver;
use futures::SinkExt;
use futures::StreamExt;
use metrics::histogram;
use opensrv_clickhouse::types::Block as ClickHouseBlock;
use opensrv_clickhouse::CHContext;
use tokio_stream::wrappers::IntervalStream;
use tokio_stream::wrappers::ReceiverStream;

use super::writers::from_clickhouse_block;
use crate::interpreters::InterpreterFactory;
use crate::interpreters::InterpreterQueryLog;
use crate::pipelines::new::processors::port::OutputPort;
use crate::pipelines::new::processors::SyncReceiverCkSource;
use crate::pipelines::new::SourcePipeBuilder;
use crate::sessions::QueryContext;
use crate::sessions::SessionRef;
use crate::sql::PlanParser;

pub struct InteractiveWorkerBase;

pub enum BlockItem {
    Block(Result<DataBlock>),
    // for insert prepare, we do not need to send another block again
    InsertSample(DataBlock),
    ProgressTicker(ProgressValues),
}

impl InteractiveWorkerBase {
    pub async fn do_query(
        ch_ctx: &mut CHContext,
        session: SessionRef,
    ) -> Result<Receiver<BlockItem>> {
        let query = &ch_ctx.state.query;
        tracing::debug!("{}", query);

        let ctx = session.create_query_context().await?;
        ctx.attach_query_str(query);

        let plan = PlanParser::parse(ctx.clone(), query).await;

        let plan = match plan {
            Ok(p) => p,
            Err(e) => {
                InterpreterQueryLog::fail_to_start(ctx, e.clone()).await;
                return Err(e);
            }
        };

        match plan {
            PlanNode::Insert(ref insert) => {
                // It has select plan, so we do not need to consume data from client
                // data is from server and insert into server, just behave like select query
                if insert.has_select_plan() {
                    return Self::process_select_query(plan, ctx).await;
                }

                Self::process_insert_query(insert.clone(), ch_ctx, ctx).await
            }
            _ => Self::process_select_query(plan, ctx).await,
        }
    }

    pub async fn process_insert_query(
        insert: InsertPlan,
        ch_ctx: &mut CHContext,
        ctx: Arc<QueryContext>,
    ) -> Result<Receiver<BlockItem>> {
        let sample_block = DataBlock::empty_with_schema(insert.schema());
        let (sender, rec) = channel(4);
        ch_ctx.state.out = Some(sender);

        let sc = sample_block.schema().clone();
        let stream = ReceiverStream::new(rec);
        let ck_stream = FromClickHouseBlockStream {
            input: stream,
            schema: sc.clone(),
        };

        let interpreter = InterpreterFactory::get(ctx.clone(), PlanNode::Insert(insert))?;
        let name = interpreter.name().to_string();

        if ctx.get_settings().get_enable_new_processor_framework()? != 0
            && ctx.get_cluster().is_empty()
        {
            let output_port = OutputPort::create();
            let sync_receiver_ck_source = SyncReceiverCkSource::create(
                ctx.clone(),
                ck_stream.input.into_inner(),
                output_port.clone(),
                sc,
            )?;
            let mut source_pipe_builder = SourcePipeBuilder::create();
            source_pipe_builder.add_source(output_port, sync_receiver_ck_source);

            let _ = interpreter
                .set_source_pipe_builder(Option::from(source_pipe_builder))
                .map_err(|e| tracing::error!("interpreter.set_source_pipe_builder.error: {:?}", e));

            let (mut tx, rx) = mpsc::channel(20);
            tx.send(BlockItem::InsertSample(sample_block)).await.ok();

            // the data is coming in async mode
            let sent_all_data = ch_ctx.state.sent_all_data.clone();
            let start = Instant::now();
            ctx.try_spawn(async move {
                interpreter.execute(None).await.unwrap();
                sent_all_data.notify_one();
            })?;
            histogram!(
                super::clickhouse_metrics::METRIC_INTERPRETER_USEDTIME,
                start.elapsed(),
                "interpreter" => name
            );
            return Ok(rx);
        }

        let (mut tx, rx) = mpsc::channel(20);
        tx.send(BlockItem::InsertSample(sample_block)).await.ok();

        // the data is coming in async mode
        let sent_all_data = ch_ctx.state.sent_all_data.clone();
        let start = Instant::now();
        ctx.try_spawn(async move {
            interpreter
                .execute(Some(Box::pin(ck_stream)))
                .await
                .unwrap();
            sent_all_data.notify_one();
        })?;
        histogram!(
            super::clickhouse_metrics::METRIC_INTERPRETER_USEDTIME,
            start.elapsed(),
            "interpreter" => name
        );
        Ok(rx)
    }

    pub async fn process_select_query(
        plan: PlanNode,
        ctx: Arc<QueryContext>,
    ) -> Result<Receiver<BlockItem>> {
        let start = Instant::now();
        let interpreter = InterpreterFactory::get(ctx.clone(), plan)?;
        let name = interpreter.name().to_string();
        histogram!(
            super::clickhouse_metrics::METRIC_INTERPRETER_USEDTIME,
            start.elapsed(),
            "interpreter" => name
        );

        let cancel = Arc::new(AtomicBool::new(false));
        let cancel_clone = cancel.clone();

        let (tx, rx) = mpsc::channel(20);
        let mut data_tx = tx.clone();
        let mut progress_tx = tx;

        let progress_ctx = ctx.clone();
        let mut interval_stream = IntervalStream::new(interval(Duration::from_millis(30)));
        tokio::spawn(async move {
            let mut prev_progress_values = ProgressValues::default();
            while !cancel.load(Ordering::Relaxed) {
                let _ = interval_stream.next().await;
                let cur_progress_values = progress_ctx.get_scan_progress_value();
                let diff_progress_values = ProgressValues {
                    rows: cur_progress_values.rows - prev_progress_values.rows,
                    bytes: cur_progress_values.bytes - prev_progress_values.bytes,
                };
                prev_progress_values = cur_progress_values;
                progress_tx
                    .send(BlockItem::ProgressTicker(diff_progress_values))
                    .await
                    .ok();
            }
        });

        let query_result = ctx.try_spawn(async move {
            // Query log start.
            let _ = interpreter
                .start()
                .await
                .map_err(|e| tracing::error!("interpreter.start.error: {:?}", e));

            // Execute and read stream data.
            match interpreter.execute(None).await {
                Err(e) => {
                    cancel_clone.store(true, Ordering::Relaxed);
                    Err(e)
                }
                Ok(mut data_stream) => {
                    while let Some(block) = data_stream.next().await {
                        data_tx.send(BlockItem::Block(block)).await.ok();
                    }
                    let _ = interpreter
                        .finish()
                        .await
                        .map_err(|e| tracing::error!("interpreter.finish.error: {:?}", e));
                    cancel_clone.store(true, Ordering::Relaxed);
                    Ok::<(), ErrorCode>(())
                }
            }
        })?;
        let query_result = query_result
            .await
            .map_err_to_code(ErrorCode::TokioError, || {
                "Cannot join handle from context's runtime"
            })?;
        query_result.map(|_| rx)
    }
}

pub struct FromClickHouseBlockStream {
    input: ReceiverStream<ClickHouseBlock>,
    schema: DataSchemaRef,
}

impl futures::stream::Stream for FromClickHouseBlockStream {
    type Item = Result<DataBlock>;

    fn poll_next(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        self.input.poll_next_unpin(cx).map(|x| match x {
            Some(v) => Some(from_clickhouse_block(self.schema.clone(), v)),
            _ => None,
        })
    }
}
