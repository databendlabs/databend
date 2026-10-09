// Copyright 2023 Datafuse Labs.
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

use std::any::Any;
use std::sync::Arc;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;
use std::time::Duration;

use databend_common_base::runtime::spawn_blocking;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::DataBlock;
use databend_common_pipeline::core::Event;
use databend_common_pipeline::core::ExecutionInfo;
use databend_common_pipeline::core::InputPort;
use databend_common_pipeline::core::OutputPort;
use databend_common_pipeline::core::Pipe;
use databend_common_pipeline::core::PipeItem;
use databend_common_pipeline::core::Pipeline;
use databend_common_pipeline::core::Processor;
use databend_common_pipeline::core::ProcessorPtr;
use databend_common_pipeline::sinks::SyncSenderSink;
use databend_common_pipeline::sources::SyncReceiverSource;
use databend_query::pipelines::executor::ExecutorSettings;
use databend_query::pipelines::executor::QueryPipelineExecutor;
use databend_query::sessions::QueryContext;
use databend_query::sessions::TableContextProgress;
use databend_query::test_kits::TestFixture;
use tokio::sync::Notify;
use tokio::sync::mpsc::Receiver;
use tokio::sync::mpsc::Sender;
use tokio::sync::mpsc::channel;

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_always_call_on_finished() -> anyhow::Result<()> {
    let fixture = TestFixture::setup().await?;

    let settings = ExecutorSettings {
        query_id: Arc::new("".to_string()),
        max_execute_time_in_seconds: Default::default(),
        enable_queries_executor: false,
        max_threads: 8,
        executor_node_id: "".to_string(),
        perf_event_groups: vec![],
    };

    {
        let (called_finished, pipeline) = create_pipeline();

        match QueryPipelineExecutor::create(pipeline, settings.clone()) {
            Ok(_) => unreachable!(),
            Err(error) => {
                assert_eq!(error.code(), 1001);
                assert_eq!(
                    error.message().as_str(),
                    "Pipeline max threads cannot be zero"
                );
                assert!(called_finished.load(Ordering::SeqCst));
            }
        }
    }

    let ctx = fixture.new_query_ctx().await?;
    {
        let (called_finished, mut pipeline) = create_pipeline();
        let (_rx, sink_pipe) = create_sink_pipe(1)?;
        let (_tx, source_pipe) = create_source_pipe(ctx, 1)?;
        pipeline.add_pipe(source_pipe);
        pipeline.add_pipe(sink_pipe);
        pipeline.set_max_threads(1);

        let executor = QueryPipelineExecutor::create(pipeline, settings.clone())?;

        match executor.execute() {
            Ok(_) => unreachable!(),
            Err(error) => {
                assert_eq!(error.code(), 1001);
                assert_eq!(
                    error.message().as_str(),
                    "test failure\n(while in query pipeline init)"
                );
                assert!(!called_finished.load(Ordering::SeqCst));
                drop(executor);
                assert!(called_finished.load(Ordering::SeqCst));
            }
        }
    }

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_cancel_async_process_when_outputs_finished() -> anyhow::Result<()> {
    let _fixture = TestFixture::setup().await?;

    // The source waits forever. It only finishes because the executor drops its pending
    // `async_process` once the sink finished the source's only output; otherwise the query hits
    // the execution time limit.
    let (async_process_dropped, pipeline) = create_pending_source_pipeline(true);
    let executor = QueryPipelineExecutor::create(
        pipeline,
        cancel_async_executor_settings("test-cancel-async-process"),
    )?;
    let result = spawn_blocking(move || executor.execute()).await?;
    result?;
    assert!(async_process_dropped.load(Ordering::SeqCst));

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_async_event_after_cancel_is_an_error() -> anyhow::Result<()> {
    let _fixture = TestFixture::setup().await?;

    // The source keeps asking for `async_process` although its output finished. Every such call
    // would be cancelled right away, so the executor fails the query instead of spinning.
    let (async_process_dropped, pipeline) = create_pending_source_pipeline(false);
    let executor = QueryPipelineExecutor::create(
        pipeline,
        cancel_async_executor_settings("test-async-event-after-cancel"),
    )?;
    let result = spawn_blocking(move || executor.execute()).await?;
    let error = result.expect_err("Event::Async after a cancellation must fail the query");
    assert_eq!(error.code(), ErrorCode::INTERNAL);
    assert!(error.message().contains("PendingAsyncSource"));
    assert!(async_process_dropped.load(Ordering::SeqCst));

    Ok(())
}

fn cancel_async_executor_settings(query_id: &str) -> ExecutorSettings {
    ExecutorSettings {
        query_id: Arc::new(query_id.to_string()),
        max_execute_time_in_seconds: Duration::from_secs(30),
        enable_queries_executor: false,
        max_threads: 2,
        executor_node_id: "".to_string(),
        perf_event_groups: vec![],
    }
}

/// `PendingAsyncSource -> FinishAfterSourceStartedSink`. Returns the flag set when the source's
/// pending `async_process` is dropped.
fn create_pending_source_pipeline(finish_on_outputs_finished: bool) -> (Arc<AtomicBool>, Pipeline) {
    let async_process_dropped = Arc::new(AtomicBool::new(false));
    let source_started = Arc::new(Notify::new());

    let mut pipeline = Pipeline::create();
    let output = OutputPort::create();
    pipeline.add_pipe(Pipe::create(0, 1, vec![PipeItem::create(
        ProcessorPtr::create(Box::new(PendingAsyncSource {
            output: output.clone(),
            finish_on_outputs_finished,
            started: source_started.clone(),
            async_process_dropped: async_process_dropped.clone(),
        })),
        vec![],
        vec![output],
    )]));
    let input = InputPort::create();
    pipeline.add_pipe(Pipe::create(1, 0, vec![PipeItem::create(
        ProcessorPtr::create(Box::new(FinishAfterSourceStartedSink {
            input: input.clone(),
            source_started,
            waited: false,
        })),
        vec![input],
        vec![],
    )]));
    pipeline.set_max_threads(2);

    (async_process_dropped, pipeline)
}

struct SetOnDrop(Arc<AtomicBool>);

impl Drop for SetOnDrop {
    fn drop(&mut self) {
        self.0.store(true, Ordering::SeqCst);
    }
}

struct PendingAsyncSource {
    output: Arc<OutputPort>,
    /// `false` breaks the `cancel_async_on_outputs_finished` contract on purpose.
    finish_on_outputs_finished: bool,
    started: Arc<Notify>,
    async_process_dropped: Arc<AtomicBool>,
}

#[async_trait::async_trait]
impl Processor for PendingAsyncSource {
    fn name(&self) -> String {
        "PendingAsyncSource".to_string()
    }

    fn as_any(&mut self) -> &mut dyn Any {
        self
    }

    fn event(&mut self) -> Result<Event> {
        if self.finish_on_outputs_finished && self.output.is_finished() {
            return Ok(Event::Finished);
        }
        Ok(Event::Async)
    }

    async fn async_process(&mut self) -> Result<()> {
        let _dropped = SetOnDrop(self.async_process_dropped.clone());
        // `notify_one` keeps a permit, so the sink sees it even if it has not started waiting.
        self.started.notify_one();
        std::future::pending::<()>().await;
        Ok(())
    }

    fn cancel_async_on_outputs_finished(&self) -> bool {
        true
    }
}

/// Finishes its input only after the source's `async_process` is running.
struct FinishAfterSourceStartedSink {
    input: Arc<InputPort>,
    source_started: Arc<Notify>,
    waited: bool,
}

#[async_trait::async_trait]
impl Processor for FinishAfterSourceStartedSink {
    fn name(&self) -> String {
        "FinishAfterSourceStartedSink".to_string()
    }

    fn as_any(&mut self) -> &mut dyn Any {
        self
    }

    fn event(&mut self) -> Result<Event> {
        if !self.waited {
            self.input.set_need_data();
            return Ok(Event::Async);
        }
        self.input.finish();
        Ok(Event::Finished)
    }

    async fn async_process(&mut self) -> Result<()> {
        self.source_started.notified().await;
        self.waited = true;
        Ok(())
    }
}

fn create_pipeline() -> (Arc<AtomicBool>, Pipeline) {
    let called_finished = Arc::new(AtomicBool::new(false));
    let mut pipeline = Pipeline::create();
    pipeline.set_on_init(|| Err(ErrorCode::Internal("test failure")));
    pipeline.set_on_finished({
        let called_finished = called_finished.clone();
        move |_info: &ExecutionInfo| {
            called_finished.fetch_or(true, Ordering::SeqCst);
            Ok(())
        }
    });

    (called_finished, pipeline)
}

fn create_source_pipe(
    ctx: Arc<QueryContext>,
    size: usize,
) -> Result<(Vec<Sender<Result<DataBlock>>>, Pipe)> {
    let mut txs = Vec::with_capacity(size);
    let mut items = Vec::with_capacity(size);

    for _index in 0..size {
        let output = OutputPort::create();
        let (tx, rx) = channel(1);
        txs.push(tx);
        items.push(PipeItem::create(
            SyncReceiverSource::create(ctx.get_scan_progress(), rx, output.clone())?,
            vec![],
            vec![output],
        ));
    }
    Ok((txs, Pipe::create(0, size, items)))
}

fn create_sink_pipe(size: usize) -> Result<(Vec<Receiver<Result<DataBlock>>>, Pipe)> {
    let mut rxs = Vec::with_capacity(size);
    let mut items = Vec::with_capacity(size);
    for _index in 0..size {
        let input = InputPort::create();
        let (tx, rx) = channel(1);
        rxs.push(rx);
        items.push(PipeItem::create(
            ProcessorPtr::create(SyncSenderSink::create(tx, input.clone())),
            vec![input],
            vec![],
        ));
    }

    Ok((rxs, Pipe::create(size, 0, items)))
}
