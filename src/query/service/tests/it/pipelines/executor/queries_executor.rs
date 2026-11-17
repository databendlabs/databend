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

//! Tests of the executor shared by all the queries, `enable_queries_executor`.

use std::any::Any;
use std::collections::VecDeque;
use std::sync::Arc;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;
use std::time::Duration;
use std::time::Instant;

use databend_common_base::base::Progress;
use databend_common_base::runtime::Thread;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::DataBlock;
use databend_common_expression::FromData;
use databend_common_expression::ScalarRef;
use databend_common_expression::types::Int32Type;
use databend_common_expression::types::number::NumberScalar;
use databend_common_pipeline::core::Event;
use databend_common_pipeline::core::ExecutionInfo;
use databend_common_pipeline::core::InputPort;
use databend_common_pipeline::core::OutputPort;
use databend_common_pipeline::core::Pipe;
use databend_common_pipeline::core::PipeItem;
use databend_common_pipeline::core::Pipeline;
use databend_common_pipeline::core::Processor;
use databend_common_pipeline::core::ProcessorPtr;
use databend_common_pipeline::core::always_callback;
use databend_common_pipeline::sources::BlocksSource;
use databend_common_pipeline::sources::EmptySource;
use databend_query::pipelines::PipelineBuildResult;
use databend_query::pipelines::executor::ExecutorSettings;
use databend_query::pipelines::executor::GlobalQueriesExecutor;
use databend_query::pipelines::executor::PipelineCompleteExecutor;
use databend_query::pipelines::executor::PipelinePullingExecutor;
use databend_query::pipelines::executor::QueriesPipelineExecutor;
use databend_query::pipelines::executor::RunningGraph;
use databend_query::test_kits::ConfigBuilder;
use databend_query::test_kits::TestFixture;
use futures::TryStreamExt;
use parking_lot::Mutex;
use tokio::time::sleep;
use tokio::time::timeout;

type Finished = Arc<(Mutex<bool>, parking_lot::Condvar)>;

async fn setup() -> anyhow::Result<TestFixture> {
    let config = ConfigBuilder::create().enable_queries_executor().config();
    Ok(TestFixture::setup_with_config(&config).await?)
}

#[tokio::test(flavor = "multi_thread")]
async fn test_query_runs_on_queries_executor() -> anyhow::Result<()> {
    let fixture = setup().await?;
    assert!(GlobalQueriesExecutor::try_instance().is_some());

    let blocks = fixture
        .execute_query("SELECT sum(number) FROM numbers(100000)")
        .await?
        .try_collect::<Vec<_>>()
        .await?;
    let block = DataBlock::concat(&blocks)?;
    assert!(matches!(
        block.get_by_offset(0).index(0),
        Some(ScalarRef::Number(NumberScalar::UInt64(4999950000)))
    ));

    Ok(())
}

/// The waiter of a graph runs the finish callbacks of the query: it must not be
/// woken while a processor of the graph still runs on another worker.
#[tokio::test(flavor = "multi_thread")]
async fn test_queries_executor_finishes_after_running_tasks() -> anyhow::Result<()> {
    let _fixture = TestFixture::setup().await?;

    let started = Arc::new(AtomicBool::new(false));
    let done = Arc::new(AtomicBool::new(false));
    let slow = OnceSource::create({
        let started = started.clone();
        let done = done.clone();
        move || {
            started.store(true, Ordering::SeqCst);
            std::thread::sleep(Duration::from_millis(500));
            done.store(true, Ordering::SeqCst);
            Ok(())
        }
    });
    let failing = OnceSource::create(move || {
        // Fail while the other processor runs.
        let deadline = Instant::now() + Duration::from_secs(10);
        while !started.load(Ordering::SeqCst) && Instant::now() < deadline {
            std::thread::sleep(Duration::from_millis(1));
        }
        Err(ErrorCode::Internal("test failure"))
    });
    let (graph, finished) = create_graph("test-finish-after-running-tasks", vec![slow, failing])?;

    let executor = QueriesPipelineExecutor::create(2)?;
    let workers = Thread::spawn({
        let executor = executor.clone();
        move || executor.execute()
    });
    executor.send_graph(graph.clone())?;

    assert!(wait_finished(&finished), "the graph must finish");
    assert!(
        done.load(Ordering::SeqCst),
        "the graph must finish after its running tasks"
    );
    let error = graph.get_error().expect("the graph must fail");
    assert_eq!(error.code(), ErrorCode::INTERNAL);

    executor.finish(None);
    workers.join().unwrap()?;
    Ok(())
}

/// A worker drops the tasks of a finished graph instead of running them, and
/// keeps no reference to the graph.
#[tokio::test(flavor = "multi_thread")]
async fn test_queries_executor_drops_tasks_of_finished_graph() -> anyhow::Result<()> {
    let _fixture = TestFixture::setup().await?;

    let ran = Arc::new(AtomicBool::new(false));
    let (finished_graph, _) = create_graph("test-finished-graph", vec![OnceSource::create({
        let ran = ran.clone();
        move || {
            ran.store(true, Ordering::SeqCst);
            Ok(())
        }
    })])?;
    finished_graph.should_finish(Err(ErrorCode::AbortedQuery("test abort")))?;
    let (live_graph, live_finished) =
        create_graph("test-live-graph", vec![OnceSource::create(|| Ok(()))])?;

    // The only worker takes the tasks in the order of the graphs.
    let executor = QueriesPipelineExecutor::create(1)?;
    let workers = Thread::spawn({
        let executor = executor.clone();
        move || executor.execute()
    });
    executor.send_graph(finished_graph.clone())?;
    executor.send_graph(live_graph.clone())?;

    assert!(wait_finished(&live_finished), "the live graph must finish");
    assert!(
        !ran.load(Ordering::SeqCst),
        "the task of the finished graph must not run"
    );
    assert_eq!(Arc::strong_count(&finished_graph), 1);

    executor.finish(None);
    workers.join().unwrap()?;
    Ok(())
}

/// The first call of `should_finish` decides the result of the graph, and wakes
/// its waiter when no task of the graph runs.
#[tokio::test(flavor = "multi_thread")]
async fn test_queries_executor_first_finish_decides() -> anyhow::Result<()> {
    let _fixture = TestFixture::setup().await?;

    let (graph, finished) = create_graph("test-first-error", vec![OnceSource::create(|| Ok(()))])?;
    graph.should_finish(Err(ErrorCode::Internal("first")))?;
    graph.should_finish::<()>(Ok(()))?;
    graph.should_finish(Err(ErrorCode::AbortedQuery("second")))?;
    assert!(wait_finished(&finished));
    assert_eq!(
        graph.get_error().map(|e| e.code()),
        Some(ErrorCode::INTERNAL)
    );

    let (graph, finished) = create_graph("test-first-ok", vec![OnceSource::create(|| Ok(()))])?;
    graph.should_finish::<()>(Ok(()))?;
    graph.should_finish(Err(ErrorCode::AbortedQuery("late")))?;
    assert!(wait_finished(&finished));
    assert!(graph.get_error().is_none());

    Ok(())
}

/// The processors of the query all finish in their first event, so no worker runs
/// a task of it that would finish it.
#[tokio::test(flavor = "multi_thread")]
async fn test_queries_executor_runs_pipeline_without_tasks() -> anyhow::Result<()> {
    let _fixture = setup().await?;

    let (pipeline, succeeded) = empty_pipeline()?;
    let executor = PipelineCompleteExecutor::try_create(pipeline, queries_executor_settings())?;
    timeout(Duration::from_secs(10), executor.execute()).await??;
    assert_eq!(*succeeded.lock(), Some(true));

    Ok(())
}

/// A query that fails to start still runs its finish callbacks, which e.g. release
/// its table locks.
#[tokio::test(flavor = "multi_thread")]
async fn test_queries_executor_finishes_query_failed_to_start() -> anyhow::Result<()> {
    let _fixture = setup().await?;

    let (mut pipeline, succeeded) = empty_pipeline()?;
    pipeline.set_on_init(|| Err(ErrorCode::Internal("test init failure")));
    let executor = PipelineCompleteExecutor::try_create(pipeline, queries_executor_settings())?;
    let res = timeout(Duration::from_secs(10), executor.execute()).await?;
    assert_eq!(res.unwrap_err().code(), ErrorCode::INTERNAL);
    assert_eq!(*succeeded.lock(), Some(false));

    Ok(())
}

/// A query stopped without an error before its nodes finished did not complete,
/// as on the per-query executor.
#[tokio::test(flavor = "multi_thread")]
async fn test_queries_executor_fails_query_finished_early() -> anyhow::Result<()> {
    let _fixture = setup().await?;

    let started = Arc::new(AtomicBool::new(false));
    let mut pipeline = Pipeline::create();
    pipeline.add_pipe(Pipe::create(0, 1, vec![PipeItem::create(
        ProcessorPtr::create(Box::new(PendingSource {
            started: started.clone(),
        })),
        vec![],
        vec![OutputPort::create()],
    )]));
    pipeline.add_sink(|input| Ok(ProcessorPtr::create(Box::new(DrainSink { input }))))?;
    let succeeded = record_result(&mut pipeline);

    let executor = PipelineCompleteExecutor::try_create(pipeline, queries_executor_settings())?;
    let finish_early = async {
        let deadline = Instant::now() + Duration::from_secs(10);
        while !started.load(Ordering::SeqCst) && Instant::now() < deadline {
            sleep(Duration::from_millis(10)).await;
        }
        executor.finish(None);
    };
    let (res, _) = tokio::join!(
        timeout(Duration::from_secs(20), executor.execute()),
        finish_early
    );
    assert_eq!(res?.unwrap_err().code(), ErrorCode::INTERNAL);
    assert_eq!(*succeeded.lock(), Some(false));

    Ok(())
}

/// A query that is dropped without being executed runs its finish callbacks, as
/// the per-query executor does.
#[tokio::test(flavor = "multi_thread")]
async fn test_queries_executor_finishes_query_never_executed() -> anyhow::Result<()> {
    let _fixture = setup().await?;

    let (pipeline, succeeded) = empty_pipeline()?;
    let executor = PipelineCompleteExecutor::try_create(pipeline, queries_executor_settings())?;
    assert_eq!(*succeeded.lock(), None);
    drop(executor);
    assert_eq!(*succeeded.lock(), Some(false));

    Ok(())
}

/// A query whose result is not consumed waits for its consumer without holding
/// the workers shared by all the queries, and can still be killed.
#[tokio::test(flavor = "multi_thread")]
async fn test_queries_executor_unconsumed_result_holds_no_worker() -> anyhow::Result<()> {
    const BLOCK_ROWS: usize = 4;

    let fixture = setup().await?;

    // The channel of the result holds a block per sink, so two more blocks per
    // sink leave more sinks waiting for the consumer than there are workers.
    let workers = num_cpus::get();
    let sinks = workers * 2;
    let blocks = (0..sinks * 3)
        .map(|_| DataBlock::new_from_columns(vec![Int32Type::from_data(vec![0; BLOCK_ROWS])]))
        .collect::<VecDeque<_>>();
    let blocks = Arc::new(std::sync::Mutex::new(blocks));

    let mut pipeline = Pipeline::create();
    let scan_progress = Arc::new(Progress::create());
    pipeline.add_source(
        |output| BlocksSource::create(scan_progress.clone(), output, blocks.clone()),
        sinks,
    )?;
    let mut build_res = PipelineBuildResult::create();
    build_res.main_pipeline = pipeline;

    let result_progress = Arc::new(Progress::create());
    let mut unconsumed = PipelinePullingExecutor::from_pipelines_with_progress(
        build_res,
        queries_executor_settings(),
        Some(result_progress.clone()),
    )?;
    unconsumed.start();

    // A sink counts a block before it waits for the consumer: wait until enough
    // sinks wait to hold all the workers, if waiting held one.
    let waiting_rows = (sinks + workers) * BLOCK_ROWS;
    let deadline = Instant::now() + Duration::from_secs(10);
    while result_progress.get_values().rows < waiting_rows && Instant::now() < deadline {
        sleep(Duration::from_millis(10)).await;
    }

    let collect = async {
        fixture
            .execute_query("SELECT sum(number) FROM numbers(100000)")
            .await?
            .try_collect::<Vec<_>>()
            .await
    };
    let blocks = timeout(Duration::from_secs(30), collect)
        .await
        .expect("another query must complete while the result is not consumed")?;
    let block = DataBlock::concat(&blocks)?;
    assert!(matches!(
        block.get_by_offset(0).index(0),
        Some(ScalarRef::Number(NumberScalar::UInt64(4999950000)))
    ));

    // The query is killed without its result being consumed.
    unconsumed.finish(Some(ErrorCode::AbortedQuery("test kill")));
    let killed = async {
        loop {
            match unconsumed.pull_data().await {
                Ok(Some(_)) => continue,
                Ok(None) => panic!("a killed query must fail"),
                Err(cause) => break cause,
            }
        }
    };
    let cause = timeout(Duration::from_secs(30), killed)
        .await
        .expect("a query must be killed while its result is not consumed");
    assert_eq!(cause.code(), ErrorCode::ABORTED_QUERY);

    Ok(())
}

fn queries_executor_settings() -> ExecutorSettings {
    ExecutorSettings {
        query_id: Arc::new("test-queries-executor".to_string()),
        max_threads: 2,
        enable_queries_executor: true,
        max_execute_time_in_seconds: Default::default(),
        executor_node_id: "".to_string(),
        perf_event_groups: vec![],
    }
}

/// An empty source drained by a sink. Its finish callback records whether the
/// query succeeded.
fn empty_pipeline() -> Result<(Pipeline, Arc<Mutex<Option<bool>>>)> {
    let mut pipeline = Pipeline::create();
    pipeline.add_source(EmptySource::create, 1)?;
    pipeline.add_sink(|input| Ok(ProcessorPtr::create(Box::new(DrainSink { input }))))?;

    let succeeded = record_result(&mut pipeline);
    Ok((pipeline, succeeded))
}

/// Records whether the query succeeded when its finish callbacks run.
fn record_result(pipeline: &mut Pipeline) -> Arc<Mutex<Option<bool>>> {
    let succeeded = Arc::new(Mutex::new(None));
    pipeline.set_on_finished(always_callback({
        let succeeded = succeeded.clone();
        move |info: &ExecutionInfo| {
            *succeeded.lock() = Some(info.res.is_ok());
            Ok(())
        }
    }));
    succeeded
}

/// A graph of `processors`, each a source of its own that is not connected.
fn create_graph(
    query_id: &str,
    processors: Vec<Box<dyn Processor>>,
) -> Result<(Arc<RunningGraph>, Finished)> {
    let items = processors
        .into_iter()
        .map(|processor| {
            PipeItem::create(ProcessorPtr::create(processor), vec![], vec![
                OutputPort::create(),
            ])
        })
        .collect::<Vec<_>>();
    let mut pipeline = Pipeline::create();
    pipeline.add_pipe(Pipe::create(0, items.len(), items));

    let finished = Finished::default();
    let graph = RunningGraph::create(
        pipeline,
        1,
        Arc::new(query_id.to_string()),
        Some(finished.clone()),
        vec![],
    )?;
    Ok((graph, finished))
}

fn wait_finished(finished: &Finished) -> bool {
    let (lock, cvar) = &**finished;
    let mut finished = lock.lock();
    let deadline = Instant::now() + Duration::from_secs(10);
    while !*finished {
        if cvar.wait_until(&mut finished, deadline).timed_out() {
            break;
        }
    }
    *finished
}

/// A source that runs `process` once, then finishes.
struct OnceSource<F> {
    processed: bool,
    process: F,
}

impl<F: FnMut() -> Result<()> + Send + 'static> OnceSource<F> {
    fn create(process: F) -> Box<dyn Processor> {
        Box::new(OnceSource {
            processed: false,
            process,
        })
    }
}

impl<F: FnMut() -> Result<()> + Send + 'static> Processor for OnceSource<F> {
    fn name(&self) -> String {
        "OnceSource".to_string()
    }

    fn as_any(&mut self) -> &mut dyn Any {
        self
    }

    fn event(&mut self) -> Result<Event> {
        match self.processed {
            true => Ok(Event::Finished),
            false => Ok(Event::Sync),
        }
    }

    fn process(&mut self) -> Result<()> {
        self.processed = true;
        (self.process)()
    }
}

/// A sink that drops its input, and finishes in the event that sees its input
/// finished.
struct DrainSink {
    input: Arc<InputPort>,
}

impl Processor for DrainSink {
    fn name(&self) -> String {
        "DrainSink".to_string()
    }

    fn as_any(&mut self) -> &mut dyn Any {
        self
    }

    fn event(&mut self) -> Result<Event> {
        if self.input.is_finished() {
            return Ok(Event::Finished);
        }
        if self.input.has_data() {
            drop(self.input.pull_data());
        }
        self.input.set_need_data();
        Ok(Event::NeedData)
    }
}

/// A source whose async task never completes on its own.
struct PendingSource {
    started: Arc<AtomicBool>,
}

#[async_trait::async_trait]
impl Processor for PendingSource {
    fn name(&self) -> String {
        "PendingSource".to_string()
    }

    fn as_any(&mut self) -> &mut dyn Any {
        self
    }

    fn event(&mut self) -> Result<Event> {
        Ok(Event::Async)
    }

    async fn async_process(&mut self) -> Result<()> {
        self.started.store(true, Ordering::SeqCst);
        futures::future::pending().await
    }
}
