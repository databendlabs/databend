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

use std::sync::Arc;

use common_base::base::tokio;
use common_base::base::tokio::sync::mpsc::channel;
use common_base::base::tokio::sync::mpsc::Receiver;
use common_base::base::tokio::sync::mpsc::Sender;
use common_exception::Result;
use common_expression::DataBlock;
use common_pipeline_core::pipe::Pipe;
use common_pipeline_core::pipe::PipeItem;
use common_pipeline_core::processors::processor::ProcessorPtr;
use common_pipeline_sinks::SyncSenderSink;
use common_pipeline_sources::SyncReceiverSource;
use common_pipeline_transforms::processors::transforms::TransformDummy;
use databend_query::pipelines::executor::RunningGraph;
use databend_query::pipelines::processors::port::InputPort;
use databend_query::pipelines::processors::port::OutputPort;
use databend_query::pipelines::Pipeline;
use databend_query::sessions::QueryContext;
use databend_query::test_kits::create_query_context;

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_create_simple_pipeline() -> Result<()> {
    let (_guard, ctx) = create_query_context().await?;
    assert_eq!(
        format!("{:?}", create_simple_pipeline(ctx)?),
        "digraph {\
            \n    0 [ label = \"SyncReceiverSource\" ]\
            \n    1 [ label = \"DummyTransform\" ]\
            \n    2 [ label = \"SyncSenderSink\" ]\
            \n    0 -> 1 [ ]\
            \n    1 -> 2 [ ]\
        \n}\n"
    );

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_create_parallel_simple_pipeline() -> Result<()> {
    let (_guard, ctx) = create_query_context().await?;
    assert_eq!(
        format!("{:?}", create_parallel_simple_pipeline(ctx)?),
        "digraph {\
            \n    0 [ label = \"SyncReceiverSource\" ]\
            \n    1 [ label = \"SyncReceiverSource\" ]\
            \n    2 [ label = \"DummyTransform\" ]\
            \n    3 [ label = \"DummyTransform\" ]\
            \n    4 [ label = \"SyncSenderSink\" ]\
            \n    5 [ label = \"SyncSenderSink\" ]\
            \n    0 -> 2 [ ]\
            \n    1 -> 3 [ ]\
            \n    2 -> 4 [ ]\
            \n    3 -> 5 [ ]\
        \n}\n"
    );

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_create_resize_pipeline() -> Result<()> {
    let (_guard, ctx) = create_query_context().await?;
    assert_eq!(
        format!("{:?}", create_resize_pipeline(ctx)?),
        "digraph {\
            \n    0 [ label = \"SyncReceiverSource\" ]\
            \n    1 [ label = \"Resize\" ]\
            \n    2 [ label = \"DummyTransform\" ]\
            \n    3 [ label = \"DummyTransform\" ]\
            \n    4 [ label = \"Resize\" ]\
            \n    5 [ label = \"DummyTransform\" ]\
            \n    6 [ label = \"Resize\" ]\
            \n    7 [ label = \"SyncSenderSink\" ]\
            \n    8 [ label = \"SyncSenderSink\" ]\
            \n    0 -> 1 [ ]\
            \n    1 -> 2 [ ]\
            \n    1 -> 3 [ ]\
            \n    2 -> 4 [ ]\
            \n    3 -> 4 [ ]\
            \n    4 -> 5 [ ]\
            \n    5 -> 6 [ ]\
            \n    6 -> 7 [ ]\
            \n    6 -> 8 [ ]\
        \n}\n"
    );

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_simple_pipeline_init_queue() -> Result<()> {
    let (_guard, ctx) = create_query_context().await?;
    unsafe {
        assert_eq!(
            format!("{:?}", create_simple_pipeline(ctx)?.init_schedule_queue(0)?),
            "ScheduleQueue { \
                sync_queue: [\
                    QueueItem { id: 2, name: \"SyncSenderSink\" }\
                ], \
                async_queue: [] \
            }"
        );
        Ok(())
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_parallel_simple_pipeline_init_queue() -> Result<()> {
    let (_guard, ctx) = create_query_context().await?;
    unsafe {
        assert_eq!(
            format!(
                "{:?}",
                create_parallel_simple_pipeline(ctx)?.init_schedule_queue(0)?
            ),
            "ScheduleQueue { \
                sync_queue: [\
                    QueueItem { id: 4, name: \"SyncSenderSink\" }, \
                    QueueItem { id: 5, name: \"SyncSenderSink\" }\
                ], \
                async_queue: [] \
            }"
        );
        Ok(())
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_resize_pipeline_init_queue() -> Result<()> {
    let (_guard, ctx) = create_query_context().await?;
    unsafe {
        assert_eq!(
            format!("{:?}", create_resize_pipeline(ctx)?.init_schedule_queue(0)?),
            "ScheduleQueue { \
                sync_queue: [\
                    QueueItem { id: 7, name: \"SyncSenderSink\" }, \
                    QueueItem { id: 8, name: \"SyncSenderSink\" }\
                ], \
                async_queue: [] \
            }"
        );

        Ok(())
    }
}

fn create_simple_pipeline(ctx: Arc<QueryContext>) -> Result<RunningGraph> {
    let (_rx, sink_pipe) = create_sink_pipe(1)?;
    let (_tx, source_pipe) = create_source_pipe(ctx, 1)?;

    let mut pipeline = Pipeline::create();
    pipeline.add_pipe(source_pipe);
    pipeline.add_pipe(create_transform_pipe(1)?);
    pipeline.add_pipe(sink_pipe);

    RunningGraph::create(pipeline)
}

fn create_parallel_simple_pipeline(ctx: Arc<QueryContext>) -> Result<RunningGraph> {
    let (_rx, sink_pipe) = create_sink_pipe(2)?;
    let (_tx, source_pipe) = create_source_pipe(ctx, 2)?;

    let mut pipeline = Pipeline::create();
    pipeline.add_pipe(source_pipe);
    pipeline.add_pipe(create_transform_pipe(2)?);
    pipeline.add_pipe(sink_pipe);

    RunningGraph::create(pipeline)
}

fn create_resize_pipeline(ctx: Arc<QueryContext>) -> Result<RunningGraph> {
    let (_rx, sink_pipe) = create_sink_pipe(2)?;
    let (_tx, source_pipe) = create_source_pipe(ctx, 1)?;

    let mut pipeline = Pipeline::create();
    pipeline.add_pipe(source_pipe);
    pipeline.resize(2)?;
    pipeline.add_pipe(create_transform_pipe(2)?);
    pipeline.resize(1)?;
    pipeline.add_pipe(create_transform_pipe(1)?);
    pipeline.resize(2)?;
    pipeline.add_pipe(sink_pipe);

    RunningGraph::create(pipeline)
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
            SyncReceiverSource::create(ctx.clone(), rx, output.clone())?,
            vec![],
            vec![output],
        ));
    }
    Ok((txs, Pipe::create(0, size, items)))
}

fn create_transform_pipe(size: usize) -> Result<Pipe> {
    let mut items = Vec::with_capacity(size);

    for _index in 0..size {
        let input = InputPort::create();
        let output = OutputPort::create();

        items.push(PipeItem::create(
            TransformDummy::create(input.clone(), output.clone()),
            vec![input],
            vec![output],
        ));
    }

    Ok(Pipe::create(size, size, items))
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
