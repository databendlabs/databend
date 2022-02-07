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

use common_base::tokio;
use common_base::tokio::sync::mpsc::channel;
use common_base::tokio::sync::mpsc::Receiver;
use common_base::tokio::sync::mpsc::Sender;
use common_datablocks::DataBlock;
use common_exception::Result;
use databend_query::pipelines::new::executor::RunningGraph;
use databend_query::pipelines::new::processors::port::InputPort;
use databend_query::pipelines::new::processors::port::OutputPort;
use databend_query::pipelines::new::processors::SyncReceiverSource;
use databend_query::pipelines::new::processors::SyncSenderSink;
use databend_query::pipelines::new::processors::TransformDummy;
use databend_query::pipelines::new::NewPipe;
use databend_query::pipelines::new::NewPipeline;

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_create_simple_pipeline() -> Result<()> {
    assert_eq!(
        format!("{:?}", create_simple_pipeline()?),
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
    assert_eq!(
        format!("{:?}", create_parallel_simple_pipeline()?),
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
    assert_eq!(
        format!("{:?}", create_resize_pipeline()?),
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
    unsafe {
        assert_eq!(
            format!("{:?}", create_simple_pipeline()?.init_schedule_queue()?),
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
    unsafe {
        assert_eq!(
            format!(
                "{:?}",
                create_parallel_simple_pipeline()?.init_schedule_queue()?
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
    unsafe {
        assert_eq!(
            format!("{:?}", create_resize_pipeline()?.init_schedule_queue()?),
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

// #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
// async fn test_simple_pipeline_schedule_queue() -> Result<()> {
//     unsafe {
//         let (mut rx, sink_pipe) = create_sink_pipe(1)?;
//         let (mut tx, source_pipe) = create_source_pipe(1)?;
//
//         let mut pipeline = NewPipeline::create();
//         pipeline.add_pipe(source_pipe);
//         pipeline.add_pipe(create_transform_pipe(1)?);
//         pipeline.add_pipe(sink_pipe);
//
//         let executor = PipelineExecutor::create(pipeline, 1)?;
//         let thread1_executor = executor.clone();
//         let thread1 = std::thread::spawn(move || {
//             thread1_executor.execute_with_single_worker(0);
//         });
//
//         let thread2 = std::thread::spawn(move || {
//             let tx = tx.remove(0);
//             for index in 0..5 {
//                 let schema = DataSchema::new(vec![DataField::new("field",i8:to_data_type())]);
//                 tx.blocking_send(Ok(DataBlock::create(Arc::new(schema), vec![DataColumn::Constant(DataValue::Int64(index), 2)])));
//             }
//         });
//
//         let thread3 = std::thread::spawn(move || {
//             while let Some(data) = rx[0].blocking_recv() {
//                 println!("{:?}", data);
//             }
//         });
//
//         thread2.join();
//         // thread3.join();
//         executor.finish();
//         thread1.join();
//     }
//
//
//     unimplemented!("")
// }

fn create_simple_pipeline() -> Result<RunningGraph> {
    let (_rx, sink_pipe) = create_sink_pipe(1)?;
    let (_tx, source_pipe) = create_source_pipe(1)?;

    let mut pipeline = NewPipeline::create();
    pipeline.add_pipe(source_pipe);
    pipeline.add_pipe(create_transform_pipe(1)?);
    pipeline.add_pipe(sink_pipe);

    RunningGraph::create(pipeline)
}

fn create_parallel_simple_pipeline() -> Result<RunningGraph> {
    let (_rx, sink_pipe) = create_sink_pipe(2)?;
    let (_tx, source_pipe) = create_source_pipe(2)?;

    let mut pipeline = NewPipeline::create();
    pipeline.add_pipe(source_pipe);
    pipeline.add_pipe(create_transform_pipe(2)?);
    pipeline.add_pipe(sink_pipe);

    RunningGraph::create(pipeline)
}

fn create_resize_pipeline() -> Result<RunningGraph> {
    let (_rx, sink_pipe) = create_sink_pipe(2)?;
    let (_tx, source_pipe) = create_source_pipe(1)?;

    let mut pipeline = NewPipeline::create();
    pipeline.add_pipe(source_pipe);
    pipeline.resize(2)?;
    pipeline.add_pipe(create_transform_pipe(2)?);
    pipeline.resize(1)?;
    pipeline.add_pipe(create_transform_pipe(1)?);
    pipeline.resize(2)?;
    pipeline.add_pipe(sink_pipe);

    RunningGraph::create(pipeline)
}

fn create_source_pipe(size: usize) -> Result<(Vec<Sender<Result<DataBlock>>>, NewPipe)> {
    let mut txs = Vec::with_capacity(size);
    let mut outputs = Vec::with_capacity(size);
    let mut processors = Vec::with_capacity(size);

    for _index in 0..size {
        let output = OutputPort::create();
        let (tx, rx) = channel(1);
        txs.push(tx);
        outputs.push(output.clone());
        processors.push(SyncReceiverSource::create(rx, output)?);
    }
    Ok((txs, NewPipe::SimplePipe {
        processors,
        inputs_port: vec![],
        outputs_port: outputs,
    }))
}

fn create_transform_pipe(size: usize) -> Result<NewPipe> {
    let mut inputs = Vec::with_capacity(size);
    let mut outputs = Vec::with_capacity(size);
    let mut processors = Vec::with_capacity(size);

    for _index in 0..size {
        let input = InputPort::create();
        let output = OutputPort::create();

        inputs.push(input.clone());
        outputs.push(output.clone());
        processors.push(TransformDummy::create(input, output));
    }

    Ok(NewPipe::SimplePipe {
        processors,
        inputs_port: inputs,
        outputs_port: outputs,
    })
}

fn create_sink_pipe(size: usize) -> Result<(Vec<Receiver<Result<DataBlock>>>, NewPipe)> {
    let mut rxs = Vec::with_capacity(size);
    let mut inputs = Vec::with_capacity(size);
    let mut processors = Vec::with_capacity(size);
    for _index in 0..size {
        let input = InputPort::create();
        let (tx, rx) = channel(1);
        rxs.push(rx);
        inputs.push(input.clone());
        processors.push(SyncSenderSink::create(tx, input));
    }

    Ok((rxs, NewPipe::SimplePipe {
        processors,
        inputs_port: inputs,
        outputs_port: vec![],
    }))
}
