use std::fmt::format;
use petgraph::graph::node_index;
use common_base::tokio;
use common_base::tokio::sync::mpsc::{channel, Receiver, Sender};
use common_datablocks::DataBlock;
use common_exception::Result;
use databend_query::pipelines::new::{NewPipe, NewPipeline};
use databend_query::pipelines::new::executor::RunningGraph;
use databend_query::pipelines::new::processors::{SyncReceiverSource, SyncSenderSink, TransformDummy};
use databend_query::pipelines::new::processors::port::{InputPort, OutputPort};

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
        format!("{:?}", create_parallel_simple_pipeline()),
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
                    QueueItem { id: 0, name: \"SyncReceiverSource\" }\
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
            format!("{:?}", create_parallel_simple_pipeline()?.init_schedule_queue()?),
            "ScheduleQueue { \
                sync_queue: [\
                    QueueItem { id: 0, name: \"SyncReceiverSource\" }, \
                    QueueItem { id: 1, name: \"SyncReceiverSource\" }\
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
                sync_queue: [QueueItem { id: 0, name: \"SyncReceiverSource\" }], \
                async_queue: [] \
            }"
        );

        Ok(())
    }
}

// #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
// async fn test_simple_pipeline_schedule_queue() -> Result<()> {
//     unsafe {
//         let (rx, sink_pipe) = create_sink_pipe(1)?;
//         let (tx, source_pipe) = create_source_pipe(1)?;
//
//         let mut pipeline = NewPipeline::create();
//         pipeline.add_pipe(source_pipe);
//         pipeline.add_pipe(create_transform_pipe(1)?);
//         pipeline.add_pipe(sink_pipe);
//
//         let graph = RunningGraph::create(pipeline)?;
//         let mut schedule_queue = graph.schedule_queue(node_index(0))?;
//
//         // match schedule_queue.pop_task() {
//         //     Some(ExecutorTask::) =>
//         // }
//         println!("{:?}", schedule_queue.pop_task());
//         println!("{:?}", schedule_queue.pop_task());
//         // let s = schedule_queue.pop_task();
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

    Ok(RunningGraph::create(pipeline)?)
}

fn create_parallel_simple_pipeline() -> Result<RunningGraph> {
    let (_rx, sink_pipe) = create_sink_pipe(2)?;
    let (_tx, source_pipe) = create_source_pipe(2)?;

    let mut pipeline = NewPipeline::create();
    pipeline.add_pipe(source_pipe);
    pipeline.add_pipe(create_transform_pipe(2)?);
    pipeline.add_pipe(sink_pipe);

    Ok(RunningGraph::create(pipeline)?)
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

    Ok(RunningGraph::create(pipeline)?)
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
