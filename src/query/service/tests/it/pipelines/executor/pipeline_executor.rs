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

use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;
use std::sync::Arc;

use common_base::base::tokio;
use common_base::base::tokio::sync::mpsc::channel;
use common_base::base::tokio::sync::mpsc::Receiver;
use common_base::base::tokio::sync::mpsc::Sender;
use common_exception::ErrorCode;
use common_exception::Result;
use common_expression::DataBlock;
use common_pipeline_core::pipe::Pipe;
use common_pipeline_core::pipe::PipeItem;
use common_pipeline_core::processors::port::InputPort;
use common_pipeline_core::processors::port::OutputPort;
use common_pipeline_core::processors::processor::ProcessorPtr;
use common_pipeline_core::Pipeline;
use common_pipeline_sinks::SyncSenderSink;
use common_pipeline_sources::SyncReceiverSource;
use databend_query::pipelines::executor::ExecutorSettings;
use databend_query::pipelines::executor::PipelineExecutor;
use databend_query::sessions::QueryContext;
use databend_query::test_kits::create_query_context;

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_always_call_on_finished() -> Result<()> {
    let settings = ExecutorSettings {
        query_id: Arc::new("".to_string()),
        max_execute_time: Default::default(),
    };

    {
        let (called_finished, pipeline) = create_pipeline();

        match PipelineExecutor::create(pipeline, settings.clone()) {
            Ok(_) => unreachable!(),
            Err(error) => {
                assert_eq!(error.code(), 1001);
                assert_eq!(
                    error.message().as_str(),
                    "Pipeline max threads cannot equals zero."
                );
                assert!(called_finished.load(Ordering::SeqCst));
            }
        }
    }

    let (_guard, ctx) = create_query_context().await?;
    {
        let (called_finished, mut pipeline) = create_pipeline();
        let (_rx, sink_pipe) = create_sink_pipe(1)?;
        let (_tx, source_pipe) = create_source_pipe(ctx, 1)?;
        pipeline.add_pipe(source_pipe);
        pipeline.add_pipe(sink_pipe);
        pipeline.set_max_threads(1);

        let executor = PipelineExecutor::create(pipeline, settings.clone())?;

        match executor.execute() {
            Ok(_) => unreachable!(),
            Err(error) => {
                assert_eq!(error.code(), 1001);
                assert_eq!(
                    error.message().as_str(),
                    "test failure(while in query pipeline init)"
                );
                assert!(!called_finished.load(Ordering::SeqCst));
                drop(executor);
                assert!(called_finished.load(Ordering::SeqCst));
            }
        }
    }

    Ok(())
}

fn create_pipeline() -> (Arc<AtomicBool>, Pipeline) {
    let called_finished = Arc::new(AtomicBool::new(false));
    let mut pipeline = Pipeline::create();
    pipeline.set_on_init(|| Err(ErrorCode::Internal("test failure")));
    pipeline.set_on_finished({
        let called_finished = called_finished.clone();
        move |_may_error| {
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
            SyncReceiverSource::create(ctx.clone(), rx, output.clone())?,
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
