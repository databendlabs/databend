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

use std::collections::VecDeque;
use std::sync::Arc;

use databend_common_base::base::tokio;
use databend_common_base::base::tokio::sync::mpsc::channel;
use databend_common_base::base::tokio::sync::mpsc::Receiver;
use databend_common_exception::Result;
use databend_common_expression::block_debug::pretty_format_blocks;
use databend_common_expression::types::Int32Type;
use databend_common_expression::DataBlock;
use databend_common_expression::DataField;
use databend_common_expression::DataSchemaRefExt;
use databend_common_expression::FromData;
use databend_common_expression::SortColumnDescription;
use databend_common_pipeline_core::processors::InputPort;
use databend_common_pipeline_core::processors::OutputPort;
use databend_common_pipeline_core::processors::ProcessorPtr;
use databend_common_pipeline_core::Pipe;
use databend_common_pipeline_core::PipeItem;
use databend_common_pipeline_core::Pipeline;
use databend_common_pipeline_sinks::SyncSenderSink;
use databend_common_pipeline_sources::BlocksSource;
use databend_common_pipeline_transforms::processors::add_k_way_merge_sort;
use databend_query::pipelines::executor::ExecutorSettings;
use databend_query::pipelines::executor::QueryPipelineExecutor;
use databend_query::sessions::QueryContext;
use databend_query::test_kits::TestFixture;
use itertools::Itertools;
use parking_lot::Mutex;
use rand::rngs::ThreadRng;
use rand::Rng;

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_k_way_merge_sort() -> Result<()> {
    let fixture = TestFixture::setup().await?;
    let ctx = fixture.new_query_ctx().await?;

    let worker = 3;
    let block_size = 4;
    let limit = None;
    let (data, expected) = basic_test_data(None);
    let (executor, mut rx) = create_pipeline(ctx, data, worker, block_size, limit)?;

    executor.execute()?;

    let mut got: Vec<DataBlock> = Vec::new();
    while !rx.is_empty() {
        got.push(rx.recv().await.unwrap()?);
    }

    check_result(got, expected);

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_k_way_merge_sort_fuzz() -> Result<()> {
    let mut rng = rand::thread_rng();
    let fixture = TestFixture::setup().await?;
    let ctx = fixture.new_query_ctx().await?;

    let worker = 3;
    let block_size = 10;
    let (data, expected, limit) = random_test_data(&mut rng, false);

    for (input, blocks) in data.iter().enumerate() {
        println!("intput {input} {blocks:?}\n\n\n");
    }

    let (executor, mut rx) = create_pipeline(ctx, data, worker, block_size, limit)?;
    executor.execute()?;

    let mut got: Vec<DataBlock> = Vec::new();
    while !rx.is_empty() {
        got.push(rx.recv().await.unwrap()?);
    }

    check_result(got, expected);

    Ok(())
}

fn create_pipeline(
    ctx: Arc<QueryContext>,
    data: Vec<Vec<DataBlock>>,
    worker: usize,
    block_size: usize,
    limit: Option<usize>,
) -> Result<(Arc<QueryPipelineExecutor>, Receiver<Result<DataBlock>>)> {
    let mut pipeline = Pipeline::create();

    let data_type = data[0][0].get_by_offset(0).data_type.clone();
    let source_pipe = create_source_pipe(ctx, data)?;
    pipeline.add_pipe(source_pipe);

    let schema = DataSchemaRefExt::create(vec![DataField::new("a", data_type)]);
    let sort_desc = Arc::new(vec![SortColumnDescription {
        offset: 0,
        asc: true,
        nulls_first: true,
        is_nullable: false,
    }]);
    add_k_way_merge_sort(
        &mut pipeline,
        schema,
        worker,
        block_size,
        limit,
        sort_desc,
        false,
        true,
    )?;

    let (mut rx, sink_pipe) = create_sink_pipe(1)?;
    let rx = rx.pop().unwrap();
    pipeline.add_pipe(sink_pipe);
    pipeline.set_max_threads(3);

    let settings = ExecutorSettings {
        query_id: Arc::new("".to_string()),
        max_execute_time_in_seconds: Default::default(),
        enable_queries_executor: false,
        max_threads: 8,
        executor_node_id: "".to_string(),
    };
    let executor = QueryPipelineExecutor::create(pipeline, settings)?;
    Ok((executor, rx))
}

fn create_source_pipe(ctx: Arc<QueryContext>, data: Vec<Vec<DataBlock>>) -> Result<Pipe> {
    let size = data.len();
    let mut items = Vec::with_capacity(size);

    for blocks in data.into_iter() {
        let output = OutputPort::create();
        items.push(PipeItem::create(
            BlocksSource::create(
                ctx.clone(),
                output.clone(),
                Arc::new(Mutex::new(VecDeque::from(blocks))),
            )?,
            vec![],
            vec![output],
        ));
    }
    Ok(Pipe::create(0, size, items))
}

fn create_sink_pipe(size: usize) -> Result<(Vec<Receiver<Result<DataBlock>>>, Pipe)> {
    let mut rxs = Vec::with_capacity(size);
    let mut items = Vec::with_capacity(size);
    for _index in 0..size {
        let input = InputPort::create();
        let (tx, rx) = channel(100);
        rxs.push(rx);
        items.push(PipeItem::create(
            ProcessorPtr::create(SyncSenderSink::create(tx, input.clone())),
            vec![input],
            vec![],
        ));
    }

    Ok((rxs, Pipe::create(size, 0, items)))
}

/// Returns (input, expected)
pub fn basic_test_data(limit: Option<usize>) -> (Vec<Vec<DataBlock>>, DataBlock) {
    let data = vec![
        vec![vec![1, 2, 3, 4], vec![4, 5, 6, 7]],
        vec![vec![1, 1, 1, 1], vec![1, 10, 100, 2000]],
        vec![vec![0, 2, 4, 5]],
    ];

    prepare_input_and_result(data, limit)
}

fn prepare_input_and_result(
    data: Vec<Vec<Vec<i32>>>,
    limit: Option<usize>,
) -> (Vec<Vec<DataBlock>>, DataBlock) {
    let input = data
        .clone()
        .into_iter()
        .map(|v| {
            v.into_iter()
                .map(|v| DataBlock::new_from_columns(vec![Int32Type::from_data(v)]))
                .collect::<Vec<_>>()
        })
        .collect::<Vec<_>>();
    let result = data
        .into_iter()
        .flatten()
        .flatten()
        .sorted()
        .take(limit.unwrap_or(usize::MAX))
        .collect::<Vec<_>>();
    let result = DataBlock::new_from_columns(vec![Int32Type::from_data(result)]);

    (input, result)
}

fn check_result(result: Vec<DataBlock>, expected: DataBlock) {
    if expected.is_empty() {
        if !result.is_empty() && !DataBlock::concat(&result).unwrap().is_empty() {
            panic!(
                "\nexpected empty block, but got:\n {}",
                pretty_format_blocks(&result).unwrap()
            )
        }
        return;
    }

    let result_rows: usize = result.iter().map(|v| v.num_rows()).sum();
    let result = pretty_format_blocks(&result).unwrap();
    let expected_rows = expected.num_rows();
    let expected = pretty_format_blocks(&[expected]).unwrap();
    assert_eq!(
        expected, result,
        "\nexpected (num_rows = {}):\n{}\nactual (num_rows = {}):\n{}",
        expected_rows, expected, result_rows, result
    );
}

fn random_test_data(
    rng: &mut ThreadRng,
    with_limit: bool,
) -> (Vec<Vec<DataBlock>>, DataBlock, Option<usize>) {
    let random_batch_size = rng.gen_range(1..=10);
    let random_num_streams = rng.gen_range(5..=10);

    let random_data = (0..random_num_streams)
        .map(|_| {
            let random_num_blocks = rng.gen_range(1..=10);
            let mut data = (0..random_batch_size * random_num_blocks)
                .map(|_| rng.gen_range(0..=1000))
                .collect::<Vec<_>>();
            data.sort();
            data.chunks(random_batch_size)
                .map(|v| v.to_vec())
                .collect::<Vec<_>>()
        })
        .collect::<Vec<_>>();

    let num_rows = random_data
        .iter()
        .map(|v| v.iter().map(|v| v.len()).sum::<usize>())
        .sum::<usize>();
    let limit = if with_limit {
        Some(rng.gen_range(0..=num_rows))
    } else {
        None
    };
    let (input, expected) = prepare_input_and_result(random_data, limit);
    (input, expected, limit)
}
