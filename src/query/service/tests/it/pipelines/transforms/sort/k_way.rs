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

use databend_common_pipeline_transforms::processors::add_k_way_merge_sort;

use super::*;

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
    let sort_desc = [SortColumnDescription {
        offset: 0,
        asc: true,
        nulls_first: true,
    }];
    add_k_way_merge_sort(
        &mut pipeline,
        schema,
        worker,
        block_size,
        limit,
        sort_desc.into(),
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

async fn run_fuzz(ctx: Arc<QueryContext>, rng: &mut ThreadRng, with_limit: bool) -> Result<()> {
    let worker = rng.gen_range(1..=5);
    let block_size = rng.gen_range(1..=20);
    let (data, expected, limit) = random_test_data(rng, with_limit);

    // println!("\nwith_limit {with_limit}");
    // for (input, blocks) in data.iter().enumerate() {
    //     println!("intput {input}");
    //     for b in blocks {
    //         println!("{:?}", b.columns()[0].value);
    //     }
    // }

    let (executor, mut rx) = create_pipeline(ctx, data, worker, block_size, limit)?;
    executor.execute()?;

    let mut got: Vec<DataBlock> = Vec::new();
    while !rx.is_empty() {
        got.push(rx.recv().await.unwrap()?);
    }

    check_result(got, expected);

    Ok(())
}

/// Returns (input, expected)
fn basic_test_data(limit: Option<usize>) -> (Vec<Vec<DataBlock>>, DataBlock) {
    let data = vec![
        vec![vec![1, 2, 3, 4], vec![4, 5, 6, 7]],
        vec![vec![1, 1, 1, 1], vec![1, 10, 100, 2000]],
        vec![vec![0, 2, 4, 5]],
    ];

    prepare_multi_input_and_result(data, limit)
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
    let (input, expected) = prepare_multi_input_and_result(random_data, limit);
    (input, expected, limit)
}

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

    for _ in 0..3 {
        let ctx = fixture.new_query_ctx().await?;
        run_fuzz(ctx, &mut rng, false).await?;
    }

    for _ in 0..3 {
        let ctx = fixture.new_query_ctx().await?;
        run_fuzz(ctx, &mut rng, true).await?;
    }
    Ok(())
}
