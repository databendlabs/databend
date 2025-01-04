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

use std::sync::Arc;

use databend_common_pipeline_transforms::sort::SimpleRowConverter;
use databend_common_pipeline_transforms::sort::SimpleRowsAsc;
use databend_common_pipeline_transforms::TransformPipelineHelper;
use databend_common_pipeline_transforms::TransformSortMerge;
use databend_common_pipeline_transforms::TransformSortMergeBase;
use databend_common_storage::DataOperator;
use databend_query::pipelines::processors::transforms::create_transform_sort_spill;
use databend_query::spillers::Spiller;
use databend_query::spillers::SpillerConfig;
use databend_query::spillers::SpillerType;

use super::*;

fn create_sort_spill_pipeline(
    ctx: Arc<QueryContext>,
    data: Vec<DataBlock>,
    block_size: usize,
    limit: Option<usize>,
) -> Result<(Arc<QueryPipelineExecutor>, Receiver<Result<DataBlock>>)> {
    let mut pipeline = Pipeline::create();

    let data_type = data[0].get_by_offset(0).data_type.clone();
    let source_pipe = create_source_pipe(ctx.clone(), vec![data])?;
    pipeline.add_pipe(source_pipe);

    let schema = DataSchemaRefExt::create(vec![DataField::new("a", data_type)]);
    let sort_desc = Arc::new(vec![SortColumnDescription {
        offset: 0,
        asc: true,
        nulls_first: true,
    }]);

    let order_col_generated = false;
    let output_order_col = false;
    let max_memory_usage = 100;
    let spilling_bytes_threshold_per_core = 1;
    let spilling_batch_bytes = 1000;
    let enable_loser_tree = true;

    pipeline.try_add_accumulating_transformer(|| {
        TransformSortMergeBase::<
            TransformSortMerge<SimpleRowsAsc<Int32Type>>,
            SimpleRowsAsc<Int32Type>,
            SimpleRowConverter<Int32Type>,
        >::try_create(
            schema.clone(),
            sort_desc.clone(),
            order_col_generated,
            output_order_col,
            max_memory_usage,
            spilling_bytes_threshold_per_core,
            spilling_batch_bytes,
            TransformSortMerge::create(
                schema.clone(),
                sort_desc.clone(),
                block_size,
                enable_loser_tree,
            ),
        )
    })?;

    let spill_config = SpillerConfig {
        spiller_type: SpillerType::OrderBy,
        location_prefix: "_sort_spill".to_string(),
        disk_spill: None,
        use_parquet: true,
    };
    let op = DataOperator::instance().spill_operator();
    let spiller = Spiller::create(ctx.clone(), op, spill_config.clone())?;
    pipeline.add_transform(|input, output| {
        Ok(ProcessorPtr::create(create_transform_sort_spill(
            input,
            output,
            schema.clone(),
            sort_desc.clone(),
            limit,
            spiller.clone(),
            true,
            enable_loser_tree,
        )))
    })?;

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

fn basic_test_data(limit: Option<usize>) -> (Vec<DataBlock>, DataBlock) {
    let data = vec![vec![1, 1, 1, 1], vec![1, 2, 3, 4], vec![4, 5, 6, 7]];

    prepare_single_input_and_result(data, limit)
}

fn random_test_data(
    rng: &mut ThreadRng,
    with_limit: bool,
) -> (Vec<DataBlock>, DataBlock, Option<usize>) {
    let num_rows = rng.gen_range(1..=100);
    let mut data = (0..num_rows)
        .map(|_| rng.gen_range(0..1000))
        .collect::<Vec<_>>();
    data.sort();

    let mut data = VecDeque::from(data);
    let mut random_data = Vec::new();
    while !data.is_empty() {
        let n = rng.gen_range(1..=10).min(data.len());
        random_data.push(data.drain(..n).collect::<Vec<_>>());
    }

    let limit = if with_limit {
        Some(rng.gen_range(1..=num_rows))
    } else {
        None
    };
    let (input, expected) = prepare_single_input_and_result(random_data, limit);
    (input, expected, limit)
}

async fn run_fuzz(ctx: Arc<QueryContext>, rng: &mut ThreadRng, with_limit: bool) -> Result<()> {
    let block_size = rng.gen_range(1..=20);
    let (data, expected, limit) = random_test_data(rng, with_limit);

    // println!("\nwith_limit {with_limit}");
    // for (input, block) in data.iter().enumerate() {
    //     println!("intput {input}");
    //     println!("{:?}", block.columns()[0].value);
    // }

    let (executor, mut rx) = create_sort_spill_pipeline(ctx, data, block_size, limit)?;
    executor.execute()?;

    let mut got: Vec<DataBlock> = Vec::new();
    while !rx.is_empty() {
        got.push(rx.recv().await.unwrap()?);
    }

    check_result(got, expected);

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_sort_spill() -> Result<()> {
    let fixture = TestFixture::setup().await?;
    let ctx = fixture.new_query_ctx().await?;

    let block_size = 4;
    let limit = None;
    let (data, expected) = basic_test_data(None);
    let (executor, mut rx) = create_sort_spill_pipeline(ctx, data, block_size, limit)?;
    executor.execute()?;

    let mut got: Vec<DataBlock> = Vec::new();
    while !rx.is_empty() {
        got.push(rx.recv().await.unwrap()?);
    }

    check_result(got, expected);

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_sort_spill_fuzz() -> Result<()> {
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
