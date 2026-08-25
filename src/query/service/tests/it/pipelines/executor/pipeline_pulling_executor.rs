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
use std::sync::Mutex;

use databend_common_base::base::Progress;
use databend_common_expression::DataBlock;
use databend_common_expression::FromData;
use databend_common_expression::types::Int32Type;
use databend_common_pipeline::core::Pipeline;
use databend_common_pipeline::sources::BlocksSource;
use databend_query::pipelines::PipelineBuildResult;
use databend_query::pipelines::executor::ExecutorSettings;
use databend_query::pipelines::executor::PipelinePullingExecutor;
use databend_query::sessions::TableContextProgress;
use databend_query::test_kits::TestFixture;

const BLOCK_ROWS: usize = 100;

fn executor_settings(max_threads: u64) -> ExecutorSettings {
    ExecutorSettings {
        query_id: Arc::new("test_result_progress".to_string()),
        max_execute_time_in_seconds: Default::default(),
        enable_queries_executor: false,
        max_threads,
        executor_node_id: "".to_string(),
        perf_event_groups: vec![],
    }
}

fn build_blocks(block_count: usize) -> VecDeque<DataBlock> {
    (0..block_count)
        .map(|i| {
            let start = (i * BLOCK_ROWS) as i32;
            DataBlock::new_from_columns(vec![Int32Type::from_data(
                (start..start + BLOCK_ROWS as i32).collect::<Vec<i32>>(),
            )])
        })
        .collect()
}

/// `result_rows` must account for every produced row even when the consumer
/// lags behind the executor.
///
/// The pulling channel is bounded by the pipeline parallelism, so the executor
/// can finish (and the query-finish log can fire) while the last blocks are
/// still buffered. Counting on the consumer side lost those blocks, making
/// `result_rows` smaller than the rows actually delivered to the client.
async fn assert_result_progress_counts_all_rows(max_threads: u64) -> anyhow::Result<()> {
    let fixture = TestFixture::setup().await?;
    let ctx = fixture.new_query_ctx().await?;

    // Produce more blocks than the channel can hold so that completion
    // necessarily races with consumption.
    let block_count = max_threads as usize * 4;
    let expected_rows = block_count * BLOCK_ROWS;
    let source_blocks = Arc::new(Mutex::new(build_blocks(block_count)));

    let mut pipeline = Pipeline::create();
    let scan_progress = ctx.get_scan_progress();
    pipeline.add_source(
        |output| BlocksSource::create(scan_progress.clone(), output, source_blocks.clone()),
        max_threads as usize,
    )?;
    pipeline.set_max_threads(max_threads as usize);

    let mut build_res = PipelineBuildResult::create();
    build_res.main_pipeline = pipeline;

    let result_progress = Arc::new(Progress::create());
    let mut executor = PipelinePullingExecutor::from_pipelines_with_progress(
        build_res,
        executor_settings(max_threads),
        Some(result_progress.clone()),
    )?;

    executor.start();

    let mut pulled_rows = 0;
    while let Some(block) = executor.pull_data().await? {
        pulled_rows += block.num_rows();
    }

    assert_eq!(
        pulled_rows, expected_rows,
        "consumer should receive every row (max_threads={max_threads})"
    );
    assert_eq!(
        result_progress.get_values().rows,
        expected_rows,
        "result_progress must match delivered rows (max_threads={max_threads})"
    );

    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn test_result_progress_counts_all_rows_single_thread() -> anyhow::Result<()> {
    assert_result_progress_counts_all_rows(1).await
}

#[tokio::test(flavor = "multi_thread")]
async fn test_result_progress_counts_all_rows_multi_thread() -> anyhow::Result<()> {
    // Before the fix the shortfall grew with parallelism, because the channel
    // capacity equals the pipeline parallelism.
    for max_threads in [2, 4, 8] {
        assert_result_progress_counts_all_rows(max_threads).await?;
    }
    Ok(())
}
