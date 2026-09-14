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
use std::sync::atomic::AtomicBool;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;

use databend_common_base::base::Progress;
use databend_common_expression::DataBlock;
use databend_common_expression::FromData;
use databend_common_expression::types::Int32Type;
use databend_common_pipeline::core::ExecutionInfo;
use databend_common_pipeline::core::Pipeline;
use databend_common_pipeline::core::always_callback;
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

/// `result_rows` must be complete by the time the pipeline finish callback runs,
/// because that callback is what emits the query-finish log.
///
/// The executor treats itself as done once the last blocks are pushed into the
/// bounded pulling channel, so it can finish while the consumer has read nothing
/// at all. Counting rows as the consumer pulls them therefore left the buffered
/// blocks out of `result_rows`, and the log was written with the incomplete
/// value. Counting rows as they are produced closes that window.
///
/// To keep this deterministic rather than timing-dependent, the pipeline emits at
/// most one block per parallel source, which is exactly the channel capacity
/// (`max(1, pipeline.output_len())`). The executor can then run to completion
/// with no consumer at all, so the snapshot below is taken at a well defined
/// point instead of racing with consumption.
async fn assert_result_progress_complete_at_finish(max_threads: u64) -> anyhow::Result<()> {
    let fixture = TestFixture::setup().await?;
    let ctx = fixture.new_query_ctx().await?;

    let parallelism = max_threads as usize;
    // One block per source: fits entirely in the channel, so no consumer needed.
    let expected_rows = parallelism * BLOCK_ROWS;
    let source_blocks = Arc::new(Mutex::new(build_blocks(parallelism)));

    let mut pipeline = Pipeline::create();
    let scan_progress = ctx.get_scan_progress();
    pipeline.add_source(
        |output| BlocksSource::create(scan_progress.clone(), output, source_blocks.clone()),
        parallelism,
    )?;
    pipeline.set_max_threads(parallelism);

    let result_progress = Arc::new(Progress::create());

    // Snapshot the counter exactly where `log_query_finished` would read it.
    let rows_at_finish = Arc::new(AtomicUsize::new(0));
    let finish_fired = Arc::new(AtomicBool::new(false));
    pipeline.set_on_finished(always_callback({
        let result_progress = result_progress.clone();
        let rows_at_finish = rows_at_finish.clone();
        let finish_fired = finish_fired.clone();
        move |_info: &ExecutionInfo| {
            rows_at_finish.store(result_progress.get_values().rows, Ordering::SeqCst);
            finish_fired.store(true, Ordering::SeqCst);
            Ok(())
        }
    }));

    let mut build_res = PipelineBuildResult::create();
    build_res.main_pipeline = pipeline;

    let mut executor = PipelinePullingExecutor::from_pipelines_with_progress(
        build_res,
        executor_settings(max_threads),
        Some(result_progress.clone()),
    )?;

    executor.start();

    // Deliberately do not consume yet: the executor must be able to finish on its
    // own, which is the situation that used to lose rows.
    for _ in 0..100 {
        if finish_fired.load(Ordering::SeqCst) {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }
    assert!(
        finish_fired.load(Ordering::SeqCst),
        "pipeline should finish without a consumer (max_threads={max_threads})"
    );

    assert_eq!(
        rows_at_finish.load(Ordering::SeqCst),
        expected_rows,
        "result_rows must be complete when the finish callback runs (max_threads={max_threads})"
    );

    // The rows are still delivered afterwards, and the counter must not double up.
    let mut pulled_rows = 0;
    while let Some(block) = executor.pull_data().await? {
        pulled_rows += block.num_rows();
    }
    assert_eq!(
        pulled_rows, expected_rows,
        "consumer should receive all rows"
    );
    assert_eq!(
        result_progress.get_values().rows,
        expected_rows,
        "consuming must not count rows twice (max_threads={max_threads})"
    );

    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn test_result_progress_complete_at_finish_single_thread() -> anyhow::Result<()> {
    assert_result_progress_complete_at_finish(1).await
}

#[tokio::test(flavor = "multi_thread")]
async fn test_result_progress_complete_at_finish_multi_thread() -> anyhow::Result<()> {
    // The lost amount used to scale with parallelism, since the channel capacity
    // is the pipeline parallelism.
    for max_threads in [2, 4, 8] {
        assert_result_progress_complete_at_finish(max_threads).await?;
    }
    Ok(())
}
