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

use databend_common_base::base::SpillProgress;
use databend_common_exception::Result;
use databend_common_expression::DataBlock;
use databend_common_expression::ScalarRef;
use databend_common_expression::types::NumberScalar;
use databend_query::sessions::QueryContext;
use databend_query::sessions::TableContextQueryIdentity;
use databend_query::sessions::TableContextSettings;
use databend_query::test_kits::TestFixture;
use databend_query::test_kits::config_with_spill;
use databend_query::test_kits::execute_query;
use futures_util::TryStreamExt;

fn scalar_as_u64(block: &DataBlock, column: usize) -> u64 {
    match block.get_by_offset(column).index(0).unwrap() {
        ScalarRef::Number(NumberScalar::UInt64(v)) => v,
        ScalarRef::Number(NumberScalar::Int64(v)) => v as u64,
        other => panic!("unexpected scalar at column {column}: {other:?}"),
    }
}

async fn run_materialized_cte_query(ctx: Arc<QueryContext>) -> Result<(u64, u64)> {
    let stream = execute_query(
        ctx,
        "WITH t AS (
             SELECT number AS a FROM numbers(64)
         )
         SELECT count(), sum(x.a + y.a)
         FROM t AS x
         INNER JOIN t AS y ON x.a = y.a",
    )
    .await?;
    let blocks = stream.try_collect::<Vec<DataBlock>>().await?;
    let block = DataBlock::concat(&blocks)?;
    assert_eq!(block.num_rows(), 1);
    Ok((scalar_as_u64(&block, 0), scalar_as_u64(&block, 1)))
}

async fn run_zero_column_materialized_cte_query(ctx: Arc<QueryContext>) -> Result<u64> {
    let stream = execute_query(
        ctx,
        "WITH t AS (
             SELECT number AS a FROM numbers(64)
         )
         SELECT count()
         FROM t AS x
         INNER JOIN t AS y ON true",
    )
    .await?;
    let blocks = stream.try_collect::<Vec<DataBlock>>().await?;
    let block = DataBlock::concat(&blocks)?;
    assert_eq!(block.num_rows(), 1);
    Ok(scalar_as_u64(&block, 0))
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_materialized_cte_forced_spill_preserves_results_and_records_progress() -> Result<()> {
    let config = config_with_spill();
    let fixture = TestFixture::setup_with_config(&config).await?;
    let ctx = fixture.new_query_ctx().await?;
    let settings = ctx.get_settings();
    settings.set_setting("enable_auto_materialize_cte".into(), "1".into())?;
    settings.set_setting("force_materialized_cte_spill".into(), "1".into())?;

    let (row_count, sum_value) = run_materialized_cte_query(ctx.clone()).await?;
    assert_eq!(row_count, 64);
    assert_eq!(sum_value, 4032);

    let SpillProgress { file_nums, bytes } = ctx.get_total_spill_progress();
    assert!(
        file_nums > 0,
        "forced MaterializedCTE spill should write spill files"
    );
    assert!(
        bytes > 0,
        "forced MaterializedCTE spill should record bytes"
    );

    let _ = databend_storages_common_cache::TempDirManager::instance()
        .drop_disk_spill_dir(&ctx.get_id());
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_materialized_cte_forced_spill_preserves_zero_column_row_count() -> Result<()> {
    let config = config_with_spill();
    let fixture = TestFixture::setup_with_config(&config).await?;
    let ctx = fixture.new_query_ctx().await?;
    let settings = ctx.get_settings();
    settings.set_setting("enable_auto_materialize_cte".into(), "1".into())?;
    settings.set_setting("force_materialized_cte_spill".into(), "1".into())?;

    let row_count = run_zero_column_materialized_cte_query(ctx.clone()).await?;
    assert_eq!(row_count, 4096);

    let _ = databend_storages_common_cache::TempDirManager::instance()
        .drop_disk_spill_dir(&ctx.get_id());
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_materialized_cte_spill_can_be_disabled_by_memory_ratio() -> Result<()> {
    let config = config_with_spill();
    let fixture = TestFixture::setup_with_config(&config).await?;
    let ctx = fixture.new_query_ctx().await?;
    let settings = ctx.get_settings();
    settings.set_setting("enable_auto_materialize_cte".into(), "1".into())?;
    settings.set_setting("materialized_cte_spilling_memory_ratio".into(), "0".into())?;
    settings.set_setting("force_materialized_cte_spill".into(), "0".into())?;

    let (row_count, sum_value) = run_materialized_cte_query(ctx.clone()).await?;
    assert_eq!(row_count, 64);
    assert_eq!(sum_value, 4032);

    let SpillProgress { file_nums, bytes } = ctx.get_total_spill_progress();
    assert_eq!(file_nums, 0);
    assert_eq!(bytes, 0);

    Ok(())
}
