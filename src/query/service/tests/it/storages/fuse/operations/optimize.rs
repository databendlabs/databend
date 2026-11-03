//  Copyright 2021 Datafuse Labs.
//
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.

use databend_common_expression::DataBlock;
use databend_common_expression::Scalar;
use databend_common_sql::Planner;
use databend_common_storages_fuse::FuseTable;
use databend_query::interpreters::InterpreterFactory;
use databend_query::sessions::TableContextSettings;
use databend_query::sessions::TableContextTableAccess;
use databend_query::test_kits::*;
use databend_storages_common_cache::CachedItem;
use futures_util::TryStreamExt;

#[tokio::test(flavor = "multi_thread")]
async fn test_fuse_table_optimize() -> anyhow::Result<()> {
    let fixture = TestFixture::setup().await?;
    let ctx = fixture.new_query_ctx().await?;
    let tbl_name = fixture.default_table_name();
    let db_name = fixture.default_db_name();

    fixture.create_default_database().await?;
    fixture.create_normal_table().await?;

    // insert 5 times
    let n = 5;
    for _ in 0..n {
        let table = fixture.latest_default_table().await?;
        let num_blocks = 1;
        let stream = TestFixture::gen_sample_blocks_stream(num_blocks, 1);

        let blocks = stream.try_collect().await?;
        fixture
            .append_commit_blocks(table.clone(), blocks, false, true)
            .await?;
    }

    // there will be 5 blocks
    let table = fixture.latest_default_table().await?;
    let (_, parts) = table.read_partitions(ctx.clone(), None, true).await?;
    assert_eq!(parts.len(), n);

    // do compact
    let query = format!("optimize table {db_name}.{tbl_name} compact");

    let mut planner = Planner::new(ctx.clone());
    let (plan, _) = planner.plan_sql(&query).await?;
    let interpreter = InterpreterFactory::get(ctx.clone(), &plan).await?;

    // `PipelineBuilder` will parallelize the table reading according to value of setting `max_threads`,
    // and `Table::read` will also try to de-queue read jobs preemptively. thus, the number of blocks
    // that `Table::append` takes are not deterministic (`append` is also executed in parallel in this case),
    // therefore, the final number of blocks varies.
    // To avoid flaky test, the value of setting `max_threads` is set to be 1, so that pipeline_builder will
    // only arrange one worker for the `ReadDataSourcePlan`.
    ctx.get_settings().set_max_threads(1)?;
    let data_stream = interpreter.execute(ctx.clone()).await?;
    let _ = data_stream.try_collect::<Vec<_>>().await;

    // verify compaction
    let table = fixture.latest_default_table().await?;
    let (_, parts) = table.read_partitions(ctx.clone(), None, true).await?;
    // blocks are so tiny, they should be compacted into one
    assert_eq!(parts.len(), 1);

    Ok(())
}

async fn layout_rows(fixture: &TestFixture, sql: &str) -> anyhow::Result<Vec<Scalar>> {
    let blocks: Vec<DataBlock> = fixture.execute_query(sql).await?.try_collect().await?;
    Ok(blocks
        .iter()
        .flat_map(|block| {
            (0..block.num_rows())
                .map(move |row| block.get_by_offset(0).index(row).unwrap().to_owned())
        })
        .collect())
}

#[tokio::test(flavor = "multi_thread")]
async fn test_snapshot_segment_layout_uses_partial_observations() -> anyhow::Result<()> {
    let fixture = TestFixture::setup_with_segment_cache_bytes(0).await?;
    for sql in [
        "set enable_auto_analyze = 0",
        "set auto_compaction_imperfect_blocks_threshold = 0",
        "set enable_compact_after_write = 0",
        "set enable_prune_cache = 0",
        "set enable_distributed_pruning = 0",
        "create table learned_segments(k bigint) cluster by(k) block_per_segment=1",
    ] {
        fixture.execute_command(sql).await?;
    }
    for key in 0..16 {
        fixture
            .execute_command(&format!("insert into learned_segments values ({key})"))
            .await?;
    }
    let ctx = fixture.new_query_ctx().await?;
    let table = ctx
        .get_catalog("default")
        .await?
        .get_table(&ctx.get_tenant(), "default", "learned_segments")
        .await?;
    let table = FuseTable::try_from_table(table.as_ref())?;
    let snapshot = table.read_table_snapshot().await?.unwrap();
    let cache_charge = snapshot.mem_bytes();
    assert!(
        snapshot.segment_layout.heap_bytes() > 0
            && cache_charge
                >= std::mem::size_of_val(snapshot.as_ref()) + snapshot.segment_layout.heap_bytes()
    );
    let column_id = snapshot.schema.field_with_name("k")?.column_id();
    let names = snapshot.segments[4..12]
        .iter()
        .map(|location| format!("'{}'", location.0))
        .collect::<Vec<_>>()
        .join(",");
    assert!(
        layout_rows(
            &fixture,
            &format!("select k from learned_segments where _segment_name in ({names}) and k = -1",)
        )
        .await?
        .is_empty()
    );
    let partial = snapshot.segment_layout.runs(column_id).unwrap();
    assert_eq!(partial.len(), 1);
    assert_eq!(partial[0].range, 4..12);
    assert_eq!(snapshot.mem_bytes(), cache_charge);

    // Segment cache is disabled. Missing edges prove partial runs are used;
    // k=15 lies in an unread hole and must still be returned.
    let operator = table.get_operator();
    let mut removed = Vec::new();
    for index in [4, 11] {
        let path = &snapshot.segments[index].0;
        removed.push((path.clone(), operator.read(path).await?.to_vec()));
        operator.delete(path).await?;
    }
    let result = layout_rows(
        &fixture,
        "select k from learned_segments where k = 7 or k = 15 order by k",
    )
    .await;
    for (path, data) in removed {
        operator.write(&path, data).await?;
    }
    assert_eq!(result?, vec![Scalar::from(7_i64), Scalar::from(15_i64)]);
    Ok(())
}
