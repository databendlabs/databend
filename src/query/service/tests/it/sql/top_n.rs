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
use databend_common_catalog::table_context::TableContextRuntimeFilter;
use databend_common_exception::Result;
use databend_common_expression::DataBlock;
use databend_common_expression::Scalar;
use databend_common_expression::ScalarRef;
use databend_common_expression::types::NumberScalar;
use databend_query::sessions::QueryContext;
use databend_query::sessions::TableContextQueryIdentity;
use databend_query::sessions::TableContextSettings;
use databend_query::test_kits::TestFixture;
use databend_query::test_kits::config_with_spill;
use databend_query::test_kits::execute_query;
use futures_util::TryStreamExt;

async fn execute_rows(ctx: Arc<QueryContext>, sql: &str) -> Result<Vec<Vec<Scalar>>> {
    let blocks = execute_query(ctx, sql)
        .await?
        .try_collect::<Vec<DataBlock>>()
        .await?;
    if blocks.is_empty() {
        return Ok(vec![]);
    }
    let block = DataBlock::concat(&blocks)?;
    let mut rows = Vec::with_capacity(block.num_rows());
    for row in 0..block.num_rows() {
        rows.push(
            block
                .columns()
                .iter()
                .map(|column| column.index(row).unwrap().to_owned())
                .collect(),
        );
    }
    Ok(rows)
}

async fn explain_text(ctx: Arc<QueryContext>, sql: &str) -> Result<String> {
    let blocks = execute_query(ctx, &format!("EXPLAIN {sql}"))
        .await?
        .try_collect::<Vec<DataBlock>>()
        .await?;
    let block = DataBlock::concat(&blocks)?;
    Ok((0..block.num_rows())
        .filter_map(|row| match block.get_by_offset(0).index(row) {
            Some(ScalarRef::String(line)) => Some(line.to_string()),
            _ => None,
        })
        .collect::<Vec<_>>()
        .join("\n"))
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_top_n_matches_sort_limit_for_multikey_nulls_and_offset() -> Result<()> {
    let fixture = TestFixture::setup().await?;
    let ctx = fixture.new_query_ctx().await?;
    let settings = ctx.get_settings();
    settings.set_max_threads(4)?;
    settings.set_max_block_size(7)?;
    settings.set_setting("max_push_down_limit".into(), "10000".into())?;
    settings.set_setting("enable_top_n".into(), "1".into())?;

    let query = "SELECT
            if(number % 7 = 0, NULL, to_int64(number % 11)) AS k,
            concat('v', to_string(number % 5)) AS s,
            number
        FROM numbers(200)
        ORDER BY k ASC NULLS LAST, s DESC, number DESC
        LIMIT 17 OFFSET 9";

    for enable_fixed_rows in ["0", "1"] {
        settings.set_setting("enable_fixed_rows_sort".into(), enable_fixed_rows.into())?;

        let explain = explain_text(ctx.clone(), query).await?;
        assert!(
            explain.contains("TopN(Final)"),
            "expected fused final TopN in EXPLAIN, got:\n{}",
            explain
        );
        assert!(
            explain.contains("TopN(Partial)"),
            "expected fused partial TopN in EXPLAIN, got:\n{}",
            explain
        );

        let top_n_rows = execute_rows(ctx.clone(), query).await?;

        // Disabling TopN keeps the existing Sort + Limit path as a differential
        // correctness oracle without disabling ordinary limit pushdown.
        settings.set_setting("enable_top_n".into(), "0".into())?;
        let fallback_explain = explain_text(ctx.clone(), query).await?;
        assert!(
            !fallback_explain.contains("TopN(")
                && fallback_explain.contains("Limit")
                && fallback_explain.contains("Sort"),
            "expected Limit + Sort when TopN is disabled, got:\n{}",
            fallback_explain
        );
        let sort_limit_rows = execute_rows(ctx.clone(), query).await?;
        assert_eq!(top_n_rows, sort_limit_rows);
        assert_eq!(top_n_rows.len(), 17);

        settings.set_setting("enable_top_n".into(), "1".into())?;
    }

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_top_n_forced_spill_restores_candidates_and_records_progress() -> Result<()> {
    let config = config_with_spill();
    let fixture = TestFixture::setup_with_config(&config).await?;
    let ctx = fixture.new_query_ctx().await?;
    let settings = ctx.get_settings();
    settings.set_max_threads(4)?;
    settings.set_max_block_size(31)?;
    settings.set_setting("max_push_down_limit".into(), "10000".into())?;
    settings.set_setting("enable_top_n".into(), "1".into())?;
    settings.set_setting("force_sort_data_spill".into(), "1".into())?;

    let query = "SELECT number
        FROM numbers(4096)
        ORDER BY number DESC
        LIMIT 37 OFFSET 23";
    let rows = execute_rows(ctx.clone(), query).await?;

    assert_eq!(rows.len(), 37);
    for (index, row) in rows.iter().enumerate() {
        assert_eq!(row, &vec![Scalar::Number(NumberScalar::UInt64(
            4095 - 23 - index as u64
        ))]);
    }

    let SpillProgress { file_nums, bytes } = ctx.get_total_spill_progress();
    assert!(file_nums > 0, "forced TopN spill should write spill files");
    assert!(bytes > 0, "forced TopN spill should record spilled bytes");

    let _ = databend_storages_common_cache::TempDirManager::instance()
        .drop_disk_spill_dir(&ctx.get_id());
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_top_n_distributed_plan_has_partial_and_final_stages() -> Result<()> {
    let fixture = TestFixture::setup().await?;
    let ctx = fixture
        .new_query_ctx_with_cluster(
            databend_query::test_kits::ClusterDescriptor::new()
                .with_node_info("node-a", "127.0.0.1:9090", "cluster", "warehouse")
                .with_node_info("node-b", "127.0.0.1:9091", "cluster", "warehouse")
                .with_local_id("node-a"),
        )
        .await?;
    let settings = ctx.get_settings();
    settings.set_max_threads(4)?;
    settings.set_setting("max_push_down_limit".into(), "10000".into())?;
    settings.set_setting("enable_top_n".into(), "1".into())?;

    let explain = explain_text(
        ctx,
        "SELECT number FROM numbers(200) ORDER BY number DESC LIMIT 7 OFFSET 3",
    )
    .await?;

    let final_pos = explain.find("TopN(Final)").unwrap_or_else(|| {
        panic!(
            "expected distributed final TopN in EXPLAIN, got:\n{}",
            explain
        )
    });
    let exchange_pos = explain.find("exchange type: Merge").unwrap_or_else(|| {
        panic!(
            "expected merge exchange in distributed TopN, got:\n{}",
            explain
        )
    });
    let partial_pos = explain.find("TopN(Partial)").unwrap_or_else(|| {
        panic!(
            "expected distributed partial TopN in EXPLAIN, got:\n{}",
            explain
        )
    });
    assert!(
        final_pos < exchange_pos && exchange_pos < partial_pos,
        "expected Final TopN -> Merge exchange -> Partial TopN, got:\n{}",
        explain
    );

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_top_n_preserves_lazy_row_fetch() -> Result<()> {
    let fixture = TestFixture::setup().await?;
    fixture.create_default_database().await?;
    let db = fixture.default_db_name();
    let table = format!("{}.top_n_lazy", db);
    fixture
        .execute_command(&format!(
            "CREATE TABLE {table} (id UInt64, payload String) ENGINE=FUSE"
        ))
        .await?;
    fixture
        .execute_command(&format!(
            "INSERT INTO {table} SELECT number, concat('payload-', to_string(number)) FROM numbers(20)"
        ))
        .await?;

    let ctx = fixture.new_query_ctx().await?;
    let settings = ctx.get_settings();
    settings.set_max_threads(4)?;
    settings.set_setting("lazy_read_threshold".into(), "100".into())?;
    settings.set_setting("max_push_down_limit".into(), "10000".into())?;
    settings.set_setting("enable_top_n".into(), "1".into())?;
    let query = format!("SELECT id, payload FROM {table} ORDER BY id DESC LIMIT 3 OFFSET 2");

    let explain = explain_text(ctx.clone(), &query).await?;
    assert!(
        explain.contains("RowFetch") && explain.contains("TopN(Final)"),
        "expected RowFetch above fused TopN, got:\n{}",
        explain
    );

    let top_n_rows = execute_rows(ctx.clone(), &query).await?;
    assert_eq!(top_n_rows, vec![
        vec![
            Scalar::Number(NumberScalar::UInt64(17)),
            Scalar::String("payload-17".to_string()),
        ],
        vec![
            Scalar::Number(NumberScalar::UInt64(16)),
            Scalar::String("payload-16".to_string()),
        ],
        vec![
            Scalar::Number(NumberScalar::UInt64(15)),
            Scalar::String("payload-15".to_string()),
        ],
    ]);

    settings.set_setting("max_push_down_limit".into(), "0".into())?;
    let sort_limit_rows = execute_rows(ctx, &query).await?;
    assert_eq!(top_n_rows, sort_limit_rows);
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_top_n_runtime_boundary_on_nullable_key_over_multi_block_fuse() -> Result<()> {
    let fixture = TestFixture::setup().await?;
    fixture.create_default_database().await?;
    let db = fixture.default_db_name();
    let table = format!("{}.top_n_nullable", db);
    fixture
        .execute_command(&format!(
            "CREATE TABLE {table} (id UInt64 NULL) ENGINE=FUSE ROW_PER_BLOCK=5"
        ))
        .await?;
    // Two inserts create two segments, so the scan takes the lazy pruning
    // pipeline and exercises the segment reorder stage as well.
    fixture
        .execute_command(&format!(
            "INSERT INTO {table} SELECT number FROM numbers(20)"
        ))
        .await?;
    fixture
        .execute_command(&format!(
            "INSERT INTO {table} SELECT if(number + 20 >= 35, null, number + 20) FROM numbers(20)"
        ))
        .await?;

    let ctx = fixture.new_query_ctx().await?;
    let settings = ctx.get_settings();
    settings.set_max_threads(4)?;
    settings.set_setting("max_push_down_limit".into(), "10000".into())?;
    settings.set_setting("enable_top_n".into(), "1".into())?;

    // Nullable keys go through the row-converter path: the boundary must be
    // published from the source column and respect the null ordering.
    let nulls_last = format!("SELECT id FROM {table} ORDER BY id DESC NULLS LAST LIMIT 3");
    let top_n_rows = execute_rows(ctx.clone(), &nulls_last).await?;
    assert_eq!(top_n_rows, vec![
        vec![Scalar::Number(NumberScalar::UInt64(34))],
        vec![Scalar::Number(NumberScalar::UInt64(33))],
        vec![Scalar::Number(NumberScalar::UInt64(32))],
    ]);

    let nulls_first = format!("SELECT id FROM {table} ORDER BY id ASC NULLS FIRST LIMIT 7");
    let top_n_rows_nulls_first = execute_rows(ctx.clone(), &nulls_first).await?;
    assert_eq!(top_n_rows_nulls_first, vec![
        vec![Scalar::Null],
        vec![Scalar::Null],
        vec![Scalar::Null],
        vec![Scalar::Null],
        vec![Scalar::Null],
        vec![Scalar::Number(NumberScalar::UInt64(0))],
        vec![Scalar::Number(NumberScalar::UInt64(1))],
    ]);

    // The Sort + Limit path is the differential oracle.
    settings.set_setting("enable_top_n".into(), "0".into())?;
    assert_eq!(top_n_rows, execute_rows(ctx.clone(), &nulls_last).await?);
    assert_eq!(
        top_n_rows_nulls_first,
        execute_rows(ctx, &nulls_first).await?
    );
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_aggregate_rank_limit_registers_scan_filter_without_premature_boundary() -> Result<()>
{
    let fixture = TestFixture::setup().await?;
    fixture.create_default_database().await?;
    let db = fixture.default_db_name();
    let table = format!("{}.aggregate_rank_limit", db);
    fixture
        .execute_command(&format!(
            "CREATE TABLE {table} (id UInt64) ENGINE=FUSE ROW_PER_BLOCK=5"
        ))
        .await?;

    // The preferred ASC scan order visits the all-zero block first. It does
    // not contain enough distinct groups to establish a LIMIT 3 boundary.
    fixture
        .execute_command(&format!(
            "INSERT INTO {table} VALUES (0), (0), (0), (0), (0)"
        ))
        .await?;
    fixture
        .execute_command(&format!(
            "INSERT INTO {table} VALUES (1), (2), (3), (4), (5)"
        ))
        .await?;
    fixture
        .execute_command(&format!(
            "INSERT INTO {table} VALUES (6), (7), (8), (9), (10)"
        ))
        .await?;

    let ctx = fixture.new_query_ctx().await?;
    let settings = ctx.get_settings();
    settings.set_max_threads(1)?;
    settings.set_max_block_size(5)?;
    settings.set_setting("max_push_down_limit".into(), "10000".into())?;

    let rows = execute_rows(
        ctx.clone(),
        &format!("SELECT id, count(*) FROM {table} GROUP BY id ORDER BY id LIMIT 3"),
    )
    .await?;
    assert_eq!(rows, vec![
        vec![
            Scalar::Number(NumberScalar::UInt64(0)),
            Scalar::Number(NumberScalar::UInt64(5)),
        ],
        vec![
            Scalar::Number(NumberScalar::UInt64(1)),
            Scalar::Number(NumberScalar::UInt64(1)),
        ],
        vec![
            Scalar::Number(NumberScalar::UInt64(2)),
            Scalar::Number(NumberScalar::UInt64(1)),
        ],
    ]);
    assert!(!ctx.get_runtime_scan_filters(0).is_empty());

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_top_n_in_correlated_scalar_subquery() -> Result<()> {
    let fixture = TestFixture::setup().await?;
    let ctx = fixture.new_query_ctx().await?;
    let settings = ctx.get_settings();
    settings.set_setting("max_push_down_limit".into(), "10000".into())?;
    settings.set_setting("enable_top_n".into(), "1".into())?;

    let rows = execute_rows(
        ctx,
        "SELECT outer_t.a,
            (SELECT inner_t.x
             FROM (SELECT number AS x FROM numbers(5)) AS inner_t
             WHERE inner_t.x > outer_t.a
             ORDER BY inner_t.x ASC
             LIMIT 1 OFFSET 1) AS picked
         FROM (SELECT number AS a FROM numbers(3)) AS outer_t
         ORDER BY outer_t.a",
    )
    .await?;

    assert_eq!(rows, vec![
        vec![
            Scalar::Number(NumberScalar::UInt64(0)),
            Scalar::Number(NumberScalar::UInt64(2)),
        ],
        vec![
            Scalar::Number(NumberScalar::UInt64(1)),
            Scalar::Number(NumberScalar::UInt64(3)),
        ],
        vec![
            Scalar::Number(NumberScalar::UInt64(2)),
            Scalar::Number(NumberScalar::UInt64(4)),
        ],
    ]);
    Ok(())
}
