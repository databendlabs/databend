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

use std::collections::HashMap;
use std::sync::Arc;

use databend_common_base::base::tokio;
use databend_common_catalog::plan::PrewhereInfo;
use databend_common_catalog::plan::Projection;
use databend_common_catalog::runtime_filter_info::RuntimeFilterBloom;
use databend_common_catalog::runtime_filter_info::RuntimeFilterEntry;
use databend_common_catalog::runtime_filter_info::RuntimeFilterInfo;
use databend_common_catalog::runtime_filter_info::RuntimeFilterStats;
use databend_common_catalog::sbbf::Sbbf;
use databend_common_exception::Result;
use databend_common_expression::Column;
use databend_common_expression::ColumnId;
use databend_common_expression::ColumnRef;
use databend_common_expression::Constant;
use databend_common_expression::DataBlock;
use databend_common_expression::Expr;
use databend_common_expression::FromData;
use databend_common_expression::Scalar;
use databend_common_expression::TableDataType;
use databend_common_expression::TableField;
use databend_common_expression::TableSchema;
use databend_common_expression::TableSchemaRef;
use databend_common_expression::hash_util::hash_by_method_for_bloom;
use databend_common_expression::type_check::check_function;
use databend_common_expression::types::AccessType;
use databend_common_expression::types::DataType;
use databend_common_expression::types::Int32Type;
use databend_common_expression::types::Int64Type;
use databend_common_expression::types::NumberDataType;
use databend_common_expression::types::NumberScalar;
use databend_common_expression::types::StringType;
use databend_common_functions::BUILTIN_FUNCTIONS;
use databend_common_storages_fuse::FuseBlockPartInfo;
use databend_common_storages_fuse::io::BlockReader;
use databend_common_storages_fuse::io::DataItem;
use databend_common_storages_fuse::io::WriteSettings;
use databend_common_storages_fuse::io::serialize_block;
use databend_common_storages_fuse::operations::BlockScanStats;
use databend_common_storages_fuse::operations::ReadState;
use databend_common_storages_fuse::operations::ReadStateBatch;
use databend_common_storages_fuse::operations::ReadStateOutput;
use databend_common_storages_fuse::operations::read::selected_row_offsets;
use databend_query::sessions::TableContext;
use databend_query::sessions::TableContextRuntimeFilter;
use databend_query::test_kits::TestFixture;
use databend_storages_common_cache::CachePolicy;
use databend_storages_common_pruner::BlockMetaIndex;
use databend_storages_common_pruner::IndexMatches;
use databend_storages_common_table_meta::meta::ColumnMeta;
use databend_storages_common_table_meta::meta::Compression;
use opendal::Buffer;

#[tokio::test(flavor = "multi_thread")]
async fn test_prewhere() -> Result<()> {
    run_prewhere_test_with_threshold(50, false).await
}

#[tokio::test(flavor = "multi_thread")]
async fn test_prewhere_without_row_selection_pushdown() -> Result<()> {
    run_prewhere_test_with_threshold(10, false).await
}

#[tokio::test(flavor = "multi_thread")]
async fn test_prewhere_with_single_reader() -> Result<()> {
    run_prewhere_test_with_threshold(0, true).await
}

// Finish filtering the deciding batch, then skip subsequent low-yield applies.
#[tokio::test(flavor = "multi_thread")]
async fn test_low_yield_bloom_skips_second_probe_block() -> Result<()> {
    // Keep the fixture alive until its context and readers have been dropped.
    let PrewhereTestSetup {
        _fixture,
        ctx,
        block_reader,
        scan_id,
        ..
    } = prepare_prewhere_data().await?;
    let _ = _fixture;
    let mut state = ReadState::create(ctx, scan_id, None, block_reader)?;
    state.runtime_filters.truncate(1);
    let filter = &mut state.runtime_filters[0];
    filter.probe_expr = Expr::ColumnRef(ColumnRef {
        span: None,
        id: 0,
        data_type: DataType::Number(NumberDataType::Int32),
        display_name: "probe".into(),
    });
    filter.filter = Arc::new(create_bloom_filter(Int32Type::from_data(vec![1])));
    let stats = filter.stats.clone();

    // Just two distinct values in one repeated primitive column, no large
    // business fixture or SQL. This reaches the production minimum sample
    // using only APIs already present on the parent revision.
    let mut values = vec![1; 1_000_000];
    values[0] = 2;
    let block = DataBlock::new_from_columns(vec![Int32Type::from_data(values)]);
    let bitmap = state.runtime_filter(&block, block.num_rows())?.unwrap();
    assert_eq!(bitmap.null_count(), 1); // Current batch still filters.
    assert_eq!(stats.snapshot().bloom_rows_filtered, 1);
    assert!(state.runtime_filter(&block, block.num_rows())?.is_none());
    assert_eq!(stats.snapshot().bloom_rows_filtered, 1);
    Ok(())
}

// A small sample isolates skip/bitmap behavior from the production threshold.
// The second filter remains active after the first one is disabled.
#[tokio::test(flavor = "multi_thread")]
async fn test_adaptive_bloom_keeps_current_bitmap_and_other_filter() -> Result<()> {
    // Match the existing prewhere tests' fixture/context destruction order.
    let PrewhereTestSetup {
        _fixture,
        ctx,
        block_reader,
        scan_id,
        ..
    } = prepare_prewhere_data().await?;
    let _ = _fixture;
    let mut state = ReadState::create(ctx, scan_id, None, block_reader)?;
    for (filter, value) in state.runtime_filters.iter_mut().zip([1, 2]) {
        filter.probe_expr = Expr::ColumnRef(ColumnRef {
            span: None,
            id: 0,
            data_type: DataType::Number(NumberDataType::Int32),
            display_name: "probe".into(),
        });
        filter.filter = Arc::new(create_bloom_filter(Int32Type::from_data(vec![value])));
        filter.sample_rows = 100;
    }
    let low = state.runtime_filters[0].stats.clone();
    let high = state.runtime_filters[1].stats.clone();
    let mut values = vec![1; 100];
    values[0] = 2;
    let block = DataBlock::new_from_columns(vec![Int32Type::from_data(values)]);
    // The low-yield filter still rejects row 0 in its deciding batch. The
    // high-yield filter rejects every other row: their AND must reject all.
    let bitmap = state.runtime_filter(&block, block.num_rows())?.unwrap();
    assert_eq!(bitmap.null_count(), 100);
    assert!(low.bloom_disabled());
    assert!(!high.bloom_disabled());
    let bitmap = state.runtime_filter(&block, block.num_rows())?.unwrap();
    assert_eq!(bitmap.null_count(), 99);
    assert_eq!(low.snapshot().bloom_rows_checked, 100);
    assert_eq!(low.snapshot().bloom_rows_filtered, 1);
    assert_eq!(high.snapshot().bloom_rows_checked, 200);
    assert_eq!(high.snapshot().bloom_rows_filtered, 198);
    Ok(())
}

async fn run_prewhere_test_with_threshold(
    selectivity_threshold: u64,
    expect_single_reader: bool,
) -> Result<()> {
    let PrewhereTestSetup {
        _fixture,
        ctx,
        prewhere_info,
        block_reader,
        column_chunks,
        part,
        num_rows,
        scan_id,
        runtime_filter_stats,
    } = prepare_prewhere_data().await?;
    let _ = _fixture;

    ctx.get_settings().set_setting(
        "prewhere_selectivity_threshold".to_string(),
        selectivity_threshold.to_string(),
    )?;

    // Create ReadState which combines prewhere and runtime filter logic
    let mut read_state = ReadState::create(
        ctx.clone(),
        scan_id,
        Some(&prewhere_info),
        block_reader.clone(),
    )?;
    assert_eq!(read_state.use_single_prewhere_reader, expect_single_reader);

    // Use the new unified API that handles all states internally
    let ReadStateOutput {
        block: data_block,
        bitmap_selection,
        scan_stats,
    } = read_state.deserialize_and_filter(column_chunks.clone(), &part, false)?;

    // Scan statistics describe the whole block that was read and decoded,
    // not the single row left after prewhere and runtime filters. All five
    // Int32 columns are projected, so the block is 5 rows * 5 * 4 bytes.
    assert_eq!(scan_stats, BlockScanStats {
        rows: num_rows,
        bytes: num_rows * 5 * std::mem::size_of::<i32>(),
    });

    // Static prewhere leaves one row, and both Bloom filters match it. A zero
    // filtered-row count proves Bloom was not evaluated on the four rows that
    // prewhere had already rejected.
    for stats in runtime_filter_stats {
        let snapshot = stats.snapshot();
        assert_eq!(snapshot.bloom_rows_filtered, 0);
        assert_eq!(snapshot.bloom_rows_checked, 1);
    }

    // Verify the final data_block (all columns in order: x, y, z, d, e)
    // The new API internally handles all filter stages (prewhere + runtime filters)
    {
        assert_eq!(data_block.num_rows(), 1); // Only 1 row should pass all filters
        assert_eq!(data_block.num_columns(), 5); // All 5 columns

        // Verify column order matches src_schema: x, y, z, d, e
        let col_x: Vec<i32> =
            Int32Type::try_downcast_column(&data_block.get_by_offset(0).to_column())
                .unwrap()
                .iter()
                .copied()
                .collect();
        assert_eq!(col_x, vec![3]);

        let col_y: Vec<i32> =
            Int32Type::try_downcast_column(&data_block.get_by_offset(1).to_column())
                .unwrap()
                .iter()
                .copied()
                .collect();
        assert_eq!(col_y, vec![30]);

        let col_z: Vec<i32> =
            Int32Type::try_downcast_column(&data_block.get_by_offset(2).to_column())
                .unwrap()
                .iter()
                .copied()
                .collect();
        assert_eq!(col_z, vec![300]);

        let col_d: Vec<i32> =
            Int32Type::try_downcast_column(&data_block.get_by_offset(3).to_column())
                .unwrap()
                .iter()
                .copied()
                .collect();
        assert_eq!(col_d, vec![3000]);

        let col_e: Vec<i32> =
            Int32Type::try_downcast_column(&data_block.get_by_offset(4).to_column())
                .unwrap()
                .iter()
                .copied()
                .collect();
        assert_eq!(col_e, vec![7]);
    }

    // Verify the bitmap_selection is present and has the correct format
    {
        assert!(bitmap_selection.is_some());
        let bitmap = bitmap_selection.unwrap();
        // Row 2 (x=3, y=30, z=300, d=3000) should be the only one selected
        // Bitmap should reflect the filtering result
        assert_eq!(bitmap.len(), num_rows);
        assert_eq!(bitmap.null_count(), num_rows - 1); // 4 out of 5 rows filtered out
    }

    Ok(())
}

// A block the scan outputs whole is decoded in `max_block_size` batches, at least 8192
// rows each, that do not share the pages of the other rows of the block.
#[tokio::test(flavor = "multi_thread")]
async fn test_deserialize_batches() -> Result<()> {
    let fixture = TestFixture::setup().await?;
    let ctx: Arc<dyn TableContext> = fixture.new_query_ctx().await?;
    let schema: TableSchemaRef = Arc::new(TableSchema::new(vec![
        TableField::new("id", TableDataType::Number(NumberDataType::Int64)),
        TableField::new("s", TableDataType::String),
    ]));
    let num_rows = 30_000;
    let ids: Vec<i64> = (0..num_rows as i64).collect();
    // 1000-byte strings: the column spans about thirty 1 MiB pages.
    let strings: Vec<String> = (0..num_rows).map(|i| format!("{i:01000}")).collect();
    let block = DataBlock::new_from_columns(vec![
        Int64Type::from_data(ids.clone()),
        StringType::from_data(strings.clone()),
    ]);
    let write_settings = WriteSettings::default();
    let compression: Compression = write_settings.table_compression.into();
    let (column_metas, buf) = serialize_block(&write_settings, &schema, block)?;
    let column_chunks = extract_column_chunks(&buf.to_bytes(), &column_metas)?;
    let part = FuseBlockPartInfo {
        location: "test_block".to_string(),
        bloom_filter_index_location: None,
        bloom_filter_index_size: 0,
        create_on: None,
        nums_rows: num_rows,
        columns_meta: column_metas,
        columns_stat: None,
        compression,
        sort_min_max: None,
        block_meta_index: None,
        column_groups: vec![],
    };
    let operator = opendal::Operator::via_iter(opendal::Scheme::Memory, [])?;
    let block_reader = BlockReader::create(
        ctx.clone(),
        operator,
        schema,
        Projection::Columns(vec![0, 1]),
        CachePolicy::READ_ONLY,
    )?;
    // No runtime filter is registered for this scan.
    let scan_id = 1000;

    // Batches keep 8192 rows when `max_block_size` is smaller.
    ctx.get_settings()
        .set_setting("max_block_size".to_string(), "1".to_string())?;
    let mut read_state = ReadState::create(ctx.clone(), scan_id, None, block_reader.clone())?;
    let (batches, scan_stats) = read_state
        .deserialize_batches(&column_chunks, &part, true)?
        .expect("a block without scan filters is decoded in batches");
    let whole = read_state
        .deserialize_and_filter(column_chunks.clone(), &part, false)?
        .block;

    assert_eq!(scan_stats.rows, num_rows);
    let batch_blocks: Vec<DataBlock> = batches.iter().map(|b| b.block.clone()).collect();
    assert_eq!(
        batch_blocks
            .iter()
            .map(DataBlock::num_rows)
            .collect::<Vec<_>>(),
        vec![8192, 8192, 8192, 5424]
    );
    let merged_offsets: Vec<u64> = batches
        .iter()
        .flat_map(|b| b.offsets.as_ref().unwrap().iter())
        .collect();
    assert_eq!(merged_offsets, (0..num_rows as u64).collect::<Vec<_>>());
    let string_column = |block: &DataBlock| {
        StringType::try_downcast_column(&block.get_by_offset(1).to_column()).unwrap()
    };
    let merged = DataBlock::concat(&batch_blocks)?;
    let merged_ids: Vec<i64> = Int64Type::try_downcast_column(&merged.get_by_offset(0).to_column())
        .unwrap()
        .iter()
        .copied()
        .collect();
    assert_eq!(merged_ids, ids);
    let merged_strings: Vec<String> = string_column(&merged).iter().map(str::to_string).collect();
    assert_eq!(merged_strings, strings);
    let whole_buffers = string_column(&whole).total_buffer_len();
    for batch in &batch_blocks {
        // A batch keeps its own pages and the pages straddling its boundaries.
        assert!(string_column(batch).total_buffer_len() < whole_buffers / 2);
    }

    // A block of at most `max_block_size` rows stays one block.
    ctx.get_settings()
        .set_setting("max_block_size".to_string(), num_rows.to_string())?;
    let mut read_state = ReadState::create(ctx.clone(), scan_id, None, block_reader)?;
    assert!(
        read_state
            .deserialize_batches(&column_chunks, &part, false)?
            .is_none()
    );
    Ok(())
}

// Prewhere (pushdown, non-pushdown, and single-reader), runtime filters, and index row
// selection decode oversized blocks in batches with independent buffers and exact offsets.
#[tokio::test(flavor = "multi_thread")]
async fn test_deserialize_batches_with_filters() -> Result<()> {
    let fixture = TestFixture::setup().await?;
    let ctx: Arc<dyn TableContext> = fixture.new_query_ctx().await?;
    ctx.get_settings()
        .set_setting("max_block_size".to_string(), "8192".to_string())?;

    let schema: TableSchemaRef = Arc::new(TableSchema::new(vec![
        TableField::new("id", TableDataType::Number(NumberDataType::Int64)),
        TableField::new("flag", TableDataType::Number(NumberDataType::Int32)),
        TableField::new("s", TableDataType::String),
    ]));
    // 30_000 rows => 4 batches of 8192, 8192, 8192, 5424.
    // Clustered flag=1 on rows [0..8192) and [16384..22000) (~46% overall):
    // batch 0 passes 100%, batch 1 passes 0%, batch 2 passes ~69%, batch 3 passes 0%.
    let num_rows = 30_000usize;
    let ids: Vec<i64> = (0..num_rows as i64).collect();
    let flags: Vec<i32> = (0..num_rows)
        .map(|i| i32::from(i < 8192 || (16384..22000).contains(&i)))
        .collect();
    let strings: Vec<String> = (0..num_rows).map(|i| format!("{i:01000}")).collect();
    let block = DataBlock::new_from_columns(vec![
        Int64Type::from_data(ids),
        Int32Type::from_data(flags.clone()),
        StringType::from_data(strings),
    ]);

    let write_settings = WriteSettings::default();
    let compression: Compression = write_settings.table_compression.into();
    let (column_metas, buf) = serialize_block(&write_settings, &schema, block)?;
    let column_chunks = extract_column_chunks(&buf.to_bytes(), &column_metas)?;
    let part = FuseBlockPartInfo {
        location: "test_block_filtered".to_string(),
        bloom_filter_index_location: None,
        bloom_filter_index_size: 0,
        create_on: None,
        nums_rows: num_rows,
        columns_meta: column_metas,
        columns_stat: None,
        compression,
        sort_min_max: None,
        block_meta_index: None,
        column_groups: vec![],
    };

    let operator = opendal::Operator::via_iter(opendal::Scheme::Memory, [])?;
    let flag_col: Expr<String> = Expr::ColumnRef(ColumnRef {
        span: None,
        id: "flag".to_string(),
        data_type: DataType::Number(NumberDataType::Int32),
        display_name: "flag".to_string(),
    });
    let one_lit: Expr<String> = Expr::Constant(Constant {
        span: None,
        scalar: Scalar::Number(NumberScalar::Int32(1)),
        data_type: DataType::Number(NumberDataType::Int32),
    });
    let flag_eq_one = check_function(None, "eq", &[], &[flag_col, one_lit], &BUILTIN_FUNCTIONS)?;

    // Project (id, s) while filtering on `flag` (not projected) and also test projecting `flag`.
    for (output_cols, remain_cols) in [(vec![0, 1, 2], vec![0, 2]), (vec![0, 2], vec![0, 2])] {
        let prewhere_info = PrewhereInfo {
            output_columns: Projection::Columns(output_cols.clone()),
            prewhere_columns: Projection::Columns(vec![1]),
            remain_columns: Projection::Columns(remain_cols),
            filter: flag_eq_one.as_remote_expr(),
            virtual_column_ids: None,
        };
        let block_reader = BlockReader::create(
            ctx.clone(),
            operator.clone(),
            schema.clone(),
            Projection::Columns(output_cols.clone()),
            CachePolicy::READ_ONLY,
        )?;
        let s_col_idx = output_cols.len() - 1;
        let string_column = |b: &DataBlock| {
            StringType::try_downcast_column(&b.get_by_offset(s_col_idx).to_column()).unwrap()
        };

        // Test prewhere modes: pushdown (80), non-pushdown (10), and single-reader (0, which
        // requires `block_reader` to include the prewhere columns as the planner builds it).
        let thresholds: &[u64] = if output_cols.contains(&1) {
            &[80, 10, 0]
        } else {
            &[80, 10]
        };
        for &threshold in thresholds {
            ctx.get_settings().set_setting(
                "prewhere_selectivity_threshold".to_string(),
                threshold.to_string(),
            )?;
            let mut state_batched = ReadState::create(
                ctx.clone(),
                2000,
                Some(&prewhere_info),
                block_reader.clone(),
            )?;
            let (batches, batched_stats) = state_batched
                .deserialize_batches(&column_chunks, &part, true)?
                .expect("deterministic prewhere should decode in batches");

            let mut state_whole = ReadState::create(
                ctx.clone(),
                2000,
                Some(&prewhere_info),
                block_reader.clone(),
            )?;
            let ReadStateOutput {
                block: whole_block,
                bitmap_selection: whole_bitmap,
                scan_stats: whole_stats,
            } = state_whole.deserialize_and_filter(column_chunks.clone(), &part, false)?;

            assert_eq!(batched_stats.rows, whole_stats.rows);
            assert!(
                batches.len() >= 2,
                "expected multiple batches for threshold={threshold}"
            );
            let batch_blocks: Vec<DataBlock> = batches.iter().map(|b| b.block.clone()).collect();
            let merged = DataBlock::concat(&batch_blocks)?;
            assert_eq!(merged.num_rows(), whole_block.num_rows());
            assert_eq!(
                Int64Type::try_downcast_column(&merged.get_by_offset(0).to_column())
                    .unwrap()
                    .iter()
                    .copied()
                    .collect::<Vec<_>>(),
                Int64Type::try_downcast_column(&whole_block.get_by_offset(0).to_column())
                    .unwrap()
                    .iter()
                    .copied()
                    .collect::<Vec<_>>()
            );
            assert_eq!(
                string_column(&merged).iter().collect::<Vec<_>>(),
                string_column(&whole_block).iter().collect::<Vec<_>>()
            );

            let expected_offsets: Vec<u64> = selected_row_offsets(whole_bitmap.as_ref().unwrap())
                .iter()
                .collect();
            let actual_offsets: Vec<u64> = batches
                .iter()
                .flat_map(|ReadStateBatch { offsets, .. }| offsets.as_ref().unwrap().iter())
                .collect();
            assert_eq!(actual_offsets, expected_offsets);

            // Each surviving batch (including batch 0 which passed 100% zero-copy) must hold
            // only its own pages rather than pinning the whole 30 MiB block.
            let full_block_bytes = num_rows * 1000;
            for batch in &batch_blocks {
                assert!(
                    string_column(batch).total_buffer_len() < full_block_bytes / 2,
                    "threshold={threshold}: batch pinned buffers outside its own pages"
                );
            }
            let batched_bufs: usize = batch_blocks
                .iter()
                .map(|b| string_column(b).total_buffer_len())
                .sum();
            assert!(
                batched_bufs < full_block_bytes * 2 / 3,
                "threshold={threshold}: batched_bufs={batched_bufs} should not pin dropped batches (full={full_block_bytes})"
            );
        }
    }

    // Runtime filter + prewhere combined.
    let kept_ids: Vec<i64> = (0..num_rows as i64)
        .filter(|&i| (i < 8192 || (16384..22000).contains(&i)) && i % 2 == 0)
        .collect();
    let bloom = create_bloom_filter(Int64Type::from_data(kept_ids.clone()));
    let probe_id = Expr::ColumnRef(ColumnRef {
        span: None,
        id: "id".to_string(),
        data_type: DataType::Number(NumberDataType::Int64),
        display_name: "id".to_string(),
    });
    let rf_scan_id = 2001;
    let rf_stats = Arc::new(RuntimeFilterStats::default());
    let mut rf_map = HashMap::new();
    rf_map.insert(rf_scan_id, RuntimeFilterInfo {
        filters: vec![RuntimeFilterEntry {
            id: 0,
            probe_expr: probe_id,
            bloom: Some(RuntimeFilterBloom {
                filter: Arc::new(bloom),
            }),
            inlist: None,
            inlist_value_count: 0,
            min_max: None,
            stats: rf_stats.clone(),
            build_rows: kept_ids.len(),
            build_table_rows: None,
            enabled: true,
        }],
    });
    ctx.set_runtime_filter(rf_map);
    ctx.get_settings().set_setting(
        "prewhere_selectivity_threshold".to_string(),
        "50".to_string(),
    )?;
    let prewhere_info = PrewhereInfo {
        output_columns: Projection::Columns(vec![0, 1, 2]),
        prewhere_columns: Projection::Columns(vec![1]),
        remain_columns: Projection::Columns(vec![0, 2]),
        filter: flag_eq_one.as_remote_expr(),
        virtual_column_ids: None,
    };
    let block_reader = BlockReader::create(
        ctx.clone(),
        operator.clone(),
        schema.clone(),
        Projection::Columns(vec![0, 1, 2]),
        CachePolicy::READ_ONLY,
    )?;
    let mut state_rf = ReadState::create(
        ctx.clone(),
        rf_scan_id,
        Some(&prewhere_info),
        block_reader.clone(),
    )?;
    let (rf_batches, _) = state_rf
        .deserialize_batches(&column_chunks, &part, true)?
        .expect("prewhere + runtime filter should decode in batches");
    let mut state_rf_whole = ReadState::create(
        ctx.clone(),
        rf_scan_id,
        Some(&prewhere_info),
        block_reader.clone(),
    )?;
    let whole_rf = state_rf_whole.deserialize_and_filter(column_chunks.clone(), &part, false)?;
    let rf_blocks: Vec<DataBlock> = rf_batches.iter().map(|b| b.block.clone()).collect();
    let rf_merged = DataBlock::concat(&rf_blocks)?;
    assert_eq!(rf_merged.num_rows(), whole_rf.block.num_rows());
    let rf_offsets: Vec<u64> = rf_batches
        .iter()
        .flat_map(|b| b.offsets.as_ref().unwrap().iter())
        .collect();
    let whole_rf_offsets: Vec<u64> =
        selected_row_offsets(whole_rf.bitmap_selection.as_ref().unwrap())
            .iter()
            .collect();
    assert_eq!(rf_offsets, whole_rf_offsets);

    // Index row selection (`selected_rows`) without prewhere, both pushdown and non-pushdown.
    let matched_rows: Vec<u32> = (0..num_rows as u32)
        .filter(|&i| i < 10_000 || (20_000..25_000).contains(&i))
        .collect();
    let indexed_part = FuseBlockPartInfo {
        location: part.location.clone(),
        bloom_filter_index_location: None,
        bloom_filter_index_size: 0,
        create_on: None,
        nums_rows: num_rows,
        columns_meta: part.columns_meta.clone(),
        columns_stat: None,
        compression,
        sort_min_max: None,
        block_meta_index: Some(BlockMetaIndex {
            segment_idx: 0,
            block_idx: 0,
            range: None,
            block_id: 0,
            block_location: "test_block_filtered".to_string(),
            segment_location: "seg".to_string(),
            snapshot_location: None,
            inverted_index_matches: Some(IndexMatches {
                rows: matched_rows.clone(),
                scores: None,
                rows_only: true,
            }),
            vector_index_matches: None,
            virtual_block_meta: None,
        }),
        column_groups: vec![],
    };
    for threshold in [80u64, 10u64] {
        ctx.get_settings().set_setting(
            "prewhere_selectivity_threshold".to_string(),
            threshold.to_string(),
        )?;
        let mut state_idx = ReadState::create(ctx.clone(), 2002, None, block_reader.clone())?;
        let (idx_batches, _) = state_idx
            .deserialize_batches(&column_chunks, &indexed_part, true)?
            .expect("index selected_rows should decode in batches");
        let actual_offsets: Vec<u64> = idx_batches
            .iter()
            .flat_map(|b| b.offsets.as_ref().unwrap().iter())
            .collect();
        let expected_offsets: Vec<u64> = matched_rows.iter().map(|&r| r as u64).collect();
        assert_eq!(actual_offsets, expected_offsets);
        let idx_blocks: Vec<DataBlock> = idx_batches.iter().map(|b| b.block.clone()).collect();
        let idx_merged = DataBlock::concat(&idx_blocks)?;
        let actual_ids: Vec<i64> =
            Int64Type::try_downcast_column(&idx_merged.get_by_offset(0).to_column())
                .unwrap()
                .iter()
                .copied()
                .collect();
        let expected_ids: Vec<i64> = matched_rows.iter().map(|&r| r as i64).collect();
        assert_eq!(actual_ids, expected_ids);
    }

    // All rows filtered out returns empty batch list while still accounting for scan bytes.
    let two_lit: Expr<String> = Expr::Constant(Constant {
        span: None,
        scalar: Scalar::Number(NumberScalar::Int32(2)),
        data_type: DataType::Number(NumberDataType::Int32),
    });
    let flag_col2: Expr<String> = Expr::ColumnRef(ColumnRef {
        span: None,
        id: "flag".to_string(),
        data_type: DataType::Number(NumberDataType::Int32),
        display_name: "flag".to_string(),
    });
    let flag_eq_two = check_function(None, "eq", &[], &[flag_col2, two_lit], &BUILTIN_FUNCTIONS)?;
    let empty_prewhere = PrewhereInfo {
        output_columns: Projection::Columns(vec![0, 1, 2]),
        prewhere_columns: Projection::Columns(vec![1]),
        remain_columns: Projection::Columns(vec![0, 2]),
        filter: flag_eq_two.as_remote_expr(),
        virtual_column_ids: None,
    };
    let mut state_empty =
        ReadState::create(ctx.clone(), 2003, Some(&empty_prewhere), block_reader)?;
    let (empty_batches, empty_stats) = state_empty
        .deserialize_batches(&column_chunks, &part, true)?
        .expect("all-filtered-out prewhere should still run batched");
    assert!(empty_batches.is_empty());
    assert_eq!(empty_stats.rows, num_rows);
    assert!(empty_stats.bytes > 0);
    Ok(())
}

fn create_bloom_filter(column: Column) -> Sbbf {
    let data_type = column.data_type();
    let num_rows = column.len();
    let method =
        DataBlock::choose_hash_method_with_types(std::slice::from_ref(&data_type)).unwrap();
    let entries = &[column.into()];
    let group_columns = entries.into();
    let mut hashes = Vec::with_capacity(num_rows);
    hash_by_method_for_bloom(&method, group_columns, num_rows, &mut hashes).unwrap();

    let mut filter = Sbbf::new_with_ndv_fpp(num_rows as u64, 0.01).unwrap();
    filter.insert_hash_batch(&hashes);
    filter
}

struct PrewhereTestSetup {
    _fixture: TestFixture,
    ctx: Arc<dyn TableContext>,
    prewhere_info: PrewhereInfo,
    block_reader: Arc<BlockReader>,
    column_chunks: HashMap<ColumnId, DataItem<'static>>,
    part: FuseBlockPartInfo,
    num_rows: usize,
    scan_id: usize,
    runtime_filter_stats: Vec<Arc<RuntimeFilterStats>>,
}

async fn prepare_prewhere_data() -> Result<PrewhereTestSetup> {
    let fixture = TestFixture::setup().await?;
    let ctx = fixture.new_query_ctx().await?;

    // Create schema: x, y, z (prewhere), d-e (remain)
    let schema: TableSchemaRef = Arc::new(TableSchema::new(vec![
        TableField::new("x", TableDataType::Number(NumberDataType::Int32)),
        TableField::new("y", TableDataType::Number(NumberDataType::Int32)),
        TableField::new("z", TableDataType::Number(NumberDataType::Int32)),
        TableField::new("d", TableDataType::Number(NumberDataType::Int32)),
        TableField::new("e", TableDataType::Number(NumberDataType::Int32)),
    ]));

    // Create test data
    let values_x: Vec<i32> = vec![1, 2, 3, 4, 5];
    let values_y: Vec<i32> = vec![10, 20, 30, 40, 50];
    let values_z: Vec<i32> = vec![100, 200, 300, 400, 500];
    let values_d: Vec<i32> = vec![1000, 2000, 3000, 4000, 5000];
    let values_e: Vec<i32> = vec![7, 7, 7, 7, 7];
    let num_rows = values_x.len();

    // Create and serialize DataBlock
    let block = DataBlock::new_from_columns(vec![
        Int32Type::from_data(values_x.clone()),
        Int32Type::from_data(values_y.clone()),
        Int32Type::from_data(values_z.clone()),
        Int32Type::from_data(values_d.clone()),
        Int32Type::from_data(values_e.clone()),
    ]);

    let write_settings = WriteSettings::default();
    let compression: Compression = write_settings.table_compression.into();
    let (column_metas, buf) = serialize_block(&write_settings, &schema, block)?;
    let parquet_bytes = buf.to_bytes();

    // Create operator (memory-based for testing)
    let operator = opendal::Operator::via_iter(opendal::Scheme::Memory, [])?;

    // Build filter expression: z == 300 AND x > 2 AND y < 40
    let col_x_expr: Expr<String> = Expr::ColumnRef(ColumnRef {
        span: None,
        id: "x".to_string(),
        data_type: DataType::Number(NumberDataType::Int32),
        display_name: "x".to_string(),
    });
    let col_y_expr: Expr<String> = Expr::ColumnRef(ColumnRef {
        span: None,
        id: "y".to_string(),
        data_type: DataType::Number(NumberDataType::Int32),
        display_name: "y".to_string(),
    });
    let col_z_expr: Expr<String> = Expr::ColumnRef(ColumnRef {
        span: None,
        id: "z".to_string(),
        data_type: DataType::Number(NumberDataType::Int32),
        display_name: "z".to_string(),
    });

    let const_2_expr: Expr<String> = Expr::Constant(Constant {
        span: None,
        scalar: Scalar::Number(NumberScalar::Int32(2)),
        data_type: DataType::Number(NumberDataType::Int32),
    });
    let const_40_expr: Expr<String> = Expr::Constant(Constant {
        span: None,
        scalar: Scalar::Number(NumberScalar::Int32(40)),
        data_type: DataType::Number(NumberDataType::Int32),
    });
    let const_300_expr: Expr<String> = Expr::Constant(Constant {
        span: None,
        scalar: Scalar::Number(NumberScalar::Int32(300)),
        data_type: DataType::Number(NumberDataType::Int32),
    });

    let filter_x = check_function(
        None,
        "gt",
        &[],
        &[col_x_expr, const_2_expr],
        &BUILTIN_FUNCTIONS,
    )?;
    let filter_y = check_function(
        None,
        "lt",
        &[],
        &[col_y_expr, const_40_expr],
        &BUILTIN_FUNCTIONS,
    )?;
    let filter_z = check_function(
        None,
        "eq",
        &[],
        &[col_z_expr, const_300_expr],
        &BUILTIN_FUNCTIONS,
    )?;

    let filter_zy = check_function(
        None,
        "and_filters",
        &[],
        &[filter_z, filter_y],
        &BUILTIN_FUNCTIONS,
    )?;
    let filter_expr = check_function(
        None,
        "and_filters",
        &[],
        &[filter_zy, filter_x],
        &BUILTIN_FUNCTIONS,
    )?;

    // Create PrewhereInfo
    let prewhere_info = PrewhereInfo {
        output_columns: Projection::Columns(vec![0, 1, 2, 3, 4]),
        prewhere_columns: Projection::Columns(vec![2, 0, 1]),
        remain_columns: Projection::Columns(vec![3, 4]),
        filter: filter_expr.as_remote_expr(),
        virtual_column_ids: None,
    };

    let block_reader = BlockReader::create(
        ctx.clone(),
        operator.clone(),
        schema.clone(),
        prewhere_info.output_columns.clone(),
        CachePolicy::READ_ONLY,
    )?;

    // Extract column chunks from parquet bytes
    let column_chunks = extract_column_chunks(&parquet_bytes, &column_metas)?;

    // Construct FuseBlockPartInfo for deserialize_and_apply_prewhere
    let part = FuseBlockPartInfo {
        location: "test_block".to_string(),
        bloom_filter_index_location: None,
        bloom_filter_index_size: 0,
        create_on: None,
        nums_rows: num_rows,
        columns_meta: column_metas.clone(),
        columns_stat: None,
        compression,
        sort_min_max: None,
        block_meta_index: None,
        column_groups: vec![],
    };

    let bloom_y_plus_ten = create_bloom_filter(Int64Type::from_data(vec![40]));
    let bloom_d = create_bloom_filter(Int32Type::from_data(vec![3000]));
    let probe_y = Expr::ColumnRef(ColumnRef {
        span: None,
        id: "y".to_string(),
        data_type: DataType::Number(NumberDataType::Int32),
        display_name: "y".to_string(),
    });
    let ten = Expr::Constant(Constant {
        span: None,
        scalar: Scalar::Number(NumberScalar::Int32(10)),
        data_type: DataType::Number(NumberDataType::Int32),
    });
    let probe_y_plus_ten = check_function(None, "plus", &[], &[probe_y, ten], &BUILTIN_FUNCTIONS)?;
    let probe_d = Expr::ColumnRef(ColumnRef {
        span: None,
        id: "d".to_string(),
        data_type: DataType::Number(NumberDataType::Int32),
        display_name: "d".to_string(),
    });

    let scan_id = 999;
    let bloom_y_stats = Arc::new(RuntimeFilterStats::default());
    let bloom_d_stats = Arc::new(RuntimeFilterStats::default());
    let mut filters = HashMap::new();
    filters.insert(scan_id, RuntimeFilterInfo {
        filters: vec![
            RuntimeFilterEntry {
                id: 0,
                probe_expr: probe_y_plus_ten,
                bloom: Some(RuntimeFilterBloom {
                    filter: Arc::new(bloom_y_plus_ten),
                }),
                inlist: None,
                inlist_value_count: 0,
                min_max: None,
                stats: bloom_y_stats.clone(),
                build_rows: 1,
                build_table_rows: None,
                enabled: true,
            },
            RuntimeFilterEntry {
                id: 1,
                probe_expr: probe_d,
                bloom: Some(RuntimeFilterBloom {
                    filter: Arc::new(bloom_d),
                }),
                inlist: None,
                inlist_value_count: 0,
                min_max: None,
                stats: bloom_d_stats.clone(),
                build_rows: 1,
                build_table_rows: None,
                enabled: true,
            },
        ],
    });
    ctx.set_runtime_filter(filters);

    Ok(PrewhereTestSetup {
        _fixture: fixture,
        ctx,
        prewhere_info,
        block_reader,
        column_chunks,
        part,
        num_rows,
        scan_id,
        runtime_filter_stats: vec![bloom_y_stats, bloom_d_stats],
    })
}

/// Extract column chunks from parquet bytes based on column metadata.
fn extract_column_chunks(
    parquet_bytes: &bytes::Bytes,
    column_metas: &HashMap<ColumnId, ColumnMeta>,
) -> Result<HashMap<ColumnId, DataItem<'static>>> {
    let mut chunks = HashMap::new();

    for (column_id, meta) in column_metas {
        let (offset, len) = meta.offset_length();
        let data = parquet_bytes.slice(offset as usize..(offset + len) as usize);
        chunks.insert(*column_id, DataItem::RawData(Buffer::from(data)));
    }

    Ok(chunks)
}
