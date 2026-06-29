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
use databend_common_expression::types::NumberDataType;
use databend_common_expression::types::NumberScalar;
use databend_common_functions::BUILTIN_FUNCTIONS;
use databend_common_storages_fuse::FuseBlockPartInfo;
use databend_common_storages_fuse::io::BlockReader;
use databend_common_storages_fuse::io::DataItem;
use databend_common_storages_fuse::io::WriteSettings;
use databend_common_storages_fuse::io::serialize_block;
use databend_common_storages_fuse::operations::ReadState;
use databend_query::sessions::TableContext;
use databend_query::sessions::TableContextRuntimeFilter;
use databend_query::test_kits::TestFixture;
use databend_storages_common_pruner::BlockMetaIndex;
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

#[tokio::test(flavor = "multi_thread")]
async fn test_prewhere_split_deserialize() -> Result<()> {
    let PrewhereTestSetup {
        _fixture,
        ctx,
        prewhere_info,
        block_reader,
        column_chunks,
        part,
        ..
    } = prepare_prewhere_data().await?;
    let _ = _fixture;

    ctx.set_runtime_filter(HashMap::new());
    ctx.get_settings().set_setting(
        "prewhere_selectivity_threshold".to_string(),
        "50".to_string(),
    )?;

    let read_state = ReadState::create(ctx, 1000, Some(&prewhere_info), block_reader)?;
    assert!(!read_state.use_single_prewhere_reader);

    let pre_columns_chunks = filter_chunks(&column_chunks, &read_state.pre_column_ids);
    let remain_columns_chunks = filter_chunks(&column_chunks, &read_state.remain_column_ids);

    let prewhere_result = read_state.deserialize_prewhere(pre_columns_chunks, &part)?;
    assert_eq!(prewhere_result.preread_block.num_rows(), 1);
    assert!(prewhere_result.row_selection.is_some());
    assert!(prewhere_result.bitmap_selection.is_some());
    assert!(prewhere_result.push_down_row_selection);

    let data_block = read_state.deserialize_remaining(
        prewhere_result.preread_block,
        remain_columns_chunks,
        &part,
        prewhere_result.row_selection.as_ref(),
        prewhere_result.bitmap_selection.as_ref(),
        prewhere_result.push_down_row_selection,
    )?;

    assert_result_row(&data_block, 3, 30, 300, 3000, 7);
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn test_prewhere_split_deserialize_empty_selection() -> Result<()> {
    let PrewhereTestSetup {
        _fixture,
        ctx,
        mut prewhere_info,
        block_reader,
        column_chunks,
        part,
        ..
    } = prepare_prewhere_data().await?;
    let _ = _fixture;

    ctx.set_runtime_filter(HashMap::new());
    ctx.get_settings().set_setting(
        "prewhere_selectivity_threshold".to_string(),
        "50".to_string(),
    )?;

    prewhere_info.filter = build_filter_eq("z", 999)?.as_remote_expr();
    let read_state = ReadState::create(ctx, 1001, Some(&prewhere_info), block_reader)?;

    let pre_columns_chunks = filter_chunks(&column_chunks, &read_state.pre_column_ids);
    let prewhere_result = read_state.deserialize_prewhere(pre_columns_chunks, &part)?;
    let row_selection = prewhere_result.row_selection.as_ref().unwrap();
    assert_eq!(row_selection.selected_rows, 0);
    assert!(prewhere_result.push_down_row_selection);

    let data_block = read_state.deserialize_remaining(
        prewhere_result.preread_block,
        HashMap::new(),
        &part,
        prewhere_result.row_selection.as_ref(),
        prewhere_result.bitmap_selection.as_ref(),
        prewhere_result.push_down_row_selection,
    )?;

    assert_eq!(data_block.num_rows(), 0);
    assert_eq!(data_block.num_columns(), 5);
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn test_runtime_filter_only_split_deserialize() -> Result<()> {
    let PrewhereTestSetup {
        _fixture,
        ctx,
        block_reader,
        column_chunks,
        part,
        ..
    } = prepare_prewhere_data().await?;
    let _ = _fixture;

    ctx.get_settings().set_setting(
        "prewhere_selectivity_threshold".to_string(),
        "50".to_string(),
    )?;

    let read_state = ReadState::create(ctx, 999, None, block_reader)?;
    assert!(!read_state.use_single_prewhere_reader);
    assert!(read_state.filters.is_none());
    assert_eq!(read_state.runtime_filters.len(), 2);

    let pre_columns_chunks = filter_chunks(&column_chunks, &read_state.pre_column_ids);
    let remain_columns_chunks = filter_chunks(&column_chunks, &read_state.remain_column_ids);
    assert_eq!(pre_columns_chunks.len(), 2);
    assert_eq!(remain_columns_chunks.len(), 3);

    let prewhere_result = read_state.deserialize_prewhere(pre_columns_chunks, &part)?;
    assert_eq!(prewhere_result.preread_block.num_rows(), 1);
    assert!(prewhere_result.row_selection.is_some());
    assert!(prewhere_result.bitmap_selection.is_some());
    assert!(prewhere_result.push_down_row_selection);

    let data_block = read_state.deserialize_remaining(
        prewhere_result.preread_block,
        remain_columns_chunks,
        &part,
        prewhere_result.row_selection.as_ref(),
        prewhere_result.bitmap_selection.as_ref(),
        prewhere_result.push_down_row_selection,
    )?;

    assert_result_row(&data_block, 3, 30, 300, 3000, 7);
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn test_page_range_without_prewhere() -> Result<()> {
    let PrewhereTestSetup {
        _fixture,
        ctx,
        block_reader,
        column_chunks,
        part,
        ..
    } = prepare_prewhere_data().await?;
    let _ = _fixture;

    ctx.set_runtime_filter(HashMap::new());
    let read_state = ReadState::create(ctx, 1002, None, block_reader)?;
    let part = with_page_range(part, 1..4, 1);

    let (data_block, row_selection, bitmap_selection) =
        read_state.deserialize_and_filter(column_chunks, &part)?;

    assert_eq!(row_selection.as_ref().unwrap().selected_rows, 3);
    assert_eq!(bitmap_selection.as_ref().unwrap().len(), 5);
    assert_eq!(bitmap_selection.as_ref().unwrap().null_count(), 2);
    assert_i32_column(&data_block, 0, &[2, 3, 4]);
    assert_i32_column(&data_block, 1, &[20, 30, 40]);
    assert_i32_column(&data_block, 2, &[200, 300, 400]);
    assert_i32_column(&data_block, 3, &[2000, 3000, 4000]);
    assert_i32_column(&data_block, 4, &[7, 7, 7]);
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn test_page_range_prewhere_forces_remaining_pushdown() -> Result<()> {
    let PrewhereTestSetup {
        _fixture,
        ctx,
        prewhere_info,
        block_reader,
        column_chunks,
        part,
        ..
    } = prepare_prewhere_data().await?;
    let _ = _fixture;

    ctx.set_runtime_filter(HashMap::new());
    ctx.get_settings().set_setting(
        "prewhere_selectivity_threshold".to_string(),
        "10".to_string(),
    )?;

    let read_state = ReadState::create(ctx, 1003, Some(&prewhere_info), block_reader)?;
    let part = with_page_range(part, 1..4, 1);
    let pre_columns_chunks = filter_chunks(&column_chunks, &read_state.pre_column_ids);
    let remain_columns_chunks = filter_chunks(&column_chunks, &read_state.remain_column_ids);

    let prewhere_result = read_state.deserialize_prewhere(pre_columns_chunks, &part)?;
    assert!(prewhere_result.push_down_row_selection);
    assert_eq!(
        prewhere_result
            .row_selection
            .as_ref()
            .unwrap()
            .selected_rows,
        1
    );

    let data_block = read_state.deserialize_remaining(
        prewhere_result.preread_block,
        remain_columns_chunks,
        &part,
        prewhere_result.row_selection.as_ref(),
        prewhere_result.bitmap_selection.as_ref(),
        prewhere_result.push_down_row_selection,
    )?;

    assert_result_row(&data_block, 3, 30, 300, 3000, 7);
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
    } = prepare_prewhere_data().await?;
    let _ = _fixture;

    ctx.get_settings().set_setting(
        "prewhere_selectivity_threshold".to_string(),
        selectivity_threshold.to_string(),
    )?;

    // Create ReadState which combines prewhere and runtime filter logic
    let read_state = ReadState::create(
        ctx.clone(),
        scan_id,
        Some(&prewhere_info),
        block_reader.clone(),
    )?;
    assert_eq!(read_state.use_single_prewhere_reader, expect_single_reader);

    // Use the new unified API that handles all states internally
    let (data_block, row_selection, bitmap_selection) =
        read_state.deserialize_and_filter(column_chunks.clone(), &part)?;

    // Verify the final data_block (all columns in order: x, y, z, d, e)
    // The new API internally handles all filter stages (prewhere + runtime filters)
    {
        assert_result_row(&data_block, 3, 30, 300, 3000, 7);
    }

    // Verify the bitmap_selection is present and has the correct format
    {
        assert!(row_selection.is_some());
        assert!(bitmap_selection.is_some());
        let bitmap = bitmap_selection.unwrap();
        // Row 2 (x=3, y=30, z=300, d=3000) should be the only one selected
        // Bitmap should reflect the filtering result
        assert_eq!(bitmap.len(), num_rows);
        assert_eq!(bitmap.null_count(), num_rows - 1); // 4 out of 5 rows filtered out
    }

    Ok(())
}

fn assert_result_row(
    data_block: &DataBlock,
    expected_x: i32,
    expected_y: i32,
    expected_z: i32,
    expected_d: i32,
    expected_e: i32,
) {
    assert_eq!(data_block.num_rows(), 1);
    assert_eq!(data_block.num_columns(), 5);

    assert_i32_column(data_block, 0, &[expected_x]);
    assert_i32_column(data_block, 1, &[expected_y]);
    assert_i32_column(data_block, 2, &[expected_z]);
    assert_i32_column(data_block, 3, &[expected_d]);
    assert_i32_column(data_block, 4, &[expected_e]);
}

fn assert_i32_column(data_block: &DataBlock, offset: usize, expected: &[i32]) {
    let values: Vec<i32> =
        Int32Type::try_downcast_column(&data_block.get_by_offset(offset).to_column())
            .unwrap()
            .iter()
            .copied()
            .collect();
    assert_eq!(values, expected);
}

fn with_page_range(
    mut part: FuseBlockPartInfo,
    range: std::ops::Range<usize>,
    page_size: usize,
) -> FuseBlockPartInfo {
    part.block_meta_index = Some(BlockMetaIndex {
        range: Some(range),
        page_size,
        block_location: part.location.clone(),
        ..Default::default()
    });
    part
}

fn filter_chunks(
    column_chunks: &HashMap<ColumnId, DataItem<'static>>,
    column_ids: &std::collections::HashSet<ColumnId>,
) -> HashMap<ColumnId, DataItem<'static>> {
    column_chunks
        .iter()
        .filter(|(column_id, _)| column_ids.contains(column_id))
        .map(|(column_id, data_item)| (*column_id, data_item.clone()))
        .collect()
}

fn build_filter_eq(column_name: &str, value: i32) -> Result<Expr<String>> {
    let column_expr: Expr<String> = Expr::ColumnRef(ColumnRef {
        span: None,
        id: column_name.to_string(),
        data_type: DataType::Number(NumberDataType::Int32),
        display_name: column_name.to_string(),
    });
    let const_expr: Expr<String> = Expr::Constant(Constant {
        span: None,
        scalar: Scalar::Number(NumberScalar::Int32(value)),
        data_type: DataType::Number(NumberDataType::Int32),
    });

    check_function(
        None,
        "eq",
        &[],
        &[column_expr, const_expr],
        &BUILTIN_FUNCTIONS,
    )
}

fn create_bloom_filter_for_int32(values: &[i32]) -> Sbbf {
    let column = Int32Type::from_data(values.to_vec());
    let data_type = column.data_type();
    let num_rows = column.len();
    let method =
        DataBlock::choose_hash_method_with_types(std::slice::from_ref(&data_type)).unwrap();
    let entries = &[column.into()];
    let group_columns = entries.into();
    let mut hashes = Vec::with_capacity(num_rows);
    hash_by_method_for_bloom(&method, group_columns, num_rows, &mut hashes).unwrap();

    let mut filter = Sbbf::new_with_ndv_fpp(values.len() as u64, 0.01).unwrap();
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
        false,
    )?;

    // Extract column chunks from parquet bytes
    let column_chunks = extract_column_chunks(&parquet_bytes, &column_metas)?;

    // Construct FuseBlockPartInfo for deserialize_and_apply_prewhere
    let part = FuseBlockPartInfo {
        location: "test_block".to_string(),
        bloom_filter_index_location: None,
        bloom_filter_index_size: 0,
        spatial_index_location: None,
        spatial_index_size: 0,
        create_on: None,
        nums_rows: num_rows,
        columns_meta: column_metas.clone(),
        columns_stat: None,
        spatial_stats: None,
        compression,
        sort_min_max: None,
        cluster_stats: None,
        preserve_order_stream: None,
        block_meta_index: None,
    };

    let bloom_y = create_bloom_filter_for_int32(&[30]);
    let bloom_d = create_bloom_filter_for_int32(&[3000]);

    let scan_id = 999;
    let mut filters = HashMap::new();
    filters.insert(scan_id, RuntimeFilterInfo {
        filters: vec![
            RuntimeFilterEntry {
                id: 0,
                probe_expr: Expr::Constant(Constant {
                    span: None,
                    scalar: Scalar::Null,
                    data_type: DataType::Null,
                }),
                bloom: Some(RuntimeFilterBloom {
                    column_name: "y".to_string(),
                    filter: Arc::new(bloom_y),
                }),
                spatial: None,
                inlist: None,
                inlist_value_count: 0,
                min_max: None,
                stats: Arc::new(RuntimeFilterStats::default()),
                build_rows: 1,
                build_table_rows: None,
                enabled: true,
            },
            RuntimeFilterEntry {
                id: 1,
                probe_expr: Expr::Constant(Constant {
                    span: None,
                    scalar: Scalar::Null,
                    data_type: DataType::Null,
                }),
                bloom: Some(RuntimeFilterBloom {
                    column_name: "d".to_string(),
                    filter: Arc::new(bloom_d),
                }),
                spatial: None,
                inlist: None,
                inlist_value_count: 0,
                min_max: None,
                stats: Arc::new(RuntimeFilterStats::default()),
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
