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
use databend_common_storages_fuse::TableContext;
use databend_common_storages_fuse::io::BlockReader;
use databend_common_storages_fuse::io::DataItem;
use databend_common_storages_fuse::io::WriteSettings;
use databend_common_storages_fuse::io::serialize_block;
use databend_common_storages_fuse::operations::ReadState;
use databend_query::test_kits::TestFixture;
use databend_storages_common_table_meta::meta::ColumnMeta;
use databend_storages_common_table_meta::meta::Compression;
use opendal::Buffer;

#[tokio::test(flavor = "multi_thread")]
async fn test_prewhere() -> Result<()> {
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

    // Create ReadState which combines prewhere and runtime filter logic
    let read_state = ReadState::create(ctx.clone(), scan_id, Some(&prewhere_info), &block_reader)?;

    // Use the new unified API that handles all states internally
    let (data_block, _row_selection, bitmap_selection) =
        read_state.deserialize_and_filter(column_chunks.clone(), &part)?;

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
    let mut buf = Vec::new();
    let column_metas = serialize_block(&write_settings, &schema, block, &mut buf)?;
    let parquet_bytes = bytes::Bytes::from(buf);

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
        false,
        false,
    )?;

    // Extract column chunks from parquet bytes
    let column_chunks = extract_column_chunks(&parquet_bytes, &column_metas)?;

    // Construct FuseBlockPartInfo for deserialize_and_apply_prewhere
    let part = FuseBlockPartInfo {
        location: "test_block".to_string(),
        create_on: None,
        nums_rows: num_rows,
        columns_meta: column_metas.clone(),
        columns_stat: None,
        compression,
        sort_min_max: None,
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
                inlist: None,
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
                inlist: None,
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
