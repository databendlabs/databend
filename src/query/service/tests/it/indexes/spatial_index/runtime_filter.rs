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

use std::collections::BTreeMap;
use std::collections::HashMap;
use std::sync::Arc;

use databend_common_catalog::plan::PartInfoPtr;
use databend_common_catalog::runtime_filter_info::RuntimeFilterEntry;
use databend_common_catalog::runtime_filter_info::RuntimeFilterSpatial;
use databend_common_catalog::runtime_filter_info::RuntimeFilterStats;
use databend_common_exception::Result;
use databend_common_expression::ColumnId;
use databend_common_expression::Constant;
use databend_common_expression::DataBlock;
use databend_common_expression::Expr;
use databend_common_expression::FromData;
use databend_common_expression::Scalar;
use databend_common_expression::TableDataType;
use databend_common_expression::TableField;
use databend_common_expression::TableSchemaRef;
use databend_common_expression::TableSchemaRefExt;
use databend_common_expression::types::DataType;
use databend_common_expression::types::GeometryType;
use databend_common_expression::types::NumberDataType;
use databend_common_expression::types::number::UInt64Type;
use databend_common_io::geometry::geometry_from_str;
use databend_common_meta_app::schema::TableIndex;
use databend_common_meta_app::schema::TableIndexType;
use databend_common_storages_fuse::FuseBlockPartInfo;
use databend_common_storages_fuse::io::SpatialIndexBuilder;
use databend_common_storages_fuse::pruning::SpatialRuntimePruner;
use databend_query::sessions::TableContext;
use databend_query::test_kits::TestFixture;
use databend_storages_common_io::ReadSettings;
use databend_storages_common_table_meta::meta::Compression;
use databend_storages_common_table_meta::meta::Location;
use databend_storages_common_table_meta::meta::SpatialStatistics;
use geo_index::rtree::RTreeBuilder;
use geo_index::rtree::sort::HilbertSort;
use opendal::Operator;

fn build_schema() -> TableSchemaRef {
    TableSchemaRefExt::create(vec![
        TableField::new("id", TableDataType::Number(NumberDataType::UInt64)),
        TableField::new("g", TableDataType::Geometry),
    ])
}

fn build_block() -> Result<DataBlock> {
    let points = vec![
        geometry_from_str("SRID=4326;POINT(0 0)", None)?,
        geometry_from_str("SRID=4326;POINT(1 1)", None)?,
    ];
    Ok(DataBlock::new_from_columns(vec![
        UInt64Type::from_data(vec![1_u64, 2]),
        GeometryType::from_data(points),
    ]))
}

async fn build_spatial_index(
    operator: &Operator,
    schema: TableSchemaRef,
    block: DataBlock,
) -> Result<(Location, u64, HashMap<ColumnId, SpatialStatistics>)> {
    let geom_column_id = schema.fields()[1].column_id();
    let index_name = "spatial_idx".to_string();
    let mut table_indexes = BTreeMap::new();
    table_indexes.insert(index_name.clone(), TableIndex {
        index_type: TableIndexType::Spatial,
        name: index_name,
        column_ids: vec![geom_column_id],
        sync_creation: true,
        version: "v1".to_string(),
        options: BTreeMap::new(),
    });

    let mut builder =
        SpatialIndexBuilder::try_create(&table_indexes, schema.clone(), true).unwrap();
    builder.add_block(&block)?;

    let location: Location = ("spatial_index".to_string(), 0);
    let result = builder.finalize(&location)?;

    let index_state = result.index_state.unwrap();

    operator
        .write(&index_state.location.0, index_state.data.clone())
        .await?;

    Ok((
        index_state.location.clone(),
        index_state.size,
        result.spatial_stats.unwrap_or_default(),
    ))
}

fn build_runtime_filter_entry(
    bounds: [f64; 4],
    column_name: &str,
    srid: i32,
) -> RuntimeFilterEntry {
    let mut builder = RTreeBuilder::<f64>::new(1);
    builder.add(bounds[0], bounds[1], bounds[2], bounds[3]);
    let rtree_bytes = builder.finish::<HilbertSort>().into_inner();

    RuntimeFilterEntry {
        id: 0,
        probe_expr: Expr::Constant(Constant {
            span: None,
            scalar: Scalar::Null,
            data_type: DataType::Null,
        }),
        bloom: None,
        spatial: Some(RuntimeFilterSpatial {
            column_name: column_name.to_string(),
            srid,
            rtrees: Arc::new(rtree_bytes),
            rtree_bounds: Some(bounds),
        }),
        inlist: None,
        inlist_value_count: 0,
        min_max: None,
        stats: Arc::new(RuntimeFilterStats::new()),
        build_rows: 0,
        build_table_rows: None,
        enabled: true,
    }
}

async fn prune_with_bounds(
    schema: TableSchemaRef,
    operator: Operator,
    settings: ReadSettings,
    part: &PartInfoPtr,
    bounds: [f64; 4],
) -> Result<bool> {
    let entry = build_runtime_filter_entry(bounds, "g", 4326);
    let pruner = SpatialRuntimePruner::try_create(schema, operator, settings, &[entry])?.unwrap();
    pruner.prune(part).await
}

#[tokio::test(flavor = "multi_thread")]
async fn test_spatial_runtime_filter_pruner() -> anyhow::Result<()> {
    let fixture = TestFixture::setup().await?;
    let ctx = fixture.new_query_ctx().await?;
    let table_ctx: Arc<dyn TableContext> = ctx.clone();
    let settings = ReadSettings::from_ctx(&table_ctx)?;

    let operator = Operator::new(opendal::services::Memory::default())?.finish();
    let schema = build_schema();
    let block = build_block()?;
    let rows_count = block.num_rows() as u64;
    let (index_location, index_size, spatial_stats) =
        build_spatial_index(&operator, schema.clone(), block).await?;

    let part = FuseBlockPartInfo::create(
        "block".to_string(),
        None,
        0,
        Some(index_location),
        index_size,
        rows_count,
        HashMap::new(),
        None,
        Some(spatial_stats),
        Compression::Lz4Raw,
        None,
        None,
        None,
    );

    let pruned_by_stats = prune_with_bounds(schema.clone(), operator.clone(), settings, &part, [
        10.0, 10.0, 11.0, 11.0,
    ])
    .await?;
    assert!(pruned_by_stats);

    let pruned_by_index = prune_with_bounds(schema.clone(), operator.clone(), settings, &part, [
        0.1, 0.1, 0.2, 0.2,
    ])
    .await?;
    assert!(pruned_by_index);

    let kept_by_index =
        prune_with_bounds(schema, operator, settings, &part, [-0.5, -0.5, 0.5, 0.5]).await?;
    assert!(!kept_by_index);

    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn test_spatial_runtime_filter_pruner_distance_within_bounds() -> anyhow::Result<()> {
    let fixture = TestFixture::setup().await?;
    let ctx = fixture.new_query_ctx().await?;
    let table_ctx: Arc<dyn TableContext> = ctx.clone();
    let settings = ReadSettings::from_ctx(&table_ctx)?;

    let operator = Operator::new(opendal::services::Memory::default())?.finish();
    let schema = build_schema();
    let block = build_block()?;
    let rows_count = block.num_rows() as u64;
    let (index_location, index_size, spatial_stats) =
        build_spatial_index(&operator, schema.clone(), block).await?;

    let part = FuseBlockPartInfo::create(
        "block".to_string(),
        None,
        0,
        Some(index_location),
        index_size,
        rows_count,
        HashMap::new(),
        None,
        Some(spatial_stats),
        Compression::Lz4Raw,
        None,
        None,
        None,
    );

    // Equivalent to building a distance runtime filter from POINT(100 100) with threshold 10.
    let pruned_far = prune_with_bounds(schema.clone(), operator.clone(), settings, &part, [
        90.0, 90.0, 110.0, 110.0,
    ])
    .await?;
    assert!(pruned_far);

    // Equivalent to building a distance runtime filter from POINT(5 5) with threshold 10.
    let kept_near =
        prune_with_bounds(schema, operator, settings, &part, [-5.0, -5.0, 15.0, 15.0]).await?;
    assert!(!kept_near);

    Ok(())
}
