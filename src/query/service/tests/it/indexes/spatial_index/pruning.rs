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
use std::collections::HashSet;
use std::sync::Arc;

use databend_common_ast::ast::Engine;
use databend_common_catalog::plan::PushDownInfo;
use databend_common_exception::Result;
use databend_common_expression::ColumnId;
use databend_common_expression::DataBlock;
use databend_common_expression::FromData;
use databend_common_expression::TableDataType;
use databend_common_expression::TableField;
use databend_common_expression::TableSchemaRef;
use databend_common_expression::TableSchemaRefExt;
use databend_common_expression::types::GeometryType;
use databend_common_expression::types::NumberDataType;
use databend_common_expression::types::number::UInt64Type;
use databend_common_io::geo_to_wkb;
use databend_common_meta_app::schema::CreateOption;
use databend_common_meta_app::schema::TableIndex;
use databend_common_meta_app::schema::TableIndexType;
use databend_common_sql::BloomIndexColumns;
use databend_common_sql::parse_to_filters;
use databend_common_sql::plans::CreateTablePlan;
use databend_common_storages_fuse::FuseTable;
use databend_common_storages_fuse::pruning::FusePruner;
use databend_common_storages_fuse::pruning::create_segment_location_vector;
use databend_query::interpreters::CreateTableInterpreter;
use databend_query::interpreters::Interpreter;
use databend_query::sessions::QueryContext;
use databend_query::sessions::TableContext;
use databend_query::storages::fuse::FUSE_OPT_KEY_BLOCK_PER_SEGMENT;
use databend_query::storages::fuse::FUSE_OPT_KEY_ROW_PER_BLOCK;
use databend_query::test_kits::*;
use databend_storages_common_pruner::BlockMetaIndex;
use databend_storages_common_table_meta::meta::BlockMeta;
use databend_storages_common_table_meta::meta::TableSnapshot;
use databend_storages_common_table_meta::table::OPT_KEY_DATABASE_ID;
use geo::Geometry;
use geo::Point;
use opendal::Operator;

async fn apply_block_pruning(
    table_snapshot: Arc<TableSnapshot>,
    schema: TableSchemaRef,
    push_down: &Option<PushDownInfo>,
    ctx: Arc<QueryContext>,
    dal: Operator,
    bloom_index_cols: BloomIndexColumns,
    spatial_index_columns: HashSet<ColumnId>,
) -> Result<Vec<(BlockMetaIndex, Arc<BlockMeta>)>> {
    let ctx: Arc<dyn TableContext> = ctx;
    let segment_locs = table_snapshot.segments.clone();
    let segment_locs = create_segment_location_vector(segment_locs, None);

    FusePruner::create(
        &ctx,
        dal,
        schema,
        push_down,
        bloom_index_cols,
        vec![],
        spatial_index_columns,
        None,
    )?
    .read_pruning(segment_locs)
    .await
}

fn point_wkb(x: f64, y: f64) -> Result<Vec<u8>> {
    geo_to_wkb(Geometry::from(Point::new(x, y)))
}

fn build_block(start_id: u64, points: &[(f64, f64)]) -> Result<DataBlock> {
    let ids = (start_id..start_id + points.len() as u64).collect::<Vec<_>>();
    let geoms = points
        .iter()
        .map(|(x, y)| point_wkb(*x, *y))
        .collect::<Result<Vec<_>>>()?;
    Ok(DataBlock::new_from_columns(vec![
        UInt64Type::from_data(ids),
        GeometryType::from_data(geoms),
    ]))
}

fn polygon_wkt(min_x: f64, min_y: f64, max_x: f64, max_y: f64) -> String {
    format!(
        "POLYGON(({} {}, {} {}, {} {}, {} {}, {} {}))",
        min_x, min_y, min_x, max_y, max_x, max_y, max_x, min_y, min_x, min_y
    )
}

#[tokio::test(flavor = "multi_thread")]
async fn test_spatial_index_pruning_geometry() -> anyhow::Result<()> {
    let fixture = TestFixture::setup().await?;
    let ctx = fixture.new_query_ctx().await?;
    fixture.create_default_database().await?;

    let test_tbl_name = "test_spatial_index_pruning";
    let test_schema = TableSchemaRefExt::create(vec![
        TableField::new("id", TableDataType::Number(NumberDataType::UInt64)),
        TableField::new("g", TableDataType::Geometry),
    ]);

    let index_name = "spatial_idx".to_string();
    let mut table_indexes = BTreeMap::new();
    table_indexes.insert(index_name.clone(), TableIndex {
        index_type: TableIndexType::Spatial,
        name: index_name.clone(),
        column_ids: vec![1],
        sync_creation: true,
        version: "v1".to_string(),
        options: BTreeMap::new(),
    });

    let row_per_block = 4;
    let create_table_plan = CreateTablePlan {
        catalog: "default".to_owned(),
        create_option: CreateOption::Create,
        tenant: fixture.default_tenant(),
        database: fixture.default_db_name(),
        table: test_tbl_name.to_string(),
        schema: test_schema.clone(),
        engine: Engine::Fuse,
        engine_options: Default::default(),
        storage_params: None,
        options: [
            (
                FUSE_OPT_KEY_ROW_PER_BLOCK.to_owned(),
                row_per_block.to_string(),
            ),
            (FUSE_OPT_KEY_BLOCK_PER_SEGMENT.to_owned(), "1".to_owned()),
            (OPT_KEY_DATABASE_ID.to_owned(), "1".to_owned()),
        ]
        .into(),
        field_comments: vec![],
        as_select: None,
        cluster_key: None,
        table_indexes: Some(table_indexes),
        table_constraints: None,
        attached_columns: None,
        table_partition: None,
        table_properties: None,
    };

    let interpreter = CreateTableInterpreter::try_create(ctx.clone(), create_table_plan)?;
    let _ = interpreter.execute(ctx.clone()).await?;

    let catalog = ctx.get_catalog("default").await?;
    let table = catalog
        .get_table(
            &fixture.default_tenant(),
            fixture.default_db_name().as_str(),
            test_tbl_name,
        )
        .await?;

    let block_points = vec![
        vec![(0.0, 0.0), (1.0, 0.0), (0.0, 1.0), (1.0, 1.0)],
        vec![(10.0, 10.0), (11.0, 10.0), (10.0, 11.0), (11.0, 11.0)],
        vec![(20.0, 20.0), (21.0, 20.0), (20.0, 21.0), (21.0, 21.0)],
        vec![
            (-10.0, -10.0),
            (-11.0, -10.0),
            (-10.0, -11.0),
            (-11.0, -11.0),
        ],
        vec![
            (100.0, 100.0),
            (101.0, 100.0),
            (100.0, 101.0),
            (101.0, 101.0),
        ],
        vec![(50.0, 0.0), (51.0, 0.0), (50.0, 1.0), (51.0, 1.0)],
        vec![(0.0, 50.0), (1.0, 50.0), (0.0, 51.0), (1.0, 51.0)],
        vec![(-50.0, 0.0), (-51.0, 0.0), (-50.0, 1.0), (-51.0, 1.0)],
    ];

    let mut blocks = Vec::with_capacity(block_points.len());
    let mut next_id = 1;
    for points in block_points {
        blocks.push(build_block(next_id, &points)?);
        next_id += points.len() as u64;
    }

    fixture
        .append_commit_blocks(table.clone(), blocks, false, true)
        .await?;

    let table = catalog
        .get_table(
            &fixture.default_tenant(),
            fixture.default_db_name().as_str(),
            test_tbl_name,
        )
        .await?;

    let fuse_table = FuseTable::create_without_refresh_table_info(
        table.get_table_info().clone(),
        ctx.get_settings().get_s3_storage_class()?,
    )?;
    let snapshot = fuse_table.read_table_snapshot().await?;
    assert!(snapshot.is_some());
    let snapshot = snapshot.unwrap();
    let spatial_index_columns =
        FuseTable::create_spatial_index_columns(&table.get_table_info().meta.indexes);

    let cases = vec![
        (polygon_wkt(9.0, 9.0, 12.0, 12.0), 1),
        (polygon_wkt(-12.0, -12.0, -8.0, -8.0), 1),
        (polygon_wkt(90.0, 90.0, 110.0, 110.0), 1),
        (polygon_wkt(-1.0, -1.0, 2.0, 2.0), 1),
        (polygon_wkt(-1.0, -1.0, 60.0, 2.0), 2),
        (polygon_wkt(200.0, 200.0, 210.0, 210.0), 0),
    ];

    for (wkt, expected_blocks) in cases {
        let filter = format!("st_intersects(g, to_geometry('{wkt}'))");
        let push_down = Some(PushDownInfo {
            filters: Some(parse_to_filters(ctx.clone(), table.clone(), &filter)?),
            ..Default::default()
        });

        let block_metas = apply_block_pruning(
            snapshot.clone(),
            table.get_table_info().schema(),
            &push_down,
            ctx.clone(),
            fuse_table.get_operator(),
            fuse_table.bloom_index_cols(),
            spatial_index_columns.clone(),
        )
        .await?;

        assert_eq!(expected_blocks, block_metas.len());
    }

    Ok(())
}
