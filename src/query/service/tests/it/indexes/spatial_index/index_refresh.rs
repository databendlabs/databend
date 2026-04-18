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

use databend_common_catalog::table::Table;
use databend_common_catalog::table::TableExt;
use databend_common_exception::Result;
use databend_common_expression::DataBlock;
use databend_common_expression::FromData;
use databend_common_expression::TableDataType;
use databend_common_expression::types::GeometryType;
use databend_common_expression::types::number::UInt64Type;
use databend_common_io::geometry::geometry_from_str;
use databend_common_meta_app::schema::CreateOption;
use databend_common_meta_app::schema::CreateTableIndexReq;
use databend_common_meta_app::schema::TableIndexType;
use databend_common_sql::plans::RefreshTableIndexPlan;
use databend_common_storage::read_parquet_schema_async_rs;
use databend_common_storages_fuse::FuseTable;
use databend_common_storages_fuse::io::MetaReaders;
use databend_query::interpreters::Interpreter;
use databend_query::interpreters::RefreshTableIndexInterpreter;
use databend_query::sessions::QueryContext;
use databend_query::sessions::table_context_ext::*;
use databend_query::test_kits::*;
use databend_storages_common_cache::LoadParams;
use futures_util::TryStreamExt;

fn build_block(rows: &[(u64, &str, &str)]) -> Result<DataBlock> {
    let ids = rows.iter().map(|(id, _, _)| *id).collect::<Vec<_>>();
    let geom1 = rows
        .iter()
        .map(|(_, g1, _)| geometry_from_str(&format!("SRID=4326;{g1}"), None))
        .collect::<Result<Vec<_>>>()?;
    let geom2 = rows
        .iter()
        .map(|(_, _, g2)| geometry_from_str(&format!("SRID=4326;{g2}"), None))
        .collect::<Result<Vec<_>>>()?;

    Ok(DataBlock::new_from_columns(vec![
        UInt64Type::from_data(ids).wrap_nullable(None),
        GeometryType::from_data(geom1).wrap_nullable(None),
        GeometryType::from_data(geom2).wrap_nullable(None),
    ]))
}

fn find_column_id(table: &Arc<dyn Table>, column_name: &str) -> u32 {
    table
        .get_table_info()
        .meta
        .schema
        .fields()
        .iter()
        .find(|field| field.name() == column_name)
        .unwrap()
        .column_id()
}

#[tokio::test(flavor = "multi_thread")]
async fn test_fuse_do_refresh_spatial_index() -> anyhow::Result<()> {
    let fixture = TestFixture::setup().await?;

    fixture
        .default_session()
        .get_settings()
        .set_data_retention_time_in_days(0)?;
    fixture.create_default_database().await?;

    let ctx = fixture.new_query_ctx().await?;
    let catalog = ctx.get_catalog(&fixture.default_catalog_name()).await?;

    let sql = format!(
        "CREATE TABLE {}.{}.{} (
            id UInt64,
            geom1 Geometry,
            geom2 Geometry
        ) ENGINE=FUSE",
        fixture.default_catalog_name(),
        fixture.default_db_name(),
        fixture.default_table_name()
    );
    fixture.execute_command(&sql).await?;

    let table = fixture.latest_default_table().await?;
    let blocks = vec![
        build_block(&[
            (1, "POINT(0 0)", "POINT(100 100)"),
            (2, "POINT(1 1)", "POINT(101 101)"),
            (3, "POINT(2 2)", "POINT(102 102)"),
        ])?,
        build_block(&[
            (4, "POINT(10 10)", "POINT(200 200)"),
            (5, "POINT(11 11)", "POINT(201 201)"),
            (6, "POINT(12 12)", "POINT(202 202)"),
        ])?,
    ];
    fixture
        .append_commit_blocks(table.clone(), blocks, false, true)
        .await?;

    let table = table.refresh(ctx.as_ref()).await?;
    let table_id = table.get_id();
    let tenant = ctx.get_tenant();
    let geom1_column_id = find_column_id(&table, "geom1");
    let geom2_column_id = find_column_id(&table, "geom2");

    let index_name1 = "idx_geom1".to_string();
    let req1 = CreateTableIndexReq {
        create_option: CreateOption::Create,
        table_id,
        tenant: tenant.clone(),
        name: index_name1.clone(),
        column_ids: vec![geom1_column_id],
        sync_creation: false,
        options: BTreeMap::new(),
        index_type: TableIndexType::Spatial,
    };
    catalog.create_table_index(req1).await?;

    let index_name2 = "idx_geom2".to_string();
    let req2 = CreateTableIndexReq {
        create_option: CreateOption::Create,
        table_id,
        tenant,
        name: index_name2.clone(),
        column_ids: vec![geom2_column_id],
        sync_creation: false,
        options: BTreeMap::new(),
        index_type: TableIndexType::Spatial,
    };
    catalog.create_table_index(req2).await?;

    let refresh_index_plan1 = RefreshTableIndexPlan {
        index_type: databend_common_ast::ast::TableIndexType::Spatial,
        catalog: fixture.default_catalog_name(),
        database: fixture.default_db_name(),
        table: fixture.default_table_name(),
        index_name: index_name1.clone(),
        segment_locs: None,
    };
    let interpreter = RefreshTableIndexInterpreter::try_create(ctx.clone(), refresh_index_plan1)?;
    let _ = interpreter
        .execute(ctx.clone())
        .await?
        .try_collect::<Vec<_>>()
        .await?;

    check_index_data(ctx.clone(), table.clone(), vec![index_name1.clone()]).await?;

    let ctx = fixture.new_query_ctx().await?;
    let refresh_index_plan2 = RefreshTableIndexPlan {
        index_type: databend_common_ast::ast::TableIndexType::Spatial,
        catalog: fixture.default_catalog_name(),
        database: fixture.default_db_name(),
        table: fixture.default_table_name(),
        index_name: index_name2.clone(),
        segment_locs: None,
    };
    let interpreter = RefreshTableIndexInterpreter::try_create(ctx.clone(), refresh_index_plan2)?;
    let _ = interpreter
        .execute(ctx.clone())
        .await?
        .try_collect::<Vec<_>>()
        .await?;

    check_index_data(ctx.clone(), table.clone(), vec![index_name1, index_name2]).await?;

    Ok(())
}

async fn check_index_data(
    ctx: Arc<QueryContext>,
    table: Arc<dyn Table>,
    index_names: Vec<String>,
) -> Result<()> {
    let new_table = table.refresh(ctx.as_ref()).await?;
    let table_info = new_table.get_table_info().clone();
    let table_indexes = table_info.meta.indexes.clone();
    let geometry_column_ids = table_info
        .meta
        .schema
        .fields()
        .iter()
        .filter(|field| matches!(field.data_type().remove_nullable(), TableDataType::Geometry))
        .map(|field| field.column_id())
        .collect::<Vec<_>>();

    let new_fuse_table = FuseTable::create_without_refresh_table_info(
        table_info,
        ctx.get_settings().get_s3_storage_class()?,
    )?;

    let snapshot = new_fuse_table.read_table_snapshot().await?.unwrap();
    let dal = new_fuse_table.get_operator_ref();
    let segment_reader =
        MetaReaders::segment_info_reader(new_fuse_table.get_operator(), new_fuse_table.schema());

    let mut block_metas = Vec::new();
    for (segment_loc, ver) in &snapshot.segments {
        let segment_info = segment_reader
            .read(&LoadParams {
                location: segment_loc.to_string(),
                len_hint: None,
                ver: *ver,
                put_cache: false,
            })
            .await?;
        for block_meta in segment_info.block_metas()? {
            block_metas.push(block_meta);
        }
    }

    for block_meta in block_metas {
        assert!(block_meta.spatial_index_location.is_some());
        assert!(block_meta.spatial_index_size.is_some());

        let path = block_meta.spatial_index_location.clone().unwrap();
        let file_size = block_meta.spatial_index_size;
        let index_schema = read_parquet_schema_async_rs(dal, &path.0, file_size).await?;
        let spatial_stats = block_meta.spatial_stats.clone().unwrap();

        let mut expected_fields = HashSet::new();
        for index_name in &index_names {
            let table_index = table_indexes.get(index_name).unwrap();
            let index_version = index_schema.metadata.get(index_name).unwrap();
            assert_eq!(index_version, &table_index.version);

            for column_id in &table_index.column_ids {
                expected_fields.insert(column_id.to_string());

                let spatial_stat = spatial_stats.get(column_id).unwrap();
                assert!(spatial_stat.is_valid);
                assert_eq!(spatial_stat.srid, 4326);
            }
        }

        for field in index_schema.fields() {
            assert!(expected_fields.remove(field.name()));
        }
        assert!(expected_fields.is_empty());

        for column_id in &geometry_column_ids {
            let spatial_stat = spatial_stats.get(column_id).unwrap();
            assert!(spatial_stat.is_valid);
            assert_eq!(spatial_stat.srid, 4326);
        }
        assert_eq!(spatial_stats.len(), geometry_column_ids.len());
    }

    Ok(())
}
