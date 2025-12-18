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

use databend_common_base::base::tokio;
use databend_common_catalog::table::Table;
use databend_common_catalog::table::TableExt;
use databend_common_exception::Result;
use databend_common_meta_app::schema::CreateOption;
use databend_common_meta_app::schema::CreateTableIndexReq;
use databend_common_meta_app::schema::TableIndexType;
use databend_common_sql::plans::RefreshTableIndexPlan;
use databend_common_storage::read_parquet_schema_async_rs;
use databend_common_storages_fuse::FuseTable;
use databend_common_storages_fuse::TableContext;
use databend_common_storages_fuse::io::MetaReaders;
use databend_query::interpreters::Interpreter;
use databend_query::interpreters::RefreshTableIndexInterpreter;
use databend_query::sessions::QueryContext;
use databend_query::test_kits::*;
use databend_storages_common_cache::LoadParams;

#[tokio::test(flavor = "multi_thread")]
async fn test_fuse_do_refresh_vector_index() -> Result<()> {
    let fixture = TestFixture::setup().await?;

    fixture
        .default_session()
        .get_settings()
        .set_data_retention_time_in_days(0)?;
    fixture.create_default_database().await?;

    // Create a table with vector columns
    let ctx = fixture.new_query_ctx().await?;
    let catalog = ctx.get_catalog(&fixture.default_catalog_name()).await?;

    // Create a table with two vector columns
    let sql = format!(
        "CREATE TABLE {}.{}.{} (
            id UInt64,
            embedding1 VECTOR(4),
            embedding2 VECTOR(4)
        ) ENGINE=FUSE",
        fixture.default_catalog_name(),
        fixture.default_db_name(),
        fixture.default_table_name()
    );
    fixture.execute_command(&sql).await?;

    // Insert data with two vector columns
    let sql = format!(
        "INSERT INTO {}.{}.{} VALUES 
        (1, [1.0, 2.0, 3.0, 4.0], [5.0, 6.0, 7.0, 8.0]),
        (2, [2.0, 3.0, 4.0, 5.0], [6.0, 7.0, 8.0, 9.0]),
        (3, [3.0, 4.0, 5.0, 6.0], [7.0, 8.0, 9.0, 10.0])",
        fixture.default_catalog_name(),
        fixture.default_db_name(),
        fixture.default_table_name()
    );
    fixture.execute_command(&sql).await?;

    let sql = format!(
        "INSERT INTO {}.{}.{} VALUES 
        (4, [1.0, 2.0, 3.0, 4.0], [5.0, 6.0, 7.0, 8.0]),
        (5, [2.0, 3.0, 4.0, 5.0], [6.0, 7.0, 8.0, 9.0]),
        (6, [3.0, 4.0, 5.0, 6.0], [7.0, 8.0, 9.0, 10.0])",
        fixture.default_catalog_name(),
        fixture.default_db_name(),
        fixture.default_table_name()
    );
    fixture.execute_command(&sql).await?;

    let table = fixture.latest_default_table().await?;
    let table_id = table.get_id();
    let tenant = ctx.get_tenant();

    // Create first vector index on embedding1
    let index_name1 = "idx_embedding1".to_string();
    let mut options1 = BTreeMap::new();
    options1.insert("distance".to_string(), "l1,l2".to_string());

    let req1 = CreateTableIndexReq {
        create_option: CreateOption::Create,
        table_id,
        tenant: tenant.clone(),
        name: index_name1.clone(),
        column_ids: vec![1], // embedding1
        sync_creation: false,
        options: options1,
        index_type: TableIndexType::Vector,
    };

    let res = catalog.create_table_index(req1).await;
    assert!(res.is_ok());

    // Create second vector index on embedding2
    let index_name2 = "idx_embedding2".to_string();
    let mut options2 = BTreeMap::new();
    options2.insert("distance".to_string(), "cosine".to_string());

    let req2 = CreateTableIndexReq {
        create_option: CreateOption::Create,
        table_id,
        tenant: tenant.clone(),
        name: index_name2.clone(),
        column_ids: vec![2], // embedding2
        sync_creation: false,
        options: options2,
        index_type: TableIndexType::Vector,
    };

    let res = catalog.create_table_index(req2).await;
    assert!(res.is_ok());

    // Refresh the first vector index
    let refresh_index_plan1 = RefreshTableIndexPlan {
        index_type: databend_common_ast::ast::TableIndexType::Vector,
        catalog: fixture.default_catalog_name(),
        database: fixture.default_db_name(),
        table: fixture.default_table_name(),
        index_name: index_name1.clone(),
        segment_locs: None,
    };

    let interpreter = RefreshTableIndexInterpreter::try_create(ctx.clone(), refresh_index_plan1)?;
    let _ = interpreter.execute(ctx.clone()).await?;

    let index_names = vec![index_name1.clone()];
    check_index_data(ctx.clone(), table.clone(), index_names).await?;

    let ctx = fixture.new_query_ctx().await?;
    let refresh_index_plan2 = RefreshTableIndexPlan {
        index_type: databend_common_ast::ast::TableIndexType::Vector,
        catalog: fixture.default_catalog_name(),
        database: fixture.default_db_name(),
        table: fixture.default_table_name(),
        index_name: index_name2.clone(),
        segment_locs: None,
    };

    let interpreter = RefreshTableIndexInterpreter::try_create(ctx.clone(), refresh_index_plan2)?;
    let _ = interpreter.execute(ctx.clone()).await?;

    let index_names = vec![index_name1.clone(), index_name2.clone()];
    check_index_data(ctx.clone(), table.clone(), index_names).await?;

    // Update first vector index on embedding1 with different options
    let index_name1 = "idx_embedding1".to_string();
    let mut new_options1 = BTreeMap::new();
    new_options1.insert("distance".to_string(), "l1,l2,cosine".to_string());

    let req3 = CreateTableIndexReq {
        create_option: CreateOption::CreateOrReplace,
        table_id,
        tenant: tenant.clone(),
        name: index_name1.clone(),
        column_ids: vec![1], // embedding1
        sync_creation: false,
        options: new_options1,
        index_type: TableIndexType::Vector,
    };

    let res = catalog.create_table_index(req3).await;
    assert!(res.is_ok());

    // Refresh the first vector index again
    let ctx = fixture.new_query_ctx().await?;
    let table = table.refresh(ctx.as_ref()).await?;
    let refresh_index_plan3 = RefreshTableIndexPlan {
        index_type: databend_common_ast::ast::TableIndexType::Vector,
        catalog: fixture.default_catalog_name(),
        database: fixture.default_db_name(),
        table: fixture.default_table_name(),
        index_name: index_name1.clone(),
        segment_locs: None,
    };

    let interpreter = RefreshTableIndexInterpreter::try_create(ctx.clone(), refresh_index_plan3)?;
    let _ = interpreter.execute(ctx.clone()).await?;

    let index_names = vec![index_name1, index_name2];
    check_index_data(ctx.clone(), table.clone(), index_names).await?;

    Ok(())
}

async fn check_index_data(
    ctx: Arc<QueryContext>,
    table: Arc<dyn Table>,
    index_names: Vec<String>,
) -> Result<()> {
    // Get the updated table and verify both indexes exist
    let new_table = table.refresh(ctx.as_ref()).await?;
    let new_table_info = new_table.get_table_info().clone();
    let table_indexes = new_table_info.meta.indexes.clone();

    let new_fuse_table = FuseTable::create_without_refresh_table_info(
        new_table_info,
        ctx.get_settings().get_s3_storage_class()?,
    )?;

    // Get index locations from new table snapshot
    let new_snapshot = new_fuse_table.read_table_snapshot().await?;
    assert!(new_snapshot.is_some());
    let new_snapshot = new_snapshot.unwrap();
    let dal = new_fuse_table.get_operator_ref();

    // Get block metas to check vector index data
    let table_schema = new_fuse_table.schema();
    let segment_reader =
        MetaReaders::segment_info_reader(new_fuse_table.get_operator(), table_schema.clone());

    let mut block_metas = vec![];
    for (segment_loc, ver) in &new_snapshot.segments {
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
        assert!(block_meta.vector_index_location.is_some());
        assert!(block_meta.vector_index_size.is_some());

        let path = block_meta.vector_index_location.clone().unwrap();
        let file_size = block_meta.vector_index_size;

        let index_schema = read_parquet_schema_async_rs(dal, &path.0, file_size).await?;

        let mut index_column_names = HashSet::new();
        for index_name in &index_names {
            let table_index = table_indexes.get(index_name).unwrap();
            // check index version in metadata
            let index_version = index_schema.metadata.get(index_name);
            assert!(index_version.is_some());
            let index_version = index_version.unwrap();
            assert_eq!(index_version, &table_index.version);

            let distances = if let Some(distance_option) = table_index.options.get("distance") {
                let mut distances: Vec<_> = distance_option.split(',').collect();
                for distance in distances.iter_mut() {
                    if *distance == "cosine" {
                        *distance = "dot";
                    }
                }
                distances
            } else {
                vec!["dot", "l1", "l2"]
            };
            let names = vec![
                "graph_links",
                "graph_data",
                "encoded_u8_meta",
                "encoded_u8_data",
            ];
            for column_id in &table_index.column_ids {
                for distance in &distances {
                    for name in &names {
                        let index_column_name = format!("{}-{}_{}", column_id, distance, name);
                        index_column_names.insert(index_column_name);
                    }
                }
            }
        }

        // check all the index data columns exist
        for field in index_schema.fields() {
            assert!(index_column_names.remove(field.name()));
        }
        assert!(index_column_names.is_empty());
    }

    Ok(())
}
