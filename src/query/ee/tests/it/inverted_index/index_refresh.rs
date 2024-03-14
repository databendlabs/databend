// Copyright 2023 Databend Cloud
//
// Licensed under the Elastic License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.elastic.co/licensing/elastic-license
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use databend_common_base::base::tokio;
use databend_common_exception::Result;
use databend_common_expression::DataSchema;
use databend_common_meta_app::schema::CreateTableIndexReq;
use databend_common_meta_app::schema::OnExist;
use databend_common_storages_fuse::io::read::InvertedIndexReader;
use databend_common_storages_fuse::FuseTable;
use databend_common_storages_fuse::TableContext;
use databend_enterprise_inverted_index::get_inverted_index_handler;
use databend_enterprise_query::test_kits::context::EESetup;
use databend_query::test_kits::append_string_sample_data;
use databend_query::test_kits::*;
use databend_storages_common_table_meta::table::OPT_KEY_SNAPSHOT_LOCATION;

#[tokio::test(flavor = "multi_thread")]
async fn test_fuse_do_refresh_inverted_index() -> Result<()> {
    let fixture = TestFixture::setup_with_custom(EESetup::new()).await?;

    fixture
        .default_session()
        .get_settings()
        .set_data_retention_time_in_days(0)?;
    fixture.create_default_database().await?;
    fixture.create_string_table().await?;

    let number_of_block = 2;
    append_string_sample_data(number_of_block, &fixture).await?;

    let table = fixture.latest_default_table().await?;

    let handler = get_inverted_index_handler();

    let table_ctx = fixture.new_query_ctx().await?;
    let catalog = table_ctx
        .get_catalog(&fixture.default_catalog_name())
        .await?;
    let table_id = table.get_id();
    let index_name = "idx1".to_string();
    let req = CreateTableIndexReq {
        create_option: OnExist::Error,
        table_id,
        name: index_name.clone(),
        column_ids: vec![0, 1],
    };

    let res = handler.do_create_table_index(catalog, req).await;
    assert!(res.is_ok());

    let table = fixture.latest_default_table().await?;
    let table_schema = table.schema();
    let fuse_table = FuseTable::try_from_table(table.as_ref())?;
    let dal = fuse_table.get_operator_ref();

    let new_snapshot_location = handler
        .do_refresh_index(
            fuse_table,
            table_ctx.clone(),
            index_name.clone(),
            table_schema.clone(),
            None,
        )
        .await?;
    assert!(new_snapshot_location.is_some());
    let new_snapshot_location = new_snapshot_location.unwrap();

    // add new snapshot location to table meta options
    let mut new_table_info = table.get_table_info().clone();
    new_table_info.meta.options.insert(
        OPT_KEY_SNAPSHOT_LOCATION.to_owned(),
        new_snapshot_location.clone(),
    );

    let new_fuse_table = FuseTable::do_create(new_table_info.clone())?;

    // get index location from new table snapshot
    let new_snapshot = new_fuse_table.read_table_snapshot().await?;
    assert!(new_snapshot.is_some());
    let new_snapshot = new_snapshot.unwrap();
    assert!(new_snapshot.index_info_locations.is_some());
    let index_info_locations = new_snapshot.index_info_locations.clone().unwrap();
    let index_info_loc = index_info_locations.get(&index_name);
    assert!(index_info_loc.is_some());
    let index_info = new_fuse_table.read_index_info(index_info_loc).await?;
    assert!(index_info.is_some());
    let index_info = index_info.unwrap();
    assert_eq!(index_info.indexes.len(), 1);

    let index_locs: Vec<_> = index_info.indexes.keys().cloned().collect();
    let index_loc = &index_locs[0];

    let schema = DataSchema::from(table_schema);
    let index_reader = InvertedIndexReader::create(dal.clone(), schema);

    let num = 5;
    let query = "rust";
    let docs = index_reader.do_read(index_loc.clone(), query, num)?;
    assert_eq!(docs.len(), 2);
    assert_eq!(docs[0].1.doc_id, 0);
    assert_eq!(docs[1].1.doc_id, 1);

    let query = "java";
    let docs = index_reader.do_read(index_loc.clone(), query, num)?;
    assert_eq!(docs.len(), 1);
    assert_eq!(docs[0].1.doc_id, 2);

    let query = "data";
    let docs = index_reader.do_read(index_loc.clone(), query, num)?;
    assert_eq!(docs.len(), 3);
    assert_eq!(docs[0].1.doc_id, 4);
    assert_eq!(docs[1].1.doc_id, 1);
    assert_eq!(docs[2].1.doc_id, 5);

    Ok(())
}
