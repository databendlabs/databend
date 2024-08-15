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

use std::collections::BTreeMap;

use databend_common_base::base::tokio;
use databend_common_catalog::table::Table;
use databend_common_catalog::table::TableExt;
use databend_common_exception::Result;
use databend_common_meta_app::schema::CreateOption;
use databend_common_meta_app::schema::CreateTableIndexReq;
use databend_common_sql::plans::RefreshTableIndexPlan;
use databend_common_storages_fuse::io::read::InvertedIndexReader;
use databend_common_storages_fuse::io::MetaReaders;
use databend_common_storages_fuse::io::TableMetaLocationGenerator;
use databend_common_storages_fuse::FuseTable;
use databend_common_storages_fuse::TableContext;
use databend_enterprise_inverted_index::get_inverted_index_handler;
use databend_enterprise_query::test_kits::context::EESetup;
use databend_query::interpreters::Interpreter;
use databend_query::interpreters::RefreshTableIndexInterpreter;
use databend_query::test_kits::append_string_sample_data;
use databend_query::test_kits::*;
use databend_storages_common_cache::LoadParams;
use tantivy::schema::Field;
use tantivy::tokenizer::LowerCaser;
use tantivy::tokenizer::SimpleTokenizer;
use tantivy::tokenizer::TextAnalyzer;
use tantivy::tokenizer::TokenizerManager;

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

    let ctx = fixture.new_query_ctx().await?;
    let catalog = ctx.get_catalog(&fixture.default_catalog_name()).await?;
    let table_id = table.get_id();
    let index_name = "idx1".to_string();
    let mut options = BTreeMap::new();
    options.insert("tokenizer".to_string(), "english".to_string());
    options.insert(
        "filters".to_string(),
        "english_stop,english_stemmer,chinese_stop".to_string(),
    );

    let req = CreateTableIndexReq {
        create_option: CreateOption::Create,
        table_id,
        name: index_name.clone(),
        column_ids: vec![0, 1],
        sync_creation: false,
        options,
    };

    let res = handler.do_create_table_index(catalog.clone(), req).await;
    assert!(res.is_ok());

    let refresh_index_plan = RefreshTableIndexPlan {
        catalog: fixture.default_catalog_name(),
        database: fixture.default_db_name(),
        table: fixture.default_table_name(),
        index_name: index_name.clone(),
        segment_locs: None,
    };
    let interpreter = RefreshTableIndexInterpreter::try_create(ctx.clone(), refresh_index_plan)?;
    let _ = interpreter.execute(ctx.clone()).await?;

    let new_table = table.refresh(ctx.as_ref()).await?;
    let new_fuse_table = FuseTable::do_create(new_table.get_table_info().clone())?;
    let table_schema = new_fuse_table.schema();

    // get index location from new table snapshot
    let new_snapshot = new_fuse_table.read_table_snapshot().await?;
    assert!(new_snapshot.is_some());
    let new_snapshot = new_snapshot.unwrap();

    let table_info = new_table.get_table_info();
    let table_indexes = &table_info.meta.indexes;
    let table_index = table_indexes.get(&index_name);
    assert!(table_index.is_some());
    let table_index = table_index.unwrap();
    let index_version = table_index.version.clone();

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
    assert_eq!(block_metas.len(), 1);
    let block_meta = &block_metas[0];

    let dal = new_fuse_table.get_operator_ref();
    let fields = ["title".to_string(), "content".to_string()];

    let index_loc = TableMetaLocationGenerator::gen_inverted_index_location_from_block_location(
        &block_meta.location.0,
        &index_name,
        &index_version,
    );

    let field_nums = fields.len();
    let has_score = true;
    let need_position = false;

    let mut query_fields = Vec::with_capacity(fields.len());
    let query_field_boosts = Vec::new();
    for i in 0..fields.len() {
        let field = Field::from_field_id(i as u32);
        query_fields.push(field);
    }
    let tokenizer_manager = TokenizerManager::new();
    let english_analyzer = TextAnalyzer::builder(SimpleTokenizer::default())
        .filter(LowerCaser)
        .build();
    tokenizer_manager.register("english", english_analyzer);

    let inverted_index_option = None;

    let index_reader =
        InvertedIndexReader::try_create(dal.clone(), field_nums, need_position, &index_loc).await?;

    let query = "rust";
    let matched_rows = index_reader.clone().do_filter(
        query,
        has_score,
        &query_fields,
        &query_field_boosts,
        tokenizer_manager.clone(),
        &inverted_index_option,
        block_meta.row_count,
    )?;
    assert!(matched_rows.is_some());
    let matched_rows = matched_rows.unwrap();
    assert_eq!(matched_rows.len(), 2);
    assert_eq!(matched_rows[0].0, 0);
    assert_eq!(matched_rows[1].0, 1);

    let query = "java";
    let matched_rows = index_reader.clone().do_filter(
        query,
        has_score,
        &query_fields,
        &query_field_boosts,
        tokenizer_manager.clone(),
        &inverted_index_option,
        block_meta.row_count,
    )?;
    assert!(matched_rows.is_some());
    let matched_rows = matched_rows.unwrap();
    assert_eq!(matched_rows.len(), 1);
    assert_eq!(matched_rows[0].0, 2);

    let query = "data";
    let matched_rows = index_reader.do_filter(
        query,
        has_score,
        &query_fields,
        &query_field_boosts,
        tokenizer_manager.clone(),
        &inverted_index_option,
        block_meta.row_count,
    )?;
    assert!(matched_rows.is_some());
    let matched_rows = matched_rows.unwrap();
    assert_eq!(matched_rows.len(), 3);
    assert_eq!(matched_rows[0].0, 4);
    assert_eq!(matched_rows[1].0, 1);
    assert_eq!(matched_rows[2].0, 5);

    Ok(())
}
