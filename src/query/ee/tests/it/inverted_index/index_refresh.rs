// Copyright 2024 Databend Cloud
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
use databend_common_storages_fuse::io::read::InvertedIndexReader;
use databend_common_storages_fuse::FuseTable;
use databend_enterprise_inverted_index::get_inverted_index_handler;
use databend_enterprise_query::test_kits::context::EESetup;
use databend_query::test_kits::append_string_sample_data;
use databend_query::test_kits::*;

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
    let table_schema = table.schema();
    let fuse_table = FuseTable::try_from_table(table.as_ref())?;
    let dal = fuse_table.get_operator_ref();

    let table_ctx = fixture.new_query_ctx().await?;
    let schema = DataSchema::from(table_schema);

    let handler = get_inverted_index_handler();
    let location = handler
        .do_refresh_index(fuse_table, table_ctx.clone(), schema.clone(), None)
        .await?;

    let index_reader = InvertedIndexReader::create(dal.clone(), schema);

    let num = 5;
    let query = "rust";
    let docs = index_reader.do_read(location.clone(), query, num)?;
    assert_eq!(docs.len(), 2);
    assert_eq!(docs[0].1.doc_id, 0);
    assert_eq!(docs[1].1.doc_id, 1);

    let query = "java";
    let docs = index_reader.do_read(location.clone(), query, num)?;
    assert_eq!(docs.len(), 1);
    assert_eq!(docs[0].1.doc_id, 2);

    let query = "data";
    let docs = index_reader.do_read(location, query, num)?;
    assert_eq!(docs.len(), 3);
    assert_eq!(docs[0].1.doc_id, 4);
    assert_eq!(docs[1].1.doc_id, 1);
    assert_eq!(docs[2].1.doc_id, 5);

    Ok(())
}
