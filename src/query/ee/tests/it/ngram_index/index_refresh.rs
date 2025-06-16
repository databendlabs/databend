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

use std::sync::Arc;

use databend_common_base::base::tokio;
use databend_common_exception::Result;
use databend_common_storage::DataOperator;
use databend_common_storages_fuse::index::BloomIndexMeta;
use databend_common_storages_fuse::io::read::bloom::block_filter_reader::load_index_meta;
use databend_enterprise_query::test_kits::context::EESetup;
use databend_query::test_kits::TestFixture;
use databend_storages_common_table_meta::meta::SingleColumnMeta;
use futures_util::StreamExt;

#[tokio::test(flavor = "multi_thread")]
async fn test_fuse_do_refresh_ngram_index() -> Result<()> {
    let fixture = TestFixture::setup_with_custom(EESetup::new()).await?;
    fixture
        .default_session()
        .get_settings()
        .set_data_retention_time_in_days(0)?;
    fixture.create_default_database().await?;

    // Create table
    fixture
        .execute_command("CREATE TABLE default.t3 (a int, b int, c int, d string, e string) storage_format = 'parquet'")
        .await?;
    // Insert data
    fixture
        .execute_command("INSERT INTO default.t3 VALUES(1,2,3, 'aaaaaaaaaa', 'aaaaaaaaaaaaa'),(4,5,6,'xxxxxxxxxxx','yyyyyyyyyyy');")
        .await?;
    fixture
        .execute_command("CREATE NGRAM INDEX idx2 ON default.t3(d);")
        .await?;

    let meta_0 = get_bloom_index_meta(&fixture).await?;
    assert_eq!(meta_0.columns.len(), 5);
    fixture
        .execute_command("REFRESH NGRAM INDEX idx2 ON default.t3;")
        .await?;
    let meta_1 = get_bloom_index_meta(&fixture).await?;

    assert_eq!(&meta_0.columns[..], &meta_1.columns[..5]);
    assert_eq!(meta_1.columns.len(), 6);
    assert_eq!(
        &meta_1.columns[5],
        &("Ngram(3)_3_1048576".to_string(), SingleColumnMeta {
            offset: 424,
            len: 1048644,
            num_values: 1,
        })
    );

    fixture
        .execute_command("DROP NGRAM INDEX idx2 ON default.t3;")
        .await?;
    fixture
        .execute_command("CREATE NGRAM INDEX idx2 ON default.t3(d) gram_size = 5;")
        .await?;
    fixture
        .execute_command("REFRESH NGRAM INDEX idx2 ON default.t3;")
        .await?;
    let meta_2 = get_bloom_index_meta(&fixture).await?;
    assert_eq!(meta_2.columns.len(), 6);
    assert_eq!(
        &meta_2.columns[5],
        &("Ngram(3)_5_1048576".to_string(), SingleColumnMeta {
            offset: 424,
            len: 1048644,
            num_values: 1,
        })
    );

    Ok(())
}

async fn get_bloom_index_meta(fixture: &TestFixture) -> Result<Arc<BloomIndexMeta>> {
    let block = fixture
        .execute_query(
            "select bloom_filter_location, bloom_filter_size from fuse_block('default', 't3');",
        )
        .await?
        .next()
        .await
        .transpose()?
        .unwrap();
    let path = block.columns()[0].to_column().remove_nullable();
    let path_scalar = path.as_string().unwrap().index(0).unwrap();
    let length = block.columns()[1].to_column();
    let length_scalar = length.as_number().unwrap().index(0).unwrap();

    load_index_meta(
        DataOperator::instance().operator(),
        path_scalar,
        *length_scalar.as_u_int64().unwrap(),
    )
    .await
}
