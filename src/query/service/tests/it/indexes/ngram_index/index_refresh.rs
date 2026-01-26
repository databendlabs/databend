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

use databend_common_exception::Result;
use databend_common_storage::DataOperator;
use databend_common_storages_fuse::io::read::bloom::block_filter_reader::load_bloom_filter_by_columns;
use databend_query::storages::index::filters::BlockFilter;
use databend_query::test_kits::TestFixture;
use futures_util::StreamExt;

#[tokio::test(flavor = "multi_thread")]
async fn test_fuse_do_refresh_ngram_index() -> anyhow::Result<()> {
    let fixture = TestFixture::setup().await?;
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

    let block_filter_0 = get_block_filter(&fixture, &[
        "Bloom(0)".to_string(),
        "Bloom(1)".to_string(),
        "Bloom(2)".to_string(),
        "Bloom(3)".to_string(),
        "Bloom(4)".to_string(),
    ])
    .await?;
    assert_eq!(block_filter_0.filter_schema.fields().len(), 5);
    assert_eq!(block_filter_0.filters.len(), 5);
    fixture
        .execute_command("REFRESH NGRAM INDEX idx2 ON default.t3;")
        .await?;
    let block_filter_1 = get_block_filter(&fixture, &[
        "Bloom(0)".to_string(),
        "Bloom(1)".to_string(),
        "Bloom(2)".to_string(),
        "Bloom(3)".to_string(),
        "Bloom(4)".to_string(),
        "Ngram(3)_3_1048576".to_string(),
    ])
    .await?;
    assert_eq!(block_filter_1.filter_schema.fields().len(), 6);
    assert_eq!(block_filter_1.filters.len(), 6);

    assert_eq!(
        &block_filter_0.filter_schema.fields()[..],
        &block_filter_1.filter_schema.fields()[..5]
    );
    if block_filter_0.filters[..] != block_filter_1.filters[..5] {
        unreachable!()
    }

    fixture
        .execute_command("DROP NGRAM INDEX idx2 ON default.t3;")
        .await?;
    fixture
        .execute_command(
            "CREATE NGRAM INDEX idx2 ON default.t3(d) gram_size = 8 bloom_size = 1048570;",
        )
        .await?;
    fixture
        .execute_command("REFRESH NGRAM INDEX idx2 ON default.t3;")
        .await?;
    let block_filter_2 = get_block_filter(&fixture, &[
        "Bloom(0)".to_string(),
        "Bloom(1)".to_string(),
        "Bloom(2)".to_string(),
        "Bloom(3)".to_string(),
        "Bloom(4)".to_string(),
        "Ngram(3)_8_1048570".to_string(),
    ])
    .await?;
    assert_eq!(block_filter_2.filter_schema.fields().len(), 6);
    assert_eq!(block_filter_2.filters.len(), 6);

    if block_filter_1.filters[5] == block_filter_2.filters[5] {
        unreachable!()
    }

    Ok(())
}

async fn get_block_filter(fixture: &TestFixture, columns: &[String]) -> Result<BlockFilter> {
    let block = fixture
        .execute_query(
            "select bloom_filter_location, bloom_filter_size from fuse_block('default', 't3');",
        )
        .await?
        .next()
        .await
        .transpose()?
        .unwrap();
    let path = block.get_by_offset(0).index(0).unwrap();
    let path = *path.as_string().unwrap();
    let length = block.get_by_offset(1).index(0).unwrap();

    load_bloom_filter_by_columns(
        DataOperator::instance().operator(),
        columns,
        path,
        *length.as_number().unwrap().as_u_int64().unwrap(),
    )
    .await
}
