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

use std::collections::HashSet;
use std::sync::Arc;

use databend_common_exception::Result;
use databend_common_storage::DataOperator;
use databend_common_storages_fuse::io::read::bloom::block_filter_reader::load_bloom_filter_by_columns;
use databend_query::sessions::TableContext;
use databend_query::storages::index::filters::BlockFilter;
use databend_query::test_kits::TestFixture;
use databend_query::test_kits::latest_default_block_meta;
use databend_storages_common_io::ReadSettings;
use databend_storages_common_table_meta::meta::Versioned;
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

    let block_filter_0 = get_block_filter(&fixture, "default", "t3", &[
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
    let block_filter_1 = get_block_filter(&fixture, "default", "t3", &[
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
    let block_filter_2 = get_block_filter(&fixture, "default", "t3", &[
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

#[tokio::test(flavor = "multi_thread")]
async fn test_refresh_ngram_preserves_split_bloom_files() -> anyhow::Result<()> {
    let fixture = TestFixture::setup().await?;
    let db = fixture.default_db_name();
    let table_name = fixture.default_table_name();
    fixture.create_default_database().await?;
    fixture
        .execute_command(&format!(
            "create table {db}.{table_name} (id int, a int, d string, e string) engine=fuse \
             bloom_index_columns='id,a' enable_partial_update=true"
        ))
        .await?;
    fixture
        .execute_command(&format!(
            "insert into {db}.{table_name} values \
             (1, 10, 'aaaaaaaaaa', 'xxxxxxxxxx'), \
             (2, 20, 'bbbbbbbbbb', 'yyyyyyyyyy')"
        ))
        .await?;
    fixture
        .execute_command("set enable_partial_update = 1")
        .await?;
    fixture
        .execute_command(&format!(
            "update {db}.{table_name} set a = a + 1 where id = 1"
        ))
        .await?;

    let split_meta = latest_default_block_meta(&fixture).await?;
    assert_eq!(split_meta.bloom_index_files.len(), 2);
    let table = fixture.latest_default_table().await?;
    let id_column_id = table.schema().field(0).column_id();
    let a_column_id = table.schema().field(1).column_id();
    let d_column_id = table.schema().field(2).column_id();
    let e_column_id = table.schema().field(3).column_id();
    let active_column_ids = split_meta
        .bloom_index_files
        .iter()
        .flat_map(|file| file.active_column_ids.iter().copied())
        .collect::<HashSet<_>>();
    assert_eq!(
        active_column_ids,
        HashSet::from([id_column_id, a_column_id])
    );

    let operator = DataOperator::instance().operator();
    let split_files = split_meta
        .bloom_index_files
        .iter()
        .map(|file| (file.location.0.clone(), file.file_size))
        .collect::<Vec<_>>();
    fixture
        .execute_command(&format!(
            "create ngram index idx_e on {db}.{table_name}(e) gram_size=4"
        ))
        .await?;
    fixture
        .execute_command(&format!("create ngram index idx_d on {db}.{table_name}(d)"))
        .await?;
    fixture
        .execute_command(&format!("refresh ngram index idx_d on {db}.{table_name}"))
        .await?;

    let refreshed_meta = latest_default_block_meta(&fixture).await?;
    assert!(refreshed_meta.bloom_index_files.is_empty());
    let refreshed_location = refreshed_meta
        .bloom_filter_index_location
        .as_ref()
        .expect("refreshed Bloom index location");
    assert_eq!(refreshed_location.1, BlockFilter::VERSION);
    assert!(
        split_files
            .iter()
            .all(|(path, _)| path != &refreshed_location.0)
    );
    for (path, size) in &split_files {
        assert_eq!(operator.stat(path).await?.content_length(), *size);
    }

    let columns = [
        format!("Bloom({id_column_id})"),
        format!("Bloom({a_column_id})"),
        format!("Ngram({d_column_id})_3_1048576"),
        format!("Ngram({e_column_id})_4_1048576"),
    ];
    let block_filter = get_block_filter(&fixture, &db, &table_name, &columns).await?;
    assert_eq!(block_filter.filters.len(), columns.len());
    for column in columns {
        assert!(block_filter.filter_schema.index_of(&column).is_ok());
    }

    Ok(())
}

async fn get_block_filter(
    fixture: &TestFixture,
    database: &str,
    table: &str,
    columns: &[String],
) -> Result<BlockFilter> {
    let block = fixture
        .execute_query(&format!(
            "select bloom_filter_location, bloom_filter_size \
             from fuse_block('{database}', '{table}')"
        ))
        .await?
        .next()
        .await
        .transpose()?
        .unwrap();
    let path = block.get_by_offset(0).index(0).unwrap();
    let path = *path.as_string().unwrap();
    let length = block.get_by_offset(1).index(0).unwrap();

    let ctx = fixture.new_query_ctx().await?;
    let table_ctx: Arc<dyn TableContext> = ctx.clone();
    load_bloom_filter_by_columns(
        DataOperator::instance().operator(),
        &ReadSettings::from_ctx(&table_ctx)?,
        columns,
        path,
        *length.as_number().unwrap().as_u_int64().unwrap(),
    )
    .await
}
