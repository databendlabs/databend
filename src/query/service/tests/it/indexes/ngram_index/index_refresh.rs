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

use std::sync::Arc;

use databend_common_exception::Result;
use databend_common_storage::DataOperator;
use databend_common_storages_fuse::io::read::bloom::block_filter_reader::load_bloom_filter_by_columns;
use databend_query::sessions::TableContext;
use databend_query::storages::index::filters::BlockFilter;
use databend_query::test_kits::TestFixture;
use databend_query::test_kits::latest_default_block_meta;
use databend_query::test_kits::query_count;
use databend_storages_common_io::ReadSettings;
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
async fn test_ngram_index_is_incompatible_with_partial_update() -> anyhow::Result<()> {
    let fixture = TestFixture::setup().await?;
    let db = fixture.default_db_name();
    let table_name = fixture.default_table_name();
    fixture.create_default_database().await?;

    fixture
        .execute_command(&format!(
            "create table {db}.{table_name} (id int, content string) engine=fuse \
             enable_partial_update=true"
        ))
        .await?;
    assert!(
        fixture
            .execute_command(&format!(
                "create ngram index idx_content on {db}.{table_name}(content)"
            ))
            .await
            .is_err()
    );
    fixture
        .execute_command(&format!("drop table {db}.{table_name}"))
        .await?;

    fixture
        .execute_command(&format!(
            "create table {db}.{table_name} (id int, content string) engine=fuse"
        ))
        .await?;
    fixture
        .execute_command(&format!(
            "insert into {db}.{table_name} values (1, 'before'), (2, 'unchanged')"
        ))
        .await?;
    fixture
        .execute_command(&format!(
            "create ngram index idx_content on {db}.{table_name}(content)"
        ))
        .await?;
    fixture
        .execute_command(&format!(
            "refresh ngram index idx_content on {db}.{table_name}"
        ))
        .await?;
    assert!(
        fixture
            .execute_command(&format!(
                "alter table {db}.{table_name} \
                 set options(enable_partial_update=true)"
            ))
            .await
            .is_err()
    );
    drop_ngram_enable_partial_update_and_check(&fixture, &db, &table_name).await?;

    fixture
        .execute_command(&format!("drop table {db}.{table_name}"))
        .await?;
    fixture
        .execute_command(&format!(
            "create table {db}.{table_name} (id int, content string) engine=fuse"
        ))
        .await?;
    fixture
        .execute_command(&format!(
            "create ngram index idx_content on {db}.{table_name}(content)"
        ))
        .await?;
    fixture
        .execute_command(&format!(
            "insert into {db}.{table_name} values (1, 'before'), (2, 'unchanged')"
        ))
        .await?;
    assert!(
        latest_default_block_meta(&fixture)
            .await?
            .ngram_filter_index_size
            .is_some()
    );
    assert!(
        fixture
            .execute_command(&format!(
                "alter table {db}.{table_name} \
                 set options(enable_partial_update=true)"
            ))
            .await
            .is_err()
    );
    drop_ngram_enable_partial_update_and_check(&fixture, &db, &table_name).await?;

    Ok(())
}

async fn drop_ngram_enable_partial_update_and_check(
    fixture: &TestFixture,
    database: &str,
    table: &str,
) -> anyhow::Result<()> {
    fixture
        .execute_command(&format!(
            "drop ngram index idx_content on {database}.{table}"
        ))
        .await?;
    fixture
        .execute_command(&format!(
            "alter table {database}.{table} set options(enable_partial_update=true)"
        ))
        .await?;
    fixture
        .execute_command("set enable_partial_update = 1")
        .await?;
    fixture
        .execute_command(&format!(
            "update {database}.{table} set content = 'after' where id = 1"
        ))
        .await?;

    let block_meta = latest_default_block_meta(fixture).await?;
    assert_eq!(block_meta.column_groups.len(), 2);
    assert!(block_meta.bloom_filter_index_location.is_none());
    assert_eq!(
        block_meta
            .column_groups
            .iter()
            .filter(|group| group.bloom.is_some())
            .count(),
        1
    );
    let rows = fixture
        .execute_query(&format!(
            "select count(*) from {database}.{table} \
             where (id = 1 and content = 'after') or (id = 2 and content = 'unchanged')"
        ))
        .await?;
    assert_eq!(query_count(rows).await?, 2);
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
