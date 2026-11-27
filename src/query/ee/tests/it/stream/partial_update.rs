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

use databend_common_expression::DataBlock;
use databend_common_expression::block_debug::pretty_format_blocks;
use databend_enterprise_query::test_kits::context::EESetup;
use databend_query::test_kits::TestFixture;
use futures::TryStreamExt;

async fn query(fixture: &TestFixture, sql: &str) -> anyhow::Result<String> {
    let blocks: Vec<DataBlock> = fixture.execute_query(sql).await?.try_collect().await?;
    Ok(pretty_format_blocks(&blocks)?)
}

/// A stream reports the same changes for a partial UPDATE, which writes a column group file of
/// each changed block, as for an UPDATE that rewrites the whole blocks.
#[tokio::test(flavor = "multi_thread")]
async fn test_stream_on_partial_update() -> anyhow::Result<()> {
    let fixture = TestFixture::setup_with_custom(EESetup::new()).await?;
    for (table, options) in [("t", "enable_partial_update = true"), ("t_ref", "")] {
        for sql in [
            format!(
                "create table {table} (id int not null, a int, b string) row_per_block = 4 \
                 change_tracking = true {options}"
            ),
            format!("insert into {table} select number, number, 'x' from numbers(6)"),
            format!("create stream {table}_s on table {table}"),
            format!("insert into {table} select number + 6, number, 'y' from numbers(6)"),
            format!("update {table} set a = a + 100 where id % 3 = 0"),
            format!("update {table} set b = 'z' where id % 4 = 1"),
        ] {
            fixture.execute_command(&sql).await?;
        }
    }

    // `fuse_column` reports each column in the file that stores it: `t` has split blocks.
    let split = query(
        &fixture,
        "select count(distinct block_location) > \
         (select count(*) from fuse_block('default', 't')) from fuse_column('default', 't')",
    )
    .await?;
    assert!(split.contains("true"), "{split}");

    let sql = "select id, a, b, change$action, change$is_update from {s} \
               order by id, change$action";
    assert_eq!(
        query(&fixture, &sql.replace("{s}", "t_s")).await?,
        query(&fixture, &sql.replace("{s}", "t_ref_s")).await?,
    );
    Ok(())
}
