//  Copyright 2021 Datafuse Labs.
//
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.

use std::default::Default;

use databend_common_base::base::tokio;
use databend_common_exception::Result;
use databend_common_meta_app::schema::TableInfo;
use databend_common_sql::executor::table_read_plan::ToReadDataSourcePlan;
use databend_query::storages::fuse::FuseTable;
use databend_query::stream::ReadDataBlockStream;
use databend_query::test_kits::*;
use databend_storages_common_table_meta::table::OPT_KEY_DATABASE_ID;
use futures::TryStreamExt;

#[tokio::test(flavor = "multi_thread")]
async fn test_fuse_table_normal_case() -> Result<()> {
    let fixture = TestFixture::setup().await?;
    let ctx = fixture.new_query_ctx().await?;

    fixture.create_default_database().await?;
    fixture.create_default_table().await?;

    let mut table = fixture.latest_default_table().await?;

    // basic append and read
    {
        let num_blocks = 2;
        let rows_per_block = 2;
        let value_start_from = 1;
        let stream =
            TestFixture::gen_sample_blocks_stream_ex(num_blocks, rows_per_block, value_start_from);

        let blocks = stream.try_collect().await?;
        fixture
            .append_commit_blocks(table.clone(), blocks, false, true)
            .await?;

        // get the latest tbl
        let prev_version = table.get_table_info().ident.seq;
        table = fixture.latest_default_table().await?;
        assert_ne!(prev_version, table.get_table_info().ident.seq);

        let (stats, _) = table.read_partitions(ctx.clone(), None, true).await?;
        assert_eq!(stats.read_rows, num_blocks * rows_per_block);

        let plan = table
            .read_plan(ctx.clone(), None, None, false, true)
            .await?;

        let stream = table.read_data_block_stream(ctx.clone(), &plan).await?;
        let blocks = stream.try_collect::<Vec<_>>().await?;
        let rows: usize = blocks.iter().map(|block| block.num_rows()).sum();
        assert_eq!(rows, num_blocks * rows_per_block);

        // recall our test setting:
        //   - num_blocks = 2;
        //   - rows_per_block = 2;
        //   - value_start_from = 1
        // thus
        let expected = vec![
            "+----------+----------+",
            "| Column 0 | Column 1 |",
            "+----------+----------+",
            "| 1        | (2, 3)   |",
            "| 2        | (2, 3)   |",
            "| 2        | (4, 6)   |",
            "| 4        | (4, 6)   |",
            "+----------+----------+",
        ];
        databend_common_expression::block_debug::assert_blocks_sorted_eq(
            expected,
            blocks.as_slice(),
        );
    }

    // test commit with overwrite

    {
        // insert overwrite 5 blocks
        let num_blocks = 2;
        let rows_per_block = 2;
        let value_start_from = 2;
        let stream =
            TestFixture::gen_sample_blocks_stream_ex(num_blocks, rows_per_block, value_start_from);

        let blocks = stream.try_collect().await?;
        fixture
            .append_commit_blocks(table.clone(), blocks, true, true)
            .await?;

        // get the latest tbl
        let prev_version = table.get_table_info().ident.seq;
        let table = fixture.latest_default_table().await?;
        assert_ne!(prev_version, table.get_table_info().ident.seq);

        let (stats, _) = table.read_partitions(ctx.clone(), None, true).await?;
        assert_eq!(stats.read_rows, num_blocks * rows_per_block);

        let plan = table
            .read_plan(ctx.clone(), None, None, false, true)
            .await?;
        let stream = table.read_data_block_stream(ctx.clone(), &plan).await?;

        let blocks = stream.try_collect::<Vec<_>>().await?;
        let rows: usize = blocks.iter().map(|block| block.num_rows()).sum();
        assert_eq!(rows, num_blocks * rows_per_block);

        // two block, two rows for each block, value starts with 2
        let expected = vec![
            "+----------+----------+",
            "| Column 0 | Column 1 |",
            "+----------+----------+",
            "| 2        | (4, 6)   |",
            "| 3        | (6, 9)   |",
            "| 4        | (4, 6)   |",
            "| 6        | (6, 9)   |",
            "+----------+----------+",
        ];
        databend_common_expression::block_debug::assert_blocks_sorted_eq(
            expected,
            blocks.as_slice(),
        );
    }

    Ok(())
}

#[test]
fn test_parse_storage_prefix() -> Result<()> {
    let mut tbl_info = TableInfo::default();
    let db_id = 2;
    let tbl_id = 1;
    tbl_info.ident.table_id = tbl_id;
    tbl_info
        .meta
        .options
        .insert(OPT_KEY_DATABASE_ID.to_owned(), db_id.to_string());
    let prefix = FuseTable::parse_storage_prefix(&tbl_info)?;
    assert_eq!(format!("{}/{}", db_id, tbl_id), prefix);
    Ok(())
}
