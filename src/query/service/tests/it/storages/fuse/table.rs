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

use common_base::base::tokio;
use common_catalog::plan::DataSourceInfo;
use common_catalog::plan::DataSourcePlan;
use common_exception::Result;
use common_meta_app::schema::TableInfo;
use common_storages_table_meta::table::OPT_KEY_DATABASE_ID;
use databend_query::sessions::TableContext;
use databend_query::storages::fuse::FuseTable;
use databend_query::stream::ReadDataBlockStream;
use futures::TryStreamExt;

use crate::storages::fuse::table_test_fixture::TestFixture;

#[tokio::test]
async fn test_fuse_table_normal_case() -> Result<()> {
    let fixture = TestFixture::new().await;
    let ctx = fixture.ctx();

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

        let (stats, parts) = table.read_partitions(ctx.clone(), None).await?;
        assert_eq!(stats.read_rows, num_blocks * rows_per_block);

        ctx.try_set_partitions(parts.clone())?;
        let stream = table
            .read_data_block_stream(ctx.clone(), &DataSourcePlan {
                catalog: "default".to_owned(),
                source_info: DataSourceInfo::TableSource(Default::default()),
                scan_fields: None,
                parts,
                statistics: Default::default(),
                description: "".to_string(),
                tbl_args: None,
                push_downs: None,
            })
            .await?;
        let blocks = stream.try_collect::<Vec<_>>().await?;
        let rows: usize = blocks.iter().map(|block| block.num_rows()).sum();
        assert_eq!(rows, num_blocks * rows_per_block);

        // recall our test setting:
        //   - num_blocks = 2;
        //   - rows_per_block = 2;
        //   - value_start_from = 1
        // thus
        let expected = vec![
            "+----+--------+", //
            "| id | t      |", //
            "+----+--------+", //
            "| 1  | (2, 3) |", //
            "| 1  | (2, 3) |", //
            "| 2  | (4, 6) |", //
            "| 2  | (4, 6) |", //
            "+----+--------+", //
        ];
        common_datablocks::assert_blocks_sorted_eq(expected, blocks.as_slice());
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

        let (stats, parts) = table.read_partitions(ctx.clone(), None).await?;
        assert_eq!(stats.read_rows, num_blocks * rows_per_block);

        // inject partitions to current ctx
        ctx.try_set_partitions(parts.clone())?;

        let stream = table
            .read_data_block_stream(ctx.clone(), &DataSourcePlan {
                catalog: "default".to_owned(),
                source_info: DataSourceInfo::TableSource(Default::default()),
                scan_fields: None,
                parts,
                statistics: Default::default(),
                description: "".to_string(),
                tbl_args: None,
                push_downs: None,
            })
            .await?;
        let blocks = stream.try_collect::<Vec<_>>().await?;
        let rows: usize = blocks.iter().map(|block| block.num_rows()).sum();
        assert_eq!(rows, num_blocks * rows_per_block);

        // two block, two rows for each block, value starts with 2
        let expected = vec![
            "+----+--------+", //
            "| id | t      |", //
            "+----+--------+", //
            "| 2  | (4, 6) |", //
            "| 2  | (4, 6) |", //
            "| 3  | (6, 9) |", //
            "| 3  | (6, 9) |", //
            "+----+--------+", //
        ];
        common_datablocks::assert_blocks_sorted_eq(expected, blocks.as_slice());
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
