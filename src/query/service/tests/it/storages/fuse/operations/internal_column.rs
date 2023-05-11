// Copyright 2023 Datafuse Labs.
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

use common_base::base::tokio;
use common_catalog::plan::InternalColumn;
use common_catalog::plan::InternalColumnMeta;
use common_catalog::plan::Partitions;
use common_exception::Result;
use common_expression::DataBlock;
use common_sql::binder::INTERNAL_COLUMN_FACTORY;
use common_sql::Planner;
use common_storages_fuse::io::MetaReaders;
use common_storages_fuse::FuseTable;
use databend_query::interpreters::InterpreterFactory;
use databend_query::storages::fuse::fuse_part::FusePartInfo;
use databend_query::test_kits::table_test_fixture::execute_query;
use databend_query::test_kits::table_test_fixture::TestFixture;
use futures::TryStreamExt;
use storages_common_cache::LoadParams;
use storages_common_table_meta::meta::SegmentInfo;
use storages_common_table_meta::meta::TableSnapshot;
use storages_common_table_meta::meta::Versioned;
use storages_common_table_meta::table::OPT_KEY_SNAPSHOT_LOCATION;

fn expected_data_block(
    parts: &Partitions,
    internal_columns: &Vec<InternalColumn>,
) -> Result<Vec<DataBlock>> {
    let mut data_blocks = Vec::new();
    for part in &parts.partitions {
        let fuse_part = FusePartInfo::from_part(part)?;
        let num_rows = fuse_part.nums_rows;
        let block_meta = fuse_part.block_meta_index.as_ref().unwrap();
        let mut columns = Vec::new();
        let internal_column_meta = InternalColumnMeta {
            segment_id: block_meta.segment_id,
            block_id: block_meta.block_id,
            block_location: block_meta.block_location.clone(),
            segment_location: block_meta.segment_location.clone(),
            snapshot_location: block_meta.snapshot_location.clone().unwrap(),
            offsets: None,
        };
        for internal_column in internal_columns {
            let column = internal_column.generate_column_values(&internal_column_meta, num_rows);
            columns.push(column);
        }
        data_blocks.push(DataBlock::new(columns, num_rows));
    }
    data_blocks.reverse();

    Ok(data_blocks)
}

fn check_data_block(expected: Vec<DataBlock>, blocks: Vec<DataBlock>) -> Result<()> {
    let expected_data_block = DataBlock::concat(&expected)?;
    let data_block = DataBlock::concat(&blocks)?;

    for (expected_column, column) in expected_data_block
        .columns()
        .iter()
        .zip(data_block.columns())
    {
        assert_eq!(expected_column.data_type, column.data_type);
        assert_eq!(expected_column.value, column.value);
    }

    Ok(())
}

async fn check_partitions(parts: &Partitions, fixture: &TestFixture) -> Result<()> {
    let mut segment_name = HashSet::new();
    let mut block_name = HashSet::new();

    let table = fixture.latest_default_table().await?;
    let fuse_table = FuseTable::try_from_table(table.as_ref())?;

    let snapshot_name = table
        .get_table_info()
        .options()
        .get(OPT_KEY_SNAPSHOT_LOCATION)
        .unwrap();
    let reader = MetaReaders::table_snapshot_reader(fuse_table.get_operator());

    let load_params = LoadParams {
        location: snapshot_name.clone(),
        len_hint: None,
        ver: TableSnapshot::VERSION,
        put_cache: true,
    };

    let snapshot = reader.read(&load_params).await?;
    for segment in &snapshot.segments {
        segment_name.insert(segment.0.clone());

        let compact_segment_reader = MetaReaders::segment_info_reader(
            fuse_table.get_operator(),
            TestFixture::default_table_schema(),
        );
        let params = LoadParams {
            location: segment.0.clone(),
            len_hint: None,
            ver: SegmentInfo::VERSION,
            put_cache: false,
        };
        let compact_segment_info = compact_segment_reader.read(&params).await?;
        let segment_info = SegmentInfo::try_from(compact_segment_info.as_ref())?;

        for block in &segment_info.blocks {
            block_name.insert(block.location.0.clone());
        }
    }

    for part in &parts.partitions {
        let fuse_part = FusePartInfo::from_part(part)?;
        let block_meta = fuse_part.block_meta_index.as_ref().unwrap();
        assert_eq!(
            block_meta.snapshot_location.clone().unwrap(),
            snapshot_name.to_owned()
        );
        assert!(segment_name.contains(&block_meta.segment_location));
        assert!(block_name.contains(&block_meta.block_location));
    }

    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn test_internal_column() -> Result<()> {
    let fixture = TestFixture::new().await;
    let db = fixture.default_db_name();
    let tbl = fixture.default_table_name();
    let ctx = fixture.ctx();
    fixture.create_default_table().await?;

    let internal_columns = vec![
        INTERNAL_COLUMN_FACTORY
            .get_internal_column("_row_id")
            .unwrap(),
        INTERNAL_COLUMN_FACTORY
            .get_internal_column("_snapshot_name")
            .unwrap(),
        INTERNAL_COLUMN_FACTORY
            .get_internal_column("_segment_name")
            .unwrap(),
        INTERNAL_COLUMN_FACTORY
            .get_internal_column("_block_name")
            .unwrap(),
    ];

    // insert 5 times
    let n = 5;
    for _ in 0..n {
        let table = fixture.latest_default_table().await?;
        let num_blocks = 1;
        let stream = TestFixture::gen_sample_blocks_stream(num_blocks, 1);

        let blocks = stream.try_collect().await?;
        fixture
            .append_commit_blocks(table.clone(), blocks, false, true)
            .await?;
    }

    let query = format!(
        "select _row_id,_snapshot_name,_segment_name,_block_name from {}.{} order by _row_id",
        db, tbl
    );
    let res = execute_query(ctx.clone(), &query).await?;
    let blocks = res.try_collect::<Vec<DataBlock>>().await?;

    let table = fixture.latest_default_table().await?;
    let (_, parts) = table.read_partitions(ctx.clone(), None).await?;
    let expected = expected_data_block(&parts, &internal_columns)?;
    check_partitions(&parts, &fixture).await?;
    check_data_block(expected, blocks)?;

    // do compact
    let query = format!("optimize table {db}.{tbl} compact");
    let mut planner = Planner::new(ctx.clone());
    let (plan, _) = planner.plan_sql(&query).await?;
    let interpreter = InterpreterFactory::get(ctx.clone(), &plan).await?;
    let data_stream = interpreter.execute(ctx.clone()).await?;
    let _ = data_stream.try_collect::<Vec<_>>().await;

    let query = format!(
        "select _row_id,_snapshot_name,_segment_name,_block_name from {}.{} order by _row_id",
        db, tbl
    );
    let res = execute_query(ctx.clone(), &query).await?;
    let blocks = res.try_collect::<Vec<DataBlock>>().await?;

    let table = fixture.latest_default_table().await?;
    let (_, parts) = table.read_partitions(ctx.clone(), None).await?;
    let expected = expected_data_block(&parts, &internal_columns)?;
    check_partitions(&parts, &fixture).await?;
    check_data_block(expected, blocks)?;

    Ok(())
}
