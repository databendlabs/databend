//  Copyright 2023 Datafuse Labs.
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

use std::collections::HashSet;

use common_base::base::tokio;
use common_exception::Result;
use common_expression::types::Int32Type;
use common_expression::types::NumberDataType;
use common_expression::types::UInt64Type;
use common_expression::ColumnId;
use common_expression::DataBlock;
use common_expression::FromData;
use common_expression::TableDataType;
use common_expression::TableField;
use common_expression::TableSchemaRefExt;
use common_sql::plans::AddTableColumnPlan;
use common_sql::plans::DropTableColumnPlan;
use common_sql::Planner;
use common_storages_fuse::io::MetaReaders;
use common_storages_fuse::FuseTable;
use common_storages_fuse::TableContext;
use databend_query::interpreters::AddTableColumnInterpreter;
use databend_query::interpreters::DropTableColumnInterpreter;
use databend_query::interpreters::Interpreter;
use databend_query::interpreters::InterpreterFactory;
use futures_util::TryStreamExt;
use storages_common_cache::LoadParams;
use storages_common_table_meta::meta::SegmentInfo;
use storages_common_table_meta::meta::TableSnapshot;
use storages_common_table_meta::meta::Versioned;
use storages_common_table_meta::table::OPT_KEY_SNAPSHOT_LOCATION;

use crate::storages::fuse::table_test_fixture::TestFixture;

async fn check_segment_column_ids(
    fixture: &TestFixture,
    expected_column_ids: Vec<ColumnId>,
) -> Result<()> {
    let catalog = fixture.ctx().get_catalog("default")?;
    // get the latest tbl
    let table = catalog
        .get_table(
            fixture.default_tenant().as_str(),
            fixture.default_db_name().as_str(),
            fixture.default_table_name().as_str(),
        )
        .await?;

    let snapshot_loc = table
        .get_table_info()
        .options()
        .get(OPT_KEY_SNAPSHOT_LOCATION)
        .unwrap();
    let fuse_table = FuseTable::try_from_table(table.as_ref())?;

    let snapshot_reader = MetaReaders::table_snapshot_reader(fuse_table.get_operator());
    let params = LoadParams {
        location: snapshot_loc.clone(),
        len_hint: None,
        ver: TableSnapshot::VERSION,
    };

    let snapshot = snapshot_reader.read(&params).await?;
    let expected_column_ids =
        HashSet::<ColumnId>::from_iter(expected_column_ids.clone().iter().cloned());
    for (seg_loc, _) in &snapshot.segments {
        let segment_reader = MetaReaders::segment_info_reader(
            fuse_table.get_operator(),
            TestFixture::default_table_schema(),
        );
        let params = LoadParams {
            location: seg_loc.clone(),
            len_hint: None,
            ver: SegmentInfo::VERSION,
        };
        let segment_info = segment_reader.read(&params).await?;
        segment_info.blocks.iter().for_each(|block_meta| {
            assert_eq!(
                HashSet::from_iter(
                    block_meta
                        .col_stats
                        .keys()
                        .cloned()
                        .collect::<Vec<ColumnId>>()
                        .iter()
                        .cloned()
                ),
                expected_column_ids,
            );
        });
    }

    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn test_fuse_table_optimize_alter_table() -> Result<()> {
    let fixture = TestFixture::new().await;
    let ctx = fixture.ctx();
    let tbl_name = fixture.default_table_name();
    let db_name = fixture.default_db_name();
    let catalog_name = fixture.default_catalog_name();

    fixture.create_normal_table().await?;

    // insert values
    let table = fixture.latest_default_table().await?;
    let num_blocks = 1;
    let stream = TestFixture::gen_sample_blocks_stream(num_blocks, 1);

    let blocks = stream.try_collect().await?;
    fixture
        .append_commit_blocks(table.clone(), blocks, false, true)
        .await?;

    // check column ids
    // the table contains two fields: id int32, t tuple(int32, int32)
    let expected_leaf_column_ids = vec![0, 1, 2];
    check_segment_column_ids(&fixture, expected_leaf_column_ids).await?;

    // drop a column
    let drop_table_column_plan = DropTableColumnPlan {
        catalog: fixture.default_catalog_name(),
        database: fixture.default_db_name(),
        table: fixture.default_table_name(),
        column: "t".to_string(),
    };
    let interpreter = DropTableColumnInterpreter::try_create(ctx.clone(), drop_table_column_plan)?;
    interpreter.execute(ctx.clone()).await?;

    // add a column
    let fields = vec![TableField::new(
        "b",
        TableDataType::Number(NumberDataType::UInt64),
    )];
    let schema = TableSchemaRefExt::create(fields);

    let add_table_column_plan = AddTableColumnPlan {
        catalog: fixture.default_catalog_name(),
        database: fixture.default_db_name(),
        table: fixture.default_table_name(),
        schema,
        field_default_exprs: vec![],
        field_comments: vec![],
    };
    let interpreter = AddTableColumnInterpreter::try_create(ctx.clone(), add_table_column_plan)?;
    interpreter.execute(ctx.clone()).await?;

    // insert values for new schema
    let block = {
        let column0 = Int32Type::from_data(vec![1, 2]);
        let column2 = UInt64Type::from_data(vec![3, 4]);

        DataBlock::new_from_columns(vec![column0, column2])
    };

    // get the latest tbl
    let table = fixture
        .ctx()
        .get_catalog(&catalog_name)?
        .get_table(
            fixture.default_tenant().as_str(),
            fixture.default_db_name().as_str(),
            fixture.default_table_name().as_str(),
        )
        .await?;

    fixture
        .append_commit_blocks(table.clone(), vec![block], false, true)
        .await?;

    // do compact
    let query = format!("optimize table {db_name}.{tbl_name} compact");
    let mut planner = Planner::new(ctx.clone());
    let (plan, _, _) = planner.plan_sql(&query).await?;
    let interpreter = InterpreterFactory::get(ctx.clone(), &plan).await?;
    ctx.get_settings().set_max_threads(1)?;
    let data_stream = interpreter.execute(ctx.clone()).await?;
    let _ = data_stream.try_collect::<Vec<_>>().await;

    // verify statistics has only [0,3]
    let expected_column_ids = vec![0, 3];
    check_segment_column_ids(&fixture, expected_column_ids).await?;

    Ok(())
}
