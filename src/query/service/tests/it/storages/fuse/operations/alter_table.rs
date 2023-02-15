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
use storages_common_table_meta::meta::TableSnapshot;
use storages_common_table_meta::meta::Versioned;
use storages_common_table_meta::table::OPT_KEY_SNAPSHOT_LOCATION;

use crate::storages::fuse::table_test_fixture::TestFixture;

#[tokio::test(flavor = "multi_thread")]
async fn test_fuse_table_optimize_alter_table() -> Result<()> {
    let fixture = TestFixture::new().await;
    let ctx = fixture.ctx();
    let tbl_name = fixture.default_table_name();
    let db_name = fixture.default_db_name();

    fixture.create_normal_table().await?;

    let table = fixture.latest_default_table().await?;
    let fuse_table = FuseTable::try_from_table(table.as_ref())?;

    // insert values
    let table = fixture.latest_default_table().await?;
    let num_blocks = 1;
    let stream = TestFixture::gen_sample_blocks_stream(num_blocks, 1);

    let blocks = stream.try_collect().await?;
    fixture
        .append_commit_blocks(table.clone(), blocks, false, true)
        .await?;

    // get statistics
    let snapshot_loc = table
        .get_table_info()
        .options()
        .get(OPT_KEY_SNAPSHOT_LOCATION)
        .unwrap();
    let reader = MetaReaders::segment_info_reader(
        fuse_table.get_operator(),
        TestFixture::default_table_schema(),
    );
    let expected_column_ids = vec![0, 1];
    let params = LoadParams {
        location: snapshot_loc.clone(),
        len_hint: None,
        ver: TableSnapshot::VERSION,
    };

    let segment_info = reader.read(&params).await?;
    assert_eq!(
        segment_info.blocks[0]
            .col_stats
            .keys()
            .cloned()
            .collect::<Vec<ColumnId>>(),
        expected_column_ids,
    );

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
    let fields = vec![
        TestFixture::default_table_schema().fields()[0].clone(),
        TableField::new("b", TableDataType::Number(NumberDataType::UInt64)),
    ];
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

    // insert values
    let block = {
        let column0 = Int32Type::from_data(vec![1, 2]);
        let column1 = UInt64Type::from_data(vec![3, 4]);

        DataBlock::new_from_columns(vec![column0, column1])
    };

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

    // verify statistics
    let expected_column_ids = vec![0, 2];
    let segment_info = reader.read(&params).await?;
    assert_eq!(
        segment_info.blocks[0]
            .col_stats
            .keys()
            .cloned()
            .collect::<Vec<ColumnId>>(),
        expected_column_ids,
    );

    Ok(())
}
