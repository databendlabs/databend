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

use common_base::base::tokio;
use common_catalog::plan::PushDownInfo;
use common_datavalues::prelude::*;
use common_exception::Result;
use common_sql::executor::table_read_plan::ToReadDataSourcePlan;
use databend_query::interpreters::InterpreterFactory;
use databend_query::sessions::SessionManager;
use databend_query::sessions::SessionType;
use databend_query::sessions::TableContext;
use databend_query::sql::Planner;
use databend_query::stream::ReadDataBlockStream;
use databend_query::table_functions::generate_numbers_parts;
use databend_query::table_functions::NumbersPartInfo;
use databend_query::table_functions::NumbersTable;
use futures::TryStreamExt;
use pretty_assertions::assert_eq;

use crate::tests::ConfigBuilder;
use crate::tests::TestGlobalServices;

#[tokio::test]
async fn test_number_table() -> Result<()> {
    let tbl_args = Some(vec![DataValue::UInt64(8)]);
    let (_guard, ctx) = crate::tests::create_query_context().await?;
    let table = NumbersTable::create("system", "numbers_mt", 1, tbl_args)?;

    let source_plan = table
        .clone()
        .as_table()
        .read_plan(ctx.clone(), Some(PushDownInfo::default()))
        .await?;
    ctx.try_set_partitions(source_plan.parts.clone())?;

    let stream = table
        .as_table()
        .read_data_block_stream(ctx, &source_plan)
        .await?;
    let result = stream.try_collect::<Vec<_>>().await?;
    let block = &result[0];
    assert_eq!(block.num_columns(), 1);

    let expected = vec![
        "+--------+",
        "| number |",
        "+--------+",
        "| 0      |",
        "| 1      |",
        "| 2      |",
        "| 3      |",
        "| 4      |",
        "| 5      |",
        "| 6      |",
        "| 7      |",
        "+--------+",
    ];
    common_datablocks::assert_blocks_sorted_eq(expected, result.as_slice());

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_limit_push_down() -> Result<()> {
    struct Test {
        name: &'static str,
        query: &'static str,
        result: Vec<&'static str>,
    }

    let tests: Vec<Test> = vec![
        Test {
            name: "only-limit",
            query: "select * from numbers_mt(10) limit 2",
            result: vec![
                "+--------+",
                "| number |",
                "+--------+",
                "| 0      |",
                "| 1      |",
                "+--------+",
            ],
        },
        Test {
            name: "limit-with-filter",
            query: "select * from numbers_mt(10) where number > 8 limit 2",
            result: vec![
                "+--------+",
                "| number |",
                "+--------+",
                "| 9      |",
                "+--------+",
            ],
        },
    ];

    let _guard = TestGlobalServices::setup(ConfigBuilder::create().build()).await?;
    for test in tests {
        let session = SessionManager::instance()
            .create_session(SessionType::Dummy)
            .await?;
        let ctx = session.create_query_context().await?;
        let mut planner = Planner::new(ctx.clone());
        let (plan, _, _) = planner.plan_sql(test.query).await?;

        let executor = InterpreterFactory::get(ctx.clone(), &plan).await?;

        let stream = executor.execute(ctx.clone()).await?;
        let result = stream.try_collect::<Vec<_>>().await?;
        let expect = test.result;
        let actual = result.as_slice();
        common_datablocks::assert_blocks_sorted_eq_with_name(test.name, expect, actual);
    }
    Ok(())
}

#[test]
fn test_util_generate_parts() -> Result<()> {
    {
        // deal with remainder
        let ps = generate_numbers_parts(0, 3, 11);

        assert_eq!(3, ps.len());

        let numbers_part = NumbersPartInfo::from_part(&ps.partitions[0])?;
        assert_eq!(numbers_part.part_start, 0);
        assert_eq!(numbers_part.part_end, 3);
        assert_eq!(numbers_part.total, 11);

        let numbers_part = NumbersPartInfo::from_part(&ps.partitions[1])?;
        assert_eq!(numbers_part.part_start, 3);
        assert_eq!(numbers_part.part_end, 6);
        assert_eq!(numbers_part.total, 11);

        let numbers_part = NumbersPartInfo::from_part(&ps.partitions[2])?;
        assert_eq!(numbers_part.part_start, 6);
        assert_eq!(numbers_part.part_end, 11);
        assert_eq!(numbers_part.total, 11);
    }

    {
        // total is zero
        let ps = generate_numbers_parts(0, 3, 0);

        assert_eq!(1, ps.len());
        let numbers_part = NumbersPartInfo::from_part(&ps.partitions[0])?;
        assert_eq!(numbers_part.part_start, 0);
        assert_eq!(numbers_part.part_end, 0);
        assert_eq!(numbers_part.total, 0);
    }
    {
        // only one part, total < workers
        let ps = generate_numbers_parts(0, 3, 2);

        assert_eq!(1, ps.len());
        let numbers_part = NumbersPartInfo::from_part(&ps.partitions[0])?;
        assert_eq!(numbers_part.part_start, 0);
        assert_eq!(numbers_part.part_end, 2);
        assert_eq!(numbers_part.total, 2);
    }

    Ok(())
}
