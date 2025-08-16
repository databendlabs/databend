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

use databend_common_base::base::tokio;
use databend_common_catalog::plan::PushDownInfo;
use databend_common_catalog::table_args::TableArgs;
use databend_common_exception::Result;
use databend_common_expression::Scalar;
use databend_common_sql::executor::table_read_plan::ToReadDataSourcePlan;
use databend_query::sessions::TableContext;
use databend_query::stream::ReadDataBlockStream;
use databend_query::table_functions::generate_numbers_parts;
use databend_query::table_functions::NumbersPartInfo;
use databend_query::table_functions::NumbersTable;
use databend_query::test_kits::TestFixture;
use futures::TryStreamExt;
use pretty_assertions::assert_eq;

#[tokio::test(flavor = "multi_thread")]
async fn test_number_table() -> Result<()> {
    let fixture = TestFixture::setup().await?;
    let ctx = fixture.new_query_ctx().await?;

    let tbl_args = TableArgs::new_positioned(vec![Scalar::from(8u64)]);
    let table = NumbersTable::create("system", "numbers_mt", 1, tbl_args)?;

    let source_plan = table
        .clone()
        .as_table()
        .read_plan(
            ctx.clone(),
            Some(PushDownInfo::default()),
            None,
            false,
            true,
        )
        .await?;
    ctx.set_partitions(source_plan.parts.clone())?;

    let stream = table
        .as_table()
        .read_data_block_stream(ctx, &source_plan)
        .await?;
    let result = stream.try_collect::<Vec<_>>().await?;
    let block = &result[0];
    assert_eq!(block.num_columns(), 1);

    let expected = vec![
        "+----------+",
        "| Column 0 |",
        "+----------+",
        "| 0        |",
        "| 1        |",
        "| 2        |",
        "| 3        |",
        "| 4        |",
        "| 5        |",
        "| 6        |",
        "| 7        |",
        "+----------+",
    ];
    databend_common_expression::block_debug::assert_blocks_sorted_eq(expected, result.as_slice());

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
