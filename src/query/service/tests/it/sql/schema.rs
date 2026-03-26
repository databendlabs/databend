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

use databend_common_expression::DataBlock;
use databend_common_expression::ScalarRef;
use databend_common_expression::types::DataType;
use databend_common_expression::types::number::NumberDataType;
use databend_common_expression::types::number::NumberScalar;
use databend_query::interpreters::InterpreterFactory;
use databend_query::sql::Planner;
use databend_query::test_kits::TestFixture;
use futures_util::TryStreamExt;

#[tokio::test(flavor = "multi_thread")]
async fn test_full_outer_join_using_reports_nullable_result_schema() -> anyhow::Result<()> {
    let fixture = TestFixture::setup().await?;
    let ctx = fixture.new_query_ctx().await?;
    let sql = "SELECT x FROM (SELECT 1::INT64 AS x) AS a FULL OUTER JOIN (SELECT 2::INT64 AS x) AS b USING (x) ORDER BY x NULLS LAST";

    let mut planner = Planner::new(ctx.clone());
    let (plan, _) = planner.plan_sql(sql).await?;

    let expected = DataType::Number(NumberDataType::Int64).wrap_nullable();
    assert_eq!(plan.schema().field(0).data_type(), &expected);

    let executor = InterpreterFactory::get(ctx.clone(), &plan).await?;
    let blocks: Vec<DataBlock> = executor.execute(ctx).await?.try_collect().await?;
    let block = DataBlock::concat(&blocks)?;
    assert_eq!(block.infer_schema().field(0).data_type(), &expected);
    assert_eq!(block.num_rows(), 2, "unexpected rows: {}", block.num_rows());

    let values = (0..block.num_rows())
        .map(
            |row| match block.get_by_offset(0).index(row).expect("scalar at row") {
                ScalarRef::Number(NumberScalar::Int64(value)) => value,
                other => panic!("unexpected scalar type at row {row}: {other:?}"),
            },
        )
        .collect::<Vec<_>>();
    assert_eq!(values, vec![1, 2]);

    Ok(())
}
