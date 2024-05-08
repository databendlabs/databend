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

use databend_common_base::base::tokio;
use databend_common_catalog::table_context::TableContext;
use databend_common_exception::Result;
use databend_common_expression::types::DataType;
use databend_common_expression::types::Int32Type;
use databend_common_expression::types::NumberDataType;
use databend_common_expression::types::NumberScalar;
use databend_common_expression::DataBlock;
use databend_common_expression::FromData;
use databend_common_expression::ScalarRef;
use databend_common_pipeline_core::query_spill_prefix;
use databend_common_storage::DataOperator;
use databend_query::spillers::Spiller;
use databend_query::spillers::SpillerConfig;
use databend_query::spillers::SpillerType;
use databend_query::test_kits::TestFixture;

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_spill_with_partition() -> Result<()> {
    let fixture = TestFixture::setup().await?;

    let ctx = fixture.new_query_ctx().await?;
    let tenant = ctx.get_tenant();
    let spiller_config =
        SpillerConfig::create(query_spill_prefix(tenant.tenant_name(), &ctx.get_id()));
    let operator = DataOperator::instance().operator();

    let mut spiller = Spiller::create(ctx, operator, spiller_config, SpillerType::HashJoinBuild)?;

    // Generate data block: two columns, type is i32, 100 rows
    let data = DataBlock::new_from_columns(vec![
        Int32Type::from_data((0..100).collect::<Vec<_>>()),
        Int32Type::from_data((1..101).collect::<Vec<_>>()),
    ]);

    let res = spiller.spill_with_partition(0_u8, data).await;

    assert!(res.is_ok());
    assert!(spiller.partition_location.get(&0).unwrap()[0].starts_with("_query_spill"));

    // Test read spilled data
    let block = DataBlock::concat(&spiller.read_spilled_partition(&(0_u8)).await?)?;
    assert_eq!(block.num_rows(), 100);
    assert_eq!(block.num_columns(), 2);
    for (col_idx, col) in block.columns().iter().enumerate() {
        for (idx, cell) in col
            .value
            .convert_to_full_column(&DataType::Number(NumberDataType::Int32), 100)
            .iter()
            .enumerate()
        {
            assert_eq!(
                cell,
                ScalarRef::Number(NumberScalar::Int32((col_idx + idx) as i32))
            );
        }
    }

    Ok(())
}
