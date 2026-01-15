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

use std::assert_matches::assert_matches;

use databend_common_catalog::table_context::TableContext;
use databend_common_exception::Result;
use databend_common_expression::DataBlock;
use databend_common_expression::FromData;
use databend_common_expression::ScalarRef;
use databend_common_expression::types::Int32Type;
use databend_common_expression::types::NumberScalar;
use databend_common_storage::DataOperator;
use databend_query::spillers::Location;
use databend_query::spillers::Spiller;
use databend_query::spillers::SpillerConfig;
use databend_query::spillers::SpillerType;
use databend_query::test_kits::TestFixture;
use tokio;

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_spill_with_partition() -> Result<()> {
    let fixture = TestFixture::setup().await?;

    let ctx = fixture.new_query_ctx().await?;
    let location_prefix = ctx.query_id_spill_prefix();
    let spiller_config = SpillerConfig {
        spiller_type: SpillerType::HashJoinBuild,
        location_prefix,
        disk_spill: None,
        use_parquet: ctx.get_settings().get_spilling_file_format()?.is_parquet(),
    };
    let operator = DataOperator::instance().spill_operator();

    let mut spiller = Spiller::create(ctx, operator, spiller_config)?;

    // Generate data block: two columns, type is i32, 100 rows
    let data = DataBlock::new_from_columns(vec![
        Int32Type::from_data((0..100).collect::<Vec<_>>()),
        Int32Type::from_data((1..101).collect::<Vec<_>>()),
    ]);

    let res = spiller.spill_with_partition(0, vec![data]).await;

    assert!(res.is_ok());
    let location = &spiller.get_partition_locations(&0).unwrap()[0];
    assert_matches!(location, Location::Remote(_));

    // Test read spilled data
    let block = DataBlock::concat(&spiller.read_spilled_partition(&(0)).await?)?;
    assert_eq!(block.num_rows(), 100);
    assert_eq!(block.num_columns(), 2);
    for (col_idx, col) in block.columns().iter().enumerate() {
        for (idx, cell) in col.to_column().iter().enumerate() {
            assert_eq!(
                cell,
                ScalarRef::Number(NumberScalar::Int32((col_idx + idx) as i32))
            );
        }
    }

    Ok(())
}
