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

use common_base::base::tokio;
use common_base::base::tokio::fs;
use common_base::base::GlobalInstance;
use common_config::InnerConfig;
use common_exception::Result;
use common_expression::types::DataType;
use common_expression::types::Int32Type;
use common_expression::types::NumberDataType;
use common_expression::types::NumberScalar;
use common_expression::DataBlock;
use common_expression::FromData;
use common_expression::ScalarRef;
use common_storage::DataOperator;
use databend_query::spillers::Spiller;
use databend_query::spillers::SpillerConfig;
use databend_query::spillers::SpillerType;
use databend_query::GlobalServices;

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_spill_with_partition() -> Result<()> {
    let mut spiller = create_spiller().await?;
    spiller.partition_set = vec![0, 1, 2];

    // Generate data block: two columns, type is i32, 100 rows
    let data = DataBlock::new_from_columns(vec![
        Int32Type::from_data((0..100).collect::<Vec<_>>()),
        Int32Type::from_data((1..101).collect::<Vec<_>>()),
    ]);

    let res = spiller.spill_with_partition(&(0_u8), &data).await;

    assert!(res.is_ok());
    assert!(spiller.partition_location.get(&0).unwrap()[0].starts_with("_hash_join_build_spill"));

    // Test read spilled data
    let data_blocks = spiller.read_spilled_data(&(0_u8)).await?;
    for block in data_blocks {
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
    }
    // Delete `_hash_join_build_spill` dir
    fs::remove_dir_all("_data/_hash_join_build_spill").await?;
    Ok(())
}

#[cfg(debug_assertions)]
async fn create_spiller() -> Result<Spiller> {
    let thread_name = match std::thread::current().name() {
        None => panic!("thread name is none"),
        Some(thread_name) => thread_name.to_string(),
    };

    GlobalInstance::init_testing(&thread_name);
    GlobalServices::init_with(InnerConfig::default()).await?;

    let spiller_config = SpillerConfig {
        location_prefix: "_hash_join_build_spill".to_string(),
    };
    let operator = DataOperator::instance().operator();
    Ok(Spiller::create(
        operator,
        spiller_config,
        SpillerType::HashJoin,
    ))
}
