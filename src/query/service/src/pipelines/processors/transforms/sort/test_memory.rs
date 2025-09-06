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

use std::sync::Arc;

use bytesize::ByteSize;
use databend_common_base::mem_allocator::TrackingGlobalAllocator;
use databend_common_base::runtime::GLOBAL_MEM_STAT;
use databend_common_catalog::table_context::TableContext;
use databend_common_config::SpillConfig;
use databend_common_exception::Result;
use databend_common_expression::types::decimal::DecimalSize;
use databend_common_expression::types::number::Int32Type;
use databend_common_expression::types::number::Int64Type;
use databend_common_expression::types::DateType;
use databend_common_expression::types::Decimal128Type;
use databend_common_expression::types::StringType;
use databend_common_expression::Column;
use databend_common_expression::DataBlock;
use databend_common_expression::FromData;
use databend_common_pipeline_transforms::traits::Location;
use databend_common_storage::DataOperator;
use databend_storages_common_cache::TempDirManager;
use rand::Rng;

use crate::sessions::QueryContext;
use crate::spillers::Spiller;
use crate::spillers::SpillerConfig;
use crate::spillers::SpillerDiskConfig;
use crate::spillers::SpillerType;
use crate::test_kits::ConfigBuilder;
use crate::test_kits::TestFixture;

#[global_allocator]
pub static GLOBAL_ALLOCATOR: TrackingGlobalAllocator = TrackingGlobalAllocator::create();

pub struct MockDataGen;

impl MockDataGen {
    /// Generate a complete TPC-H orders table DataBlock with specified number of rows
    pub fn generate_orders_table(rows: usize) -> DataBlock {
        let columns = vec![
            Self::o_orderkey(rows),
            Self::o_custkey(rows),
            Self::o_orderstatus(rows),
            Self::o_totalprice(rows).unwrap(),
            Self::o_orderdate(rows).unwrap(),
            Self::o_orderpriority(rows),
            Self::o_clerk(rows),
            Self::o_shippriority(rows),
            Self::o_comment(rows),
        ];

        DataBlock::new_from_columns(columns)
    }

    pub fn o_orderkey(rows: usize) -> Column {
        Int64Type::from_data((0..rows).map(|i| 215898241 + i as i64).collect())
    }

    pub fn o_custkey(rows: usize) -> Column {
        let mut rng = rand::thread_rng();
        Int64Type::from_data((0..rows).map(|_| rng.gen_range(1..15000000)).collect())
    }

    pub fn o_orderstatus(rows: usize) -> Column {
        let mut rng = rand::thread_rng();
        let statuses = ["O", "F", "P"];
        StringType::from_data(
            (0..rows)
                .map(|_| statuses[rng.gen_range(0..statuses.len())].to_string())
                .collect(),
        )
    }

    pub fn o_totalprice(rows: usize) -> Result<Column> {
        let mut rng = rand::thread_rng();

        // Generate random decimal values between 1000.00 and 500000.00
        let decimal_values: Vec<i128> = (0..rows)
            .map(|_| {
                let random_price = rng.gen_range(1000.0..500000.0);
                // Scale by 2 decimal places (multiply by 100)
                (random_price * 100.0) as i128
            })
            .collect();

        let size = DecimalSize::new_unchecked(15, 2);
        Ok(Decimal128Type::from_data_with_size(
            decimal_values,
            Some(size),
        ))
    }

    pub fn o_orderdate(rows: usize) -> Result<Column> {
        let mut rng = rand::thread_rng();

        // Generate random dates between 1992-01-01 and 1998-12-31
        // Date range: from day 8036 (1992-01-01) to day 10592 (1998-12-31)
        let start_day = 8036; // 1992-01-01
        let end_day = 10592; // 1998-12-31

        let date_values: Vec<i32> = (0..rows)
            .map(|_| rng.gen_range(start_day..=end_day))
            .collect();

        Ok(DateType::from_data(date_values))
    }

    pub fn o_orderpriority(rows: usize) -> Column {
        let mut rng = rand::thread_rng();
        let priorities = ["1-URGENT", "2-HIGH", "3-MEDIUM", "4-NOT SPECIFIED", "5-LOW"];
        StringType::from_data(
            (0..rows)
                .map(|_| priorities[rng.gen_range(0..priorities.len())].to_string())
                .collect(),
        )
    }

    pub fn o_clerk(rows: usize) -> Column {
        let mut rng = rand::thread_rng();
        StringType::from_data(
            (0..rows)
                .map(|_| {
                    let clerk_id = rng.gen_range(1..1000000);
                    format!("Clerk#{:09}", clerk_id)
                })
                .collect(),
        )
    }

    pub fn o_shippriority(rows: usize) -> Column {
        Int32Type::from_data(vec![0; rows])
    }

    pub fn o_comment(rows: usize) -> Column {
        let mut rng = rand::thread_rng();
        let comment_parts = [
            "carefully",
            "quickly",
            "furiously",
            "fluffily",
            "blithely",
            "express",
            "ironic",
            "special",
            "regular",
            "final",
            "deposits",
            "packages",
            "instructions",
            "requests",
            "accounts",
            "above",
            "across",
            "against",
            "along",
            "around",
            "the",
            "some",
            "all",
            "many",
            "few",
        ];

        StringType::from_data(
            (0..rows)
                .map(|_| {
                    let num_words = rng.gen_range(3..8);
                    let words: Vec<&str> = (0..num_words)
                        .map(|_| comment_parts[rng.gen_range(0..comment_parts.len())])
                        .collect();
                    words.join(" ")
                })
                .collect(),
        )
    }
}

async fn init() -> Result<(TestFixture, Arc<QueryContext>, Spiller)> {
    let mut config = ConfigBuilder::create().config();
    config.spill = SpillConfig::new_for_test("test_data".to_string(), 0.01, 1 << 30);
    let fixture = TestFixture::setup_with_config(&config).await?;

    let ctx = fixture.new_query_ctx().await?;

    let temp_dir_manager = TempDirManager::instance();
    let disk_spill = temp_dir_manager
        .get_disk_spill_dir(1024 * 1024 * 1024 * 10, &ctx.get_id())
        .map(|temp_dir| SpillerDiskConfig::new(temp_dir, true))
        .transpose()?;

    // Create spiller configuration (using remote storage)
    let location_prefix = ctx.query_id_spill_prefix();
    let spiller_config = SpillerConfig {
        spiller_type: SpillerType::OrderBy,
        location_prefix,
        disk_spill,
        use_parquet: true,
    };
    let operator = DataOperator::instance().spill_operator();
    let spiller = Spiller::create(ctx.clone(), operator, spiller_config)?;
    Ok((fixture, ctx, spiller))
}

#[derive(Debug)]
#[allow(dead_code)]
pub struct TestResult {
    pub rows: usize,
    pub memory_size: ByteSize,
    pub spill_size: ByteSize,
    pub memory_usage_real: ByteSize,
}

async fn run_spill_test(spiller: &Spiller, rows: usize) -> Result<TestResult> {
    let data_block = MockDataGen::generate_orders_table(rows);
    let memory_size = ByteSize(data_block.memory_size() as _);

    // Spill the data block
    let Location::Local(path) = spiller.spill(vec![data_block]).await? else {
        unreachable!()
    };

    let spill_size = ByteSize(path.metadata().unwrap().len());

    let before = GLOBAL_MEM_STAT.get_memory_usage();
    let data_blocks = (0..100)
        .map(|_| MockDataGen::generate_orders_table(rows))
        .collect::<Vec<_>>();
    let real_usage = (GLOBAL_MEM_STAT.get_memory_usage() - before) / 100;
    drop(data_blocks);

    Ok(TestResult {
        rows,
        memory_size,
        spill_size,
        memory_usage_real: ByteSize(real_usage as _),
    })
}

// #[tokio::test] manual test only
#[allow(dead_code)]
async fn test_block_spill_sizes() -> Result<()> {
    let (fixture, _, spiller) = init().await?;
    fixture.keep_alive();

    // Test different row counts
    let row_counts = vec![1000, 10000, 50000, 65535];
    for rows in row_counts {
        let result = run_spill_test(&spiller, rows).await?;
        println!("{result:?}");
    }

    // TestResult { rows: 1000, memory_size: 145.9 KiB (149387 bytes), spill_size: 51.4 KiB (52645 bytes), memory_usage_real: 164.2 KiB (168147 bytes) }
    // TestResult { rows: 10000, memory_size: 1.4 MiB (1490811 bytes), spill_size: 470.3 KiB (481620 bytes), memory_usage_real: 1.8 MiB (1854174 bytes) }
    // TestResult { rows: 50000, memory_size: 7.1 MiB (7448980 bytes), spill_size: 2.2 MiB (2348600 bytes), memory_usage_real: 8.2 MiB (8582166 bytes) }
    // TestResult { rows: 65535, memory_size: 9.3 MiB (9761210 bytes), spill_size: 2.9 MiB (3045927 bytes), memory_usage_real: 11.7 MiB (12298628 bytes) }

    Ok(())
}
