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
use databend_common_expression::DataBlock;
use databend_common_expression::FromData;
use databend_common_expression::types::Int32Type;
use databend_common_storage::DataOperator;
use databend_query::spillers::Location;
use databend_query::spillers::Spiller;
use databend_query::spillers::SpillerConfig;
use databend_query::spillers::SpillerDiskConfig;
use databend_query::spillers::SpillerType;
use databend_query::test_kits::TestFixture;
use databend_query::test_kits::config_with_spill;
use databend_storages_common_cache::TempDirManager;

/// ASCII flow of the test (data view):
///
///   Int32 rows
///       |
///       v
///   DataBlock (rows_per_block = 32 * 1024)
///       |
///       v
///   Spiller::spill(vec![block.clone()])  -- repeated writes
///       |
///       v
///   Local spill files on disk (quota: 32 * 1024 bytes)
///       |
///       |  accumulate used_local_bytes from each Local(path)
///       v
///   when used_local_bytes > quota
///       |
///       v
///   next spill -> Location::Remote(...)
#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_spill_fallback_to_remote_when_local_full() -> Result<()> {
    let config = config_with_spill();
    let fixture = TestFixture::setup_with_config(&config).await?;
    let ctx = fixture.new_query_ctx().await?;

    // Prepare a small local spill quota to force fallback after a few writes.
    let limit = 32 * 1024; // 32KB
    let temp_manager = TempDirManager::instance();
    let temp_dir = temp_manager
        .get_disk_spill_dir(limit, &ctx.get_id())
        .expect("local spill should be available");
    let disk_spill = SpillerDiskConfig::new(temp_dir, false)?;

    let spiller_config = SpillerConfig {
        spiller_type: SpillerType::Aggregation,
        location_prefix: ctx.query_id_spill_prefix(),
        disk_spill: Some(disk_spill),
        use_parquet: false,
    };

    let operator = DataOperator::instance().spill_operator();
    let spiller = Spiller::create(ctx.clone(), operator, spiller_config)?;

    // Use a stable block size that fits into the tiny quota but will exhaust it after a few rounds.
    let rows_per_block = 32 * 1024;
    let block = DataBlock::new_from_columns(vec![Int32Type::from_data(vec![1i32; rows_per_block])]);

    let loc1 = spiller.spill(vec![block.clone()]).await?;
    let first_block_bytes = match &loc1 {
        Location::Local(path) => path.size(),
        Location::Remote(_) => {
            panic!("first spill should land on local because quota is available")
        }
    };
    let mut locations = vec![loc1];

    let mut used_local_bytes = first_block_bytes;
    let mut saw_remote = false;
    // Cap the attempts to ensure the test finishes quickly even with tiny blocks.
    let max_attempts = (limit / first_block_bytes.max(1)).saturating_add(8);
    for _ in 0..max_attempts {
        let loc = spiller.spill(vec![block.clone()]).await?;
        match &loc {
            Location::Local(path) => {
                used_local_bytes += path.size();
            }
            Location::Remote(_) => {
                saw_remote = true;
                break;
            }
        }
        locations.push(loc);
    }
    assert!(
        saw_remote,
        "should fallback to remote when local quota is exhausted (used_local_bytes={}, limit={}, first_block_bytes={}, attempts={})",
        used_local_bytes, limit, first_block_bytes, max_attempts
    );

    // Cleanup the temp directory for this query.
    let _ = temp_manager.drop_disk_spill_dir(&ctx.get_id());

    Ok(())
}
