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
use databend_common_exception::Result;
use databend_common_expression::types::Int32Type;
use databend_common_expression::DataBlock;
use databend_common_expression::FromData;
use databend_common_storage::DataOperator;
use databend_query::spillers::Spiller;
use databend_query::spillers::SpillerConfig;
use databend_query::spillers::SpillerDiskConfig;
use databend_query::spillers::SpillerType;
use databend_query::test_kits::ConfigBuilder;
use databend_query::test_kits::TestFixture;
use databend_common_catalog::table_context::TableContext;
use databend_storages_common_cache::TempDirManager;

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_spill_fallback_to_remote_when_local_full() -> Result<()> {
    let config = ConfigBuilder::create().build();
    let fixture = TestFixture::setup_with_config(&config).await?;
    let ctx = fixture.new_query_ctx().await?;

    // Prepare a tiny local spill quota to force fallback on the second spill.
    let limit = 512 * 1024; // 512KB
    let temp_manager = TempDirManager::instance();
    let Some(temp_dir) = temp_manager.get_disk_spill_dir(limit, &ctx.get_id()) else {
        // Local spill is not configured in this environment; skip.
        return Ok(());
    };
    let disk_spill = SpillerDiskConfig::new(temp_dir, false)?;

    let spiller_config = SpillerConfig {
        spiller_type: SpillerType::Aggregation,
        location_prefix: ctx.query_id_spill_prefix(),
        disk_spill: Some(disk_spill),
        use_parquet: false,
    };

    let operator = DataOperator::instance().spill_operator();
    let spiller = Spiller::create(ctx.clone(), operator, spiller_config)?;

    // First spill: small enough to stay on local disk.
    let small_block =
        DataBlock::new_from_columns(vec![Int32Type::from_data(vec![1i32; 8 * 1024])]); // ~32KB
    let loc1 = spiller.spill(vec![small_block]).await?;
    assert!(
        loc1.is_local(),
        "first spill should land on local because quota is available"
    );

    // Second spill: large enough to exceed remaining quota and fallback to remote.
    let big_block =
        DataBlock::new_from_columns(vec![Int32Type::from_data(vec![2i32; 512 * 1024])]); // ~2MB
    let loc2 = spiller.spill(vec![big_block]).await?;
    assert!(
        loc2.is_remote(),
        "second spill should fallback to remote when local quota is exhausted"
    );

    // Cleanup the temp directory for this query.
    let _ = temp_manager.drop_disk_spill_dir(&ctx.get_id());

    Ok(())
}
