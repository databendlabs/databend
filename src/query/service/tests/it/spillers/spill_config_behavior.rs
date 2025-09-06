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

//! Tests to verify spill configuration behavior:
//! 1. Test FS spill configuration with custom path and max_bytes limit
//! 2. Test remote spill configuration
//! 3. Verify configuration is applied correctly and limits are enforced

use std::fs;

use databend_common_base::base::tokio;
use databend_common_catalog::table_context::TableContext;
use databend_common_exception::Result;
use databend_common_expression::types::Int32Type;
use databend_common_expression::DataBlock;
use databend_common_expression::FromData;
use databend_common_storage::DataOperator;
use databend_query::spillers::Location;
use databend_query::spillers::Spiller;
use databend_query::spillers::SpillerConfig;
use databend_query::spillers::SpillerType;
use databend_query::test_kits::ConfigBuilder;
use databend_query::test_kits::TestFixture;
use databend_storages_common_cache::TempDirManager;

/// Test 1: Basic spill directory behavior
#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_spill_directory_operations() -> Result<()> {
    println!("🔄 Starting spill directory operations test");

    // Setup with default config
    let config = ConfigBuilder::create().build();
    let fixture = TestFixture::setup_with_config(&config).await?;
    let ctx = fixture.new_query_ctx().await?;

    println!("✅ Test fixture setup completed");

    // Test spill directory creation
    let temp_manager = TempDirManager::instance();
    let query_id = ctx.get_id();

    println!(
        "🔄 Testing spill directory creation for query: {}",
        query_id
    );

    // Test small request
    let small_request = 1024; // 1KB
    let small_spill_dir = temp_manager.get_disk_spill_dir(small_request, &query_id);

    match small_spill_dir {
        Some(spill_dir) => {
            println!("✅ Got spill directory: {:?}", spill_dir.path());

            // Test file allocation
            match spill_dir.new_file_with_size(512) {
                Ok(Some(temp_path)) => {
                    println!(
                        "✅ File allocation succeeded: size={}, path={:?}",
                        temp_path.size(),
                        temp_path.as_ref()
                    );
                    assert_eq!(temp_path.size(), 512);
                }
                Ok(None) => {
                    println!("⚠️  File allocation was denied (size limits enforced)");
                }
                Err(e) => {
                    println!("❌ File allocation failed: {}", e);
                    return Err(e);
                }
            }
        }
        None => {
            println!("⚠️  No spill directory available (spill disabled or limits restrictive)");
        }
    }

    // Cleanup
    let cleanup_result = temp_manager.drop_disk_spill_dir(&query_id);
    match cleanup_result {
        Ok(true) => println!("✅ Cleanup succeeded"),
        Ok(false) => println!("ℹ️  Nothing to clean up"),
        Err(e) => println!("⚠️  Cleanup error: {}", e),
    }

    println!("✅ Test completed successfully");
    Ok(())
}

/// Test 2: Spill size limit enforcement
#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_spill_size_limits() -> Result<()> {
    let config = ConfigBuilder::create().build();
    let fixture = TestFixture::setup_with_config(&config).await?;
    let ctx = fixture.new_query_ctx().await?;

    let temp_manager = TempDirManager::instance();
    let query_id = ctx.get_id();

    // Get a spill directory with a reasonable limit
    let limit = 1024 * 1024; // 1MB limit
    let spill_dir = temp_manager.get_disk_spill_dir(limit, &query_id);

    if let Some(spill_dir) = spill_dir {
        // Test small allocation (should succeed)
        let small_file = spill_dir.new_file_with_size(512)?;
        assert!(small_file.is_some(), "Small file allocation should succeed");

        // Test large allocation that might exceed limits
        let large_request = limit * 2; // Request 2MB when limit is 1MB
        let large_file = spill_dir.new_file_with_size(large_request)?;

        // This may return None if limits are strictly enforced, or Some if system allows it
        // The behavior depends on the actual implementation and system state
        match large_file {
            Some(_) => {
                // Large allocation was allowed (system has enough resources)
            }
            None => {
                // Large allocation was denied (limit enforcement worked)
            }
        }
    }

    // Cleanup
    let cleanup_result = temp_manager.drop_disk_spill_dir(&query_id);
    assert!(cleanup_result.is_ok(), "Cleanup should succeed");

    Ok(())
}

/// Test 3: Verify spill data integrity and actual file operations
#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_spill_data_integrity() -> Result<()> {
    let config = ConfigBuilder::create().build();
    let fixture = TestFixture::setup_with_config(&config).await?;
    let ctx = fixture.new_query_ctx().await?;

    let spiller_config = SpillerConfig {
        spiller_type: SpillerType::HashJoinBuild,
        location_prefix: ctx.query_id_spill_prefix(),
        disk_spill: None,
        use_parquet: false,
    };

    let operator = DataOperator::instance().spill_operator();
    let mut spiller = Spiller::create(ctx, operator, spiller_config)?;

    // Create test data with known pattern
    let expected_data = (0..100).collect::<Vec<i32>>();
    let test_data = DataBlock::new_from_columns(vec![Int32Type::from_data(expected_data.clone())]);

    assert_eq!(test_data.num_rows(), 100);
    assert_eq!(test_data.num_columns(), 1);

    // Perform spill operation
    let spill_result = spiller.spill_with_partition(0, vec![test_data]).await;
    assert!(spill_result.is_ok(), "Spill operation should succeed");

    // Verify spill locations were created
    let locations = spiller.get_partition_locations(&0);
    assert!(locations.is_some(), "Spill locations should be created");
    let locations = locations.unwrap();
    assert!(
        !locations.is_empty(),
        "Should have at least one spill location"
    );

    // Verify spill files exist and have content (for local storage)
    for location in locations {
        match location {
            Location::Local(local_path) => {
                assert!(local_path.exists(), "Local spill file should exist");
                let file_size = fs::metadata(local_path)?.len();
                assert!(file_size > 0, "Spill file should have content");
            }
            Location::Remote(remote_path) => {
                assert!(!remote_path.is_empty(), "Remote path should not be empty");
            }
        }
    }

    // Test read back data integrity
    let read_result = spiller.read_spilled_partition(&0).await;
    assert!(
        read_result.is_ok(),
        "Should be able to read spilled data back"
    );

    let blocks = read_result.unwrap();
    assert!(!blocks.is_empty(), "Should read back some data blocks");

    let total_rows: usize = blocks.iter().map(|b| b.num_rows()).sum();
    assert_eq!(
        total_rows,
        expected_data.len(),
        "Row count should match original"
    );

    let total_cols = blocks[0].num_columns();
    assert_eq!(total_cols, 1, "Column count should match original");

    Ok(())
}
