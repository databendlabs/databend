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

use std::collections::HashMap;
use std::path::Path;
use std::path::PathBuf;
use std::sync::Arc;

use databend_common_config::CacheConfig;
use databend_common_config::DiskCacheKeyReloadPolicy;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use log::info;

use crate::CacheKind;
use crate::DiskCacheAccessor;
use crate::DiskCacheBuilder;
use crate::Pool;

// Minimum threshold for table data disk cache size (in bytes).
// Any configuration value less than this threshold will be ignored,
// and table data disk cache will not be enabled.
// This threshold exists to accommodate the current cloud platform logic:
// When attempting to disable table data cache in compute node configurations, setting the disk
// cache size to zero prevents the physical volume from being loaded, so this threshold provides
// a better approach.
// Eventually, we should refactor the compute node configurations instead, to make those options more sensible.
const TABLE_DATA_DISK_CACHE_SIZE_THRESHOLD: usize = 1024;

/// The disk tiers of the kinds that define one, sized by `config`. Disk tiers exist only in
/// EE mode.
pub(crate) fn disk_tiers(
    config: &CacheConfig,
    tenant_id: &str,
    ee_mode: bool,
) -> Result<HashMap<CacheKind, Arc<DiskCacheAccessor>>> {
    let queue_size: u32 = if config.table_data_cache_population_queue_size > 0 {
        config.table_data_cache_population_queue_size
    } else {
        std::cmp::max(
            1,
            std::thread::available_parallelism()
                .expect("Cannot get thread count")
                .get() as u32,
        ) * 5
    };
    info!("[CacheManager] On-disk cache population queue size: {queue_size}");

    // As before, `enable_table_meta_cache = false` also turns off the disk tiers of the table
    // meta and index kinds. The shared backend ignores the option.
    let meta_enabled = config.enable_table_meta_cache || config.shared_memory_enabled;
    let mut disks = HashMap::new();
    for kind in CacheKind::all() {
        let Some(disk) = kind.info().disk else {
            continue;
        };
        if kind.pool() != Pool::Data && !meta_enabled {
            continue;
        }
        let bytes = usize::try_from((disk.bytes)(config)).map_err(|_| {
            ErrorCode::BadArguments(format!(
                "disk cache size of {} exceeds this platform",
                kind.name()
            ))
        })?;
        // Same names as the pre-refactor hybrid caches: `disk_cache_<...>`.
        let name = format!("disk_{}", kind.name().trim_start_matches("memory_"));
        let path = PathBuf::from(&config.disk_cache_config.path)
            .join(tenant_id)
            .join(disk.dir);
        if let Some(cache) = build(
            name,
            &path,
            queue_size,
            bytes,
            (disk.reload_policy)(config),
            config.disk_cache_config.sync_data,
            ee_mode,
        )? {
            disks.insert(kind, Arc::new(cache));
        }
    }
    Ok(disks)
}

fn build(
    cache_name: String,
    path: &Path,
    population_queue_size: u32,
    disk_cache_bytes_size: usize,
    disk_cache_key_reload_policy: DiskCacheKeyReloadPolicy,
    sync_data: bool,
    ee_mode: bool,
) -> Result<Option<DiskCacheAccessor>> {
    if disk_cache_bytes_size <= TABLE_DATA_DISK_CACHE_SIZE_THRESHOLD || !ee_mode {
        info!(
            "[CacheManager] On-disk cache {cache_name} disabled, size {disk_cache_bytes_size}, threshold {TABLE_DATA_DISK_CACHE_SIZE_THRESHOLD}, ee mode {ee_mode}"
        );
        Ok(None)
    } else {
        let cache_holder = DiskCacheBuilder::try_build_disk_cache(
            cache_name,
            &path.to_path_buf(),
            population_queue_size,
            disk_cache_bytes_size,
            disk_cache_key_reload_policy,
            sync_data,
        )?;
        Ok(Some(cache_holder))
    }
}

#[cfg(test)]
mod tests {
    use tempfile::TempDir;

    use super::*;

    #[test]
    fn test_disk_cache_size_threshold() -> Result<()> {
        // Create a temporary directory for the test
        let temp_dir = TempDir::new().unwrap();
        let cache_path = temp_dir.path().to_path_buf();

        // Test parameters
        let cache_name = "test_threshold_cache".to_string();
        let population_queue_size = 5;
        let ee_mode = true; // Always use EE mode for this test
        let sync_data = false;
        let key_reload_policy = DiskCacheKeyReloadPolicy::Fuzzy;

        // Case 1: Size below threshold (should disable cache)
        let below_threshold_size = TABLE_DATA_DISK_CACHE_SIZE_THRESHOLD - 1;
        let result = build(
            cache_name.clone(),
            &cache_path,
            population_queue_size,
            below_threshold_size,
            key_reload_policy.clone(),
            sync_data,
            ee_mode,
        )?;
        assert!(
            result.is_none(),
            "Disk cache should be disabled when size is below threshold"
        );

        // Case 2: Size exactly at threshold (should disable cache)
        let at_threshold_size = TABLE_DATA_DISK_CACHE_SIZE_THRESHOLD;
        let result = build(
            cache_name.clone(),
            &cache_path,
            population_queue_size,
            at_threshold_size,
            key_reload_policy.clone(),
            sync_data,
            ee_mode,
        )?;
        assert!(
            result.is_none(),
            "Disk cache should be disabled when size equals threshold"
        );

        // Case 3: Size above threshold (should enable cache)
        let above_threshold_size = TABLE_DATA_DISK_CACHE_SIZE_THRESHOLD + 1024;
        let result = build(
            cache_name.clone(),
            &cache_path,
            population_queue_size,
            above_threshold_size,
            key_reload_policy.clone(),
            sync_data,
            ee_mode,
        )?;
        assert!(
            result.is_some(),
            "Disk cache should be enabled when size is above threshold"
        );

        Ok(())
    }
}
