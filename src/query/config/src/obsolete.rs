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

use databend_common_exception::ErrorCode;
use databend_common_exception::Result;

use crate::Config;

// Obsoleted configuration entries checking
//
// The following code should be removed from the release after the next release.
// Just give user errors without any detail explanation and migration suggestions.
impl Config {
    pub const fn obsoleted_option_keys() -> &'static [&'static str; 11] {
        &[
            "table_disk_cache_mb_size",
            "table_meta_cache_enabled",
            "table_cache_block_meta_count",
            "table_memory_cache_mb_size",
            "table_disk_cache_root",
            "table_cache_snapshot_count",
            "table_cache_statistic_count",
            "table_cache_segment_count",
            "table_cache_bloom_index_meta_count",
            "table_cache_bloom_index_filter_count",
            "table_cache_bloom_index_data_bytes",
        ]
    }

    pub(crate) fn check_obsoleted(&self) -> Result<()> {
        let check_results = vec![
            Self::check(
                &self.query.table_disk_cache_mb_size,
                "table-disk-cache-mb-size",
                "cache-disk-max-bytes",
                r#"
                    [cache]
                    ...
                    data_cache_storage = "disk"
                    ...
                    [cache.disk]
                    max_bytes = [MAX_BYTES]
                    ...
                  "#,
                "CACHE_DISK_MAX_BYTES",
            ),
            Self::check(
                &self.query.table_meta_cache_enabled,
                "table-meta-cache-enabled",
                "cache-enable-table-meta-cache",
                r#"
                    [cache]
                    enable_table_meta_cache=[true|false]
                  "#,
                "CACHE_ENABLE_TABLE_META_CACHE",
            ),
            Self::check(
                &self.query.table_cache_block_meta_count,
                "table-cache-block-meta-count",
                "N/A",
                "N/A",
                "N/A",
            ),
            Self::check(
                &self.query.table_memory_cache_mb_size,
                "table-memory-cache-mb-size",
                "N/A",
                "N/A",
                "N/A",
            ),
            Self::check(
                &self.query.table_disk_cache_root,
                "table-disk-cache-root",
                "cache-disk-path",
                r#"
                    [cache]
                    ...
                    data_cache_storage = "disk"
                    ...
                    [cache.disk]
                    max_bytes = [MAX_BYTES]
                    path = [PATH]
                    ...
                    "#,
                "CACHE_DISK_PATH",
            ),
            Self::check(
                &self.query.table_cache_snapshot_count,
                "table-cache-snapshot-count",
                "cache-table-meta-snapshot-count",
                r#"
                    [cache]
                    table_meta_snapshot_count = [COUNT]
                    "#,
                "CACHE_TABLE_META_SNAPSHOT_COUNT",
            ),
            Self::check(
                &self.query.table_cache_statistic_count,
                "table-cache-statistic-count",
                "cache-table-meta-statistic-count",
                r#"
                    [cache]
                    table_meta_statistic_count = [COUNT]
                    "#,
                "CACHE_TABLE_META_STATISTIC_COUNT",
            ),
            Self::check(
                &self.query.table_cache_segment_count,
                "table-cache-segment-count",
                "cache-table-meta-segment-count",
                r#"
                    [cache]
                    table_meta_segment_count = [COUNT]
                    "#,
                "CACHE_TABLE_META_SEGMENT_COUNT",
            ),
            Self::check(
                &self.query.table_cache_bloom_index_meta_count,
                "table-cache-bloom-index-meta-count",
                "cache-table-bloom-index-meta-count",
                r#"
                    [cache]
                    table_bloom_index_meta_count = [COUNT]
                    "#,
                "CACHE_TABLE_BLOOM_INDEX_META_COUNT",
            ),
            Self::check(
                &self.query.table_cache_bloom_index_filter_count,
                "table-cache-bloom-index-filter-count",
                "cache-table-bloom-index-filter-count",
                r#"
                    [cache]
                    table_bloom_index_filter_count = [COUNT]
                    "#,
                "CACHE_TABLE_BLOOM_INDEX_FILTER_COUNT",
            ),
            Self::check(
                &self.query.table_cache_bloom_index_data_bytes,
                "table-cache-bloom-index-data-bytes",
                "N/A",
                "N/A",
                "N/A",
            ),
            Self::check(
                &self.cache.table_meta_segment_count,
                "cache-table-meta-segment-count",
                "cache-table-meta-segment-bytes",
                r#"
                    [cache]
                    table_meta_segment_bytes = [BYTES]
                    "#,
                "CACHE_TABLE_META_SEGMENT_BYTES",
            ),
        ];

        let messages = check_results.into_iter().flatten().collect::<Vec<_>>();
        if !messages.is_empty() {
            let errors = messages.join("\n");
            Err(ErrorCode::InvalidConfig(format!("\n{errors}")))
        } else {
            Ok(())
        }
    }

    fn check<T>(
        target: &Option<T>,
        cli_arg_name: &str,
        alternative_cli_arg: &str,
        alternative_toml_config: &str,
        alternative_env_var: &str,
    ) -> Option<String> {
        target.as_ref().map(|_| {
            format!(
                "
 --------------------------------------------------------------
 *** {cli_arg_name} *** is obsoleted :
 --------------------------------------------------------------
   alternative command-line options : {alternative_cli_arg}
   alternative environment variable : {alternative_env_var}
            alternative toml config : {alternative_toml_config}
 --------------------------------------------------------------
"
            )
        })
    }
}
