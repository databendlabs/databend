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
use std::sync::Arc;

use common_config::GlobalConfig;
use common_exception::ErrorCode;
use common_exception::Result;
use common_meta_app::principal::UserSettingValue;
use once_cell::sync::OnceCell;

static DEFAULT_SETTINGS: OnceCell<Arc<DefaultSettings>> = OnceCell::new();

#[derive(Clone, Debug)]
pub struct DefaultSettingValue {
    pub(crate) value: UserSettingValue,
    pub(crate) desc: &'static str,
    pub(crate) possible_values: Option<Vec<&'static str>>,
    pub(crate) display_in_show_settings: bool,
}

#[derive(Clone)]
pub struct DefaultSettings {
    pub(crate) settings: HashMap<String, DefaultSettingValue>,
}

impl DefaultSettings {
    pub fn instance() -> Result<Arc<DefaultSettings>> {
        Ok(Arc::clone(DEFAULT_SETTINGS.get_or_try_init(|| -> Result<Arc<DefaultSettings>> {
            let num_cpus = Self::num_cpus();
            let max_memory_usage = Self::max_memory_usage()?;
            let recluster_block_size = Self::recluster_block_size()?;
            let default_max_storage_io_requests = Self::storage_io_requests(num_cpus);

            let default_settings = HashMap::from([
                ("max_block_size", DefaultSettingValue {
                    value: UserSettingValue::UInt64(65536),
                    desc: "Sets the maximum byte size of a single data block that can be read.",
                    possible_values: None,
                    display_in_show_settings: true,
                }),
                ("max_threads", DefaultSettingValue {
                    value: UserSettingValue::UInt64(num_cpus),
                    desc: "Sets the maximum number of threads to execute a request.",
                    possible_values: None,
                    display_in_show_settings: true,
                }),
                ("max_memory_usage", DefaultSettingValue {
                    value: UserSettingValue::UInt64(max_memory_usage),
                    desc: "Sets the maximum memory usage in bytes for processing a single query.",
                    possible_values: None,
                    display_in_show_settings: true,
                }),
                ("retention_period", DefaultSettingValue {
                    // unit of retention_period is hour
                    value: UserSettingValue::UInt64(12),
                    desc: "Sets the retention period in hours.",
                    possible_values: None,
                    display_in_show_settings: true,
                }),
                ("max_storage_io_requests", DefaultSettingValue {
                    value: UserSettingValue::UInt64(default_max_storage_io_requests),
                    desc: "Sets the maximum number of concurrent I/O requests.",
                    possible_values: None,
                    display_in_show_settings: true,
                }),
                ("storage_io_min_bytes_for_seek", DefaultSettingValue {
                    value: UserSettingValue::UInt64(48),
                    desc: "Sets the minimum byte size of data that must be read from storage in a single I/O operation \
                when seeking a new location in the data file.",
                    possible_values: None,
                    display_in_show_settings: true,
                }),
                ("storage_io_max_page_bytes_for_read", DefaultSettingValue {
                    value: UserSettingValue::UInt64(512 * 1024),
                    desc: "Sets the maximum byte size of data pages that can be read from storage in a single I/O operation.",
                    possible_values: None,
                    display_in_show_settings: true,
                }),
                ("flight_client_timeout", DefaultSettingValue {
                    value: UserSettingValue::UInt64(60),
                    desc: "Sets the maximum time in seconds that a flight client request can be processed.",
                    possible_values: None,
                    display_in_show_settings: true,
                }),
                ("storage_read_buffer_size", DefaultSettingValue {
                    value: UserSettingValue::UInt64(1024 * 1024),
                    desc: "Sets the byte size of the buffer used for reading data into memory.",
                    possible_values: None,
                    display_in_show_settings: true,
                }),
                ("input_read_buffer_size", DefaultSettingValue {
                    value: UserSettingValue::UInt64(1024 * 1024),
                    desc: "Sets the memory size in bytes allocated to the buffer used by the buffered reader to read data from storage.",
                    possible_values: None,
                    display_in_show_settings: true,
                }),
                ("timezone", DefaultSettingValue {
                    value: UserSettingValue::String("UTC".to_owned()),
                    desc: "Sets the timezone.",
                    possible_values: None,
                    display_in_show_settings: true,
                }),
                ("group_by_two_level_threshold", DefaultSettingValue {
                    value: UserSettingValue::UInt64(20000),
                    desc: "Sets the number of keys in a GROUP BY operation that will trigger a two-level aggregation.",
                    possible_values: None,
                    display_in_show_settings: true,
                }),
                ("max_inlist_to_or", DefaultSettingValue {
                    value: UserSettingValue::UInt64(3),
                    desc: "Sets the maximum number of values that can be included in an IN expression to be converted to an OR operator.",
                    possible_values: None,
                    display_in_show_settings: true,
                }),
                ("unquoted_ident_case_sensitive", DefaultSettingValue {
                    value: UserSettingValue::UInt64(0),
                    desc: "Determines whether Databend treats unquoted identifiers as case-sensitive.",
                    possible_values: None,
                    display_in_show_settings: true,
                }),
                ("quoted_ident_case_sensitive", DefaultSettingValue {
                    value: UserSettingValue::UInt64(1),
                    desc: "Determines whether Databend treats quoted identifiers as case-sensitive.",
                    possible_values: None,
                    display_in_show_settings: true,
                }),
                ("sql_dialect", DefaultSettingValue {
                    value: UserSettingValue::String("PostgreSQL".to_owned()),
                    desc: "Sets the SQL dialect. Available values include \"PostgreSQL\", \"MySQL\", and \"Hive\".",
                    possible_values: Some(vec!["PostgreSQL", "MySQL", "Hive"]),
                    display_in_show_settings: true,
                }),
                ("enable_dphyp", DefaultSettingValue {
                    value: UserSettingValue::UInt64(1),
                    desc: "Enables dphyp join order algorithm.",
                    possible_values: None,
                    display_in_show_settings: true,
                }),
                ("enable_cbo", DefaultSettingValue {
                    value: UserSettingValue::UInt64(1),
                    desc: "Enables cost-based optimization.",
                    possible_values: None,
                    display_in_show_settings: true,
                }),
                ("disable_join_reorder", DefaultSettingValue {
                    value: UserSettingValue::UInt64(0),
                    desc: "Disable join reorder optimization.",
                    possible_values: None,
                    display_in_show_settings: false,}),
                ("join_spilling_threshold", DefaultSettingValue {
                    value: UserSettingValue::UInt64(0),
                    desc: "Maximum amount of memory can use for hash join, 0 is unlimited.",
                    possible_values: None,
                    display_in_show_settings: true,
                }),
                ("enable_runtime_filter", DefaultSettingValue {
                    value: UserSettingValue::UInt64(0),
                    desc: "Enables runtime filter optimization for JOIN.",
                    possible_values: None,
                    display_in_show_settings: true,
                }),
                ("max_execute_time_in_seconds", DefaultSettingValue {
                    value: UserSettingValue::UInt64(0),
                    desc: "Sets the maximum query execution time in seconds. Setting it to 0 means no limit.",
                    possible_values: None,
                    display_in_show_settings: true,
                }),
                ("collation", DefaultSettingValue {
                    value: UserSettingValue::String("binary".to_owned()),
                    desc: "Sets the character collation. Available values include \"binary\" and \"utf8\".",
                    possible_values: Some(vec!["binary", "utf8"]),
                    display_in_show_settings: true,
                }),
                ("max_result_rows", DefaultSettingValue {
                    value: UserSettingValue::UInt64(0),
                    desc: "Sets the maximum number of rows that can be returned in a query result when no specific row count is specified. Setting it to 0 means no limit.",
                    possible_values: None,
                    display_in_show_settings: true,
                }),
                ("prefer_broadcast_join", DefaultSettingValue {
                    value: UserSettingValue::UInt64(1),
                    desc: "Enables broadcast join.",
                    possible_values: None,
                    display_in_show_settings: true,
                }),
                ("storage_fetch_part_num", DefaultSettingValue {
                    value: UserSettingValue::UInt64(2),
                    desc: "Sets the number of partitions that are fetched in parallel from storage during query execution.",
                    possible_values: None,
                    display_in_show_settings: true,
                }),
                ("load_file_metadata_expire_hours", DefaultSettingValue {
                    value: UserSettingValue::UInt64(24 * 7),
                    desc: "Sets the hours that the metadata of files you load data from with COPY INTO will expire in.",
                    possible_values: None,
                    display_in_show_settings: true,
                }),
                ("hide_options_in_show_create_table", DefaultSettingValue {
                    value: UserSettingValue::UInt64(1),
                    desc: "Hides table-relevant information, such as SNAPSHOT_LOCATION and STORAGE_FORMAT, at the end of the result of SHOW TABLE CREATE.",
                    possible_values: None,
                    display_in_show_settings: true,
                }),
                ("sandbox_tenant", DefaultSettingValue {
                    value: UserSettingValue::String("".to_string()),
                    desc: "Injects a custom 'sandbox_tenant' into this session. This is only for testing purposes and will take effect only when 'internal_enable_sandbox_tenant' is turned on.",
                    possible_values: None,
                    display_in_show_settings: true,
                }),
                ("parquet_uncompressed_buffer_size", DefaultSettingValue {
                    value: UserSettingValue::UInt64(2 * 1024 * 1024),
                    desc: "Sets the byte size of the buffer used for reading Parquet files.",
                    possible_values: None,
                    display_in_show_settings: true,
                }),
                ("enable_bushy_join", DefaultSettingValue {
                    value: UserSettingValue::UInt64(0),
                    desc: "Enables generating a bushy join plan with the optimizer.",
                    possible_values: None,
                    display_in_show_settings: true,
                }),
                ("enable_query_result_cache", DefaultSettingValue {
                    value: UserSettingValue::UInt64(0),
                    desc: "Enables caching query results to improve performance for identical queries.",
                    possible_values: None,
                    display_in_show_settings: true,
                }),
                ("query_result_cache_max_bytes", DefaultSettingValue {
                    value: UserSettingValue::UInt64(1048576), // 1MB
                    desc: "Sets the maximum byte size of cache for a single query result.",
                    possible_values: None,
                    display_in_show_settings: true,
                }),
                ("query_result_cache_ttl_secs", DefaultSettingValue {
                    value: UserSettingValue::UInt64(300), // seconds
                    desc: "Sets the time-to-live (TTL) in seconds for cached query results. \
                Once the TTL for a cached result has expired, the result is considered stale and will not be used for new queries.",
                    possible_values: None,
                    display_in_show_settings: true,
                }),
                ("query_result_cache_allow_inconsistent", DefaultSettingValue {
                    value: UserSettingValue::UInt64(0),
                    desc: "Determines whether Databend will return cached query results that are inconsistent with the underlying data.",
                    possible_values: None,
                    display_in_show_settings: true,
                }),
                ("enable_hive_parquet_predict_pushdown", DefaultSettingValue {
                    value: UserSettingValue::UInt64(1),
                    desc: "Enable hive parquet predict pushdown  by setting this variable to 1, default value: 1",
                    possible_values: None,
                    display_in_show_settings: true,
                }),
                ("hive_parquet_chunk_size", DefaultSettingValue {
                    value: UserSettingValue::UInt64(16384),
                    desc: "the max number of rows each read from parquet to databend processor",
                    possible_values: None,
                    display_in_show_settings: true,
                }),
                ("spilling_bytes_threshold_per_proc", DefaultSettingValue {
                    value: UserSettingValue::UInt64(0),
                    desc: "Sets the maximum amount of memory in bytes that an aggregator can use before spilling data to storage during query execution.",
                    possible_values: None,
                    display_in_show_settings: true,
                }),
                ("spilling_memory_ratio", DefaultSettingValue {
                    value: UserSettingValue::UInt64(0),
                    desc: "Sets the maximum memory ratio in bytes that an aggregator can use before spilling data to storage during query execution.",
                    possible_values: None,
                    display_in_show_settings: true,
                }),
                ("group_by_shuffle_mode", DefaultSettingValue {
                    value: UserSettingValue::String(String::from("before_merge")),
                    desc: "Group by shuffle mode, 'before_partial' is more balanced, but more data needs to exchange.",
                    possible_values: Some(vec!["before_partial", "before_merge"]),
                    display_in_show_settings: true,
                }),
                ("efficiently_memory_group_by", DefaultSettingValue {
                    value: UserSettingValue::UInt64(0),
                    desc: "Memory is used efficiently, but this may cause performance degradation.",
                    possible_values: None,
                    display_in_show_settings: true,
                }),
                ("lazy_read_threshold", DefaultSettingValue {
                    value: UserSettingValue::UInt64(1000),
                    desc: "Sets the maximum LIMIT in a query to enable lazy read optimization. Setting it to 0 disables the optimization.",
                    possible_values: None,
                    display_in_show_settings: true,
                }),
                ("parquet_fast_read_bytes", DefaultSettingValue {
                    value: UserSettingValue::UInt64(0),
                    desc: "Parquet file with smaller size will be read as a whole file, instead of column by column.",
                    possible_values: None,
                    display_in_show_settings: true,
                }),

                // enterprise license related settings
                ("enterprise_license", DefaultSettingValue {
                    value: UserSettingValue::String("".to_owned()),
                    desc: "License key for use enterprise features",
                    possible_values: None,
                    // license key should not be reported
                    display_in_show_settings: false,
                }),
                ("enable_table_lock", DefaultSettingValue {
                    value: UserSettingValue::UInt64(1),
                    desc: "Enables table lock if necessary (enabled by default).",
                    possible_values: None,
                    display_in_show_settings: true,
                }),
                ("table_lock_expire_secs", DefaultSettingValue {
                    value: UserSettingValue::UInt64(5),
                    desc: "Sets the seconds that the table lock will expire in.",
                    possible_values: None,
                    display_in_show_settings: true,
                }),
                ("deduplicate_label", DefaultSettingValue {
                    value: UserSettingValue::String("".to_owned()),
                    desc: "Sql duplicate label for deduplication.",
                    possible_values: None,
                    display_in_show_settings: false,
                }),
                ("enable_distributed_copy_into", DefaultSettingValue {
                    value: UserSettingValue::UInt64(0),
                    desc: "Enable distributed execution of copy into.",
                    possible_values: None,
                    display_in_show_settings: true,
                }),
                ("enable_experimental_merge_into", DefaultSettingValue {
                    value: UserSettingValue::UInt64(0),
                    desc: "Enable unstable merge into.",
                    possible_values: None,
                    display_in_show_settings: true,
                }),
                ("enable_distributed_replace_into", DefaultSettingValue {
                    value: UserSettingValue::UInt64(0),
                    desc: "Enable distributed execution of replace into.",
                    possible_values: None,
                    display_in_show_settings: true,
                }),
                ("enable_distributed_compact", DefaultSettingValue {
                    value: UserSettingValue::UInt64(0),
                    desc: "Enable distributed execution of table compaction.",
                    possible_values: None,
                    display_in_show_settings: true,
                }),
                ("enable_aggregating_index_scan", DefaultSettingValue {
                    value: UserSettingValue::UInt64(1),
                    desc: "Enable scanning aggregating index data while querying.",
                    possible_values: None,
                    display_in_show_settings: true,
                }),
                ("enable_recluster_after_write", DefaultSettingValue {
                    value: UserSettingValue::UInt64(1),
                    desc: "Enables re-clustering after write(copy/replace-into).",
                    possible_values: None,
                    display_in_show_settings: true,
                }),
                ("use_parquet2", DefaultSettingValue {
                    value: UserSettingValue::UInt64(1),
                    desc: "Use parquet2 instead of parquet_rs when infer_schema().",
                    possible_values: None,
                    display_in_show_settings: true,
                }),
                ("enable_replace_into_partitioning", DefaultSettingValue {
                    value: UserSettingValue::UInt64(1),
                    desc: "Enables partitioning for replace-into statement (if table has cluster keys).",
                    possible_values: None,
                    display_in_show_settings: true,
                }),
                ("enable_replace_into_bloom_pruning", DefaultSettingValue {
                    value: UserSettingValue::UInt64(1),
                    desc: "Enables bloom pruning for replace-into statement.",
                    possible_values: None,
                    display_in_show_settings: true,
                }),
                ("replace_into_bloom_pruning_max_column_number", DefaultSettingValue {
                    value: UserSettingValue::UInt64(4),
                    desc: "Max number of columns used by bloom pruning for replace-into statement.",
                    possible_values: None,
                    display_in_show_settings: true,
                }),
                ("replace_into_shuffle_strategy", DefaultSettingValue {
                    value: UserSettingValue::UInt64(0),
                    desc: "0 for Block level shuffle, 1 for segment level shuffle",
                    possible_values: None,
                    display_in_show_settings: true,
                }),
                ("recluster_timeout_secs", DefaultSettingValue {
                    value: UserSettingValue::UInt64(12 * 60 * 60),
                    desc: "Sets the seconds that recluster final will be timeout.",
                    possible_values: None,
                    display_in_show_settings: true,
                }),
                ("enable_refresh_aggregating_index_after_write", DefaultSettingValue {
                    value: UserSettingValue::UInt64(0),
                    desc: "Refresh aggregating index after new data written",
                    possible_values: None,
                    display_in_show_settings: true,
                }),
                ("ddl_column_type_nullable", DefaultSettingValue {
                    value: UserSettingValue::UInt64(1),
                    desc: "If columns are default nullable when create or alter table",
                    possible_values: None,
                    display_in_show_settings: true,
                }),
                ("enable_query_profiling", DefaultSettingValue {
                        value: UserSettingValue::UInt64(0),
                        desc: "Enables recording query profile",
                        possible_values: None,
                        display_in_show_settings: true,
                }),
                ("recluster_block_size", DefaultSettingValue {
                    value: UserSettingValue::UInt64(recluster_block_size),
                    desc: "Sets the maximum byte size of blocks for recluster",
                    possible_values: None,
                    display_in_show_settings: true,
                }),
                ("enable_parquet_page_index", DefaultSettingValue {
                        value: UserSettingValue::UInt64(1),
                        desc: "Enables parquet page index",
                        possible_values: None,
                        display_in_show_settings: true,
                }),
                ("enable_parquet_rowgroup_pruning", DefaultSettingValue {
                        value: UserSettingValue::UInt64(1),
                        desc: "Enables parquet rowgroup pruning",
                        possible_values: None,
                        display_in_show_settings: true,
                }),
                ("enable_parquet_prewhere", DefaultSettingValue {
                        value: UserSettingValue::UInt64(1),
                        desc: "Enables parquet prewhere",
                        possible_values: None,
                        display_in_show_settings: true,
                }),
            ]);

            Ok(Arc::new(DefaultSettings {
                settings: default_settings.into_iter().map(|(k, v)| (k.to_string(), v))
                    .collect()
            }))
        })?))
    }

    fn storage_io_requests(num_cpus: u64) -> u64 {
        match GlobalConfig::try_get_instance() {
            None => std::cmp::min(num_cpus, 64),
            Some(conf) => match conf.storage.params.is_fs() {
                true => 48,
                false => std::cmp::min(num_cpus, 64),
            },
        }
    }

    fn num_cpus() -> u64 {
        match GlobalConfig::try_get_instance() {
            None => num_cpus::get() as u64,
            Some(conf) => {
                let mut num_cpus = num_cpus::get() as u64;

                if conf.storage.params.is_fs() {
                    if let Ok(n) = std::thread::available_parallelism() {
                        num_cpus = n.get() as u64;
                    }

                    // Most of x86_64 CPUs have 2-way Hyper-Threading
                    #[cfg(target_arch = "x86_64")]
                    {
                        if num_cpus >= 32 {
                            num_cpus /= 2;
                        }
                    }
                    // Detect CGROUPS ?
                }

                if conf.query.num_cpus != 0 {
                    num_cpus = conf.query.num_cpus;
                }

                num_cpus.clamp(1, 96)
            }
        }
    }

    fn max_memory_usage() -> Result<u64> {
        let memory_info = sys_info::mem_info().map_err(ErrorCode::from_std_error)?;

        Ok(match GlobalConfig::try_get_instance() {
            None => 1024 * memory_info.total * 80 / 100,
            Some(conf) => match conf.query.max_server_memory_usage {
                0 => 1024 * memory_info.total * 80 / 100,
                max_server_memory_usage => max_server_memory_usage,
            },
        })
    }

    fn recluster_block_size() -> Result<u64> {
        let max_memory_usage = Self::max_memory_usage()?;
        // The sort merge consumes more than twice as much memory,
        // so the block size is set relatively conservatively here.
        let recluster_block_size = max_memory_usage * 35 / 100;
        Ok(recluster_block_size)
    }

    pub fn has_setting(key: &str) -> Result<bool> {
        Ok(Self::instance()?.settings.contains_key(key))
    }

    pub fn convert_value(k: String, v: String) -> Result<(String, Option<UserSettingValue>)> {
        let default_settings = DefaultSettings::instance()?;

        match default_settings.settings.get(&k) {
            None => Ok((k, None)),
            Some(setting_value) => {
                if let Some(possible_values) = &setting_value.possible_values {
                    if !possible_values.iter().any(|x| x.eq_ignore_ascii_case(&v)) {
                        return Err(ErrorCode::WrongValueForVariable(format!(
                            "Invalid setting value: {:?} for variable {:?}, possible values: {:?}",
                            v, k, possible_values
                        )));
                    }
                }
                match setting_value.value {
                    UserSettingValue::UInt64(_) => {
                        // decimal 10 * 1.5 to string may result in string like "15.0"
                        let val = if let Some(p) = v.find('.') {
                            if v[(p + 1)..].chars().all(|x| x == '0') {
                                &v[..p]
                            } else {
                                return Err(ErrorCode::BadArguments("not a integer"));
                            }
                        } else {
                            &v[..]
                        };

                        let u64_val = val.parse::<u64>()?;
                        Ok((k, Some(UserSettingValue::UInt64(u64_val))))
                    }
                    UserSettingValue::String(_) => Ok((k, Some(UserSettingValue::String(v)))),
                }
            }
        }
    }

    pub fn try_get_u64(key: &str) -> Result<u64> {
        match DefaultSettings::instance()?.settings.get(key) {
            Some(v) => v.value.as_u64(),
            None => Err(ErrorCode::UnknownVariable(format!(
                "Unknown variable: {:?}",
                key
            ))),
        }
    }

    pub fn try_get_string(key: &str) -> Result<String> {
        match DefaultSettings::instance()?.settings.get(key) {
            Some(v) => v.value.as_string(),
            None => Err(ErrorCode::UnknownVariable(format!(
                "Unknown variable: {:?}",
                key
            ))),
        }
    }
}

pub enum ReplaceIntoShuffleStrategy {
    SegmentLevelShuffling,
    BlockLevelShuffling,
}

impl TryFrom<u64> for ReplaceIntoShuffleStrategy {
    type Error = ErrorCode;

    fn try_from(value: u64) -> std::result::Result<Self, Self::Error> {
        match value {
            0 => Ok(ReplaceIntoShuffleStrategy::BlockLevelShuffling),
            1 => Ok(ReplaceIntoShuffleStrategy::SegmentLevelShuffling),
            _ => Err(ErrorCode::InvalidConfig(
                "value of replace_into_shuffle_strategy should be one of {0,1}, 0 for block level shuffling, 1 for segment level shuffling",
            )),
        }
    }
}
