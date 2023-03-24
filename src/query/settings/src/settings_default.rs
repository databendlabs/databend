use std::collections::HashMap;
use std::sync::Arc;

use common_exception::ErrorCode;
use common_exception::Result;
use common_meta_app::principal::UserSettingValue;
use once_cell::sync::OnceCell;

use crate::settings::NewSettings;
use crate::ScopeLevel;

static DEFAULT_SETTINGS: OnceCell<Arc<DefaultSettings>> = OnceCell::new();

#[derive(Clone, Debug)]
pub struct DefaultSettingValue {
    pub(crate) level: ScopeLevel,
    pub(crate) value: UserSettingValue,
    pub(crate) desc: &'static str,
    pub(crate) possible_values: Option<Vec<&'static str>>,
}

#[derive(Clone)]
pub struct DefaultSettings {
    pub(crate) settings: HashMap<String, DefaultSettingValue>,
}

impl DefaultSettings {
    pub fn instance() -> Arc<DefaultSettings> {
        Arc::clone(DEFAULT_SETTINGS.get_or_init(|| {
            let default_settings = HashMap::from([
                ("max_block_size", DefaultSettingValue {
                    level: ScopeLevel::Session,
                    value: UserSettingValue::UInt64(65536),
                    desc: "Sets the maximum byte size of a single data block that can be read.",
                    possible_values: None,
                }),
                // ("max_threads", NewSettingValue {
                //     level: ScopeLevel::Session,
                //     value: UserSettingValue::UInt64(num_cpus),
                //     desc: "Sets the maximum number of threads to execute a request.",
                //     possible_values: None,
                // }),
                // ("max_memory_usage", NewSettingValue {
                //     level: ScopeLevel::Session,
                //     value: UserSettingValue::UInt64(default_max_memory_usage),
                //     desc: "Sets the maximum memory usage in bytes for processing a single query.",
                //     possible_values: None,
                // }),
                ("retention_period", DefaultSettingValue {
                    // unit of retention_period is hour
                    level: ScopeLevel::Session,
                    value: UserSettingValue::UInt64(12),
                    desc: "Sets the retention period in hours.",
                    possible_values: None,
                }),
                // ("max_storage_io_requests", NewSettingValue {
                //     level: ScopeLevel::Session,
                //     value: UserSettingValue::UInt64(default_max_storage_io_requests),
                //     desc: "Sets the maximum number of concurrent I/O requests.",
                //     possible_values: None,
                // }),
                ("storage_io_min_bytes_for_seek", DefaultSettingValue {
                    level: ScopeLevel::Session,
                    value: UserSettingValue::UInt64(48),
                    desc: "Sets the minimum byte size of data that must be read from storage in a single I/O operation \
                when seeking a new location in the data file.",
                    possible_values: None,
                }),
                ("storage_io_max_page_bytes_for_read", DefaultSettingValue {
                    level: ScopeLevel::Session,
                    value: UserSettingValue::UInt64(512 * 1024),
                    desc: "Sets the maximum byte size of data pages that can be read from storage in a single I/O operation.",
                    possible_values: None,
                }),
                ("flight_client_timeout", DefaultSettingValue {
                    level: ScopeLevel::Session,
                    value: UserSettingValue::UInt64(60),
                    desc: "Sets the maximum time in seconds that a flight client request can be processed.",
                    possible_values: None,
                }),
                ("storage_read_buffer_size", DefaultSettingValue {
                    level: ScopeLevel::Session,
                    value: UserSettingValue::UInt64(1024 * 1024),
                    desc: "Sets the byte size of the buffer used for reading data into memory.",
                    possible_values: None,
                }),
                ("input_read_buffer_size", DefaultSettingValue {
                    level: ScopeLevel::Session,
                    value: UserSettingValue::UInt64(1024 * 1024),
                    desc: "Sets the memory size in bytes allocated to the buffer used by the buffered reader to read data from storage.",
                    possible_values: None,
                }),
                ("timezone", DefaultSettingValue {
                    level: ScopeLevel::Session,
                    value: UserSettingValue::String("UTC".to_owned()),
                    desc: "Sets the timezone.",
                    possible_values: None,
                }),
                ("group_by_two_level_threshold", DefaultSettingValue {
                    level: ScopeLevel::Session,
                    value: UserSettingValue::UInt64(20000),
                    desc: "Sets the number of keys in a GROUP BY operation that will trigger a two-level aggregation.",
                    possible_values: None,
                }),
                ("max_inlist_to_or", DefaultSettingValue {
                    level: ScopeLevel::Session,
                    value: UserSettingValue::UInt64(3),
                    desc: "Sets the maximum number of values that can be included in an IN expression to be converted to an OR operator.",
                    possible_values: None,
                }),
                ("unquoted_ident_case_sensitive", DefaultSettingValue {
                    level: ScopeLevel::Session,
                    value: UserSettingValue::UInt64(0),
                    desc: "Determines whether Databend treats unquoted identifiers as case-sensitive.",
                    possible_values: None,
                }),
                ("quoted_ident_case_sensitive", DefaultSettingValue {
                    level: ScopeLevel::Session,
                    value: UserSettingValue::UInt64(1),
                    desc: "Determines whether Databend treats quoted identifiers as case-sensitive.",
                    possible_values: None,
                }),
                ("sql_dialect", DefaultSettingValue {
                    level: ScopeLevel::Session,
                    value: UserSettingValue::String("PostgreSQL".to_owned()),
                    desc: "Sets the SQL dialect. Available values include \"PostgreSQL\", \"MySQL\", and \"Hive\".",
                    possible_values: Some(vec!["PostgreSQL", "MySQL", "Hive"]),
                }),
                ("enable_cbo", DefaultSettingValue {
                    level: ScopeLevel::Session,
                    value: UserSettingValue::UInt64(1),
                    desc: "Enables cost-based optimization.",
                    possible_values: None,
                }),
                ("enable_runtime_filter", DefaultSettingValue {
                    level: ScopeLevel::Session,
                    value: UserSettingValue::UInt64(0),
                    desc: "Enables runtime filter optimization for JOIN.",
                    possible_values: None,
                }),
                ("max_execute_time", DefaultSettingValue {
                    level: ScopeLevel::Session,
                    value: UserSettingValue::UInt64(0),
                    desc: "Sets the maximum query execution time in seconds. Setting it to 0 means no limit.",
                    possible_values: None,
                }),
                ("collation", DefaultSettingValue {
                    level: ScopeLevel::Session,
                    value: UserSettingValue::String("binary".to_owned()),
                    desc: "Sets the character collation. Available values include \"binary\" and \"utf8\".",
                    possible_values: Some(vec!["binary", "utf8"]),
                }),
                ("max_result_rows", DefaultSettingValue {
                    level: ScopeLevel::Session,
                    value: UserSettingValue::UInt64(0),
                    desc: "Sets the maximum number of rows that can be returned in a query result when no specific row count is specified. Setting it to 0 means no limit.",
                    possible_values: None,
                }),
                ("enable_distributed_eval_index", DefaultSettingValue {
                    level: ScopeLevel::Session,
                    value: UserSettingValue::UInt64(1),
                    desc: "Enables evaluated indexes to be created and maintained across multiple nodes.",
                    possible_values: None,
                }),
                ("prefer_broadcast_join", DefaultSettingValue {
                    level: ScopeLevel::Session,
                    value: UserSettingValue::UInt64(1),
                    desc: "Enables broadcast join.",
                    possible_values: None,
                }),
                ("storage_fetch_part_num", DefaultSettingValue {
                    level: ScopeLevel::Session,
                    value: UserSettingValue::UInt64(2),
                    desc: "Sets the number of partitions that are fetched in parallel from storage during query execution.",
                    possible_values: None,
                }),
                ("load_file_metadata_expire_hours", DefaultSettingValue {
                    level: ScopeLevel::Session,
                    value: UserSettingValue::UInt64(24 * 7),
                    desc: "Sets the hours that the metadata of files you load data from with COPY INTO will expire in.",
                    possible_values: None,
                }),
                ("hide_options_in_show_create_table", DefaultSettingValue {
                    level: ScopeLevel::Session,
                    value: UserSettingValue::UInt64(1),
                    desc: "Hides table-relevant information, such as SNAPSHOT_LOCATION and STORAGE_FORMAT, at the end of the result of SHOW TABLE CREATE.",
                    possible_values: None,
                }),
                ("sandbox_tenant", DefaultSettingValue {
                    level: ScopeLevel::Session,
                    value: UserSettingValue::String("".to_string()),
                    desc: "Injects a custom 'sandbox_tenant' into this session. This is only for testing purposes and will take effect only when 'internal_enable_sandbox_tenant' is turned on.",
                    possible_values: None,
                }),
                ("parquet_uncompressed_buffer_size", DefaultSettingValue {
                    level: ScopeLevel::Session,
                    value: UserSettingValue::UInt64(2 * 1024 * 1024),
                    desc: "Sets the byte size of the buffer used for reading Parquet files.",
                    possible_values: None,
                }),
                ("enable_bushy_join", DefaultSettingValue {
                    level: ScopeLevel::Session,
                    value: UserSettingValue::UInt64(0),
                    desc: "Enables generating a bushy join plan with the optimizer.",
                    possible_values: None,
                }),
                ("enable_query_result_cache", DefaultSettingValue {
                    level: ScopeLevel::Session,
                    value: UserSettingValue::UInt64(0),
                    desc: "Enables caching query results to improve performance for identical queries.",
                    possible_values: None,
                }),
                ("query_result_cache_max_bytes", DefaultSettingValue {
                    level: ScopeLevel::Session,
                    value: UserSettingValue::UInt64(1048576), // 1MB
                    desc: "Sets the maximum byte size of cache for a single query result.",
                    possible_values: None,
                }),
                ("query_result_cache_ttl_secs", DefaultSettingValue {
                    level: ScopeLevel::Session,
                    value: UserSettingValue::UInt64(300), // seconds
                    desc: "Sets the time-to-live (TTL) in seconds for cached query results. \
                Once the TTL for a cached result has expired, the result is considered stale and will not be used for new queries.",
                    possible_values: None,
                }),
                ("query_result_cache_allow_inconsistent", DefaultSettingValue {
                    level: ScopeLevel::Session,
                    value: UserSettingValue::UInt64(0),
                    desc: "Determines whether Databend will return cached query results that are inconsistent with the underlying data.",
                    possible_values: None,
                }),
                ("spilling_bytes_threshold_per_proc", DefaultSettingValue {
                    level: ScopeLevel::Session,
                    value: UserSettingValue::UInt64(0),
                    desc: "Sets the maximum amount of memory in bytes that an aggregator can use before spilling data to storage during query execution.",
                    possible_values: None,
                }),
            ]);


            // #[cfg(feature = "hive")])
            // {
            //
            // }
            // {
            //     ("enable_hive_parquet_predict_pushdown", SettingValue {
            //         default_value: UserSettingValue::UInt64(1),
            //         user_setting: UserSetting::create("enable_hive_parquet_predict_pushdown", UserSettingValue::UInt64(1)),
            //         level: ScopeLevel::Session,
            //         desc: "Enable hive parquet predict pushdown  by setting this variable to 1, default value: 1",
            //         possible_values: None,
            //     },
            // }
            // # [cfg(feature = "hive")])
            // ("hive_parquet_chunk_size", SettingValue {
            //     default_value: UserSettingValue::UInt64(16384),
            //     user_setting: UserSetting::create("hive_parquet_chunk_size", UserSettingValue::UInt64(16384)),
            //     level: ScopeLevel::Session,
            //     desc: "the max number of rows each read from parquet to databend processor",
            //     possible_values: None,
            // },

            unimplemented!()
        }))
    }

    pub fn has_setting(key: &str) -> bool {
        Self::instance().settings.contains_key(key)
    }

    pub fn convert_value(k: String, v: String) -> Result<(String, Option<UserSettingValue>)> {
        let default_settings = DefaultSettings::instance();

        match default_settings.settings.get(&k) {
            None => Ok((k, None)),
            Some(setting_value) => match setting_value.value {
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
            },
        }
    }

    pub fn try_get_u64(key: &str) -> Result<u64> {
        match DefaultSettings::instance().settings.get(key) {
            Some(v) => v.value.as_u64(),
            None => Err(ErrorCode::UnknownVariable(format!(
                "Unknown variable: {:?}",
                key
            ))),
        }
    }

    pub fn try_get_string(key: &str) -> Result<String> {
        match DefaultSettings::instance().settings.get(key) {
            Some(v) => v.value.as_string(),
            None => Err(ErrorCode::UnknownVariable(format!(
                "Unknown variable: {:?}",
                key
            ))),
        }
    }
}
