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

use std::collections::BTreeMap;
use std::collections::HashSet;
use std::sync::LazyLock;

use chrono::Duration;
use databend_common_ast::ast::Engine;
use databend_common_exception::ErrorCode;
use databend_common_expression::TableSchemaRef;
use databend_common_io::constants::DEFAULT_BLOCK_MAX_ROWS;
use databend_common_io::constants::DEFAULT_MIN_TABLE_LEVEL_DATA_RETENTION_PERIOD_IN_HOURS;
use databend_common_settings::Settings;
use databend_common_sql::BloomIndexColumns;
use databend_common_storages_fuse::FUSE_OPT_KEY_BLOCK_IN_MEM_SIZE_THRESHOLD;
use databend_common_storages_fuse::FUSE_OPT_KEY_BLOCK_PER_SEGMENT;
use databend_common_storages_fuse::FUSE_OPT_KEY_DATA_RETENTION_PERIOD_IN_HOURS;
use databend_common_storages_fuse::FUSE_OPT_KEY_ROW_AVG_DEPTH_THRESHOLD;
use databend_common_storages_fuse::FUSE_OPT_KEY_ROW_PER_BLOCK;
use databend_common_storages_fuse::FUSE_OPT_KEY_ROW_PER_PAGE;
use databend_storages_common_index::BloomIndex;
use databend_storages_common_table_meta::table::OPT_KEY_BLOOM_INDEX_COLUMNS;
use databend_storages_common_table_meta::table::OPT_KEY_CHANGE_TRACKING;
use databend_storages_common_table_meta::table::OPT_KEY_CLUSTER_TYPE;
use databend_storages_common_table_meta::table::OPT_KEY_COMMENT;
use databend_storages_common_table_meta::table::OPT_KEY_CONNECTION_NAME;
use databend_storages_common_table_meta::table::OPT_KEY_DATABASE_ID;
use databend_storages_common_table_meta::table::OPT_KEY_ENABLE_COPY_DEDUP_FULL_PATH;
use databend_storages_common_table_meta::table::OPT_KEY_ENGINE;
use databend_storages_common_table_meta::table::OPT_KEY_LOCATION;
use databend_storages_common_table_meta::table::OPT_KEY_RANDOM_MAX_ARRAY_LEN;
use databend_storages_common_table_meta::table::OPT_KEY_RANDOM_MAX_STRING_LEN;
use databend_storages_common_table_meta::table::OPT_KEY_RANDOM_MIN_STRING_LEN;
use databend_storages_common_table_meta::table::OPT_KEY_RANDOM_SEED;
use databend_storages_common_table_meta::table::OPT_KEY_SEGMENT_FORMAT;
use databend_storages_common_table_meta::table::OPT_KEY_STORAGE_FORMAT;
use databend_storages_common_table_meta::table::OPT_KEY_TABLE_COMPRESSION;
use databend_storages_common_table_meta::table::OPT_KEY_TEMP_PREFIX;
use log::error;

/// Table option keys that can occur in 'create table statement'.
pub static CREATE_FUSE_OPTIONS: LazyLock<HashSet<&'static str>> = LazyLock::new(|| {
    let mut r = HashSet::new();
    r.insert(FUSE_OPT_KEY_ROW_PER_PAGE);
    r.insert(FUSE_OPT_KEY_BLOCK_PER_SEGMENT);
    r.insert(FUSE_OPT_KEY_ROW_PER_BLOCK);
    r.insert(FUSE_OPT_KEY_BLOCK_IN_MEM_SIZE_THRESHOLD);
    r.insert(FUSE_OPT_KEY_ROW_AVG_DEPTH_THRESHOLD);
    r.insert(FUSE_OPT_KEY_DATA_RETENTION_PERIOD_IN_HOURS);

    r.insert(OPT_KEY_BLOOM_INDEX_COLUMNS);
    r.insert(OPT_KEY_TABLE_COMPRESSION);
    r.insert(OPT_KEY_STORAGE_FORMAT);
    r.insert(OPT_KEY_DATABASE_ID);
    r.insert(OPT_KEY_COMMENT);
    r.insert(OPT_KEY_CHANGE_TRACKING);
    r.insert(OPT_KEY_CLUSTER_TYPE);

    r.insert(OPT_KEY_ENGINE);

    r.insert(OPT_KEY_CONNECTION_NAME);

    r.insert("transient");
    r.insert(OPT_KEY_TEMP_PREFIX);
    r.insert(OPT_KEY_SEGMENT_FORMAT);
    r.insert(OPT_KEY_ENABLE_COPY_DEDUP_FULL_PATH);
    r
});

pub static CREATE_LAKE_OPTIONS: LazyLock<HashSet<&'static str>> = LazyLock::new(|| {
    let mut r = HashSet::new();
    r.insert(OPT_KEY_ENGINE);
    r.insert(OPT_KEY_LOCATION);
    r.insert(OPT_KEY_CONNECTION_NAME);
    r
});

pub static CREATE_RANDOM_OPTIONS: LazyLock<HashSet<&'static str>> = LazyLock::new(|| {
    let mut r = HashSet::new();
    r.insert(OPT_KEY_ENGINE);
    r.insert(OPT_KEY_RANDOM_SEED);
    r.insert(OPT_KEY_RANDOM_MIN_STRING_LEN);
    r.insert(OPT_KEY_RANDOM_MAX_STRING_LEN);
    r.insert(OPT_KEY_RANDOM_MAX_ARRAY_LEN);
    r
});

pub static CREATE_MEMORY_OPTIONS: LazyLock<HashSet<&'static str>> = LazyLock::new(|| {
    let mut r = HashSet::new();
    r.insert(OPT_KEY_ENGINE);
    r.insert(OPT_KEY_DATABASE_ID);
    r.insert(OPT_KEY_TEMP_PREFIX);
    r
});

pub static UNSET_TABLE_OPTIONS_WHITE_LIST: LazyLock<HashSet<&'static str>> = LazyLock::new(|| {
    let mut r = HashSet::new();
    r.insert(FUSE_OPT_KEY_ROW_PER_PAGE);
    r.insert(FUSE_OPT_KEY_BLOCK_PER_SEGMENT);
    r.insert(FUSE_OPT_KEY_ROW_PER_BLOCK);
    r.insert(FUSE_OPT_KEY_BLOCK_IN_MEM_SIZE_THRESHOLD);
    r.insert(FUSE_OPT_KEY_ROW_AVG_DEPTH_THRESHOLD);
    r.insert(FUSE_OPT_KEY_DATA_RETENTION_PERIOD_IN_HOURS);
    r.insert(OPT_KEY_ENABLE_COPY_DEDUP_FULL_PATH);
    r
});

pub fn is_valid_create_opt<S: AsRef<str>>(opt_key: S, engine: &Engine) -> bool {
    let opt_key = opt_key.as_ref().to_lowercase();
    let opt_key = opt_key.as_str();
    match engine {
        Engine::Fuse => CREATE_FUSE_OPTIONS.contains(opt_key),
        Engine::Iceberg | Engine::Delta => CREATE_LAKE_OPTIONS.contains(&opt_key),
        Engine::Random => CREATE_RANDOM_OPTIONS.contains(&opt_key),
        Engine::Memory => CREATE_MEMORY_OPTIONS.contains(&opt_key),
        Engine::Null | Engine::View => opt_key == OPT_KEY_ENGINE,
    }
}

pub fn is_valid_block_per_segment(
    options: &BTreeMap<String, String>,
) -> databend_common_exception::Result<()> {
    // check block_per_segment is not over 1000.
    if let Some(value) = options.get(FUSE_OPT_KEY_BLOCK_PER_SEGMENT) {
        let blocks_per_segment = value.parse::<u64>()?;
        let error_str = "invalid block_per_segment option, can't be over 1000";
        if blocks_per_segment > 1000 {
            error!("{}", &error_str);
            return Err(ErrorCode::TableOptionInvalid(error_str));
        }
    }

    Ok(())
}

pub fn is_valid_row_per_block(
    options: &BTreeMap<String, String>,
) -> databend_common_exception::Result<()> {
    // check row_per_block can not be over 1000000.
    if let Some(value) = options.get(FUSE_OPT_KEY_ROW_PER_BLOCK) {
        let row_per_block = value.parse::<u64>()?;
        let error_str = "invalid row_per_block option, can't be over 1000000";

        if row_per_block > DEFAULT_BLOCK_MAX_ROWS as u64 {
            error!("{}", error_str);
            return Err(ErrorCode::TableOptionInvalid(error_str));
        }
    }
    Ok(())
}

pub fn is_valid_data_retention_period(
    options: &BTreeMap<String, String>,
) -> databend_common_exception::Result<()> {
    if let Some(value) = options.get(FUSE_OPT_KEY_DATA_RETENTION_PERIOD_IN_HOURS) {
        let new_duration_in_hours = value.parse::<u64>()?;

        if new_duration_in_hours < DEFAULT_MIN_TABLE_LEVEL_DATA_RETENTION_PERIOD_IN_HOURS {
            return Err(ErrorCode::TableOptionInvalid(format!(
                "Invalid data_retention_period_in_hours {:?}, it should not be lesser than {:?}",
                new_duration_in_hours, DEFAULT_MIN_TABLE_LEVEL_DATA_RETENTION_PERIOD_IN_HOURS
            )));
        }

        let default_max_period_in_days = Settings::get_max_data_retention_period_in_days();

        let default_max_duration = Duration::days(default_max_period_in_days as i64);
        let new_duration = Duration::hours(new_duration_in_hours as i64);

        if new_duration > default_max_duration {
            return Err(ErrorCode::TableOptionInvalid(format!(
                "Invalid data_retention_period_in_hours {:?}, it should not be larger than {:?}",
                new_duration, default_max_duration
            )));
        }
    }
    Ok(())
}

pub fn is_valid_bloom_index_columns(
    options: &BTreeMap<String, String>,
    schema: TableSchemaRef,
) -> databend_common_exception::Result<()> {
    if let Some(value) = options.get(OPT_KEY_BLOOM_INDEX_COLUMNS) {
        BloomIndexColumns::verify_definition(value, schema, BloomIndex::supported_type)?;
    }
    Ok(())
}

pub fn is_valid_change_tracking(
    options: &BTreeMap<String, String>,
) -> databend_common_exception::Result<()> {
    if let Some(value) = options.get(OPT_KEY_CHANGE_TRACKING) {
        value.to_lowercase().parse::<bool>()?;
    }
    Ok(())
}

pub fn is_valid_random_seed(
    options: &BTreeMap<String, String>,
) -> databend_common_exception::Result<()> {
    if let Some(value) = options.get(OPT_KEY_RANDOM_SEED) {
        value.parse::<u64>()?;
    }
    Ok(())
}
