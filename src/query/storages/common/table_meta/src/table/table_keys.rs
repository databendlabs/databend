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

use std::collections::HashSet;
use std::fmt::Display;
use std::fmt::Formatter;
use std::sync::LazyLock;
pub const OPT_KEY_DATABASE_ID: &str = "database_id";
pub const OPT_KEY_STORAGE_PREFIX: &str = "storage_prefix";
pub const OPT_KEY_TEMP_PREFIX: &str = "temp_prefix";
pub const OPT_KEY_SNAPSHOT_LOCATION: &str = "snapshot_location";
pub const OPT_KEY_SNAPSHOT_LOCATION_FIXED_FLAG: &str = "snapshot_location_fixed";
pub const OPT_KEY_STORAGE_FORMAT: &str = "storage_format";
pub const OPT_KEY_SEGMENT_FORMAT: &str = "segment_format";
pub const OPT_KEY_TABLE_COMPRESSION: &str = "compression";
pub const OPT_KEY_COMMENT: &str = "comment";
pub const OPT_KEY_ENGINE: &str = "engine";
pub const OPT_KEY_BLOOM_INDEX_COLUMNS: &str = "bloom_index_columns";
pub const OPT_KEY_CHANGE_TRACKING: &str = "change_tracking";
pub const OPT_KEY_CHANGE_TRACKING_BEGIN_VER: &str = "begin_version";

// Attached table options.
pub const OPT_KEY_TABLE_ATTACHED_DATA_URI: &str = "table_data_uri";

// the following are used in for delta and iceberg engine
pub const OPT_KEY_LOCATION: &str = "location";
pub const OPT_KEY_CONNECTION_NAME: &str = "connection_name";
// TableMeta need to contain all info needed to create a Table, store them under this internal key as a JSON.
// e.g. the partition columns of a Delta table
pub const OPT_KEY_ENGINE_META: &str = "engine_meta";

/// Legacy table snapshot location key
///
/// # Deprecated
///
/// For backward compatibility, this option key can still be recognized,
/// but use can no longer use this key in DDLs
///
/// If both OPT_KEY_SNAPSHOT_LOC and OPT_KEY_SNAPSHOT_LOCATION exist, the latter will be used
pub const OPT_KEY_LEGACY_SNAPSHOT_LOC: &str = "snapshot_loc";
// the following are used in for random engine
pub const OPT_KEY_RANDOM_SEED: &str = "seed";
pub const OPT_KEY_RANDOM_MIN_STRING_LEN: &str = "min_string_len";
pub const OPT_KEY_RANDOM_MAX_STRING_LEN: &str = "max_string_len";
pub const OPT_KEY_RANDOM_MAX_ARRAY_LEN: &str = "max_array_len";

pub const OPT_KEY_CLUSTER_TYPE: &str = "cluster_type";
pub const OPT_KEY_ENABLE_COPY_DEDUP_FULL_PATH: &str = "copy_dedup_full_path";
pub const LINEAR_CLUSTER_TYPE: &str = "linear";
pub const HILBERT_CLUSTER_TYPE: &str = "hilbert";

/// Table option keys that reserved for internal usage only
/// - Users are not allowed to specified this option keys in DDL
/// - Should not be shown in `show create table` statement
pub static RESERVED_TABLE_OPTION_KEYS: LazyLock<HashSet<&'static str>> = LazyLock::new(|| {
    let mut r = HashSet::new();
    r.insert(OPT_KEY_DATABASE_ID);
    r.insert(OPT_KEY_LEGACY_SNAPSHOT_LOC);
    r
});

/// Table option keys that Should not be shown in `show create table` statement
pub static INTERNAL_TABLE_OPTION_KEYS: LazyLock<HashSet<&'static str>> = LazyLock::new(|| {
    let mut r = HashSet::new();
    r.insert(OPT_KEY_LEGACY_SNAPSHOT_LOC);
    r.insert(OPT_KEY_DATABASE_ID);
    r.insert(OPT_KEY_ENGINE_META);
    r.insert(OPT_KEY_CHANGE_TRACKING_BEGIN_VER);
    r.insert(OPT_KEY_TEMP_PREFIX);
    r
});

pub fn is_reserved_opt_key<S: AsRef<str>>(opt_key: S) -> bool {
    RESERVED_TABLE_OPTION_KEYS.contains(opt_key.as_ref().to_lowercase().as_str())
}

pub fn is_internal_opt_key<S: AsRef<str>>(opt_key: S) -> bool {
    INTERNAL_TABLE_OPTION_KEYS.contains(opt_key.as_ref().to_lowercase().as_str())
}

#[derive(Debug, serde::Serialize, serde::Deserialize, Clone, Eq, PartialEq, Copy)]
pub enum ClusterType {
    Linear,
    Hilbert,
}

impl Display for ClusterType {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        write!(f, "{}", match self {
            ClusterType::Linear => LINEAR_CLUSTER_TYPE.to_string(),
            ClusterType::Hilbert => HILBERT_CLUSTER_TYPE.to_string(),
        })
    }
}

impl std::str::FromStr for ClusterType {
    type Err = databend_common_exception::ErrorCode;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "linear" => Ok(ClusterType::Linear),
            "hilbert" => Ok(ClusterType::Hilbert),
            _ => Err(databend_common_exception::ErrorCode::Internal(format!(
                "invalid cluster type: {}",
                s
            ))),
        }
    }
}
