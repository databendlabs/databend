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

pub const FUSE_OPT_KEY_BLOCK_IN_MEM_SIZE_THRESHOLD: &str = "block_size_threshold";
pub const FUSE_OPT_KEY_BLOCK_PER_SEGMENT: &str = "block_per_segment";
pub const FUSE_OPT_KEY_ROW_PER_BLOCK: &str = "row_per_block";
pub const FUSE_OPT_KEY_ROW_PER_PAGE: &str = "row_per_page";
pub const FUSE_OPT_KEY_ROW_AVG_DEPTH_THRESHOLD: &str = "row_avg_depth_threshold";
pub const FUSE_OPT_KEY_FILE_SIZE: &str = "file_size";
pub const FUSE_OPT_KEY_DATA_RETENTION_PERIOD_IN_HOURS: &str = "data_retention_period_in_hours";
pub const FUSE_OPT_KEY_DATA_RETENTION_NUM_SNAPSHOTS_TO_KEEP: &str =
    "data_retention_num_snapshots_to_keep";
pub const FUSE_OPT_KEY_ENABLE_AUTO_VACUUM: &str = "enable_auto_vacuum";
pub const FUSE_OPT_KEY_ENABLE_AUTO_ANALYZE: &str = "enable_auto_analyze";
pub const FUSE_OPT_KEY_ATTACH_COLUMN_IDS: &str = "attach_column_ids";
pub const FUSE_OPT_KEY_ENABLE_PARQUET_DICTIONARY: &str = "enable_parquet_dictionary";

pub const FUSE_TBL_BLOCK_PREFIX: &str = "_b";
pub const FUSE_TBL_BLOCK_INDEX_PREFIX: &str = "_i";
pub const FUSE_TBL_XOR_BLOOM_INDEX_PREFIX: &str = "_i_b_v2";
pub const FUSE_TBL_SEGMENT_PREFIX: &str = "_sg";
pub const FUSE_TBL_SNAPSHOT_PREFIX: &str = "_ss";
pub const FUSE_TBL_SNAPSHOT_STATISTICS_PREFIX: &str = "_ts";
pub const FUSE_TBL_SEGMENT_STATISTICS_PREFIX: &str = "_hs";
pub const FUSE_TBL_LAST_SNAPSHOT_HINT: &str = "last_snapshot_location_hint";
pub const FUSE_TBL_LAST_SNAPSHOT_HINT_V2: &str = "last_snapshot_location_hint_v2";
pub const FUSE_TBL_VIRTUAL_BLOCK_PREFIX: &str = "_vb_v2";
pub const FUSE_TBL_VIRTUAL_BLOCK_PREFIX_V1: &str = "_vb";
pub const FUSE_TBL_AGG_INDEX_PREFIX: &str = "_i_a";
pub const FUSE_TBL_INVERTED_INDEX_PREFIX: &str = "_i_i";
pub const FUSE_TBL_VECTOR_INDEX_PREFIX: &str = "_i_v";
pub const FUSE_TBL_REF_PREFIX: &str = "_ref";

pub const DEFAULT_ROW_PER_PAGE: usize = 8192;
pub const DEFAULT_ROW_PER_INDEX: usize = 100000;

pub const DEFAULT_AVG_DEPTH_THRESHOLD: f64 = 0.001;
