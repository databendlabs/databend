//  Copyright 2021 Datafuse Labs.
//
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.

use std::collections::HashSet;

use lazy_static::lazy_static;

pub const FUSE_OPT_KEY_BLOCK_IN_MEM_SIZE_THRESHOLD: &str = "block_size_threshold";
pub const FUSE_OPT_KEY_BLOCK_PER_SEGMENT: &str = "block_per_segment";
pub const FUSE_OPT_KEY_ROW_PER_BLOCK: &str = "row_per_block";
pub const FUSE_OPT_KEY_SNAPSHOT_LOC: &str = "snapshot_loc";
pub const FUSE_OPT_KEY_SNAPSHOT_VER: &str = "snapshot_ver";
pub const FUSE_OPT_KEY_DATABASE_ID: &str = "database_id";

pub const FUSE_TBL_BLOCK_PREFIX: &str = "_b";
pub const FUSE_TBL_SEGMENT_PREFIX: &str = "_sg";
pub const FUSE_TBL_SNAPSHOT_PREFIX: &str = "_ss";

pub const DEFAULT_BLOCK_PER_SEGMENT: usize = 1000;
pub const DEFAULT_ROW_PER_BLOCK: usize = 1000 * 1000;
pub const DEFAULT_BLOCK_SIZE_IN_MEM_SIZE_THRESHOLD: usize = 100 * 1024 * 1024;

lazy_static! {
    pub static ref RESERVED_OPTS: HashSet<&'static str> = {
        let mut r = HashSet::new();
        r.insert(FUSE_OPT_KEY_DATABASE_ID);
        r.insert(FUSE_OPT_KEY_SNAPSHOT_VER);
        r
    };
}

pub fn is_fuse_reserved_opt_key<S: AsRef<str>>(opt_key: S) -> bool {
    RESERVED_OPTS.contains(opt_key.as_ref().to_lowercase().as_str())
}
