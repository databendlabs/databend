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
//

pub const TBL_OPT_KEY_SNAPSHOT_LOC: &str = "SNAPSHOT_LOC";
pub const TBL_OPT_KEY_CHUNK_BLOCK_NUM: &str = "CHUNK_BLOCK_NUM";
pub const TBL_OPT_KEY_BLOCK_IN_MEM_SIZE_THRESHOLD: &str = "BLOCK_SIZE_THRESHOLD";

pub const TBL_OPT_KEY_BLOCK_PER_SEGMENT: &str = "BLOCK_PER_SEGMENT";
pub const TBL_OPT_KEY_ROW_PER_BLOCK: &str = "ROW_PER_BLOCK";
pub const FUSE_TBL_BLOCK_PREFIX: &str = "_b";
pub const FUSE_TBL_SEGMENT_PREFIX: &str = "_sg";
pub const FUSE_TBL_SNAPSHOT_PREFIX: &str = "_ss";

pub const DEFAULT_CHUNK_BLOCK_NUM: usize = 1000;

pub const DEFAULT_BLOCK_PER_SEGMENT: usize = 1000;
pub const DEFAULT_ROW_PER_BLOCK: usize = 1000 * 1000;
pub const DEFAULT_BLOCK_SIZE_IN_MEM_SIZE_THRESHOLD: usize = 100 * 1024 * 1024;
