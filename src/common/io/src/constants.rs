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

pub const TRUE_BYTES_LOWER: &str = "true";
pub const FALSE_BYTES_LOWER: &str = "false";
pub const TRUE_BYTES_NUM: &str = "1";
pub const FALSE_BYTES_NUM: &str = "0";
pub const NULL_BYTES_UPPER: &str = "NULL";
pub const NULL_BYTES_LOWER: &str = "null";
pub const NULL_BYTES_ESCAPE: &str = "\\N";
pub const NAN_BYTES_SNAKE: &str = "NaN";
pub const NAN_BYTES_LOWER: &str = "nan";
pub const INF_BYTES_LOWER: &str = "inf";
pub const INF_BYTES_LONG: &str = "Infinity";

// The size of the I/O read/write block buffer by default.
pub const DEFAULT_BLOCK_BUFFER_SIZE: usize = 100 * 1024 * 1024;
// The size of the I/O read/write block index buffer by default.
pub const DEFAULT_BLOCK_INDEX_BUFFER_SIZE: usize = 300 * 1024;
// The max number of a block by default.
pub const DEFAULT_BLOCK_MAX_ROWS: usize = 1000 * 1000;
// The min number of a block by default.
pub const DEFAULT_BLOCK_MIN_ROWS: usize = 800 * 1000;

// The min values of data_retention_period_in_hours
pub const DEFAULT_MIN_TABLE_LEVEL_DATA_RETENTION_PERIOD_IN_HOURS: u64 = 1;
