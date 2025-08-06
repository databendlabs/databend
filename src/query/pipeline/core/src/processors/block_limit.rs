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

use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;

// this is used to avoid after limiting by bytes, the rows limit is too small
// TODO: this magic 10 may need to be look backed in the future
const MIN_ROWS_BY_BYTES: usize = 10;

/// DataBlock limit in rows and bytes
pub struct BlockLimit {
    rows: AtomicUsize,
    bytes: AtomicUsize,
}

impl BlockLimit {
    pub fn new(rows: usize, bytes: usize) -> Self {
        BlockLimit {
            rows: AtomicUsize::new(rows),
            bytes: AtomicUsize::new(bytes),
        }
    }

    /// Calculate the number of rows to take based on both row and byte limits
    pub fn calculate_limit_rows(&self, total_rows: usize, total_bytes: usize) -> usize {
        if total_rows == 0 {
            return 0;
        }
        // max with 1 used to avoid division by zero
        let average_bytes_per_row = (total_bytes / total_rows).max(1);
        let rows_by_bytes =
            (self.bytes.load(Ordering::Relaxed) / average_bytes_per_row).max(MIN_ROWS_BY_BYTES);
        let rows_limit = self.rows.load(Ordering::Relaxed);
        rows_limit.min(rows_by_bytes)
    }
}

impl Default for BlockLimit {
    fn default() -> Self {
        BlockLimit {
            rows: AtomicUsize::new(usize::MAX),
            bytes: AtomicUsize::new(usize::MAX),
        }
    }
}
