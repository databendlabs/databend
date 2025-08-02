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

/// An indicator to limit the size of datablocks transferred between processor
#[derive(Debug, Copy, Clone)]
pub struct DataBlockLimit {
    /// The maximum number of rows allowed in a data block
    pub max_rows: usize,
    /// The maximum size in bytes allowed for a data block
    pub max_bytes: usize,
}

impl DataBlockLimit {
    pub fn new(mut max_rows: usize, mut max_bytes: usize) -> Self {
        // If max_rows or max_bytes is set to 0, we treat it as no limit
        if max_rows == 0 {
            max_rows = usize::MAX;
        }
        if max_bytes == 0 {
            max_bytes = usize::MAX;
        }

        Self {
            max_rows,
            max_bytes,
        }
    }

    /// Calculate the number of rows to take based on both row and byte limits
    pub fn rows_to_take(&self, total_rows: usize, total_bytes: usize) -> usize {
        debug_assert!(total_rows > 0);
        // check row limit
        let rows_by_limit = self.max_rows.min(total_rows);

        // check byte limit
        let average_bytes_per_row = total_bytes / total_rows;
        let rows_by_bytes = if average_bytes_per_row > 0 {
            // TODO: this 1 may be not suitable
            (self.max_bytes / average_bytes_per_row).max(1)
        } else {
            // Avoid division by zero
            total_rows
        };

        // the minimum of the two limits
        rows_by_limit.min(rows_by_bytes)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_rows_to_take_basic() {
        let limit = DataBlockLimit::new(1000, 1_000_000);

        // Case 1: Both limits not exceeded
        let rows = limit.rows_to_take(500, 500_000);
        assert_eq!(rows, 500);

        // Case 2: Row limit exceeded
        let rows = limit.rows_to_take(2000, 500_000);
        assert_eq!(rows, 1000);

        // Case 3: Byte limit exceeded
        // 2_000_000 bytes / 1000 rows = 2000 bytes per row
        // 1_000_000 bytes / 2000 bytes per row = 500 rows
        let rows = limit.rows_to_take(1000, 2_000_000);
        assert_eq!(rows, 500);

        let limit = DataBlockLimit::new(1000, 1_000_000);

        // Both limits exceeded, byte limit is more restrictive
        // 4_000_000 bytes / 2000 rows = 2000 bytes per row
        // 1_000_000 bytes / 2000 bytes per row = 500 rows
        let rows = limit.rows_to_take(2000, 4_000_000);
        assert_eq!(rows, 500);

        // Both limits exceeded, row limit is more restrictive
        // 1_500_000 bytes / 2000 rows = 750 bytes per row
        // 1_000_000 bytes / 750 bytes per row = 1333 rows
        // But row limit is 1000, so we take 1000
        let rows = limit.rows_to_take(2000, 1_500_000);
        assert_eq!(rows, 1000);
    }
}
