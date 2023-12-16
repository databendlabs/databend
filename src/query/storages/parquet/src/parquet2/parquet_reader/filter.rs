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

use databend_common_arrow::arrow::bitmap::Bitmap;
use databend_common_arrow::parquet::indexes::Interval;

/// A wrapper of [`Bitmap`] with a position mark. It is used to filter rows when reading a parquet file.
///
/// If a whole page is filtered, there is no need to decompress the page.
/// If a row is filtered, there is no need to decode the row.
pub struct FilterState {
    bitmap: Bitmap,
    pos: usize,
}

impl FilterState {
    pub fn new(bitmap: Bitmap) -> Self {
        Self { bitmap, pos: 0 }
    }

    #[inline]
    pub fn advance(&mut self, num: usize) {
        self.pos += num;
    }

    /// Return true if [`self.pos`, `self.pos + num`) are set.
    #[inline]
    pub fn range_all_set(&self, num: usize) -> bool {
        self.bitmap.null_count_range(self.pos, num) == 0
    }

    /// Return true if [`self.pos`, `self.pos + num`) are unset.
    #[inline]
    pub fn range_all_unset(&self, num: usize) -> bool {
        self.bitmap.null_count_range(self.pos, num) == num
    }

    /// Convert the validity of [`self.pos`, `self.pos + num`) to [`Interval`]s.
    pub fn convert_to_intervals(&self, num_rows: usize) -> Vec<Interval> {
        let mut res = vec![];
        let mut started = false;
        let mut start = 0;
        for (i, v) in self.bitmap.iter().skip(self.pos).take(num_rows).enumerate() {
            if v {
                if !started {
                    start = i;
                    started = true;
                }
            } else if started {
                res.push(Interval::new(start, i - start));
                started = false;
            }
        }

        if started {
            res.push(Interval::new(start, num_rows - start));
        }

        res
    }
}
