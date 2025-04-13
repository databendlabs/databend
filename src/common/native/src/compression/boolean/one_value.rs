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

use databend_common_column::bitmap::Bitmap;
use databend_common_column::bitmap::MutableBitmap;

use super::BooleanCompression;
use crate::compression::integer::OneValue;
use crate::compression::Compression;
use crate::error::Error;
use crate::error::Result;
use crate::general_err;

impl BooleanCompression for OneValue {
    fn to_compression(&self) -> Compression {
        Compression::OneValue
    }

    fn compress_ratio(&self, stats: &super::BooleanStats) -> f64 {
        if stats.true_count == 0 || stats.false_count == 0 {
            stats.rows as f64
        } else {
            0.0f64
        }
    }

    fn compress(
        &self,
        col: &Bitmap,
        _validity: Option<Bitmap>,
        output_buf: &mut Vec<u8>,
    ) -> Result<usize> {
        let val = col.iter().next_back().unwrap_or_default();
        output_buf.push(val as u8);
        Ok(1)
    }

    fn decompress(&self, input: &[u8], length: usize, output: &mut MutableBitmap) -> Result<()> {
        if input.is_empty() {
            return Err(general_err!("data size is less than {}", 1));
        }
        let val = input[0] > 0;
        output.extend_constant(length, val);
        Ok(())
    }
}
