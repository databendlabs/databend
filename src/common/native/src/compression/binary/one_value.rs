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

use std::io::BufRead;

use byteorder::LittleEndian;
use byteorder::ReadBytesExt;
use databend_common_column::binary::BinaryColumn;

use super::BinaryCompression;
use super::BinaryStats;
use crate::compression::Compression;
use crate::compression::integer::OneValue;
use crate::error::Error;
use crate::error::Result;
use crate::write::WriteOptions;

impl BinaryCompression for OneValue {
    fn to_compression(&self) -> Compression {
        Compression::OneValue
    }

    fn compress_ratio(&self, stats: &super::BinaryStats) -> f64 {
        if stats.unique_count <= 1 {
            stats.tuple_count as f64
        } else {
            0.0f64
        }
    }

    fn compress(
        &self,
        col: &BinaryColumn,
        _stats: &BinaryStats,
        _write_options: &WriteOptions,
        output_buf: &mut Vec<u8>,
    ) -> Result<usize> {
        let val = col.iter().next().unwrap_or(&[]);

        let start = output_buf.len();

        output_buf.extend_from_slice(&(val.len() as u32).to_le_bytes());
        output_buf.extend_from_slice(val);

        Ok(output_buf.len() - start)
    }

    fn decompress(
        &self,
        mut input: &[u8],
        length: usize,
        offsets: &mut Vec<u64>,
        values: &mut Vec<u8>,
    ) -> Result<()> {
        let len = input.read_u32::<LittleEndian>()? as usize;

        if input.len() < len {
            return Err(general_err!("data size is less than {}", len));
        }

        let val = &input[..len];
        input.consume(len);

        if offsets.is_empty() {
            offsets.push(0);
        }

        offsets.reserve(length);
        values.reserve(length * val.len());

        for _ in 0..length {
            values.extend_from_slice(val);
            offsets.push(values.len() as u64);
        }
        Ok(())
    }
}
