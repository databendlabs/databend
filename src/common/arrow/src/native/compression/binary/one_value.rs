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

use super::BinaryCompression;
use super::BinaryStats;
use crate::arrow::array::BinaryArray;
use crate::arrow::error::Error;
use crate::arrow::error::Result;
use crate::arrow::types::Offset;
use crate::general_err;
use crate::native::compression::integer::OneValue;
use crate::native::compression::Compression;
use crate::native::write::WriteOptions;

impl<O: Offset> BinaryCompression<O> for OneValue {
    fn to_compression(&self) -> Compression {
        Compression::OneValue
    }

    fn compress_ratio(&self, stats: &super::BinaryStats<O>) -> f64 {
        if stats.unique_count <= 1 {
            stats.tuple_count as f64
        } else {
            0.0f64
        }
    }

    fn compress(
        &self,
        array: &BinaryArray<O>,
        _stats: &BinaryStats<O>,
        _write_options: &WriteOptions,
        output_buf: &mut Vec<u8>,
    ) -> Result<usize> {
        let val = array.iter().find(|v| v.is_some());
        let val = match val {
            Some(Some(v)) => v,
            _ => &[],
        };

        let start = output_buf.len();

        output_buf.extend_from_slice(&(val.len() as u32).to_le_bytes());
        output_buf.extend_from_slice(val);

        Ok(output_buf.len() - start)
    }

    fn decompress(
        &self,
        mut input: &[u8],
        length: usize,
        offsets: &mut Vec<O>,
        values: &mut Vec<u8>,
    ) -> Result<()> {
        let len = input.read_u32::<LittleEndian>()? as usize;

        if input.len() < len {
            return Err(general_err!("data size is less than {}", len));
        }

        let val = &input[..len];
        input.consume(len);

        if offsets.is_empty() {
            offsets.push(O::zero());
        }

        offsets.reserve(length);
        values.reserve(length * val.len());

        for _ in 0..length {
            values.extend_from_slice(val);
            offsets.push(O::from_usize(values.len()).unwrap());
        }
        Ok(())
    }
}
