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
use databend_common_column::types::Index;

use super::BinaryCompression;
use super::BinaryStats;
use crate::compression::get_bits_needed;
use crate::compression::integer::compress_integer;
use crate::compression::integer::decompress_integer;
use crate::compression::integer::Dict;
use crate::compression::integer::DictEncoder;
use crate::compression::is_valid;
use crate::compression::Compression;
use crate::error::Error;
use crate::error::Result;
use crate::general_err;
use crate::util::AsBytes;
use crate::write::WriteOptions;

impl BinaryCompression for Dict {
    fn to_compression(&self) -> Compression {
        Compression::Dict
    }

    fn compress_ratio(&self, stats: &super::BinaryStats) -> f64 {
        const MIN_DICT_RATIO: usize = 3;
        if stats.unique_count * MIN_DICT_RATIO >= stats.tuple_count {
            return 0.0f64;
        }

        let mut after_size = stats.total_unique_size
            + stats.tuple_count * (get_bits_needed(stats.unique_count as u64) / 8) as usize;
        after_size += (stats.tuple_count) * 2 / 128;
        stats.total_bytes as f64 / after_size as f64
    }

    fn compress(
        &self,
        col: &BinaryColumn,
        stats: &BinaryStats,
        write_options: &WriteOptions,
        output_buf: &mut Vec<u8>,
    ) -> Result<usize> {
        let start = output_buf.len();
        let mut encoder = DictEncoder::with_capacity(col.len());

        for (i, range) in col.offsets().windows(2).enumerate() {
            if !is_valid(stats.validity.as_ref(), i) && !encoder.is_empty() {
                encoder.push_last_index();
            } else {
                let data = col.data().clone().sliced(
                    range[0].to_usize(),
                    range[1].to_usize() - range[0].to_usize(),
                );
                encoder.push(&data);
            }
        }

        let indices = encoder.take_indices();
        // dict data use custom encoding
        let mut write_options = write_options.clone();
        write_options.forbidden_compressions.push(Compression::Dict);
        compress_integer(&indices, stats.validity.clone(), &write_options, output_buf)?;

        // data page use plain encoding
        let sets = encoder.get_sets();
        output_buf.extend_from_slice(&(sets.len() as u32).to_le_bytes());
        for val in sets.iter() {
            let bs = val.as_bytes();
            output_buf.extend_from_slice(&(bs.len() as u64).to_le_bytes());
            output_buf.extend_from_slice(bs.as_ref());
        }

        Ok(output_buf.len() - start)
    }

    fn decompress(
        &self,
        mut input: &[u8],
        length: usize,
        offsets: &mut Vec<u64>,
        values: &mut Vec<u8>,
    ) -> Result<()> {
        let mut indices: Vec<u32> = Vec::new();
        decompress_integer(&mut input, length, &mut indices, &mut vec![])?;

        let mut data: Vec<u8> = vec![];
        let mut data_offsets = vec![0];

        let mut last_offset = 0;

        let data_size = input.read_u32::<LittleEndian>()? as usize;
        for _ in 0..data_size {
            let len = input.read_u64::<LittleEndian>()? as usize;
            if input.len() < len {
                return Err(general_err!("data size is less than {}", len));
            }
            last_offset += len;
            data_offsets.push(last_offset);
            data.extend_from_slice(&input[..len]);
            input.consume(len);
        }

        last_offset = if offsets.is_empty() {
            offsets.push(0);
            0
        } else {
            offsets.last().unwrap().to_usize()
        };

        offsets.reserve(indices.len());

        for i in indices.iter() {
            let off = data_offsets[*i as usize];
            let end = data_offsets[(*i + 1) as usize];

            values.extend_from_slice(&data[off..end]);

            last_offset += end - off;
            offsets.push(last_offset as u64);
        }
        Ok(())
    }
}
