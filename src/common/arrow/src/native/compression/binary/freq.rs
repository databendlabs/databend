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
use std::ops::Deref;

use byteorder::LittleEndian;
use byteorder::ReadBytesExt;
use roaring::RoaringBitmap;

use super::BinaryCompression;
use super::BinaryStats;
use crate::arrow::array::BinaryArray;
use crate::arrow::error::Error;
use crate::arrow::error::Result;
use crate::arrow::types::Offset;
use crate::general_err;
use crate::native::compression::integer::Freq;
use crate::native::compression::Compression;
use crate::native::write::WriteOptions;

impl<O: Offset> BinaryCompression<O> for Freq {
    fn to_compression(&self) -> Compression {
        Compression::Freq
    }

    fn compress(
        &self,
        array: &BinaryArray<O>,
        stats: &BinaryStats<O>,
        write_options: &WriteOptions,
        output: &mut Vec<u8>,
    ) -> Result<usize> {
        let size = output.len();

        let mut top_value_is_null = false;
        let t = vec![];
        let mut top_value: &[u8] = &t;
        let mut max_count = 0;

        if stats.null_count as f64 / stats.tuple_count as f64 >= 0.9 {
            top_value_is_null = true;
        } else {
            for (val, count) in stats.distinct_values.iter() {
                if *count > max_count {
                    max_count = *count;
                    top_value = val.deref();
                }
            }
        }

        let mut exceptions_bitmap = RoaringBitmap::new();
        let mut exceptions = Vec::with_capacity(stats.tuple_count - max_count);

        for (i, val) in array.iter().enumerate() {
            if let Some(val) = val {
                if top_value_is_null || val != top_value {
                    exceptions_bitmap.insert(i as u32);
                    exceptions.push(val);
                }
            }
        }

        // Write TopValue
        output.extend_from_slice(&(top_value.len() as u64).to_le_bytes());
        output.extend_from_slice(top_value);

        // Write exceptions bitmap
        output.extend_from_slice(&(exceptions_bitmap.serialized_size() as u32).to_le_bytes());

        exceptions_bitmap.serialize_into(&mut (*output))?;

        // Write exceptions
        let mut write_options = write_options.clone();
        write_options.forbidden_compressions.push(Compression::Freq);

        // Plain encoding
        for exception in exceptions {
            output.extend_from_slice(&(exception.len() as u64).to_le_bytes());
            output.extend_from_slice(exception);
        }
        Ok(output.len() - size)
    }

    fn decompress(
        &self,
        mut input: &[u8],
        length: usize,
        offsets: &mut Vec<O>,
        values: &mut Vec<u8>,
    ) -> Result<()> {
        let len = input.read_u64::<LittleEndian>()? as usize;
        if input.len() < len {
            return Err(general_err!("data size is less than {}", len));
        }
        let top_value = &input[..len];
        input.consume(len);

        // read exceptions bitmap
        let exceptions_bitmap_size = input.read_u32::<LittleEndian>()?;
        let exceptions_bitmap =
            RoaringBitmap::deserialize_from(&input[..exceptions_bitmap_size as usize])?;
        input.consume(exceptions_bitmap_size as usize);

        if offsets.is_empty() {
            offsets.push(O::default());
        }

        offsets.reserve(length);
        for i in 0..length {
            if exceptions_bitmap.contains(i as u32) {
                let len = input.read_u64::<LittleEndian>()? as usize;
                if input.len() < len {
                    return Err(general_err!("data size is less than {}", len));
                }
                let val = &input[..len];
                input.consume(len);

                values.extend_from_slice(val);
                offsets.push(O::from_usize(values.len()).unwrap());
            } else {
                values.extend_from_slice(top_value);
                offsets.push(O::from_usize(values.len()).unwrap());
            }
        }

        Ok(())
    }

    fn compress_ratio(&self, stats: &super::BinaryStats<O>) -> f64 {
        if stats.unique_count <= 1 {
            return 0.0f64;
        }

        if stats.null_count as f64 / stats.tuple_count as f64 >= 0.9 {
            return (stats.tuple_count - 1) as f64;
        }

        let mut max_count = 0;

        for (_val, count) in stats.distinct_values.iter() {
            if *count > max_count {
                max_count = *count;
            }
        }

        if max_count as f64 / stats.tuple_count as f64 >= 0.9 {
            return (stats.tuple_count - 1) as f64;
        }

        0.0f64
    }
}
