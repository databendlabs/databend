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
use std::io::Read;

use byteorder::LittleEndian;
use byteorder::ReadBytesExt;
use roaring::RoaringBitmap;

use super::compress_integer;
use super::decompress_integer;
use super::IntegerCompression;
use super::IntegerStats;
use super::IntegerType;
use crate::arrow::array::PrimitiveArray;
use crate::arrow::error::Result;
use crate::native::compression::Compression;
use crate::native::write::WriteOptions;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct Freq {}

impl<T: IntegerType> IntegerCompression<T> for Freq {
    fn compress(
        &self,
        array: &PrimitiveArray<T>,
        stats: &IntegerStats<T>,
        write_options: &WriteOptions,
        output: &mut Vec<u8>,
    ) -> Result<usize> {
        let size = output.len();

        let mut top_value_is_null = false;
        let mut top_value = T::default();
        let mut max_count = 0;

        if stats.null_count as f64 / stats.tuple_count as f64 >= 0.9 {
            top_value_is_null = true;
        } else {
            for (val, count) in stats.distinct_values.iter() {
                if *count > max_count {
                    max_count = *count;
                    top_value = *val;
                }
            }
        }

        let mut exceptions_bitmap = RoaringBitmap::new();
        let mut exceptions = Vec::with_capacity(stats.tuple_count - max_count);

        for (i, val) in array.iter().enumerate() {
            if let Some(val) = val {
                if top_value_is_null || *val != top_value {
                    exceptions_bitmap.insert(i as u32);
                    exceptions.push(*val);
                }
            }
        }

        // Write TopValue
        output.extend_from_slice(top_value.to_le_bytes().as_ref());

        // Write exceptions bitmap
        output.extend_from_slice(&(exceptions_bitmap.serialized_size() as u32).to_le_bytes());

        exceptions_bitmap.serialize_into(&mut (*output))?;

        // Write exceptions
        let mut write_options = write_options.clone();
        write_options.forbidden_compressions.push(Compression::Freq);

        let exceptions = PrimitiveArray::<T>::from_vec(exceptions);
        compress_integer(&exceptions, write_options, output)?;

        Ok(output.len() - size)
    }

    fn decompress(&self, mut input: &[u8], length: usize, output: &mut Vec<T>) -> Result<()> {
        let begin = output.len();

        let mut bs = vec![0u8; std::mem::size_of::<T>()];
        input.read_exact(&mut bs)?;
        let a: T::Bytes = match bs.as_slice().try_into() {
            Ok(a) => a,
            Err(_) => unreachable!(),
        };
        let top_value = T::from_le_bytes(a);

        output.reserve(length);
        output.extend(std::iter::repeat(top_value).take(length));

        // read exceptions bitmap
        let exceptions_bitmap_size = input.read_u32::<LittleEndian>()?;
        let exceptions_bitmap =
            RoaringBitmap::deserialize_from(&input[..exceptions_bitmap_size as usize])?;
        input.consume(exceptions_bitmap_size as usize);

        let mut exceptions: Vec<T> = Vec::with_capacity(exceptions_bitmap.len() as usize);
        decompress_integer(
            &mut input,
            exceptions_bitmap.len() as usize,
            &mut exceptions,
            &mut vec![],
        )?;

        assert_eq!(exceptions_bitmap.len() as usize, exceptions.len());

        for (i, val) in exceptions_bitmap.iter().enumerate() {
            output[begin + val as usize] = exceptions[i];
        }

        Ok(())
    }

    fn to_compression(&self) -> Compression {
        Compression::Freq
    }

    fn compress_ratio(&self, stats: &IntegerStats<T>) -> f64 {
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

        if max_count as f64 / stats.tuple_count as f64 >= 0.9 && stats.max.as_i64() >= (1 << 8) {
            return (stats.tuple_count - 1) as f64;
        }

        0.0f64
    }
}
