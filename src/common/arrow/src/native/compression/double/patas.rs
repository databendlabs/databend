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

use std::collections::HashMap;
use std::io::BufRead;
use std::io::Read;

use byteorder::LittleEndian;
use byteorder::ReadBytesExt;
use ringbuffer::AllocRingBuffer;
use ringbuffer::RingBuffer;

use super::compress_sample_ratio;
use super::DoubleCompression;
use super::DoubleStats;
use super::DoubleType;
use crate::arrow::array::PrimitiveArray;
use crate::arrow::error::Result;
use crate::arrow::types::NativeType;
use crate::native::compression::Compression;
use crate::native::compression::SAMPLE_COUNT;
use crate::native::compression::SAMPLE_SIZE;
use crate::native::util::ByteWriter;
use crate::native::write::WriteOptions;

pub(crate) struct Patas {}

impl<T: DoubleType> DoubleCompression<T> for Patas {
    fn compress(
        &self,
        array: &PrimitiveArray<T>,
        _stats: &DoubleStats<T>,
        _write_options: &WriteOptions,
        output: &mut Vec<u8>,
    ) -> Result<usize> {
        let mut is_first = false;
        const BLOCK_SIZE: usize = 128;
        let mut ring = AllocRingBuffer::<T::BitType>::new(BLOCK_SIZE);
        let mut indices = HashMap::with_capacity(BLOCK_SIZE);

        let mut byte_writer =
            ByteWriter::<false>::with_capacity(array.len() * (std::mem::size_of::<T>() + 2));

        for (i, val) in array.values().iter().enumerate() {
            let val = val.as_bits();

            if !is_first {
                is_first = true;
                byte_writer.write_value(val);
            } else {
                let mut reference_index = indices.get(&val).cloned().unwrap_or(0);
                let exceeds_highest_index = reference_index > i;
                let difference_too_big = (i - reference_index) >= BLOCK_SIZE;

                if exceeds_highest_index || difference_too_big {
                    reference_index = i - 1;
                };

                let reference_diff = i - reference_index;

                let refer_value = ring.get(-(reference_diff as isize)).unwrap();
                let xor_result = val ^ *refer_value;

                let trailing_zeros = T::trailing_zeros(&xor_result) as u8;
                let leading_zeros = T::leading_zeros(&xor_result) as u8;

                let is_equal = u8::from(trailing_zeros == std::mem::size_of::<T>() as u8 * 8);
                let significant_bits = if is_equal == 1 {
                    0
                } else {
                    std::mem::size_of::<T>() as u8 * 8 - trailing_zeros - leading_zeros
                };

                let significant_bytes = (((significant_bits as usize) >> 3)
                    + usize::from((significant_bits & 7) != 0))
                    as u8;

                let pack_data = pack(
                    reference_diff as u8,
                    significant_bytes,
                    trailing_zeros - is_equal,
                );

                byte_writer.write_value(pack_data);
                byte_writer.write_value_bytes(
                    xor_result >> (trailing_zeros - is_equal) as usize,
                    significant_bytes as usize,
                );
            }

            ring.push(val);
            indices.insert(val, i);
        }

        output.extend_from_slice(byte_writer.data());
        Ok(byte_writer.data().len())
    }

    fn decompress(&self, mut input: &[u8], length: usize, output: &mut Vec<T>) -> Result<()> {
        let mut bs = vec![0u8; std::mem::size_of::<T>()];
        input.read_exact(&mut bs)?;
        let a: T::Bytes = match bs.as_slice().try_into() {
            Ok(a) => a,
            Err(_) => unreachable!(),
        };
        let first = T::from_le_bytes(a);

        output.reserve(length);
        output.push(first);

        for _ in 0..length - 1 {
            let packed_data = input.read_u16::<LittleEndian>()?;
            let (reference_diff, significant_bytes, trailing_zeros) = unpack(packed_data);

            let val: T::BitType = read_value_custom(input, significant_bytes, trailing_zeros);
            input.consume(significant_bytes as usize);

            let previous = output[output.len() - (reference_diff as usize)].as_bits();
            let xor_result = (val << (trailing_zeros as usize)) ^ previous;

            output.push(T::from_bits_val(xor_result));
        }
        Ok(())
    }

    fn to_compression(&self) -> Compression {
        Compression::Patas
    }

    fn compress_ratio(&self, stats: &DoubleStats<T>) -> f64 {
        compress_sample_ratio(self, stats, SAMPLE_COUNT, SAMPLE_SIZE)
    }
}

// Combine the reference index (7 bits), the amount of significant bytes (3 bits), and the trailing zeros (6 bits) into a single 2-byte integer.
#[inline]
pub fn pack(reference_index: u8, significant_bytes: u8, trailing_zeros: u8) -> u16 {
    ((reference_index as u16) << 9)
        | (((significant_bytes & 7) as u16) << 6)
        | (trailing_zeros as u16)
}

#[inline]
pub fn unpack(packed_data: u16) -> (u8, u8, u8) {
    // Extract the values from the packed data using bit masks and shifts.
    let reference_index = ((packed_data >> 9) & 0b0111_1111) as u8;
    let mut significant_bytes = ((packed_data >> 6) & 0b0000_0111) as u8;
    let trailing_zeros = (packed_data & 0b0011_1111) as u8;

    if trailing_zeros < 63 && significant_bytes == 0 {
        significant_bytes = 8;
    }
    (reference_index, significant_bytes, trailing_zeros)
}

#[inline]
pub fn read_value_custom<T: NativeType>(input: &[u8], mut bytes: u8, trailing_zero: u8) -> T {
    if (bytes > 8) && trailing_zero < 8 {
        bytes = 8;
    }

    if bytes > 8 {
        return T::default();
    }

    let mut bs = vec![0u8; std::mem::size_of::<T>()];
    unsafe { std::ptr::copy(input.as_ptr(), bs.as_mut_ptr(), bytes as usize) }

    let a: T::Bytes = match bs.as_slice().try_into() {
        Ok(a) => a,
        Err(_) => unreachable!(),
    };

    T::from_le_bytes(a)
}

#[test]
fn test_unpack() {
    let data = vec![(692, (1, 2, 52)), (1026, (2, 8, 2))];

    for (p, (reference_index, significant_bytes, trailing_zeros)) in data {
        assert_eq!(pack(reference_index, significant_bytes, trailing_zeros), p);
        assert_eq!(
            (reference_index, significant_bytes, trailing_zeros),
            unpack(p)
        );
    }
}
