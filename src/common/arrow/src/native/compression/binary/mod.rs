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

mod dict;
mod freq;
mod one_value;

use std::collections::HashMap;
use std::hash::Hash;
use std::marker::PhantomData;

use super::basic::CommonCompression;
use super::integer::Dict;
use super::integer::Freq;
use super::integer::OneValue;
use super::Compression;
use crate::arrow::array::BinaryArray;
use crate::arrow::buffer::Buffer;
use crate::arrow::error::Error;
use crate::arrow::error::Result;
use crate::arrow::types::Offset;
use crate::native::read::read_basic::read_compress_header;
use crate::native::read::NativeReadBuf;
use crate::native::write::WriteOptions;

pub fn compress_binary<O: Offset>(
    array: &BinaryArray<O>,
    buf: &mut Vec<u8>,
    write_options: WriteOptions,
) -> Result<()> {
    // choose compressor
    let stats = gen_stats(array);
    let compressor = choose_compressor(array, &stats, &write_options);

    log::debug!(
        "choose binary compression : {:?}",
        compressor.to_compression()
    );

    let codec = u8::from(compressor.to_compression());

    match compressor {
        BinaryCompressor::Basic(c) => {
            // offsets
            let offsets = array.offsets();
            let offsets = if offsets.first().is_zero() {
                offsets.buffer().clone()
            } else {
                let first = offsets.first();
                let mut zero_offsets = Vec::with_capacity(offsets.len());
                for offset in offsets.iter() {
                    zero_offsets.push(*offset - *first);
                }
                zero_offsets.into()
            };

            let input_buf = bytemuck::cast_slice(&offsets);
            buf.extend_from_slice(&codec.to_le_bytes());
            let pos = buf.len();
            buf.extend_from_slice(&[0u8; 8]);

            let compressed_size = c.compress(input_buf, buf)?;

            buf[pos..pos + 4].copy_from_slice(&(compressed_size as u32).to_le_bytes());
            buf[pos + 4..pos + 8].copy_from_slice(&(input_buf.len() as u32).to_le_bytes());

            // values
            let mut values = array.values().clone();
            values.slice(
                array.offsets().first().to_usize(),
                array.offsets().last().to_usize() - array.offsets().first().to_usize(),
            );
            let input_buf = bytemuck::cast_slice(&values);
            buf.extend_from_slice(&codec.to_le_bytes());
            let pos = buf.len();
            buf.extend_from_slice(&[0u8; 8]);

            let compressed_size = c.compress(input_buf, buf)?;
            buf[pos..pos + 4].copy_from_slice(&(compressed_size as u32).to_le_bytes());
            buf[pos + 4..pos + 8].copy_from_slice(&(input_buf.len() as u32).to_le_bytes());
        }
        BinaryCompressor::Extend(c) => {
            buf.extend_from_slice(&codec.to_le_bytes());
            let pos = buf.len();
            buf.extend_from_slice(&[0u8; 8]);
            let compressed_size = c.compress(array, &stats, &write_options, buf)?;
            buf[pos..pos + 4].copy_from_slice(&(compressed_size as u32).to_le_bytes());
            buf[pos + 4..pos + 8].copy_from_slice(&(array.values().len() as u32).to_le_bytes());
        }
    }

    Ok(())
}

pub fn decompress_binary<O: Offset, R: NativeReadBuf>(
    reader: &mut R,
    length: usize,
    offsets: &mut Vec<O>,
    values: &mut Vec<u8>,
    scratch: &mut Vec<u8>,
) -> Result<()> {
    let (codec, compressed_size, _uncompressed_size) = read_compress_header(reader)?;
    let compression = Compression::from_codec(codec)?;

    // already fit in buffer
    let mut use_inner = false;
    reader.fill_buf()?;
    let input = if reader.buffer_bytes().len() >= compressed_size {
        use_inner = true;
        reader.buffer_bytes()
    } else {
        scratch.resize(compressed_size, 0);
        reader.read_exact(scratch.as_mut_slice())?;
        scratch.as_slice()
    };

    let encoder = BinaryCompressor::<O>::from_compression(compression)?;

    match encoder {
        BinaryCompressor::Basic(c) => {
            let last = offsets.last().cloned();
            offsets.reserve(length + 1);
            let out_slice = unsafe {
                core::slice::from_raw_parts_mut(
                    offsets.as_mut_ptr().add(offsets.len()) as *mut u8,
                    (length + 1) * std::mem::size_of::<O>(),
                )
            };
            c.decompress(&input[..compressed_size], out_slice)?;

            if use_inner {
                reader.consume(compressed_size);
            }

            if let Some(last) = last {
                // fix offset:
                // because the offsets in current page is append to the original offsets,
                // each new offset value must add the last value in original offsets.
                let new_length = offsets.len() + length;
                for i in offsets.len()..new_length {
                    let next_val = unsafe { *offsets.get_unchecked(i + 1) };
                    let val = unsafe { offsets.get_unchecked_mut(i) };
                    *val = last + next_val;
                }
                unsafe { offsets.set_len(new_length) };
            } else {
                unsafe { offsets.set_len(length + 1) };
            }

            // values
            let (_, compressed_size, uncompressed_size) = read_compress_header(reader)?;
            use_inner = false;
            reader.fill_buf()?;
            let input = if reader.buffer_bytes().len() >= compressed_size {
                use_inner = true;
                reader.buffer_bytes()
            } else {
                scratch.resize(compressed_size, 0);
                reader.read_exact(scratch.as_mut_slice())?;
                scratch.as_slice()
            };

            values.reserve(uncompressed_size);
            let out_slice = unsafe {
                core::slice::from_raw_parts_mut(
                    values.as_mut_ptr().add(values.len()),
                    uncompressed_size,
                )
            };
            c.decompress(&input[..compressed_size], out_slice)?;
            unsafe { values.set_len(values.len() + uncompressed_size) };

            if use_inner {
                reader.consume(compressed_size);
            }
        }
        BinaryCompressor::Extend(c) => {
            c.decompress(input, length, offsets, values)?;
            if use_inner {
                reader.consume(compressed_size);
            }
        }
    }

    Ok(())
}

pub trait BinaryCompression<O: Offset> {
    fn compress(
        &self,
        array: &BinaryArray<O>,
        stats: &BinaryStats<O>,
        write_options: &WriteOptions,
        output: &mut Vec<u8>,
    ) -> Result<usize>;

    fn decompress(
        &self,
        input: &[u8],
        length: usize,
        offsets: &mut Vec<O>,
        values: &mut Vec<u8>,
    ) -> Result<()>;

    fn compress_ratio(&self, stats: &BinaryStats<O>) -> f64;
    fn to_compression(&self) -> Compression;
}

enum BinaryCompressor<O: Offset> {
    Basic(CommonCompression),
    Extend(Box<dyn BinaryCompression<O>>),
}

impl<O: Offset> BinaryCompressor<O> {
    fn to_compression(&self) -> Compression {
        match self {
            Self::Basic(c) => c.to_compression(),
            Self::Extend(c) => c.to_compression(),
        }
    }

    fn from_compression(compression: Compression) -> Result<Self> {
        if let Ok(c) = CommonCompression::try_from(&compression) {
            return Ok(Self::Basic(c));
        }
        match compression {
            Compression::OneValue => Ok(Self::Extend(Box::new(OneValue {}))),
            Compression::Freq => Ok(Self::Extend(Box::new(Freq {}))),
            Compression::Dict => Ok(Self::Extend(Box::new(Dict {}))),
            other => Err(Error::OutOfSpec(format!(
                "Unknown compression codec {other:?}",
            ))),
        }
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct U8Buffer(pub(crate) Buffer<u8>);

impl Hash for U8Buffer {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.0.as_slice().hash(state)
    }
}

impl Eq for U8Buffer {}

impl std::ops::Deref for U8Buffer {
    type Target = [u8];

    fn deref(&self) -> &Self::Target {
        self.0.as_slice()
    }
}

#[allow(dead_code)]
#[derive(Debug)]
pub struct BinaryStats<O> {
    tuple_count: usize,
    total_bytes: usize,
    unique_count: usize,
    total_unique_size: usize,
    null_count: usize,
    distinct_values: HashMap<U8Buffer, usize>,
    _data: PhantomData<O>,
}

fn gen_stats<O: Offset>(array: &BinaryArray<O>) -> BinaryStats<O> {
    let mut stats = BinaryStats {
        tuple_count: array.len(),
        total_bytes: array.values().len() + (array.len() + 1) * std::mem::size_of::<O>(),
        unique_count: 0,
        total_unique_size: 0,
        null_count: array.validity().map(|v| v.unset_bits()).unwrap_or_default(),
        distinct_values: HashMap::new(),
        _data: PhantomData,
    };

    for o in array.offsets().windows(2) {
        let mut values = array.values().clone();
        values.slice(o[0].to_usize(), o[1].to_usize() - o[0].to_usize());

        *stats.distinct_values.entry(U8Buffer(values)).or_insert(0) += 1;
    }

    stats.total_unique_size = stats
        .distinct_values
        .keys()
        .map(|v| v.0.len() + 8)
        .sum::<usize>();
    stats.unique_count = stats.distinct_values.len();

    stats
}

fn choose_compressor<O: Offset>(
    _value: &BinaryArray<O>,
    stats: &BinaryStats<O>,
    write_options: &WriteOptions,
) -> BinaryCompressor<O> {
    #[cfg(debug_assertions)]
    {
        if crate::native::util::env::check_freq_env()
            && !write_options
                .forbidden_compressions
                .contains(&Compression::Freq)
        {
            return BinaryCompressor::Extend(Box::new(Freq {}));
        }
        if crate::native::util::env::check_dict_env()
            && !write_options
                .forbidden_compressions
                .contains(&Compression::Dict)
        {
            return BinaryCompressor::Extend(Box::new(Dict {}));
        }
    }
    // todo
    let basic = BinaryCompressor::Basic(write_options.default_compression);
    if let Some(ratio) = write_options.default_compress_ratio {
        let mut max_ratio = ratio;
        let mut result = basic;

        let compressors: Vec<Box<dyn BinaryCompression<O>>> = vec![
            Box::new(OneValue {}) as _,
            Box::new(Freq {}) as _,
            Box::new(Dict {}) as _,
        ];

        for encoder in compressors {
            if write_options
                .forbidden_compressions
                .contains(&encoder.to_compression())
            {
                continue;
            }
            let r = encoder.compress_ratio(stats);
            if r > max_ratio {
                max_ratio = r;
                result = BinaryCompressor::Extend(encoder);

                if r == stats.tuple_count as f64 {
                    break;
                }
            }
        }
        result
    } else {
        basic
    }
}
