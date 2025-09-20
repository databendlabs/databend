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

use databend_common_column::binary::BinaryColumn;
use databend_common_column::bitmap::Bitmap;
use databend_common_column::types::Index;
use databend_common_expression::types::Buffer;

use super::basic::CommonCompression;
use super::integer::Dict;
use super::integer::Freq;
use super::integer::OneValue;
use super::Compression;
use crate::error::Error;
use crate::error::Result;
use crate::read::read_basic::read_compress_header;
use crate::read::NativeReadBuf;
use crate::write::WriteOptions;

pub fn compress_binary(
    col: &BinaryColumn,
    validity: Option<Bitmap>,
    buf: &mut Vec<u8>,
    write_options: WriteOptions,
) -> Result<()> {
    // choose compressor
    let stats = gen_stats(col, validity);
    let compressor = choose_compressor(col, &stats, &write_options);

    log::debug!(
        "choose binary compression : {:?}",
        compressor.to_compression()
    );

    let codec = compressor.to_compression() as u8;

    match compressor {
        BinaryCompressor::Basic(c) => {
            // offsets
            let offsets = col.offsets();
            let offsets = if *offsets.first().unwrap() == 0 {
                offsets.clone()
            } else {
                let first = offsets.first().unwrap();
                let mut zero_offsets = vec![0_u64; offsets.len()];
                for (idx, offset) in offsets.iter().enumerate() {
                    zero_offsets[idx] = *offset - *first;
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
            let mut values = col.data().clone();
            values.slice(
                col.offsets().first().unwrap().to_usize(),
                col.offsets().last().unwrap().to_usize()
                    - col.offsets().first().unwrap().to_usize(),
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
            let compressed_size = c.compress(col, &stats, &write_options, buf)?;
            buf[pos..pos + 4].copy_from_slice(&(compressed_size as u32).to_le_bytes());
            buf[pos + 4..pos + 8].copy_from_slice(&(col.data().len() as u32).to_le_bytes());
        }
    }

    Ok(())
}

pub fn decompress_binary<R: NativeReadBuf>(
    reader: &mut R,
    length: usize,
    offsets: &mut Vec<u64>,
    values: &mut Vec<u8>,
    scratch: &mut Vec<u8>,
) -> Result<()> {
    let (compression, compressed_size, _uncompressed_size) = read_compress_header(reader, scratch)?;

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

    let encoder = BinaryCompressor::from_compression(compression)?;

    match encoder {
        BinaryCompressor::Basic(c) => {
            let last = offsets.last().cloned();
            offsets.reserve(length + 1);
            let out_slice = unsafe {
                core::slice::from_raw_parts_mut(
                    offsets.as_mut_ptr().add(offsets.len()) as *mut u8,
                    (length + 1) * std::mem::size_of::<u64>(),
                )
            };
            c.decompress(&input[..compressed_size], out_slice)?;

            if use_inner {
                reader.consume(compressed_size);
            }
            let old_length = offsets.len();
            let new_length = offsets.len() + length;
            unsafe { offsets.set_len(new_length + 1) };

            if let Some(last) = last {
                // fix offset:
                // because the offsets in current page is append to the original offsets,
                // each new offset value must add the last value in original offsets.
                for i in old_length..new_length {
                    let next_val = unsafe { *offsets.get_unchecked(i + 1) };
                    let val = unsafe { offsets.get_unchecked_mut(i) };
                    *val = last + next_val;
                }
                unsafe { offsets.set_len(new_length) };
            }

            // values
            let (_, compressed_size, uncompressed_size) = read_compress_header(reader, scratch)?;
            c.decompress_common_binary(
                reader,
                uncompressed_size,
                compressed_size,
                values,
                scratch,
            )?;
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

pub trait BinaryCompression {
    fn compress(
        &self,
        col: &BinaryColumn,
        stats: &BinaryStats,
        write_options: &WriteOptions,
        output: &mut Vec<u8>,
    ) -> Result<usize>;

    fn decompress(
        &self,
        input: &[u8],
        length: usize,
        offsets: &mut Vec<u64>,
        values: &mut Vec<u8>,
    ) -> Result<()>;

    fn compress_ratio(&self, stats: &BinaryStats) -> f64;
    fn to_compression(&self) -> Compression;
}

enum BinaryCompressor {
    Basic(CommonCompression),
    Extend(Box<dyn BinaryCompression>),
}

impl BinaryCompressor {
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
pub struct BinaryStats {
    tuple_count: usize,
    total_bytes: usize,
    unique_count: usize,
    total_unique_size: usize,
    validity: Option<Bitmap>,
    null_count: usize,
    distinct_values: HashMap<U8Buffer, usize>,
}

fn gen_stats(col: &BinaryColumn, validity: Option<Bitmap>) -> BinaryStats {
    let mut stats = BinaryStats {
        tuple_count: col.len(),
        total_bytes: col.data().len() + (col.len() + 1) * std::mem::size_of::<u64>(),
        unique_count: 0,
        total_unique_size: 0,
        null_count: validity
            .as_ref()
            .map(|v| v.null_count())
            .unwrap_or_default(),
        validity,
        distinct_values: HashMap::new(),
    };

    for o in col.offsets().windows(2) {
        let mut values = col.data().clone();
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

fn choose_compressor(
    _value: &BinaryColumn,
    stats: &BinaryStats,
    write_options: &WriteOptions,
) -> BinaryCompressor {
    #[cfg(debug_assertions)]
    {
        if crate::util::env::check_freq_env()
            && !write_options
                .forbidden_compressions
                .contains(&Compression::Freq)
        {
            return BinaryCompressor::Extend(Box::new(Freq {}));
        }
        if crate::util::env::check_dict_env()
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

        let compressors: Vec<Box<dyn BinaryCompression>> = vec![
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
