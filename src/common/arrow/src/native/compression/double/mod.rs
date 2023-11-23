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
mod patas;
mod rle;
mod traits;

use std::collections::HashMap;

use rand::thread_rng;
use rand::Rng;

pub use self::one_value::OneValue;
use self::patas::Patas;
pub use self::traits::DoubleType;
use super::basic::CommonCompression;
use super::integer::Dict;
use super::integer::Freq;
use super::integer::Rle;
use super::is_valid;
use super::Compression;
use crate::arrow::array::Array;
use crate::arrow::array::MutablePrimitiveArray;
use crate::arrow::array::PrimitiveArray;
use crate::arrow::error::Error;
use crate::arrow::error::Result;
use crate::native::read::read_basic::read_compress_header;
use crate::native::read::NativeReadBuf;
use crate::native::write::WriteOptions;

pub fn compress_double<T: DoubleType>(
    array: &PrimitiveArray<T>,
    write_options: WriteOptions,
    buf: &mut Vec<u8>,
) -> Result<()> {
    // choose compressor
    let stats = gen_stats(array);
    let compressor = choose_compressor(array, &stats, &write_options);

    log::debug!(
        "choose double compression : {:?}",
        compressor.to_compression()
    );

    let codec = u8::from(compressor.to_compression());
    buf.extend_from_slice(&codec.to_le_bytes());
    let pos = buf.len();
    buf.extend_from_slice(&[0u8; 8]);

    let compressed_size = match compressor {
        DoubleCompressor::Basic(c) => {
            let input_buf = bytemuck::cast_slice(array.values());
            c.compress(input_buf, buf)
        }
        DoubleCompressor::Extend(c) => c.compress(array, &stats, &write_options, buf),
    }?;
    buf[pos..pos + 4].copy_from_slice(&(compressed_size as u32).to_le_bytes());
    buf[pos + 4..pos + 8]
        .copy_from_slice(&((array.len() * std::mem::size_of::<T>()) as u32).to_le_bytes());

    log::debug!(
        "double compress ratio {}",
        stats.total_bytes as f64 / compressed_size as f64
    );
    Ok(())
}

pub fn decompress_double<T: DoubleType, R: NativeReadBuf>(
    reader: &mut R,
    length: usize,
    output: &mut Vec<T>,
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

    let compressor = DoubleCompressor::<T>::from_compression(compression)?;

    match compressor {
        DoubleCompressor::Basic(c) => {
            output.reserve(length);
            let out_slice = unsafe {
                core::slice::from_raw_parts_mut(
                    output.as_mut_ptr().add(output.len()) as *mut u8,
                    length * std::mem::size_of::<T>(),
                )
            };
            c.decompress(&input[..compressed_size], out_slice)?;
            unsafe { output.set_len(output.len() + length) };
        }
        DoubleCompressor::Extend(c) => {
            c.decompress(input, length, output)?;
        }
    }

    if use_inner {
        reader.consume(compressed_size);
    }
    Ok(())
}

pub trait DoubleCompression<T: DoubleType> {
    fn compress(
        &self,
        array: &PrimitiveArray<T>,
        stats: &DoubleStats<T>,
        write_options: &WriteOptions,
        output: &mut Vec<u8>,
    ) -> Result<usize>;
    fn decompress(&self, input: &[u8], length: usize, output: &mut Vec<T>) -> Result<()>;

    fn to_compression(&self) -> Compression;
    fn compress_ratio(&self, stats: &DoubleStats<T>) -> f64;
}

enum DoubleCompressor<T: DoubleType> {
    Basic(CommonCompression),
    Extend(Box<dyn DoubleCompression<T>>),
}

impl<T: DoubleType> DoubleCompressor<T> {
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
            Compression::Dict => Ok(Self::Extend(Box::new(Dict {}))),
            Compression::Freq => Ok(Self::Extend(Box::new(Freq {}))),
            Compression::Rle => Ok(Self::Extend(Box::new(Rle {}))),
            Compression::Patas => Ok(Self::Extend(Box::new(Patas {}))),

            other => Err(Error::OutOfSpec(format!(
                "Unknown compression codec {other:?}",
            ))),
        }
    }
}

#[derive(Debug, Clone)]
pub struct DoubleStats<T: DoubleType> {
    pub src: PrimitiveArray<T>,
    pub tuple_count: usize,
    pub total_bytes: usize,
    pub null_count: usize,

    pub is_sorted: bool,
    pub min: T::OrderType,
    pub max: T::OrderType,

    pub average_run_length: f64,
    pub distinct_values: HashMap<T::OrderType, usize>,
    pub unique_count: usize,
    pub set_count: usize,
}

fn gen_stats<T: DoubleType>(array: &PrimitiveArray<T>) -> DoubleStats<T> {
    let mut stats = DoubleStats::<T> {
        src: array.clone(),
        tuple_count: array.len(),
        total_bytes: array.len() * std::mem::size_of::<T>(),
        null_count: array.null_count(),
        is_sorted: true,
        min: T::default().as_order(),
        max: T::default().as_order(),
        average_run_length: 0.0,
        distinct_values: HashMap::new(),
        unique_count: 0,
        set_count: array.len() - array.null_count(),
    };

    let mut is_init_value_initialized = false;
    let mut last_value = T::default().as_order();
    let mut run_count = 0;

    let validity = array.validity();
    for (i, current_value) in array.values().iter().cloned().enumerate() {
        let current_value = current_value.as_order();
        if is_valid(&validity, i) {
            if current_value < last_value {
                stats.is_sorted = false;
            }

            if last_value != current_value {
                run_count += 1;
                last_value = current_value;
            }
        }

        if !is_init_value_initialized {
            is_init_value_initialized = true;
            stats.min = current_value;
            stats.max = current_value;
        }

        if current_value > stats.max {
            stats.max = current_value;
        } else if current_value < stats.min {
            stats.min = current_value;
        }

        *stats.distinct_values.entry(current_value).or_insert(0) += 1;
    }
    stats.unique_count = stats.distinct_values.len();
    stats.average_run_length = array.len() as f64 / run_count as f64;

    stats
}

fn choose_compressor<T: DoubleType>(
    _value: &PrimitiveArray<T>,
    stats: &DoubleStats<T>,
    write_options: &WriteOptions,
) -> DoubleCompressor<T> {
    #[cfg(debug_assertions)]
    {
        if crate::native::util::env::check_freq_env()
            && !write_options
                .forbidden_compressions
                .contains(&Compression::Freq)
        {
            return DoubleCompressor::Extend(Box::new(Freq {}));
        }
        if crate::native::util::env::check_dict_env()
            && !write_options
                .forbidden_compressions
                .contains(&Compression::Dict)
        {
            return DoubleCompressor::Extend(Box::new(Dict {}));
        }
        if crate::native::util::env::check_rle_env()
            && !write_options
                .forbidden_compressions
                .contains(&Compression::Rle)
        {
            return DoubleCompressor::Extend(Box::new(Rle {}));
        }
        if crate::native::util::env::check_patas_env()
            && !write_options
                .forbidden_compressions
                .contains(&Compression::Patas)
        {
            return DoubleCompressor::Extend(Box::new(Patas {}));
        }
    }
    let basic = DoubleCompressor::Basic(write_options.default_compression);
    if let Some(ratio) = write_options.default_compress_ratio {
        let mut max_ratio = ratio;
        let mut result = basic;
        let compressors: Vec<Box<dyn DoubleCompression<T>>> = vec![
            Box::new(OneValue {}) as _,
            Box::new(Freq {}) as _,
            Box::new(Dict {}) as _,
            Box::new(Patas {}) as _,
            Box::new(Rle {}) as _,
        ];
        for c in compressors {
            if write_options
                .forbidden_compressions
                .contains(&c.to_compression())
            {
                continue;
            }
            let r = c.compress_ratio(stats);

            log::debug!(
                "compress ratio {:?} : {}, max_ratio: {}",
                c.to_compression(),
                r,
                max_ratio
            );

            if r > max_ratio {
                max_ratio = r;
                result = DoubleCompressor::Extend(c);

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

fn compress_sample_ratio<T: DoubleType, C: DoubleCompression<T>>(
    c: &C,
    stats: &DoubleStats<T>,
    sample_count: usize,
    sample_size: usize,
) -> f64 {
    let mut rng = thread_rng();

    let stats = if stats.src.len() / sample_count <= sample_size {
        stats.clone()
    } else {
        let array = &stats.src;
        let separator = array.len() / sample_count;
        let remainder = array.len() % sample_count;
        let mut builder = MutablePrimitiveArray::with_capacity(sample_count * sample_size);

        for sample_i in 0..sample_count {
            let range_end = if sample_i == sample_count - 1 {
                separator + remainder
            } else {
                separator
            } - sample_size;

            let partition_begin = sample_i * separator + rng.gen_range(0..range_end);

            let mut s = array.clone();
            s.slice(partition_begin, sample_size);
            builder.extend_trusted_len(s.into_iter());
        }
        let sample_array: PrimitiveArray<T> = builder.into();
        gen_stats(&sample_array)
    };

    let size = c
        .compress(&stats.src, &stats, &WriteOptions::default(), &mut vec![])
        .unwrap_or(stats.total_bytes);

    stats.total_bytes as f64 / size as f64
}
