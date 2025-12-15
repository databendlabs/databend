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

mod one_value;
mod rle;

use databend_common_column::bitmap::MutableBitmap;
use databend_common_expression::types::Bitmap;
use rand::Rng;
use rand::thread_rng;

use super::Compression;
use super::basic::CommonCompression;
use super::integer::OneValue;
use super::integer::Rle;
use crate::error::Error;
use crate::error::Result;
use crate::read::NativeReadBuf;
use crate::read::read_basic::read_compress_header;
use crate::write::WriteOptions;

pub fn compress_boolean(
    col: &Bitmap,
    validity: Option<Bitmap>,
    buf: &mut Vec<u8>,
    write_options: &WriteOptions,
) -> Result<()> {
    // choose compressor
    let stats = gen_stats(col, validity.clone());
    let compressor = choose_compressor(col, &stats, write_options);

    log::debug!(
        "choose boolean compression : {:?}",
        compressor.to_compression()
    );

    let codec = compressor.to_compression() as u8;
    buf.extend_from_slice(&codec.to_le_bytes());
    let pos = buf.len();
    buf.extend_from_slice(&[0u8; 8]);

    let compressed_size = match compressor {
        BooleanCompressor::Basic(c) => {
            let (_, slice_offset, _) = col.as_slice();

            let bitmap = if slice_offset != 0 {
                // case where we can't slice the bitmap as the offsets are not multiple of 8
                Bitmap::from_trusted_len_iter(col.iter())
            } else {
                col.clone()
            };

            let (slice, _, _) = bitmap.as_slice();
            c.compress(slice, buf)
        }
        BooleanCompressor::Extend(c) => c.compress(col, validity, buf),
    }?;
    buf[pos..pos + 4].copy_from_slice(&(compressed_size as u32).to_le_bytes());
    buf[pos + 4..pos + 8].copy_from_slice(&(col.len() as u32).to_le_bytes());
    Ok(())
}

pub fn decompress_boolean<R: NativeReadBuf>(
    reader: &mut R,
    length: usize,
    output: &mut MutableBitmap,
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

    let compressor = BooleanCompressor::from_compression(compression)?;
    match compressor {
        BooleanCompressor::Basic(c) => {
            let bytes = length.div_ceil(8);
            let mut buffer = vec![0u8; bytes];
            c.decompress(&input[..compressed_size], &mut buffer)?;
            output.extend_from_slice(buffer.as_slice(), 0, length);
        }
        BooleanCompressor::Extend(c) => {
            c.decompress(input, length, output)?;
        }
    }

    if use_inner {
        reader.consume(compressed_size);
    }
    Ok(())
}

pub trait BooleanCompression {
    fn compress(
        &self,
        col: &Bitmap,
        validity: Option<Bitmap>,
        output: &mut Vec<u8>,
    ) -> Result<usize>;
    fn decompress(&self, input: &[u8], length: usize, output: &mut MutableBitmap) -> Result<()>;
    fn to_compression(&self) -> Compression;

    fn compress_ratio(&self, stats: &BooleanStats) -> f64;
}

enum BooleanCompressor {
    Basic(CommonCompression),
    Extend(Box<dyn BooleanCompression>),
}

impl BooleanCompressor {
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
            Compression::Rle => Ok(Self::Extend(Box::new(Rle {}))),
            other => Err(Error::OutOfSpec(format!(
                "Unknown compression codec {other:?}",
            ))),
        }
    }
}

#[allow(dead_code)]
#[derive(Debug, Clone)]
pub struct BooleanStats {
    pub src: Bitmap,
    pub total_bytes: usize,
    pub rows: usize,
    pub validity: Option<Bitmap>,
    pub null_count: usize,
    pub false_count: usize,
    pub true_count: usize,
    pub average_run_length: f64,
}

fn gen_stats(col: &Bitmap, validity: Option<Bitmap>) -> BooleanStats {
    let mut null_count = 0;
    let mut false_count = 0;
    let mut true_count = 0;

    let mut is_init_value_initialized = false;
    let mut last_value = false;
    let mut run_count = 0;

    for v in col.option_iter(validity.as_ref()) {
        if !is_init_value_initialized {
            is_init_value_initialized = true;
            last_value = v.unwrap_or_default();
        }

        match v {
            Some(v) => {
                if v {
                    true_count += 1;
                } else {
                    false_count += 1;
                }

                if last_value != v {
                    run_count += 1;
                    last_value = v;
                }
            }
            None => null_count += 1,
        }
    }

    BooleanStats {
        src: col.clone(),
        rows: col.len(),
        total_bytes: col.len() / 8,
        validity,
        null_count,
        false_count,
        true_count,
        average_run_length: col.len() as f64 / 8.0f64 / run_count as f64,
    }
}

fn choose_compressor(
    _col: &Bitmap,
    stats: &BooleanStats,
    write_options: &WriteOptions,
) -> BooleanCompressor {
    #[cfg(debug_assertions)]
    {
        if crate::util::env::check_rle_env()
            && !write_options
                .forbidden_compressions
                .contains(&Compression::Rle)
        {
            return BooleanCompressor::Extend(Box::new(Rle {}));
        }
    }
    let basic = BooleanCompressor::Basic(write_options.default_compression);
    if let Some(ratio) = write_options.default_compress_ratio {
        let mut max_ratio = ratio;
        let mut result = basic;

        let compressors: Vec<Box<dyn BooleanCompression>> =
            vec![Box::new(OneValue {}) as _, Box::new(Rle {}) as _];

        for c in compressors {
            if write_options
                .forbidden_compressions
                .contains(&c.to_compression())
            {
                continue;
            }

            let r = c.compress_ratio(stats);
            if r > max_ratio {
                max_ratio = r;
                result = BooleanCompressor::Extend(c);

                if r == stats.rows as f64 {
                    break;
                }
            }
        }
        result
    } else {
        basic
    }
}

fn compress_sample_ratio<C: BooleanCompression>(
    c: &C,
    stats: &BooleanStats,
    sample_count: usize,
    sample_size: usize,
) -> f64 {
    let mut rng = thread_rng();

    let stats = if stats.src.len() / sample_count <= sample_size {
        stats.clone()
    } else {
        let col = &stats.src;
        let separator = col.len() / sample_count;
        let remainder = col.len() % sample_count;
        let mut builder = MutableBitmap::with_capacity(sample_count * sample_size);

        let mut validity = if stats.null_count > 0 {
            Some(MutableBitmap::with_capacity(sample_count * sample_size))
        } else {
            None
        };

        for sample_i in 0..sample_count {
            let range_end = if sample_i == sample_count - 1 {
                separator + remainder
            } else {
                separator
            } - sample_size;

            let partition_begin = sample_i * separator + rng.gen_range(0..range_end);

            let mut s = col.clone();
            s.slice(partition_begin, sample_size);

            if let (Some(b), Some(validity)) = (&mut validity, &stats.validity) {
                let mut v = validity.clone();
                v.slice(partition_begin, sample_size);
                b.extend_from_trusted_len_iter(v.into_iter());
            }

            builder.extend_from_trusted_len_iter(s.into_iter());
        }
        let sample_col: Bitmap = builder.into();
        gen_stats(&sample_col, validity.map(|x| x.into()))
    };

    let size = c
        .compress(&stats.src, stats.validity.clone(), &mut vec![])
        .unwrap_or(stats.total_bytes);

    stats.total_bytes as f64 / size as f64
}
