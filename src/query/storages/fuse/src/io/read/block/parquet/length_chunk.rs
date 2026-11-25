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

//! Reads the Parquet column chunk of a `String` column whose values a scan only needs the byte
//! lengths of (see `BlockReader::length_only_columns`). The values are neither copied nor
//! validated as UTF-8: each view has the length of its value, and all point to the same buffer of
//! zero bytes, as long as the longest value. The column holds strings of NUL bytes of the lengths
//! of the values; only functions of the lengths, such as `octet_length`, may read it.
//!
//! Two page layouts are read:
//!
//! * `PLAIN` pages hold `[length][bytes]` per value, so a value is found through the lengths of
//!   the values before it: the whole page has to be decompressed.
//! * `DELTA_LENGTH_BYTE_ARRAY` and `DELTA_BYTE_ARRAY` pages hold the lengths of all the values
//!   first, as a `DELTA_BINARY_PACKED` stream, then the bytes of the values. The lengths are a
//!   couple of percent of the page, so only the start of the page is decompressed (`Payload`),
//!   and the bytes of the values are never looked at. The pages are walked by their headers
//!   (see [`page_header`]), so the bytes of a page that is skipped are never decompressed either.

use std::io::BufReader;
use std::io::Cursor;
use std::io::Read;
use std::ops::Range;
use std::sync::Arc;

use arrow_array::StringViewArray;
use arrow_buffer::Buffer as ArrowBuffer;
use arrow_buffer::NullBuffer;
use arrow_buffer::ScalarBuffer;
use bytes::Bytes;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::Column;
use databend_common_expression::types::Bitmap;
use databend_common_expression::types::DataType;
use opendal::Buffer;
use parquet::basic::Compression as ParquetCompression;

use super::dict_chunk::decode_hybrid;
use super::page_header;
use super::page_header::PageHeader;

/// How much of a payload is decompressed before the bytes asked for are known to be in it.
const INITIAL_WINDOW: usize = 8 << 10;

fn error(msg: &str) -> ErrorCode {
    ErrorCode::StorageOther(format!("invalid string column chunk: {msg}"))
}

/// The lengths of the values of the column chunk `chunk` of a `String` column (`nullable` for
/// `Nullable(String)`) with `num_rows` rows, as described in the module documentation.
///
/// `None` if the chunk cannot be read this way, which the arrow reader reads instead: a codec
/// that is not decompressed here, an encoding that is not read, or page headers that do not
/// fill the chunk.
pub fn read_string_lengths_chunk(
    chunk: Buffer,
    compression: ParquetCompression,
    num_rows: usize,
    nullable: bool,
) -> Result<Option<StringLengths>> {
    let Some(codec) = Codec::of(compression) else {
        return Ok(None);
    };
    let data = flatten(&chunk);
    let Some(pages) = page_header::pages(std::slice::from_ref(&data)) else {
        return Ok(None);
    };
    let mut lengths = Lengths::new(num_rows, nullable);
    for page in &pages {
        let header = &page.header;
        if data.get(page.data.clone()).is_none() {
            return Ok(None);
        }
        let payload = data.slice(page.data.clone());
        let read = match header.page_type {
            // Index pages carry no values.
            page_header::INDEX_PAGE => true,
            page_header::DICTIONARY_PAGE => lengths.read_dictionary_page(codec, payload, header)?,
            page_header::DATA_PAGE | page_header::DATA_PAGE_V2 => {
                lengths.read_data_page(codec, payload, header)?
            }
            _ => false,
        };
        if !read {
            return Ok(None);
        }
    }
    Ok(Some(lengths.finish(num_rows, nullable)?))
}

/// The chunk as one piece of bytes, which the pages are told apart in.
fn flatten(chunk: &Buffer) -> Bytes {
    match page_header::chunk_parts(chunk).as_slice() {
        [data] => data.clone(),
        parts => Bytes::from(parts.concat()),
    }
}

/// The codecs a payload is decompressed from here.
#[derive(Clone, Copy)]
enum Codec {
    /// No compression.
    None,
    /// The LZ4 block format, `LZ4_RAW` in Parquet.
    Lz4,
    /// Zstandard frames.
    Zstd,
}

impl Codec {
    fn of(compression: ParquetCompression) -> Option<Codec> {
        match compression {
            ParquetCompression::UNCOMPRESSED => Some(Codec::None),
            ParquetCompression::LZ4_RAW => Some(Codec::Lz4),
            ParquetCompression::ZSTD(_) => Some(Codec::Zstd),
            _ => None,
        }
    }
}

/// The payload of a page of `len` bytes, decompressed from its start on demand.
enum Payload {
    Plain(Bytes),
    Lz4(Lz4),
    Zstd(Zstd),
}

impl Payload {
    /// The payload `data`, which is of `len` bytes once decompressed, compressed unless
    /// `compressed` or the codec does not compress.
    fn new(codec: Codec, data: Bytes, len: usize, compressed: bool) -> Result<Payload> {
        Ok(match codec {
            _ if !compressed => Payload::Plain(data),
            Codec::None => Payload::Plain(data),
            Codec::Lz4 => Payload::Lz4(Lz4::new(data, len)),
            Codec::Zstd => Payload::Zstd(Zstd::new(data, len)?),
        })
    }

    /// The first `n` bytes of the payload, or all of it if it is shorter.
    fn ensure(&mut self, n: usize) -> Result<&[u8]> {
        match self {
            Payload::Plain(data) => Ok(&data[..n.min(data.len())]),
            Payload::Lz4(lz4) => lz4.ensure(n),
            Payload::Zstd(zstd) => zstd.ensure(n),
        }
    }
}

/// How much of a payload to decompress to serve `n` bytes, the payload being `len` bytes.
fn window(produced: usize, n: usize, len: usize) -> usize {
    n.max(produced.saturating_mul(2))
        .max(INITIAL_WINDOW)
        .min(len)
}

/// An LZ4 block, decompressed from its start on demand. The format is a sequence of literals
/// copied from the input and matches copied from the output so far, which is why the output of
/// the block is decompressed in order, and why a prefix of it is a prefix of the block.
struct Lz4 {
    input: Bytes,
    /// The offset in `input` of the next token.
    ip: usize,
    out: Vec<u8>,
    /// The length of the decompressed block.
    len: usize,
    /// Whether the last token has been read.
    done: bool,
}

impl Lz4 {
    fn new(input: Bytes, len: usize) -> Lz4 {
        Lz4 {
            input,
            ip: 0,
            out: Vec::new(),
            len,
            done: false,
        }
    }

    fn ensure(&mut self, n: usize) -> Result<&[u8]> {
        let target = n.min(self.len);
        let goal = window(self.out.len(), target, self.len);
        while self.out.len() < goal && !self.done {
            self.token()?;
        }
        if self.out.len() < target {
            return Err(error("truncated values"));
        }
        Ok(&self.out)
    }

    /// The sum of the bytes of an extended length.
    fn length(&mut self) -> Result<usize> {
        let mut total = 0usize;
        loop {
            let byte = *self
                .input
                .get(self.ip)
                .ok_or_else(|| error("truncated token"))?;
            self.ip += 1;
            total = total
                .checked_add(byte as usize)
                .ok_or_else(|| error("length beyond the page"))?;
            if byte != 0xff {
                return Ok(total);
            }
        }
    }

    /// Reads the next literals and match.
    fn token(&mut self) -> Result<()> {
        let Some(&token) = self.input.get(self.ip) else {
            self.done = true;
            return (self.out.len() == self.len)
                .then_some(())
                .ok_or_else(|| error("short values"));
        };
        self.ip += 1;
        let mut literals = (token >> 4) as usize;
        if literals == 15 {
            literals += self.length()?;
        }
        let end = self
            .ip
            .checked_add(literals)
            .filter(|end| *end <= self.input.len())
            .ok_or_else(|| error("literals beyond the page"))?;
        self.out.extend_from_slice(&self.input[self.ip..end]);
        self.ip = end;
        let Some(&low) = self.input.get(self.ip) else {
            self.done = true;
            return (self.out.len() == self.len)
                .then_some(())
                .ok_or_else(|| error("short values"));
        };
        let high = *self
            .input
            .get(self.ip + 1)
            .ok_or_else(|| error("truncated match"))?;
        let offset = u16::from_le_bytes([low, high]) as usize;
        self.ip += 2;
        let mut matched = (token & 0x0f) as usize;
        if matched == 15 {
            matched += self.length()?;
        }
        // The minimum match length is not encoded.
        let matched = matched + 4;
        let from = self
            .out
            .len()
            .checked_sub(offset)
            .filter(|_| offset > 0)
            .ok_or_else(|| error("match before the start"))?;
        if from + matched <= self.out.len() {
            self.out.extend_from_within(from..from + matched);
        } else {
            // The match repeats bytes it writes: a run.
            for i in 0..matched {
                let byte = self.out[from + i];
                self.out.push(byte);
            }
        }
        if self.out.len() > self.len {
            return Err(error("values beyond the page"));
        }
        Ok(())
    }
}

/// A Zstandard payload, decompressed from its start on demand by a decoder that keeps the
/// state of the frames it has read, so that a longer prefix does not decompress the start again.
struct Zstd {
    decoder: zstd::stream::read::Decoder<'static, BufReader<Cursor<Bytes>>>,
    out: Vec<u8>,
    /// The length of the decompressed payload.
    len: usize,
}

impl Zstd {
    fn new(input: Bytes, len: usize) -> Result<Zstd> {
        let decoder = zstd::stream::read::Decoder::new(Cursor::new(input))?;
        Ok(Zstd {
            decoder,
            out: Vec::new(),
            len,
        })
    }

    fn ensure(&mut self, n: usize) -> Result<&[u8]> {
        let target = n.min(self.len);
        let goal = window(self.out.len(), target, self.len);
        while self.out.len() < goal {
            let produced = self.out.len();
            self.out.resize(goal, 0);
            let read = self.decoder.read(&mut self.out[produced..])?;
            self.out.truncate(produced + read);
            if read == 0 {
                break;
            }
        }
        if self.out.len() < target {
            return Err(error("truncated values"));
        }
        Ok(&self.out)
    }
}

/// The lengths of the rows read so far.
struct Lengths {
    lengths: Vec<u32>,
    /// Whether each row is valid, for a nullable column.
    validity: Option<Vec<bool>>,
    /// The lengths of the values of the dictionary page.
    dictionary: Option<Vec<u32>>,
    /// Buffers reused by the pages.
    keys: Vec<u32>,
    levels: Vec<u8>,
}

impl Lengths {
    fn new(num_rows: usize, nullable: bool) -> Lengths {
        Lengths {
            lengths: Vec::with_capacity(num_rows),
            validity: nullable.then(|| Vec::with_capacity(num_rows)),
            dictionary: None,
            keys: Vec::new(),
            levels: Vec::new(),
        }
    }

    /// Reads the lengths of the values of a dictionary page, which holds them, PLAIN encoded,
    /// after their lengths.
    fn read_dictionary_page(
        &mut self,
        codec: Codec,
        payload: Bytes,
        header: &PageHeader,
    ) -> Result<bool> {
        let Some(num_values) = header.dictionary_len else {
            return Ok(false);
        };
        let mut payload = Payload::new(codec, payload, header.uncompressed_size, true)?;
        let data = payload.ensure(header.uncompressed_size)?;
        let mut dictionary = Vec::with_capacity(num_values);
        plain_lengths(data, num_values, &mut dictionary)?;
        self.dictionary = Some(dictionary);
        Ok(true)
    }

    /// Reads the lengths of the values of a data page. `false` if the page is not read.
    fn read_data_page(
        &mut self,
        codec: Codec,
        payload: Bytes,
        header: &PageHeader,
    ) -> Result<bool> {
        let (Some(encoding), Some(num_values)) = (header.encoding, header.num_values) else {
            return Ok(false);
        };
        let page_start = self.lengths.len();
        // The levels of a V2 page come before its values and are not compressed; those of a V1
        // page are the first bytes of the compressed payload.
        let (levels, values, values_len) = match header.page_type {
            page_header::DATA_PAGE_V2 => {
                if header.rep_levels_len.unwrap_or(0) > 0 || header.def_levels_len.is_none() {
                    return Ok(false);
                }
                let levels_len =
                    header.rep_levels_len.unwrap_or(0) + header.def_levels_len.unwrap();
                if payload.len() < levels_len {
                    return Ok(false);
                }
                let levels = self.validity.is_some().then(|| payload.slice(..levels_len));
                let values_len = header.uncompressed_size.saturating_sub(levels_len);
                (levels, payload.slice(levels_len..), values_len)
            }
            _ => (None, payload, header.uncompressed_size),
        };
        let compressed = header.values_compressed;
        let mut values = Payload::new(codec, values, values_len, compressed)?;
        let start = match levels {
            Some(levels) => {
                self.read_levels(&levels, num_values)?;
                0
            }
            // A nullable column whose levels are compressed with its values.
            None if self.validity.is_none() => 0,
            None if header.def_level_encoding == Some(page_header::RLE) => {
                // The length of the levels, then the levels.
                let len = self.u32_at(&mut values, 0)? as usize;
                let data = values.ensure(4 + len)?;
                let Some(data) = data.get(4..4 + len) else {
                    return Ok(false);
                };
                self.read_levels(data, num_values)?;
                4 + len
            }
            None => return Ok(false),
        };
        // The values of the page are those of its valid rows.
        let valid = match self.validity.as_ref() {
            None => num_values,
            Some(validity) => validity[validity.len() - num_values..]
                .iter()
                .filter(|valid| **valid)
                .count(),
        };
        match encoding {
            page_header::DELTA_LENGTH_BYTE_ARRAY => {
                let (lengths, _) = delta_lengths(&mut values, start, valid)?;
                self.lengths.extend(lengths);
            }
            page_header::DELTA_BYTE_ARRAY => {
                // The lengths of the suffixes of the values follow those of their prefixes.
                let (prefixes, end) = delta_lengths(&mut values, start, valid)?;
                let (suffixes, _) = delta_lengths(&mut values, end, valid)?;
                let lengths = prefixes.iter().zip(&suffixes).map(|(a, b)| a + b);
                self.lengths.extend(lengths);
            }
            page_header::PLAIN => {
                let data = values.ensure(values_len)?;
                let data = data.get(start..).ok_or_else(|| error("short page"))?;
                plain_lengths(data, valid, &mut self.lengths)?;
            }
            page_header::PLAIN_DICTIONARY | page_header::RLE_DICTIONARY => {
                let data = values.ensure(values_len)?;
                let data = data.get(start..).ok_or_else(|| error("short page"))?;
                self.dictionary_lengths(data, valid)?;
            }
            _ => return Ok(false),
        }
        self.spread_nulls(page_start, valid, num_values);
        Ok(true)
    }

    /// Appends the validity of the `num_values` rows whose definition levels are `levels`, and
    /// returns how many of them are valid.
    fn read_levels(&mut self, levels: &[u8], num_values: usize) -> Result<usize> {
        let Some(validity) = self.validity.as_mut() else {
            return Ok(num_values);
        };
        self.levels.clear();
        decode_hybrid(levels, 1, num_values, &mut self.levels)?;
        validity.extend(self.levels.iter().map(|level| *level == 1));
        Ok(self.levels.iter().filter(|level| **level == 1).count())
    }

    /// The little-endian `u32` at `pos` of `payload`.
    fn u32_at(&mut self, payload: &mut Payload, pos: usize) -> Result<u32> {
        let bytes = payload.ensure(pos + 4)?;
        let bytes = bytes
            .get(pos..pos + 4)
            .ok_or_else(|| error("truncated levels"))?;
        Ok(u32::from_le_bytes(bytes.try_into().unwrap()))
    }

    /// Appends the lengths of the `num_values` dictionary codes of `data`.
    fn dictionary_lengths(&mut self, data: &[u8], num_values: usize) -> Result<()> {
        let dictionary = self
            .dictionary
            .as_ref()
            .ok_or_else(|| error("data page before the dictionary page"))?;
        let (&bit_width, runs) = data.split_first().ok_or_else(|| error("empty data page"))?;
        if bit_width > 32 {
            return Err(error("key bit width beyond 32"));
        }
        self.keys.clear();
        decode_hybrid(runs, bit_width, num_values, &mut self.keys)?;
        for key in &self.keys {
            let length = dictionary
                .get(*key as usize)
                .ok_or_else(|| error("key beyond the dictionary"))?;
            self.lengths.push(*length);
        }
        Ok(())
    }

    /// The null rows have no value: spreads the lengths of the values of the last page, which
    /// started at `start` with `valid` values, over its `num_values` rows.
    fn spread_nulls(&mut self, start: usize, valid: usize, num_values: usize) {
        let Some(validity) = self.validity.as_ref().filter(|_| valid < num_values) else {
            return;
        };
        let rows = &validity[validity.len() - num_values..];
        self.lengths.resize(start + num_values, 0);
        let mut value = start + valid;
        for row in (0..num_values).rev() {
            if rows[row] {
                value -= 1;
                let length = self.lengths[value];
                self.lengths[start + row] = length;
            } else {
                self.lengths[start + row] = 0;
            }
        }
    }

    fn finish(self, num_rows: usize, nullable: bool) -> Result<StringLengths> {
        if self.lengths.len() != num_rows {
            return Err(error("row count mismatch"));
        }
        let max_len = self.lengths.iter().copied().max().unwrap_or(0);
        Ok(StringLengths {
            lengths: self.lengths,
            nulls: self.validity.map(NullBuffer::from),
            zeros: (max_len > 12).then(|| ArrowBuffer::from(vec![0u8; max_len as usize])),
            nullable,
        })
    }
}

/// The lengths of the values of a column chunk, see [`read_string_lengths_chunk`].
pub struct StringLengths {
    lengths: Vec<u32>,
    nulls: Option<NullBuffer>,
    /// As long as the longest value, if it is not inline in its view.
    zeros: Option<ArrowBuffer>,
    nullable: bool,
}

impl StringLengths {
    /// The lengths of the rows `selection` selects.
    pub fn filter(self, selection: &Bitmap) -> StringLengths {
        let lengths = self
            .lengths
            .iter()
            .zip(selection.iter())
            .filter_map(|(len, selected)| selected.then_some(*len))
            .collect();
        let nulls = self.nulls.map(|nulls| {
            NullBuffer::from_iter(
                nulls
                    .iter()
                    .zip(selection.iter())
                    .filter_map(|(valid, selected)| selected.then_some(valid)),
            )
        });
        StringLengths {
            lengths,
            nulls,
            ..self
        }
    }

    /// The column of the rows `rows`. The views are built for the rows asked for, so that a
    /// column read in batches is built in batches too.
    pub fn column(&self, rows: Range<usize>) -> Result<Column> {
        // A view of at most 12 bytes holds its value: zeros; a longer one points to the start of
        // the zero buffer, with a prefix of zeros.
        let views = self.lengths[rows.clone()]
            .iter()
            .map(|len| *len as u128)
            .collect::<Vec<_>>();
        let nulls = self
            .nulls
            .as_ref()
            .map(|nulls| nulls.slice(rows.start, rows.len()));
        let buffers = self.zeros.iter().cloned().collect::<Vec<_>>();
        // SAFETY: the views point into the zero buffer, and zero bytes are valid UTF-8.
        let array =
            unsafe { StringViewArray::new_unchecked(ScalarBuffer::from(views), buffers, nulls) };
        let data_type = if self.nullable {
            DataType::String.wrap_nullable()
        } else {
            DataType::String
        };
        Column::from_arrow_rs(Arc::new(array), &data_type)
    }
}

/// Appends the lengths of the `num_values` PLAIN encoded values of `data`, each after its length.
fn plain_lengths(data: &[u8], num_values: usize, lengths: &mut Vec<u32>) -> Result<()> {
    // How far ahead the values are fetched: each length is at the end of the previous value, so
    // the loads of the lengths depend on each other, and the page was just decompressed out of
    // the caches.
    const PREFETCH_DISTANCE: usize = 1024;
    lengths.reserve(num_values);
    let mut pos = 0;
    for _ in 0..num_values {
        let start = pos + 4;
        if start > data.len() {
            return Err(error("truncated values"));
        }
        prefetch(data.as_ptr().wrapping_add(pos + PREFETCH_DISTANCE));
        let len = u32::from_le_bytes(data[pos..start].try_into().unwrap());
        pos = start + len as usize;
        lengths.push(len);
    }
    if pos > data.len() {
        return Err(error("truncated values"));
    }
    Ok(())
}

/// The `num` lengths of the `DELTA_BINARY_PACKED` stream that starts at `start` of `payload`,
/// and the offset just past the stream.
///
/// The stream is a header, then blocks of at most `block_size` deltas: the smallest delta of
/// the block, the bit widths of its mini blocks, then the deltas. Only the bytes of the values
/// asked for are decompressed, and the bytes after the last delta are not, so that the stream of
/// a page is read without decompressing the bytes of its values.
fn delta_lengths(payload: &mut Payload, start: usize, num: usize) -> Result<(Vec<u32>, usize)> {
    let mut pos = start;
    let block_size = vlq(payload, &mut pos)? as usize;
    let mini_blocks = vlq(payload, &mut pos)? as usize;
    let total = vlq(payload, &mut pos)? as usize;
    let first = zigzag(payload, &mut pos)?;
    if total != num {
        return Err(error("length count mismatch"));
    }
    if block_size == 0
        || mini_blocks == 0
        || !block_size.is_multiple_of(128)
        || !block_size.is_multiple_of(mini_blocks)
    {
        return Err(error("invalid delta block"));
    }
    if num == 0 {
        return Ok((Vec::new(), pos));
    }
    let mini_size = block_size / mini_blocks;
    let mut lengths = Vec::with_capacity(num);
    let mut value = first;
    lengths.push(u32::try_from(value).map_err(|_| error("length beyond 32 bits"))?);
    while lengths.len() < num {
        let min_delta = zigzag(payload, &mut pos)?;
        let widths = payload.ensure(pos + mini_blocks)?;
        let widths = widths
            .get(pos..pos + mini_blocks)
            .ok_or_else(|| error("truncated delta block"))?
            .to_vec();
        pos += mini_blocks;
        let in_block = (num - lengths.len()).min(block_size);
        for (i, width) in widths.iter().enumerate() {
            let width = *width as usize;
            // A mini block is padded to its size, and unneeded ones take no room.
            let encoded = mini_size * width / 8;
            if i * mini_size >= in_block {
                pos += encoded;
                continue;
            }
            let count = (in_block - i * mini_size).min(mini_size);
            let used = (count * width).div_ceil(8);
            let bytes = payload.ensure(pos + used)?;
            let bytes = bytes
                .get(pos..pos + used)
                .ok_or_else(|| error("truncated mini block"))?;
            unpack(bytes, width, count, min_delta, &mut value, &mut lengths)?;
            pos += encoded;
        }
    }
    Ok((lengths, pos))
}

/// Appends the `count` deltas of `width` bits of `data`, each added to `min_delta` and to
/// `value`, the value before the block.
fn unpack(
    data: &[u8],
    width: usize,
    count: usize,
    min_delta: i64,
    value: &mut i64,
    lengths: &mut Vec<u32>,
) -> Result<()> {
    if width > 32 {
        return Err(error("delta bit width beyond 32"));
    }
    let mask = (1u64 << width).wrapping_sub(1);
    let mut bit = 0;
    for _ in 0..count {
        let byte = bit / 8;
        let shift = bit % 8;
        let mut acc = 0u64;
        for k in 0..(shift + width).div_ceil(8) {
            let b = *data
                .get(byte + k)
                .ok_or_else(|| error("truncated mini block"))?;
            acc |= (b as u64) << (k * 8);
        }
        let delta = (acc >> shift) & mask;
        *value = value
            .checked_add(min_delta)
            .and_then(|value| value.checked_add(delta as i64))
            .ok_or_else(|| error("length beyond 64 bits"))?;
        lengths.push(u32::try_from(*value).map_err(|_| error("length beyond 32 bits"))?);
        bit += width;
    }
    Ok(())
}

/// The u64 that ends the variable-length integer at `pos` of `payload`.
fn vlq(payload: &mut Payload, pos: &mut usize) -> Result<u64> {
    let mut result = 0u64;
    for shift in (0..64).step_by(7) {
        let bytes = payload.ensure(*pos + 1)?;
        let byte = *bytes.get(*pos).ok_or_else(|| error("truncated varint"))?;
        *pos += 1;
        result |= ((byte & 0x7f) as u64) << shift;
        if byte & 0x80 == 0 {
            return Ok(result);
        }
    }
    Err(error("varint beyond 64 bits"))
}

/// The i64 that ends the zigzag variable-length integer at `pos` of `payload`.
fn zigzag(payload: &mut Payload, pos: &mut usize) -> Result<i64> {
    let v = vlq(payload, pos)?;
    Ok((v >> 1) as i64 ^ -((v & 1) as i64))
}

/// Hint to fetch the line holding `ptr` into the caches. Never faults, even on invalid
/// addresses.
#[inline(always)]
fn prefetch(ptr: *const u8) {
    #[cfg(target_arch = "x86_64")]
    {
        use std::arch::x86_64::_MM_HINT_T0;
        use std::arch::x86_64::_mm_prefetch;

        // SAFETY: `_mm_prefetch` is a non-faulting hint for any address.
        unsafe { _mm_prefetch::<_MM_HINT_T0>(ptr.cast::<i8>()) }
    }
    #[cfg(not(target_arch = "x86_64"))]
    let _ = ptr;
}

#[cfg(test)]
mod tests {
    use arrow_array::ArrayRef;
    use arrow_array::RecordBatch;
    use arrow_array::StringArray;
    use arrow_schema::DataType as ArrowDataType;
    use arrow_schema::Field;
    use arrow_schema::Schema;
    use databend_common_expression::ScalarRef;
    use parquet::arrow::ArrowWriter;
    use parquet::basic::Compression;
    use parquet::basic::Encoding;
    use parquet::basic::ZstdLevel;
    use parquet::file::properties::WriterProperties;
    use parquet::file::properties::WriterVersion;
    use parquet::file::reader::FileReader;
    use parquet::file::serialized_reader::SerializedFileReader;
    use parquet::schema::types::ColumnPath;

    use super::*;

    /// The column chunk of a one-column Parquet file of `values`, written as fuse writes blocks:
    /// with `dictionary` and a dictionary page limit of `dictionary_limit` bytes, beyond which
    /// the writer falls back to `encoding` pages.
    fn column_chunk(
        values: &[Option<String>],
        nullable: bool,
        version: WriterVersion,
        compression: Compression,
        dictionary: bool,
        dictionary_limit: usize,
        encoding: Encoding,
    ) -> Buffer {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "s",
            ArrowDataType::Utf8,
            nullable,
        )]));
        let array: ArrayRef = Arc::new(StringArray::from(values.to_vec()));
        let batch = RecordBatch::try_new(schema.clone(), vec![array]).unwrap();
        let props = WriterProperties::builder()
            .set_writer_version(version)
            .set_compression(compression)
            .set_encoding(Encoding::PLAIN)
            .set_column_encoding(ColumnPath::new(vec!["s".to_string()]), encoding)
            .set_data_page_row_count_limit(300)
            .set_write_batch_size(300)
            .set_dictionary_enabled(dictionary)
            .set_dictionary_page_size_limit(dictionary_limit)
            .build();
        let mut file = Vec::new();
        let mut writer = ArrowWriter::try_new(&mut file, schema, Some(props)).unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();
        let file = Bytes::from(file);
        let reader = SerializedFileReader::new(file.clone()).unwrap();
        let (start, len) = reader.metadata().row_group(0).column(0).byte_range();
        Buffer::from(file.slice(start as usize..(start + len) as usize))
    }

    fn check(values: Vec<Option<String>>) {
        let nullable = values.iter().any(Option::is_none);
        let compressions = [
            Compression::UNCOMPRESSED,
            Compression::LZ4_RAW,
            Compression::ZSTD(ZstdLevel::default()),
        ];
        for version in [WriterVersion::PARQUET_1_0, WriterVersion::PARQUET_2_0] {
            for compression in compressions {
                for encoding in [
                    Encoding::PLAIN,
                    Encoding::DELTA_LENGTH_BYTE_ARRAY,
                    Encoding::DELTA_BYTE_ARRAY,
                ] {
                    // The pages of the encoding, and dictionary pages then pages of it.
                    for (dictionary, limit) in [(false, 1 << 20), (true, 1 << 20), (true, 256)] {
                        let chunk = column_chunk(
                            &values,
                            nullable,
                            version,
                            compression,
                            dictionary,
                            limit,
                            encoding,
                        );
                        let lengths =
                            read_string_lengths_chunk(chunk, compression, values.len(), nullable)
                                .unwrap()
                                .unwrap();
                        let column = lengths.column(0..values.len()).unwrap();
                        // Or some of the rows.
                        let some = lengths.column(7..values.len() - 3).unwrap();
                        assert_eq!(some, column.slice(7..values.len() - 3));
                        let selection = Bitmap::from_iter((0..values.len()).map(|i| i % 3 != 1));
                        let selected = lengths.filter(&selection);
                        assert_eq!(
                            selected.column(0..selection.true_count()).unwrap(),
                            column.filter(&selection)
                        );
                        let context = format!(
                            "{version:?} {compression:?} {dictionary} {limit} {encoding:?}"
                        );
                        assert_eq!(column.len(), values.len(), "{context}");
                        for (row, value) in values.iter().enumerate() {
                            match (value, column.index(row).unwrap()) {
                                (None, ScalarRef::Null) => {}
                                (Some(value), ScalarRef::String(read)) => {
                                    assert_eq!(read.len(), value.len(), "{context} row {row}");
                                    assert!(read.bytes().all(|b| b == 0), "{context} row {row}");
                                }
                                (value, read) => panic!("{context} row {row}: {value:?} {read:?}"),
                            }
                        }
                    }
                }
            }
        }
    }

    #[test]
    fn test_read_string_lengths_chunk() {
        let value = |i: usize| match i % 4 {
            0 => format!("{i}"),
            1 => format!("https://example.com/{i}/{}", "path/".repeat(i % 50)),
            2 => format!("数据库-{i}-{}", "é".repeat(i % 100)),
            _ => String::new(),
        };
        // Few distinct values, then many: the dictionary of the latter outgrows small limits.
        check((0..2000).map(|i| Some(value(i % 40))).collect());
        check((0..3000).map(|i| Some(value(i))).collect());
        // Null rows, also in runs and in whole pages.
        check(
            (0..3000)
                .map(|i| (i % 7 != 0 && !(900..1300).contains(&i)).then(|| value(i % 300)))
                .collect(),
        );
        // Values of at most 12 bytes are inline in the views: no buffer.
        check((0..1000).map(|i| Some(format!("{}", i % 50))).collect());
    }

    #[test]
    fn test_read_invalid_string_lengths_chunk() {
        let values: Vec<_> = (0..1000).map(|i| Some(format!("value-{i}"))).collect();
        let chunk = column_chunk(
            &values,
            false,
            WriterVersion::PARQUET_2_0,
            Compression::UNCOMPRESSED,
            false,
            1 << 20,
            Encoding::DELTA_LENGTH_BYTE_ARRAY,
        );
        // A row count that does not match the pages.
        assert!(
            read_string_lengths_chunk(chunk.clone(), Compression::UNCOMPRESSED, 999, false)
                .is_err()
        );
        // Truncated pages are left to the arrow reader.
        let truncated = chunk.slice(..chunk.len() - 10);
        assert!(
            read_string_lengths_chunk(truncated, Compression::UNCOMPRESSED, 1000, false)
                .unwrap()
                .is_none()
        );
    }

    #[test]
    fn test_unread_compression() {
        let values: Vec<_> = (0..1000).map(|i| Some(format!("value-{i}"))).collect();
        let chunk = column_chunk(
            &values,
            false,
            WriterVersion::PARQUET_2_0,
            Compression::SNAPPY,
            false,
            1 << 20,
            Encoding::DELTA_LENGTH_BYTE_ARRAY,
        );
        assert!(
            read_string_lengths_chunk(chunk, Compression::SNAPPY, 1000, false)
                .unwrap()
                .is_none()
        );
    }
}
