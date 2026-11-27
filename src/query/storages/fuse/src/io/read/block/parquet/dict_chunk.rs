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

//! Reads a dictionary encoded Parquet column chunk of a non-nullable `String` column straight
//! into a dictionary encoded column (see `enable_encoded_columns`): the values are views into
//! the decompressed dictionary page and the keys are decoded from the data pages in their
//! narrowest width. The arrow reader, asked for a `Dictionary(UInt32, Utf8)` array, copies the
//! dictionary into an offset buffer that is then cast to views, which costs more than reading
//! the column as plain views.

use std::sync::Arc;
use std::sync::LazyLock;

use arrow_array::Array;
use arrow_array::StringViewArray;
use arrow_array::builder::make_view;
use arrow_buffer::Buffer as ArrowBuffer;
use arrow_buffer::ScalarBuffer;
use bytes::Bytes;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::Column;
use databend_common_expression::types::DataType;
use databend_common_expression::types::DictColumn;
use databend_common_expression::types::EncodedColumn;
use databend_common_expression::types::NarrowBuffer;
use databend_storages_common_io::BufferReader;
use opendal::Buffer;
use parquet::basic::Compression as ParquetCompression;
use parquet::basic::Encoding;
use parquet::basic::Repetition;
use parquet::basic::Type as PhysicalType;
use parquet::column::page::Page;
use parquet::column::page::PageReader;
use parquet::file::metadata::ColumnChunkMetaData;
use parquet::file::serialized_reader::SerializedPageReader;
use parquet::schema::types::ColumnDescriptor;
use parquet::schema::types::ColumnPath;
use parquet::schema::types::Type;

/// A required `BYTE_ARRAY` leaf: all the page reader needs to know about the column.
static REQUIRED_BYTE_ARRAY: LazyLock<Arc<ColumnDescriptor>> = LazyLock::new(|| {
    let ty = Type::primitive_type_builder("c", PhysicalType::BYTE_ARRAY)
        .with_repetition(Repetition::REQUIRED)
        .build()
        .unwrap();
    Arc::new(ColumnDescriptor::new(
        Arc::new(ty),
        0,
        0,
        ColumnPath::new(vec!["c".to_string()]),
    ))
});

fn error(msg: &str) -> ErrorCode {
    ErrorCode::StorageOther(format!("invalid dictionary encoded column chunk: {msg}"))
}

/// Decodes the column chunk `chunk` of a non-nullable string column with `num_rows` rows,
/// whose pages are all dictionary encoded (see `all_pages_dictionary_encoded`), into a
/// dictionary encoded column.
pub fn read_string_dictionary_chunk(
    chunk: Buffer,
    compression: ParquetCompression,
    num_rows: usize,
) -> Result<Column> {
    let meta = ColumnChunkMetaData::builder(REQUIRED_BYTE_ARRAY.clone())
        .set_compression(compression)
        .set_data_page_offset(0)
        .set_total_compressed_size(chunk.len() as i64)
        .build()?;
    let reader = Arc::new(BufferReader(chunk));
    let mut pages = SerializedPageReader::new(reader, &meta, num_rows, None)?;

    let mut dictionary: Option<(StringViewArray, Keys)> = None;
    while let Some(page) = pages.get_next_page()? {
        let (data, num_values) = match page {
            Page::DictionaryPage {
                buf, num_values, ..
            } => {
                if dictionary.is_some() {
                    return Err(error("several dictionary pages"));
                }
                let values = dictionary_values(buf, num_values as usize)?;
                let keys = Keys::with_capacity(values.len(), num_rows);
                dictionary = Some((values, keys));
                continue;
            }
            Page::DataPage {
                buf,
                num_values,
                encoding,
                ..
            } => {
                check_encoding(encoding)?;
                (buf, num_values as usize)
            }
            Page::DataPageV2 {
                buf,
                num_values,
                encoding,
                def_levels_byte_len,
                rep_levels_byte_len,
                ..
            } => {
                check_encoding(encoding)?;
                let levels = (def_levels_byte_len + rep_levels_byte_len) as usize;
                (buf.slice(levels.min(buf.len())..), num_values as usize)
            }
        };
        let (_, keys) = dictionary
            .as_mut()
            .ok_or_else(|| error("data page before the dictionary page"))?;
        keys.decode(&data, num_values)?;
    }
    let (values, keys) = dictionary.ok_or_else(|| error("no dictionary page"))?;
    let keys = keys.finish(values.len(), num_rows)?;
    let values = Column::from_arrow_rs(Arc::new(values), &DataType::String)?;
    Ok(Column::Encoded(Box::new(EncodedColumn::Dict(
        DictColumn::new(keys, values),
    ))))
}

fn check_encoding(encoding: Encoding) -> Result<()> {
    match encoding {
        Encoding::RLE_DICTIONARY | Encoding::PLAIN_DICTIONARY => Ok(()),
        _ => Err(error("data page is not dictionary encoded")),
    }
}

/// Views of the PLAIN encoded values of a dictionary page, pointing into the page.
fn dictionary_values(page: Bytes, num_values: usize) -> Result<StringViewArray> {
    let data = page.as_ref();
    if data.len() > u32::MAX as usize {
        return Err(error("dictionary page too large"));
    }
    let mut views = Vec::with_capacity(num_values);
    let mut pos = 0;
    // UTF-8 is validated in runs, as by the arrow reader: a length below 128 is encoded as four
    // ASCII bytes, so a run of values with such lengths is validated in one call, the length
    // prefixes included. A longer length ends the run before its prefix.
    let mut run_start = 0;
    for _ in 0..num_values {
        let prefix = data
            .get(pos..pos + 4)
            .ok_or_else(|| error("truncated dictionary page"))?;
        let len = u32::from_le_bytes(prefix.try_into().unwrap()) as usize;
        if len >= 128 {
            validate_utf8(&data[run_start..pos])?;
            run_start = pos + 4;
        }
        pos += 4;
        let value = data
            .get(pos..pos + len)
            .ok_or_else(|| error("truncated dictionary page"))?;
        views.push(make_view(value, 0, pos as u32));
        pos += len;
    }
    validate_utf8(&data[run_start..pos])?;
    let buffer = ArrowBuffer::from(page);
    // SAFETY: the views point into `buffer`, and their values are valid UTF-8.
    Ok(unsafe { StringViewArray::new_unchecked(ScalarBuffer::from(views), vec![buffer], None) })
}

fn validate_utf8(bytes: &[u8]) -> Result<()> {
    simdutf8::basic::from_utf8(bytes)
        .map(|_| ())
        .map_err(|_| error("invalid UTF-8"))
}

/// The keys of the rows, in the narrowest width that indexes the dictionary.
enum Keys {
    U8(Vec<u8>),
    U16(Vec<u16>),
    U32(Vec<u32>),
}

impl Keys {
    fn with_capacity(dictionary_len: usize, num_rows: usize) -> Self {
        match NarrowBuffer::width_for(dictionary_len.saturating_sub(1) as u64) {
            1 => Keys::U8(Vec::with_capacity(num_rows)),
            2 => Keys::U16(Vec::with_capacity(num_rows)),
            _ => Keys::U32(Vec::with_capacity(num_rows)),
        }
    }

    /// Decodes the `num_values` keys of a data page: its bit width, then RLE / bit-packed
    /// hybrid runs.
    fn decode(&mut self, data: &[u8], num_values: usize) -> Result<()> {
        let (&bit_width, runs) = data.split_first().ok_or_else(|| error("empty data page"))?;
        if bit_width > 32 {
            return Err(error("key bit width beyond 32"));
        }
        match self {
            Keys::U8(keys) => decode_hybrid(runs, bit_width, num_values, keys),
            Keys::U16(keys) => decode_hybrid(runs, bit_width, num_values, keys),
            Keys::U32(keys) => decode_hybrid(runs, bit_width, num_values, keys),
        }
    }

    /// Checks the row count, and that all keys index the dictionary: kernels index it without
    /// bounds checks.
    fn finish(self, dictionary_len: usize, num_rows: usize) -> Result<NarrowBuffer> {
        fn check<K: Key>(keys: &[K], dictionary_len: usize, num_rows: usize) -> Result<()> {
            if keys.len() != num_rows {
                return Err(error("row count mismatch"));
            }
            let len = dictionary_len as u32;
            // Without early exit, so it vectorizes.
            if keys
                .iter()
                .fold(false, |out, key| out | (key.widen() >= len))
            {
                return Err(error("key beyond the dictionary"));
            }
            Ok(())
        }
        Ok(match self {
            Keys::U8(keys) => {
                check(&keys, dictionary_len, num_rows)?;
                NarrowBuffer::U8(keys.into())
            }
            Keys::U16(keys) => {
                check(&keys, dictionary_len, num_rows)?;
                NarrowBuffer::U16(keys.into())
            }
            Keys::U32(keys) => {
                check(&keys, dictionary_len, num_rows)?;
                NarrowBuffer::U32(keys.into())
            }
        })
    }
}

trait Key: Copy + Default {
    /// Truncates a decoded key; keys beyond the width are rejected by [`Keys::finish`], as
    /// the dictionary has fewer values than the width can index.
    fn narrow(value: u32) -> Self;
    fn widen(self) -> u32;
}

macro_rules! impl_key {
    ($($ty:ty),*) => {
        $(impl Key for $ty {
            #[inline]
            fn narrow(value: u32) -> Self {
                // Out-of-range keys must not wrap into valid ones.
                <$ty>::try_from(value).unwrap_or(<$ty>::MAX)
            }

            #[inline]
            fn widen(self) -> u32 {
                self as u32
            }
        })*
    };
}

impl_key!(u8, u16, u32);

/// Appends `num_values` values of the RLE / bit-packed hybrid encoding `runs` to `out`.
fn decode_hybrid<K: Key>(
    mut runs: &[u8],
    bit_width: u8,
    num_values: usize,
    out: &mut Vec<K>,
) -> Result<()> {
    let bit_width = bit_width as usize;
    let value_bytes = bit_width.div_ceil(8);
    let end = out.len() + num_values;
    while out.len() < end {
        let (header, len) = uleb128(runs).ok_or_else(|| error("truncated key runs"))?;
        runs = &runs[len..];
        let remaining = end - out.len();
        if header & 1 == 0 {
            // RLE run: a count and the repeated value.
            let count = usize::try_from(header >> 1).map_err(|_| error("key run too long"))?;
            let bytes = runs
                .get(..value_bytes)
                .ok_or_else(|| error("truncated key runs"))?;
            let mut le = [0u8; 4];
            le[..value_bytes].copy_from_slice(bytes);
            runs = &runs[value_bytes..];
            let value = K::narrow(u32::from_le_bytes(le));
            out.resize(out.len() + count.min(remaining), value);
        } else {
            // Bit-packed run: groups of 8 values, the least significant bits first.
            let groups = usize::try_from(header >> 1).map_err(|_| error("key run too long"))?;
            let bytes = groups
                .checked_mul(bit_width)
                .ok_or_else(|| error("key run too long"))?;
            let packed = runs
                .get(..bytes)
                .ok_or_else(|| error("truncated key runs"))?;
            runs = &runs[bytes..];
            let count = (groups * 8).min(remaining);
            unpack(packed, bit_width, count, out);
        }
    }
    Ok(())
}

/// Appends the first `count` values of `bit_width` bits packed in `packed`.
fn unpack<K: Key>(packed: &[u8], bit_width: usize, count: usize, out: &mut Vec<K>) {
    if bit_width == 0 {
        out.resize(out.len() + count, K::narrow(0));
        return;
    }
    let mask = (1u64 << bit_width) - 1;
    out.reserve(count);
    for i in 0..count {
        let bit = i * bit_width;
        let byte = bit / 8;
        // A value spans at most 5 bytes (32 bits at a 7 bit offset).
        let mut word = [0u8; 8];
        let available = (packed.len() - byte).min(8);
        word[..available].copy_from_slice(&packed[byte..byte + available]);
        let value = (u64::from_le_bytes(word) >> (bit % 8)) & mask;
        out.push(K::narrow(value as u32));
    }
}

/// An unsigned LEB128 integer and its length in bytes.
fn uleb128(data: &[u8]) -> Option<(u64, usize)> {
    let mut value = 0u64;
    for (i, b) in data.iter().take(10).enumerate() {
        value |= ((b & 0x7f) as u64) << (7 * i);
        if b & 0x80 == 0 {
            return Some((value, i + 1));
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use arrow_array::ArrayRef;
    use arrow_array::RecordBatch;
    use arrow_array::StringArray;
    use arrow_schema::DataType as ArrowDataType;
    use arrow_schema::Field;
    use arrow_schema::Schema;
    use databend_common_expression::BlockEntry;
    use parquet::arrow::ArrowWriter;
    use parquet::basic::Compression;
    use parquet::basic::ZstdLevel;
    use parquet::file::properties::WriterProperties;
    use parquet::file::properties::WriterVersion;
    use parquet::file::reader::FileReader;
    use parquet::file::serialized_reader::SerializedFileReader;

    use super::*;

    /// The column chunk of a one-column Parquet file of `values`.
    fn column_chunk(values: &[String], version: WriterVersion, compression: Compression) -> Buffer {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "s",
            ArrowDataType::Utf8,
            false,
        )]));
        let array: ArrayRef = Arc::new(StringArray::from(values.to_vec()));
        let batch = RecordBatch::try_new(schema.clone(), vec![array]).unwrap();
        let props = WriterProperties::builder()
            .set_writer_version(version)
            .set_compression(compression)
            .set_data_page_row_count_limit(300)
            .set_write_batch_size(300)
            .set_dictionary_page_size_limit(1 << 24)
            .build();
        let mut file = Vec::new();
        let mut writer = ArrowWriter::try_new(&mut file, schema, Some(props)).unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();
        let file = Bytes::from(file);
        let reader = SerializedFileReader::new(file.clone()).unwrap();
        let (start, len) = reader.metadata().row_group(0).column(0).byte_range();
        let chunk = Buffer::from(file.slice(start as usize..(start + len) as usize));
        assert!(crate::io::read::block::parquet::page_header::all_pages_dictionary_encoded(&chunk));
        chunk
    }

    fn decoded_strings(column: &Column) -> Vec<String> {
        let decoded = BlockEntry::Column(column.clone()).decoded_value();
        let decoded = decoded.as_column().unwrap();
        decoded
            .iter()
            .map(|v| v.as_string().unwrap().to_string())
            .collect()
    }

    fn check(values: Vec<String>, key_width: usize) {
        let compressions = [
            Compression::UNCOMPRESSED,
            Compression::LZ4_RAW,
            Compression::ZSTD(ZstdLevel::default()),
        ];
        for version in [WriterVersion::PARQUET_1_0, WriterVersion::PARQUET_2_0] {
            for compression in compressions {
                let chunk = column_chunk(&values, version, compression);
                let column =
                    read_string_dictionary_chunk(chunk, compression, values.len()).unwrap();
                let Column::Encoded(encoded) = &column else {
                    panic!("expected a dictionary encoded column");
                };
                assert_eq!(encoded.as_dict().unwrap().keys.byte_width(), key_width);
                assert_eq!(
                    decoded_strings(&column),
                    values,
                    "{version:?} {compression:?}"
                );
            }
        }
    }

    #[test]
    fn test_read_string_dictionary_chunk() {
        // Few values, long runs of one value: RLE runs.
        check((0..2000).map(|i| format!("v{}", i / 500)).collect(), 1);
        // Inline (up to 12 bytes) and long values, non-ASCII, and lengths of 128 bytes and more,
        // which split the UTF-8 validation runs.
        let distinct = |i: usize| match i % 4 {
            0 => format!("{i}"),
            1 => format!("https://example.com/{i}/{}", "path/".repeat(i % 50)),
            2 => format!("数据库-{i}-{}", "é".repeat(i % 100)),
            _ => String::new(),
        };
        check((0..3000).map(|i| distinct(i % 200)).collect(), 1);
        // More than 256 values: 2-byte keys, bit-packed runs.
        check((0..3000).map(|i| distinct(i * 7 % 1000)).collect(), 2);
    }

    #[test]
    fn test_read_invalid_dictionary_chunk() {
        let values: Vec<String> = (0..1000).map(|i| format!("value-{}", i % 10)).collect();
        let chunk = column_chunk(
            &values,
            WriterVersion::PARQUET_2_0,
            Compression::UNCOMPRESSED,
        );
        // A row count that does not match the pages.
        assert!(
            read_string_dictionary_chunk(chunk.clone(), Compression::UNCOMPRESSED, 999).is_err()
        );
        // Truncated pages.
        let truncated = chunk.slice(..chunk.len() - 10);
        assert!(read_string_dictionary_chunk(truncated, Compression::UNCOMPRESSED, 1000).is_err());
    }

    #[test]
    fn test_decode_hybrid() {
        // RLE run of 5 values 3, then a bit-packed group of 8 values of 3 bits (0..8), of which
        // only 6 are used.
        let mut runs = vec![5 << 1, 3];
        runs.push((1 << 1) | 1);
        let mut bits = 0u32;
        for v in 0..8u32 {
            bits |= v << (3 * v);
        }
        runs.extend_from_slice(&bits.to_le_bytes()[..3]);
        let mut out: Vec<u8> = Vec::new();
        decode_hybrid(&runs, 3, 11, &mut out).unwrap();
        assert_eq!(out, vec![3, 3, 3, 3, 3, 0, 1, 2, 3, 4, 5]);

        // Truncated runs.
        assert!(decode_hybrid(&runs[..4], 3, 11, &mut Vec::<u8>::new()).is_err());
        // Keys beyond the dictionary are rejected, also when they do not fit the key width.
        let keys = Keys::U8(vec![0, 1, 2]);
        assert!(keys.finish(2, 3).is_err());
        let mut keys = Keys::U8(Vec::new());
        keys.decode(&[9, 2 << 1, 0x00, 0x01], 2).unwrap();
        assert!(keys.finish(200, 2).is_err());
    }

    #[test]
    fn test_invalid_utf8() {
        let mut page = Vec::new();
        for value in [&b"ok"[..], &[0xff, 0xfe][..]] {
            page.extend_from_slice(&(value.len() as u32).to_le_bytes());
            page.extend_from_slice(value);
        }
        assert!(dictionary_values(Bytes::from(page.clone()), 2).is_err());
        assert!(dictionary_values(Bytes::from(page.clone()), 1).is_ok());
        assert!(dictionary_values(Bytes::from(page), 3).is_err());
    }
}
