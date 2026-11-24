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

use std::ops::Range;
use std::sync::Arc;
use std::sync::LazyLock;

use arrow_array::StringViewArray;
use arrow_buffer::Buffer as ArrowBuffer;
use arrow_buffer::NullBuffer;
use arrow_buffer::ScalarBuffer;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::Column;
use databend_common_expression::types::DataType;
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

use super::dict_chunk::decode_hybrid;

/// A `BYTE_ARRAY` leaf, required or optional: all the page reader needs to know about the column.
fn byte_array_leaf(repetition: Repetition) -> Arc<ColumnDescriptor> {
    let ty = Type::primitive_type_builder("c", PhysicalType::BYTE_ARRAY)
        .with_repetition(repetition)
        .build()
        .unwrap();
    let max_def_level = (repetition == Repetition::OPTIONAL) as i16;
    Arc::new(ColumnDescriptor::new(
        Arc::new(ty),
        max_def_level,
        0,
        ColumnPath::new(vec!["c".to_string()]),
    ))
}

static REQUIRED_BYTE_ARRAY: LazyLock<Arc<ColumnDescriptor>> =
    LazyLock::new(|| byte_array_leaf(Repetition::REQUIRED));
static OPTIONAL_BYTE_ARRAY: LazyLock<Arc<ColumnDescriptor>> =
    LazyLock::new(|| byte_array_leaf(Repetition::OPTIONAL));

fn error(msg: &str) -> ErrorCode {
    ErrorCode::StorageOther(format!("invalid string column chunk: {msg}"))
}

/// The lengths of the values of the column chunk `chunk` of a `String` column (`nullable` for
/// `Nullable(String)`) with `num_rows` rows, as described in the module documentation.
///
/// `None` if a page has an encoding other than `PLAIN` and the dictionary encodings, which the
/// arrow reader reads instead.
pub fn read_string_lengths_chunk(
    chunk: Buffer,
    compression: ParquetCompression,
    num_rows: usize,
    nullable: bool,
) -> Result<Option<StringLengths>> {
    let descriptor = if nullable {
        OPTIONAL_BYTE_ARRAY.clone()
    } else {
        REQUIRED_BYTE_ARRAY.clone()
    };
    let meta = ColumnChunkMetaData::builder(descriptor)
        .set_compression(compression)
        .set_data_page_offset(0)
        .set_total_compressed_size(chunk.len() as i64)
        .build()?;
    let reader = Arc::new(BufferReader(chunk));
    let mut pages = SerializedPageReader::new(reader, &meta, num_rows, None)?;

    let mut lengths = Lengths {
        lengths: Vec::with_capacity(num_rows),
        validity: nullable.then(|| Vec::with_capacity(num_rows)),
        dictionary: None,
        keys: Vec::new(),
        levels: Vec::new(),
    };
    while let Some(page) = pages.get_next_page()? {
        let read = match page {
            Page::DictionaryPage {
                buf, num_values, ..
            } => {
                if lengths.dictionary.is_some() {
                    return Err(error("several dictionary pages"));
                }
                let mut dictionary = Vec::with_capacity(num_values as usize);
                plain_lengths(&buf, num_values as usize, &mut dictionary)?;
                lengths.dictionary = Some(dictionary);
                continue;
            }
            Page::DataPage {
                buf,
                num_values,
                encoding,
                def_level_encoding,
                ..
            } => {
                let mut data = buf.as_ref();
                let levels = match nullable {
                    // The levels, after their length.
                    true if def_level_encoding == Encoding::RLE => {
                        let len = data.get(..4).ok_or_else(|| error("truncated levels"))?;
                        let len = u32::from_le_bytes(len.try_into().unwrap()) as usize;
                        let levels = data
                            .get(4..4 + len)
                            .ok_or_else(|| error("truncated levels"))?;
                        data = &data[4 + len..];
                        Some(levels)
                    }
                    true => return Ok(None),
                    false => None,
                };
                lengths.read_page(levels, data, num_values as usize, encoding)?
            }
            Page::DataPageV2 {
                buf,
                num_values,
                encoding,
                def_levels_byte_len,
                rep_levels_byte_len,
                ..
            } => {
                let data = buf.as_ref();
                let rep = rep_levels_byte_len as usize;
                let def = def_levels_byte_len as usize;
                if rep != 0 || data.len() < def {
                    return Err(error("invalid levels"));
                }
                let levels = nullable.then(|| &data[..def]);
                lengths.read_page(levels, &data[def..], num_values as usize, encoding)?
            }
        };
        if !read {
            return Ok(None);
        }
    }
    lengths.finish(num_rows, nullable).map(Some)
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
    /// Reads the `num_values` rows of a data page: `levels` are its definition levels for a
    /// nullable column, and `data` its values. `false` if the encoding is not read.
    fn read_page(
        &mut self,
        levels: Option<&[u8]>,
        data: &[u8],
        num_values: usize,
        encoding: Encoding,
    ) -> Result<bool> {
        if !matches!(
            encoding,
            Encoding::PLAIN | Encoding::RLE_DICTIONARY | Encoding::PLAIN_DICTIONARY
        ) {
            return Ok(false);
        }
        // The values are those of the valid rows.
        let valid = match (levels, self.validity.as_mut()) {
            (Some(levels), Some(validity)) => {
                self.levels.clear();
                decode_hybrid(levels, 1, num_values, &mut self.levels)?;
                validity.extend(self.levels.iter().map(|level| *level == 1));
                self.levels.iter().filter(|level| **level == 1).count()
            }
            _ => num_values,
        };
        let start = self.lengths.len();
        if encoding == Encoding::PLAIN {
            plain_lengths(data, valid, &mut self.lengths)?;
        } else {
            let dictionary = self
                .dictionary
                .as_ref()
                .ok_or_else(|| error("data page before the dictionary page"))?;
            let (&bit_width, runs) = data.split_first().ok_or_else(|| error("empty data page"))?;
            if bit_width > 32 {
                return Err(error("key bit width beyond 32"));
            }
            self.keys.clear();
            decode_hybrid(runs, bit_width, valid, &mut self.keys)?;
            for key in &self.keys {
                let len = dictionary
                    .get(*key as usize)
                    .ok_or_else(|| error("key beyond the dictionary"))?;
                self.lengths.push(*len);
            }
        }
        // The null rows have no value: spread the values to the valid rows.
        if valid < num_values {
            let validity = self.validity.as_ref().unwrap();
            let rows = &validity[validity.len() - num_values..];
            self.lengths.resize(start + num_values, 0);
            let mut value = start + valid;
            for row in (0..num_values).rev() {
                if rows[row] {
                    value -= 1;
                    self.lengths[start + row] = self.lengths[value];
                } else {
                    self.lengths[start + row] = 0;
                }
            }
        }
        Ok(true)
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
    use bytes::Bytes;
    use databend_common_expression::ScalarRef;
    use parquet::arrow::ArrowWriter;
    use parquet::basic::Compression;
    use parquet::basic::ZstdLevel;
    use parquet::file::properties::WriterProperties;
    use parquet::file::properties::WriterVersion;
    use parquet::file::reader::FileReader;
    use parquet::file::serialized_reader::SerializedFileReader;

    use super::*;

    /// The column chunk of a one-column Parquet file of `values`, written as fuse writes blocks:
    /// with `dictionary` and a dictionary page limit of `dictionary_limit` bytes, beyond which
    /// the writer falls back to PLAIN pages.
    fn column_chunk(
        values: &[Option<String>],
        nullable: bool,
        version: WriterVersion,
        compression: Compression,
        dictionary: bool,
        dictionary_limit: usize,
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
                // PLAIN pages, dictionary pages, and dictionary pages then PLAIN pages.
                for (dictionary, limit) in [(false, 1 << 20), (true, 1 << 20), (true, 256)] {
                    let chunk =
                        column_chunk(&values, nullable, version, compression, dictionary, limit);
                    let lengths =
                        read_string_lengths_chunk(chunk, compression, values.len(), nullable)
                            .unwrap()
                            .unwrap();
                    let column = lengths.column(0..values.len()).unwrap();
                    // Or some of the rows.
                    let some = lengths.column(7..values.len() - 3).unwrap();
                    assert_eq!(some, column.slice(7..values.len() - 3));
                    let context = format!("{version:?} {compression:?} {dictionary} {limit}");
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
        );
        // A row count that does not match the pages.
        assert!(
            read_string_lengths_chunk(chunk.clone(), Compression::UNCOMPRESSED, 999, false)
                .is_err()
        );
        // Truncated pages.
        let truncated = chunk.slice(..chunk.len() - 10);
        assert!(
            read_string_lengths_chunk(truncated, Compression::UNCOMPRESSED, 1000, false).is_err()
        );
    }

    #[test]
    fn test_unread_encoding() {
        let values: Vec<_> = (0..1000).map(|i| Some(format!("value-{i}"))).collect();
        let schema = Arc::new(Schema::new(vec![Field::new(
            "s",
            ArrowDataType::Utf8,
            false,
        )]));
        let array: ArrayRef = Arc::new(StringArray::from(values.clone()));
        let batch = RecordBatch::try_new(schema.clone(), vec![array]).unwrap();
        let props = WriterProperties::builder()
            .set_dictionary_enabled(false)
            .set_encoding(Encoding::DELTA_LENGTH_BYTE_ARRAY)
            .build();
        let mut file = Vec::new();
        let mut writer = ArrowWriter::try_new(&mut file, schema, Some(props)).unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();
        let file = Bytes::from(file);
        let reader = SerializedFileReader::new(file.clone()).unwrap();
        let (start, len) = reader.metadata().row_group(0).column(0).byte_range();
        let chunk = Buffer::from(file.slice(start as usize..(start + len) as usize));
        assert!(
            read_string_lengths_chunk(chunk, Compression::UNCOMPRESSED, 1000, false)
                .unwrap()
                .is_none()
        );
    }
}
