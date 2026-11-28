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

//! Reads the page headers of a Parquet column chunk (Thrift compact protocol), to tell the
//! encoding and the layout of its pages without decompressing them. The parquet crate does not
//! expose page encodings without reading the pages.
//!
//! The header walk works in place: a local file read keeps a chunk in several pieces, and only
//! the headers are needed.

use std::ops::Range;

use bytes::Bytes;
use opendal::Buffer;

/// `PageType` of `parquet.thrift`.
pub const DATA_PAGE: i32 = 0;
pub const INDEX_PAGE: i32 = 1;
pub const DICTIONARY_PAGE: i32 = 2;
pub const DATA_PAGE_V2: i32 = 3;

/// `Encoding` of `parquet.thrift`, for the ones the readers here tell apart.
pub const PLAIN: i32 = 0;
pub const PLAIN_DICTIONARY: i32 = 2;
pub const RLE: i32 = 3;
pub const DELTA_LENGTH_BYTE_ARRAY: i32 = 6;
pub const DELTA_BYTE_ARRAY: i32 = 7;
pub const RLE_DICTIONARY: i32 = 8;

// Thrift compact protocol field types.
const BOOLEAN_TRUE: u8 = 1;
const BOOLEAN_FALSE: u8 = 2;
const BYTE: u8 = 3;
const I16: u8 = 4;
const I32: u8 = 5;
const I64: u8 = 6;
const DOUBLE: u8 = 7;
const BINARY: u8 = 8;
const LIST: u8 = 9;
const SET: u8 = 10;
const MAP: u8 = 11;
const STRUCT: u8 = 12;

/// Whether the column chunk starts with a dictionary page and all its data pages are
/// dictionary encoded, i.e. the writer did not fall back to plain pages once the dictionary
/// grew too large. `false` if a page header cannot be read.
pub fn all_pages_dictionary_encoded(chunk: &Buffer) -> bool {
    dictionary_len(chunk).is_some()
}

/// The number of values of the dictionary of the column chunk, if it is dictionary encoded
/// throughout (see [`all_pages_dictionary_encoded`]). Reads the chunk in place: a local file
/// read keeps a chunk in several pieces, and only the page headers are needed.
pub fn dictionary_len(chunk: &Buffer) -> Option<usize> {
    let pages = pages(&chunk_parts(chunk))?;
    let dictionary = pages.first()?;
    if dictionary.header.page_type != DICTIONARY_PAGE || pages.len() < 2 {
        return None;
    }
    let all_dictionary = pages[1..].iter().all(|page| {
        matches!(page.header.page_type, DATA_PAGE | DATA_PAGE_V2)
            && matches!(
                page.header.encoding,
                Some(PLAIN_DICTIONARY | RLE_DICTIONARY)
            )
    });
    if !all_dictionary {
        return None;
    }
    dictionary.header.dictionary_len
}

/// The pieces of the column chunk `chunk`.
pub fn chunk_parts(chunk: &Buffer) -> Vec<Bytes> {
    chunk.clone().collect()
}

/// The value range in the concatenation of `parts` that starts at `range`.
pub struct Page {
    pub header: PageHeader,
    /// The payload of the page, compressed unless the page is not.
    pub data: Range<usize>,
}

/// The headers of the pages of the column chunk in `parts`, in order. `None` if a header
/// cannot be read, or if the pages do not exactly fill the chunk.
pub fn pages(parts: &[Bytes]) -> Option<Vec<Page>> {
    let mut input = Input {
        parts,
        part: 0,
        pos: 0,
        consumed: 0,
    };
    let mut pages = Vec::new();
    while !input.at_end() {
        let header = PageHeader::read(&mut input)?;
        let start = input.consumed;
        let end = start.checked_add(header.compressed_size)?;
        input.skip(header.compressed_size)?;
        pages.push(Page {
            header,
            data: start..end,
        });
    }
    Some(pages)
}

/// The fields of `parquet.thrift` this crate reads, `None` where the header has none.
#[derive(Clone, Debug)]
pub struct PageHeader {
    pub page_type: i32,
    /// `uncompressed_page_size`: the size of the payload after decompression.
    pub uncompressed_size: usize,
    /// `compressed_page_size`: the size of the payload in the chunk.
    pub compressed_size: usize,
    /// The encoding of the values of a data page.
    pub encoding: Option<i32>,
    /// `num_values` of a data page, nulls and values of a dictionary page included.
    pub num_values: Option<usize>,
    /// `num_values` of a dictionary page.
    pub dictionary_len: Option<usize>,
    /// Of a V1 data page: the encodings of its definition and repetition levels.
    pub def_level_encoding: Option<i32>,
    pub rep_level_encoding: Option<i32>,
    /// Of a V2 data page: the lengths of its levels, which are not compressed.
    pub def_levels_len: Option<usize>,
    pub rep_levels_len: Option<usize>,
    /// Of a V2 data page: whether the values are compressed (`true` when the header omits it).
    pub values_compressed: bool,
}

impl PageHeader {
    fn read(input: &mut Input) -> Option<PageHeader> {
        let mut header = PageHeader {
            page_type: 0,
            uncompressed_size: 0,
            compressed_size: 0,
            encoding: None,
            num_values: None,
            dictionary_len: None,
            def_level_encoding: None,
            rep_level_encoding: None,
            def_levels_len: None,
            rep_levels_len: None,
            values_compressed: true,
        };
        let (mut page_type, mut uncompressed, mut compressed) = (None, None, None);
        input.read_struct(|input, id, ty| {
            match (id, ty) {
                (1, I32) => page_type = Some(i32::try_from(input.zigzag()?).ok()?),
                (2, I32) => uncompressed = usize::try_from(input.zigzag()?).ok(),
                (3, I32) => compressed = usize::try_from(input.zigzag()?).ok(),
                // `DataPageHeader`.
                (5, STRUCT) => input.read_struct(|input, id, ty| {
                    match (id, ty) {
                        (1, I32) => header.num_values = usize::try_from(input.zigzag()?).ok(),
                        (2, I32) => header.encoding = Some(i32::try_from(input.zigzag()?).ok()?),
                        (3, I32) => {
                            header.def_level_encoding = Some(i32::try_from(input.zigzag()?).ok()?)
                        }
                        (4, I32) => {
                            header.rep_level_encoding = Some(i32::try_from(input.zigzag()?).ok()?)
                        }
                        _ => return Some(false),
                    }
                    Some(true)
                })?,
                // `DictionaryPageHeader`.
                (7, STRUCT) => input.read_struct(|input, id, ty| {
                    match (id, ty) {
                        (1, I32) => header.dictionary_len = usize::try_from(input.zigzag()?).ok(),
                        (2, I32) => header.encoding = Some(i32::try_from(input.zigzag()?).ok()?),
                        _ => return Some(false),
                    }
                    Some(true)
                })?,
                // `DataPageHeaderV2`.
                (8, STRUCT) => input.read_struct(|input, id, ty| {
                    match (id, ty) {
                        (1, I32) => header.num_values = usize::try_from(input.zigzag()?).ok(),
                        (4, I32) => header.encoding = Some(i32::try_from(input.zigzag()?).ok()?),
                        (5, I32) => header.def_levels_len = usize::try_from(input.zigzag()?).ok(),
                        (6, I32) => header.rep_levels_len = usize::try_from(input.zigzag()?).ok(),
                        (7, BOOLEAN_TRUE) => header.values_compressed = true,
                        (7, BOOLEAN_FALSE) => header.values_compressed = false,
                        _ => return Some(false),
                    }
                    Some(true)
                })?,
                _ => return Some(false),
            }
            Some(true)
        })?;
        header.page_type = page_type?;
        header.uncompressed_size = uncompressed?;
        header.compressed_size = compressed?;
        Some(header)
    }
}

/// A position in the chunk stored in `parts`.
struct Input<'a> {
    parts: &'a [Bytes],
    part: usize,
    /// Offset in `parts[part]`.
    pos: usize,
    /// The bytes read so far, i.e. the offset in the concatenation of `parts`.
    consumed: usize,
}

impl Input<'_> {
    /// Moves past the ends of the parts, so that `parts[part]` has a byte at `pos` unless
    /// the chunk is exhausted.
    fn settle(&mut self) {
        while self.part < self.parts.len() && self.pos >= self.parts[self.part].len() {
            self.pos -= self.parts[self.part].len();
            self.part += 1;
        }
    }

    fn at_end(&mut self) -> bool {
        self.settle();
        self.part == self.parts.len()
    }

    fn byte(&mut self) -> Option<u8> {
        self.settle();
        let b = *self.parts.get(self.part)?.get(self.pos)?;
        self.pos += 1;
        self.consumed += 1;
        Some(b)
    }

    fn skip(&mut self, n: usize) -> Option<()> {
        self.consumed = self.consumed.checked_add(n)?;
        self.pos = self.pos.checked_add(n)?;
        self.settle();
        // Skipping to the very end is fine, past it is not.
        (self.part < self.parts.len() || self.pos == 0).then_some(())
    }

    fn varint(&mut self) -> Option<u64> {
        let mut result = 0u64;
        for shift in (0..64).step_by(7) {
            let b = self.byte()?;
            result |= ((b & 0x7f) as u64) << shift;
            if b & 0x80 == 0 {
                return Some(result);
            }
        }
        None
    }

    fn zigzag(&mut self) -> Option<i64> {
        let v = self.varint()?;
        Some((v >> 1) as i64 ^ -((v & 1) as i64))
    }

    /// The next field header of a struct: `None` at its end, else (field id, field type).
    fn field(&mut self, last_id: &mut i16) -> Option<Option<(i16, u8)>> {
        let b = self.byte()?;
        let ty = b & 0x0f;
        if ty == 0 {
            return Some(None);
        }
        let delta = (b >> 4) as i16;
        let id = if delta == 0 {
            i16::try_from(self.zigzag()?).ok()?
        } else {
            last_id.checked_add(delta)?
        };
        *last_id = id;
        Some(Some((id, ty)))
    }

    /// Reads a struct, `visit` returning `false` for the fields it does not read.
    fn read_struct(
        &mut self,
        mut visit: impl FnMut(&mut Input<'_>, i16, u8) -> Option<bool>,
    ) -> Option<()> {
        let mut last = 0;
        while let Some((id, ty)) = self.field(&mut last)? {
            if !visit(self, id, ty)? {
                self.skip_value(ty)?;
            }
        }
        Some(())
    }

    fn skip_value(&mut self, ty: u8) -> Option<()> {
        match ty {
            BOOLEAN_TRUE | BOOLEAN_FALSE => Some(()),
            BYTE => self.skip(1),
            I16 | I32 | I64 => self.varint().map(|_| ()),
            DOUBLE => self.skip(8),
            BINARY => {
                let len = usize::try_from(self.varint()?).ok()?;
                self.skip(len)
            }
            LIST | SET => {
                let b = self.byte()?;
                let size = match b >> 4 {
                    15 => usize::try_from(self.varint()?).ok()?,
                    n => n as usize,
                };
                let elem = b & 0x0f;
                for _ in 0..size {
                    // Booleans in a list take one byte each.
                    if elem == BOOLEAN_TRUE || elem == BOOLEAN_FALSE {
                        self.skip(1)?;
                    } else {
                        self.skip_value(elem)?;
                    }
                }
                Some(())
            }
            MAP => {
                let size = usize::try_from(self.varint()?).ok()?;
                if size > 0 {
                    let types = self.byte()?;
                    for _ in 0..size {
                        self.skip_value(types >> 4)?;
                        self.skip_value(types & 0x0f)?;
                    }
                }
                Some(())
            }
            STRUCT => self.read_struct(|_, _, _| Some(false)),
            _ => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow_array::ArrayRef;
    use arrow_array::RecordBatch;
    use arrow_array::StringArray;
    use arrow_schema::DataType as ArrowDataType;
    use arrow_schema::Field;
    use arrow_schema::Schema;
    use parquet::arrow::ArrowWriter;
    use parquet::basic::Compression;
    use parquet::basic::Encoding;
    use parquet::file::properties::WriterProperties;
    use parquet::file::properties::WriterPropertiesBuilder;
    use parquet::file::properties::WriterVersion;
    use parquet::file::reader::FileReader;
    use parquet::file::serialized_reader::SerializedFileReader;
    use parquet::schema::types::ColumnPath;

    use super::*;

    /// The column chunk of a one-column Parquet file of `values`.
    fn column_chunk(values: Vec<String>, props: WriterPropertiesBuilder) -> Bytes {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "s",
            ArrowDataType::Utf8,
            false,
        )]));
        let array: ArrayRef = Arc::new(StringArray::from(values));
        let batch = RecordBatch::try_new(schema.clone(), vec![array]).unwrap();
        let mut file = Vec::new();
        let mut writer = ArrowWriter::try_new(&mut file, schema, Some(props.build())).unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();
        let file = Bytes::from(file);
        let reader = SerializedFileReader::new(file.clone()).unwrap();
        let (start, len) = reader.metadata().row_group(0).column(0).byte_range();
        file.slice(start as usize..(start + len) as usize)
    }

    fn props() -> parquet::file::properties::WriterPropertiesBuilder {
        WriterProperties::builder()
            .set_writer_version(WriterVersion::PARQUET_1_0)
            .set_compression(Compression::LZ4_RAW)
            .set_data_page_row_count_limit(100)
            .set_write_batch_size(100)
    }

    fn values(distinct: usize) -> Vec<String> {
        (0..1000)
            .map(|i| format!("value-{}", i % distinct))
            .collect()
    }

    /// `chunk` as one piece, and split into pieces of `piece` bytes, as a local file read
    /// keeps it.
    fn buffers(chunk: &Bytes) -> [Buffer; 3] {
        let pieces = |piece: usize| {
            Buffer::from(
                (0..chunk.len())
                    .step_by(piece)
                    .map(|i| chunk.slice(i..(i + piece).min(chunk.len())))
                    .collect::<Vec<_>>(),
            )
        };
        [Buffer::from(chunk.clone()), pieces(1), pieces(7)]
    }

    #[test]
    fn test_all_pages_dictionary_encoded() {
        for version in [WriterVersion::PARQUET_1_0, WriterVersion::PARQUET_2_0] {
            let chunk = column_chunk(values(10), props().set_writer_version(version));
            for buffer in buffers(&chunk) {
                assert!(all_pages_dictionary_encoded(&buffer), "{version:?}");
                assert_eq!(dictionary_len(&buffer), Some(10));
            }

            // A dictionary page too small for the values: the writer falls back.
            let fallback = props()
                .set_writer_version(version)
                .set_dictionary_page_size_limit(64);
            let chunk = column_chunk(values(500), fallback);
            for buffer in buffers(&chunk) {
                assert!(!all_pages_dictionary_encoded(&buffer));
            }

            let plain = props()
                .set_writer_version(version)
                .set_dictionary_enabled(false);
            let chunk = column_chunk(values(10), plain);
            for buffer in buffers(&chunk) {
                assert!(!all_pages_dictionary_encoded(&buffer));
            }
        }
    }

    #[test]
    fn test_pages() {
        let chunk = column_chunk(
            values(700),
            props().set_dictionary_enabled(false).set_column_encoding(
                ColumnPath::new(vec!["s".to_string()]),
                Encoding::DELTA_BYTE_ARRAY,
            ),
        );
        let walked = pages(std::slice::from_ref(&chunk)).unwrap();
        assert!(walked.len() > 1);
        // The pages tile the chunk, each after its header.
        assert!(walked[0].data.start > 0);
        let mut read = 0;
        for page in &walked {
            assert!(page.data.start >= read, "pages overlap");
            assert_eq!(page.header.page_type, DATA_PAGE);
            assert_eq!(page.header.encoding, Some(DELTA_BYTE_ARRAY));
            assert_eq!(page.header.num_values, Some(100));
            assert_eq!(page.header.compressed_size, page.data.len());
            read = page.data.end;
        }
        assert_eq!(read, chunk.len());

        // A dictionary page, then the pages the encoding replaced the dictionary with.
        let chunk = column_chunk(
            values(700),
            props()
                .set_dictionary_enabled(true)
                .set_dictionary_page_size_limit(256),
        );
        let walked = pages(std::slice::from_ref(&chunk)).unwrap();
        assert_eq!(walked[0].header.page_type, DICTIONARY_PAGE);
        assert!(walked[0].header.dictionary_len.is_some());
        assert!(!all_pages_dictionary_encoded(&Buffer::from(chunk.clone())));
    }

    #[test]
    fn test_invalid_page_headers() {
        let chunk = column_chunk(
            values(10),
            props().set_writer_version(WriterVersion::PARQUET_2_0),
        );
        for invalid in [
            chunk.slice(..chunk.len() - 1),
            chunk.slice(1..),
            Bytes::new(),
            Bytes::from_static(&[0xff; 16]),
        ] {
            for buffer in buffers(&invalid) {
                assert!(!all_pages_dictionary_encoded(&buffer));
            }
        }
    }
}
