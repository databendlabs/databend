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
//! encoding of its pages without decompressing them. The parquet crate does not expose page
//! encodings without reading the pages.

const DATA_PAGE: i32 = 0;
const DICTIONARY_PAGE: i32 = 2;
const DATA_PAGE_V2: i32 = 3;
const PLAIN_DICTIONARY: i32 = 2;
const RLE_DICTIONARY: i32 = 8;

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
pub fn all_pages_dictionary_encoded(chunk: &[u8]) -> bool {
    dictionary_len(chunk).is_some()
}

/// The number of values of the dictionary of the column chunk, if it is dictionary encoded
/// throughout (see [`all_pages_dictionary_encoded`]).
pub fn dictionary_len(chunk: &[u8]) -> Option<usize> {
    let mut input = Input { buf: chunk, pos: 0 };
    let mut dictionary_len = None;
    let mut data_pages = 0;
    while input.pos < chunk.len() {
        let header = PageHeader::read(&mut input)?;
        match header.page_type {
            DICTIONARY_PAGE if dictionary_len.is_none() => {
                dictionary_len = Some(header.dictionary_len?);
            }
            DATA_PAGE | DATA_PAGE_V2
                if dictionary_len.is_some()
                    && matches!(header.encoding, Some(PLAIN_DICTIONARY | RLE_DICTIONARY)) =>
            {
                data_pages += 1;
            }
            _ => return None,
        }
        input.skip(header.compressed_size)?;
    }
    dictionary_len.filter(|_| data_pages > 0)
}

struct PageHeader {
    page_type: i32,
    /// Encoding of the values of a data page.
    encoding: Option<i32>,
    /// Number of values of a dictionary page.
    dictionary_len: Option<usize>,
    compressed_size: usize,
}

impl PageHeader {
    fn read(input: &mut Input) -> Option<PageHeader> {
        let (mut page_type, mut encoding, mut compressed_size) = (None, None, None);
        let mut dictionary_len = None;
        let mut last = 0;
        while let Some((id, ty)) = input.field(&mut last)? {
            match (id, ty) {
                (1, I32) => page_type = Some(input.zigzag()? as i32),
                (3, I32) => compressed_size = usize::try_from(input.zigzag()?).ok(),
                // `DataPageHeader.encoding` and `DataPageHeaderV2.encoding`.
                (5, STRUCT) => encoding = input.struct_i32(2)?,
                (8, STRUCT) => encoding = input.struct_i32(4)?,
                // `DictionaryPageHeader.num_values`.
                (7, STRUCT) => {
                    dictionary_len = input
                        .struct_i32(1)?
                        .and_then(|len| usize::try_from(len).ok())
                }
                _ => input.skip_value(ty)?,
            }
        }
        Some(PageHeader {
            page_type: page_type?,
            encoding,
            dictionary_len,
            compressed_size: compressed_size?,
        })
    }
}

struct Input<'a> {
    buf: &'a [u8],
    pos: usize,
}

impl Input<'_> {
    fn byte(&mut self) -> Option<u8> {
        let b = *self.buf.get(self.pos)?;
        self.pos += 1;
        Some(b)
    }

    fn skip(&mut self, n: usize) -> Option<()> {
        let end = self.pos.checked_add(n)?;
        (end <= self.buf.len()).then(|| self.pos = end)
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
            STRUCT => {
                let mut last = 0;
                while let Some((_, ty)) = self.field(&mut last)? {
                    self.skip_value(ty)?;
                }
                Some(())
            }
            _ => None,
        }
    }

    /// Reads a struct, returning its i32 field `id` if present.
    fn struct_i32(&mut self, id: i16) -> Option<Option<i32>> {
        let mut last = 0;
        let mut value = None;
        while let Some((field, ty)) = self.field(&mut last)? {
            if field == id && ty == I32 {
                value = Some(i32::try_from(self.zigzag()?).ok()?);
            } else {
                self.skip_value(ty)?;
            }
        }
        Some(value)
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
    use bytes::Bytes;
    use parquet::arrow::ArrowWriter;
    use parquet::file::properties::WriterProperties;
    use parquet::file::properties::WriterVersion;
    use parquet::file::reader::FileReader;
    use parquet::file::serialized_reader::SerializedFileReader;

    use super::*;

    /// The column chunk of a one-column Parquet file of `values`.
    fn column_chunk(values: Vec<String>, props: WriterProperties) -> Bytes {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "s",
            ArrowDataType::Utf8,
            false,
        )]));
        let array: ArrayRef = Arc::new(StringArray::from(values));
        let batch = RecordBatch::try_new(schema.clone(), vec![array]).unwrap();
        let mut file = Vec::new();
        let mut writer = ArrowWriter::try_new(&mut file, schema, Some(props)).unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();
        let file = Bytes::from(file);
        let reader = SerializedFileReader::new(file.clone()).unwrap();
        let (start, len) = reader.metadata().row_group(0).column(0).byte_range();
        file.slice(start as usize..(start + len) as usize)
    }

    fn props(version: WriterVersion) -> parquet::file::properties::WriterPropertiesBuilder {
        WriterProperties::builder()
            .set_writer_version(version)
            .set_data_page_row_count_limit(100)
            .set_write_batch_size(100)
    }

    fn values(distinct: usize) -> Vec<String> {
        (0..1000)
            .map(|i| format!("value-{}", i % distinct))
            .collect()
    }

    #[test]
    fn test_all_pages_dictionary_encoded() {
        for version in [WriterVersion::PARQUET_1_0, WriterVersion::PARQUET_2_0] {
            let chunk = column_chunk(values(10), props(version).build());
            assert!(all_pages_dictionary_encoded(&chunk), "{version:?}");
            assert_eq!(dictionary_len(&chunk), Some(10));

            // A dictionary page too small for the values: the writer falls back to plain.
            let fallback = props(version).set_dictionary_page_size_limit(64).build();
            let chunk = column_chunk(values(500), fallback);
            assert!(!all_pages_dictionary_encoded(&chunk));

            let plain = props(version).set_dictionary_enabled(false).build();
            let chunk = column_chunk(values(10), plain);
            assert!(!all_pages_dictionary_encoded(&chunk));
        }
    }

    #[test]
    fn test_invalid_page_headers() {
        let chunk = column_chunk(values(10), props(WriterVersion::PARQUET_2_0).build());
        assert!(!all_pages_dictionary_encoded(&chunk[..chunk.len() - 1]));
        assert!(!all_pages_dictionary_encoded(&chunk[1..]));
        assert!(!all_pages_dictionary_encoded(&[]));
        assert!(!all_pages_dictionary_encoded(&[0xff; 16]));
    }
}
