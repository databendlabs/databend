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

use std::io::Read;
use std::iter;
use bytes::Bytes;
use parquet::basic::{Encoding, Type};
use parquet::bloom_filter::Sbbf;
use parquet::column::page::{Page, PageMetadata, PageReader};
use parquet::compression::{Codec, CodecOptionsBuilder, create_codec};
use parquet::errors::ParquetError;
use parquet::file::metadata::{ColumnChunkMetaData, ParquetMetaData, ParquetMetaDataReader, RowGroupMetaData};
use parquet::file::page_index::offset_index::OffsetIndexMetaData;
use parquet::file::properties::{ReaderProperties, ReaderPropertiesPtr};
use parquet::file::reader::{ChunkReader, FileReader, RowGroupReader};
use parquet::file::statistics;
use parquet::format::{PageHeader, PageLocation, PageType};
use parquet::record::reader::RowIter;
use parquet::record::Row;
use parquet::thrift::TSerializable;
use tantivy_common::HasLen;
use thrift::protocol::TCompactInputProtocol;

/// Reads a [`PageHeader`] from the provided [`Read`]
pub(crate) fn read_page_header<T: Read>(input: &mut T) -> parquet::errors::Result<PageHeader> {
    let mut prot = TCompactInputProtocol::new(input);
    let page_header = PageHeader::read_from_in_protocol(&mut prot)?;
    Ok(page_header)
}

/// Reads a [`PageHeader`] from the provided [`Read`] returning the number of bytes read
fn read_page_header_len<T: Read>(input: &mut T) -> parquet::errors::Result<(usize, PageHeader)> {
    /// A wrapper around a [`std::io::Read`] that keeps track of the bytes read
    struct TrackedRead<R> {
        inner: R,
        bytes_read: usize,
    }

    impl<R: Read> Read for TrackedRead<R> {
        fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
            let v = self.inner.read(buf)?;
            self.bytes_read += v;
            Ok(v)
        }
    }

    let mut tracked = TrackedRead {
        inner: input,
        bytes_read: 0,
    };
    let header = read_page_header(&mut tracked)?;
    Ok((tracked.bytes_read, header))
}

/// Decodes a [`Page`] from the provided `buffer`
pub(crate) fn decode_page(
    page_header: PageHeader,
    buffer: Bytes,
    physical_type: Type,
    decompressor: Option<&mut Box<dyn Codec>>,
) -> parquet::errors::Result<Page> {
    // When processing data page v2, depending on enabled compression for the
    // page, we should account for uncompressed data ('offset') of
    // repetition and definition levels.
    //
    // We always use 0 offset for other pages other than v2, `true` flag means
    // that compression will be applied if decompressor is defined
    let mut offset: usize = 0;
    let mut can_decompress = true;

    if let Some(ref header_v2) = page_header.data_page_header_v2 {
        offset = (header_v2.definition_levels_byte_length + header_v2.repetition_levels_byte_length)
            as usize;
        // When is_compressed flag is missing the page is considered compressed
        can_decompress = header_v2.is_compressed.unwrap_or(true);
    }

    // TODO: page header could be huge because of statistics. We should set a
    // maximum page header size and abort if that is exceeded.
    let buffer = match decompressor {
        Some(decompressor) if can_decompress => {
            let uncompressed_size = page_header.uncompressed_page_size as usize;
            let mut decompressed = Vec::with_capacity(uncompressed_size);
            let compressed = &buffer.as_ref()[offset..];
            decompressed.extend_from_slice(&buffer.as_ref()[..offset]);
            decompressor.decompress(
                compressed,
                &mut decompressed,
                Some(uncompressed_size - offset),
            )?;

            if decompressed.len() != uncompressed_size {
                panic!("Actual decompressed size doesn't match the expected one ({} vs {})",
                       decompressed.len(),
                       uncompressed_size
                );
            }

            Bytes::from(decompressed)
        }
        _ => buffer,
    };

    let result = match page_header.type_ {
        PageType::DICTIONARY_PAGE => {
            let dict_header = page_header.dictionary_page_header.as_ref().ok_or_else(|| {
                ParquetError::General("Missing dictionary page header".to_string())
            })?;
            let is_sorted = dict_header.is_sorted.unwrap_or(false);
            Page::DictionaryPage {
                buf: buffer,
                num_values: dict_header.num_values as u32,
                encoding: Encoding::try_from(dict_header.encoding)?,
                is_sorted,
            }
        }
        PageType::DATA_PAGE => {
            let header = page_header
                .data_page_header
                .ok_or_else(|| ParquetError::General("Missing V1 data page header".to_string()))?;
            Page::DataPage {
                buf: buffer,
                num_values: header.num_values as u32,
                encoding: Encoding::try_from(header.encoding)?,
                def_level_encoding: Encoding::try_from(header.definition_level_encoding)?,
                rep_level_encoding: Encoding::try_from(header.repetition_level_encoding)?,
                statistics: statistics::from_thrift(physical_type, header.statistics)?,
            }
        }
        PageType::DATA_PAGE_V2 => {
            let header = page_header
                .data_page_header_v2
                .ok_or_else(|| ParquetError::General("Missing V2 data page header".to_string()))?;
            let is_compressed = header.is_compressed.unwrap_or(true);
            Page::DataPageV2 {
                buf: buffer,
                num_values: header.num_values as u32,
                encoding: Encoding::try_from(header.encoding)?,
                num_nulls: header.num_nulls as u32,
                num_rows: header.num_rows as u32,
                def_levels_byte_len: header.definition_levels_byte_length as u32,
                rep_levels_byte_len: header.repetition_levels_byte_length as u32,
                is_compressed,
                statistics: statistics::from_thrift(physical_type, header.statistics)?,
            }
        }
        _ => {
            // For unknown page type (e.g., INDEX_PAGE), skip and read next.
            unimplemented!("Page type {:?} is not supported", page_header.type_)
        }
    };

    Ok(result)
}

pub struct InMemorySerializedPageReader {
    /// The data
    bytes: Bytes,

    /// The compression codec for this column chunk. Only set for non-PLAIN codec.
    decompressor: Option<Box<dyn Codec>>,

    /// Column chunk type.
    physical_type: Type,

    /// The current byte offset in the reader
    offset: usize,

    /// The length of the chunk in bytes
    remaining_bytes: usize,

    // If the next page header has already been "peeked", we will cache it and it`s length here
    next_page_header: Option<Box<PageHeader>>,
}

impl InMemorySerializedPageReader {
    pub fn new(bytes: Bytes, meta: &ColumnChunkMetaData, total_rows: usize) -> parquet::errors::Result<Self> {
        let options = CodecOptionsBuilder::default().build();
        let decompressor = create_codec(meta.compression(), &options)?;
        let (start, len) = meta.byte_range();

        Ok(Self {
            bytes,
            decompressor,
            physical_type: meta.column_type(),
            offset: start as usize,
            remaining_bytes: len as usize,
            next_page_header: None,
        })
    }
}

impl Iterator for InMemorySerializedPageReader {
    type Item = parquet::errors::Result<Page>;

    fn next(&mut self) -> Option<Self::Item> {
        self.get_next_page().transpose()
    }
}

impl PageReader for InMemorySerializedPageReader {
    fn get_next_page(&mut self) -> parquet::errors::Result<Option<Page>> {
        loop {
            if self.remaining_bytes == 0 {
                return Ok(None);
            }

            let header = if let Some(header) = self.next_page_header.take() {
                *header
            } else {
                let mut bytes = &self.bytes.as_ref()[self.offset..];
                let (header_len, header) = read_page_header_len(&mut bytes)?;
                self.offset += header_len;
                self.remaining_bytes -= header_len;
                header
            };

            let data_len = header.compressed_page_size as usize;
            self.offset += data_len;
            self.remaining_bytes -= data_len;

            if header.type_ == PageType::INDEX_PAGE {
                continue;
            }

            let buffer = self.bytes.slice(self.offset..self.offset + data_len);

            if buffer.len() != data_len {
                panic!("Expected to read {} bytes of page, read only {}", data_len, buffer.len());
            }

            return Ok(Some(decode_page(
                header,
                buffer,
                self.physical_type,
                self.decompressor.as_mut(),
            )?));
        }
    }

    fn peek_next_page(&mut self) -> parquet::errors::Result<Option<PageMetadata>> {
        loop {
            if self.remaining_bytes == 0 {
                return Ok(None);
            }
            return if let Some(header) = self.next_page_header.as_ref() {
                if let Ok(page_meta) = (&**header).try_into() {
                    Ok(Some(page_meta))
                } else {
                    // For unknown page type (e.g., INDEX_PAGE), skip and read next.
                    self.next_page_header = None;
                    continue;
                }
            } else {
                let mut bytes = &self.bytes.as_ref()[self.offset..];
                let (header_len, header) = read_page_header_len(&mut bytes)?;
                self.offset += header_len;
                self.remaining_bytes -= header_len;
                let page_meta = if let Ok(page_meta) = (&header).try_into() {
                    Ok(Some(page_meta))
                } else {
                    // For unknown page type (e.g., INDEX_PAGE), skip and read next.
                    continue;
                };
                self.next_page_header = Some(Box::new(header));
                page_meta
            };
        }
    }

    fn skip_next_page(&mut self) -> parquet::errors::Result<()> {
        if let Some(buffered_header) = self.next_page_header.take() {
            // The next page header has already been peeked, so just advance the offset
            self.offset += buffered_header.compressed_page_size as usize;
            self.remaining_bytes -= buffered_header.compressed_page_size as usize;
        } else {
            let mut bytes = &self.bytes.as_ref()[self.offset..];
            let (header_len, header) = read_page_header_len(&mut bytes)?;
            let data_page_size = header.compressed_page_size as usize;
            self.offset += header_len + data_page_size;
            self.remaining_bytes -= header_len + data_page_size;
        }
        Ok(())
    }

    fn at_record_boundary(&mut self) -> parquet::errors::Result<bool> {
        Ok(self.peek_next_page()?.is_none())
    }
}
