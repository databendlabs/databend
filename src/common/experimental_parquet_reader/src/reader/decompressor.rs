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

//! Decompressor that integrates with zero-copy PageReader

use parquet2::compression::Compression;
use parquet2::error::Error;
use parquet2::page::DataPage;
use parquet2::page::DataPageHeader;
use parquet2::page::DataPageHeaderV2;
use parquet2::page::DictPage;
use parquet2::page::Page;
use parquet2::FallibleStreamingIterator;

use crate::reader::page_reader::PageReader;
use crate::reader::pages::BorrowedCompressedDataPage;
use crate::reader::pages::BorrowedCompressedDictPage;
use crate::reader::pages::BorrowedCompressedPage;

pub struct Decompressor<'a> {
    page_reader: PageReader<'a>,
    decompression_buffer: Vec<u8>,
    current_page: Option<Page>,
    was_decompressed: bool,
}

impl<'a> Decompressor<'a> {
    pub fn new(page_reader: PageReader<'a>, decompression_buffer: Vec<u8>) -> Self {
        Self {
            page_reader,
            decompression_buffer,
            current_page: None,
            was_decompressed: false,
        }
    }

    fn decompress_borrowed_page(
        compressed_page: BorrowedCompressedPage<'_>,
        uncompressed_buffer: &mut Vec<u8>,
    ) -> parquet2::error::Result<Page> {
        match compressed_page {
            BorrowedCompressedPage::Data(compressed_data_page) => {
                let BorrowedCompressedDataPage {
                    header,
                    buffer,
                    compression,
                    uncompressed_page_size,
                    descriptor,
                } = *compressed_data_page;

                uncompressed_buffer.clear();
                uncompressed_buffer.reserve(uncompressed_page_size);
                unsafe {
                    uncompressed_buffer.set_len(uncompressed_page_size);
                }

                match &header {
                    DataPageHeader::V2(v2) => decode_data_page_v2(
                        buffer,
                        compression,
                        v2,
                        uncompressed_buffer.as_mut_slice(),
                    )?,
                    _ => {
                        decode_full_slice(buffer, compression, uncompressed_buffer.as_mut_slice())?
                    }
                }

                Ok(Page::Data(DataPage::new(
                    header,
                    std::mem::take(uncompressed_buffer),
                    descriptor,
                    None,
                )))
            }
            BorrowedCompressedPage::Dict(dict_page) => {
                let BorrowedCompressedDictPage {
                    buffer,
                    compression,
                    uncompressed_page_size,
                    num_values,
                    is_sorted,
                } = dict_page;

                uncompressed_buffer.clear();
                uncompressed_buffer.reserve(uncompressed_page_size);
                unsafe {
                    uncompressed_buffer.set_len(uncompressed_page_size);
                }
                decode_full_slice(buffer, compression, uncompressed_buffer.as_mut_slice())?;

                Ok(Page::Dict(DictPage::new(
                    std::mem::take(uncompressed_buffer),
                    num_values,
                    is_sorted,
                )))
            }
        }
    }

    pub fn next_owned(&mut self) -> Result<Option<Page>, Error> {
        let page_tuple = self.page_reader.next_page()?;

        if let Some(page) = page_tuple {
            self.was_decompressed = page.compression() != Compression::Uncompressed;

            let decompress_page =
                Self::decompress_borrowed_page(page, &mut self.decompression_buffer)?;

            Ok(Some(decompress_page))
        } else {
            Ok(None)
        }
    }
}

fn decode_full_slice(
    src: &[u8],
    compression: Compression,
    dst: &mut [u8],
) -> parquet2::error::Result<()> {
    match compression {
        Compression::Uncompressed => {
            if src.len() != dst.len() {
                return Err(Error::OutOfSpec(format!(
                    "Uncompressed data length mismatch: src={}, dst={}",
                    src.len(),
                    dst.len()
                )));
            }
            dst.copy_from_slice(src);
            Ok(())
        }
        Compression::Lz4Raw => lz4_flex::decompress_into(src, dst)
            .map(|_| ())
            .map_err(|e| Error::OutOfSpec(format!("LZ4 decompression failed: {}", e))),
        Compression::Zstd => zstd::bulk::decompress_to_buffer(src, dst)
            .map(|_| ())
            .map_err(|e| Error::OutOfSpec(format!("Zstd decompression failed: {}", e))),
        other => Err(Error::FeatureNotSupported(format!(
            "Compression {:?} not supported",
            other
        ))),
    }
}

fn decode_data_page_v2(
    buffer: &[u8],
    compression: Compression,
    header: &DataPageHeaderV2,
    dst: &mut [u8],
) -> parquet2::error::Result<()> {
    let rep_len = usize_from_i32(header.repetition_levels_byte_length, "repetition")?;
    let def_len = usize_from_i32(header.definition_levels_byte_length, "definition")?;
    let prefix_len = rep_len
        .checked_add(def_len)
        .ok_or_else(|| Error::OutOfSpec("Level bytes length overflow".to_string()))?;

    if buffer.len() < prefix_len || dst.len() < prefix_len {
        return Err(Error::OutOfSpec(
            "Data page v2 prefix longer than buffers".to_string(),
        ));
    }

    dst[..rep_len].copy_from_slice(&buffer[..rep_len]);
    dst[rep_len..prefix_len].copy_from_slice(&buffer[rep_len..rep_len + def_len]);

    let values_src = &buffer[prefix_len..];
    let values_dst = &mut dst[prefix_len..];

    if header.is_compressed.unwrap_or(true) {
        decode_full_slice(values_src, compression, values_dst)
    } else {
        if values_dst.len() != values_src.len() {
            return Err(Error::OutOfSpec(
                "Uncompressed data page v2 values length mismatch".to_string(),
            ));
        }
        values_dst.copy_from_slice(values_src);
        Ok(())
    }
}


// TODO do we need this? value:i32 should not be negative according to spec
fn usize_from_i32(value: i32, field: &str) -> parquet2::error::Result<usize> {
    usize::try_from(value).map_err(|_| {
        Error::OutOfSpec(format!(
            "Negative {} levels byte length in data page header v2",
            field
        ))
    })
}

impl<'a> FallibleStreamingIterator for Decompressor<'a> {
    type Item = Page;
    type Error = Error;

    fn advance(&mut self) -> Result<(), Self::Error> {
        self.current_page = None;
        let page_tuple = self.page_reader.next_page()?;

        if let Some(page) = page_tuple {
            self.was_decompressed = page.compression() != Compression::Uncompressed;

            let decompress_page =
                Self::decompress_borrowed_page(page, &mut self.decompression_buffer)?;

            self.current_page = Some(decompress_page);
        }

        Ok(())
    }

    fn get(&self) -> Option<&Self::Item> {
        self.current_page.as_ref()
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        (0, None)
    }
}
