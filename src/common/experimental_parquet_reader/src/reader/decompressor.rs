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
use parquet2::page::DictPage;
use parquet2::page::Page;
use parquet2::FallibleStreamingIterator;

use crate::reader::page_reader::PageReader;
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
        let uncompressed_size = compressed_page.uncompressed_size();
        uncompressed_buffer.reserve(uncompressed_size);

        #[allow(clippy::uninit_vec)]
        unsafe {
            uncompressed_buffer.set_len(uncompressed_size);
        }

        if !compressed_page.is_compressed() {
            // No decompression needed - copy directly from the borrowed slice
            uncompressed_buffer.extend_from_slice(compressed_page.data());
        } else {
            // Decompress directly into the buffer
            match compressed_page.compression() {
                Compression::Lz4 => {
                    let _decompressed_len =
                        lz4_flex::decompress_into(compressed_page.data(), uncompressed_buffer)
                            .map_err(|e| {
                                Error::OutOfSpec(format!("LZ4 decompression failed: {}", e))
                            })?;
                }
                Compression::Zstd => {
                    zstd::bulk::decompress_to_buffer(compressed_page.data(), uncompressed_buffer)
                        .map(|_| ())
                        .map_err(|e| {
                            Error::OutOfSpec(format!("Zstd decompression failed: {}", e))
                        })?;
                }
                _ => {
                    return Err(Error::FeatureNotSupported(format!(
                        "Compression {:?} not supported",
                        compressed_page.compression()
                    )));
                }
            }
        };

        // Create a DataPage from the decompressed data
        // Note: We need to take ownership of the buffer data here
        let page = match compressed_page {
            BorrowedCompressedPage::Data(compressed_data_page) => Page::Data(DataPage::new(
                compressed_data_page.header,
                std::mem::take(uncompressed_buffer),
                compressed_data_page.descriptor,
                None,
            )),
            BorrowedCompressedPage::Dict(compressed_dict_page) => Page::Dict(DictPage::new(
                std::mem::take(uncompressed_buffer),
                compressed_dict_page.num_values,
                compressed_dict_page.is_sorted,
            )),
        };

        Ok(page)
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
