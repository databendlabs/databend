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

use parquet2::compression::Compression;
use parquet2::encoding::Encoding;
use parquet2::error::Error;
use parquet2::metadata::Descriptor;
use parquet2::page::DataPageHeader;
use parquet2::page::PageType;
use parquet2::page::ParquetPageHeader;
use parquet2::read::PageMetaData;
use parquet_format_safe::thrift::protocol::TCompactInputProtocol;

use crate::reader::pages::BorrowedCompressedDataPage;
use crate::reader::pages::BorrowedCompressedDictPage;
use crate::reader::pages::BorrowedCompressedPage;

/// "Zero-copy" Parquet page reader
pub struct PageReader<'a> {
    raw_data_slice: &'a [u8],
    compression: Compression,
    seen_num_values: i64,
    total_num_values: i64,
    descriptor: Descriptor,
    max_page_size: usize,
}

impl<'a> PageReader<'a> {
    pub fn new_with_page_meta(
        raw_data: &'a [u8],
        reader_meta: PageMetaData,
        max_page_size: usize,
    ) -> Self {
        Self {
            raw_data_slice: raw_data,
            total_num_values: reader_meta.num_values,
            compression: reader_meta.compression,
            seen_num_values: 0,
            descriptor: reader_meta.descriptor,
            max_page_size,
        }
    }

    pub fn next_page(&mut self) -> parquet2::error::Result<Option<BorrowedCompressedPage>> {
        if self.seen_num_values >= self.total_num_values {
            return Ok(None);
        };

        let page_header =
            read_page_header_from_slice(&mut self.raw_data_slice, self.max_page_size)?;

        self.seen_num_values += get_page_header(&page_header)?
            .map(|x| x.num_values() as i64)
            .unwrap_or_default();

        let read_size: usize = page_header.compressed_page_size.try_into()?;

        if read_size > self.max_page_size {
            return Err(Error::WouldOverAllocate);
        }

        if self.raw_data_slice.len() < read_size {
            return Err(Error::OutOfSpec(
                "Not enough data in slice for page".to_string(),
            ));
        }

        let data_slice = &self.raw_data_slice[..read_size];
        self.raw_data_slice = &self.raw_data_slice[read_size..];

        match page_header.type_.try_into()? {
            PageType::DataPage => {
                let header = page_header.data_page_header.ok_or_else(|| {
                    Error::OutOfSpec(
                        "The page header type is a v1 data page but the v1 data header is empty"
                            .to_string(),
                    )
                })?;
                Ok(Some(BorrowedCompressedPage::new_data_page(
                    BorrowedCompressedDataPage::new(
                        DataPageHeader::V1(header),
                        data_slice,
                        self.compression,
                        page_header.uncompressed_page_size.try_into()?,
                        self.descriptor.clone(),
                    ),
                )))
            }
            PageType::DataPageV2 => {
                let header = page_header.data_page_header_v2.ok_or_else(|| {
                    Error::OutOfSpec(
                        "The page header type is a v2 data page but the v2 data header is empty"
                            .to_string(),
                    )
                })?;
                Ok(Some(BorrowedCompressedPage::new_data_page(
                    BorrowedCompressedDataPage::new(
                        DataPageHeader::V2(header),
                        data_slice,
                        self.compression,
                        page_header.uncompressed_page_size.try_into()?,
                        self.descriptor.clone(),
                    ),
                )))
            }
            PageType::DictionaryPage => {
                let dict_header = page_header.dictionary_page_header.as_ref().ok_or_else(|| {
                    Error::OutOfSpec(
                        "The page header type is a dictionary page but the dictionary header is empty".to_string(),
                    )
                })?;
                let num_values = dict_header.num_values.try_into()?;
                let is_sorted = dict_header.is_sorted.unwrap_or(false);

                Ok(Some(BorrowedCompressedPage::Dict(
                    BorrowedCompressedDictPage::new(
                        data_slice,
                        self.compression,
                        page_header.uncompressed_page_size.try_into()?,
                        num_values,
                        is_sorted,
                    ),
                )))
            }
        }
    }
}

fn read_page_header_from_slice(
    reader: &mut &[u8],
    max_size: usize,
) -> parquet2::error::Result<ParquetPageHeader> {
    let mut prot = TCompactInputProtocol::new(reader, max_size);
    let page_header = ParquetPageHeader::read_from_in_protocol(&mut prot)?;
    Ok(page_header)
}

pub(crate) fn get_page_header(
    header: &ParquetPageHeader,
) -> parquet2::error::Result<Option<DataPageHeader>> {
    let type_ = header.type_.try_into()?;
    Ok(match type_ {
        PageType::DataPage => {
            let header = header.data_page_header.clone().ok_or_else(|| {
                Error::OutOfSpec(
                    "The page header type is a v1 data page but the v1 header is empty".to_string(),
                )
            })?;

            let _: Encoding = header.encoding.try_into()?;
            let _: Encoding = header.repetition_level_encoding.try_into()?;
            let _: Encoding = header.definition_level_encoding.try_into()?;

            Some(DataPageHeader::V1(header))
        }
        PageType::DataPageV2 => {
            let header = header.data_page_header_v2.clone().ok_or_else(|| {
                Error::OutOfSpec(
                    "The page header type is a v1 data page but the v1 header is empty".to_string(),
                )
            })?;
            let _: Encoding = header.encoding.try_into()?;
            Some(DataPageHeader::V2(header))
        }
        _ => None,
    })
}
