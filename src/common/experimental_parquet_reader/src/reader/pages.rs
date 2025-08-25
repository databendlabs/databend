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

//! Borrowed page types for zero-copy parquet reading
//!
//! This module provides page types that reference data without owning it,
//! enabling "zero-copy" reading when the source is a slice.

use parquet2::compression::Compression;
use parquet2::metadata::Descriptor;
use parquet2::page::DataPageHeader;

/// A compressed page that borrows its data from a slice
#[derive(Debug)]
pub enum BorrowedCompressedPage<'a> {
    Data(Box<BorrowedCompressedDataPage<'a>>),
    Dict(BorrowedCompressedDictPage<'a>),
}

impl<'a> BorrowedCompressedPage<'a> {
    pub fn new_data_page(data_page: BorrowedCompressedDataPage<'a>) -> Self {
        Self::Data(Box::new(data_page))
    }
}

/// A borrowed compressed data page
#[derive(Debug)]
pub struct BorrowedCompressedDataPage<'a> {
    pub header: DataPageHeader,
    pub buffer: &'a [u8],
    pub compression: Compression,
    pub uncompressed_page_size: usize,
    pub descriptor: Descriptor,
}

/// A borrowed compressed dictionary page
#[derive(Debug)]
pub struct BorrowedCompressedDictPage<'a> {
    pub buffer: &'a [u8],
    pub compression: Compression,
    pub uncompressed_page_size: usize,
    pub num_values: usize,
    pub is_sorted: bool,
}

impl<'a> BorrowedCompressedPage<'a> {
    /// Get the compression type of this page
    pub fn compression(&self) -> Compression {
        match self {
            BorrowedCompressedPage::Data(data_page) => data_page.compression,
            BorrowedCompressedPage::Dict(dict_page) => dict_page.compression,
        }
    }

    /// Check if this page is compressed
    pub fn is_compressed(&self) -> bool {
        self.compression() != Compression::Uncompressed
    }

    /// Get the uncompressed size of this page
    pub fn uncompressed_size(&self) -> usize {
        match self {
            BorrowedCompressedPage::Data(data_page) => data_page.uncompressed_page_size,
            BorrowedCompressedPage::Dict(dict_page) => dict_page.uncompressed_page_size,
        }
    }

    /// Get the compressed data as a slice
    pub fn data(&self) -> &[u8] {
        match self {
            BorrowedCompressedPage::Data(data_page) => data_page.buffer,
            BorrowedCompressedPage::Dict(dict_page) => dict_page.buffer,
        }
    }
}

impl<'a> BorrowedCompressedDataPage<'a> {
    pub fn new(
        header: DataPageHeader,
        buffer: &'a [u8],
        compression: Compression,
        uncompressed_page_size: usize,
        descriptor: Descriptor,
    ) -> Self {
        Self {
            header,
            buffer,
            compression,
            uncompressed_page_size,
            descriptor,
        }
    }
}

impl<'a> BorrowedCompressedDictPage<'a> {
    pub fn new(
        buffer: &'a [u8],
        compression: Compression,
        uncompressed_page_size: usize,
        num_values: usize,
        is_sorted: bool,
    ) -> Self {
        Self {
            buffer,
            compression,
            uncompressed_page_size,
            num_values,
            is_sorted,
        }
    }
}
