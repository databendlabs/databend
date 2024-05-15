// Copyright [2021] [Jorge C Leitao]
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

mod indexed_reader;
mod reader;
#[cfg(feature = "async")]
mod stream;

pub use indexed_reader::IndexedPageReader;
pub use reader::PageFilter;
pub use reader::PageMetaData;
pub use reader::PageReader;

use crate::error::Error;
use crate::page::CompressedPage;

pub trait PageIterator: Iterator<Item = Result<CompressedPage, Error>> {
    fn swap_buffer(&mut self, buffer: &mut Vec<u8>);
}

#[cfg(feature = "async")]
#[cfg_attr(docsrs, doc(cfg(feature = "async")))]
pub use stream::get_page_stream;
#[cfg(feature = "async")]
#[cfg_attr(docsrs, doc(cfg(feature = "async")))]
pub use stream::get_page_stream_from_column_start;
