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

#[macro_use]
mod errors;
#[macro_use]
mod util;

mod compression;
pub use compression::CommonCompression;
pub mod read;
pub mod stat;
pub mod write;

const ARROW_MAGIC: [u8; 6] = [b'A', b'R', b'R', b'O', b'W', b'2'];
pub(crate) const CONTINUATION_MARKER: [u8; 4] = [0xff; 4];

#[derive(
    Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd, serde::Serialize, serde::Deserialize,
)]
pub struct ColumnMeta {
    pub offset: u64,
    pub pages: Vec<PageMeta>,
}

impl ColumnMeta {
    // [start_page_index, end_page_index)
    pub fn slice(&self, start_page_index: usize, end_page_index: usize) -> Self {
        assert!(start_page_index < self.pages.len());
        assert!(end_page_index <= self.pages.len());

        let offset = self
            .pages
            .iter()
            .take(start_page_index)
            .map(|meta| meta.length)
            .sum::<u64>()
            + self.offset;
        let pages = self.pages[start_page_index..end_page_index].to_vec();

        Self { offset, pages }
    }

    pub fn skip_one_page(&self) -> Self {
        self.slice(1, self.pages.len())
    }

    pub fn total_len(&self) -> u64 {
        self.pages.iter().map(|m| m.length).sum::<u64>()
    }
}

#[derive(
    Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd, serde::Serialize, serde::Deserialize,
)]
pub struct PageMeta {
    // compressed size of this page
    pub length: u64,
    // num values(rows) of this page
    pub num_values: u64,
}
