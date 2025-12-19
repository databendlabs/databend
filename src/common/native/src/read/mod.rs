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

mod array;
pub mod batch_read;
pub mod deserialize;
use batch_read::batch_read_column;
use databend_common_expression::Column;
use databend_common_expression::TableDataType;
use databend_common_expression::TableField;
pub use deserialize::ColumnIter;
pub use deserialize::column_iters;

use crate::error::Result;
pub(crate) mod read_basic;
use std::io::BufReader;

use super::PageMeta;
use super::nested::InitNested;
pub mod reader;

pub trait NativeReadBuf: std::io::BufRead {
    fn buffer_bytes(&self) -> &[u8];
}

impl<R: std::io::Read> NativeReadBuf for BufReader<R> {
    fn buffer_bytes(&self) -> &[u8] {
        self.buffer()
    }
}

impl<R: bytes::Buf> NativeReadBuf for bytes::buf::Reader<R> {
    fn buffer_bytes(&self) -> &[u8] {
        self.get_ref().chunk()
    }
}

impl NativeReadBuf for &[u8] {
    fn buffer_bytes(&self) -> &[u8] {
        self
    }
}

impl<T: AsRef<[u8]>> NativeReadBuf for std::io::Cursor<T> {
    fn buffer_bytes(&self) -> &[u8] {
        let len = self.position().min(self.get_ref().as_ref().len() as u64);
        &self.get_ref().as_ref()[(len as usize)..]
    }
}

impl<B: NativeReadBuf + ?Sized> NativeReadBuf for Box<B> {
    fn buffer_bytes(&self) -> &[u8] {
        (**self).buffer_bytes()
    }
}

pub trait PageIterator {
    fn swap_buffer(&mut self, buffer: &mut Vec<u8>);
}

#[derive(Clone)]
pub struct NativeColumnsReader {}

impl NativeColumnsReader {
    pub fn new() -> Result<Self> {
        Ok(Self {})
    }

    /// An iterator adapter that maps [`PageIterator`]s into an iterator of [`Array`]s.
    pub fn column_iters<'a, I>(
        &self,
        readers: Vec<I>,
        field: TableField,
        init: Vec<InitNested>,
    ) -> Result<ColumnIter<'a>>
    where
        I: Iterator<Item = Result<(u64, Vec<u8>)>> + PageIterator + Send + Sync + 'a,
    {
        column_iters(readers, field, init)
    }

    /// Read all pages of column at once.
    pub fn batch_read_column<R: NativeReadBuf>(
        &self,
        readers: Vec<R>,
        data_type: TableDataType,
        page_metas: Vec<Vec<PageMeta>>,
    ) -> Result<Column> {
        batch_read_column(readers, data_type, page_metas)
    }
}
