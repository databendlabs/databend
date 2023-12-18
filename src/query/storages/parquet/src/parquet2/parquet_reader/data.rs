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

use std::collections::HashMap;

use bytes::Bytes;
use databend_common_exception::Result;
use databend_common_expression::DataBlock;
use databend_common_expression::FieldIndex;

pub trait BlockIterator: Iterator<Item = Result<DataBlock>> + Send {
    /// checking has_next() after next can avoid processor from entering SYNC for nothing.
    fn has_next(&self) -> bool;
}

pub struct OneBlock(pub Option<DataBlock>);

impl Iterator for OneBlock {
    type Item = Result<DataBlock>;

    fn next(&mut self) -> Option<Self::Item> {
        Ok(self.0.take()).transpose()
    }
}

impl BlockIterator for OneBlock {
    fn has_next(&self) -> bool {
        self.0.is_some()
    }
}

pub trait SeekRead: std::io::Read + std::io::Seek {}

impl<T> SeekRead for T where T: std::io::Read + std::io::Seek {}

pub type IndexedChunk = (FieldIndex, Vec<u8>);
pub type IndexedChunks = HashMap<FieldIndex, Bytes>;

pub enum Parquet2PartData {
    RowGroup(IndexedChunks),
    SmallFiles(Vec<Vec<u8>>),
}
