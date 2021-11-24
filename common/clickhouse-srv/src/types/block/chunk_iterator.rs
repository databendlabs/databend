// Copyright 2021 Datafuse Labs.
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

use std::cmp;

use crate::types::Block;
use crate::types::ColumnType;

pub struct ChunkIterator<'a, K: ColumnType> {
    position: usize,
    size: usize,
    block: &'a Block<K>,
}

impl<'a, K: ColumnType> Iterator for ChunkIterator<'a, K> {
    type Item = Block;

    fn next(&mut self) -> Option<Block> {
        let m = self.block.row_count();

        if m == 0 && self.position == 0 {
            self.position += 1;
            let mut result = Block::default();

            for column in self.block.columns().iter() {
                let data = column.slice(0..0);
                result = result.column(column.name(), data);
            }

            return Some(result);
        }

        if self.position >= m {
            return None;
        }

        let mut result = Block::new();
        let size = cmp::min(self.size, m - self.position);

        for column in self.block.columns().iter() {
            let range = self.position..self.position + size;
            let data = column.slice(range);
            result = result.column(column.name(), data);
        }

        self.position += size;
        Some(result)
    }
}

impl<'a, K: ColumnType> ChunkIterator<'a, K> {
    pub fn new(size: usize, block: &Block<K>) -> ChunkIterator<K> {
        ChunkIterator {
            position: 0,
            size,
            block,
        }
    }
}
