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

use std::sync::Arc;

use common_exception::Result;

use crate::Chunk;

pub type Aborting = Arc<Box<dyn Fn() -> bool + Send + Sync + 'static>>;

#[derive(Clone)]
pub struct SortColumnDescription {
    pub column_name: String,
    pub asc: bool,
    pub nulls_first: bool,
}

impl Chunk {
    pub fn sort(
        _chunk: &Chunk,
        _sort_columns_descriptions: &[SortColumnDescription],
        _limit: Option<usize>,
    ) -> Result<Chunk> {
        todo!("expression")
    }

    pub fn merge_sort_block(
        _lhs: &Chunk,
        _rhs: &Chunk,
        _sort_columns_descriptions: &[SortColumnDescription],
        _limit: Option<usize>,
    ) -> Result<Chunk> {
        todo!("expression")
    }

    pub fn merge_sort_blocks(
        _blocks: &[Chunk],
        _sort_columns_descriptions: &[SortColumnDescription],
        _limit: Option<usize>,
        _aborting: Aborting,
    ) -> Result<Chunk> {
        todo!("expression")
    }
}
