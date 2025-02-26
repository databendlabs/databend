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

use databend_common_expression::DataBlock;

pub trait AbstractBlockMeta {
    fn location_path(&self) -> &str;
}

pub struct ColumnOrientedBlockMeta {
    blocks: DataBlock,
    index: usize,
}

impl ColumnOrientedBlockMeta {
    pub fn new(blocks: DataBlock, index: usize) -> Self {
        Self { blocks, index }
    }
}

impl AbstractBlockMeta for ColumnOrientedBlockMeta {
    fn location_path(&self) -> &str {
        todo!()
    }
}
