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

use std::sync::Arc;

use databend_common_expression::BlockMetaInfo;
use databend_common_expression::DataBlock;
use databend_common_expression::local_block_meta_serde;

#[derive(Debug)]
pub struct FileOutputBuffer {
    pub buffer: Vec<u8>,
    pub row_counts: usize,
}

impl FileOutputBuffer {
    pub fn create(buffer: Vec<u8>, row_counts: usize) -> Self {
        FileOutputBuffer { buffer, row_counts }
    }
}

#[derive(Debug)]
pub struct FileOutputBuffers {
    pub buffers: Vec<FileOutputBuffer>,
    pub partition: Option<Arc<str>>,
}

impl FileOutputBuffers {
    pub fn create_block(buffers: Vec<FileOutputBuffer>, partition: Option<Arc<str>>) -> DataBlock {
        DataBlock::empty_with_meta(Box::new(FileOutputBuffers { buffers, partition }))
    }
}

impl Clone for FileOutputBuffers {
    fn clone(&self) -> Self {
        unreachable!("Buffers should not be cloned")
    }
}

local_block_meta_serde!(FileOutputBuffers);

#[typetag::serde(name = "unload_buffers")]
impl BlockMetaInfo for FileOutputBuffers {}
