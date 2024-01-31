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

use databend_common_expression::BlockMetaInfo;

#[derive(serde::Serialize, serde::Deserialize, Debug)]
pub struct BytesBatch {
    pub data: Vec<u8>,

    pub path: String,
    pub offset: usize,
    pub is_eof: bool,
}

impl BytesBatch {
    pub fn meta(&self) -> Self {
        Self {
            data: vec![],
            path: self.path.clone(),
            offset: self.offset,
            is_eof: self.is_eof,
        }
    }
}

#[typetag::serde(name = "raw_batch")]
impl BlockMetaInfo for BytesBatch {
    fn equals(&self, _info: &Box<dyn BlockMetaInfo>) -> bool {
        unreachable!("RawBatch as BlockMetaInfo is not expected to be compared.")
    }

    fn clone_self(&self) -> Box<dyn BlockMetaInfo> {
        unreachable!("RawBatch as BlockMetaInfo is not expected to be cloned.")
    }
}

#[derive(serde::Serialize, serde::Deserialize, Debug, Default)]
pub struct RowBatch {
    /// row[i] starts at row_ends[i-1] and ends at row_ends[i]
    /// has num_fields[i] fields
    /// field[j] starts at field_ends[i-1][j] and ends at field_ends[i-1][j]
    pub data: Vec<u8>,
    pub row_ends: Vec<usize>,
    pub field_ends: Vec<usize>,
    pub num_fields: Vec<usize>,

    pub path: String,
    pub offset: usize,
    // start from 0
    pub start_row_id: usize,
}

impl RowBatch {
    pub fn new(raw: &BytesBatch, start_row_id: usize) -> Self {
        Self {
            path: raw.path.clone(),
            offset: raw.offset,
            start_row_id,
            ..Default::default()
        }
    }

    pub fn rows(&self) -> usize {
        self.row_ends.len()
    }

    pub fn size(&self) -> usize {
        self.data.len()
    }
}

#[typetag::serde(name = "row_batch")]
impl BlockMetaInfo for RowBatch {
    fn equals(&self, _info: &Box<dyn BlockMetaInfo>) -> bool {
        unreachable!("RowBatch as BlockMetaInfo is not expected to be compared.")
    }

    fn clone_self(&self) -> Box<dyn BlockMetaInfo> {
        unreachable!("RowBatch as BlockMetaInfo is not expected to be cloned.")
    }
}
