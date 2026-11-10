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

use std::fmt::Debug;
use std::fmt::Formatter;
use std::sync::Arc;
use std::time::Instant;

use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::BlockMetaInfo;
use databend_common_expression::BlockMetaInfoDowncast;
use databend_common_expression::DataBlock;
use databend_common_expression::local_block_meta_serde;

use super::fetch::RowsFetchMetadata;

/// The input rows of a fetch, to which `Assemble` adds the fetched columns.
pub(super) struct RowFetchBatch {
    pub(super) id: u64,
    pub(super) inputs: Vec<DataBlock>,
    /// For each row with a row id, the part it is in and its row in that part.
    pub(super) indices: Vec<(u32, u32)>,
    pub(super) parts: usize,
    pub(super) start: Instant,
}

/// What `Plan` sends to `Fetch`, as the meta of an empty block.
pub(super) struct RowFetchTask {
    pub(super) batch: Arc<RowFetchBatch>,
    /// `None` for a batch without row ids, so that it also reaches `Assemble`.
    pub(super) block: Option<RowFetchBlockTask>,
}

/// The rows to fetch from one block of a batch.
pub(super) struct RowFetchBlockTask {
    pub(super) part: u32,
    pub(super) metadata: Arc<RowsFetchMetadata>,
    pub(super) take_indices: Vec<u32>,
}

/// What `Fetch` sends to `Assemble`, as the meta of an empty block.
pub(super) struct RowFetched {
    pub(super) batch: Arc<RowFetchBatch>,
    /// The part and its rows, `None` for a batch without row ids.
    pub(super) block: Option<(u32, DataBlock)>,
}

impl Debug for RowFetchTask {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RowFetchTask").finish()
    }
}

impl Debug for RowFetched {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RowFetched").finish()
    }
}

local_block_meta_serde!(RowFetchTask);
local_block_meta_serde!(RowFetched);

#[typetag::serde(name = "row_fetch_task")]
impl BlockMetaInfo for RowFetchTask {}

#[typetag::serde(name = "row_fetched")]
impl BlockMetaInfo for RowFetched {}

/// The message a block of a fetch carries.
pub(super) fn take_row_fetch_meta<T: BlockMetaInfoDowncast>(mut block: DataBlock) -> Result<T> {
    block
        .take_meta()
        .and_then(T::downcast_from)
        .ok_or_else(|| ErrorCode::Internal("Row fetch got a block without its message"))
}
