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

use databend_common_catalog::merge_into_join::MergeIntoJoinType;
use databend_common_catalog::table_context::TableContext;
use databend_common_exception::Result;
use databend_common_expression::DataBlock;

use crate::operations::BlockMetaIndex;
use crate::FusePartInfo;

pub fn need_reserve_block_info(ctx: Arc<dyn TableContext>, table_idx: usize) -> (bool, bool) {
    let merge_into_join = ctx.get_merge_into_join();
    (
        matches!(
            merge_into_join.merge_into_join_type,
            MergeIntoJoinType::Left
        ) && merge_into_join.target_tbl_idx == table_idx,
        merge_into_join.is_distributed,
    )
}

// for merge into target build, in this situation, we don't need rowid
pub(crate) fn add_row_prefix_meta(
    need_reserve_block_info: bool,
    fuse_part: &FusePartInfo,
    mut block: DataBlock,
) -> Result<DataBlock> {
    if need_reserve_block_info && fuse_part.block_meta_index.is_some() {
        let block_meta_index = fuse_part.block_meta_index.as_ref().unwrap();
        // in fact, inner_meta is none for now, for merge into target build, we don't need
        // to get row_id.
        let inner_meta = block.take_meta();
        block.add_meta(Some(Box::new(BlockMetaIndex {
            segment_idx: block_meta_index.segment_idx,
            block_idx: block_meta_index.block_id,
            inner: inner_meta,
        })))
    } else {
        Ok(block)
    }
}
