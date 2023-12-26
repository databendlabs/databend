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

use async_channel::Sender;
use async_trait::async_trait;
use async_trait::unboxed_simple;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::BlockMetaInfoDowncast;
use databend_common_expression::DataBlock;
use databend_common_expression::TableSchemaRef;
use databend_common_expression::BLOCK_NAME_COL_NAME;
use databend_common_pipeline_sinks::AsyncSink;
use databend_storages_common_index::RangeIndex;
use databend_storages_common_pruner::InternalColumnPruner;
use databend_storages_common_pruner::Limiter;
use databend_storages_common_pruner::RangePruner;
use databend_storages_common_table_meta::meta::BlockMeta;
use databend_storages_common_table_meta::meta::SegmentInfoVersion;
use databend_storages_common_table_meta::readers::segment_reader::deserialize_segment_info;

use super::segment_source::SegmentBytes;
use crate::operations::BlockIndex;
use crate::operations::DeletedSegmentInfo;
use crate::operations::SegmentIndex;

struct BlockPruneSink {
    block_meta_sender: Option<Sender<Arc<BlockMeta>>>,
    schema: TableSchemaRef,
    range_pruner: Arc<dyn RangePruner + Send + Sync>,
    limit_pruner: Arc<dyn Limiter + Send + Sync>,
    internal_column_pruner: Option<Arc<InternalColumnPruner>>,
    inverse_range_index_context: Option<InverseRangeIndexContext>,
}

struct InverseRangeIndexContext {
    whole_block_delete_sender: Sender<(SegmentIndex, BlockIndex)>,
    whole_segment_delete_sender: Sender<DeletedSegmentInfo>,
    inverse_range_index: RangeIndex,
}

#[async_trait]
impl AsyncSink for BlockPruneSink {
    const NAME: &'static str = "BlockPruneSink";

    #[async_backtrace::framed]
    async fn on_finish(&mut self) -> Result<()> {
        drop(self.block_meta_sender.take());
        drop(self.inverse_range_index_context.take());
        Ok(())
    }

    #[unboxed_simple]
    #[async_backtrace::framed]
    async fn consume(&mut self, mut data_block: DataBlock) -> Result<bool> {
        let block_meta_sender = self.block_meta_sender.as_ref().unwrap();
        let segment_bytes = SegmentBytes::downcast_from(data_block.take_meta().unwrap()).unwrap();
        let compact_segment = deserialize_segment_info(
            &SegmentInfoVersion::try_from(segment_bytes.segment_location.1)?,
            &self.schema,
            &segment_bytes.bytes,
        )
        .await?;
        if !self
            .range_pruner
            .should_keep(&compact_segment.summary.col_stats, None)
        {
            return Ok(false);
        }
        if let Some(r) = self.inverse_range_index_context.as_mut() {
            if !r
                .inverse_range_index
                .should_keep(&compact_segment.summary.col_stats, None)
            {
                r.whole_segment_delete_sender
                    .send(DeletedSegmentInfo {
                        index: segment_bytes.segment_index,
                        summary: compact_segment.summary,
                    })
                    .await
                    .map_err(|e| {
                        ErrorCode::Internal(format!("send whole segment delete block error: {}", e))
                    })?;
                return Ok(false);
            }
        }
        let segment_block_metas = compact_segment.block_metas()?;
        for (block_index, block_meta) in segment_block_metas.into_iter().enumerate() {
            if self.limit_pruner.exceeded() {
                break;
            }
            if let Some(p) = self.internal_column_pruner.as_ref() {
                if !p.should_keep(BLOCK_NAME_COL_NAME, &block_meta.location.0) {
                    continue;
                }
            }
            if !self
                .range_pruner
                .should_keep(&block_meta.col_stats, Some(&block_meta.col_metas))
            {
                continue;
            }
            if let Some(r) = self.inverse_range_index_context.as_mut() {
                if !r
                    .inverse_range_index
                    .should_keep(&block_meta.col_stats, None)
                {
                    r.whole_block_delete_sender
                        .send((segment_bytes.segment_index, block_index))
                        .await
                        .map_err(|e| {
                            ErrorCode::Internal(format!(
                                "send whole block delete block error: {}",
                                e
                            ))
                        })?;
                    continue;
                }
            }
            self.limit_pruner.within_limit(block_meta.row_count);
            block_meta_sender
                .send(block_meta)
                .await
                .map_err(|e| ErrorCode::Internal(format!("send block meta error: {}", e)))?;
        }
        Ok(false)
    }
}
