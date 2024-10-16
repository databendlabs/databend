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

use databend_common_catalog::table_context::TableContext;
use databend_common_exception::ErrorCode;
use databend_common_expression::DataBlock;
use databend_common_expression::SEGMENT_NAME_COL_NAME;
use databend_common_pipeline_core::processors::OutputPort;
use databend_common_pipeline_core::processors::ProcessorPtr;
use databend_common_pipeline_sources::SyncSource;
use databend_common_pipeline_sources::SyncSourcer;
use databend_storages_common_pruner::InternalColumnPruner;

use crate::pruning_pipeline::meta_info::SegmentLocationMeta;
use crate::FuseLazyPartInfo;
use crate::SegmentLocation;

/// ReadSegmentSource Workflow:
/// 1. Retrieve the FuseLazyPartInfo
/// 2. Apply internal column pruning
pub struct ReadSegmentSource {
    ctx: Arc<dyn TableContext>,
    internal_column_pruner: Option<Arc<InternalColumnPruner>>,
    snapshot_location: Option<String>,
}

impl ReadSegmentSource {
    pub fn create(
        ctx: Arc<dyn TableContext>,
        internal_column_pruner: Option<Arc<InternalColumnPruner>>,
        snapshot_location: Option<String>,
        output: Arc<OutputPort>,
    ) -> databend_common_exception::Result<ProcessorPtr> {
        SyncSourcer::create(ctx.clone(), output, ReadSegmentSource {
            ctx,
            internal_column_pruner,
            snapshot_location,
        })
    }
}

impl SyncSource for ReadSegmentSource {
    const NAME: &'static str = "ReadSegmentSource";

    fn generate(&mut self) -> databend_common_exception::Result<Option<DataBlock>> {
        if let Some(ptr) = self.ctx.get_partition() {
            if let Some(part) = ptr.as_any().downcast_ref::<FuseLazyPartInfo>() {
                if let Some(pruner) = &self.internal_column_pruner {
                    if !pruner.should_keep(SEGMENT_NAME_COL_NAME, &part.segment_location.0) {
                        return Ok(None);
                    }
                }
                Ok(Some(DataBlock::empty_with_meta(
                    SegmentLocationMeta::create(SegmentLocation {
                        segment_idx: part.segment_index,
                        location: part.segment_location.clone(),
                        snapshot_loc: self.snapshot_location.clone(),
                    }),
                )))
            } else {
                Err(ErrorCode::Internal(
                    "ReadSegmentSource failed downcast partition to FuseLazyPartInfo",
                ))
            }
        } else {
            Ok(None)
        }
    }
}
