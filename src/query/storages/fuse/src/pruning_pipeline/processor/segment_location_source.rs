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
