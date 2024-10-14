use std::sync::Arc;

use databend_common_exception::ErrorCode;
use databend_common_expression::BlockMetaInfoDowncast;
use databend_common_expression::DataBlock;
use databend_common_expression::TableSchemaRef;
use databend_common_pipeline_core::processors::InputPort;
use databend_common_pipeline_core::processors::OutputPort;
use databend_common_pipeline_core::processors::ProcessorPtr;
use databend_common_pipeline_transforms::processors::AsyncAccumulatingTransform;
use databend_common_pipeline_transforms::processors::AsyncAccumulatingTransformer;
use databend_storages_common_pruner::RangePruner;
use opendal::Operator;

use crate::io::SegmentsIO;
use crate::pruning_pipeline::meta_info::CompactSegmentMeta;
use crate::pruning_pipeline::meta_info::SegmentLocationMeta;

/// CompactReadTransform Workflow:
/// 1. Read the compact segment from the location (Async)
/// 2. Prune the segment with the range pruner
pub struct CompactReadTransform {
    dal: Operator,
    table_schema: TableSchemaRef,
    range_pruner: Arc<dyn RangePruner + Send + Sync>,
}

impl CompactReadTransform {
    pub fn create(
        dal: Operator,
        table_schema: TableSchemaRef,
        range_pruner: Arc<dyn RangePruner + Send + Sync>,
        input: Arc<InputPort>,
        output: Arc<OutputPort>,
    ) -> databend_common_exception::Result<ProcessorPtr> {
        Ok(ProcessorPtr::create(AsyncAccumulatingTransformer::create(
            input,
            output,
            CompactReadTransform {
                dal,
                table_schema,
                range_pruner,
            },
        )))
    }
}

#[async_trait::async_trait]
impl AsyncAccumulatingTransform for CompactReadTransform {
    const NAME: &'static str = "CompactReadTransform";

    #[async_backtrace::framed]
    async fn transform(
        &mut self,
        data: DataBlock,
    ) -> databend_common_exception::Result<Option<DataBlock>> {
        if let Some(ptr) = data.get_meta() {
            if let Some(meta) = SegmentLocationMeta::downcast_from(ptr.clone()) {
                let info = SegmentsIO::read_compact_segment(
                    self.dal.clone(),
                    meta.segment_location.location.clone(),
                    self.table_schema.clone(),
                    true,
                )
                .await?;

                if !self.range_pruner.should_keep(&info.summary.col_stats, None) {
                    return Ok(None);
                };
                return Ok(Some(DataBlock::empty_with_meta(
                    CompactSegmentMeta::create(info, meta.segment_location),
                )));
            }
        }

        Err(ErrorCode::Internal(
            "Cannot downcast meta to SegmentLocationMeta",
        ))
    }
}
