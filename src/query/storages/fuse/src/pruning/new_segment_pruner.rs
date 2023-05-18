use std::sync::Arc;

use common_exception::Result;
use common_expression::TableSchemaRef;
use common_expression::SEGMENT_NAME_COL_NAME;
use storages_common_cache::LoadParams;
use storages_common_table_meta::meta::CompactSegmentInfo;

use crate::io::MetaReaders;
use crate::metrics::metrics_inc_bytes_segment_range_pruning_after;
use crate::metrics::metrics_inc_bytes_segment_range_pruning_before;
use crate::metrics::metrics_inc_segments_range_pruning_after;
use crate::metrics::metrics_inc_segments_range_pruning_before;
use crate::pruning::PruningContext;
use crate::pruning::SegmentLocation;

pub struct NewSegmentPruner {
    pub pruning_ctx: Arc<PruningContext>,
    pub table_schema: TableSchemaRef,
}

impl NewSegmentPruner {
    pub fn create(
        pruning_ctx: Arc<PruningContext>,
        table_schema: TableSchemaRef,
    ) -> Result<Arc<NewSegmentPruner>> {
        Ok(Arc::new(NewSegmentPruner {
            pruning_ctx,
            table_schema,
        }))
    }

    #[async_backtrace::framed]
    pub async fn pruning(
        &self,
        segment_locs: Vec<SegmentLocation>,
    ) -> Result<Vec<(SegmentLocation, Arc<CompactSegmentInfo>)>> {
        if segment_locs.is_empty() {
            return Ok(vec![]);
        }

        let mut res = Vec::with_capacity(segment_locs.len());

        let pruning_stats = self.pruning_ctx.pruning_stats.clone();
        let range_pruner = self.pruning_ctx.range_pruner.clone();

        for segment_location in segment_locs {
            let info = self.read_segment_info(&segment_location).await?;

            let total_bytes = info.summary.uncompressed_byte_size;
            // Perf.
            {
                metrics_inc_segments_range_pruning_before(1);
                metrics_inc_bytes_segment_range_pruning_before(total_bytes);

                pruning_stats.set_segments_range_pruning_before(1);
            }

            if range_pruner.should_keep(&info.summary.col_stats) {
                // Perf.
                {
                    metrics_inc_segments_range_pruning_after(1);
                    metrics_inc_bytes_segment_range_pruning_after(total_bytes);

                    pruning_stats.set_segments_range_pruning_after(1);
                }

                res.push((segment_location, info.clone()));
            }
        }

        Ok(res)
    }

    async fn read_segment_info(
        &self,
        segment_location: &SegmentLocation,
    ) -> Result<Arc<CompactSegmentInfo>> {
        let dal = self.pruning_ctx.dal.clone();
        let table_schema = self.table_schema.clone();

        // Keep in mind that segment_info_read must need a schema
        let segment_reader = MetaReaders::segment_info_reader(dal, table_schema);
        let (location, ver) = segment_location.location.clone();
        let load_params = LoadParams {
            location,
            len_hint: None,
            ver,
            put_cache: true,
        };

        segment_reader.read(&load_params).await
    }
}
