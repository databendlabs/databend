use std::sync::Arc;

use common_catalog::plan::Projection;
use common_catalog::table::Table;
use common_catalog::table_context::TableContext;
use common_exception::ErrorCode;
use common_exception::Result;
use storages_common_cache::LoadParams;
use storages_common_table_meta::meta::SegmentInfo;
use storages_common_table_meta::meta::Versioned;

use crate::io::MetaReaders;
use crate::io::TableMetaLocationGenerator;
use crate::FuseTable;

impl FuseTable {
    pub async fn create_vector_index(
        &self,
        ctx: Arc<dyn TableContext>,
        column_idx: usize,
    ) -> Result<()> {
        let projection = Projection::Columns(vec![column_idx]);
        let snapshot_location = self.snapshot_loc().await?.ok_or(ErrorCode::StorageOther(
            "try to create vector index on a table without snapshot",
        ))?;
        let snapshot_reader = MetaReaders::table_snapshot_reader(self.get_operator());
        let ver = TableMetaLocationGenerator::snapshot_version(snapshot_location.as_str());
        let params = LoadParams {
            location: snapshot_location,
            len_hint: None,
            ver,
            put_cache: true,
        };
        let snapshot = snapshot_reader.read(&params).await?;
        let schema = Arc::new(snapshot.schema.clone());
        for (seg_loc, _) in &snapshot.segments {
            let segment_reader =
                MetaReaders::segment_info_reader(self.get_operator(), schema.clone());
            let params = LoadParams {
                location: seg_loc.clone(),
                len_hint: None,
                ver: SegmentInfo::VERSION,
                put_cache: true,
            };
            let segment_info = segment_reader.read(&params).await?;
            for block_meta in &segment_info.blocks {
                let (block_loc, _) = &block_meta.location;
                let block_reader =
                    self.create_block_reader(projection.clone(), false, ctx.clone())?;
            }
        }
        Ok(())
    }
}
