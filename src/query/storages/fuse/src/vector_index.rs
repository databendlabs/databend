use std::sync::Arc;

use common_catalog::plan::Projection;
use common_catalog::table_context::TableContext;
use common_exception::ErrorCode;
use common_exception::Result;
use common_expression::types::array::ArrayColumn;
use common_expression::types::Float32Type;
use faiss::index::io::serialize;
use faiss::index_factory;
use faiss::Index;
use faiss::MetricType;
use storages_common_cache::LoadParams;
use storages_common_table_meta::meta::BlockMeta;
use storages_common_table_meta::meta::SegmentInfo;
use storages_common_table_meta::meta::TableSnapshot;
use storages_common_table_meta::meta::Versioned;
use uuid::Uuid;

use crate::io::MetaReaders;
use crate::io::ReadSettings;
use crate::io::SegmentWriter;
use crate::io::TableMetaLocationGenerator;
use crate::FuseTable;

impl FuseTable {
    pub async fn create_vector_index(
        &self,
        ctx: Arc<dyn TableContext>,
        column_idx: usize,
        nlists: u64,
    ) -> Result<()> {
        let projection = Projection::Columns(vec![column_idx]);
        let read_settings = ReadSettings::from_ctx(&ctx)?;
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
        let mut new_segments = Vec::with_capacity(snapshot.segments.len());
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
            let mut new_block_metas = Vec::with_capacity(segment_info.blocks.len());
            let segment_writer =
                SegmentWriter::new(&self.get_operator_ref(), &self.meta_location_generator());
            for block_meta in &segment_info.blocks {
                let block_reader =
                    self.create_block_reader(projection.clone(), false, ctx.clone())?;
                let block = block_reader
                    .read_by_meta(&read_settings, block_meta, &self.storage_format)
                    .await?;
                debug_assert_eq!(block.columns().len(), 1);
                let column = block.columns()[0]
                    .value
                    .as_column()
                    .and_then(|column| column.as_array())
                    .ok_or(ErrorCode::StorageOther(
                        "vector index can only be created on an array column",
                    ))?;
                let column: ArrayColumn<Float32Type> = column.try_downcast().unwrap();
                let len = column.len();
                let raw_data: Vec<f32> = column
                    .underlying_column()
                    .as_slice()
                    .iter()
                    .map(|x| x.into_inner())
                    .collect();
                let dimension = raw_data.len() / len;
                let desp = format!("IVF{},Flat", nlists);
                let mut index = index_factory(dimension as u32, desp, MetricType::L2).unwrap();
                index.train(&raw_data).unwrap();
                index.add(&raw_data).unwrap();
                let index_bin = serialize(&index).unwrap();
                let (location, _) = self.meta_location_generator().gen_block_location();
                self.get_operator().write(&location.0, index_bin).await?;
                let new_block_meta = BlockMeta {
                    vector_index_location: Some(location),
                    ..block_meta.as_ref().clone()
                };
                new_block_metas.push(Arc::new(new_block_meta));
            }
            let new_segment = SegmentInfo {
                blocks: new_block_metas,
                format_version: segment_info.format_version,
                summary: segment_info.summary.clone(),
            };
            let location = segment_writer.write_segment(new_segment).await?;
            new_segments.push(location);
        }
        let new_snapshot = TableSnapshot::new(
            Uuid::new_v4(),
            &snapshot.timestamp,
            Some((snapshot.snapshot_id, snapshot.format_version)),
            snapshot.schema.clone(),
            snapshot.summary.clone(),
            new_segments,
            snapshot.cluster_key_meta.clone(),
            snapshot.table_statistics_location.clone(),
        );
        FuseTable::commit_to_meta_server(
            ctx.as_ref(),
            &self.table_info,
            &self.meta_location_generator,
            new_snapshot,
            None,
            &None,
            self.get_operator_ref(),
        )
        .await?;
        Ok(())
    }
}
