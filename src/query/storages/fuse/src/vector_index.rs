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

use std::collections::BinaryHeap;
use std::sync::Arc;

use common_arrow::arrow::compute::limit;
use common_catalog::plan::Projection;
use common_catalog::table_context::TableContext;
use common_exception::ErrorCode;
use common_exception::Result;
use common_expression::types::array::ArrayColumn;
use common_expression::types::Float32Type;
use common_expression::TopKSorter;
use faiss::index::io::deserialize;
use faiss::index::io::serialize;
use faiss::index::SearchResult;
use faiss::index_factory;
use faiss::Idx;
use faiss::Index;
use faiss::MetricType;
use storages_common_cache::LoadParams;
use storages_common_table_meta::meta::BlockMeta;
use storages_common_table_meta::meta::SegmentInfo;
use storages_common_table_meta::meta::Versioned;

use crate::io::MetaReaders;
use crate::io::ReadSettings;
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
            let block_metas = segment_info.block_metas()?;
            for block_meta in &block_metas {
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
                let index_location = block_meta.location.0.clone() + "_ivf.index";
                self.get_operator()
                    .write(&index_location, index_bin)
                    .await?;
            }
        }
        Ok(())
    }

    pub async fn read_by_vector_index(
        &self,
        projections: usize,
        ctx: Arc<dyn TableContext>,
        limit: usize,
    ) -> Result<()> {
        let projection = Projection::Columns(vec![projections]);
        let read_settings = ReadSettings::from_ctx(&ctx)?;
        let snapshot_location = self.snapshot_loc().await?;
        if snapshot_location.is_none() {
            return Ok(());
        }
        let snapshot_location = snapshot_location.unwrap();
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
        let mut search_results_snapshot = Vec::new();
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
            let block_metas = segment_info.block_metas()?;
            let mut search_results_segment = Vec::new();
            for block_meta in &block_metas {
                let index_location = block_meta.location.0.clone() + "_ivf.index";
                let index_bin = self.get_operator().read(&index_location).await?;
                let mut index = deserialize(&index_bin).unwrap();
                let SearchResult {
                    distances: _,
                    labels,
                } = index.search(&[0.0f32; 128], limit).unwrap();
                search_results_segment.push((labels, block_meta.clone()));
            }
            search_results_snapshot.extend(search_results_segment);
        }
        let merged_search_results = merge_search_results(search_results_snapshot, limit);
        let block_reader = self.create_block_reader(projection.clone(), false, ctx.clone())?;
        for (idx, block_meta) in merged_search_results {
            let block = block_reader
                .read_by_meta(&read_settings, &block_meta, &self.storage_format)
                .await?;
        }
        Ok(())
    }
}

fn merge_search_results(
    search_results: Vec<(Vec<Idx>, Arc<BlockMeta>)>,
    limit: usize,
) -> Vec<(Idx, Arc<BlockMeta>)> {
    todo!()
}
