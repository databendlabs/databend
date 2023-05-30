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

use std::collections::HashMap;
use std::sync::Arc;

use common_catalog::plan::Projection;
use common_catalog::table_context::TableContext;
use common_exception::ErrorCode;
use common_exception::Result;
use common_expression::types::array::ArrayColumn;
use common_expression::types::Float32Type;
use common_expression::DataBlock;
use common_vector::index::normalize;
use common_vector::index::IvfFlatIndex;
use common_vector::index::MetricType;
use common_vector::index::VectorIndex;
use faiss::index::io::deserialize;
use faiss::index::io::serialize;
use faiss::index::SearchResult;
use faiss::index_factory;
use faiss::Idx;
use faiss::Index;
use storages_common_table_meta::meta::compress;
use storages_common_table_meta::meta::decompress;
use storages_common_table_meta::meta::BlockMeta;
use storages_common_table_meta::meta::MetaCompression;

use crate::io::BlockReader;
use crate::io::ReadSettings;
use crate::FuseTable;

const POST_FIX_IVF: &str = ".ivf";

const POST_FIX_COSINE: &str = ".cosine";

const COMPRESSION_TYPE: MetaCompression = MetaCompression::Zstd;

impl FuseTable {
    /// read all blocks of the vector column and build a vector index on each block,
    ///
    /// then save the index to the storage, with post fix which is defined in `POST_FIX_*`
    pub async fn create_vector_index(
        &self,
        ctx: Arc<dyn TableContext>,
        column_idx: usize,
        vector_index: &VectorIndex,
        metric_type: &MetricType,
    ) -> Result<()> {
        let block_reader =
            self.create_block_reader(Projection::Columns(vec![column_idx]), false, ctx.clone())?;
        let read_settings = ReadSettings::from_ctx(&ctx)?;
        for ref block_meta in self.collect_block_metas().await? {
            let block = block_reader
                .read_by_meta(&read_settings, block_meta, &self.storage_format)
                .await?;
            let column: ArrayColumn<Float32Type> = block.columns()[0]
                .value
                .as_column()
                .and_then(|column| column.as_array())
                .and_then(|column| column.try_downcast())
                .ok_or(ErrorCode::StorageOther(
                    "vector index can only be created on an array(float32) column",
                ))?;
            let mut raw_data: Vec<f32> = column
                .underlying_column()
                .as_slice()
                .iter()
                .map(|x| x.into_inner())
                .collect();

            // TODO(SKy): this assumes our array(float32) type is fixed length, we should add a new type for fixed length array
            let dimension = raw_data.len() / column.len();

            let (desp, index_location) = match vector_index {
                VectorIndex::IvfFlat(IvfFlatIndex { nlists }) => {
                    let desp = format!("IVF{},Flat", nlists);
                    let index_location = block_meta.location.0.clone() + POST_FIX_IVF;
                    (desp, index_location)
                }
            };
            let (index_location, metric) = match metric_type {
                MetricType::Cosine => {
                    normalize(&mut raw_data);
                    (index_location + POST_FIX_COSINE, faiss::MetricType::L2)
                }
            };

            let mut index = index_factory(dimension as u32, desp, metric)?;
            index.train(&raw_data)?;
            index.add(&raw_data)?;
            // TODO(sky): we should add an indendpendent sql for this, like: SET ivfflat.nprobe = 10;
            index.set_nprobe(70);
            // TODO(sky): provide a safe interface for serialize in faiss-rs
            let index_bin = unsafe { serialize(&index)? };
            let index_bin = compress(&COMPRESSION_TYPE, index_bin)?;
            self.get_operator()
                .write(&index_location, index_bin)
                .await?;
        }
        Ok(())
    }

    pub async fn read_by_vector_index(
        &self,
        block_reader: Arc<BlockReader>,
        ctx: Arc<dyn TableContext>,
        limit: usize,
        target: &mut [f32],
        index_type: &VectorIndex,
        metric_type: &MetricType,
    ) -> Result<Option<DataBlock>> {
        let read_settings = ReadSettings::from_ctx(&ctx)?;
        let block_metas = self.collect_block_metas().await?;
        let post_fix = match index_type {
            VectorIndex::IvfFlat(_) => POST_FIX_IVF.to_string(),
        };
        let pos_fix = match metric_type {
            MetricType::Cosine => {
                normalize(target);
                post_fix + POST_FIX_COSINE
            }
        };

        // 1. get knn of each block
        let mut result_per_block: Vec<(Vec<Idx>, Vec<f32>, Arc<BlockMeta>)> =
            Vec::with_capacity(block_metas.len());
        for ref block_meta in block_metas {
            let index_location = block_meta.location.0.clone() + &pos_fix;
            let index_bin = self.get_operator().read(&index_location).await?;
            let index_bin = decompress(&COMPRESSION_TYPE, index_bin)?;
            let mut index = deserialize(&index_bin).unwrap();
            let SearchResult { distances, labels } = index.search(target, limit).unwrap();
            result_per_block.push((labels, distances, block_meta.clone()));
        }
        // 2. merge knn results of all blocks
        let merged_results = merge_results(&result_per_block, limit);
        // 3. merge io requests
        let mut merged_io_results = HashMap::new();
        for (rank, (row_id, block_meta)) in merged_results.iter().enumerate() {
            let io_result = merged_io_results
                .entry(block_meta.clone())
                .or_insert(Vec::new());
            io_result.push((row_id, rank));
        }
        // 4. read rows
        let mut result_rows = vec![vec![]; limit];
        for (block_meta, rows) in merged_io_results {
            let block = block_reader
                .read_by_meta(&read_settings, &block_meta, &self.storage_format)
                .await?;
            for (row_id, rank) in rows {
                let row_id = row_id.get().unwrap() as usize;
                result_rows[rank] = unsafe { block.get_row_unchecked(row_id) };
            }
        }
        // 5. convert rows to block
        Ok(Some(DataBlock::from_rows(result_rows)))
    }
}

fn merge_results(
    results: &[(Vec<Idx>, Vec<f32>, Arc<BlockMeta>)],
    k: usize,
) -> Vec<(Idx, Arc<BlockMeta>)> {
    let mut topk = Vec::with_capacity(k);
    let mut start = vec![0; results.len()];
    for _ in 0..k {
        let mut min = 0;
        for i in 0..results.len() {
            if results[i].1[start[i]] < results[min].1[start[i]] {
                min = i;
            }
        }
        topk.push((results[min].0[start[min]], results[min].2.clone()));
        start[min] += 1;
    }
    topk
}
