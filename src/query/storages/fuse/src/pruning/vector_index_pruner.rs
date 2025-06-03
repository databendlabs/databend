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

use std::cmp::Ordering;
use std::collections::HashSet;
use std::sync::Arc;

use databend_common_catalog::plan::Filters;
use databend_common_catalog::plan::VectorIndexInfo;
use databend_common_catalog::table_context::TableContext;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::types::F32;
use databend_common_expression::RemoteExpr;
use databend_common_expression::TableSchemaRef;
use databend_common_expression::VECTOR_SCORE_COL_NAME;
use databend_storages_common_index::DistanceType;
use databend_storages_common_index::FixedLengthPriorityQueue;
use databend_storages_common_io::ReadSettings;
use databend_storages_common_pruner::BlockMetaIndex;
use databend_storages_common_table_meta::meta::BlockMeta;
use opendal::Operator;

use crate::io::read::VectorIndexReader;

/// Vector index pruner.
#[derive(Clone)]
pub struct VectorIndexPruner {
    ctx: Arc<dyn TableContext>,
    operator: Operator,
    _schema: TableSchemaRef,
    vector_index: VectorIndexInfo,
    filters: Option<Filters>,
    sort: Vec<(RemoteExpr<String>, bool, bool)>,
    limit: Option<usize>,
}

impl VectorIndexPruner {
    pub fn create(
        ctx: Arc<dyn TableContext>,
        operator: Operator,
        schema: TableSchemaRef,
        vector_index: VectorIndexInfo,
        filters: Option<Filters>,
        sort: Vec<(RemoteExpr<String>, bool, bool)>,
        limit: Option<usize>,
    ) -> Result<Self> {
        Ok(Self {
            ctx,
            operator,
            _schema: schema,
            vector_index,
            filters,
            sort,
            limit,
        })
    }
}

impl VectorIndexPruner {
    pub async fn prune(
        &self,
        metas: Vec<(BlockMetaIndex, Arc<BlockMeta>)>,
    ) -> Result<Vec<(BlockMetaIndex, Arc<BlockMeta>)>> {
        let settings = ReadSettings::from_ctx(&self.ctx)?;
        let distance_type = match self.vector_index.func_name.as_str() {
            "cosine_distance" => DistanceType::Dot,
            "l1_distance" => DistanceType::L1,
            "l2_distance" => DistanceType::L2,
            _ => unreachable!(),
        };
        let columns = vec![
            format!(
                "{}-{}_graph_links",
                self.vector_index.column_id, distance_type
            ),
            format!(
                "{}-{}_graph_data",
                self.vector_index.column_id, distance_type
            ),
            format!(
                "{}-{}_encoded_u8_meta",
                self.vector_index.column_id, distance_type
            ),
            format!(
                "{}-{}_encoded_u8_data",
                self.vector_index.column_id, distance_type
            ),
        ];

        let query_values = unsafe {
            std::mem::transmute::<Vec<F32>, Vec<f32>>(self.vector_index.query_values.clone())
        };

        let vector_reader = VectorIndexReader::create(
            self.operator.clone(),
            settings,
            distance_type,
            columns,
            query_values,
        );

        // @TODO support filters
        if self.filters.is_none() && !self.sort.is_empty() && self.limit.is_some() {
            let (sort, asc, _nulls_first) = &self.sort[0];
            if let RemoteExpr::ColumnRef { id, .. } = sort {
                if id == VECTOR_SCORE_COL_NAME && *asc {
                    let limit = self.limit.unwrap();
                    return self
                        .vector_index_topn_prune(vector_reader, limit, metas)
                        .await;
                }
            }
        }

        self.vector_index_prune(vector_reader, metas).await
    }

    async fn vector_index_topn_prune(
        &self,
        vector_reader: VectorIndexReader,
        limit: usize,
        metas: Vec<(BlockMetaIndex, Arc<BlockMeta>)>,
    ) -> Result<Vec<(BlockMetaIndex, Arc<BlockMeta>)>> {
        let mut top_queue = FixedLengthPriorityQueue::new(limit);

        for (index, (_, block_meta)) in metas.iter().enumerate() {
            let Some(location) = block_meta.vector_index_location.clone() else {
                return Err(ErrorCode::StorageUnavailable(format!(
                    "vector index {} file don't exist, need refresh",
                    self.vector_index.index_name
                )));
            };

            let row_count = block_meta.row_count as usize;
            let score_offsets = vector_reader.prune(limit, row_count, &location.0).await?;

            for score_offset in score_offsets {
                let vector_score = VectorScore {
                    index,
                    row_idx: score_offset.idx,
                    score: F32::from(score_offset.score),
                };
                top_queue.push(vector_score);
            }
        }
        let top_scores = top_queue.into_sorted_vec();
        let top_indexes: HashSet<usize> = top_scores.iter().map(|s| s.index).collect();

        let mut pruned_metas = Vec::with_capacity(top_indexes.len());
        for (index, (mut block_meta_index, block_meta)) in metas.into_iter().enumerate() {
            if !top_indexes.contains(&index) {
                continue;
            }
            let mut vector_scores = Vec::new();
            for top_score in &top_scores {
                if top_score.index == index {
                    vector_scores.push((top_score.row_idx as usize, top_score.score));
                }
            }
            block_meta_index.vector_scores = Some(vector_scores);
            pruned_metas.push((block_meta_index, block_meta));
        }

        Ok(pruned_metas)
    }

    async fn vector_index_prune(
        &self,
        vector_reader: VectorIndexReader,
        metas: Vec<(BlockMetaIndex, Arc<BlockMeta>)>,
    ) -> Result<Vec<(BlockMetaIndex, Arc<BlockMeta>)>> {
        // can't use vector index topn to prune, only generate vector scores.
        let mut new_metas = Vec::with_capacity(metas.len());
        for (mut block_meta_index, block_meta) in metas.into_iter() {
            let Some(location) = block_meta.vector_index_location.clone() else {
                return Err(ErrorCode::StorageUnavailable(format!(
                    "vector index {} file don't exist, need refresh",
                    self.vector_index.index_name
                )));
            };

            let row_count = block_meta.row_count as usize;
            // use row_count as limit to generate scores for all rows.
            let score_offsets = vector_reader
                .generate_scores(row_count, &location.0)
                .await?;

            let mut vector_scores = Vec::with_capacity(row_count);
            for score_offset in &score_offsets {
                vector_scores.push((score_offset.idx as usize, F32::from(score_offset.score)));
            }
            block_meta_index.vector_scores = Some(vector_scores);
            new_metas.push((block_meta_index, block_meta));
        }

        Ok(new_metas)
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct VectorScore {
    index: usize,
    row_idx: u32,
    score: F32,
}

impl Ord for VectorScore {
    fn cmp(&self, other: &Self) -> Ordering {
        // reverse order to keep lower score.
        other.score.cmp(&self.score)
    }
}

impl PartialOrd for VectorScore {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}
