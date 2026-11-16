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

use std::collections::BTreeMap;
use std::sync::Arc;

use databend_common_catalog::table::Table;
use databend_common_catalog::table_context::TableContext;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::ColumnId;
use databend_common_expression::TableDataType;
use databend_common_meta_app::schema::TableIndex;
use databend_common_meta_app::schema::TableIndexType;
use databend_common_storages_fuse::io::VectorIndexSearchOptions;
use databend_common_storages_fuse::io::read::VectorIndexIdentity;
use databend_common_storages_fuse::io::read::VectorIndexReader;
use databend_storages_common_index::DistanceType;
use databend_storages_common_io::ReadSettings;
use databend_storages_common_table_meta::meta::BlockMeta;
use databend_storages_common_table_meta::meta::IndexFileLocation;
use databend_storages_common_table_meta::table::VECTOR_INDEX_OPT_DISTANCE;
use futures::future::try_join_all;
use serde::Deserialize;

use crate::data::core::RowRef;
use crate::data::core::TableTarget;
use crate::data::search::BlockHit;
use crate::data::search::RankedHit;
use crate::data::search::Retriever;
use crate::data::search::ScoreOrder;
use crate::sessions::TableContextCachePolicy;
use crate::sessions::TableContextSettings;

/// Nearest neighbor search over the vector index of `column`, like
/// `ORDER BY <distance>_distance(column, query) LIMIT ..`: the HNSW search of each block,
/// refined with the FP16 data of the index as SQL does, by the `vector_refine_factor`
/// and `vector_search_ef` settings. Rows without a vector (NULL) are never returned.
#[derive(Deserialize, Debug, Clone, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct VectorSpec {
    /// The vector column, covered by a vector index of `distance`.
    pub column: String,
    /// The query vector, of the dimension of the column.
    pub query: Vec<f32>,
    pub distance: VectorDistance,
}

/// The distance to rank by: the score of a row, lower first.
#[derive(Deserialize, Debug, Clone, Copy, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum VectorDistance {
    /// `cosine_distance`.
    Cosine,
    /// `l1_distance`.
    L1,
    /// `l2_distance`.
    L2,
}

impl VectorDistance {
    /// The name of the SQL function of the distance.
    pub fn func_name(self) -> &'static str {
        match self {
            VectorDistance::Cosine => "cosine_distance",
            VectorDistance::L1 => "l1_distance",
            VectorDistance::L2 => "l2_distance",
        }
    }

    fn distance_type(self) -> DistanceType {
        match self {
            VectorDistance::Cosine => DistanceType::Dot,
            VectorDistance::L1 => DistanceType::L1,
            VectorDistance::L2 => DistanceType::L2,
        }
    }
}

impl VectorSpec {
    pub(crate) fn validate(&self) -> Result<()> {
        if self.query.is_empty() {
            return Err(ErrorCode::BadArguments(
                "vector search query must not be empty",
            ));
        }
        if self.query.iter().any(|value| !value.is_finite()) {
            return Err(ErrorCode::BadArguments(
                "vector search query must be finite numbers",
            ));
        }
        Ok(())
    }
}

/// The HNSW search of [`VectorSpec`], with the FP16 refinement of the best candidates of
/// all blocks when the index and the settings enable it.
pub struct VectorRetriever {
    reader: VectorIndexReader,
    index_name: String,
    index_version: String,
    /// The rows each block returns: the candidates of the search, times the refine
    /// factor with refinement.
    block_limit: usize,
    refine: bool,
}

impl VectorRetriever {
    pub async fn create(
        target: &TableTarget,
        spec: &VectorSpec,
        candidates: usize,
    ) -> Result<Self> {
        let table = target.table();
        let (column_id, index) = vector_index(table, spec)?;
        let settings = target.ctx.get_settings();
        let options = VectorIndexSearchOptions::new(
            &index.options,
            settings.get_vector_refine_factor()?,
            settings.get_vector_search_ef()?,
        )?;
        // As the candidates of a SQL vector TopN.
        let block_limit = match options.refine_factor {
            Some(factor) => candidates.saturating_mul(factor),
            None => candidates,
        };
        let ctx: Arc<dyn TableContext> = target.ctx.clone();
        let reader = VectorIndexReader::create(
            target.fuse_table().get_operator(),
            ReadSettings::from_ctx(&ctx)?,
            target.ctx.cache_policy(),
            spec.distance.distance_type(),
            spec.query.clone(),
            VectorIndexIdentity {
                name: index.name.clone(),
                version: index.version.clone(),
                column_id,
            },
            options,
        );
        Ok(VectorRetriever {
            reader,
            index_name: index.name.clone(),
            index_version: index.version.clone(),
            block_limit,
            refine: options.refine_factor.is_some(),
        })
    }

    fn index_file(&self, block: &BlockMeta) -> Result<IndexFileLocation> {
        block
            .vector_index_file(&self.index_name, &self.index_version)
            .ok_or_else(|| {
                ErrorCode::StorageUnavailable(format!(
                    "vector index {} file don't exist, need refresh",
                    self.index_name
                ))
            })
    }
}

#[async_trait::async_trait]
impl Retriever for VectorRetriever {
    fn order(&self) -> ScoreOrder {
        ScoreOrder::LowerFirst
    }

    async fn search_block(&self, block: &BlockMeta) -> Result<Vec<BlockHit>> {
        let location = self.index_file(block)?;
        // No later sort key chooses among rows of equal distance: like SQL without one,
        // any of them is a correct result.
        let scores = self
            .reader
            .prune(self.block_limit, false, block.row_count as usize, &location)
            .await?;
        Ok(scores
            .into_iter()
            .map(|score| BlockHit {
                row: score.idx,
                score: score.score,
            })
            .collect())
    }

    fn rerank_candidates(&self) -> Option<usize> {
        self.refine.then_some(self.block_limit)
    }

    /// The FP16 distances of `hits`, read per block like a SQL vector TopN.
    async fn rerank(
        &self,
        blocks: &[Arc<BlockMeta>],
        hits: Vec<RankedHit>,
    ) -> Result<Vec<RankedHit>> {
        let mut rows_by_block: BTreeMap<usize, Vec<u32>> = BTreeMap::new();
        for hit in &hits {
            rows_by_block
                .entry(hit.row.block)
                .or_default()
                .push(hit.row.row);
        }
        let refines = rows_by_block.into_iter().map(|(block, rows)| {
            let meta = blocks[block].clone();
            async move {
                let location = self.index_file(&meta)?;
                let scores = self
                    .reader
                    .refine_scores(meta.row_count as usize, &location, &rows)
                    .await?;
                Ok::<_, ErrorCode>(scores.into_iter().map(move |score| RankedHit {
                    row: RowRef {
                        block,
                        row: score.idx,
                    },
                    score: score.score,
                }))
            }
        });
        Ok(try_join_all(refines).await?.into_iter().flatten().collect())
    }
}

/// The column id of `spec.column` and the vector index of `spec` over it, chosen as the
/// binder chooses the index of `<distance>_distance(column, query)`.
fn vector_index<'a>(table: &'a dyn Table, spec: &VectorSpec) -> Result<(ColumnId, &'a TableIndex)> {
    let schema = table.schema();
    let column = &spec.column;
    let field = schema.field_with_name(column).map_err(|_| {
        ErrorCode::UnknownColumn(format!("table `{}` has no column `{column}`", table.name()))
    })?;
    let TableDataType::Vector(vector_type) = field.data_type().remove_nullable() else {
        return Err(ErrorCode::BadArguments(format!(
            "vector search column `{column}` is not a vector column"
        )));
    };
    let dimension = vector_type.dimension() as usize;
    if dimension != spec.query.len() {
        return Err(ErrorCode::BadArguments(format!(
            "vector search query has {} dimensions, column `{column}` has {dimension}",
            spec.query.len()
        )));
    }
    let column_id = field.column_id();
    let func_name = spec.distance.func_name();
    table
        .get_table_info()
        .meta
        .indexes
        .values()
        .find(|index| {
            index.index_type == TableIndexType::Vector
                && index.column_ids.contains(&column_id)
                && index
                    .options
                    .get(VECTOR_INDEX_OPT_DISTANCE)
                    .is_some_and(|distances| {
                        distances
                            .split(',')
                            .any(|distance| func_name.starts_with(distance))
                    })
        })
        .map(|index| (column_id, index))
        .ok_or_else(|| {
            ErrorCode::BadArguments(format!(
                "column `{column}` has no vector index of distance `{func_name}`"
            ))
        })
}
