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

use std::any::Any;
use std::ops::Range;
use std::sync::Arc;

use common_exception::ErrorCode;
use common_exception::Result;
use common_expression::BlockMetaInfo;
use common_expression::BlockMetaInfoDowncast;
use common_expression::BlockMetaInfoPtr;
use common_expression::RemoteExpr;
use common_expression::TableDataType;
use common_expression::TableSchemaRef;
use storages_common_table_meta::meta::BlockMeta;
use storages_common_table_meta::meta::ColumnStatistics;

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, Default, PartialEq, Eq)]
pub struct BlockMetaIndex {
    // {segment|block}_id is used in `InternalColumnMeta` to generate internal column data,
    // where older data has smaller id, but {segment|block}_idx is opposite,
    // so {segment|block}_id = {segment|block}_count - {segment|block}_idx - 1
    pub segment_idx: usize,
    pub block_idx: usize,
    pub range: Option<Range<usize>>,
    /// The page size of the block.
    /// If the block format is parquet, its page size is the rows count of the block.
    /// If the block format is native, its page size is the rows count of each page. (The rows count of the last page may be smaller than the page size.)
    pub page_size: usize,
    pub block_id: usize,
    pub block_location: String,
    pub segment_id: usize,
    pub segment_location: String,
    pub snapshot_location: Option<String>,
}

#[typetag::serde(name = "block_meta_index")]
impl BlockMetaInfo for BlockMetaIndex {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn equals(&self, info: &Box<dyn BlockMetaInfo>) -> bool {
        match BlockMetaIndex::downcast_ref_from(info) {
            None => false,
            Some(other) => self == other,
        }
    }

    fn clone_self(&self) -> Box<dyn BlockMetaInfo> {
        Box::new(self.clone())
    }
}

impl BlockMetaIndex {
    pub fn from_meta(info: &BlockMetaInfoPtr) -> Result<&BlockMetaIndex> {
        match BlockMetaIndex::downcast_ref_from(info) {
            Some(part_ref) => Ok(part_ref),
            None => Err(ErrorCode::Internal(
                "Cannot downcast from BlockMetaInfo to BlockMetaIndex.",
            )),
        }
    }
}

/// TopN pruner.
/// Pruning for order by x limit N.
pub struct TopNPrunner {
    schema: TableSchemaRef,
    sort: Vec<(RemoteExpr<String>, bool, bool)>,
    limit: usize,
}

impl TopNPrunner {
    pub fn create(
        schema: TableSchemaRef,
        sort: Vec<(RemoteExpr<String>, bool, bool)>,
        limit: usize,
    ) -> Self {
        Self {
            schema,
            sort,
            limit,
        }
    }
}

impl TopNPrunner {
    pub fn prune(
        &self,
        metas: Vec<(BlockMetaIndex, Arc<BlockMeta>)>,
    ) -> Result<Vec<(BlockMetaIndex, Arc<BlockMeta>)>> {
        if self.sort.len() != 1 {
            return Ok(metas);
        }

        if self.limit >= metas.len() {
            return Ok(metas);
        }

        let (sort, asc, nulls_first) = &self.sort[0];
        // Currently, we only support topn on single-column sort.
        // TODO: support monadic + multi expression + order by cluster key sort.

        // Currently, we only support topn on single-column sort.
        // TODO: support monadic + multi expression + order by cluster key sort.
        let column = if let RemoteExpr::ColumnRef { id, .. } = sort {
            id
        } else {
            return Ok(metas);
        };

        let sort_column_id = if let Ok(index) = self.schema.column_id_of(column.as_str()) {
            index
        } else {
            return Ok(metas);
        };

        // String Type min/max is truncated
        if matches!(
            self.schema.field_with_name(column)?.data_type(),
            TableDataType::String
        ) {
            return Ok(metas);
        }

        let mut id_stats = metas
            .iter()
            .map(|(id, meta)| {
                let stat = meta.col_stats.get(&sort_column_id).ok_or_else(|| {
                    ErrorCode::UnknownException(format!(
                        "Unable to get the colStats by ColumnId: {}",
                        sort_column_id
                    ))
                })?;
                Ok((id.clone(), stat.clone(), meta.clone()))
            })
            .collect::<Result<Vec<(BlockMetaIndex, ColumnStatistics, Arc<BlockMeta>)>>>()?;

        id_stats.sort_by(|a, b| {
            if a.1.null_count + b.1.null_count != 0 && *nulls_first {
                return a.1.null_count.cmp(&b.1.null_count).reverse();
            }
            // no nulls
            if *asc {
                a.1.min.cmp(&b.1.min)
            } else {
                a.1.max.cmp(&b.1.max).reverse()
            }
        });
        Ok(id_stats
            .iter()
            .map(|s| (s.0.clone(), s.2.clone()))
            .take(self.limit)
            .collect())
    }
}
