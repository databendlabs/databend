//  Copyright 2022 Datafuse Labs.
//
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.

use std::sync::Arc;

use common_catalog::plan::Expression;
use common_datavalues::DataSchemaRef;
use common_datavalues::DataTypeImpl;
use common_exception::ErrorCode;
use common_exception::Result;
use common_storages_table_meta::meta::BlockMeta;
use common_storages_table_meta::meta::ColumnStatistics;

use crate::pruning::BlockIndex;

pub struct TopNPrunner {
    schema: DataSchemaRef,
    sort: Vec<(Expression, bool, bool)>,
    limit: usize,
}

impl TopNPrunner {
    pub fn new(schema: DataSchemaRef, sort: Vec<(Expression, bool, bool)>, limit: usize) -> Self {
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
        metas: Vec<(BlockIndex, Arc<BlockMeta>)>,
    ) -> Result<Vec<(BlockIndex, Arc<BlockMeta>)>> {
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
        let column = if let Expression::IndexedVariable { name, .. } = sort {
            name
        } else {
            return Ok(metas);
        };

        let sort_idx = if let Ok(index) = self.schema.index_of(column.as_str()) {
            index as u32
        } else {
            return Ok(metas);
        };

        // String Type min/max is truncated
        if matches!(
            self.schema.field(sort_idx as usize).data_type(),
            DataTypeImpl::String(_)
        ) {
            return Ok(metas);
        }

        let mut id_stats = metas
            .iter()
            .map(|(id, meta)| {
                let stat = meta.col_stats.get(&sort_idx).ok_or_else(|| {
                    ErrorCode::UnknownException(format!(
                        "Unable to get the colStats by ColumnId: {}",
                        sort_idx
                    ))
                })?;
                Ok((*id, stat.clone(), meta.clone()))
            })
            .collect::<Result<Vec<(BlockIndex, ColumnStatistics, Arc<BlockMeta>)>>>()?;

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
            .map(|s| (s.0, s.2.clone()))
            .take(self.limit)
            .collect())
    }
}
