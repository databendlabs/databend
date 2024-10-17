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

use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::ColumnId;
use databend_common_expression::RemoteExpr;
use databend_common_expression::TableDataType;
use databend_common_expression::TableSchemaRef;
use databend_storages_common_table_meta::meta::BlockMeta;
use databend_storages_common_table_meta::meta::ColumnStatistics;

use crate::BlockMetaIndex;

pub type PruneResult = Vec<(
    BlockMetaIndex,
    Arc<BlockMeta>,
    Option<HashMap<ColumnId, ColumnStatistics>>,
)>;

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
    pub fn prune(&self, metas: PruneResult) -> Result<PruneResult> {
        if self.sort.len() != 1 {
            return Ok(metas);
        }

        if self.limit >= metas.len() {
            return Ok(metas);
        }

        let (sort, asc, nulls_first) = &self.sort[0];
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
            .into_iter()
            .map(|(id, meta, col_stats)| {
                let stat = col_stats
                    .as_ref()
                    .and_then(|col_stats| col_stats.get(&sort_column_id));
                let stat = match stat {
                    Some(stat) => stat,
                    None => meta.col_stats.get(&sort_column_id).ok_or_else(|| {
                        ErrorCode::UnknownException(format!(
                            "Unable to get the colStats by ColumnId: {}",
                            sort_column_id
                        ))
                    })?,
                };
                Ok((id, stat.clone(), meta, col_stats))
            })
            .collect::<Result<Vec<_>>>()?;

        id_stats.sort_by(|a, b| {
            if a.1.null_count + b.1.null_count != 0 && *nulls_first {
                return a.1.null_count.cmp(&b.1.null_count).reverse();
            }
            // no nulls
            if *asc {
                a.1.min().cmp(b.1.min())
            } else {
                a.1.max().cmp(b.1.max()).reverse()
            }
        });
        Ok(id_stats
            .into_iter()
            .map(|s| (s.0.clone(), s.2.clone(), s.3))
            .take(self.limit)
            .collect())
    }
}
