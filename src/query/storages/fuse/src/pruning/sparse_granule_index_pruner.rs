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

use std::ops::Range;
use std::sync::Arc;

use databend_common_exception::Result;
use databend_common_expression::DataSchema;
use databend_common_expression::Expr;
use databend_common_expression::FunctionContext;
use databend_common_expression::RemoteExpr;
use databend_common_expression::Scalar;
use databend_common_expression::TableSchemaRef;
use databend_common_expression::types::DataType;
use databend_storages_common_index::GranuleIndex as GranuleIndexEvaluator;
use databend_storages_common_io::ReadSettings;
use databend_storages_common_table_meta::meta::BlockMeta;
use databend_storages_common_table_meta::meta::ClusterKey;
use databend_storages_common_table_meta::meta::GranuleIndexLayout;
use opendal::Operator;

use crate::io::load_granule_mins;
use crate::io::num_granules_of;

/// Applies cluster-key predicates to per-granule mins.
pub struct SparseGranuleIndexPruner {
    evaluator: GranuleIndexEvaluator,
    dal: Operator,
    read_settings: ReadSettings,
    cluster_key_types: Vec<DataType>,
    table_cluster_key_id: u32,
}

impl SparseGranuleIndexPruner {
    #[allow(clippy::too_many_arguments)]
    pub fn try_create(
        func_ctx: FunctionContext,
        schema: &TableSchemaRef,
        filter_expr: Option<&Expr<String>>,
        cluster_key_meta: Option<ClusterKey>,
        cluster_keys: Vec<RemoteExpr<String>>,
        dal: Operator,
        read_settings: ReadSettings,
    ) -> Result<Option<Arc<SparseGranuleIndexPruner>>> {
        let Some(cluster_key_meta) = cluster_key_meta else {
            return Ok(None);
        };
        let Some(expr) = filter_expr else {
            return Ok(None);
        };
        if cluster_keys.is_empty()
            || cluster_keys
                .iter()
                .any(|expr| !matches!(expr, RemoteExpr::ColumnRef { .. }))
        {
            return Ok(None);
        }

        let cluster_keys = cluster_keys
            .iter()
            .map(|expr| match expr {
                RemoteExpr::ColumnRef { id, .. } => id.to_string(),
                _ => unreachable!(),
            })
            .collect::<Vec<_>>();

        let data_schema = DataSchema::from(schema.as_ref());
        let cluster_key_types = cluster_keys
            .iter()
            .map(|name| {
                data_schema
                    .field_with_name(name)
                    .map(|f| f.data_type().clone())
            })
            .collect::<std::result::Result<Vec<_>, _>>()?;

        let cluster_key_id = cluster_key_meta.0;
        let evaluator =
            GranuleIndexEvaluator::try_create(func_ctx, cluster_keys, expr, schema.clone())?;

        if !evaluator.touches_cluster_key() {
            return Ok(None);
        }

        Ok(Some(Arc::new(SparseGranuleIndexPruner {
            evaluator,
            dal,
            read_settings,
            cluster_key_types,
            table_cluster_key_id: cluster_key_id,
        })))
    }

    pub fn select_granule_ranges(
        &self,
        block_meta: &BlockMeta,
        granule_index: &GranuleIndexLayout,
        input: &[Range<usize>],
    ) -> Result<Vec<Range<usize>>> {
        let Some(cluster_stats) = block_meta.cluster_stats.as_ref() else {
            return Ok(input.to_vec());
        };

        if self.table_cluster_key_id != cluster_stats.cluster_key_id {
            return Ok(input.to_vec());
        }

        let num_granules = num_granules_of(
            block_meta.row_count as usize,
            granule_index.granule_rows as usize,
        );
        if num_granules == 0 {
            return Ok(input.to_vec());
        }

        let Some(mins_layout) = granule_index.mins.as_ref() else {
            return Ok(input.to_vec());
        };

        let granule_mins = load_granule_mins(
            &self.dal,
            &self.read_settings,
            mins_layout,
            &self.cluster_key_types,
            num_granules,
        )?;

        let block_max = Scalar::Tuple(cluster_stats.max().clone());
        let ranges = self.evaluator.apply(&granule_mins, &block_max)?;
        Ok(intersect_ranges(input, &ranges))
    }
}

fn intersect_ranges(left: &[Range<usize>], right: &[Range<usize>]) -> Vec<Range<usize>> {
    let mut result = Vec::new();
    let (mut l, mut r) = (0, 0);
    while l < left.len() && r < right.len() {
        let start = left[l].start.max(right[r].start);
        let end = left[l].end.min(right[r].end);
        if start < end {
            result.push(start..end);
        }
        if left[l].end <= right[r].end {
            l += 1;
        } else {
            r += 1;
        }
    }
    result
}

#[cfg(test)]
mod tests {
    use super::intersect_ranges;

    #[test]
    fn test_intersect_ranges() {
        let left = vec![0..3, 5..9];
        let right = vec![1..6, 7..8, 10..12];
        assert_eq!(intersect_ranges(&left, &right), vec![1..3, 5..6, 7..8]);
    }
}
