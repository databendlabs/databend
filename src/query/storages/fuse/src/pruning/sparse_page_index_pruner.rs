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
use databend_common_expression::Expr;
use databend_common_expression::FunctionContext;
use databend_common_expression::RemoteExpr;
use databend_common_expression::Scalar;
use databend_common_expression::TableSchemaRef;
use databend_storages_common_index::PageIndex as PageIndexEvaluator;
use databend_storages_common_table_meta::meta::BlockMeta;
use databend_storages_common_table_meta::meta::ClusterKey;
use opendal::Operator;

use crate::io::PageIndex;

/// Prune-time evaluator for the sparse page index. For a clustered table with a cluster-key
/// predicate, it loads each block's sidecar page index, decodes the per-granule cluster-key mins,
/// and narrows them to the surviving granule runs (maximally-coalesced `[start, end)`) that may
/// satisfy the predicate.
///
/// The selected runs are stashed in `BlockMetaIndex.page_granule_ranges` and consumed at read time
/// to fetch only the matching granules' byte ranges. Reuses the battle-tested predicate evaluator
/// in [`PageIndexEvaluator`] (the same one the legacy inline-`pages` path uses).
pub struct SparsePageIndexPruner {
    evaluator: PageIndexEvaluator,
    dal: Operator,
    /// The table's *current* cluster-key id. After `ALTER TABLE ... CLUSTER BY`, the table's
    /// cluster-key seq is bumped, but already-written blocks keep their old id and old-key-sorted
    /// granule mins. Narrowing such a block with the new-key predicate would be wrong, so we only
    /// narrow blocks whose cluster-key id still matches this.
    table_cluster_key_id: u32,
}

impl SparsePageIndexPruner {
    /// Create the pruner, or `None` when sparse-page narrowing cannot apply: no cluster key, no
    /// filter, the cluster keys are not plain column refs, or the filter does not touch the
    /// cluster key.
    pub fn try_create(
        func_ctx: FunctionContext,
        schema: &TableSchemaRef,
        filter_expr: Option<&Expr<String>>,
        cluster_key_meta: Option<ClusterKey>,
        cluster_keys: Vec<RemoteExpr<String>>,
        dal: Operator,
    ) -> Result<Option<Arc<SparsePageIndexPruner>>> {
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

        let cluster_key_id = cluster_key_meta.0;
        let evaluator =
            PageIndexEvaluator::try_create(func_ctx, cluster_keys, expr, schema.clone())?;

        if !evaluator.touches_cluster_key() {
            return Ok(None);
        }

        Ok(Some(Arc::new(SparsePageIndexPruner {
            evaluator,
            dal,
            table_cluster_key_id: cluster_key_id,
        })))
    }

    /// Select the surviving granule runs to read for `block_meta`. Returns:
    /// - `Ok(Some(ranges))`: the page index applied. `ranges` is the maximally-coalesced set of
    ///   surviving granule runs (an explicit set, so a later index can intersect against it):
    ///   `vec![0..N]` = all granules survive; `vec![]` = every granule pruned (drop the block);
    ///   `vec![r0, r1, ...]` = only these granules survive.
    /// - `Ok(None)`: the page index does not apply (no sidecar index, or a cluster-key-id mismatch)
    ///   — read the whole block.
    ///
    /// A corrupt or unreadable index also degrades to `None` so queries never fail.
    pub async fn select_granule_ranges(&self, block_meta: &BlockMeta) -> Option<Vec<Range<usize>>> {
        let location = block_meta.page_index_location.as_ref()?;
        let cluster_stats = block_meta.cluster_stats.as_ref()?;

        match self
            .try_select(block_meta, &location.0, cluster_stats.cluster_key_id)
            .await
        {
            Ok(ranges) => ranges,
            Err(e) => {
                log::warn!(
                    "[FUSE-PRUNER] sparse page index pruning failed for {}, reading whole block: {e}",
                    block_meta.location.0
                );
                None
            }
        }
    }

    async fn try_select(
        &self,
        block_meta: &BlockMeta,
        location: &str,
        block_cluster_key_id: u32,
    ) -> Result<Option<Vec<Range<usize>>>> {
        // The block was written/clustered under `block_cluster_key_id`. If the table's current
        // cluster key differs (an `ALTER TABLE ... CLUSTER BY` bumped the seq and this block has
        // not been reclustered yet), the granule mins are sorted by the *old* key and have the old
        // arity, so the new-key predicate must not narrow them. Read the whole block instead.
        if self.table_cluster_key_id != block_cluster_key_id {
            return Ok(None);
        }

        let size = block_meta.page_index_size.unwrap_or(0);
        let index = PageIndex::load(&self.dal, location, size).await?;

        // An offset-only index (no cluster key / no per-granule mins) cannot prune: every granule
        // survives, but the read still goes page-wise. Return the full survivor set so the read path
        // uses the page layout instead of falling back to a whole-block read.
        if index.granule_mins.is_empty() {
            let num_granules = index.meta.num_granules as usize;
            #[allow(clippy::single_range_in_vec_init)]
            return Ok(Some(vec![0..num_granules]));
        }

        // Defensive: the index's own recorded id must also match the block's. They are written
        // together so this should always hold, but a mismatch means a stale/foreign index file.
        if index.meta.cluster_key_id != Some(block_cluster_key_id) {
            return Ok(None);
        }

        let cluster_stats = block_meta.cluster_stats.as_ref().unwrap();
        let block_max = Scalar::Tuple(cluster_stats.max().clone());

        // The evaluator returns the surviving granule runs directly: `vec![]` means every granule
        // was pruned (the reader drops the block), a non-empty vec is the survivor set.
        let ranges = self.evaluator.apply(&index.granule_mins, &block_max)?;
        Ok(Some(ranges))
    }
}
