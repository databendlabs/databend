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
use opendal::Operator;

use crate::io::load_granule_mins;
use crate::io::num_granules_of;

/// Prune-time evaluator for the sparse granule index. For a clustered table with a cluster-key
/// predicate, it loads each block's `mins` sidecar (via the byte ranges recorded in block meta — no
/// footer parse), decodes the per-granule cluster-key mins, and narrows them to the surviving
/// granule runs (maximally-coalesced `[start, end)`) that may satisfy the predicate.
///
/// The selected runs are stashed in `BlockMetaIndex.granule_ranges` and consumed at read time
/// to fetch only the matching granules' byte ranges. Reuses the battle-tested predicate evaluator
/// in [`GranuleIndexEvaluator`] (the same one the legacy inline-`pages` path uses).
pub struct SparseGranuleIndexPruner {
    evaluator: GranuleIndexEvaluator,
    dal: Operator,
    read_settings: ReadSettings,
    /// Cluster-key element types (tuple order), derived from the table schema. Replaces the footer
    /// `cluster_key_types`: since we only narrow blocks whose cluster-key id matches the table's,
    /// the block's mins were written under exactly these element types.
    cluster_key_types: Vec<DataType>,
    /// The table's *current* cluster-key id. After `ALTER TABLE ... CLUSTER BY`, the table's
    /// cluster-key seq is bumped, but already-written blocks keep their old id and old-key-sorted
    /// granule mins. Narrowing such a block with the new-key predicate would be wrong, so we only
    /// narrow blocks whose cluster-key id still matches this.
    table_cluster_key_id: u32,
}

impl SparseGranuleIndexPruner {
    /// Create the pruner, or `None` when sparse-page narrowing cannot apply: no cluster key, no
    /// filter, the cluster keys are not plain column refs, or the filter does not touch the
    /// cluster key.
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

        // Cluster-key element types, in tuple order, derived from the table schema. The mins for a
        // block whose cluster-key id matches the table's were written with these element types
        // (nullable-wrapped at load time), so this replaces the old footer `cluster_key_types`.
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

    /// Select the surviving granule runs to read for `block_meta`. Returns:
    /// - `Ok(Some(ranges))`: the granule index applied. `ranges` is the maximally-coalesced set of
    ///   surviving granule runs (an explicit set, so a later index can intersect against it):
    ///   `vec![0..N]` = all granules survive; `vec![]` = every granule pruned (drop the block);
    ///   `vec![r0, r1, ...]` = only these granules survive.
    /// - `Ok(None)`: the granule index does not apply (no sidecar index, or a cluster-key-id mismatch)
    ///   — read the whole block.
    ///
    /// A corrupt or unreadable index also degrades to `None` so queries never fail.
    pub async fn select_granule_ranges(&self, block_meta: &BlockMeta) -> Option<Vec<Range<usize>>> {
        let granule_index = block_meta.granule_index.as_ref()?;
        let cluster_stats = block_meta.cluster_stats.as_ref()?;

        match self
            .try_select(block_meta, granule_index, cluster_stats.cluster_key_id)
            .await
        {
            Ok(ranges) => ranges,
            Err(e) => {
                log::warn!(
                    "[FUSE-PRUNER] sparse granule index pruning failed for {}, reading whole block: {e}",
                    block_meta.location.0
                );
                None
            }
        }
    }

    async fn try_select(
        &self,
        block_meta: &BlockMeta,
        granule_index: &databend_storages_common_table_meta::meta::GranuleIndexLayout,
        block_cluster_key_id: u32,
    ) -> Result<Option<Vec<Range<usize>>>> {
        // The block was written/clustered under `block_cluster_key_id`. If the table's current
        // cluster key differs (an `ALTER TABLE ... CLUSTER BY` bumped the seq and this block has
        // not been reclustered yet), the granule mins are sorted by the *old* key and have the old
        // arity, so the new-key predicate must not narrow them. Read the whole block instead.
        if self.table_cluster_key_id != block_cluster_key_id {
            return Ok(None);
        }

        let num_granules = num_granules_of(
            block_meta.row_count as usize,
            granule_index.granule_rows as usize,
        );
        if num_granules == 0 {
            return Ok(None);
        }

        // An offset-only index (no cluster key / no mins file) cannot prune: every granule survives,
        // but the read still goes page-wise. Return the full survivor set so the read path uses the
        // page layout instead of falling back to a whole-block read.
        let Some(mins_layout) = granule_index.mins.as_ref() else {
            #[allow(clippy::single_range_in_vec_init)]
            return Ok(Some(vec![0..num_granules]));
        };

        // Load the per-granule mins from the mins sidecar via the recorded byte ranges (no footer).
        // Cluster-key element types come from the table schema (ids match), not from any footer.
        let granule_mins = load_granule_mins(
            &self.dal,
            &self.read_settings,
            mins_layout,
            &self.cluster_key_types,
            num_granules,
        )
        .await?;

        let cluster_stats = block_meta.cluster_stats.as_ref().unwrap();
        let block_max = Scalar::Tuple(cluster_stats.max().clone());

        // The evaluator returns the surviving granule runs directly: `vec![]` means every granule
        // was pruned (the reader drops the block), a non-empty vec is the survivor set.
        let ranges = self.evaluator.apply(&granule_mins, &block_max)?;
        Ok(Some(ranges))
    }
}
