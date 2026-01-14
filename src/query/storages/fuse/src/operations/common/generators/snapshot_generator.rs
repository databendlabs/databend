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
use std::sync::Arc;

use databend_common_catalog::table_context::TableContext;
use databend_common_exception::Result;
use databend_common_expression::TableSchema;
use databend_common_meta_app::schema::TableIdent;
use databend_storages_common_session::TxnManagerRef;
use databend_storages_common_table_meta::meta::ClusterKey;
use databend_storages_common_table_meta::meta::Statistics;
use databend_storages_common_table_meta::meta::TableMetaTimestamps;
use databend_storages_common_table_meta::meta::TableSnapshot;
use log::info;

use crate::operations::common::ConflictResolveContext;
use crate::statistics::TableStatsGenerator;

#[async_trait::async_trait]
pub trait SnapshotGenerator {
    /// Convert to `Any`, to enable dynamic casting.
    fn as_any(&self) -> &dyn Any;

    fn set_conflict_resolve_context(&mut self, _ctx: ConflictResolveContext) {}

    async fn fill_default_values(
        &mut self,
        _schema: &TableSchema,
        _snapshot: &Option<Arc<TableSnapshot>>,
    ) -> Result<()> {
        Ok(())
    }

    fn generate_new_snapshot(
        &self,
        table_ident: &TableIdent,
        table_schema: &TableSchema,
        cluster_key_meta: Option<ClusterKey>,
        previous: Option<Arc<TableSnapshot>>,
        txn_mgr: TxnManagerRef,
        table_meta_timestamps: TableMetaTimestamps,
        table_stats_gen: TableStatsGenerator,
    ) -> Result<TableSnapshot> {
        let mut snapshot = self.do_generate_new_snapshot(
            table_ident,
            table_schema,
            cluster_key_meta,
            &previous,
            table_meta_timestamps,
            table_stats_gen,
        )?;
        decorate_snapshot(&mut snapshot, txn_mgr, previous, table_ident.table_id)?;
        Ok(snapshot)
    }

    fn do_generate_new_snapshot(
        &self,
        table_ident: &TableIdent,
        table_schema: &TableSchema,
        cluster_key_meta: Option<ClusterKey>,
        previous: &Option<Arc<TableSnapshot>>,
        table_meta_timestamps: TableMetaTimestamps,
        table_stats_gen: TableStatsGenerator,
    ) -> Result<TableSnapshot>;
}

pub fn decorate_snapshot(
    snapshot: &mut TableSnapshot,
    txn_mgr: TxnManagerRef,
    previous: Option<Arc<TableSnapshot>>,
    table_id: u64,
) -> Result<()> {
    let has_pending_transactional_mutations = {
        let guard = txn_mgr.lock();
        // NOTE:
        // When generating a new snapshot for a mutation of table for the first time,
        // there is no buffered table ID inside txn_mgr for this table.
        guard.is_active() && guard.get_table_from_buffer_by_id(table_id).is_some()
    };

    if has_pending_transactional_mutations {
        // Adjust the `prev_snapshot_id` of the newly created snapshot to match the
        // `prev_snapshot_id` of the table when it first appeared in the transaction.
        let previous_of_previous = previous.as_ref().and_then(|prev| prev.prev_snapshot_id);
        snapshot.prev_snapshot_id = previous_of_previous;
    }
    Ok(())
}

pub(crate) fn set_compaction_num_block_hint(
    ctx: &dyn TableContext,
    table_name: &str,
    summary: &Statistics,
) {
    if let Err(e) = try_set_compaction_num_block_hint(ctx, table_name, summary) {
        log::warn!("set_compaction_num_block_hint failed: {}", e);
    }
}

pub(crate) fn try_set_compaction_num_block_hint(
    ctx: &dyn TableContext,
    table_name: &str,
    summary: &Statistics,
) -> Result<()> {
    // check if need to auto compact
    // the algorithm is: if the number of imperfect blocks is greater than the threshold, then auto compact.
    // the threshold is set by the setting `auto_compaction_imperfect_blocks_threshold`, default is 25.
    let imperfect_count = summary.block_count - summary.perfect_block_count;
    let auto_compaction_imperfect_blocks_threshold = ctx
        .get_settings()
        .get_auto_compaction_imperfect_blocks_threshold()?;
    if imperfect_count >= auto_compaction_imperfect_blocks_threshold {
        // If imperfect_count is larger, SLIGHTLY increase the number of blocks
        // eligible for auto-compaction, this adjustment is intended to help reduce
        // fragmentation over time.
        let compact_num_block_hint = std::cmp::min(
            imperfect_count,
            (auto_compaction_imperfect_blocks_threshold as f64 * 1.5).ceil() as u64,
        );
        info!("set compact_num_block_hint to {compact_num_block_hint }");
        ctx.set_compaction_num_block_hint(table_name, compact_num_block_hint);
    }
    Ok(())
}
