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
use std::collections::HashMap;
use std::collections::HashSet;
use std::sync::Arc;

use chrono::DateTime;
use chrono::Utc;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_meta_app::principal::StageInfo;
use databend_common_meta_app::schema::TableCopiedFileInfo;
use databend_common_meta_app::schema::TableIdent;
use databend_common_meta_app::schema::TableInfo;
use databend_common_meta_app::schema::TableLvtCheck;
use databend_common_meta_app::schema::UpdateMultiTableMetaReq;
use databend_common_meta_app::schema::UpdateStreamMetaReq;
use databend_common_meta_app::schema::UpdateTableMetaReq;
use databend_common_meta_app::schema::UpdateTempTableReq;
use databend_common_meta_app::schema::UpsertTableCopiedFileReq;
use databend_common_meta_app::tenant::Tenant;
use databend_meta_client::types::MatchSeq;
use databend_storages_common_table_meta::meta::BlockHLL;
use databend_storages_common_table_meta::meta::merge_column_hll_mut;
use databend_storages_common_table_meta::table_id_ranges::is_temp_table_id;
use parking_lot::Mutex;
use serde::Deserialize;
use serde::Serialize;

use crate::temp_table::TempTable;

#[derive(Debug, Clone)]
pub struct TxnManager {
    state: TxnState,
    txn_buffer: TxnBuffer,
    txn_id: String,

    /// Tables that need to be vacuumed after the transaction completes.
    ///
    /// A single table may trigger multiple vacuum actions within the same transaction, especially
    /// when auto-vacuum is enabled.
    /// While we store the complete TableInfo here, only table_id and catalog_info are
    /// actually used in subsequent vacuum operations. The table object itself will be reloaded
    /// when the vacuum is eventually performed after transaction commit.
    tables_need_purge: HashMap<u64, TableInfo>,
}

pub type TxnManagerRef = Arc<Mutex<TxnManager>>;

#[derive(Clone, Debug, Deserialize, Serialize, Eq, PartialEq)]
pub enum TxnState {
    AutoCommit,
    Active,
    Fail,
}

/// Deltas of the writes successfully buffered for one table in a transaction.
/// Only `TxnManager::add_table_delta` creates entries: a missing entry means the
/// logical delta is unknown, so there is deliberately no `Default`.
#[derive(Debug, Clone)]
pub struct TxnTableDelta {
    /// Accumulated (updated_rows, deleted_rows) for snapshot change tracking.
    /// Some((0, 0)) means no logical updates/deletes, including INSERT and ANALYZE;
    /// None means a counter overflowed and the logical change delta is unknown.
    pub logical_change_delta: Option<(u64, u64)>,
    /// Per-column HLLs unioned from successfully buffered statement-local writes.
    /// Excludes the base snapshot's HLLs and full ANALYZE refreshes; empty means
    /// no collected HLL statistics to replay.
    pub column_hlls: BlockHLL,
    /// Collected row observations accumulated together with column_hlls, not the
    /// table's current row count. Repeated updates may count a row again, and
    /// DELETE does not subtract previously collected observations.
    pub statistics_row_count: u64,
}

#[derive(Debug, Clone, Default)]
pub struct TxnBuffer {
    tenant: Option<Tenant>,
    table_desc_to_id: HashMap<String, u64>,
    mutated_tables: HashMap<u64, TableInfo>,
    base_snapshot_location: HashMap<u64, Option<String>>,
    lvt_check: HashMap<u64, TableLvtCheck>,
    copied_files: HashMap<u64, Vec<UpsertTableCopiedFileReq>>,
    update_stream_meta: HashMap<u64, UpdateStreamMetaReq>,
    deduplicated_labels: HashSet<String>,
    stream_tables: HashMap<u64, StreamSnapshot>,
    need_purge_files: Vec<(StageInfo, Vec<String>)>,
    // Only successfully buffered writes contribute.
    table_deltas: HashMap<u64, TxnTableDelta>,

    // TODO doc this
    table_tnx_begin_timestamps: HashMap<u64, DateTime<Utc>>,

    temp_table_desc_to_id: HashMap<String, u64>,
    mutated_temp_tables: HashMap<u64, TempTable>,
}

#[derive(Debug, Clone)]
pub struct StreamSnapshot {
    pub stream: TableInfo,
    pub source: TableInfo,
    pub max_batch_size: Option<u64>,
}

impl TxnBuffer {
    fn parse_db_tbl_name(desc: &str) -> (String, String) {
        let (db_raw, table_raw) = desc
            .split_once('.')
            .unwrap_or_else(|| panic!("Invalid temp table desc: {}", desc));
        let db_name = db_raw[1..db_raw.len() - 1].to_string();
        let table_name = table_raw[1..table_raw.len() - 1].to_string();
        (db_name, table_name)
    }

    fn clear(&mut self) {
        std::mem::take(self);
    }

    fn update_multi_table_meta(
        &mut self,
        tenant: &Tenant,
        mut req: UpdateMultiTableMetaReq,
    ) -> Result<()> {
        // Bind the transaction to the first non-conflicting tenant that updates meta.
        // Later updates must stay on the same tenant; commit uses this bound value.
        match &self.tenant {
            None => self.tenant = Some(tenant.clone()),
            Some(txn_tenant) if txn_tenant == tenant => {}
            Some(txn_tenant) => {
                return Err(ErrorCode::InvalidOperation(format!(
                    "Cannot update metadata for tenant '{}' in a transaction bound to tenant '{}'",
                    tenant.tenant_name(),
                    txn_tenant.tenant_name()
                )));
            }
        }

        for (req, table_info) in req.update_table_metas {
            let table_id = req.table_id;
            self.table_desc_to_id
                .insert(table_info.desc.clone(), table_id);

            self.mutated_tables.insert(table_id, TableInfo {
                meta: req.new_table_meta.clone(),
                ..table_info.clone()
            });

            self.base_snapshot_location
                .entry(table_id)
                .or_insert(req.base_snapshot_location);

            if let Some(check) = req.lvt_check {
                self.lvt_check
                    .entry(table_id)
                    .and_modify(|existing| {
                        existing.time = existing.time.min(check.time);
                        existing.touch |= check.touch;
                    })
                    .or_insert(check);
            }
        }

        for (table_id, file) in std::mem::take(&mut req.copied_files) {
            self.copied_files.entry(table_id).or_default().push(file);
        }

        self.update_stream_metas(&req.update_stream_metas);

        self.deduplicated_labels.extend(req.deduplicated_labels);

        for req in req.update_temp_tables {
            let (db_name, table_name) = Self::parse_db_tbl_name(&req.desc);
            self.temp_table_desc_to_id
                .insert(req.desc.clone(), req.table_id);
            self.mutated_temp_tables.insert(req.table_id, TempTable {
                db_name,
                table_name,
                meta: req.new_table_meta.clone(),
                copied_files: req.copied_files.clone(),
            });
        }

        Ok(())
    }

    fn update_stream_metas(&mut self, reqs: &[UpdateStreamMetaReq]) {
        for stream_meta in reqs.iter() {
            self.update_stream_meta
                .entry(stream_meta.stream_id)
                .or_insert(stream_meta.clone());
        }
    }

    fn clear_temp_table_by_id(&mut self, table_id: u64) {
        if let Some(temp_table) = self.mutated_temp_tables.remove(&table_id) {
            let desc = format!("'{}'.'{}'", temp_table.db_name, temp_table.table_name);
            self.temp_table_desc_to_id.remove(&desc);
        }
    }
}

impl TxnManager {
    pub fn init() -> TxnManagerRef {
        Arc::new(Mutex::new(TxnManager {
            state: TxnState::AutoCommit,
            txn_buffer: TxnBuffer::default(),
            txn_id: "".to_string(),
            tables_need_purge: HashMap::new(),
        }))
    }

    pub fn begin(&mut self) {
        if let TxnState::AutoCommit = self.state {
            self.txn_id = uuid::Uuid::new_v4().to_string();
            self.state = TxnState::Active
        }
    }

    pub fn txn_id(&self) -> &str {
        &self.txn_id
    }

    pub fn clear(&mut self) {
        self.state = TxnState::AutoCommit;
        self.txn_buffer.clear();
        self.txn_id = "".to_string();
    }

    pub fn set_fail(&mut self) {
        if let TxnState::Active = self.state {
            self.force_set_fail()
        }
    }

    pub fn set_auto_commit(&mut self) {
        self.state = TxnState::AutoCommit;
    }

    pub fn force_set_fail(&mut self) {
        self.state = TxnState::Fail;
        self.txn_buffer.clear();
        // keep the txn_id until commit/abort for tracing
    }

    pub fn is_fail(&self) -> bool {
        matches!(self.state, TxnState::Fail)
    }

    pub fn is_active(&self) -> bool {
        matches!(self.state, TxnState::Active)
    }

    pub fn state(&self) -> TxnState {
        self.state.clone()
    }

    pub fn update_multi_table_meta(
        &mut self,
        tenant: &Tenant,
        req: UpdateMultiTableMetaReq,
    ) -> Result<()> {
        self.txn_buffer.update_multi_table_meta(tenant, req)
    }

    pub fn tenant(&self) -> Option<Tenant> {
        self.txn_buffer.tenant.clone()
    }

    pub fn update_table_options(
        &mut self,
        table_id: u64,
        options: HashMap<String, Option<String>>,
    ) -> bool {
        let Some(table) = self.txn_buffer.mutated_tables.get_mut(&table_id) else {
            return false;
        };
        for (key, value) in options {
            match value {
                Some(value) => {
                    table.meta.options.insert(key, value);
                }
                None => {
                    table.meta.options.remove(&key);
                }
            }
        }
        true
    }

    /// Record one successfully buffered write, never a candidate commit attempt.
    /// The logical delta is independent of persisted counter epochs and snapshot
    /// resets. No HLL means no NDV coverage, even if the write changed rows.
    pub fn add_table_delta(
        &mut self,
        table_id: u64,
        logical_change_delta: (u64, u64),
        column_hlls: &BlockHLL,
        statistics_row_count: u64,
    ) {
        if !self.is_active() {
            return;
        }
        let delta = self
            .txn_buffer
            .table_deltas
            .entry(table_id)
            .or_insert_with(|| TxnTableDelta {
                logical_change_delta: Some((0, 0)),
                column_hlls: BlockHLL::new(),
                statistics_row_count: 0,
            });
        // Change tracking must not trust a wrapped counter: overflow is unknown.
        delta.logical_change_delta = delta.logical_change_delta.and_then(|(updated, deleted)| {
            updated
                .checked_add(logical_change_delta.0)
                .zip(deleted.checked_add(logical_change_delta.1))
        });
        if statistics_row_count > 0 && !column_hlls.is_empty() {
            delta.statistics_row_count += statistics_row_count;
            merge_column_hll_mut(&mut delta.column_hlls, column_hlls);
        }
    }

    /// Transfer the fixed deltas to COMMIT without cloning their sketches. The
    /// interpreter has already switched to AutoCommit, but has not cleared the buffer.
    pub fn take_table_deltas(&mut self) -> HashMap<u64, TxnTableDelta> {
        std::mem::take(&mut self.txn_buffer.table_deltas)
    }

    /// Logical totals of the buffered writes. Missing is not a known zero.
    pub fn logical_change_deltas(&self) -> HashMap<u64, Option<(u64, u64)>> {
        self.txn_buffer
            .table_deltas
            .iter()
            .map(|(&table_id, delta)| (table_id, delta.logical_change_delta))
            .collect()
    }

    pub fn update_stream_metas(&mut self, reqs: &[UpdateStreamMetaReq]) {
        self.txn_buffer.update_stream_metas(reqs);
    }

    // for caching stream table to impl the rr semantics
    pub fn upsert_stream_table(
        &mut self,
        stream: TableInfo,
        source: TableInfo,
        max_batch_size: Option<u64>,
    ) {
        self.txn_buffer
            .stream_tables
            .insert(stream.ident.table_id, StreamSnapshot {
                stream,
                source,
                max_batch_size,
            });
    }

    pub fn upsert_table_desc_to_id(&mut self, table: TableInfo) {
        self.txn_buffer
            .table_desc_to_id
            .insert(table.desc.clone(), table.ident.table_id);
    }

    pub fn get_stream_table(&self, stream_desc: &str) -> Option<StreamSnapshot> {
        self.txn_buffer
            .table_desc_to_id
            .get(stream_desc)
            .and_then(|id| self.txn_buffer.stream_tables.get(id))
            .cloned()
    }

    pub fn get_table_from_buffer(&self, desc: &str) -> Option<TableInfo> {
        let temp_table_id = self.txn_buffer.temp_table_desc_to_id.get(desc);
        if let Some(id) = temp_table_id {
            let table = self.txn_buffer.mutated_temp_tables.get(id).unwrap();
            return Some(TableInfo::new(
                &table.db_name,
                &table.table_name,
                TableIdent {
                    table_id: *id,
                    seq: 0,
                },
                table.meta.clone(),
            ));
        }

        self.txn_buffer.table_desc_to_id.get(desc).and_then(|id| {
            self.txn_buffer.mutated_tables.get(id).cloned().or_else(|| {
                self.txn_buffer
                    .stream_tables
                    .get(id)
                    .map(|snapshot| snapshot.stream.clone())
            })
        })
    }

    pub fn get_table_from_buffer_by_id(&self, table_id: u64) -> Option<TableInfo> {
        self.txn_buffer
            .mutated_temp_tables
            .get(&table_id)
            .map(|t| {
                TableInfo::new(
                    &t.db_name,
                    &t.table_name,
                    TableIdent { table_id, seq: 0 },
                    t.meta.clone(),
                )
            })
            .or_else(|| {
                self.txn_buffer
                    .mutated_tables
                    .get(&table_id)
                    .cloned()
                    .or_else(|| {
                        self.txn_buffer
                            .stream_tables
                            .get(&table_id)
                            .map(|snapshot| snapshot.stream.clone())
                    })
            })
    }

    pub fn req(&self) -> UpdateMultiTableMetaReq {
        let mut copied_files = Vec::new();
        for (tbl_id, v) in &self.txn_buffer.copied_files {
            for file in v {
                copied_files.push((*tbl_id, file.clone()));
            }
        }
        UpdateMultiTableMetaReq {
            update_table_metas: self
                .txn_buffer
                .mutated_tables
                .iter()
                .map(|(id, info)| {
                    (
                        UpdateTableMetaReq {
                            table_id: *id,
                            seq: MatchSeq::Exact(info.ident.seq),
                            new_table_meta: info.meta.clone(),
                            base_snapshot_location: None,
                            lvt_check: self.txn_buffer.lvt_check.get(id).cloned(),
                        },
                        info.clone(),
                    )
                })
                .collect(),
            copied_files,
            update_stream_metas: self
                .txn_buffer
                .update_stream_meta
                .values()
                .cloned()
                .collect(),
            deduplicated_labels: self
                .txn_buffer
                .deduplicated_labels
                .iter()
                .cloned()
                .collect(),
            update_temp_tables: self
                .txn_buffer
                .mutated_temp_tables
                .iter()
                .map(|(id, t)| UpdateTempTableReq {
                    table_id: *id,
                    new_table_meta: t.meta.clone(),
                    copied_files: t.copied_files.clone(),
                    desc: format!("'{}'.'{}'", t.db_name, t.table_name),
                })
                .collect(),
        }
    }

    pub fn contains_deduplicated_label(&self, label: &str) -> bool {
        self.txn_buffer.deduplicated_labels.contains(label)
    }

    pub fn get_table_copied_file_info(
        &self,
        table_id: u64,
    ) -> BTreeMap<String, TableCopiedFileInfo> {
        let mut ret = BTreeMap::new();
        if !self.is_active() {
            return ret;
        }
        if is_temp_table_id(table_id) {
            let temp_table = self.txn_buffer.mutated_temp_tables.get(&table_id);
            if let Some(temp_table) = temp_table {
                ret.extend(temp_table.copied_files.clone());
            }
        } else {
            let reqs = self.txn_buffer.copied_files.get(&table_id);
            if let Some(reqs) = reqs {
                for req in reqs {
                    ret.extend(req.file_info.clone());
                }
            }
        }
        ret
    }

    pub fn add_need_purge_files(&mut self, stage_info: StageInfo, files: Vec<String>) {
        self.txn_buffer.need_purge_files.push((stage_info, files));
    }

    pub fn need_purge_files(&mut self) -> Vec<(StageInfo, Vec<String>)> {
        std::mem::take(&mut self.txn_buffer.need_purge_files)
    }

    pub fn defer_table_purge(&mut self, table_info: TableInfo) {
        self.tables_need_purge
            .insert(table_info.ident.table_id, table_info);
    }
    pub fn table_need_purge(&mut self) -> Vec<TableInfo> {
        std::mem::take(&mut self.tables_need_purge)
            .into_values()
            .collect()
    }

    pub fn get_table_txn_begin_timestamp(&self, table_id: u64) -> Option<DateTime<Utc>> {
        self.txn_buffer
            .table_tnx_begin_timestamps
            .get(&table_id)
            .cloned()
    }

    pub fn set_table_txn_begin_timestamp(&mut self, table_id: u64, timestamps: DateTime<Utc>) {
        self.txn_buffer
            .table_tnx_begin_timestamps
            .insert(table_id, timestamps);
    }

    pub fn get_base_snapshot_location(&self, table_id: u64) -> Option<String> {
        self.txn_buffer
            .base_snapshot_location
            .get(&table_id)
            .unwrap()
            .clone()
    }

    pub fn clear_temp_table_by_id(&mut self, table_id: u64) {
        self.txn_buffer.clear_temp_table_by_id(table_id);
    }
}

#[cfg(test)]
mod tests {
    use chrono::DateTime;
    use chrono::Utc;
    use databend_common_meta_app::schema::TableInfo;
    use databend_common_meta_app::schema::TableLvtCheck;
    use databend_common_meta_app::schema::UpdateMultiTableMetaReq;
    use databend_common_meta_app::schema::UpdateTableMetaReq;
    use databend_common_meta_app::tenant::Tenant;
    use databend_meta_client::types::MatchSeq;
    use databend_storages_common_table_meta::meta::BlockHLL;

    use super::TxnBuffer;
    use super::TxnManager;

    #[test]
    fn test_normalize_temp_table_desc() {
        let (db, table) = TxnBuffer::parse_db_tbl_name("'db'.'tbl'");
        assert_eq!(db, "db");
        assert_eq!(table, "tbl");
    }

    #[test]
    fn test_transaction_preserves_first_lvt_fence() {
        let tenant = Tenant::new_literal("tenant");
        let first_time = DateTime::from_timestamp(1_000, 0).unwrap();
        let later_time = DateTime::from_timestamp(2_000, 0).unwrap();
        let mut table_info = TableInfo::default();
        table_info.ident.table_id = 7;
        table_info.ident.seq = 1;

        let make_req = |time: Option<DateTime<Utc>>, touch| UpdateMultiTableMetaReq {
            update_table_metas: vec![(
                UpdateTableMetaReq {
                    table_id: 7,
                    seq: MatchSeq::Exact(1),
                    new_table_meta: table_info.meta.clone(),
                    base_snapshot_location: Some("snapshot".to_string()),
                    lvt_check: time.map(|time| TableLvtCheck { time, touch }),
                },
                table_info.clone(),
            )],
            ..Default::default()
        };

        let txn_mgr = TxnManager::init();
        let mut txn_mgr = txn_mgr.lock();
        txn_mgr.begin();
        txn_mgr
            .update_multi_table_meta(&tenant, make_req(None, false))
            .unwrap();
        txn_mgr
            .update_multi_table_meta(&tenant, make_req(Some(later_time), true))
            .unwrap();
        txn_mgr
            .update_multi_table_meta(&tenant, make_req(Some(first_time), false))
            .unwrap();

        let request = txn_mgr.req();
        assert_eq!(request.update_table_metas.len(), 1);
        let check = request.update_table_metas[0].0.lvt_check.as_ref().unwrap();
        assert_eq!(check.time, first_time);
        assert!(
            check.touch,
            "later requests must not discard a historical publication touch"
        );
    }

    fn stats_hll(values: &[u64]) -> BlockHLL {
        let mut hll = databend_common_storage::MetaHLL::new();
        for value in values {
            hll.add_object(value);
        }
        BlockHLL::from([(0, hll)])
    }

    #[test]
    fn test_table_deltas_accumulate_and_transfer_to_commit() {
        let txn_mgr = TxnManager::init();
        let mut txn = txn_mgr.lock();
        txn.add_table_delta(1, (9, 9), &stats_hll(&[99]), 100);
        assert!(txn.logical_change_deltas().is_empty());
        txn.begin();
        txn.add_table_delta(1, (5, 0), &stats_hll(&[0]), 10);
        txn.add_table_delta(1, (2, 3), &stats_hll(&[1]), 3);
        txn.add_table_delta(2, (0, 0), &stats_hll(&[2]), 20);
        let logical = txn.logical_change_deltas();
        assert_eq!(logical.get(&1), Some(&Some((7, 3))));
        assert_eq!(logical.get(&2), Some(&Some((0, 0))));
        assert!(!logical.contains_key(&3));
        // COMMIT switches to AutoCommit before taking ownership of both fields.
        txn.set_auto_commit();
        assert_eq!(txn.logical_change_deltas(), logical);
        let frozen = txn.take_table_deltas();
        assert_eq!(frozen[&1].logical_change_delta, Some((7, 3)));
        assert_eq!(frozen[&1].statistics_row_count, 13);
        assert_eq!(frozen[&1].column_hlls, stats_hll(&[0, 1]));
        assert_eq!(frozen[&2].logical_change_delta, Some((0, 0)));
        assert_eq!(frozen[&2].statistics_row_count, 20);
        assert_eq!(frozen[&2].column_hlls, stats_hll(&[2]));
        assert!(txn.logical_change_deltas().is_empty());
        assert!(txn.take_table_deltas().is_empty());
        txn.add_table_delta(1, (9, 9), &stats_hll(&[99]), 100);
        assert!(txn.take_table_deltas().is_empty());
        txn.clear();
        assert_eq!(frozen[&1].logical_change_delta, Some((7, 3)));
        assert_eq!(frozen[&1].statistics_row_count, 13);
    }

    #[test]
    fn test_table_deltas_without_statistics() {
        let txn_mgr = TxnManager::init();
        let mut txn = txn_mgr.lock();
        txn.begin();
        for (id, logical_change_delta, column_hlls, statistics_row_count) in [
            (1, (0, 0), BlockHLL::new(), 100), // collection disabled
            (2, (0, 0), stats_hll(&[99]), 0),  // zero-row statement
            (3, (0, 0), stats_hll(&[]), 5),    // collected all-NULL column
            (4, (0, 0), BlockHLL::new(), 0),   // ANALYZE
            (5, (0, 7), BlockHLL::new(), 7),   // uncollected DELETE
        ] {
            txn.add_table_delta(id, logical_change_delta, &column_hlls, statistics_row_count);
        }
        // An uncollected statement must not inflate later collected coverage.
        txn.add_table_delta(1, (0, 0), &stats_hll(&[2]), 1);
        let frozen = txn.take_table_deltas();
        assert_eq!(frozen[&1].statistics_row_count, 1);
        assert_eq!(frozen[&1].column_hlls, stats_hll(&[2]));
        assert_eq!(frozen[&3].statistics_row_count, 5);
        assert_eq!(frozen[&3].column_hlls[&0].count(), 0);
        for id in [2, 4, 5] {
            assert_eq!(frozen[&id].statistics_row_count, 0);
            assert!(frozen[&id].column_hlls.is_empty());
        }
        for id in [1, 2, 3, 4] {
            assert_eq!(frozen[&id].logical_change_delta, Some((0, 0)));
        }
        assert_eq!(frozen[&5].logical_change_delta, Some((0, 7)));
        assert!(!frozen.contains_key(&6)); // absent is not a known logical zero
    }

    #[test]
    fn test_logical_overflow_stays_unknown_without_dropping_statistics() {
        for initial in [(u64::MAX, 0), (0, u64::MAX)] {
            let txn_mgr = TxnManager::init();
            let mut txn = txn_mgr.lock();
            txn.begin();
            txn.add_table_delta(1, initial, &stats_hll(&[0]), 10);
            txn.add_table_delta(1, (1, 1), &stats_hll(&[1]), 3);
            txn.add_table_delta(1, (0, 0), &BlockHLL::new(), 0);
            assert_eq!(txn.logical_change_deltas().get(&1), Some(&None));
            txn.add_table_delta(1, (1, 1), &stats_hll(&[2]), 2);
            let frozen = txn.take_table_deltas();
            assert_eq!(frozen[&1].logical_change_delta, None);
            assert_eq!(frozen[&1].statistics_row_count, 15);
            assert_eq!(frozen[&1].column_hlls, stats_hll(&[0, 1, 2]));
        }
    }

    #[test]
    fn test_table_deltas_clear_on_abort_and_failure() {
        for fail in [false, true] {
            let txn_mgr = TxnManager::init();
            let mut txn = txn_mgr.lock();
            txn.begin();
            txn.add_table_delta(1, (2, 3), &stats_hll(&[1]), 10);
            if fail {
                txn.force_set_fail();
            } else {
                txn.clear();
            }
            assert!(txn.logical_change_deltas().is_empty());
            assert!(txn.take_table_deltas().is_empty());
            txn.clear();
            txn.begin();
            assert!(txn.logical_change_deltas().is_empty());
            assert!(txn.take_table_deltas().is_empty());
        }
    }
}
