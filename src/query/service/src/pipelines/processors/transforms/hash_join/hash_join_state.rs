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

use std::cell::SyncUnsafeCell;
use std::collections::HashSet;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;
use std::sync::Arc;

use common_base::base::tokio::sync::Notify;
use common_exception::Result;
use common_expression::types::DataType;
use common_expression::ColumnVec;
use common_expression::DataBlock;
use common_expression::DataSchemaRef;
use common_expression::HashMethodFixedKeys;
use common_expression::HashMethodSerializer;
use common_expression::HashMethodSingleString;
use common_hashtable::HashJoinHashMap;
use common_hashtable::HashtableKeyable;
use common_hashtable::StringHashJoinHashMap;
use common_sql::plans::JoinType;
use common_sql::ColumnSet;
use ethnum::U256;
use parking_lot::Mutex;
use parking_lot::RwLock;

use crate::pipelines::processors::transforms::hash_join::row::RowSpace;
use crate::pipelines::processors::transforms::hash_join::util::build_schema_wrap_nullable;
use crate::pipelines::processors::HashJoinDesc;
use crate::sessions::QueryContext;

pub struct SerializerHashJoinHashTable {
    pub(crate) hash_table: StringHashJoinHashMap,
    pub(crate) hash_method: HashMethodSerializer,
}

pub struct SingleStringHashJoinHashTable {
    pub(crate) hash_table: StringHashJoinHashMap,
    pub(crate) hash_method: HashMethodSingleString,
}

pub struct FixedKeyHashJoinHashTable<T: HashtableKeyable> {
    pub(crate) hash_table: HashJoinHashMap<T>,
    pub(crate) hash_method: HashMethodFixedKeys<T>,
}

pub enum HashJoinHashTable {
    Null,
    Serializer(SerializerHashJoinHashTable),
    SingleString(SingleStringHashJoinHashTable),
    KeysU8(FixedKeyHashJoinHashTable<u8>),
    KeysU16(FixedKeyHashJoinHashTable<u16>),
    KeysU32(FixedKeyHashJoinHashTable<u32>),
    KeysU64(FixedKeyHashJoinHashTable<u64>),
    KeysU128(FixedKeyHashJoinHashTable<u128>),
    KeysU256(FixedKeyHashJoinHashTable<U256>),
}

/// Define some shared states for hash join build and probe.
/// It will like a bridge to connect build and probe.
/// Such as build side will pass hash table to probe side by it
pub struct HashJoinState {
    /// A shared big hash table stores all the rows from build side
    pub(crate) hash_table: Arc<SyncUnsafeCell<HashJoinHashTable>>,
    /// It will be increased by 1 when a new hash join build processor is created.
    /// After the processor added its chunks to `HashTable`, it will be decreased by 1.
    /// When the counter is 0, it means all hash join build processors have added their chunks to `HashTable`.
    /// And the build phase is finished. Probe phase will start.
    pub(crate) hash_table_builders: Mutex<usize>,
    /// After `hash_table_builders` is 0, it will be set as true.
    /// It works with notify to make HashJoin start the probe phase.
    pub(crate) build_done: Mutex<bool>,
    /// Notify probe processors that build phase is finished.
    pub(crate) build_done_notify: Arc<Notify>,
    /// Some description of hash join. Such as join type, join keys, etc.
    pub(crate) hash_join_desc: HashJoinDesc,
    /// Interrupt the build phase or probe phase.
    pub(crate) interrupt: Arc<AtomicBool>,
    /// If there is no data in build side, maybe we can fast return.
    pub(crate) fast_return: Arc<RwLock<bool>>,
    /// `RowSpace` contains all rows from build side.
    pub(crate) row_space: RowSpace,
    pub(crate) build_num_rows: Arc<SyncUnsafeCell<usize>>,
    /// Data of the build side.
    pub(crate) chunks: Arc<SyncUnsafeCell<Vec<DataBlock>>>,
    pub(crate) build_columns: Arc<SyncUnsafeCell<Vec<ColumnVec>>>,
    pub(crate) build_columns_data_type: Arc<SyncUnsafeCell<Vec<DataType>>>,
    // Use the column of probe side to construct build side column.
    // (probe index, (is probe column nullable, is build column nullable))
    pub(crate) probe_to_build: Arc<Vec<(usize, (bool, bool))>>,
    pub(crate) is_build_projected: Arc<AtomicBool>,
    /// OuterScan map, initialized at `HashJoinBuildState`, used in `HashJoinProbeState`
    pub(crate) outer_scan_map: Arc<SyncUnsafeCell<Vec<Vec<bool>>>>,
    /// LeftMarkScan map, initialized at `HashJoinBuildState`, used in `HashJoinProbeState`
    pub(crate) mark_scan_map: Arc<SyncUnsafeCell<Vec<Vec<u8>>>>,
    /// Spill partition set
    pub(crate) spill_partition: Arc<RwLock<HashSet<u8>>>,
    /// After all probe processors finish spill or probe processors finish a round run, notify build processors.
    pub(crate) probe_spill_done_notify: Arc<Notify>,
    pub(crate) probe_spill_done: Mutex<bool>,
    /// After `final_probe_workers` is 0, it will be set as true
    pub(crate) final_probe_done: Mutex<bool>,
    /// Notify build workers `final scan` is done. They can go to next phase.
    pub(crate) final_probe_done_notify: Arc<Notify>,
    /// After all build processors finish spill, will pick a partition
    /// tell build processors to restore data in the partition
    /// If partition_id is -1, it means all partitions are spilled.
    pub(crate) partition_id: Arc<RwLock<i8>>,
}

impl HashJoinState {
    pub fn try_create(
        ctx: Arc<QueryContext>,
        mut build_schema: DataSchemaRef,
        build_projections: &ColumnSet,
        hash_join_desc: HashJoinDesc,
        probe_to_build: &[(usize, (bool, bool))],
    ) -> Result<Arc<HashJoinState>> {
        if matches!(
            hash_join_desc.join_type,
            JoinType::Left | JoinType::LeftSingle
        ) {
            build_schema = build_schema_wrap_nullable(&build_schema);
        };
        if hash_join_desc.join_type == JoinType::Full {
            build_schema = build_schema_wrap_nullable(&build_schema);
        }
        Ok(Arc::new(HashJoinState {
            hash_table: Arc::new(SyncUnsafeCell::new(HashJoinHashTable::Null)),
            hash_table_builders: Mutex::new(0),
            build_done: Mutex::new(false),
            build_done_notify: Arc::new(Default::default()),
            hash_join_desc,
            interrupt: Arc::new(AtomicBool::new(false)),
            fast_return: Arc::new(Default::default()),
            row_space: RowSpace::new(ctx, build_schema, build_projections)?,
            build_num_rows: Arc::new(SyncUnsafeCell::new(0)),
            chunks: Arc::new(SyncUnsafeCell::new(Vec::new())),
            build_columns: Arc::new(SyncUnsafeCell::new(Vec::new())),
            build_columns_data_type: Arc::new(SyncUnsafeCell::new(Vec::new())),
            probe_to_build: Arc::new(probe_to_build.to_vec()),
            is_build_projected: Arc::new(AtomicBool::new(true)),
            outer_scan_map: Arc::new(SyncUnsafeCell::new(Vec::new())),
            mark_scan_map: Arc::new(SyncUnsafeCell::new(Vec::new())),
            spill_partition: Default::default(),
            probe_spill_done_notify: Arc::new(Default::default()),
            probe_spill_done: Default::default(),
            final_probe_done: Default::default(),
            final_probe_done_notify: Arc::new(Default::default()),
            partition_id: Arc::new(Default::default()),
        }))
    }

    pub fn interrupt(&self) {
        self.interrupt.store(true, Ordering::Release);
    }

    /// Used by hash join probe processors, wait for build phase finished.
    #[async_backtrace::framed]
    pub async fn wait_build_hash_table_finish(&self) -> Result<()> {
        let notified = {
            let finalized_guard = self.build_done.lock();
            match *finalized_guard {
                true => None,
                false => Some(self.build_done_notify.notified()),
            }
        };
        if let Some(notified) = notified {
            notified.await;
        }
        Ok(())
    }

    pub fn fast_return(&self) -> Result<bool> {
        let fast_return = self.fast_return.read();
        Ok(*fast_return)
    }

    pub fn need_outer_scan(&self) -> bool {
        matches!(
            self.hash_join_desc.join_type,
            JoinType::Full
                | JoinType::Right
                | JoinType::RightSingle
                | JoinType::RightSemi
                | JoinType::RightAnti
        )
    }

    pub fn need_mark_scan(&self) -> bool {
        matches!(self.hash_join_desc.join_type, JoinType::LeftMark)
    }

    pub fn set_spilled_partition(&self, partitions: &HashSet<u8>) {
        let mut spill_partition = self.spill_partition.write();
        *spill_partition = partitions.clone();
    }

    #[async_backtrace::framed]
    pub(crate) async fn wait_probe_spill(&self) {
        if *self.probe_spill_done.lock() {
            return;
        }
        self.probe_spill_done_notify.notified().await;
    }

    #[async_backtrace::framed]
    pub(crate) async fn wait_final_scan(&self) {
        if *self.final_probe_done.lock() {
            return;
        }
        self.final_probe_done_notify.notified().await;
    }
}
