// Copyright 2022 Datafuse Labs.
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

use std::sync::atomic::AtomicBool;
use std::sync::Arc;
use std::sync::Mutex;

use common_arrow::arrow::bitmap::MutableBitmap;
use common_base::base::tokio::sync::Notify;
use common_datablocks::DataBlock;
use common_datablocks::HashMethod;
use common_datablocks::HashMethodFixedKeys;
use common_datablocks::HashMethodKind;
use common_datablocks::HashMethodSerializer;
use common_datavalues::combine_validities_2;
use common_datavalues::ColumnRef;
use common_datavalues::DataSchemaRef;
use common_datavalues::DataTypeImpl;
use common_exception::Result;
use common_hashtable::HashMap;
use common_hashtable::HashtableKeyable;
use common_hashtable::UnsizedHashMap;
use common_sql::executor::PhysicalScalar;
use parking_lot::RwLock;
use primitive_types::U256;
use primitive_types::U512;

use super::ProbeState;
use crate::pipelines::processors::transforms::hash_join::desc::HashJoinDesc;
use crate::pipelines::processors::transforms::hash_join::row::RowPtr;
use crate::pipelines::processors::transforms::hash_join::row::RowSpace;
use crate::pipelines::processors::transforms::hash_join::util::build_schema_wrap_nullable;
use crate::pipelines::processors::transforms::hash_join::util::probe_schema_wrap_nullable;
use crate::sessions::QueryContext;
use crate::sessions::TableContext;
use crate::sql::planner::plans::JoinType;

pub struct SerializerHashTable {
    pub(crate) hash_table: UnsizedHashMap<[u8], Vec<RowPtr>>,
    pub(crate) hash_method: HashMethodSerializer,
}

pub struct FixedKeyHashTable<T: HashtableKeyable> {
    pub(crate) hash_table: HashMap<T, Vec<RowPtr>>,
    pub(crate) hash_method: HashMethodFixedKeys<T>,
}

pub enum HashTable {
    SerializerHashTable(SerializerHashTable),
    KeyU8HashTable(FixedKeyHashTable<u8>),
    KeyU16HashTable(FixedKeyHashTable<u16>),
    KeyU32HashTable(FixedKeyHashTable<u32>),
    KeyU64HashTable(FixedKeyHashTable<u64>),
    KeyU128HashTable(FixedKeyHashTable<u128>),
    KeyU256HashTable(FixedKeyHashTable<U256>),
    KeyU512HashTable(FixedKeyHashTable<U512>),
}

pub struct JoinHashTable {
    pub(crate) ctx: Arc<QueryContext>,
    /// Reference count
    pub(crate) ref_count: Mutex<usize>,
    pub(crate) is_finished: Mutex<bool>,
    /// A shared big hash table stores all the rows from build side
    pub(crate) hash_table: RwLock<HashTable>,
    pub(crate) row_space: RowSpace,
    pub(crate) hash_join_desc: HashJoinDesc,
    pub(crate) row_ptrs: RwLock<Vec<RowPtr>>,
    pub(crate) probe_schema: DataSchemaRef,
    pub(crate) interrupt: Arc<AtomicBool>,
    pub(crate) finished_notify: Arc<Notify>,
}

impl JoinHashTable {
    pub fn create_join_state(
        ctx: Arc<QueryContext>,
        build_keys: &[PhysicalScalar],
        build_schema: DataSchemaRef,
        probe_schema: DataSchemaRef,
        hash_join_desc: HashJoinDesc,
    ) -> Result<Arc<JoinHashTable>> {
        let hash_key_types: Vec<DataTypeImpl> =
            build_keys.iter().map(|expr| expr.data_type()).collect();
        let method = DataBlock::choose_hash_method_with_types(&hash_key_types)?;
        Ok(match method {
            HashMethodKind::Serializer(_) => Arc::new(JoinHashTable::try_create(
                ctx,
                HashTable::SerializerHashTable(SerializerHashTable {
                    hash_table: UnsizedHashMap::<[u8], Vec<RowPtr>>::new(),
                    hash_method: HashMethodSerializer::default(),
                }),
                build_schema,
                probe_schema,
                hash_join_desc,
            )?),
            HashMethodKind::KeysU8(hash_method) => Arc::new(JoinHashTable::try_create(
                ctx,
                HashTable::KeyU8HashTable(FixedKeyHashTable {
                    hash_table: HashMap::<u8, Vec<RowPtr>>::new(),
                    hash_method,
                }),
                build_schema,
                probe_schema,
                hash_join_desc,
            )?),
            HashMethodKind::KeysU16(hash_method) => Arc::new(JoinHashTable::try_create(
                ctx,
                HashTable::KeyU16HashTable(FixedKeyHashTable {
                    hash_table: HashMap::<u16, Vec<RowPtr>>::new(),
                    hash_method,
                }),
                build_schema,
                probe_schema,
                hash_join_desc,
            )?),
            HashMethodKind::KeysU32(hash_method) => Arc::new(JoinHashTable::try_create(
                ctx,
                HashTable::KeyU32HashTable(FixedKeyHashTable {
                    hash_table: HashMap::<u32, Vec<RowPtr>>::new(),
                    hash_method,
                }),
                build_schema,
                probe_schema,
                hash_join_desc,
            )?),
            HashMethodKind::KeysU64(hash_method) => Arc::new(JoinHashTable::try_create(
                ctx,
                HashTable::KeyU64HashTable(FixedKeyHashTable {
                    hash_table: HashMap::<u64, Vec<RowPtr>>::new(),
                    hash_method,
                }),
                build_schema,
                probe_schema,
                hash_join_desc,
            )?),
            HashMethodKind::KeysU128(hash_method) => Arc::new(JoinHashTable::try_create(
                ctx,
                HashTable::KeyU128HashTable(FixedKeyHashTable {
                    hash_table: HashMap::<u128, Vec<RowPtr>>::new(),
                    hash_method,
                }),
                build_schema,
                probe_schema,
                hash_join_desc,
            )?),
            HashMethodKind::KeysU256(hash_method) => Arc::new(JoinHashTable::try_create(
                ctx,
                HashTable::KeyU256HashTable(FixedKeyHashTable {
                    hash_table: HashMap::<U256, Vec<RowPtr>>::new(),
                    hash_method,
                }),
                build_schema,
                probe_schema,
                hash_join_desc,
            )?),
            HashMethodKind::KeysU512(hash_method) => Arc::new(JoinHashTable::try_create(
                ctx,
                HashTable::KeyU512HashTable(FixedKeyHashTable {
                    hash_table: HashMap::<U512, Vec<RowPtr>>::new(),
                    hash_method,
                }),
                build_schema,
                probe_schema,
                hash_join_desc,
            )?),
        })
    }

    pub fn try_create(
        ctx: Arc<QueryContext>,
        hash_table: HashTable,
        mut build_data_schema: DataSchemaRef,
        mut probe_data_schema: DataSchemaRef,
        hash_join_desc: HashJoinDesc,
    ) -> Result<Self> {
        if hash_join_desc.join_type == JoinType::Left
            || hash_join_desc.join_type == JoinType::Single
        {
            build_data_schema = build_schema_wrap_nullable(&build_data_schema);
        };
        if hash_join_desc.join_type == JoinType::Right {
            probe_data_schema = probe_schema_wrap_nullable(&probe_data_schema);
        }
        if hash_join_desc.join_type == JoinType::Full {
            build_data_schema = build_schema_wrap_nullable(&build_data_schema);
            probe_data_schema = probe_schema_wrap_nullable(&probe_data_schema);
        }
        Ok(Self {
            row_space: RowSpace::new(ctx.clone(), build_data_schema)?,
            ref_count: Mutex::new(0),
            is_finished: Mutex::new(false),
            hash_join_desc,
            ctx,
            hash_table: RwLock::new(hash_table),
            row_ptrs: RwLock::new(vec![]),
            probe_schema: probe_data_schema,
            finished_notify: Arc::new(Notify::new()),
            interrupt: Arc::new(AtomicBool::new(false)),
        })
    }

    pub(crate) fn probe_join(
        &self,
        input: &DataBlock,
        probe_state: &mut ProbeState,
    ) -> Result<Vec<DataBlock>> {
        let func_ctx = self.ctx.try_get_function_context()?;
        let probe_keys = self
            .hash_join_desc
            .probe_keys
            .iter()
            .map(|expr| Ok(expr.eval(&func_ctx, input)?.vector().clone()))
            .collect::<Result<Vec<ColumnRef>>>()?;

        if self.hash_join_desc.join_type == JoinType::RightMark {
            probe_state.markers = Some(Self::init_markers(&probe_keys, input.num_rows()));
        }
        let probe_keys = probe_keys.iter().collect::<Vec<&ColumnRef>>();

        if probe_keys.iter().any(|c| c.is_nullable() || c.is_null()) {
            let mut valids = None;
            for col in probe_keys.iter() {
                let (is_all_null, tmp_valids) = col.validity();
                if is_all_null {
                    let mut m = MutableBitmap::with_capacity(input.num_rows());
                    m.extend_constant(input.num_rows(), false);
                    valids = Some(m.into());
                    break;
                } else {
                    valids = combine_validities_2(valids, tmp_valids.cloned());
                }
            }
            probe_state.valids = valids;
        }

        match &*self.hash_table.read() {
            HashTable::SerializerHashTable(table) => {
                let keys_state = table
                    .hash_method
                    .build_keys_state(&probe_keys, input.num_rows())?;
                let keys_iter = table.hash_method.build_keys_iter(&keys_state)?;

                self.result_blocks(&table.hash_table, probe_state, keys_iter, input)
            }
            HashTable::KeyU8HashTable(table) => {
                let keys_state = table
                    .hash_method
                    .build_keys_state(&probe_keys, input.num_rows())?;
                let keys_iter = table.hash_method.build_keys_iter(&keys_state)?;
                self.result_blocks(&table.hash_table, probe_state, keys_iter, input)
            }
            HashTable::KeyU16HashTable(table) => {
                let keys_state = table
                    .hash_method
                    .build_keys_state(&probe_keys, input.num_rows())?;
                let keys_iter = table.hash_method.build_keys_iter(&keys_state)?;
                self.result_blocks(&table.hash_table, probe_state, keys_iter, input)
            }
            HashTable::KeyU32HashTable(table) => {
                let keys_state = table
                    .hash_method
                    .build_keys_state(&probe_keys, input.num_rows())?;
                let keys_iter = table.hash_method.build_keys_iter(&keys_state)?;
                self.result_blocks(&table.hash_table, probe_state, keys_iter, input)
            }
            HashTable::KeyU64HashTable(table) => {
                let keys_state = table
                    .hash_method
                    .build_keys_state(&probe_keys, input.num_rows())?;
                let keys_iter = table.hash_method.build_keys_iter(&keys_state)?;
                self.result_blocks(&table.hash_table, probe_state, keys_iter, input)
            }
            HashTable::KeyU128HashTable(table) => {
                let keys_state = table
                    .hash_method
                    .build_keys_state(&probe_keys, input.num_rows())?;
                let keys_iter = table.hash_method.build_keys_iter(&keys_state)?;
                self.result_blocks(&table.hash_table, probe_state, keys_iter, input)
            }
            HashTable::KeyU256HashTable(table) => {
                let keys_state = table
                    .hash_method
                    .build_keys_state(&probe_keys, input.num_rows())?;
                let keys_iter = table.hash_method.build_keys_iter(&keys_state)?;
                self.result_blocks(&table.hash_table, probe_state, keys_iter, input)
            }
            HashTable::KeyU512HashTable(table) => {
                let keys_state = table
                    .hash_method
                    .build_keys_state(&probe_keys, input.num_rows())?;
                let keys_iter = table.hash_method.build_keys_iter(&keys_state)?;
                self.result_blocks(&table.hash_table, probe_state, keys_iter, input)
            }
        }
    }
}
