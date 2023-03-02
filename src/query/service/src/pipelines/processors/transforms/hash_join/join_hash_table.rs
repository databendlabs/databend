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

use common_arrow::arrow::bitmap::Bitmap;
use common_arrow::arrow::bitmap::MutableBitmap;
use common_base::base::tokio::sync::Notify;
use common_exception::Result;
use common_expression::arrow::and_validities;
use common_expression::with_hash_method;
use common_expression::DataBlock;
use common_expression::DataSchemaRef;
use common_expression::Evaluator;
use common_expression::HashMethod;
use common_expression::HashMethodFixedKeys;
use common_expression::HashMethodKind;
use common_expression::HashMethodSerializer;
use common_expression::HashMethodSingleString;
use common_expression::RemoteExpr;
use common_functions::scalars::BUILTIN_FUNCTIONS;
use common_hashtable::HashMap;
use common_hashtable::HashtableKeyable;
use common_hashtable::ShortStringHashMap;
use common_hashtable::StringHashMap;
use common_sql::plans::JoinType;
use ethnum::U256;
use parking_lot::RwLock;

use super::ProbeState;
use crate::pipelines::processors::transforms::hash_join::desc::HashJoinDesc;
use crate::pipelines::processors::transforms::hash_join::row::RowPtr;
use crate::pipelines::processors::transforms::hash_join::row::RowSpace;
use crate::pipelines::processors::transforms::hash_join::util::build_schema_wrap_nullable;
use crate::pipelines::processors::transforms::hash_join::util::probe_schema_wrap_nullable;
use crate::sessions::QueryContext;
use crate::sessions::TableContext;

pub struct SerializerHashTable {
    pub(crate) hash_table: StringHashMap<[u8], Vec<RowPtr>>,
    pub(crate) hash_method: HashMethodSerializer,
}

pub struct SingleStringHashTable {
    pub(crate) hash_table: ShortStringHashMap<[u8], Vec<RowPtr>>,
    pub(crate) hash_method: HashMethodSingleString,
}

pub struct FixedKeyHashTable<T: HashtableKeyable> {
    pub(crate) hash_table: HashMap<T, Vec<RowPtr>>,
    pub(crate) hash_method: HashMethodFixedKeys<T>,
}

pub enum HashTable {
    Serializer(SerializerHashTable),
    SingleString(SingleStringHashTable),
    KeysU8(FixedKeyHashTable<u8>),
    KeysU16(FixedKeyHashTable<u16>),
    KeysU32(FixedKeyHashTable<u32>),
    KeysU64(FixedKeyHashTable<u64>),
    KeysU128(FixedKeyHashTable<u128>),
    KeysU256(FixedKeyHashTable<U256>),
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
        build_keys: &[RemoteExpr],
        build_schema: DataSchemaRef,
        probe_schema: DataSchemaRef,
        hash_join_desc: HashJoinDesc,
    ) -> Result<Arc<JoinHashTable>> {
        let hash_key_types = build_keys
            .iter()
            .map(|expr| expr.as_expr(&BUILTIN_FUNCTIONS).data_type().clone())
            .collect::<Vec<_>>();
        let method = DataBlock::choose_hash_method_with_types(&hash_key_types)?;
        Ok(match method {
            HashMethodKind::Serializer(_) => Arc::new(JoinHashTable::try_create(
                ctx,
                HashTable::Serializer(SerializerHashTable {
                    hash_table: StringHashMap::<[u8], Vec<RowPtr>>::new(),
                    hash_method: HashMethodSerializer::default(),
                }),
                build_schema,
                probe_schema,
                hash_join_desc,
            )?),
            HashMethodKind::SingleString(_) => Arc::new(JoinHashTable::try_create(
                ctx,
                HashTable::SingleString(SingleStringHashTable {
                    hash_table: ShortStringHashMap::<[u8], Vec<RowPtr>>::new(),
                    hash_method: HashMethodSingleString::default(),
                }),
                build_schema,
                probe_schema,
                hash_join_desc,
            )?),
            HashMethodKind::KeysU8(hash_method) => Arc::new(JoinHashTable::try_create(
                ctx,
                HashTable::KeysU8(FixedKeyHashTable {
                    hash_table: HashMap::<u8, Vec<RowPtr>>::new(),
                    hash_method,
                }),
                build_schema,
                probe_schema,
                hash_join_desc,
            )?),
            HashMethodKind::KeysU16(hash_method) => Arc::new(JoinHashTable::try_create(
                ctx,
                HashTable::KeysU16(FixedKeyHashTable {
                    hash_table: HashMap::<u16, Vec<RowPtr>>::new(),
                    hash_method,
                }),
                build_schema,
                probe_schema,
                hash_join_desc,
            )?),
            HashMethodKind::KeysU32(hash_method) => Arc::new(JoinHashTable::try_create(
                ctx,
                HashTable::KeysU32(FixedKeyHashTable {
                    hash_table: HashMap::<u32, Vec<RowPtr>>::new(),
                    hash_method,
                }),
                build_schema,
                probe_schema,
                hash_join_desc,
            )?),
            HashMethodKind::KeysU64(hash_method) => Arc::new(JoinHashTable::try_create(
                ctx,
                HashTable::KeysU64(FixedKeyHashTable {
                    hash_table: HashMap::<u64, Vec<RowPtr>>::new(),
                    hash_method,
                }),
                build_schema,
                probe_schema,
                hash_join_desc,
            )?),
            HashMethodKind::KeysU128(hash_method) => Arc::new(JoinHashTable::try_create(
                ctx,
                HashTable::KeysU128(FixedKeyHashTable {
                    hash_table: HashMap::<u128, Vec<RowPtr>>::new(),
                    hash_method,
                }),
                build_schema,
                probe_schema,
                hash_join_desc,
            )?),
            HashMethodKind::KeysU256(hash_method) => Arc::new(JoinHashTable::try_create(
                ctx,
                HashTable::KeysU256(FixedKeyHashTable {
                    hash_table: HashMap::<U256, Vec<RowPtr>>::new(),
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
        let func_ctx = self.ctx.get_function_context()?;
        let mut input = (*input).clone();
        if matches!(
            self.hash_join_desc.join_type,
            JoinType::Right | JoinType::Full
        ) {
            let nullable_columns = input
                .columns()
                .iter()
                .map(|c| {
                    let mut validity = MutableBitmap::new();
                    validity.extend_constant(input.num_rows(), true);
                    let validity: Bitmap = validity.into();
                    Self::set_validity(c, validity.len(), &validity)
                })
                .collect::<Vec<_>>();
            input = DataBlock::new(nullable_columns, input.num_rows());
        }
        let evaluator = Evaluator::new(&input, func_ctx, &BUILTIN_FUNCTIONS);

        let probe_keys = self
            .hash_join_desc
            .probe_keys
            .iter()
            .map(|expr| {
                let return_type = expr.data_type();
                Ok((
                    evaluator
                        .run(expr)?
                        .convert_to_full_column(return_type, input.num_rows()),
                    return_type.clone(),
                ))
            })
            .collect::<Result<Vec<_>>>()?;

        if self.hash_join_desc.join_type == JoinType::RightMark {
            probe_state.markers = Some(Self::init_markers(&probe_keys, input.num_rows()));
        }

        if probe_keys
            .iter()
            .any(|(_, ty)| ty.is_nullable() || ty.is_null())
        {
            let mut valids = None;
            for (col, _) in probe_keys.iter() {
                let (is_all_null, tmp_valids) = col.validity();
                if is_all_null {
                    let mut m = MutableBitmap::with_capacity(input.num_rows());
                    m.extend_constant(input.num_rows(), false);
                    valids = Some(m.into());
                    break;
                } else {
                    valids = and_validities(valids, tmp_valids.cloned());
                }
            }
            probe_state.valids = valids;
        }

        with_hash_method!(|T| match &*self.hash_table.read() {
            HashTable::T(table) => {
                let keys_state = table
                    .hash_method
                    .build_keys_state(&probe_keys, input.num_rows())?;
                let keys_iter = table.hash_method.build_keys_iter(&keys_state)?;
                self.result_blocks(&table.hash_table, probe_state, keys_iter, &input)
            }
        })
    }
}
