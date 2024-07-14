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

use databend_common_arrow::arrow::bitmap::MutableBitmap;
use databend_common_base::runtime::profile::Profile;
use databend_common_base::runtime::profile::ProfileStatisticsName;
use databend_common_catalog::plan::PartInfoPtr;
use databend_common_exception::Result;
use databend_common_expression::types::NumberColumn;
use databend_common_expression::Column;
use databend_common_expression::ConstantFolder;
use databend_common_expression::DataBlock;
use databend_common_expression::Expr;
use databend_common_expression::FunctionContext;
use databend_common_expression::HashMethod;
use databend_common_expression::HashMethodKind;
use databend_common_expression::InputColumnsWithDataType;
use databend_common_expression::KeysState;
use databend_common_expression::KeysState::U128;
use databend_common_expression::KeysState::U256;
use databend_common_expression::Scalar;
use databend_common_expression::TableSchema;
use databend_common_functions::BUILTIN_FUNCTIONS;
use databend_common_hashtable::FastHash;
use databend_storages_common_index::statistics_to_domain;
use log::debug;
use log::info;
use xorf::BinaryFuse16;
use xorf::Filter;

use crate::FuseBlockPartInfo;

pub fn runtime_filter_pruner(
    table_schema: Arc<TableSchema>,
    part: &PartInfoPtr,
    filters: &[Expr<String>],
    func_ctx: &FunctionContext,
) -> Result<bool> {
    if filters.is_empty() {
        return Ok(false);
    }
    let part = FuseBlockPartInfo::from_part(part)?;
    let pruned = filters.iter().any(|filter| {
        let column_refs = filter.column_refs();
        // Currently only support filter with one column(probe key).
        debug_assert!(column_refs.len() == 1);
        let ty = column_refs.values().last().unwrap();
        let name = column_refs.keys().last().unwrap();
        if let Some(stats) = &part.columns_stat {
            let column_ids = table_schema.leaf_columns_of(name);
            if column_ids.len() != 1 {
                return false;
            }
            debug_assert!(column_ids.len() == 1);
            if let Some(stat) = stats.get(&column_ids[0]) {
                let stats = vec![stat];
                let domain = statistics_to_domain(stats, ty);

                let mut input_domains = HashMap::new();
                input_domains.insert(name.to_string(), domain.clone());

                let (new_expr, _) = ConstantFolder::fold_with_domain(
                    filter,
                    &input_domains,
                    func_ctx,
                    &BUILTIN_FUNCTIONS,
                );
                debug!("Runtime filter after constant fold is {:?}", new_expr.sql_display());
                return matches!(new_expr, Expr::Constant {
                    scalar: Scalar::Boolean(false),
                    ..
                });
            }
        }
        info!("Can't prune the partition by runtime filter, because there is no statistics for the partition");
        false
    });

    if pruned {
        info!(
            "Pruned partition with {:?} rows by runtime filter",
            part.nums_rows
        );
        Profile::record_usize_profile(ProfileStatisticsName::RuntimeFilterPruneParts, 1);
    }

    Ok(pruned)
}

pub(crate) fn update_bitmap_with_bloom_filter(
    column: Column,
    filter: &BinaryFuse16,
    bitmap: &mut MutableBitmap,
) -> Result<()> {
    let data_type = column.data_type();
    let num_rows = column.len();
    let method = DataBlock::choose_hash_method_with_types(&[data_type.clone()], false)?;
    let columns = &[column];
    let data_types = &[&data_type];
    let group_columns = InputColumnsWithDataType::new(columns, data_types);
    let mut idx = 0;
    match method {
        HashMethodKind::Serializer(method) => {
            let key_state = method.build_keys_state(group_columns, num_rows)?;
            match key_state {
                KeysState::Column(Column::Binary(col)) => col.iter().for_each(|key| {
                    let hash = key.fast_hash();
                    if filter.contains(&hash) {
                        bitmap.set(idx, true);
                    }
                    idx += 1;
                }),
                _ => unreachable!(),
            }
        }
        HashMethodKind::DictionarySerializer(method) => {
            let key_state = method.build_keys_state(group_columns, num_rows)?;
            match key_state {
                KeysState::Dictionary { dictionaries, .. } => dictionaries.iter().for_each(|key| {
                    let hash = key.fast_hash();
                    if filter.contains(&hash) {
                        bitmap.set(idx, true);
                    }
                    idx += 1;
                }),
                _ => unreachable!(),
            }
        }
        HashMethodKind::SingleBinary(method) => {
            let key_state = method.build_keys_state(group_columns, num_rows)?;
            match key_state {
                KeysState::Column(Column::Binary(col))
                | KeysState::Column(Column::Variant(col))
                | KeysState::Column(Column::Bitmap(col)) => col.iter().for_each(|key| {
                    let hash = key.fast_hash();
                    if filter.contains(&hash) {
                        bitmap.set(idx, true);
                    }
                    idx += 1;
                }),
                KeysState::Column(Column::String(col)) => col.iter_binary().for_each(|key| {
                    let hash = key.fast_hash();
                    if filter.contains(&hash) {
                        bitmap.set(idx, true);
                    }
                    idx += 1;
                }),
                _ => unreachable!(),
            }
        }
        HashMethodKind::KeysU8(hash_method) => {
            let key_state = hash_method.build_keys_state(group_columns, num_rows)?;
            match key_state {
                KeysState::Column(Column::Number(NumberColumn::UInt8(c))) => {
                    c.iter().for_each(|key| {
                        let hash = key.fast_hash();
                        if filter.contains(&hash) {
                            bitmap.set(idx, true);
                        }
                        idx += 1;
                    })
                }
                _ => unreachable!(),
            }
        }
        HashMethodKind::KeysU16(hash_method) => {
            let key_state = hash_method.build_keys_state(group_columns, num_rows)?;
            match key_state {
                KeysState::Column(Column::Number(NumberColumn::UInt16(c))) => {
                    c.iter().for_each(|key| {
                        let hash = key.fast_hash();
                        if filter.contains(&hash) {
                            bitmap.set(idx, true);
                        }
                        idx += 1;
                    })
                }
                _ => unreachable!(),
            }
        }
        HashMethodKind::KeysU32(hash_method) => {
            let key_state = hash_method.build_keys_state(group_columns, num_rows)?;
            match key_state {
                KeysState::Column(Column::Number(NumberColumn::UInt32(c))) => {
                    c.iter().for_each(|key| {
                        let hash = key.fast_hash();
                        if filter.contains(&hash) {
                            bitmap.set(idx, true);
                        }
                        idx += 1;
                    })
                }
                _ => unreachable!(),
            }
        }
        HashMethodKind::KeysU64(hash_method) => {
            let key_state = hash_method.build_keys_state(group_columns, num_rows)?;
            match key_state {
                KeysState::Column(Column::Number(NumberColumn::UInt64(c))) => {
                    c.iter().for_each(|key| {
                        let hash = key.fast_hash();
                        if filter.contains(&hash) {
                            bitmap.set(idx, true);
                        }
                        idx += 1;
                    })
                }
                _ => unreachable!(),
            }
        }
        HashMethodKind::KeysU128(hash_method) => {
            let key_state = hash_method.build_keys_state(group_columns, num_rows)?;
            match key_state {
                U128(c) => c.iter().for_each(|key| {
                    let hash = key.fast_hash();
                    if filter.contains(&hash) {
                        bitmap.set(idx, true);
                    }
                    idx += 1;
                }),
                _ => unreachable!(),
            }
        }
        HashMethodKind::KeysU256(hash_method) => {
            let key_state = hash_method.build_keys_state(group_columns, num_rows)?;
            match key_state {
                U256(c) => c.iter().for_each(|key| {
                    let hash = key.fast_hash();
                    if filter.contains(&hash) {
                        bitmap.set(idx, true);
                    }
                    idx += 1;
                }),
                _ => unreachable!(),
            }
        }
    }
    Ok(())
}
