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

use std::collections::hash_map::Entry;
use std::collections::HashMap;
use std::sync::Arc;

use databend_common_arrow::arrow::bitmap::MutableBitmap;
use databend_common_base::runtime::block_on;
use databend_common_base::runtime::profile::Profile;
use databend_common_base::runtime::profile::ProfileStatisticsName;
use databend_common_catalog::plan::PartInfoPtr;
use databend_common_catalog::table_context::TableContext;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::types::NumberColumn;
use databend_common_expression::Column;
use databend_common_expression::ConstantFolder;
use databend_common_expression::DataBlock;
use databend_common_expression::Expr;
use databend_common_expression::FunctionContext;
use databend_common_expression::HashMethod;
use databend_common_expression::HashMethodKind;
use databend_common_expression::KeysState;
use databend_common_expression::KeysState::U128;
use databend_common_expression::KeysState::U256;
use databend_common_expression::Scalar;
use databend_common_expression::TableSchema;
use databend_common_functions::BUILTIN_FUNCTIONS;
use databend_common_hashtable::FastHash;
use databend_storages_common_cache::LoadParams;
use databend_storages_common_index::statistics_to_domain;
use databend_storages_common_table_meta::meta::SegmentInfo;
use log::info;
use xorf::BinaryFuse16;
use xorf::Filter;

use super::util::MergeIntoSourceBuildBloomInfo;
use crate::io::MetaReaders;
use crate::operations::SegmentIndex;
use crate::FusePartInfo;
use crate::FuseTable;

pub fn runtime_filter_pruner(
    table_schema: Arc<TableSchema>,
    part: &PartInfoPtr,
    filters: &[Expr<String>],
    func_ctx: &FunctionContext,
    can_do_merge_into_target_build_bloom_filter: bool,
    ctx: Arc<dyn TableContext>,
    id: usize,
    merge_into_source_build_bloom_info: &mut MergeIntoSourceBuildBloomInfo,
) -> Result<bool> {
    if filters.is_empty() && !can_do_merge_into_target_build_bloom_filter {
        return Ok(false);
    }
    let part = FusePartInfo::from_part(part)?;
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
                if stat.min.is_null() {
                    return false;
                }
                debug_assert_eq!(stat.min().as_ref().infer_data_type(), ty.remove_nullable());
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
                return matches!(new_expr, Expr::Constant {
                    scalar: Scalar::Boolean(false),
                    ..
                });
            }
        }
        false
    });

    if pruned {
        info!(
            "Pruned partition with {:?} rows by runtime filter",
            part.nums_rows
        );
        Profile::record_usize_profile(ProfileStatisticsName::RuntimeFilterPruneParts, 1);
        return Ok(true);
    }

    // if we can't pruned this block, we can try get siphashkeys if this is a merge into source build
    if can_do_merge_into_target_build_bloom_filter {
        assert!(matches!(part.block_meta_index(), Some(_)));
        let block_meta_index = part.block_meta_index().unwrap();
        let hash_keys = ctx.get_merge_into_source_build_siphashkeys_with_id(id);
        let segment_idx = block_meta_index.segment_idx;
        let block_idx = block_meta_index.block_idx;
        let target_table_segments = ctx.get_merge_into_source_build_segments();
        if let Entry::Vacant(e) = merge_into_source_build_bloom_info
            .segment_infos
            .entry(segment_idx)
        {
            let (_,(path, ver)) = target_table_segments.get(segment_idx).ok_or_else(|| {
                return ErrorCode::Internal(format!(
                    "unexpected, segment (idx {}) not found, during do merge into source build bloom filter",
                    segment_idx
                ))
            })?;

            let load_param = LoadParams {
                location: path.clone(),
                len_hint: None,
                ver: *ver,
                put_cache: true,
            };
            let catalog_info = merge_into_source_build_bloom_info
                .catalog_info
                .as_ref()
                .unwrap();
            let table_info = merge_into_source_build_bloom_info
                .table_info
                .as_ref()
                .unwrap();
            let table = block_on(async {
                ctx.get_table(
                    catalog_info.catalog_name(),
                    &merge_into_source_build_bloom_info.database_name,
                    &table_info.name,
                )
                .await
            })?;
            let fuse_table = table.as_any().downcast_ref::<FuseTable>().ok_or_else(|| {
                ErrorCode::Unimplemented(format!(
                    "table {}, engine type {}, does not support MERGE INTO",
                    table.name(),
                    table.get_table_info().engine(),
                ))
            })?;
            let target_table_schema = table.schema_with_stream();
            let data_accessor = fuse_table.get_operator();
            let segment_reader = MetaReaders::segment_info_reader(
                data_accessor.clone(),
                target_table_schema.clone(),
            );
            let compact_segment_info = block_on(async { segment_reader.read(&load_param).await })?;
            let segment_info: SegmentInfo = compact_segment_info.try_into()?;
            e.insert(segment_info);
        }
        // load bloom filter
        let segment_info = merge_into_source_build_bloom_info
            .segment_infos
            .get(&segment_idx)
            .unwrap();
        assert!(block_idx < segment_info.blocks.len());
        info!(
            "merge into source build runtime bloom filter: segment_idx:{},blk_idx:{}",
            segment_idx, block_idx
        );
        let block_meta = segment_info.blocks[block_idx].clone();
    }

    Ok(false)
}

pub(crate) fn update_bitmap_with_bloom_filter(
    column: Column,
    filter: &BinaryFuse16,
    bitmap: &mut MutableBitmap,
) -> Result<()> {
    let data_type = column.data_type();
    let num_rows = column.len();
    let method = DataBlock::choose_hash_method_with_types(&[data_type.clone()], false)?;
    let mut idx = 0;
    match method {
        HashMethodKind::Serializer(method) => {
            let key_state = method.build_keys_state(&[(column, data_type)], num_rows)?;
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
            let key_state = method.build_keys_state(&[(column, data_type)], num_rows)?;
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
            let key_state = method.build_keys_state(&[(column, data_type)], num_rows)?;
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
            let key_state = hash_method.build_keys_state(&[(column, data_type)], num_rows)?;
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
            let key_state = hash_method.build_keys_state(&[(column, data_type)], num_rows)?;
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
            let key_state = hash_method.build_keys_state(&[(column, data_type)], num_rows)?;
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
            let key_state = hash_method.build_keys_state(&[(column, data_type)], num_rows)?;
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
            let key_state = hash_method.build_keys_state(&[(column, data_type)], num_rows)?;
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
            let key_state = hash_method.build_keys_state(&[(column, data_type)], num_rows)?;
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
