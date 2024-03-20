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

use databend_common_catalog::merge_into_join::MergeIntoJoin;
use databend_common_catalog::merge_into_join::MergeIntoJoinType;
use databend_common_catalog::plan::gen_mutation_stream_meta;
use databend_common_catalog::plan::InternalColumnMeta;
use databend_common_catalog::table_context::TableContext;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::BlockMetaInfoPtr;
use databend_common_expression::DataBlock;
use databend_common_expression::FieldIndex;
use databend_common_expression::Scalar;
use databend_common_meta_app::schema::CatalogInfo;
use databend_common_meta_app::schema::TableInfo;
use databend_storages_common_table_meta::meta::SegmentInfo;

use crate::operations::BlockMetaIndex;
use crate::operations::SegmentIndex;
use crate::FusePartInfo;

pub struct MergeIntoSourceBuildBloomInfo {
    pub can_do_merge_into_rumtime_filter_bloom: bool,
    pub segment_infos: HashMap<SegmentIndex, SegmentInfo>,
    pub table_info: Option<TableInfo>,
    pub catalog_info: Option<CatalogInfo>,
    pub database_name: String,
    pub bloom_indexes: Vec<FieldIndex>,
}

pub fn need_reserve_block_info(ctx: Arc<dyn TableContext>, table_idx: usize) -> (bool, bool) {
    let merge_into_join = ctx.get_merge_into_join();
    (
        matches!(
            merge_into_join.merge_into_join_type,
            MergeIntoJoinType::Left
        ) && merge_into_join.target_tbl_idx == table_idx
            && !merge_into_join.is_distributed, /* we don't support distributed mod for target build optimization for now, */
        merge_into_join.is_distributed,
    )
}

pub fn can_merge_into_target_build_bloom_filter(
    ctx: Arc<dyn TableContext>,
    table_idx: usize,
) -> Result<bool> {
    let merge_into_join = ctx.get_merge_into_join();
    let enabled = matches!(
        merge_into_join.merge_into_join_type,
        MergeIntoJoinType::Right
    ) && merge_into_join.target_tbl_idx == table_idx;
    if enabled {
        assert!(
            ctx.get_settings()
                .get_enable_merge_into_source_build_bloom()?
        );
        assert!(merge_into_join.database_name.as_str() != "");
        assert!(matches!(merge_into_join.catalog_info, Some(_)));
        assert!(matches!(merge_into_join.table_info, Some(_)));
        assert!(matches!(merge_into_join.table_schema, Some(_)));
    }
    Ok(enabled)
}

pub(crate) fn add_data_block_meta(
    block: DataBlock,
    fuse_part: &FusePartInfo,
    offsets: Option<Vec<usize>>,
    base_block_ids: Option<Scalar>,
    update_stream_columns: bool,
    query_internal_columns: bool,
    need_reserve_block_info: bool,
) -> Result<DataBlock> {
    // for merge into target build
    let mut meta: Option<BlockMetaInfoPtr> =
        if need_reserve_block_info && fuse_part.block_meta_index.is_some() {
            let block_meta_index = fuse_part.block_meta_index.as_ref().unwrap();
            Some(Box::new(BlockMetaIndex {
                segment_idx: block_meta_index.segment_idx,
                block_idx: block_meta_index.block_id,
            }))
        } else {
            None
        };

    if update_stream_columns {
        // Fill `BlockMetaInfoPtr` if update stream columns
        let stream_meta = gen_mutation_stream_meta(meta, &fuse_part.location)?;
        meta = Some(Box::new(stream_meta));
    }

    if query_internal_columns {
        // Fill `BlockMetaInfoPtr` if query internal columns
        let block_meta = fuse_part.block_meta_index().unwrap();
        let internal_column_meta = InternalColumnMeta {
            segment_idx: block_meta.segment_idx,
            block_id: block_meta.block_id,
            block_location: block_meta.block_location.clone(),
            segment_location: block_meta.segment_location.clone(),
            snapshot_location: block_meta.snapshot_location.clone(),
            offsets,
            base_block_ids,
            inner: meta,
        };
        meta = Some(Box::new(internal_column_meta));
    }
    block.add_meta(meta)
}

pub fn build_merge_into_source_build_bloom_info(
    ctx: Arc<dyn TableContext>,
    table_index: usize,
    merge_into_join: MergeIntoJoin,
) -> Result<MergeIntoSourceBuildBloomInfo> {
    let enabled_bloom_filter = can_merge_into_target_build_bloom_filter(ctx.clone(), table_index)?;
    let bloom_indexes = if enabled_bloom_filter {
        ctx.get_merge_into_source_build_bloom_probe_keys(table_index)
            .iter()
            .try_fold(Vec::new(), |mut acc, probe_key_name| {
                let table_schema = merge_into_join.table_schema.as_ref().ok_or_else(|| {
                    ErrorCode::Internal(
                        "can't get merge into target table schema when build bloom info, it's a bug",
                    )
                })?;
                let index = table_schema.index_of(probe_key_name)?;
                acc.push(index);
                Ok::<_, ErrorCode>(acc)
            })?
    } else {
        vec![]
    };

    Ok(MergeIntoSourceBuildBloomInfo {
        can_do_merge_into_rumtime_filter_bloom: enabled_bloom_filter,
        segment_infos: Default::default(),
        catalog_info: merge_into_join.catalog_info.clone(),
        table_info: merge_into_join.table_info.clone(),
        database_name: merge_into_join.database_name.clone(),
        bloom_indexes,
    })
}
