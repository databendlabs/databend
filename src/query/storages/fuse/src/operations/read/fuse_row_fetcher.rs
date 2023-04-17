//  Copyright 2023 Datafuse Labs.
//
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.

use std::collections::HashMap;
use std::sync::Arc;

use common_catalog::plan::compute_row_id_prefix;
use common_catalog::plan::DataSourcePlan;
use common_catalog::plan::PartInfoPtr;
use common_catalog::plan::Projection;
use common_catalog::table_context::TableContext;
use common_exception::ErrorCode;
use common_exception::Result;
use common_expression::TableSchemaRef;
use common_pipeline_core::Pipeline;
use common_storage::ColumnNodes;
use opendal::Operator;
use storages_common_cache::LoadParams;

use super::native_rows_fetcher::build_native_row_fetcher_pipeline;
use crate::io::MetaReaders;
use crate::FuseStorageFormat;
use crate::FuseTable;

// #[async_backtrace::framed]
pub(super) async fn build_partitions_map(
    snapshot_loc: String,
    ver: u64,
    operator: Operator,
    table_schema: TableSchemaRef,
    projection: &Projection,
) -> Result<HashMap<u64, PartInfoPtr>> {
    let arrow_schema = table_schema.to_arrow();
    let column_nodes = ColumnNodes::new_from_schema(&arrow_schema, Some(&table_schema));

    let snapshot = FuseTable::read_table_snapshot_impl(snapshot_loc, ver, operator.clone())
        .await?
        .ok_or_else(|| ErrorCode::Internal("No snapshot found"))?;
    let segment_id_map = snapshot.build_segment_id_map();
    let segment_reader = MetaReaders::segment_info_reader(operator, table_schema);
    let mut map = HashMap::new();

    for (location, seg_id) in segment_id_map.iter() {
        let segment_info = segment_reader
            .read(&LoadParams {
                location: location.clone(),
                len_hint: None,
                ver,
                put_cache: true,
            })
            .await?;
        for (block_idx, block_meta) in segment_info.blocks.iter().enumerate() {
            let part_info =
                FuseTable::projection_part(block_meta, &None, &column_nodes, None, projection);
            let part_id = compute_row_id_prefix(*seg_id as u64, block_idx as u64);
            map.insert(part_id, part_info);
        }
    }

    Ok(map)
}

pub fn build_row_fetcher_pipeline(
    ctx: Arc<dyn TableContext>,
    pipeline: &mut Pipeline,
    row_id_col_offset: usize,
    source: &DataSourcePlan,
    projection: Projection,
) -> Result<()> {
    let table = ctx.build_table_from_source_plan(source)?;
    let fuse_table = table
        .as_any()
        .downcast_ref::<FuseTable>()
        .ok_or_else(|| ErrorCode::Internal("Row fetcher is only supported by Fuse engine"))?;

    match &fuse_table.storage_format {
        FuseStorageFormat::Native => build_native_row_fetcher_pipeline(
            ctx,
            pipeline,
            row_id_col_offset,
            fuse_table,
            projection,
        ),
        FuseStorageFormat::Parquet => {
            todo!("row fetcher")
        }
    }
}
