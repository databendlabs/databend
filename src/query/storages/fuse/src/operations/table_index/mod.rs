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

//! Table-index refresh entry points, planning and pipeline execution.

use std::sync::Arc;

use databend_common_catalog::table::Table;
use databend_common_catalog::table_context::TableContext;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::TableSchemaRef;
use databend_common_meta_app::schema::TableIndex;
use databend_common_meta_app::schema::TableIndexType;
use databend_common_pipeline::core::Pipeline;
use databend_storages_common_table_meta::meta::Location;

use crate::FuseTable;

mod inverted_index;
mod meta;
mod plan;
mod processors;
mod refresh;

pub async fn do_refresh_table_index(
    table: &FuseTable,
    ctx: Arc<dyn TableContext>,
    index_name: String,
    index_type: TableIndexType,
    index_schema: TableSchemaRef,
    segment_locs: Option<Vec<Location>>,
    pipeline: &mut Pipeline,
) -> Result<u64> {
    if !matches!(
        index_type,
        TableIndexType::Ngram | TableIndexType::Vector | TableIndexType::Spatial
    ) {
        return Err(ErrorCode::RefreshIndexError(format!(
            "Refresh index type {index_type} not support"
        )));
    }
    let index = get_refresh_index(table, &index_name)?;
    if index.index_type != index_type {
        return Err(ErrorCode::RefreshIndexError(format!(
            "Index {index_name} is not a {index_type} index"
        )));
    }
    check_index_schema(&index_name, index, &index_schema)?;
    table
        .do_refresh_indexes(ctx, &[index_name], segment_locs, pipeline)
        .await
}

fn get_refresh_index<'a>(table: &'a FuseTable, index_name: &str) -> Result<&'a TableIndex> {
    table
        .get_table_info()
        .meta
        .indexes
        .get(index_name)
        .ok_or_else(|| ErrorCode::RefreshIndexError(format!("Index: {index_name} not found")))
}

fn check_index_schema(
    index_name: &str,
    index: &TableIndex,
    index_schema: &TableSchemaRef,
) -> Result<()> {
    if index.column_ids
        != index_schema
            .fields()
            .iter()
            .map(|field| field.column_id())
            .collect::<Vec<_>>()
    {
        return Err(ErrorCode::RefreshIndexError(format!(
            "Index {index_name} definition changed before refresh"
        )));
    }
    Ok(())
}
