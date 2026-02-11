// Copyright 2023 Databend Cloud
//
// Licensed under the Elastic License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.elastic.co/licensing/elastic-license
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::collections::HashSet;
use std::sync::Arc;

use databend_common_catalog::table_context::TableContext;
use databend_common_exception::Result;
use databend_common_meta_app::schema::ListHistoryTableBranchesReq;
use databend_common_meta_app::schema::TableIdent;
use databend_common_meta_app::schema::TableMeta;
use databend_common_storages_fuse::FuseTable;
use databend_meta_types::SeqV;
use databend_storages_common_cache::Table;
use databend_storages_common_table_meta::meta::Location;

pub(crate) fn branch_table_from_meta(
    fuse_table: &FuseTable,
    ctx: &Arc<dyn TableContext>,
    branch_name: &str,
    branch_id: u64,
    branch_meta: SeqV<TableMeta>,
) -> Result<FuseTable> {
    let mut branch_table_info = fuse_table.get_table_info().clone();
    branch_table_info.ident = TableIdent::new(branch_id, branch_meta.seq);
    branch_table_info.meta = branch_meta.data;
    branch_table_info.desc = format!("{} / '{}'", branch_table_info.desc, branch_name);
    Ok(*FuseTable::create_without_refresh_table_info(
        branch_table_info,
        ctx.get_settings().get_s3_storage_class()?,
    )?)
}

#[async_backtrace::framed]
pub(crate) async fn collect_retainable_history_branch_tables(
    fuse_table: &FuseTable,
    ctx: &Arc<dyn TableContext>,
) -> Result<Vec<FuseTable>> {
    let table_id = fuse_table.get_id();
    let catalog = ctx
        .get_catalog(fuse_table.get_table_info().catalog())
        .await?;
    let history_branches = catalog
        .list_history_table_branches(ListHistoryTableBranchesReq {
            table_id,
            only_retainable: true,
        })
        .await?;

    let mut branch_tables = Vec::with_capacity(history_branches.len());
    for branch in history_branches {
        branch_tables.push(branch_table_from_meta(
            fuse_table,
            ctx,
            &branch.branch_name,
            branch.branch_id.table_id,
            branch.branch_meta,
        )?);
    }
    Ok(branch_tables)
}

pub(crate) fn split_segments_by_prefix(
    segments: HashSet<Location>,
    first_prefix: &str,
    second_prefix: &str,
) -> (HashSet<Location>, HashSet<Location>) {
    let mut first = HashSet::new();
    let mut second = HashSet::new();

    for segment in segments {
        if segment.0.starts_with(first_prefix) {
            first.insert(segment);
        } else if segment.0.starts_with(second_prefix) {
            second.insert(segment);
        }
    }

    (first, second)
}

pub(crate) fn split_locations_by_prefix(
    locations: HashSet<String>,
    first_prefix: &str,
    second_prefix: &str,
) -> (HashSet<String>, HashSet<String>) {
    let mut first = HashSet::new();
    let mut second = HashSet::new();

    for location in locations {
        if location.starts_with(first_prefix) {
            first.insert(location);
        } else if location.starts_with(second_prefix) {
            second.insert(location);
        }
    }

    (first, second)
}
