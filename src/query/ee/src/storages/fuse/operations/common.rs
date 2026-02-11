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

use chrono::Duration;
use chrono::Utc;
use databend_common_catalog::table_context::TableContext;
use databend_common_exception::Result;
use databend_common_meta_app::schema::HistoryTableBranchMetaItem;
use databend_common_meta_app::schema::ListHistoryTableBranchesReq;
use databend_common_storages_fuse::FuseTable;
use databend_storages_common_cache::Table;
use databend_storages_common_table_meta::meta::Location;

#[async_backtrace::framed]
pub(crate) async fn collect_retainable_history_branch_tables(
    fuse_table: &FuseTable,
    ctx: &Arc<dyn TableContext>,
) -> Result<Vec<HistoryTableBranchMetaItem>> {
    let table_id = fuse_table.get_id();
    let catalog = ctx
        .get_catalog(fuse_table.get_table_info().catalog())
        .await?;
    let retention_boundary =
        Utc::now() - Duration::days(ctx.get_settings().get_data_retention_time_in_days()? as i64);
    let history_branches = catalog
        .list_history_table_branches(ListHistoryTableBranchesReq {
            table_id,
            retention_boundary: Some(retention_boundary),
        })
        .await?;
    Ok(history_branches)
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
