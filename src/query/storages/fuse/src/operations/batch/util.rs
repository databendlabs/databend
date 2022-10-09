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
//

use std::collections::BTreeMap;
use std::sync::Arc;

use common_cache::Cache;
use common_catalog::table_context::TableContext;
use common_exception::ErrorCode;
use common_exception::Result;
use common_fuse_meta::caches::CacheManager;
use common_fuse_meta::meta::Statistics;
use common_fuse_meta::meta::TableSnapshot;
use common_meta_app::schema::TableInfo;
use common_meta_app::schema::TableStatistics;
use common_meta_app::schema::UpdateTableMetaReq;
use common_meta_types::MatchSeq;
use common_storages_util::table_option_keys::OPT_KEY_LEGACY_SNAPSHOT_LOC;
use common_storages_util::table_option_keys::OPT_KEY_SNAPSHOT_LOCATION;
use opendal::Operator;

use crate::io::write_meta;
use crate::io::TableMetaLocationGenerator;
use crate::operations::AppendOperationLogEntry;
use crate::operations::TableOperationLog;
use crate::statistics::reduce_block_statistics;

#[inline]
pub async fn abort_operations(
    ctx: &dyn TableContext,
    operation_log: TableOperationLog,
) -> Result<()> {
    let operator = ctx.get_storage_operator()?;

    for entry in operation_log {
        for block in &entry.segment_info.blocks {
            let block_location = &block.location.0;
            // if deletion operation failed (after DAL retried)
            // we just left them there, and let the "major GC" collect them
            let _ = operator.object(block_location).delete().await;
        }
        let _ = operator.object(&entry.segment_location).delete().await;
    }
    Ok(())
}

#[inline]
pub fn is_error_recoverable(e: &ErrorCode, is_table_transient: bool) -> bool {
    let code = e.code();
    code == ErrorCode::table_version_mismatched_code()
        || (is_table_transient && code == ErrorCode::storage_not_found_code())
}

pub fn merge_append_operations<'a>(
    mut append_log_entries: impl Iterator<Item = &'a AppendOperationLogEntry>,
) -> Result<(Vec<String>, Statistics)> {
    let (s, seg_locs) = append_log_entries.try_fold(
        (Statistics::default(), Vec::new()),
        |(mut acc, mut segs), log_entry| {
            let loc = &log_entry.segment_location;
            let stats = &log_entry.segment_info.summary;
            acc.row_count += stats.row_count;
            acc.block_count += stats.block_count;
            acc.uncompressed_byte_size += stats.uncompressed_byte_size;
            acc.compressed_byte_size += stats.compressed_byte_size;
            acc.index_size = stats.index_size;
            acc.col_stats = if acc.col_stats.is_empty() {
                stats.col_stats.clone()
            } else {
                reduce_block_statistics(&[&acc.col_stats, &stats.col_stats])?
            };
            segs.push(loc.clone());
            Ok::<_, ErrorCode>((acc, segs))
        },
    )?;

    Ok((seg_locs, s))
}

pub async fn commit_to_meta_server(
    ctx: &dyn TableContext,
    catalog_name: &str,
    table_info: &TableInfo,
    location_generator: &TableMetaLocationGenerator,
    snapshot: TableSnapshot,
) -> Result<()> {
    let snapshot_location = location_generator
        .snapshot_location_from_uuid(&snapshot.snapshot_id, snapshot.format_version())?;

    // 1. write down snapshot
    let operator = ctx.get_storage_operator()?;
    write_meta(&operator, &snapshot_location, &snapshot).await?;

    // 2. prepare table meta
    let mut new_table_meta = table_info.meta.clone();
    // 2.1 set new snapshot location
    new_table_meta.options.insert(
        OPT_KEY_SNAPSHOT_LOCATION.to_owned(),
        snapshot_location.clone(),
    );
    // remove legacy options
    remove_legacy_options(&mut new_table_meta.options);

    // 2.2 setup table statistics
    let stats = &snapshot.summary;
    // update statistics
    new_table_meta.statistics = TableStatistics {
        number_of_rows: stats.row_count,
        data_bytes: stats.uncompressed_byte_size,
        compressed_data_bytes: stats.compressed_byte_size,
        index_data_bytes: stats.index_size,
    };

    // 3. prepare the request

    let catalog = ctx.get_catalog(catalog_name)?;
    let table_id = table_info.ident.table_id;
    let table_version = table_info.ident.seq;

    let req = UpdateTableMetaReq {
        table_id,
        seq: MatchSeq::Exact(table_version),
        new_table_meta,
    };

    // 3. let's roll
    let tenant = ctx.get_tenant();
    let db = ctx.get_current_database();
    let reply = catalog.update_table_meta(&tenant, &db, req).await;
    match reply {
        Ok(_) => {
            if let Some(snapshot_cache) = CacheManager::instance().get_table_snapshot_cache() {
                let cache = &mut snapshot_cache.write();
                cache.put(snapshot_location.clone(), Arc::new(snapshot));
            }
            // try keep a hit file of last snapshot
            write_last_snapshot_hint(&operator, location_generator, snapshot_location).await;
            Ok(())
        }
        Err(e) => {
            // commit snapshot to meta server failed, try to delete it.
            // "major GC" will collect this, if deletion failure (even after DAL retried)
            let _ = operator.object(&snapshot_location).delete().await;
            Err(e)
        }
    }
}

// check if there are any fuse table legacy options
pub fn remove_legacy_options(table_options: &mut BTreeMap<String, String>) {
    table_options.remove(OPT_KEY_LEGACY_SNAPSHOT_LOC);
}

// Left a hint file which indicates the location of the latest snapshot
pub async fn write_last_snapshot_hint(
    operator: &Operator,
    location_generator: &TableMetaLocationGenerator,
    last_snapshot_path: String,
) {
    let hint_path = location_generator.gen_last_snapshot_hint_location();
    let hint_path = hint_path.clone();
    let last_snapshot_path = {
        let operator_meta_data = operator.metadata();
        let storage_prefix = operator_meta_data.root();
        format!("{}{}", storage_prefix, last_snapshot_path)
    };

    // errors of writing down hint file are ignored.

    // TODO bring back retry which is removed in
    // https://github.com/datafuselabs/databend/pull/7905
    // Lacking knowledge of operation semantic, data access layer alone seems to be unable
    // to decide which operations are retryable
    let _ = operator.object(&hint_path).write(last_snapshot_path).await;
}
