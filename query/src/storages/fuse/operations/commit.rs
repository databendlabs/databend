//  Copyright 2021 Datafuse Labs.
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
//

use std::sync::Arc;
use std::time::Duration;
use std::time::Instant;

use backoff::backoff::Backoff;
use backoff::ExponentialBackoffBuilder;
use common_base::base::ProgressValues;
use common_cache::Cache;
use common_datavalues::DataSchema;
use common_exception::ErrorCode;
use common_exception::Result;
use common_meta_app::schema::TableInfo;
use common_meta_app::schema::TableStatistics;
use common_meta_app::schema::UpdateTableMetaReply;
use common_meta_app::schema::UpdateTableMetaReq;
use common_meta_types::MatchSeq;
use common_tracing::tracing;
use uuid::Uuid;

use crate::sessions::QueryContext;
use crate::sql::OPT_KEY_LEGACY_SNAPSHOT_LOC;
use crate::sql::OPT_KEY_SNAPSHOT_LOCATION;
use crate::storages::fuse::meta::Location;
use crate::storages::fuse::meta::SegmentInfo;
use crate::storages::fuse::meta::Statistics;
use crate::storages::fuse::meta::TableSnapshot;
use crate::storages::fuse::meta::Versioned;
use crate::storages::fuse::operations::AppendOperationLogEntry;
use crate::storages::fuse::operations::TableOperationLog;
use crate::storages::fuse::statistics;
use crate::storages::fuse::FuseTable;
use crate::storages::Table;

const OCC_DEFAULT_BACKOFF_INIT_DELAY_MS: Duration = Duration::from_millis(5);
const OCC_DEFAULT_BACKOFF_MAX_DELAY_MS: Duration = Duration::from_millis(20 * 1000);
const OCC_DEFAULT_BACKOFF_MAX_ELAPSED_MS: Duration = Duration::from_millis(120 * 1000);

impl FuseTable {
    pub async fn do_commit(
        &self,
        ctx: Arc<QueryContext>,
        catalog_name: impl AsRef<str>,
        operation_log: TableOperationLog,
        overwrite: bool,
    ) -> Result<()> {
        let tid = self.table_info.ident.table_id;

        let mut tbl = self;
        let mut latest: Arc<dyn Table>;

        let mut retry_times = 0;

        // The initial retry delay in millisecond. By default,  it is 5 ms.
        let init_delay = OCC_DEFAULT_BACKOFF_INIT_DELAY_MS;

        // The maximum  back off delay in millisecond, once the retry interval reaches this value, it stops increasing.
        // By default, it is 20 seconds.
        let max_delay = OCC_DEFAULT_BACKOFF_MAX_DELAY_MS;

        // The maximum elapsed time after the occ starts, beyond which there will be no more retries.
        // By default, it is 2 minutes
        let max_elapsed = OCC_DEFAULT_BACKOFF_MAX_ELAPSED_MS;

        // see https://aws.amazon.com/blogs/architecture/exponential-backoff-and-jitter/ for more
        // informations. (The strategy that crate backoff implements is “Equal Jitter”)

        // To simplify the settings, using fixed common values for randomization_factor and multiplier
        let mut backoff = ExponentialBackoffBuilder::new()
            .with_initial_interval(init_delay)
            .with_max_interval(max_delay)
            .with_randomization_factor(0.5)
            .with_multiplier(2.0)
            .with_max_elapsed_time(Some(max_elapsed))
            .build();

        let catalog_name = catalog_name.as_ref();
        loop {
            match tbl
                .try_commit(ctx.as_ref(), catalog_name, &operation_log, overwrite)
                .await
            {
                Ok(_) => break Ok(()),
                Err(e) if self::utils::is_error_recoverable(&e) => match backoff.next_backoff() {
                    Some(d) => {
                        let name = tbl.table_info.name.clone();
                        tracing::warn!(
                                "got error TableVersionMismatched, tx will be retried {} ms later. table name {}, identity {}",
                                d.as_millis(),
                                name.as_str(),
                                tbl.table_info.ident
                            );
                        common_base::base::tokio::time::sleep(d).await;

                        let catalog = ctx.get_catalog(catalog_name)?;
                        let (ident, meta) = catalog.get_table_meta_by_id(tid).await?;
                        let table_info: TableInfo = TableInfo {
                            ident,
                            desc: "".to_owned(),
                            name,
                            meta: meta.as_ref().clone(),
                        };
                        latest = catalog.get_table_by_info(&table_info)?;
                        tbl = FuseTable::try_from_table(latest.as_ref())?;
                        retry_times += 1;
                        continue;
                    }
                    None => {
                        tracing::info!("aborting operations");
                        let _ = self::utils::abort_operations(ctx.as_ref(), operation_log).await;
                        break Err(ErrorCode::OCCRetryFailure(format!(
                                "can not fulfill the tx after retries({} times, {} ms), aborted. table name {}, identity {}",
                                retry_times,
                                Instant::now().duration_since(backoff.start_time).as_millis(),
                                tbl.table_info.name.as_str(),
                                tbl.table_info.ident,
                            )));
                    }
                },
                Err(e) => break Err(e),
            }
        }
    }

    #[inline]
    pub async fn try_commit(
        &self,
        ctx: &QueryContext,
        catalog_name: &str,
        operation_log: &TableOperationLog,
        overwrite: bool,
    ) -> Result<()> {
        let prev = self.read_table_snapshot(ctx).await?;
        let prev_version = self.snapshot_format_version();
        let prev_timestamp = prev.as_ref().and_then(|v| v.timestamp);
        let schema = self.table_info.meta.schema.as_ref().clone();
        let (segments, summary) = Self::merge_append_operations(operation_log)?;

        let progress_values = ProgressValues {
            rows: summary.row_count as usize,
            bytes: summary.uncompressed_byte_size as usize,
        };
        ctx.get_write_progress().incr(&progress_values);

        let segments = segments
            .into_iter()
            .map(|loc| (loc, SegmentInfo::VERSION))
            .collect();
        let new_snapshot = if overwrite {
            TableSnapshot::new(
                Uuid::new_v4(),
                &prev_timestamp,
                prev.as_ref().map(|v| (v.snapshot_id, prev_version)),
                schema,
                summary,
                segments,
            )
        } else {
            Self::merge_table_operations(
                self.table_info.meta.schema.as_ref(),
                prev,
                prev_version,
                segments,
                summary,
            )?
        };

        let uuid = new_snapshot.snapshot_id;
        let snapshot_loc = self
            .meta_location_generator()
            .snapshot_location_from_uuid(&uuid, TableSnapshot::VERSION)?;
        let bytes = serde_json::to_vec(&new_snapshot)?;
        let operator = ctx.get_storage_operator()?;
        operator.object(&snapshot_loc).write(bytes).await?;

        let result = Self::commit_to_meta_server(
            ctx,
            catalog_name,
            self.get_table_info(),
            snapshot_loc.clone(),
            &new_snapshot.summary,
        )
        .await;

        match result {
            Ok(_) => {
                if let Some(snapshot_cache) =
                    ctx.get_storage_cache_manager().get_table_snapshot_cache()
                {
                    let cache = &mut snapshot_cache.write().await;
                    cache.put(snapshot_loc, Arc::new(new_snapshot));
                }
                Ok(())
            }
            Err(e) => {
                // commit snapshot to meta server failed, try to delete it.
                // "major GC" will collect this, if deletion failure (even after DAL retried)
                let _ = operator.object(&snapshot_loc).delete().await;
                Err(e)
            }
        }
    }

    fn merge_table_operations(
        schema: &DataSchema,
        previous: Option<Arc<TableSnapshot>>,
        prev_version: u64,
        mut new_segments: Vec<Location>,
        statistics: Statistics,
    ) -> Result<TableSnapshot> {
        // 1. merge stats with previous snapshot, if any
        let stats = if let Some(snapshot) = &previous {
            let summary = &snapshot.summary;
            statistics::merge_statistics(&statistics, summary)?
        } else {
            statistics
        };
        let prev_snapshot_id = previous.as_ref().map(|v| (v.snapshot_id, prev_version));
        let prev_snapshot_timestamp = previous.as_ref().and_then(|v| v.timestamp);

        // 2. merge segment locations with previous snapshot, if any
        if let Some(snapshot) = &previous {
            let mut segments = snapshot.segments.clone();
            new_segments.append(&mut segments)
        };

        let new_snapshot = TableSnapshot::new(
            Uuid::new_v4(),
            &prev_snapshot_timestamp,
            prev_snapshot_id,
            schema.clone(),
            stats,
            new_segments,
        );
        Ok(new_snapshot)
    }

    async fn commit_to_meta_server(
        ctx: &QueryContext,
        catalog_name: &str,
        table_info: &TableInfo,
        new_snapshot_location: String,
        stats: &Statistics,
    ) -> Result<UpdateTableMetaReply> {
        let catalog = ctx.get_catalog(catalog_name)?;

        let table_id = table_info.ident.table_id;
        let table_version = table_info.ident.seq;

        let mut new_table_meta = table_info.meta.clone();

        // set new snapshot location
        new_table_meta
            .options
            .insert(OPT_KEY_SNAPSHOT_LOCATION.to_owned(), new_snapshot_location);

        // remove legacy options
        self::utils::remove_legacy_options(&mut new_table_meta.options);

        // update statistics
        new_table_meta.statistics = TableStatistics {
            number_of_rows: stats.row_count,
            data_bytes: stats.uncompressed_byte_size,
            compressed_data_bytes: stats.compressed_byte_size,
            index_data_bytes: 0, // TODO we do not have it yet
        };

        let req = UpdateTableMetaReq {
            table_id,
            seq: MatchSeq::Exact(table_version),
            new_table_meta,
        };

        catalog.update_table_meta(req).await
    }

    pub fn merge_append_operations(
        append_log_entries: &[AppendOperationLogEntry],
    ) -> Result<(Vec<String>, Statistics)> {
        let (s, seg_locs) = append_log_entries.iter().try_fold(
            (
                Statistics::default(),
                Vec::with_capacity(append_log_entries.len()),
            ),
            |(mut acc, mut seg_acc), log_entry| {
                let loc = &log_entry.segment_location;
                let stats = &log_entry.segment_info.summary;
                acc.row_count += stats.row_count;
                acc.block_count += stats.block_count;
                acc.uncompressed_byte_size += stats.uncompressed_byte_size;
                acc.compressed_byte_size += stats.compressed_byte_size;
                (acc.col_stats, acc.cluster_stats) = if acc.col_stats.is_empty() {
                    (stats.col_stats.clone(), stats.cluster_stats.clone())
                } else {
                    (
                        statistics::reduce_block_statistics(&[&acc.col_stats, &stats.col_stats])?,
                        statistics::reduce_cluster_stats(&[
                            &acc.cluster_stats,
                            &stats.cluster_stats,
                        ]),
                    )
                };
                seg_acc.push(loc.clone());
                Ok::<_, ErrorCode>((acc, seg_acc))
            },
        )?;

        Ok((seg_locs, s))
    }
}

mod utils {
    use std::collections::BTreeMap;

    use super::*;
    #[inline]
    pub async fn abort_operations(
        ctx: &QueryContext,
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
    pub fn is_error_recoverable(e: &ErrorCode) -> bool {
        e.code() == ErrorCode::table_version_mismatched_code()
    }

    // check if there are any fuse table legacy options
    pub fn remove_legacy_options(table_options: &mut BTreeMap<String, String>) {
        table_options.remove(OPT_KEY_LEGACY_SNAPSHOT_LOC);
    }
}
