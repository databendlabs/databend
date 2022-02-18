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
use common_datavalues::DataSchema;
use common_exception::ErrorCode;
use common_exception::Result;
use common_meta_types::TableIdent;
use common_meta_types::TableInfo;
use common_meta_types::UpsertTableOptionReply;
use common_meta_types::UpsertTableOptionReq;
use common_tracing::tracing;
use futures::io::Cursor;
use uuid::Uuid;

use crate::catalogs::Catalog;
use crate::sessions::QueryContext;
use crate::storages::fuse::io;
use crate::storages::fuse::meta::Statistics;
use crate::storages::fuse::meta::TableSnapshot;
use crate::storages::fuse::operations::AppendOperationLogEntry;
use crate::storages::fuse::operations::TableOperationLog;
use crate::storages::fuse::statistics;
use crate::storages::fuse::FuseTable;
use crate::storages::fuse::TBL_OPT_KEY_SNAPSHOT_LOC;
use crate::storages::Table;

impl FuseTable {
    pub async fn do_commit(
        &self,
        ctx: Arc<QueryContext>,
        operation_log: TableOperationLog,
        overwrite: bool,
    ) -> Result<()> {
        let tid = self.table_info.ident.table_id;

        let mut tbl = self;
        let mut latest: Arc<dyn Table>;

        let mut retry_times = 0;

        let settings = ctx.get_settings();

        // The initial retry delay in millisecond. By default,  it is 5 ms.
        let init_delay = Duration::from_millis(settings.get_storage_occ_backoff_init_delay_ms()?);

        // The maximum  back off delay in millisecond, once the retry interval reaches this value, it stops increasing.
        // By default, it is 20 seconds.
        let max_delay = Duration::from_millis(settings.get_storage_occ_backoff_max_delay_ms()?);

        // The maximum elapsed time after the occ starts, beyond which there will be no more retries.
        // By default, it is 2 minutes
        let max_elapsed = Duration::from_millis(settings.get_storage_occ_backoff_max_elapsed_ms()?);

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

        loop {
            match tbl
                .try_commit(ctx.as_ref(), &operation_log, overwrite)
                .await
            {
                Ok(_) => break Ok(()),
                Err(e) if e.code() == ErrorCode::table_version_mismatched_code() => {
                    match backoff.next_backoff() {
                        Some(d) => {
                            let name = tbl.table_info.name.clone();
                            tracing::error!(
                                "got error TableVersionMismatched, tx will be retried {} ms later. table name {}, identity {}",
                                d.as_millis(),
                                name.as_str(),
                                tbl.table_info.ident
                            );
                            common_base::tokio::time::sleep(d).await;

                            let catalog = ctx.get_catalog();
                            let (ident, meta) = catalog.get_table_meta_by_id(tid).await?;
                            let table_info: TableInfo = TableInfo {
                                ident,
                                desc: "".to_owned(),
                                name,
                                meta: meta.as_ref().clone(),
                            };
                            latest = catalog.get_table_by_info(&table_info)?;
                            tbl = latest.as_any().downcast_ref::<FuseTable>().ok_or_else(|| {
                                ErrorCode::LogicalError(format!(
                                    "expects table engine FUSE, but got {}",
                                    latest.engine()
                                ))
                            })?;
                            retry_times += 1;
                            continue;
                        }
                        None => {
                            break Err(ErrorCode::OCCRetryFailure(format!(
                                "can not fulfill the tx after retries({} times, {} ms), aborted. table name {}, identity {}",
                                retry_times,
                                Instant::now().duration_since(backoff.start_time).as_millis(),
                                tbl.table_info.name.as_str(),
                                tbl.table_info.ident,
                            )));
                        }
                    }
                }
                Err(e) => break Err(e),
            }
        }
    }

    #[inline]
    pub async fn try_commit(
        &self,
        ctx: &QueryContext,
        operation_log: &TableOperationLog,
        overwrite: bool,
    ) -> Result<()> {
        let prev = self.read_table_snapshot(ctx).await?;
        let schema = self.table_info.meta.schema.as_ref().clone();
        let (segments, summary) = Self::merge_append_operations(&schema, operation_log)?;
        let rows_written = summary.row_count;
        let new_snapshot = if overwrite {
            TableSnapshot {
                snapshot_id: Uuid::new_v4(),
                prev_snapshot_id: prev.as_ref().map(|v| v.snapshot_id),
                schema,
                summary,
                segments,
            }
        } else {
            Self::merge_table_operations(
                self.table_info.meta.schema.as_ref(),
                prev,
                segments,
                summary,
            )?
        };

        let uuid = new_snapshot.snapshot_id;
        let snapshot_loc = io::snapshot_location(&uuid);
        let bytes = serde_json::to_vec(&new_snapshot)?;
        let operator = ctx.get_storage_operator().await?;
        operator
            .write(&snapshot_loc, bytes.len() as u64)
            .run(Box::new(Cursor::new(bytes)))
            .await
            .map_err(|e| ErrorCode::DalTransportError(e.to_string()))?;

        Self::commit_to_meta_server(ctx, &self.get_table_info().ident, snapshot_loc).await?;
        ctx.get_dal_context().inc_write_rows(rows_written as usize);
        Ok(())
    }

    fn merge_table_operations(
        schema: &DataSchema,
        previous: Option<Arc<TableSnapshot>>,
        mut new_segments: Vec<String>,
        statistics: Statistics,
    ) -> Result<TableSnapshot> {
        // 1. merge stats with previous snapshot, if any
        let stats = if let Some(snapshot) = &previous {
            let summary = &snapshot.summary;
            statistics::merge_statistics(schema, &statistics, summary)?
        } else {
            statistics
        };
        let prev_snapshot_id = previous.as_ref().map(|v| v.snapshot_id);

        // 2. merge segment locations with previous snapshot, if any
        if let Some(snapshot) = &previous {
            let mut segments = snapshot.segments.clone();
            new_segments.append(&mut segments)
        };

        let new_snapshot = TableSnapshot {
            snapshot_id: Uuid::new_v4(),
            prev_snapshot_id,
            schema: schema.clone(),
            summary: stats,
            segments: new_segments,
        };
        Ok(new_snapshot)
    }

    async fn commit_to_meta_server(
        ctx: &QueryContext,
        tbl_id: &TableIdent,
        new_snapshot_location: String,
    ) -> Result<UpsertTableOptionReply> {
        let table_id = tbl_id.table_id;
        let table_version = tbl_id.version;
        let catalog = ctx.get_catalog();
        catalog
            .upsert_table_option(UpsertTableOptionReq::new(
                &TableIdent {
                    table_id,
                    version: table_version,
                },
                TBL_OPT_KEY_SNAPSHOT_LOC,
                new_snapshot_location,
            ))
            .await
    }

    pub fn merge_append_operations(
        schema: &DataSchema,
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
                acc.col_stats =
                    statistics::reduce_block_stats(&[&acc.col_stats, &stats.col_stats], schema)?;
                seg_acc.push(loc.clone());
                Ok::<_, ErrorCode>((acc, seg_acc))
            },
        )?;

        Ok((seg_locs, s))
    }
}
