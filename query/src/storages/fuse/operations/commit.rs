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

use common_datavalues::DataSchema;
use common_exception::ErrorCode;
use common_exception::Result;
use common_meta_types::TableIdent;
use common_meta_types::UpsertTableOptionReply;
use common_meta_types::UpsertTableOptionReq;
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

impl FuseTable {
    #[inline]
    pub async fn do_commit(
        &self,
        ctx: Arc<QueryContext>,
        operation_log: TableOperationLog,
        overwrite: bool,
    ) -> Result<()> {
        // TODO OCC retry & resolves conflicts if applicable

        let prev = self.table_snapshot(ctx.clone()).await?;
        let new_snapshot = if overwrite {
            let schema = self.table_info.meta.schema.as_ref().clone();
            let (segments, summary) = Self::merge_append_operations(&schema, operation_log)?;
            TableSnapshot {
                snapshot_id: Uuid::new_v4(),
                prev_snapshot_id: prev.as_ref().map(|v| v.snapshot_id),
                schema,
                summary,
                segments,
            }
        } else {
            Self::merge_table_operations(self.table_info.meta.schema.as_ref(), prev, operation_log)?
        };

        let uuid = new_snapshot.snapshot_id;
        let snapshot_loc = io::snapshot_location(uuid.to_simple().to_string().as_str());
        let bytes = serde_json::to_vec(&new_snapshot)?;
        let da = ctx.get_data_accessor()?;
        da.put(&snapshot_loc, bytes).await?;

        self.commit_to_meta_server(ctx, snapshot_loc).await?;
        Ok(())
    }

    fn merge_table_operations(
        schema: &DataSchema,
        prev: Option<TableSnapshot>,
        ops: TableOperationLog,
    ) -> Result<TableSnapshot> {
        // 1. merge operations(appends, currently)
        let (mut segs, stats) = Self::merge_append_operations(schema, ops)?;

        // 2. merge stats with previous snapshot, if any
        let stats = if let Some(TableSnapshot { summary, .. }) = &prev {
            statistics::merge_statistics(schema, &stats, summary)?
        } else {
            stats
        };
        let prev_snapshot_id = prev.as_ref().map(|v| v.snapshot_id);

        // 3. merge segment locations with previous snapshot, if any
        if let Some(TableSnapshot { mut segments, .. }) = prev {
            segs.append(&mut segments)
        };

        let new_snapshot = TableSnapshot {
            snapshot_id: Uuid::new_v4(),
            prev_snapshot_id,
            schema: schema.clone(),
            summary: stats,
            segments: segs,
        };
        Ok(new_snapshot)
    }

    async fn commit_to_meta_server(
        &self,
        ctx: Arc<QueryContext>,
        new_snapshot_location: String,
    ) -> Result<UpsertTableOptionReply> {
        let table_id = self.table_info.ident.table_id;
        let table_version = self.table_info.ident.version;
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
        append_log_entries: Vec<AppendOperationLogEntry>,
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
