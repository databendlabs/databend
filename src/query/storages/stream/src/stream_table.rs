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

use std::any::Any;
use std::collections::HashSet;
use std::sync::Arc;

use common_catalog::catalog::StorageDescription;
use common_catalog::plan::PartStatistics;
use common_catalog::plan::Partitions;
use common_catalog::plan::PushDownInfo;
use common_catalog::table::Table;
use common_catalog::table_context::TableContext;
use common_exception::ErrorCode;
use common_exception::Result;
use common_meta_app::schema::DatabaseType;
use common_meta_app::schema::TableInfo;
use common_storages_fuse::io::SegmentsIO;
use common_storages_fuse::io::SnapshotsIO;
use common_storages_fuse::FuseTable;
use storages_common_table_meta::meta::Location;
use storages_common_table_meta::meta::SegmentInfo;
use storages_common_table_meta::table::OPT_KEY_SNAPSHOT_LOCATION;

pub const STREAM_ENGINE: &str = "STREAM";

pub const OPT_KEY_TABLE_NAME: &str = "table_name";
pub const OPT_KEY_TABLE_ID: &str = "table_id";
pub const OPT_KEY_TABLE_VER: &str = "table_version";
pub const OPT_KEY_MODE: &str = "mode";

pub const MODE_APPEND_ONLY: &str = "append_only";

pub struct StreamTable {
    stream_info: TableInfo,

    table_name: String,
    table_id: u64,
    table_version: u64,
    append_only: bool,
    snapshot_location: Option<String>,
}

impl StreamTable {
    pub fn try_create(table_info: TableInfo) -> Result<Box<dyn Table>> {
        let options = table_info.options();
        let table_name = options
            .get(OPT_KEY_TABLE_NAME)
            .ok_or_else(|| ErrorCode::Internal("table name must be set"))?
            .clone();
        let table_id = options
            .get(OPT_KEY_TABLE_ID)
            .ok_or_else(|| ErrorCode::Internal("table id must be set"))?
            .parse::<u64>()?;
        let table_version = options
            .get(OPT_KEY_TABLE_VER)
            .ok_or_else(|| ErrorCode::Internal("table version must be set"))?
            .parse::<u64>()?;
        let append_only = options
            .get(OPT_KEY_MODE)
            .map(|v| v == MODE_APPEND_ONLY)
            .unwrap_or(false);
        let snapshot_location = options.get(OPT_KEY_SNAPSHOT_LOCATION).cloned();
        Ok(Box::new(StreamTable {
            stream_info: table_info,
            table_name,
            table_id,
            table_version,
            append_only,
            snapshot_location,
        }))
    }

    pub fn description() -> StorageDescription {
        StorageDescription {
            engine_name: STREAM_ENGINE.to_string(),
            comment: "STREAM STORAGE Engine".to_string(),
            ..Default::default()
        }
    }

    pub fn try_from_table(tbl: &dyn Table) -> Result<&StreamTable> {
        tbl.as_any().downcast_ref::<StreamTable>().ok_or_else(|| {
            ErrorCode::Internal(format!(
                "expects table of engine STREAM, but got {}",
                tbl.engine()
            ))
        })
    }

    #[async_backtrace::framed]
    async fn do_read_partitions(
        &self,
        ctx: Arc<dyn TableContext>,
        push_downs: Option<PushDownInfo>,
    ) -> Result<(PartStatistics, Partitions)> {
        let catalog = ctx.get_catalog(self.stream_info.catalog()).await?;
        let (ident, meta) = catalog.get_table_meta_by_id(self.table_id).await?;
        if ident.seq <= self.table_version {
            return Ok((PartStatistics::default(), Partitions::default()));
        }
        let table_info = TableInfo {
            ident,
            desc: "".to_owned(),
            name: self.table_name.clone(),
            meta: meta.as_ref().clone(),
            tenant: "".to_owned(),
            db_type: DatabaseType::NormalDB,
        };
        let table = catalog.get_table_by_info(&table_info)?;
        let fuse_table = FuseTable::try_from_table(table.as_ref())?;
        let latest_snapshot = fuse_table.read_table_snapshot().await?;
        if latest_snapshot.is_none() {
            return Ok((PartStatistics::default(), Partitions::default()));
        }

        let latest_snapshot = latest_snapshot.unwrap();
        let latest_segments: HashSet<Location> =
            HashSet::from_iter(latest_snapshot.segments.clone());
        drop(latest_snapshot);
        let operator = fuse_table.get_operator();
        let base_segments: HashSet<Location> =
            if let Some(snapshot_location) = &self.snapshot_location {
                let (base_snapshot, _) =
                    SnapshotsIO::read_snapshot(snapshot_location.clone(), operator.clone()).await?;
                HashSet::from_iter(base_snapshot.segments.clone())
            } else {
                HashSet::new()
            };

        let fuse_segment_io = SegmentsIO::create(ctx, operator, fuse_table.schema());

        let diff_in_base = base_segments
            .difference(&latest_segments)
            .cloned()
            .collect::<Vec<_>>();
        let diff_in_base = fuse_segment_io
            .read_segments::<SegmentInfo>(&diff_in_base, true)
            .await?;
        let mut base_blocks = HashSet::new();
        for segment in diff_in_base {
            let segment = segment?;
            segment.blocks.into_iter().for_each(|block| {
                base_blocks.insert(block.location.clone());
            })
        }

        let diff_in_latest = latest_segments
            .difference(&base_segments)
            .cloned()
            .collect::<Vec<_>>();
        let diff_in_latest = fuse_segment_io
            .read_segments::<SegmentInfo>(&diff_in_latest, true)
            .await?;
        let mut latest_blocks = Vec::new();
        for segment in diff_in_latest {
            let segment = segment?;
            segment.blocks.into_iter().for_each(|block| {
                if !base_blocks.contains(&block.location) {
                    latest_blocks.push(block);
                }
            });
        }

        todo!()
    }
}

#[async_trait::async_trait]
impl Table for StreamTable {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn get_table_info(&self) -> &TableInfo {
        &self.stream_info
    }

    #[async_backtrace::framed]
    async fn read_partitions(
        &self,
        ctx: Arc<dyn TableContext>,
        push_downs: Option<PushDownInfo>,
        _dry_run: bool,
    ) -> Result<(PartStatistics, Partitions)> {
        todo!()
    }
}
