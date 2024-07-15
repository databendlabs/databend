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
use std::panic::Location;
use std::sync::Arc;
use std::time::Instant;

use chrono::DateTime;
use chrono::Days;
use chrono::Duration;
use chrono::TimeZone;
use chrono::Utc;
use databend_common_base::base::uuid::Uuid;
use databend_common_catalog::table::NavigationPoint;
use databend_common_catalog::table::Table;
use databend_common_catalog::table_context::TableContext;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_meta_app::schema::SetLVTReq;
use databend_common_storages_fuse::io::MetaReaders;
use databend_common_storages_fuse::io::SnapshotLiteExtended;
use databend_common_storages_fuse::io::SnapshotsIO;
use databend_common_storages_fuse::io::TableMetaLocationGenerator;
use databend_common_storages_fuse::FuseTable;
use databend_common_storages_fuse::FUSE_TBL_BLOCK_PREFIX;
use databend_common_storages_fuse::FUSE_TBL_SEGMENT_PREFIX;
use databend_common_storages_fuse::FUSE_TBL_SNAPSHOT_PREFIX;
use databend_storages_common_cache::LoadParams;
use databend_storages_common_table_meta::meta;
use databend_storages_common_table_meta::meta::CompactSegmentInfo;
use databend_storages_common_table_meta::meta::FormatVersion;
use databend_storages_common_table_meta::meta::SnapshotId;
use databend_storages_common_table_meta::meta::TableSnapshot;
use futures_util::TryStreamExt;
use log::info;
use opendal::Operator;
use uuid::Version;

use crate::storages::fuse;
use crate::storages::fuse::get_snapshot_referenced_segments;

#[async_backtrace::framed]
pub async fn do_vacuum2(
    fuse_table: &FuseTable,
    ctx: Arc<dyn TableContext>,
) -> Result<Option<Vec<String>>> {
    let Some(snapshot) = fuse_table.read_table_snapshot().await? else {
        // nothing to do
        return Ok(None);
    };

    if !snapshot
        .snapshot_id
        .get_version()
        .is_some_and(|v| matches!(v, Version::SortRand))
    {
        // not working for snapshot before v5
        return Err(ErrorCode::StorageOther("legacy snapshot is not supported"));
    }

    // safe to unwrap, as we have checked the version is v5
    let latest_ts = snapshot.timestamp.unwrap();
    let lvt_point_candidate =
        Utc::now() - Days::new(ctx.get_settings().get_data_retention_time_in_days()?);
    let lvt_point_candidate = std::cmp::min(lvt_point_candidate, latest_ts);

    let cat = ctx.get_default_catalog()?;
    let lvt_point = cat
        .set_table_lvt(SetLVTReq {
            table_id: fuse_table.get_table_info().ident.table_id,
            time: lvt_point_candidate,
        })
        .await?
        .time;
    todo!()
}
