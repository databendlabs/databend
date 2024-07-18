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

use std::sync::Arc;

use chrono::DateTime;
use chrono::Days;
use chrono::Utc;
use databend_common_base::base::uuid::Uuid;
use databend_common_catalog::table::Table;
use databend_common_catalog::table_context::TableContext;
use databend_common_exception::Result;
use databend_common_meta_app::schema::SetLVTReq;
use databend_common_storages_fuse::io::MetaReaders;
use databend_common_storages_fuse::io::TableMetaLocationGenerator;
use databend_common_storages_fuse::FuseTable;
use databend_storages_common_cache::LoadParams;
use databend_storages_common_table_meta::meta::uuid_from_date_time;
use databend_storages_common_table_meta::meta::TableSnapshot;
use futures_util::TryStreamExt;
use log::info;
use uuid::Version;

#[async_backtrace::framed]
pub async fn do_vacuum2(fuse_table: &FuseTable, ctx: Arc<dyn TableContext>) -> Result<()> {
    let Some(lvt) = set_lvt(fuse_table, ctx.as_ref()).await? else {
        return Ok(());
    };

    let snapshots_before_lvt = list_until_timestamp(
        fuse_table,
        &fuse_table.meta_location_generator().snapshot_dir(),
        lvt,
        true,
    )
    .await?;

    let Some(_gc_root) = select_gc_root(fuse_table, &snapshots_before_lvt).await? else {
        return Ok(());
    };

    // let gc_root_idx = snapshot_locations
    //     .binary_search(&snapshot_path)
    //     .expect("gc_root should be in snapshot_locations");
    // let snapshots_to_gc = &snapshot_locations[..gc_root_idx];
    todo!()
}

/// Try set lvt as min(latest_snapshot.timestamp, now - retention_time).
///
/// Lvt is equal or less than the latest snapshot timestamp, so latest snapshot is always safe, which means `fuse_vacuum2()` and other operations based on the latest snapshot can be executed concurrently.
///
/// If table has no snapshot yet, we don't know if files belong to a running transaction, so we can't do vacuum work.
///
/// Return `None` means we stop vacuumming, but don't want to report error to user.
async fn set_lvt(fuse_table: &FuseTable, ctx: &dyn TableContext) -> Result<Option<DateTime<Utc>>> {
    let Some(latest_snapshot) = fuse_table.read_table_snapshot().await? else {
        info!(
            "Table {} has no snapshot, stop vacuuming",
            fuse_table.get_table_info().desc
        );
        return Ok(None);
    };
    if !is_uuid_v7(&latest_snapshot.snapshot_id) {
        info!(
            "latest snapshot {:?} is not v7, stop vacuuming",
            latest_snapshot
        );
        return Ok(None);
    }
    // safe to unwrap, as we have checked the version is v5
    let latest_ts = latest_snapshot.timestamp.unwrap();
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
    Ok(Some(lvt_point))
}

fn is_uuid_v7(uuid: &Uuid) -> bool {
    let version = uuid.get_version();
    version.is_some_and(|v| matches!(v, Version::SortRand))
}

async fn list_until_prefix(
    fuse_table: &FuseTable,
    path: &str,
    until: &str,
    need_one_more: bool,
) -> Result<Vec<String>> {
    let mut lister = fuse_table.get_operator().lister(path).await?;
    let mut paths = vec![];
    while let Some(entry) = lister.try_next().await? {
        if entry.path() >= until {
            if need_one_more {
                paths.push(entry.path().to_string());
            }
            break;
        }
        paths.push(entry.path().to_string());
    }
    Ok(paths)
}

async fn list_until_timestamp(
    fuse_table: &FuseTable,
    path: &str,
    until: DateTime<Utc>,
    need_one_more: bool,
) -> Result<Vec<String>> {
    let uuid = uuid_from_date_time(until);
    let uuid_str = uuid.simple().to_string();

    // extract the most significant 48 bits, which is 12 characters
    let timestamp_component = &uuid_str[..12];
    let until = format!("{}g{}", path, timestamp_component);
    list_until_prefix(fuse_table, path, &until, need_one_more).await
}

async fn read_snapshot_from_location(
    fuse_table: &FuseTable,
    path: &str,
) -> Result<Arc<TableSnapshot>> {
    let reader = MetaReaders::table_snapshot_reader(fuse_table.get_operator());
    let ver = TableMetaLocationGenerator::snapshot_version(path);
    let params = LoadParams {
        location: path.to_owned(),
        len_hint: None,
        ver,
        put_cache: false,
    };
    reader.read(&params).await
}

async fn select_gc_root(
    fuse_table: &FuseTable,
    snapshots_before_lvt: &[String],
) -> Result<Option<Arc<TableSnapshot>>> {
    for anchor_location in snapshots_before_lvt.iter().rev() {
        let anchor = read_snapshot_from_location(fuse_table, anchor_location).await?;
        if let Some((gc_root_id, gc_root_ver)) = anchor.prev_snapshot_id {
            let gc_root_path = fuse_table
                .meta_location_generator()
                .snapshot_location_from_uuid(&gc_root_id, gc_root_ver)?;
            if !is_uuid_v7(&gc_root_id) {
                info!("gc_root {} is not v7", gc_root_path);
                // stop selecting gc_root, because previous snapshots can not be v7 either
                return Ok(None);
            }
            let gc_root = read_snapshot_from_location(fuse_table, &gc_root_path).await;
            match gc_root {
                Ok(gc_root) => {
                    info!("gc_root found: {:?}", gc_root);
                    return Ok(Some(gc_root));
                }
                Err(e) => {
                    info!("read gc_root {} failed: {:?}", gc_root_path, e);
                    // continue to find the next gc_root
                }
            }
        };
    }
    info!(
        "no gc_root found,snapshots_before_lvt: {:?}",
        snapshots_before_lvt
    );
    Ok(None)
}
