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

use chrono::DateTime;
use chrono::Days;
use chrono::Utc;
use databend_common_base::base::uuid::Uuid;
use databend_common_catalog::table::Table;
use databend_common_catalog::table_context::TableContext;
use databend_common_exception::Result;
use databend_common_meta_app::schema::ListIndexesByIdReq;
use databend_common_meta_app::schema::SetLVTReq;
use databend_common_storages_fuse::io::MetaReaders;
use databend_common_storages_fuse::io::SegmentsIO;
use databend_common_storages_fuse::io::TableMetaLocationGenerator;
use databend_common_storages_fuse::FuseTable;
use databend_storages_common_cache::LoadParams;
use databend_storages_common_io::Files;
use databend_storages_common_table_meta::meta::uuid_from_date_time;
use databend_storages_common_table_meta::meta::SegmentInfo;
use databend_storages_common_table_meta::meta::TableSnapshot;
use futures_util::TryStreamExt;
use log::info;
use uuid::Version;

use crate::inverted_index;

#[async_backtrace::framed]
pub async fn do_vacuum2(fuse_table: &FuseTable, ctx: Arc<dyn TableContext>) -> Result<Vec<String>> {
    let start = std::time::Instant::now();
    let Some(lvt) = set_lvt(fuse_table, ctx.as_ref()).await? else {
        return Ok(vec![]);
    };
    info!(
        "set lvt for table {} takes {:?}, lvt: {:?}",
        fuse_table.get_table_info().desc,
        start.elapsed(),
        lvt
    );

    let start = std::time::Instant::now();
    let snapshots_before_lvt = list_until_timestamp(
        fuse_table,
        &fuse_table.meta_location_generator().snapshot_dir(),
        lvt,
        true,
    )
    .await?;
    info!(
        "list snapshots before lvt for table {} takes {:?}, snapshots_dir: {:?}, lvt: {:?}, snapshots: {:?}",
        fuse_table.get_table_info().desc,
        start.elapsed(),
        fuse_table.meta_location_generator().snapshot_dir(),
        lvt,
        slice_summary(&snapshots_before_lvt)
    );

    let start = std::time::Instant::now();
    let Some((gc_root, snapshots_to_gc)) =
        select_gc_root(fuse_table, &snapshots_before_lvt).await?
    else {
        return Ok(vec![]);
    };
    info!(
        "select gc_root for table {} takes {:?}, gc_root: {:?}, snapshots_to_gc: {:?}",
        fuse_table.get_table_info().desc,
        start.elapsed(),
        gc_root,
        slice_summary(snapshots_to_gc)
    );

    let start = std::time::Instant::now();
    let least_visible_timestamp = gc_root.least_visible_timestamp.unwrap();
    let gc_root_segments = gc_root
        .segments
        .iter()
        .map(|(path, _)| path)
        .collect::<HashSet<_>>();
    let segments_before_gc_root = list_until_timestamp(
        fuse_table,
        &fuse_table.meta_location_generator().segment_dir(),
        least_visible_timestamp,
        false,
    )
    .await?;
    info!(
        "list segments before gc_root for table {} takes {:?}, segment_dir: {:?}, least_visible_timestamp: {:?}, segments: {:?}",
        fuse_table.get_table_info().desc,
        start.elapsed(),
        fuse_table.meta_location_generator().segment_dir(),
        least_visible_timestamp,
        slice_summary(&segments_before_gc_root)
    );

    let start = std::time::Instant::now();
    let segments_to_gc: Vec<String> = segments_before_gc_root
        .into_iter()
        .filter(|s| !gc_root_segments.contains(s))
        .collect();
    info!(
        "Filter segments to gc for table {} takes {:?}, segments_to_gc: {:?}",
        fuse_table.get_table_info().desc,
        start.elapsed(),
        slice_summary(&segments_to_gc)
    );

    let start = std::time::Instant::now();
    let segments_io =
        SegmentsIO::create(ctx.clone(), fuse_table.get_operator(), fuse_table.schema());
    let segments = segments_io
        .read_segments::<SegmentInfo>(&gc_root.segments, false)
        .await?;
    let mut gc_root_blocks = HashSet::new();
    for segment in segments {
        gc_root_blocks.extend(segment?.blocks.iter().map(|b| b.location.0.clone()));
    }
    info!(
        "read segments for table {} takes {:?}",
        fuse_table.get_table_info().desc,
        start.elapsed(),
    );

    let start = std::time::Instant::now();
    let blocks_before_gc_root = list_until_timestamp(
        fuse_table,
        &fuse_table.meta_location_generator().block_dir(),
        least_visible_timestamp,
        false,
    )
    .await?;
    info!(
        "list blocks before gc_root for table {} takes {:?}, block_dir: {:?}, least_visible_timestamp: {:?}, blocks: {:?}",
        fuse_table.get_table_info().desc,
        start.elapsed(),
        fuse_table.meta_location_generator().block_dir(),
        least_visible_timestamp,
        slice_summary(&blocks_before_gc_root)
    );

    let start = std::time::Instant::now();
    let blocks_to_gc: Vec<String> = blocks_before_gc_root
        .into_iter()
        .filter(|b| !gc_root_blocks.contains(b))
        .collect();
    info!(
        "Filter blocks to gc for table {} takes {:?}, blocks_to_gc: {:?}",
        fuse_table.get_table_info().desc,
        start.elapsed(),
        slice_summary(&blocks_to_gc)
    );

    let catalog = ctx.get_default_catalog()?;
    let table_agg_index_ids = catalog
        .list_index_ids_by_table_id(ListIndexesByIdReq::new(
            ctx.get_tenant(),
            fuse_table.get_id(),
        ))
        .await?;
    let inverted_indexes = &fuse_table.get_table_info().meta.indexes;
    let mut indexes_to_gc = Vec::with_capacity(
        blocks_to_gc.len() * (table_agg_index_ids.len() + inverted_indexes.len() + 1),
    );
    for loc in &blocks_to_gc {
        for index_id in table_agg_index_ids {
            indexes_to_gc.push(
                TableMetaLocationGenerator::gen_agg_index_location_from_block_location(
                    loc, *index_id,
                ),
            );
        }
        for idx in inverted_indexes.values() {
            indexes.push(
                TableMetaLocationGenerator::gen_inverted_index_location_from_block_location(
                    loc,
                    idx.name.as_str(),
                    idx.version.as_str(),
                ),
            );
        }
        indexes_to_gc
            .push(TableMetaLocationGenerator::gen_bloom_index_location_from_block_location(loc));
    }

    let start = std::time::Instant::now();
    let subject_files_to_gc: Vec<_> = segments_to_gc
        .into_iter()
        .chain(blocks_to_gc.into_iter())
        .chain(indexes_to_gc.into_iter())
        .collect();
    let op = Files::create(ctx, fuse_table.get_operator());

    // remove subject files first, then remove snapshots, if vacuum failed, we can retry
    op.remove_file_in_batch(&subject_files_to_gc).await?;
    op.remove_file_in_batch(snapshots_to_gc).await?;

    let files_to_gc: Vec<_> = subject_files_to_gc
        .into_iter()
        .chain(snapshots_to_gc.into_iter())
        .collect();
    info!(
        "remove files for table {} takes {:?}, files_to_gc: {:?}",
        fuse_table.get_table_info().desc,
        start.elapsed(),
        slice_summary(&files_to_gc)
    );
    Ok(files_to_gc)
}

/// Try set lvt as min(latest_snapshot.timestamp, now - retention_time).
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

async fn select_gc_root<'a>(
    fuse_table: &FuseTable,
    snapshots_before_lvt: &'a [String],
) -> Result<Option<(Arc<TableSnapshot>, &'a [String])>> {
    for anchor_location in snapshots_before_lvt.iter().rev() {
        let anchor = read_snapshot_from_location(fuse_table, anchor_location).await?;
        // when anchor is generated by a failed transaction based on an empty table, anchor.prev_snapshot_id is None, but it still has a chance to find a gc_root with other anchors
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
                    let gc_root_idx = snapshots_before_lvt.binary_search(&gc_root_path).unwrap();
                    let snapshots_to_gc = &snapshots_before_lvt[..gc_root_idx];
                    return Ok(Some((gc_root, snapshots_to_gc)));
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

fn slice_summary<T: std::fmt::Debug>(s: &[T]) -> String {
    if s.len() > 10 {
        let first_five = &s[..5];
        let last_five = &s[s.len() - 5..];
        format!(
            "First five: {:?}, Last five: {:?},Len: {}",
            first_five,
            last_five,
            s.len()
        )
    } else {
        format!("{:?}", s)
    }
}
