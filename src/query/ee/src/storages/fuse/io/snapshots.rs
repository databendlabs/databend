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

// Read all the referenced segments by all the snapshot file.
// limit: limits the number of snapshot files listed

use std::sync::Arc;
use std::time::Instant;

use databend_common_exception::Result;
use databend_common_storages_fuse::io::SnapshotLiteExtended;
use databend_common_storages_fuse::io::SnapshotsIO;
use databend_storages_common_table_meta::meta::Location;
use log::info;

#[async_backtrace::framed]
pub async fn get_snapshot_referenced_segments<T>(
    snapshots_io: &SnapshotsIO,
    root_snapshot_location: String,
    root_snapshot_lite: Arc<SnapshotLiteExtended>,
    status_callback: T,
) -> Result<Option<Vec<Location>>>
where
    T: Fn(String),
{
    let ctx = snapshots_io.get_ctx();

    // List all the snapshot file paths
    // note that snapshot file paths of ongoing txs might be included
    let mut snapshot_files = vec![];
    if let Some(prefix) = SnapshotsIO::get_s3_prefix_from_file(&root_snapshot_location) {
        snapshot_files =
            SnapshotsIO::list_files(snapshots_io.get_operator(), &prefix, None).await?;
    }

    if snapshot_files.is_empty() {
        return Ok(None);
    }

    // 1. Get all the snapshot by chunks, save all the segments location.
    let max_threads = ctx.get_settings().get_max_threads()? as usize;

    let start = Instant::now();
    let mut count = 1;
    // 2. Get all the referenced segments
    let mut segments = vec![];
    // first save root snapshot segments
    root_snapshot_lite.segments.iter().for_each(|location| {
        segments.push(location.to_owned());
    });
    for chunk in snapshot_files.chunks(max_threads) {
        // Since we want to get all the snapshot referenced files, so set `ignore_timestamp` true
        let results = snapshots_io
            .read_snapshot_lite_extends(chunk, root_snapshot_lite.clone(), true)
            .await?;

        results
            .into_iter()
            .flatten()
            .for_each(|snapshot_lite_extend| {
                snapshot_lite_extend.segments.iter().for_each(|location| {
                    segments.push(location.to_owned());
                });
            });

        // Refresh status.
        {
            count += chunk.len();
            let status = format!(
                "gc orphan: read snapshot files:{}/{}, segment files: {}, cost:{:?}",
                count,
                snapshot_files.len(),
                segments.len(),
                start.elapsed()
            );
            info!("{}", status);
            (status_callback)(status);
        }
    }

    Ok(Some(segments))
}

#[async_backtrace::framed]
async fn get_files_by_prefix(snapshots_io: &SnapshotsIO, input_file: &str) -> Result<Vec<String>> {
    if let Some(prefix) = SnapshotsIO::get_s3_prefix_from_file(input_file) {
        SnapshotsIO::list_files(snapshots_io.get_operator(), &prefix, None).await
    } else {
        Ok(vec![])
    }
}
