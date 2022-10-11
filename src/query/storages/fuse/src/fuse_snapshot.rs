//  Copyright 2022 Datafuse Labs.
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

use std::path::Path;
use std::sync::Arc;

use common_base::base::tokio::sync::Semaphore;
use common_base::base::Runtime;
use common_catalog::table_context::TableContext;
use common_exception::ErrorCode;
use common_exception::Result;
use common_fuse_meta::meta::TableSnapshot;
use futures_util::future;
use futures_util::TryStreamExt;
use opendal::ObjectMode;
use opendal::Operator;
use tracing::warn;
use tracing::Instrument;

use crate::io::MetaReaders;

async fn read_snapshot(
    ctx: Arc<dyn TableContext>,
    snapshot_location: String,
    format_version: u64,
) -> Result<Arc<TableSnapshot>> {
    let reader = MetaReaders::table_snapshot_reader(ctx);
    reader.read(snapshot_location, None, format_version).await
}

#[tracing::instrument(level = "debug", skip_all)]
pub async fn read_snapshots_by_root_file(
    ctx: Arc<dyn TableContext>,
    root_snapshot_file: String,
    format_version: u64,
    data_accessor: &Operator,
) -> Result<Vec<Result<Arc<TableSnapshot>>>> {
    let mut snapshot_files = vec![];
    if let Some(path) = Path::new(&root_snapshot_file).parent() {
        let mut snapshot_prefix = path.to_str().unwrap_or("").to_string();

        // Check if the prefix:db/table/_ss is reasonable.
        if !snapshot_prefix.contains('/') {
            return Ok(vec![]);
        }

        if !snapshot_prefix.ends_with('/') {
            snapshot_prefix += "/";
        }

        let mut ds = data_accessor.object(&snapshot_prefix).list().await?;
        while let Some(de) = ds.try_next().await? {
            match de.mode() {
                ObjectMode::FILE => {
                    snapshot_files.push(de.path().to_string());
                }
                _ => {
                    warn!(
                        "Found not snapshot file in {:}, found: {:?}",
                        snapshot_prefix, de
                    );
                    continue;
                }
            }
        }
    }

    let max_runtime_threads = ctx.get_settings().get_max_threads()? as usize;
    let max_io_requests = ctx.get_settings().get_max_storage_io_requests()? as usize;

    // 1.1 combine all the tasks.
    let mut iter = snapshot_files.iter();
    let tasks = std::iter::from_fn(move || {
        if let Some(location) = iter.next() {
            let ctx = ctx.clone();
            let location = location.clone();
            Some(
                read_snapshot(ctx, location, format_version)
                    .instrument(tracing::debug_span!("read_snapshot")),
            )
        } else {
            None
        }
    });

    // 1.2 build the runtime.
    let semaphore = Arc::new(Semaphore::new(max_io_requests));
    let segments_runtime = Arc::new(Runtime::with_worker_threads(
        max_runtime_threads,
        Some("fuse-req-snapshots-worker".to_owned()),
    )?);

    // 1.3 spawn all the tasks to the runtime.
    let join_handlers = segments_runtime
        .try_spawn_batch(semaphore.clone(), tasks)
        .await?;

    // 1.4 get all the result.
    let joint: Vec<Result<Arc<TableSnapshot>>> = future::try_join_all(join_handlers)
        .instrument(tracing::debug_span!("read_snapshots_join_all"))
        .await
        .map_err(|e| ErrorCode::StorageOther(format!("read snapshots failure, {}", e)))?;
    Ok(joint)
}
