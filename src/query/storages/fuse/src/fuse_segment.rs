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

use std::sync::Arc;

use common_base::base::tokio::sync::Semaphore;
use common_base::base::Runtime;
use common_catalog::table_context::TableContext;
use common_exception::ErrorCode;
use common_exception::Result;
use common_fuse_meta::meta::Location;
use common_fuse_meta::meta::SegmentInfo;
use common_fuse_meta::meta::TableSnapshot;
use futures_util::future;

use crate::io::MetaReaders;

// Read one segment file by location.
async fn read_segment(ctx: Arc<dyn TableContext>, loc: Location) -> Result<Arc<SegmentInfo>> {
    let (path, ver) = loc;
    let reader = MetaReaders::segment_info_reader(ctx.as_ref());
    reader.read(path, None, ver).await
}

// Read a snapshot's all segments in concurrency.
pub async fn read_segments(
    ctx: Arc<dyn TableContext>,
    snapshot: Arc<TableSnapshot>,
) -> Result<Vec<Result<Arc<SegmentInfo>>>> {
    let max_runtime_threads = ctx.get_settings().get_max_threads()? as usize;
    let max_io_requests = ctx.get_settings().get_max_storage_io_requests()? as usize;

    // 1.1 combine all the tasks.
    let mut iter = snapshot.segments.iter();
    let tasks = std::iter::from_fn(move || {
        if let Some(location) = iter.next() {
            let ctx = ctx.clone();
            let location = location.clone();
            Some(async move { read_segment(ctx, location).await })
        } else {
            None
        }
    });

    // 1.2 build the runtime.
    let semaphore = Arc::new(Semaphore::new(max_io_requests));
    let segments_runtime = Arc::new(Runtime::with_worker_threads(
        max_runtime_threads,
        Some("fuse-req-segments-worker".to_owned()),
    )?);

    // 1.3 spawn all the tasks to the runtime.
    let join_handlers = segments_runtime
        .try_spawn_batch(semaphore.clone(), tasks)
        .await?;

    // 1.4 get all the result.
    let joint: Vec<Result<Arc<SegmentInfo>>> = future::try_join_all(join_handlers)
        .await
        .map_err(|e| ErrorCode::StorageOther(format!("request segments failure, {}", e)))?;
    Ok(joint)
}
