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

use std::future::Future;
use std::sync::Arc;

use common_base::base::tokio::sync::Semaphore;
use common_base::runtime::Runtime;
use common_catalog::table_context::TableContext;
use common_exception::ErrorCode;
use common_exception::Result;
use common_storages_table_meta::meta::Location;
use common_storages_table_meta::meta::SegmentInfo;
use futures_util::future;
use opendal::Operator;
use tracing::Instrument;

use crate::io::MetaReaders;

// Read segment related operations.
pub struct SegmentsIO {
    ctx: Arc<dyn TableContext>,
    operator: Operator,
}

impl SegmentsIO {
    pub fn create(ctx: Arc<dyn TableContext>, operator: Operator) -> Self {
        Self { ctx, operator }
    }

    // Read one segment file by location.
    // The index is the index of the segment_location in segment_locations.
    async fn read_segment(dal: Operator, segment_location: Location) -> Result<Arc<SegmentInfo>> {
        let (path, ver) = segment_location;
        let reader = MetaReaders::segment_info_reader(dal);
        reader.read(path, None, ver).await
    }

    // Read all segments information from s3 in concurrency.
    #[tracing::instrument(level = "debug", skip_all)]
    pub async fn read_segments(
        &self,
        segment_locations: &[Location],
    ) -> Result<Vec<Result<Arc<SegmentInfo>>>> {
        if segment_locations.is_empty() {
            return Ok(vec![]);
        }

        // combine all the tasks.
        let mut iter = segment_locations.iter();
        let tasks = std::iter::from_fn(move || {
            if let Some(location) = iter.next() {
                let location = location.clone();
                Some(
                    Self::read_segment(self.operator.clone(), location)
                        .instrument(tracing::debug_span!("read_segment")),
                )
            } else {
                None
            }
        });

        try_join_futures(
            self.ctx.clone(),
            tasks,
            "fuse-req-segments-worker".to_owned(),
        )
        .await
    }
}

pub async fn try_join_futures<Fut>(
    ctx: Arc<dyn TableContext>,
    futures: impl IntoIterator<Item = Fut>,
    thread_name: String,
) -> Result<Vec<Fut::Output>>
where
    Fut: Future + Send + 'static,
    Fut::Output: Send + 'static,
{
    let max_runtime_threads = ctx.get_settings().get_max_threads()? as usize;
    let max_io_requests = ctx.get_settings().get_max_storage_io_requests()? as usize;

    // 1. build the runtime.
    let semaphore = Semaphore::new(max_io_requests);
    let segments_runtime = Arc::new(Runtime::with_worker_threads(
        max_runtime_threads,
        Some(thread_name),
    )?);

    // 2. spawn all the tasks to the runtime.
    let join_handlers = segments_runtime.try_spawn_batch(semaphore, futures).await?;

    // 3. get all the result.
    let joint = future::try_join_all(join_handlers)
        .instrument(tracing::debug_span!("try_join_futures_all"))
        .await
        .map_err(|e| ErrorCode::StorageOther(format!("try join futures failure, {}", e)))?;

    Ok(joint)
}
