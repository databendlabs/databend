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
use common_expression::TableSchemaRef;
use futures_util::future;
use opendal::Operator;
use storages_common_cache::LoadParams;
use storages_common_table_meta::meta::Location;
use storages_common_table_meta::meta::SegmentInfo;
use tracing::Instrument;

use crate::io::MetaReaders;

// Read segment related operations.
pub struct SegmentsIO {
    ctx: Arc<dyn TableContext>,
    operator: Operator,
    schema: TableSchemaRef,
}

impl SegmentsIO {
    pub fn create(ctx: Arc<dyn TableContext>, operator: Operator, schema: TableSchemaRef) -> Self {
        Self {
            ctx,
            operator,
            schema,
        }
    }

    // Read one segment file by location.
    // The index is the index of the segment_location in segment_locations.
    async fn read_segment(
        dal: Operator,
        segment_location: Location,
        table_schema: TableSchemaRef,
    ) -> Result<Arc<SegmentInfo>> {
        let (path, ver) = segment_location;
        let reader = MetaReaders::segment_info_reader(dal, table_schema);

        // Keep in mind that segment_info_read must need a schema
        let load_params = LoadParams {
            location: path,
            len_hint: None,
            ver,
            put_cache: true,
        };

        reader.read(&load_params).await
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
        let schema = self.schema.clone();
        let tasks = std::iter::from_fn(move || {
            if let Some(location) = iter.next() {
                let location = location.clone();
                Some(
                    Self::read_segment(self.operator.clone(), location, schema.clone())
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

    pub async fn read_segment_into<T>(
        dal: Operator,
        segment_location: Location,
        table_schema: TableSchemaRef,
        put_cache: bool,
    ) -> Result<T>
    where
        T: From<Arc<SegmentInfo>> + Send + 'static,
    {
        let (path, ver) = segment_location;
        let reader = MetaReaders::segment_info_reader(dal, table_schema);

        // Keep in mind that segment_info_read must need a schema
        let load_params = LoadParams {
            location: path.clone(),
            len_hint: None,
            ver,
            put_cache,
        };

        let segment = reader.read(&load_params).await;
        segment.map(|v| v.into())
    }

    #[tracing::instrument(level = "debug", skip_all)]
    pub async fn read_segments_into<T>(
        &self,
        segment_locations: &[Location],
        put_cache: bool,
    ) -> Result<Vec<Result<T>>>
    where
        T: From<Arc<SegmentInfo>> + Send + 'static,
    {
        // combine all the tasks.
        let mut iter = segment_locations.iter();
        let tasks = std::iter::from_fn(move || {
            iter.next().map(|location| {
                Self::read_segment_into(
                    self.operator.clone(),
                    location.clone(),
                    self.schema.clone(),
                    put_cache,
                )
                .instrument(tracing::debug_span!("read_location_tuples"))
            })
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
    future::try_join_all(join_handlers)
        .instrument(tracing::debug_span!("try_join_futures_all"))
        .await
        .map_err(|e| ErrorCode::StorageOther(format!("try join futures failure, {}", e)))
}

/// This is a workaround to address `higher-ranked lifetime error` from rustc
///
/// TODO: remove me after rustc works with try_join_futures directly.
pub async fn try_join_futures_with_vec<Fut>(
    ctx: Arc<dyn TableContext>,
    futures: Vec<Fut>,
    thread_name: String,
) -> Result<Vec<Fut::Output>>
where
    Fut: Future + Send + 'static,
    Fut::Output: Send + 'static,
{
    try_join_futures(ctx, futures, thread_name).await
}
