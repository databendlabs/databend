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

use std::sync::Arc;

use common_base::runtime::execute_futures_in_parallel;
use common_catalog::table_context::TableContext;
use common_exception::Result;
use common_expression::TableSchemaRef;
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
    #[async_backtrace::framed]
    async fn read_segment(
        dal: Operator,
        segment_location: Location,
        table_schema: TableSchemaRef,
        put_cache: bool,
    ) -> Result<Arc<SegmentInfo>> {
        let (path, ver) = segment_location;
        let reader = MetaReaders::segment_info_reader(dal, table_schema);

        // Keep in mind that segment_info_read must need a schema
        let load_params = LoadParams {
            location: path,
            len_hint: None,
            ver,
            put_cache,
        };

        let raw_bytes = reader.read(&load_params).await?;
        let segment_info = SegmentInfo::try_from(raw_bytes.as_ref())?;
        Ok(Arc::new(segment_info))
    }

    // Read all segments information from s3 in concurrently.
    #[tracing::instrument(level = "debug", skip_all)]
    #[async_backtrace::framed]
    pub async fn read_segments(
        &self,
        segment_locations: &[Location],
        put_cache: bool,
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
                    Self::read_segment(self.operator.clone(), location, schema.clone(), put_cache)
                        .instrument(tracing::debug_span!("read_segment")),
                )
            } else {
                None
            }
        });

        let threads_nums = self.ctx.get_settings().get_max_threads()? as usize;
        let permit_nums = self.ctx.get_settings().get_max_storage_io_requests()? as usize;
        execute_futures_in_parallel(
            tasks,
            threads_nums,
            permit_nums,
            "fuse-req-segments-worker".to_owned(),
        )
        .await
    }

    #[async_backtrace::framed]
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

        let compact_segment = reader.read(&load_params).await?;
        let segment = Arc::new(SegmentInfo::try_from(compact_segment.as_ref())?);
        Ok(segment.into())
    }

    #[tracing::instrument(level = "debug", skip_all)]
    #[async_backtrace::framed]
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

        let threads_nums = self.ctx.get_settings().get_max_threads()? as usize;
        let permit_nums = self.ctx.get_settings().get_max_storage_io_requests()? as usize;
        execute_futures_in_parallel(
            tasks,
            threads_nums,
            permit_nums,
            "fuse-req-segments-worker".to_owned(),
        )
        .await
    }
}
