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

use databend_common_base::runtime::execute_futures_in_parallel;
use databend_common_catalog::table_context::TableContext;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::TableSchemaRef;
use databend_storages_common_cache::CacheAccessor;
use databend_storages_common_cache::CacheManager;
use databend_storages_common_cache::LoadParams;
use databend_storages_common_table_meta::meta::CompactSegmentInfo;
use databend_storages_common_table_meta::meta::Location;
use databend_storages_common_table_meta::meta::SegmentInfo;
use databend_storages_common_table_meta::meta::Versioned;
use fastrace::func_path;
use fastrace::prelude::*;
use opendal::Operator;

use crate::io::MetaReaders;

#[derive(Clone)]
pub struct SerializedSegment {
    pub path: String,
    pub segment: Arc<SegmentInfo>,
}

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
    #[async_backtrace::framed]
    pub async fn read_compact_segment(
        dal: Operator,
        segment_location: Location,
        table_schema: TableSchemaRef,
        put_cache: bool,
    ) -> Result<Arc<CompactSegmentInfo>> {
        let (path, ver) = segment_location;
        let reader = MetaReaders::segment_info_reader(dal, table_schema);

        // Keep in mind that segment_info_read must need a schema
        let load_params = LoadParams {
            location: path,
            len_hint: None,
            ver,
            put_cache,
        };

        reader.read(&load_params).await
    }

    // Read all segments information from s3 in concurrently.
    #[async_backtrace::framed]
    #[fastrace::trace]
    pub async fn read_segments<T>(
        &self,
        segment_locations: &[Location],
        put_cache: bool,
    ) -> Result<Vec<Result<T>>>
    where
        T: TryFrom<Arc<CompactSegmentInfo>> + Send + 'static,
    {
        // combine all the tasks.
        let mut iter = segment_locations.iter();
        let tasks = std::iter::from_fn(|| {
            iter.next().map(|location| {
                let dal = self.operator.clone();
                let table_schema = self.schema.clone();
                let segment_location = location.clone();
                async move {
                    let compact_segment =
                        Self::read_compact_segment(dal, segment_location, table_schema, put_cache)
                            .await?;
                    compact_segment
                        .try_into()
                        .map_err(|_| ErrorCode::Internal("Failed to convert compact segment info"))
                }
                .in_span(Span::enter_with_local_parent(func_path!()))
            })
        });

        let threads_nums = self.ctx.get_settings().get_max_threads()? as usize;
        let permit_nums = threads_nums * 2;
        execute_futures_in_parallel(
            tasks,
            threads_nums,
            permit_nums,
            "fuse-req-segments-worker".to_owned(),
        )
        .await
    }

    #[async_backtrace::framed]
    pub async fn write_segment(dal: Operator, serialized_segment: SerializedSegment) -> Result<()> {
        assert_eq!(
            serialized_segment.segment.format_version,
            SegmentInfo::VERSION
        );
        let raw_bytes = serialized_segment.segment.to_bytes()?;
        let compact_segment_info = CompactSegmentInfo::from_slice(&raw_bytes)?;
        dal.write(&serialized_segment.path, raw_bytes).await?;
        if let Some(segment_cache) = CacheManager::instance().get_table_segment_cache() {
            segment_cache.insert(serialized_segment.path, compact_segment_info);
        }
        Ok(())
    }
}
