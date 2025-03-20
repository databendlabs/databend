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

use databend_common_exception::Result;
use databend_common_expression::ColumnId;
use databend_common_expression::TableSchemaRef;
use databend_storages_common_cache::CacheAccessor;
use databend_storages_common_cache::CacheManager;
use databend_storages_common_table_meta::meta::column_oriented_segment::deserialize_column_oriented_segment;
use databend_storages_common_table_meta::meta::column_oriented_segment::AbstractSegment;
use databend_storages_common_table_meta::meta::column_oriented_segment::ColumnOrientedSegment;
use databend_storages_common_table_meta::meta::column_oriented_segment::ColumnOrientedSegmentBuilder;
use databend_storages_common_table_meta::meta::column_oriented_segment::SegmentBuilder;
use databend_storages_common_table_meta::meta::CompactSegmentInfo;
use databend_storages_common_table_meta::meta::Location;
use databend_storages_common_table_meta::meta::SegmentInfo;
use opendal::Operator;

use super::meta::bytes_reader;
use crate::io::SegmentsIO;
use crate::operations::ColumnOrientedCompactTaskBuilder;
use crate::operations::ColumnOrientedSegmentsWithIndices;
use crate::operations::CompactSegmentsWithIndices;
use crate::operations::CompactTaskBuilder;
use crate::operations::RowOrientedCompactTaskBuilder;
use crate::operations::SegmentsWithIndices;
use crate::pruning_pipeline::PrunedColumnOrientedSegmentMeta;
use crate::pruning_pipeline::PrunedCompactSegmentMeta;
use crate::pruning_pipeline::PrunedSegmentMeta;
use crate::statistics::RowOrientedSegmentBuilder;

#[async_trait::async_trait]
pub trait SegmentReader: Send + Sync + 'static {
    type Segment: AbstractSegment;
    type CompactSegment: AbstractSegment;
    type SegmentBuilder: SegmentBuilder;
    type SegmentsWithIndices: SegmentsWithIndices<Segment = Self::CompactSegment> + Clone;
    type CompactTaskBuilder: CompactTaskBuilder<Segment = Self::CompactSegment>;
    type PrunedSegmentMeta: PrunedSegmentMeta<Segment = Self::CompactSegment>;
    async fn read_compact_segment_through_cache(
        dal: Operator,
        location: Location,
        column_ids: Vec<ColumnId>,
        table_schema: TableSchemaRef,
    ) -> Result<Arc<Self::CompactSegment>> {
        Self::read_compact_segment(dal, location, column_ids, table_schema, true).await
    }

    async fn read_compact_segment(
        dal: Operator,
        location: Location,
        column_ids: Vec<ColumnId>,
        table_schema: TableSchemaRef,
        put_cache: bool,
    ) -> Result<Arc<Self::CompactSegment>>;

    async fn read_segment(
        dal: Operator,
        location: Location,
        column_ids: Vec<ColumnId>,
        table_schema: TableSchemaRef,
        put_cache: bool,
    ) -> Result<Self::Segment>;
}

pub struct RowOrientedSegmentReader;

#[async_trait::async_trait]
impl SegmentReader for RowOrientedSegmentReader {
    type Segment = SegmentInfo;
    type CompactSegment = CompactSegmentInfo;
    type SegmentBuilder = RowOrientedSegmentBuilder;
    type SegmentsWithIndices = CompactSegmentsWithIndices;
    type CompactTaskBuilder = RowOrientedCompactTaskBuilder;
    type PrunedSegmentMeta = PrunedCompactSegmentMeta;
    async fn read_compact_segment(
        dal: Operator,
        location: Location,
        _column_ids: Vec<ColumnId>,
        table_schema: TableSchemaRef,
        put_cache: bool,
    ) -> Result<Arc<Self::CompactSegment>> {
        SegmentsIO::read_compact_segment(dal, location, table_schema, put_cache).await
    }

    async fn read_segment(
        dal: Operator,
        location: Location,
        column_ids: Vec<ColumnId>,
        table_schema: TableSchemaRef,
        put_cache: bool,
    ) -> Result<Self::Segment> {
        let segment =
            Self::read_compact_segment(dal, location, column_ids, table_schema, put_cache).await?;
        Ok(segment.try_into()?)
    }
}

pub struct ColumnOrientedSegmentReader;

#[async_trait::async_trait]
impl SegmentReader for ColumnOrientedSegmentReader {
    type Segment = ColumnOrientedSegment;
    type CompactSegment = ColumnOrientedSegment;
    type SegmentBuilder = ColumnOrientedSegmentBuilder;
    type SegmentsWithIndices = ColumnOrientedSegmentsWithIndices;
    type CompactTaskBuilder = ColumnOrientedCompactTaskBuilder;
    type PrunedSegmentMeta = PrunedColumnOrientedSegmentMeta;
    async fn read_compact_segment(
        dal: Operator,
        location: Location,
        column_ids: Vec<ColumnId>,
        _table_schema: TableSchemaRef,
        put_cache: bool,
    ) -> Result<Arc<Self::CompactSegment>> {
        read_column_oriented_segment(dal, &location.0, column_ids, put_cache)
            .await
            .map(Arc::new)
    }

    async fn read_segment(
        dal: Operator,
        location: Location,
        column_ids: Vec<ColumnId>,
        _table_schema: TableSchemaRef,
        put_cache: bool,
    ) -> Result<Self::Segment> {
        read_column_oriented_segment(dal, &location.0, column_ids, put_cache).await
    }
}

// TODO(Sky): support projection for block level meta(like block location), for example: in compact segment, only block location is needed.
pub async fn read_column_oriented_segment(
    dal: Operator,
    location: &str,
    column_ids: Vec<ColumnId>,
    put_cache: bool,
) -> Result<ColumnOrientedSegment> {
    let cache = CacheManager::instance().get_column_oriented_segment_info_cache();
    let cached_segment = cache.get(location);
    match cached_segment {
        Some(segment) => {
            let mut missed_cols = Vec::new();
            for col_id in column_ids {
                if segment.stat_col(col_id).is_none() {
                    missed_cols.push(col_id);
                }
            }
            if missed_cols.is_empty() {
                return Ok((*segment).clone());
            }
            let reader = bytes_reader(&dal, location, None).await?;
            let (block_metas, schema, _) =
                deserialize_column_oriented_segment(reader.to_bytes(), &missed_cols, true)?;

            let mut merged_block_metas = segment.block_metas.clone();
            merged_block_metas.merge_block(block_metas);
            let mut merged_schema = segment.segment_schema.clone();
            merged_schema.add_columns(&schema.fields)?;
            let merged_segment = ColumnOrientedSegment {
                block_metas: merged_block_metas,
                summary: segment.summary().clone(),
                segment_schema: merged_schema,
            };

            if put_cache {
                cache.insert(location.to_string(), merged_segment.clone());
            }
            Ok(merged_segment)
        }
        None => {
            let reader = bytes_reader(&dal, location, None).await?;
            let (block_metas, segment_schema, summary) =
                deserialize_column_oriented_segment(reader.to_bytes(), &column_ids, false)?;
            let segment = ColumnOrientedSegment {
                block_metas,
                summary: summary.unwrap(),
                segment_schema,
            };

            if put_cache {
                cache.insert(location.to_string(), segment.clone());
            }
            Ok(segment)
        }
    }
}
