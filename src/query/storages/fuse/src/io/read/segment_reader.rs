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

use std::collections::HashSet;
use std::sync::Arc;

use bytes::Bytes;
use databend_common_exception::Result;
use databend_common_expression::TableSchemaRef;
use databend_storages_common_cache::CachePolicy;
use databend_storages_common_cache::DiskCachedStorage;
use databend_storages_common_cache::kinds::segment;
use databend_storages_common_table_meta::meta::CompactSegmentInfo;
use databend_storages_common_table_meta::meta::Location;
use databend_storages_common_table_meta::meta::SegmentInfo;
use databend_storages_common_table_meta::meta::column_oriented_segment::AbstractSegment;
use databend_storages_common_table_meta::meta::column_oriented_segment::ColumnOrientedSegment;
use databend_storages_common_table_meta::meta::column_oriented_segment::ColumnOrientedSegmentBuilder;
use databend_storages_common_table_meta::meta::column_oriented_segment::SegmentBuilder;
use databend_storages_common_table_meta::meta::column_oriented_segment::deserialize_column_oriented_segment;
use opendal::Operator;

use super::meta::read_whole;
use crate::io::FuseCaches;
use crate::io::SegmentsIO;
use crate::pruning_pipeline::PrunedColumnOrientedSegmentMeta;
use crate::pruning_pipeline::PrunedCompactSegmentMeta;
use crate::pruning_pipeline::PrunedSegmentMeta;
use crate::statistics::RowOrientedSegmentBuilder;

#[async_trait::async_trait]
pub trait SegmentReader: Send + Sync + 'static {
    type Segment: AbstractSegment;
    type CompactSegment: AbstractSegment;
    type SegmentBuilder: SegmentBuilder;
    type PrunedSegmentMeta: PrunedSegmentMeta<Segment = Self::CompactSegment>;
    async fn read_compact_segment(
        dal: Operator,
        location: Location,
        projection: &HashSet<String>,
        table_schema: TableSchemaRef,
        policy: CachePolicy,
    ) -> Result<Arc<Self::CompactSegment>>;

    async fn read_segment(
        dal: Operator,
        location: Location,
        projection: &HashSet<String>,
        table_schema: TableSchemaRef,
        policy: CachePolicy,
    ) -> Result<Self::Segment>;
}

pub struct RowOrientedSegmentReader;

#[async_trait::async_trait]
impl SegmentReader for RowOrientedSegmentReader {
    type Segment = SegmentInfo;
    type CompactSegment = CompactSegmentInfo;
    type SegmentBuilder = RowOrientedSegmentBuilder;
    type PrunedSegmentMeta = PrunedCompactSegmentMeta;
    async fn read_compact_segment(
        dal: Operator,
        location: Location,
        _projection: &HashSet<String>,
        table_schema: TableSchemaRef,
        policy: CachePolicy,
    ) -> Result<Arc<Self::CompactSegment>> {
        SegmentsIO::read_compact_segment(dal, location, table_schema, policy).await
    }

    async fn read_segment(
        dal: Operator,
        location: Location,
        projection: &HashSet<String>,
        table_schema: TableSchemaRef,
        policy: CachePolicy,
    ) -> Result<Self::Segment> {
        let segment =
            Self::read_compact_segment(dal, location, projection, table_schema, policy).await?;
        Ok(segment.try_into()?)
    }
}

pub struct ColumnOrientedSegmentReader;

#[async_trait::async_trait]
impl SegmentReader for ColumnOrientedSegmentReader {
    type Segment = ColumnOrientedSegment;
    type CompactSegment = ColumnOrientedSegment;
    type SegmentBuilder = ColumnOrientedSegmentBuilder;
    type PrunedSegmentMeta = PrunedColumnOrientedSegmentMeta;
    async fn read_compact_segment(
        dal: Operator,
        location: Location,
        projection: &HashSet<String>,
        _table_schema: TableSchemaRef,
        policy: CachePolicy,
    ) -> Result<Arc<Self::CompactSegment>> {
        read_column_oriented_segment(dal, &location.0, projection, policy)
            .await
            .map(Arc::new)
    }

    async fn read_segment(
        dal: Operator,
        location: Location,
        projection: &HashSet<String>,
        _table_schema: TableSchemaRef,
        policy: CachePolicy,
    ) -> Result<Self::Segment> {
        read_column_oriented_segment(dal, &location.0, projection, policy).await
    }
}

/// The whole column-oriented segment, through the disk cache. Columns are decoded on demand, so
/// a later projection reads the object again; the disk keeps it local.
async fn read_whole_segment(dal: Operator, location: &str, policy: CachePolicy) -> Result<Bytes> {
    read_whole::<segment::ColumnOriented>(&DiskCachedStorage::new(dal), location, policy.disk).await
}

/// Read a column-oriented segment from the storage or cache.
///
/// If the segment is already in cache, we'll check if all requested columns are available.
/// If some columns are missing, we'll read only those missing columns from storage and merge them
/// with the cached segment data to avoid reading the entire segment again.
///
/// # Arguments
///
/// * `dal`: The operator to read the segment from the storage.
/// * `location`: The location of the segment.
/// * `projection`: The names of the columns to be read.
/// * `policy`: What the read may do with the memory and disk caches.
///
/// # Returns
///
/// A column-oriented segment.
pub async fn read_column_oriented_segment(
    dal: Operator,
    location: &str,
    projection: &HashSet<String>,
    policy: CachePolicy,
) -> Result<ColumnOrientedSegment> {
    let cache = &FuseCaches::instance().segments.with_policy(policy);
    let cached_segment = cache.lookup_column_oriented(location);
    let need_summary = cached_segment.is_none();
    match cached_segment {
        Some(segment) => {
            let mut missed_cols = HashSet::new();
            for col_name in projection {
                if !segment.contains_col(col_name) {
                    missed_cols.insert(col_name.clone());
                }
            }
            if missed_cols.is_empty() {
                return Ok((*segment).clone());
            }
            let bytes = read_whole_segment(dal, location, policy).await?;
            let (block_metas, schema, _) =
                deserialize_column_oriented_segment(bytes, &missed_cols, need_summary)?;

            let mut merged_block_metas = segment.block_metas.clone();
            merged_block_metas.merge_block(block_metas);
            let mut merged_schema = segment.segment_schema.clone();
            merged_schema.add_columns(&schema.fields)?;
            let merged_segment = ColumnOrientedSegment {
                block_metas: merged_block_metas,
                summary: segment.summary().clone(),
                segment_schema: merged_schema,
            };

            cache.fill_column_oriented(location, Arc::new(merged_segment.clone()));
            Ok(merged_segment)
        }
        None => {
            let bytes = read_whole_segment(dal, location, policy).await?;
            let (block_metas, segment_schema, summary) =
                deserialize_column_oriented_segment(bytes, projection, need_summary)?;
            let segment = ColumnOrientedSegment {
                block_metas,
                summary: summary.unwrap(),
                segment_schema,
            };

            cache.fill_column_oriented(location, Arc::new(segment.clone()));
            Ok(segment)
        }
    }
}
