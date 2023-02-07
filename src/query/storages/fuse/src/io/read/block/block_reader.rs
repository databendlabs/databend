// Copyright 2021 Datafuse Labs.
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

use std::collections::BTreeMap;
use std::collections::HashMap;
use std::ops::Range;
use std::sync::Arc;
use std::time::Instant;

use common_arrow::arrow::datatypes::Field;
use common_arrow::arrow::io::parquet::write::to_parquet_schema;
use common_arrow::parquet::metadata::SchemaDescriptor;
use common_base::rangemap::RangeMerger;
use common_base::runtime::UnlimitedFuture;
use common_catalog::plan::PartInfoPtr;
use common_catalog::plan::Projection;
use common_catalog::table::ColumnId;
use common_exception::ErrorCode;
use common_exception::Result;
use common_expression::types::DataType;
use common_expression::DataField;
use common_expression::DataSchema;
use common_expression::TableField;
use common_expression::TableSchemaRef;
use common_storage::ColumnNode;
use common_storage::ColumnNodes;
use futures::future::try_join_all;
use opendal::Object;
use opendal::Operator;
use storages_common_table_meta::meta::ColumnMeta;

use crate::fuse_part::FusePartInfo;
use crate::io::read::ReadSettings;
use crate::metrics::*;

// TODO: make BlockReader as a trait.
#[derive(Clone)]
pub struct BlockReader {
    pub(crate) operator: Operator,
    pub(crate) projection: Projection,
    pub(crate) projected_schema: TableSchemaRef,
    pub(crate) project_indices: BTreeMap<usize, (ColumnId, Field, DataType)>,
    pub(crate) column_nodes: ColumnNodes,
    pub(crate) parquet_schema_descriptor: SchemaDescriptor,
}

pub struct OwnerMemory {
    chunks: HashMap<usize, Vec<u8>>,
}

impl OwnerMemory {
    pub fn create(chunks: Vec<(usize, Vec<u8>)>) -> OwnerMemory {
        let chunks = chunks.into_iter().collect::<HashMap<_, _>>();
        OwnerMemory { chunks }
    }

    pub fn get_chunk(&self, index: usize, path: &str) -> Result<&[u8]> {
        match self.chunks.get(&index) {
            Some(chunk) => Ok(chunk.as_slice()),
            None => Err(ErrorCode::Internal(format!(
                "It's a terrible bug, not found range data, merged_range_idx:{}, path:{}",
                index, path
            ))),
        }
    }
}

pub struct MergeIOReadResult
where Self: 'static
{
    path: String,
    owner_memory: OwnerMemory,
    columns_chunks: HashMap<usize, (usize, Range<usize>)>,
}

impl MergeIOReadResult
where Self: 'static
{
    pub fn create(owner_memory: OwnerMemory, capacity: usize, path: String) -> MergeIOReadResult {
        MergeIOReadResult {
            path,
            owner_memory,
            columns_chunks: HashMap::with_capacity(capacity),
        }
    }

    pub fn columns_chunks(&self) -> Result<Vec<(usize, &[u8])>> {
        let mut res = Vec::with_capacity(self.columns_chunks.len());

        for (column_idx, (chunk_idx, range)) in &self.columns_chunks {
            let chunk = self.owner_memory.get_chunk(*chunk_idx, &self.path)?;
            res.push((*column_idx, &chunk[range.clone()]));
        }

        Ok(res)
    }

    pub fn get_chunk(&self, index: usize, path: &str) -> Result<&[u8]> {
        self.owner_memory.get_chunk(index, path)
    }

    pub fn add_column_chunk(&mut self, chunk: usize, column: usize, range: Range<usize>) {
        self.columns_chunks.insert(column, (chunk, range));
    }
}

impl BlockReader {
    pub fn create(
        operator: Operator,
        schema: TableSchemaRef,
        projection: Projection,
    ) -> Result<Arc<BlockReader>> {
        let projected_schema = match projection {
            Projection::Columns(ref indices) => TableSchemaRef::new(schema.project(indices)),
            Projection::InnerColumns(ref path_indices) => {
                Arc::new(schema.inner_project(path_indices))
            }
        };

        let arrow_schema = schema.to_arrow();
        let parquet_schema_descriptor = to_parquet_schema(&arrow_schema)?;

        let column_nodes = ColumnNodes::new_from_schema(&arrow_schema, Some(&schema));
        let project_column_nodes: Vec<ColumnNode> = projection
            .project_column_nodes(&column_nodes)?
            .iter()
            .map(|c| (*c).clone())
            .collect();
        let project_indices = Self::build_projection_indices(&project_column_nodes);

        Ok(Arc::new(BlockReader {
            operator,
            projection,
            projected_schema,
            parquet_schema_descriptor,
            column_nodes,
            project_indices,
        }))
    }

    pub fn support_blocking_api(&self) -> bool {
        self.operator.metadata().can_blocking()
    }

    /// This is an optimized for data read, works like the Linux kernel io-scheduler IO merging.
    /// If the distance between two IO request ranges to be read is less than storage_io_min_bytes_for_seek(Default is 48Bytes),
    /// will read the range that contains both ranges, thus avoiding extra seek.
    ///
    /// It will *NOT* merge two requests:
    /// if the last io request size is larger than storage_io_page_bytes_for_read(Default is 512KB).
    pub async fn merge_io_read(
        read_settings: &ReadSettings,
        object: Object,
        raw_ranges: Vec<(usize, Range<u64>)>,
    ) -> Result<MergeIOReadResult> {
        let path = object.path().to_string();

        // Build merged read ranges.
        let ranges = raw_ranges
            .iter()
            .map(|(_, r)| r.clone())
            .collect::<Vec<_>>();
        let range_merger = RangeMerger::from_iter(
            ranges,
            read_settings.storage_io_min_bytes_for_seek,
            read_settings.storage_io_max_page_bytes_for_read,
        );
        let merged_ranges = range_merger.ranges();

        // Read merged range data.
        let mut read_handlers = Vec::with_capacity(merged_ranges.len());
        for (idx, range) in merged_ranges.iter().enumerate() {
            // Perf.
            {
                metrics_inc_remote_io_seeks_after_merged(1);
                metrics_inc_remote_io_read_bytes_after_merged(range.end - range.start);
            }

            read_handlers.push(UnlimitedFuture::create(Self::read_range(
                object.clone(),
                idx,
                range.start,
                range.end,
            )));
        }

        let start = Instant::now();
        let owner_memory = OwnerMemory::create(try_join_all(read_handlers).await?);
        let mut read_res = MergeIOReadResult::create(owner_memory, raw_ranges.len(), path.clone());

        // Perf.
        {
            metrics_inc_remote_io_read_milliseconds(start.elapsed().as_millis() as u64);
        }

        for (raw_idx, raw_range) in &raw_ranges {
            let column_range = raw_range.start..raw_range.end;

            // Find the range index and Range from merged ranges.
            let (merged_range_idx, merged_range) = match range_merger.get(column_range.clone()) {
                None => Err(ErrorCode::Internal(format!(
                    "It's a terrible bug, not found raw range:[{:?}], path:{} from merged ranges\n: {:?}",
                    column_range, path, merged_ranges
                ))),
                Some((index, range)) => Ok((index, range)),
            }?;

            // Fetch the raw data for the raw range.
            let start = (column_range.start - merged_range.start) as usize;
            let end = (column_range.end - merged_range.start) as usize;
            read_res.add_column_chunk(merged_range_idx, *raw_idx, start..end);
        }

        Ok(read_res)
    }

    pub fn sync_merge_io_read(
        read_settings: &ReadSettings,
        object: Object,
        raw_ranges: Vec<(usize, Range<u64>)>,
    ) -> Result<MergeIOReadResult> {
        let path = object.path().to_string();

        // Build merged read ranges.
        let ranges = raw_ranges
            .iter()
            .map(|(_, r)| r.clone())
            .collect::<Vec<_>>();
        let range_merger = RangeMerger::from_iter(
            ranges,
            read_settings.storage_io_min_bytes_for_seek,
            read_settings.storage_io_max_page_bytes_for_read,
        );
        let merged_ranges = range_merger.ranges();

        // Read merged range data.
        let mut io_res = Vec::with_capacity(merged_ranges.len());
        for (idx, range) in merged_ranges.iter().enumerate() {
            io_res.push(Self::sync_read_range(
                object.clone(),
                idx,
                range.start,
                range.end,
            )?);
        }

        let owner_memory = OwnerMemory::create(io_res);
        let mut read_res = MergeIOReadResult::create(owner_memory, raw_ranges.len(), path.clone());

        for (raw_idx, raw_range) in &raw_ranges {
            let column_range = raw_range.start..raw_range.end;

            // Find the range index and Range from merged ranges.
            let (merged_range_idx, merged_range) = match range_merger.get(column_range.clone()) {
                None => Err(ErrorCode::Internal(format!(
                    "It's a terrible bug, not found raw range:[{:?}], path:{} from merged ranges\n: {:?}",
                    column_range, path, merged_ranges
                ))),
                Some((index, range)) => Ok((index, range)),
            }?;

            // Fetch the raw data for the raw range.
            let start = (column_range.start - merged_range.start) as usize;
            let end = (column_range.end - merged_range.start) as usize;
            read_res.add_column_chunk(merged_range_idx, *raw_idx, start..end);
        }

        Ok(read_res)
    }

    pub async fn read_columns_data_by_merge_io(
        &self,
        settings: &ReadSettings,
        location: &str,
        columns_meta: &HashMap<ColumnId, ColumnMeta>,
    ) -> Result<MergeIOReadResult> {
        // Perf
        {
            metrics_inc_remote_io_read_parts(1);
        }

        let mut ranges = vec![];
        for (index, (column_id, ..)) in self.project_indices.iter() {
            if let Some(column_meta) = columns_meta.get(column_id) {
                let (offset, len) = column_meta.offset_length();
                ranges.push((*index, offset..(offset + len)));

                // Perf
                {
                    metrics_inc_remote_io_seeks(1);
                    metrics_inc_remote_io_read_bytes(len);
                }
            }
        }

        let object = self.operator.object(location);

        Self::merge_io_read(settings, object, ranges).await
    }

    pub fn sync_read_columns_data_by_merge_io(
        &self,
        settings: &ReadSettings,
        part: PartInfoPtr,
    ) -> Result<MergeIOReadResult> {
        let part = FusePartInfo::from_part(&part)?;

        let mut ranges = vec![];
        for (index, (column_id, ..)) in self.project_indices.iter() {
            if let Some(column_meta) = part.columns_meta.get(column_id) {
                let (offset, len) = column_meta.offset_length();
                ranges.push((*index, offset..(offset + len)));
            }
        }

        let object = self.operator.object(&part.location);
        Self::sync_merge_io_read(settings, object, ranges)
    }

    // Build non duplicate leaf_ids to avoid repeated read column from parquet
    pub(crate) fn build_projection_indices(
        columns: &[ColumnNode],
    ) -> BTreeMap<usize, (ColumnId, Field, DataType)> {
        let mut indices = BTreeMap::new();
        for column in columns {
            for (i, index) in column.leaf_ids.iter().enumerate() {
                let f: TableField = (&column.field).into();
                let data_type: DataType = f.data_type().into();
                indices.insert(
                    *index,
                    (column.leaf_column_id(i), column.field.clone(), data_type),
                );
            }
        }
        indices
    }

    #[inline]
    pub async fn read_range(
        o: Object,
        index: usize,
        start: u64,
        end: u64,
    ) -> Result<(usize, Vec<u8>)> {
        let chunk = o.range_read(start..end).await?;
        Ok((index, chunk))
    }

    #[inline]
    pub fn sync_read_range(
        o: Object,
        index: usize,
        start: u64,
        end: u64,
    ) -> Result<(usize, Vec<u8>)> {
        let chunk = o.blocking_range_read(start..end)?;
        Ok((index, chunk))
    }

    pub fn schema(&self) -> TableSchemaRef {
        self.projected_schema.clone()
    }

    pub fn data_fields(&self) -> Vec<DataField> {
        self.schema().fields().iter().map(DataField::from).collect()
    }

    pub fn data_schema(&self) -> DataSchema {
        let fields = self.data_fields();
        DataSchema::new(fields)
    }
}
