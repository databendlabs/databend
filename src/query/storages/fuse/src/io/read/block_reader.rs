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

use std::collections::HashMap;
use std::collections::VecDeque;
use std::io::IoSliceMut;
use std::io::Read;
use std::ops::Range;
use std::pin::Pin;
use std::sync::Arc;
use std::task::Context;
use std::task::Poll;
use std::time::Instant;

use bytes::Bytes;
use common_arrow::arrow::datatypes::Field;
use common_arrow::arrow::io::parquet::write::to_parquet_schema;
use common_arrow::parquet::metadata::SchemaDescriptor;
use common_base::rangemap::RangeMerger;
use common_base::runtime::UnlimitedFuture;
use common_catalog::plan::PartInfoPtr;
use common_catalog::plan::Projection;
use common_exception::ErrorCode;
use common_exception::Result;
use common_expression::DataField;
use common_expression::DataSchema;
use common_expression::TableSchemaRef;
use common_storage::ColumnLeaf;
use common_storage::ColumnLeaves;
use common_storages_table_meta::meta::ColumnMeta;
use futures::future::try_join_all;
use futures::Stream;
use futures_util::TryStreamExt;
use opendal::raw::output::Read as DALRead;
use opendal::Object;
use opendal::ObjectReader;
use opendal::Operator;

use crate::fuse_part::FusePartInfo;
use crate::io::read::ReadSettings;
use crate::metrics::*;

// TODO: make BlockReader as a trait.
#[derive(Clone)]
pub struct BlockReader {
    pub(crate) operator: Operator,
    pub(crate) projection: Projection,
    pub(crate) projected_schema: TableSchemaRef,
    pub(crate) column_leaves: ColumnLeaves,
    pub(crate) parquet_schema_descriptor: SchemaDescriptor,
}

pub struct OwnerMemory {
    chunks: HashMap<usize, Vec<Bytes>>,
}

impl OwnerMemory {
    pub fn create(chunks: Vec<(usize, Vec<Bytes>)>) -> OwnerMemory {
        let chunks = chunks.into_iter().collect::<HashMap<_, _>>();
        OwnerMemory { chunks }
    }

    pub fn get_chunk(
        &self,
        index: usize,
        path: &str,
        range: Range<usize>,
    ) -> Result<ChunksReader<'_>> {
        match self.chunks.get(&index) {
            Some(chunks) => {
                let mut bytes = VecDeque::new();
                let mut skipping = range.start;
                let mut remaing = range.end - range.start;

                let mut pos = 0;
                while skipping > 0 && pos < chunks.len() {
                    if skipping < chunks[pos].len() {
                        unsafe {
                            let take = std::cmp::min(chunks[pos].len() - skipping, remaing);
                            let chunk_data = chunks[pos].slice(skipping..skipping + take);
                            bytes.push_back(std::slice::from_raw_parts(
                                chunk_data.as_ptr(),
                                chunk_data.len(),
                            ));
                            remaing -= take;
                        }
                    }

                    pos += 1;
                    skipping -= std::cmp::min(skipping, chunks[pos].len());
                }

                while remaing > 0 && pos < chunks.len() {
                    unsafe {
                        let take = std::cmp::min(chunks[pos].len(), remaing);
                        let chunk_data = chunks[pos].slice(0..take);
                        bytes.push_back(std::slice::from_raw_parts(
                            chunk_data.as_ptr(),
                            chunk_data.len(),
                        ));
                        remaing -= take;
                    }
                }

                Ok(ChunksReader::create(bytes))
            }
            None => Err(ErrorCode::Internal(format!(
                "It's a terrible bug, not found range data, merged_range_idx:{}, path:{}",
                index, path
            ))),
        }
    }
}

#[derive(Clone)]
pub struct ChunksReader<'a> {
    bytes: VecDeque<&'a [u8]>,
}

impl<'a> ChunksReader<'a> {
    pub fn create(bytes: VecDeque<&'a [u8]>) -> ChunksReader<'a> {
        ChunksReader::<'a> { bytes }
    }

    #[allow(clippy::wrong_self_convention)]
    pub fn to_vec(mut self) -> Vec<u8> {
        let len = self.bytes.iter().map(|s| s.len()).sum();
        let mut array = Vec::with_capacity(len);

        while let Some(bytes) = self.bytes.pop_front() {
            array.extend_from_slice(bytes);
        }

        array
    }
}

impl<'a> Read for ChunksReader<'a> {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        while let Some(first) = self.bytes.front_mut() {
            match first.read(buf)? {
                0 if !buf.is_empty() => {
                    self.bytes.pop_front();
                }
                n => return Ok(n),
            }
        }

        Ok(0)
    }

    fn read_vectored(&mut self, bufs: &mut [IoSliceMut<'_>]) -> std::io::Result<usize> {
        while let Some(first) = self.bytes.front_mut() {
            match first.read_vectored(bufs)? {
                0 if bufs.iter().any(|b| !b.is_empty()) => {
                    self.bytes.pop_front();
                }
                n => return Ok(n),
            }
        }

        Ok(0)
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

    pub fn columns_chunks(&self) -> Result<Vec<(usize, ChunksReader<'_>)>> {
        let mut res = Vec::with_capacity(self.columns_chunks.len());

        for (column_idx, (chunk_idx, range)) in &self.columns_chunks {
            let chunk = self
                .owner_memory
                .get_chunk(*chunk_idx, &self.path, range.clone())?;
            res.push((*column_idx, chunk));
        }

        Ok(res)
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
        let column_leaves = ColumnLeaves::new_from_schema(&arrow_schema);

        Ok(Arc::new(BlockReader {
            operator,
            projection,
            projected_schema,
            parquet_schema_descriptor,
            column_leaves,
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
        columns_meta: &HashMap<usize, ColumnMeta>,
    ) -> Result<MergeIOReadResult> {
        // Perf
        {
            metrics_inc_remote_io_read_parts(1);
        }

        let columns = self.projection.project_column_leaves(&self.column_leaves)?;
        let indices = Self::build_projection_indices(&columns);

        let mut ranges = vec![];
        for index in indices.keys() {
            let column_meta = &columns_meta[index];
            ranges.push((
                *index,
                column_meta.offset..(column_meta.offset + column_meta.len),
            ));

            // Perf
            {
                metrics_inc_remote_io_seeks(1);
                metrics_inc_remote_io_read_bytes(column_meta.len);
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
        let columns = self.projection.project_column_leaves(&self.column_leaves)?;
        let indices = Self::build_projection_indices(&columns);

        let mut ranges = vec![];
        for index in indices.keys() {
            let column_meta = &part.columns_meta[index];
            ranges.push((
                *index,
                column_meta.offset..(column_meta.offset + column_meta.len),
            ));
        }

        let object = self.operator.object(&part.location);
        Self::sync_merge_io_read(settings, object, ranges)
    }

    // Build non duplicate leaf_ids to avoid repeated read column from parquet
    pub(crate) fn build_projection_indices(columns: &Vec<&ColumnLeaf>) -> HashMap<usize, Field> {
        let mut indices = HashMap::with_capacity(columns.len());
        for column in columns {
            for index in &column.leaf_ids {
                indices.insert(*index, column.field.clone());
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
    ) -> Result<(usize, Vec<Bytes>)> {
        struct ReaderStream {
            reader: ObjectReader,
        }

        impl Stream for ReaderStream {
            type Item = std::io::Result<Bytes>;

            fn poll_next(
                mut self: Pin<&mut Self>,
                cx: &mut Context<'_>,
            ) -> Poll<Option<Self::Item>> {
                self.reader.poll_next(cx)
            }
        }

        let stream = ReaderStream {
            reader: o.range_reader(start..end).await?,
        };
        Ok((index, stream.try_collect::<Vec<_>>().await?))
    }

    #[inline]
    pub fn sync_read_range(
        o: Object,
        index: usize,
        start: u64,
        end: u64,
    ) -> Result<(usize, Vec<Bytes>)> {
        let chunk = o.blocking_range_read(start..end)?;
        Ok((index, vec![Bytes::from(chunk)]))
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
