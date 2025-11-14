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

use std::collections::HashMap;
use std::ops::Range;
use std::sync::Arc;

use bytes::Buf;
use bytes::Bytes;
use databend_common_base::rangemap::RangeMerger;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_storages_common_cache::CacheAccessor;
use databend_storages_common_cache::CacheManager;
use databend_storages_common_cache::ColumnData;
use opendal::Operator;
use parquet::arrow::arrow_reader::RowGroups;
use parquet::arrow::arrow_reader::RowSelection;
use parquet::arrow::ProjectionMask;
use parquet::column::page::PageIterator;
use parquet::column::page::PageReader;
use parquet::errors::ParquetError;
use parquet::file::metadata::RowGroupMetaData;
use parquet::file::reader::ChunkReader;
use parquet::file::reader::Length;
use parquet::file::serialized_reader::SerializedPageReader;
use parquet::format::PageLocation;

use crate::read_settings::ReadSettings;

/// An in-memory column chunk.
///
/// It is a private struct in apache `parquet` crate, so we just copied it here.
#[derive(Clone)]
enum ColumnChunkData {
    /// Column chunk data representing only a subset of data pages
    Sparse {
        /// Length of the full column chunk
        length: usize,
        /// Set of data pages included in this sparse chunk. Each element is a tuple
        /// of (page offset, page data)
        data: Vec<(usize, Bytes)>,
    },
    /// Full column chunk and its offset
    Dense { offset: usize, data: Bytes },
}

impl ColumnChunkData {
    fn get(&self, start: u64) -> parquet::errors::Result<Bytes> {
        match &self {
            ColumnChunkData::Sparse { data, .. } => data
                .binary_search_by_key(&start, |(offset, _)| *offset as u64)
                .map(|idx| data[idx].1.clone())
                .map_err(|_| {
                    ParquetError::General(format!(
                        "Invalid offset in sparse column chunk data: {start}"
                    ))
                }),
            ColumnChunkData::Dense { offset, data } => {
                let start = start as usize - *offset;
                Ok(data.slice(start..))
            }
        }
    }
}

impl Length for ColumnChunkData {
    fn len(&self) -> u64 {
        match &self {
            ColumnChunkData::Sparse { length, .. } => *length as u64,
            ColumnChunkData::Dense { data, .. } => data.len() as u64,
        }
    }
}

impl ChunkReader for ColumnChunkData {
    type T = bytes::buf::Reader<Bytes>;

    fn get_read(&self, start: u64) -> parquet::errors::Result<Self::T> {
        Ok(self.get(start)?.reader())
    }

    fn get_bytes(&self, start: u64, length: usize) -> parquet::errors::Result<Bytes> {
        Ok(self.get(start)?.slice(..length))
    }
}

/// An in-memory collection of column chunks.
///
/// It's inspired by `InMemoryRowGroup` in apache `parquet` crate,
/// but it is a private struct. Therefore, we copied the main codes here and did some optimizations.
pub struct InMemoryRowGroup<'a> {
    location: &'a str,
    op: Operator,

    core: RowGroupCore<&'a RowGroupMetaData>,
    read_settings: ReadSettings,
}

impl<'a> InMemoryRowGroup<'a> {
    pub fn new(
        location: &'a str,
        op: Operator,
        rg: &'a RowGroupMetaData,
        page_locations: Option<Vec<Vec<PageLocation>>>,
        read_settings: ReadSettings,
    ) -> Self {
        Self {
            location,
            op,
            core: RowGroupCore::new(rg, page_locations),
            read_settings,
        }
    }

    pub fn row_count(&self) -> usize {
        self.core.num_rows()
    }

    /// Fetches the necessary column data into memory
    ///
    /// If call `fetch` multiple times, it will only fetch the data that has not been fetched.
    pub async fn fetch(
        &mut self,
        projection: &ProjectionMask,
        selection: Option<&RowSelection>,
    ) -> Result<()> {
        self.core
            .async_fetch(projection, selection, async |ranges| {
                let (chunk, _) =
                    get_ranges(ranges, &self.read_settings, self.location, &self.op).await?;
                Ok(chunk)
            })
            .await
    }
}

pub async fn get_ranges(
    ranges: &[Range<u64>],
    read_settings: &ReadSettings,
    location: &str,
    op: &Operator,
) -> Result<(Vec<Bytes>, bool)> {
    let range_merger = RangeMerger::from_iter(
        ranges.iter().cloned(),
        read_settings.max_gap_size,
        read_settings.max_range_size,
        Some(read_settings.parquet_fast_read_bytes),
    );
    let merged_ranges = range_merger.ranges();
    let merged = merged_ranges.len() < ranges.len();

    let chunks = cached_range_read(op, location, merged_ranges, read_settings.enable_cache).await?;

    Ok((
        ranges
            .iter()
            .cloned()
            .map(|raw_range| {
                let range = range_merger.get(raw_range.clone()).unwrap().1;
                let chunk = chunks.get(&range).unwrap();
                let start = (raw_range.start - range.start) as usize;
                let end = (raw_range.end - range.start) as usize;
                chunk.clone().slice(start..end)
            })
            .collect(),
        merged,
    ))
}

pub struct RowGroupCore<T> {
    metadata: T,
    page_locations: Option<Vec<Vec<PageLocation>>>,
    column_chunks: Vec<Option<Arc<ColumnChunkData>>>,
}

impl<T: AsMetaRef> RowGroupCore<T> {
    pub fn new(meta: T, page_locations: Option<Vec<Vec<PageLocation>>>) -> RowGroupCore<T> {
        RowGroupCore {
            column_chunks: vec![None; meta.meta().num_columns()],
            metadata: meta,
            page_locations,
        }
    }

    pub async fn async_fetch(
        &mut self,
        projection: &ProjectionMask,
        selection: Option<&RowSelection>,
        get_ranges: impl AsyncFnOnce(&[Range<u64>]) -> Result<Vec<Bytes>>,
    ) -> Result<()> {
        if let Some((selection, page_locations)) = selection.zip(self.page_locations.as_ref()) {
            // If we have a `RowSelection` and an `OffsetIndex` then only fetch pages required for the
            // `RowSelection`
            let (fetch_ranges, page_start_offsets) =
                self.get_fetch_ranges_with_index(projection, selection, page_locations);

            // Fetch ranges in different async tasks.
            let chunk_data = get_ranges(&fetch_ranges).await?;

            self.set_data_with_index(projection, chunk_data, page_start_offsets);
            Ok(())
        } else {
            let fetch_ranges = self.get_fetch_ranges_without_index(projection);

            // Fetch ranges in different async tasks.
            let chunk_data = get_ranges(&fetch_ranges).await?;

            self.set_data_without_index(projection, chunk_data);
            Ok(())
        }
    }

    pub fn fetch(
        &mut self,
        projection: &ProjectionMask,
        selection: Option<&RowSelection>,
        get_ranges: impl Fn(Vec<Range<u64>>) -> Result<Vec<Bytes>>,
    ) -> Result<()> {
        if let Some((selection, page_locations)) = selection.zip(self.page_locations.as_ref()) {
            // If we have a `RowSelection` and an `OffsetIndex` then only fetch pages required for the
            // `RowSelection`
            let (fetch_ranges, page_start_offsets) =
                self.get_fetch_ranges_with_index(projection, selection, page_locations);

            // Fetch ranges in different async tasks.
            let chunk_data = get_ranges(fetch_ranges)?;

            self.set_data_with_index(projection, chunk_data, page_start_offsets);
            Ok(())
        } else {
            let fetch_ranges = self.get_fetch_ranges_without_index(projection);

            // Fetch ranges in different async tasks.
            let chunk_data = get_ranges(fetch_ranges)?;

            self.set_data_without_index(projection, chunk_data);
            Ok(())
        }
    }

    fn get_fetch_ranges_with_index(
        &self,
        projection: &ProjectionMask,
        selection: &RowSelection,
        page_locations: &[Vec<PageLocation>],
    ) -> (Vec<Range<u64>>, Vec<Vec<usize>>) {
        let mut page_start_offsets: Vec<Vec<usize>> = vec![];

        let fetch_ranges = self
            .column_chunks
            .iter()
            .zip(self.metadata.meta().columns())
            .enumerate()
            .filter(|&(idx, (chunk, _chunk_meta))| chunk.is_none() && projection.leaf_included(idx))
            .flat_map(|(idx, (_chunk, chunk_meta))| {
                // If the first page does not start at the beginning of the column,
                // then we need to also fetch a dictionary page.
                let mut ranges = vec![];
                let (start, _len) = chunk_meta.byte_range();
                match page_locations[idx].first() {
                    Some(first) if first.offset as u64 != start => {
                        ranges.push(start..first.offset as u64);
                    }
                    _ => (),
                }

                ranges.extend(
                    selection
                        .scan_ranges(&page_locations[idx])
                        .iter()
                        .map(|r| r.start..r.end),
                );
                page_start_offsets.push(ranges.iter().map(|range| range.start as usize).collect());

                ranges
            })
            .collect::<Vec<_>>();
        (fetch_ranges, page_start_offsets)
    }

    fn set_data_with_index(
        &mut self,
        projection: &ProjectionMask,
        chunk_data: Vec<Bytes>,
        page_start_offsets: Vec<Vec<usize>>,
    ) {
        let mut chunk_iter = chunk_data.into_iter();
        let mut page_start_offsets = page_start_offsets.into_iter();

        for (idx, chunk) in self.column_chunks.iter_mut().enumerate() {
            if chunk.is_some() || !projection.leaf_included(idx) {
                continue;
            }

            if let Some(offsets) = page_start_offsets.next() {
                let mut chunks = Vec::with_capacity(offsets.len());
                for _ in 0..offsets.len() {
                    chunks.push(chunk_iter.next().unwrap());
                }

                *chunk = Some(Arc::new(ColumnChunkData::Sparse {
                    length: self.metadata.meta().column(idx).byte_range().1 as usize,
                    data: offsets.into_iter().zip(chunks.into_iter()).collect(),
                }))
            }
        }
    }

    fn get_fetch_ranges_without_index(&self, projection: &ProjectionMask) -> Vec<Range<u64>> {
        self.column_chunks
            .iter()
            .enumerate()
            .filter(|&(idx, chunk)| (chunk.is_none() && projection.leaf_included(idx)))
            .map(|(idx, _chunk)| {
                let column = self.metadata.meta().column(idx);
                let (start, length) = column.byte_range();
                start..(start + length)
            })
            .collect()
    }

    fn set_data_without_index(&mut self, projection: &ProjectionMask, chunk_data: Vec<Bytes>) {
        let mut chunk_iter = chunk_data.into_iter();

        for (idx, chunk) in self.column_chunks.iter_mut().enumerate() {
            if chunk.is_some() || !projection.leaf_included(idx) {
                continue;
            }

            if let Some(data) = chunk_iter.next() {
                *chunk = Some(Arc::new(ColumnChunkData::Dense {
                    offset: self.metadata.meta().column(idx).byte_range().0 as usize,
                    data,
                }));
            }
        }
    }
}

impl<T: AsMetaRef> RowGroups for RowGroupCore<T> {
    fn num_rows(&self) -> usize {
        self.metadata.meta().num_rows() as _
    }

    fn column_chunks(&self, i: usize) -> parquet::errors::Result<Box<dyn PageIterator>> {
        match &self.column_chunks[i] {
            None => Err(ParquetError::General(format!(
                "Invalid column index {i}, column was not fetched"
            ))),
            Some(data) => {
                let page_locations = self.page_locations.as_ref().map(|index| index[i].clone());
                let page_reader: Box<dyn PageReader> = Box::new(SerializedPageReader::new(
                    data.clone(),
                    self.metadata.meta().column(i),
                    self.num_rows(),
                    page_locations,
                )?);

                Ok(Box::new(ColumnChunkIterator {
                    reader: Some(Ok(page_reader)),
                }))
            }
        }
    }
}

pub trait AsMetaRef {
    fn meta(&self) -> &RowGroupMetaData;
}

impl AsMetaRef for &RowGroupMetaData {
    fn meta(&self) -> &RowGroupMetaData {
        self
    }
}

impl AsMetaRef for RowGroupMetaData {
    fn meta(&self) -> &RowGroupMetaData {
        self
    }
}

impl AsMetaRef for Arc<RowGroupMetaData> {
    fn meta(&self) -> &RowGroupMetaData {
        self.as_ref()
    }
}

/// Takes a function and spawns it to a tokio blocking pool if available
pub async fn maybe_spawn_blocking<F, T>(f: F) -> Result<T>
where
    F: FnOnce() -> Result<T> + Send + 'static,
    T: Send + 'static,
{
    match databend_common_base::runtime::try_spawn_blocking(f) {
        Ok(handler) => handler.await.map_err(ErrorCode::from_std_error)?,
        Err(f) => f(),
    }
}

/// Implements [`PageIterator`] for a single column chunk, yielding a single [`PageReader`]
///
/// It is a private struct in apache `parquet` crate, so we just copied it here.
struct ColumnChunkIterator {
    reader: Option<parquet::errors::Result<Box<dyn PageReader>>>,
}

impl Iterator for ColumnChunkIterator {
    type Item = parquet::errors::Result<Box<dyn PageReader>>;

    fn next(&mut self) -> Option<Self::Item> {
        self.reader.take()
    }
}

impl PageIterator for ColumnChunkIterator {}

impl RowGroups for InMemoryRowGroup<'_> {
    fn num_rows(&self) -> usize {
        self.core.num_rows()
    }

    fn column_chunks(&self, i: usize) -> parquet::errors::Result<Box<dyn PageIterator>> {
        self.core.column_chunks(i)
    }
}

pub async fn cached_range_full_read(
    op: &Operator,
    location: &str,
    size: usize,
    enable_cache: bool,
) -> Result<Bytes> {
    let range = 0..size as u64;
    let merged_ranges = vec![range];
    cached_range_read(op, location, merged_ranges, enable_cache)
        .await
        .map(|map| map.values().last().unwrap().clone())
}

pub async fn cached_range_read(
    op: &Operator,
    location: &str,
    merged_ranges: Vec<Range<u64>>,
    enable_cache: bool,
) -> Result<HashMap<Range<u64>, Bytes>> {
    let mut handles = Vec::with_capacity(merged_ranges.len());
    for range in merged_ranges {
        let fut_read = op.read_with(location);
        let key = format!("{}_{location}_{range:?}", op.info().root());
        handles.push(async move {
            let column_data_cache = if enable_cache {
                CacheManager::instance().get_column_data_cache()
            } else {
                None
            };
            if let Some(buffer) = column_data_cache
                .as_ref()
                .and_then(|cache| cache.get_sized(&key, range.end - range.start))
            {
                Ok::<_, ErrorCode>((range, buffer.bytes()))
            } else {
                let data = fut_read.range(range.start..range.end).await?;
                let data = data.to_bytes();
                if let Some(cache) = &column_data_cache {
                    cache.insert(key, ColumnData::from_bytes(data.clone()));
                }
                Ok::<_, ErrorCode>((range, data))
            }
        });
    }
    let chunk_data = futures::future::try_join_all(handles).await?;
    Ok(chunk_data.into_iter().collect())
}

#[cfg(test)]
mod test {
    use std::sync::Arc;

    use arrow_array::ArrayRef;
    use arrow_array::Int64Array;
    use arrow_array::RecordBatch;
    use arrow_schema::DataType;
    use arrow_schema::Field;
    use arrow_schema::Schema;
    use bytes::Bytes;
    use databend_common_base::base::tokio;
    use opendal::services::Memory;
    use opendal::Operator;
    use parquet::arrow::ArrowWriter;
    use parquet::basic::Repetition;
    use parquet::file::metadata::RowGroupMetaData;
    use parquet::schema::types::*;
    use rand::Rng;

    use super::*;

    #[tokio::test]
    async fn test_merge() {
        // Set up random number generator
        let mut rng = rand::thread_rng();

        // Define the number of rows and columns in the Parquet file
        let num_rows = 100;
        let num_cols = rng.gen_range(2..10);

        // Generate random column names
        let mut column_names = Vec::new();
        for i in 0..num_cols {
            column_names.push(format!("column_{}", i));
        }

        // Define the schema for the Parquet file
        let mut fields: Vec<Field> = Vec::new();
        for name in &column_names {
            fields.push(Field::new(name, DataType::Int64, false));
        }
        let schema = Schema::new(fields);

        // Generate random data for each column
        let mut data = Vec::new();
        for _ in 0..num_cols {
            let values: Vec<i64> = (0..num_rows).map(|_| rng.gen_range(0..100)).collect();
            let array: ArrayRef = Arc::new(Int64Array::from(values));
            data.push(array);
        }

        // Create a RecordBatch from the data and schema
        let batch = RecordBatch::try_new(Arc::new(schema.clone()), data).unwrap();

        let mut buf = Vec::with_capacity(1024);
        let mut writer = ArrowWriter::try_new(&mut buf, batch.schema(), None).unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();

        // prepare data
        let data = Bytes::from(buf);
        let builder = Memory::default();
        let path = "/tmp/test/merged";
        let op = Operator::new(builder).unwrap().finish();
        op.write(path, data).await.unwrap();

        let schema = Type::group_type_builder("schema")
            .with_repetition(Repetition::REPEATED)
            .build()
            .unwrap();
        let descr = SchemaDescriptor::new(Arc::new(schema));
        let meta = RowGroupMetaData::builder(descr.into()).build().unwrap();

        // for gap=0;
        let gap0 = InMemoryRowGroup::new(path, op.clone(), &meta, None, ReadSettings {
            max_gap_size: 0,
            max_range_size: 0,
            parquet_fast_read_bytes: 1,
            enable_cache: false,
        });

        // for gap=10
        let gap10 = InMemoryRowGroup::new(path, op, &meta, None, ReadSettings {
            max_gap_size: 10,
            max_range_size: 200,
            parquet_fast_read_bytes: 1,
            enable_cache: false,
        });
        let ranges = [(1..10), (15..30), (40..50)];
        let (gap0_chunks, gap0_merged) = get_ranges(
            ranges.as_ref(),
            &gap0.read_settings,
            gap0.location,
            &gap0.op,
        )
        .await
        .unwrap();

        let (gap10_chunks, gap10_merged) = get_ranges(
            ranges.as_ref(),
            &gap10.read_settings,
            gap10.location,
            &gap10.op,
        )
        .await
        .unwrap();
        // gap=0 no merged
        assert!(!gap0_merged);
        // gap=10  merge happened
        assert!(gap10_merged);
        // compare chunks
        for (chunk0, chunk10) in gap0_chunks.iter().zip(gap10_chunks.iter()) {
            assert_eq!(*chunk0, *chunk10);
        }
    }
}
