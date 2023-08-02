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
use std::collections::HashSet;
use std::sync::Arc;
use std::time::Instant;

use common_arrow::arrow::bitmap::Bitmap;
use common_catalog::plan::PartInfoPtr;
use common_exception::ErrorCode;
use common_exception::Result;
use common_expression::DataBlock;
use common_expression::DataSchema;
use common_expression::FieldIndex;
use common_storage::common_metrics::copy::metrics_inc_copy_read_part_cost_milliseconds;
use common_storage::common_metrics::copy::metrics_inc_copy_read_size_bytes;
use opendal::BlockingOperator;
use opendal::Operator;

use crate::parquet_part::ParquetRowGroupPart;
use crate::ParquetPart;

pub trait BlockIterator: Iterator<Item = Result<DataBlock>> + Send {
    /// checking has_next() after next can avoid processor from entering SYNC for nothing.
    fn has_next(&self) -> bool;
}

pub struct OneBlock(pub Option<DataBlock>);

impl Iterator for OneBlock {
    type Item = Result<DataBlock>;

    fn next(&mut self) -> Option<Self::Item> {
        Ok(self.0.take()).transpose()
    }
}

impl BlockIterator for OneBlock {
    fn has_next(&self) -> bool {
        self.0.is_some()
    }
}

pub trait SeekRead: std::io::Read + std::io::Seek {}

impl<T> SeekRead for T where T: std::io::Read + std::io::Seek {}

pub struct DataReader {
    bytes: usize,
    inner: Box<dyn SeekRead + Sync + Send>,
}

impl DataReader {
    pub fn new(inner: Box<dyn SeekRead + Sync + Send>, bytes: usize) -> Self {
        Self { inner, bytes }
    }

    pub fn read_all(&mut self) -> Result<Vec<u8>> {
        let mut data = Vec::with_capacity(self.bytes);
        // `DataReader` might be reused if there is nested-type data, example:
        // Table: t Tuple(a int, b int);
        // Query: select t from table where t:a > 1;
        // The query will create two readers: Reader(a), Reader(b).
        // Prewhere phase: Reader(a).read_all();
        // Remain phase: Reader(a).read_all(); Reader(b).read_all();
        // If we don't seek to the start of the reader, the second read_all will read nothing.
        self.inner.rewind()?;
        // TODO(1): don't seek and read, but reuse the data (reduce IO).
        // TODO(2): for nested types, merge sub columns into one column (reduce deserialization).
        self.inner.read_to_end(&mut data)?;
        Ok(data)
    }
}

pub type IndexedChunk = (FieldIndex, Vec<u8>);
pub type IndexedReaders = HashMap<FieldIndex, DataReader>;

pub enum ParquetPartData {
    RowGroup(IndexedReaders),
    SmallFiles(Vec<Vec<u8>>),
}

/// Part -> IndexedReaders -> IndexedChunk -> DataBlock
#[async_trait::async_trait]
pub trait ParquetReader: Sync + Send {
    fn output_schema(&self) -> &DataSchema;
    fn columns_to_read(&self) -> &HashSet<FieldIndex>;
    fn operator(&self) -> &Operator;

    fn deserialize(
        &self,
        part: &ParquetRowGroupPart,
        chunks: Vec<(FieldIndex, Vec<u8>)>,
        filter: Option<Bitmap>,
    ) -> Result<DataBlock>;

    fn get_deserializer(
        &self,
        part: &ParquetRowGroupPart,
        chunks: Vec<(FieldIndex, Vec<u8>)>,
        filter: Option<Bitmap>,
    ) -> Result<Box<dyn BlockIterator>> {
        let block = self.deserialize(part, chunks, filter)?;
        Ok(Box::new(OneBlock(Some(block))))
    }

    fn read_from_readers(&self, readers: &mut IndexedReaders) -> Result<Vec<IndexedChunk>> {
        let mut chunks = Vec::with_capacity(self.columns_to_read().len());

        for index in self.columns_to_read() {
            let reader = readers.get_mut(index).unwrap();
            let data = reader.read_all()?;

            chunks.push((*index, data));
        }

        Ok(chunks)
    }
    fn row_group_readers_from_blocking_io(
        &self,
        part: &ParquetRowGroupPart,
        operator: &BlockingOperator,
    ) -> Result<IndexedReaders> {
        let mut readers: HashMap<usize, DataReader> =
            HashMap::with_capacity(self.columns_to_read().len());

        for index in self.columns_to_read() {
            let meta = &part.column_metas[index];
            let reader =
                operator.range_reader(&part.location, meta.offset..meta.offset + meta.length)?;
            metrics_inc_copy_read_size_bytes(meta.length);
            readers.insert(
                *index,
                DataReader::new(Box::new(reader), meta.length as usize),
            );
        }
        Ok(readers)
    }

    fn readers_from_blocking_io(&self, part: PartInfoPtr) -> Result<ParquetPartData> {
        let part = ParquetPart::from_part(&part)?;
        match part {
            ParquetPart::RowGroup(part) => Ok(ParquetPartData::RowGroup(
                self.row_group_readers_from_blocking_io(part, &self.operator().blocking())?,
            )),
            ParquetPart::SmallFiles(part) => {
                let op = self.operator().blocking();
                let mut buffers = Vec::with_capacity(part.files.len());
                for path in &part.files {
                    let buffer = op.read(path.0.as_str())?;
                    buffers.push(buffer);
                }
                metrics_inc_copy_read_size_bytes(part.compressed_size());
                Ok(ParquetPartData::SmallFiles(buffers))
            }
        }
    }

    #[async_backtrace::framed]
    async fn readers_from_non_blocking_io(&self, part: PartInfoPtr) -> Result<ParquetPartData> {
        let part = ParquetPart::from_part(&part)?;
        match part {
            ParquetPart::RowGroup(part) => {
                let mut join_handlers = Vec::with_capacity(self.columns_to_read().len());
                let path = Arc::new(part.location.to_string());

                for index in self.columns_to_read().iter() {
                    let op = self.operator().clone();
                    let path = path.clone();

                    let meta = &part.column_metas[index];
                    let (offset, length) = (meta.offset, meta.length);

                    join_handlers.push(async move {
                        // Perf.
                        {
                            metrics_inc_copy_read_size_bytes(length);
                        }

                        let data = op.range_read(&path, offset..offset + length).await?;
                        Ok::<_, ErrorCode>((
                            *index,
                            DataReader::new(Box::new(std::io::Cursor::new(data)), length as usize),
                        ))
                    });
                }

                let start = Instant::now();
                let readers = futures::future::try_join_all(join_handlers).await?;

                // Perf.
                {
                    metrics_inc_copy_read_part_cost_milliseconds(start.elapsed().as_millis() as u64);
                }

                let readers = readers.into_iter().collect::<IndexedReaders>();
                Ok(ParquetPartData::RowGroup(readers))
            }
            ParquetPart::SmallFiles(part) => {
                let mut join_handlers = Vec::with_capacity(part.files.len());
                for (path, _) in part.files.iter() {
                    let op = self.operator().clone();
                    join_handlers.push(async move { op.read(path.as_str()).await });
                }

                let start = Instant::now();
                let buffers = futures::future::try_join_all(join_handlers).await?;

                // Perf.
                {
                    metrics_inc_copy_read_size_bytes(part.compressed_size());
                    metrics_inc_copy_read_part_cost_milliseconds(start.elapsed().as_millis() as u64);
                }

                Ok(ParquetPartData::SmallFiles(buffers))
            }
        }
    }
}
