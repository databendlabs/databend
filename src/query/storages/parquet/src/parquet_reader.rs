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

use common_arrow::arrow::bitmap::Bitmap;
use common_catalog::plan::PartInfoPtr;
use common_exception::Result;
use common_expression::DataBlock;
use common_expression::DataSchema;
use common_expression::FieldIndex;
use opendal::BlockingOperator;

use crate::parquet_part::ParquetRowGroupPart;

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

    fn row_group_readers_from_blocking_io(
        &self,
        part: &ParquetRowGroupPart,
        operator: &BlockingOperator,
    ) -> Result<IndexedReaders>;

    fn read_from_readers(&self, readers: &mut IndexedReaders) -> Result<Vec<IndexedChunk>>;

    fn readers_from_blocking_io(&self, part: PartInfoPtr) -> Result<ParquetPartData>;

    async fn readers_from_non_blocking_io(&self, part: PartInfoPtr) -> Result<ParquetPartData>;

    fn deserialize(
        &self,
        part: &ParquetRowGroupPart,
        chunks: Vec<(FieldIndex, Vec<u8>)>,
        filter: Option<Bitmap>,
    ) -> Result<DataBlock>;
}
