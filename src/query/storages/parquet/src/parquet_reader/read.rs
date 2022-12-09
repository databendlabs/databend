// Copyright 2022 Datafuse Labs.
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

use common_exception::Result;
use opendal::Object;

use crate::parquet_part::ParquetRowGroupPart;
use crate::ParquetReader;

pub type IndexedChunk = (usize, Vec<u8>);

impl ParquetReader {
    /// Read columns data of one row group.
    pub fn sync_read_columns_data(&self, part: &ParquetRowGroupPart) -> Result<Vec<IndexedChunk>> {
        let mut chunks = Vec::with_capacity(self.columns_to_read.len());

        for index in &self.columns_to_read {
            let meta = &part.column_metas[index];
            let op = self.operator.clone();
            let chunk =
                Self::sync_read_column(op.object(&part.location), meta.offset, meta.length)?;
            chunks.push((*index, chunk));
        }

        Ok(chunks)
    }

    #[inline]
    pub fn sync_read_column(o: Object, offset: u64, length: u64) -> Result<Vec<u8>> {
        Ok(o.blocking_range_read(offset..offset + length)?)
    }
}
