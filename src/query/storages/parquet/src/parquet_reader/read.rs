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

use common_arrow::parquet::metadata::RowGroupMetaData;
use common_exception::Result;
use opendal::Object;

use crate::ParquetReader;

pub type IndexedChunk = (usize, Vec<u8>);

impl ParquetReader {
    /// Read columns data of one row group.
    pub fn sync_read_columns_data(
        &self,
        location: &str,
        rg: &RowGroupMetaData,
    ) -> Result<Vec<IndexedChunk>> {
        let columns = self.get_column_metas(rg);

        let mut chunks = Vec::with_capacity(columns.len());

        for (index, col) in columns {
            let (offset, length) = col.byte_range();
            let op = self.operator.clone();
            let chunk = Self::sync_read_column(op.object(location), offset, length)?;
            chunks.push((index, chunk));
        }

        Ok(chunks)
    }

    #[inline]
    pub fn sync_read_column(o: Object, offset: u64, length: u64) -> Result<Vec<u8>> {
        Ok(o.blocking_range_read(offset..offset + length)?)
    }
}
