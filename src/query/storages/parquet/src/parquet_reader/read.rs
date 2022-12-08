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

use std::collections::HashSet;

use common_catalog::plan::PartInfoPtr;
use common_exception::Result;
use common_storage::ColumnLeaf;
use opendal::Object;

use crate::ParquetPartInfo;
use crate::ParquetReader;

impl ParquetReader {
    pub async fn read_columns_data(&self, part: PartInfoPtr) -> Result<Vec<(usize, Vec<u8>)>> {
        let part = ParquetPartInfo::from_part(&part)?;
        let columns = self.projection.project_column_leaves(&self.column_leaves)?;
        let indices = Self::build_projection_indices(&columns);
        let mut join_handlers = Vec::with_capacity(indices.len());

        for index in indices {
            let column_meta = &part.columns_meta[&index];
            join_handlers.push(Self::read_column(
                self.operator.object(&part.location),
                index,
                column_meta.offset,
                column_meta.length,
            ));
        }

        futures::future::try_join_all(join_handlers).await
    }

    pub fn sync_read_columns_data(&self, part: PartInfoPtr) -> Result<Vec<(usize, Vec<u8>)>> {
        let part = ParquetPartInfo::from_part(&part)?;

        let columns = self.projection.project_column_leaves(&self.column_leaves)?;
        let indices = Self::build_projection_indices(&columns);
        let mut results = Vec::with_capacity(indices.len());

        for index in indices {
            let column_meta = &part.columns_meta[&index];

            let op = self.operator.clone();

            let location = part.location.clone();
            let offset = column_meta.offset;
            let length = column_meta.length;

            let result = Self::sync_read_column(op.object(&location), index, offset, length);
            results.push(result?);
        }

        Ok(results)
    }

    pub async fn read_column(
        o: Object,
        index: usize,
        offset: u64,
        length: u64,
    ) -> Result<(usize, Vec<u8>)> {
        let chunk = o.range_read(offset..offset + length).await?;

        Ok((index, chunk))
    }

    pub fn sync_read_column(
        o: Object,
        index: usize,
        offset: u64,
        length: u64,
    ) -> Result<(usize, Vec<u8>)> {
        let chunk = o.blocking_range_read(offset..offset + length)?;
        Ok((index, chunk))
    }

    // Build non duplicate leaf_ids to avoid repeated read column from parquet
    fn build_projection_indices(columns: &Vec<&ColumnLeaf>) -> HashSet<usize> {
        let mut indices = HashSet::with_capacity(columns.len());
        for column in columns {
            for index in &column.leaf_ids {
                indices.insert(*index);
            }
        }
        indices
    }
}
