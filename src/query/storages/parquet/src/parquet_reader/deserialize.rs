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

use std::collections::hash_map::Entry;
use std::collections::HashMap;
use std::sync::Arc;

use common_arrow::arrow::datatypes::Field;
use common_arrow::arrow::io::parquet::read::column_iter_to_arrays;
use common_arrow::arrow::io::parquet::read::ArrayIter;
use common_arrow::arrow::io::parquet::read::RowGroupDeserializer;
use common_arrow::parquet::metadata::ColumnDescriptor;
use common_arrow::parquet::read::BasicDecompressor;
use common_arrow::parquet::read::PageMetaData;
use common_arrow::parquet::read::PageReader;
use common_catalog::plan::PartInfoPtr;
use common_datablocks::DataBlock;
use common_exception::ErrorCode;
use common_exception::Result;
use common_storage::ColumnLeaf;

use crate::ParquetColumnMeta;
use crate::ParquetPartInfo;
use crate::ParquetReader;

impl ParquetReader {
    fn to_array_iter(
        metas: Vec<&ParquetColumnMeta>,
        chunks: Vec<Vec<u8>>,
        rows: usize,
        column_descriptors: Vec<&ColumnDescriptor>,
        field: Field,
    ) -> Result<ArrayIter<'static>> {
        let columns = metas
            .iter()
            .zip(chunks.into_iter().zip(column_descriptors.iter()))
            .map(|(meta, (chunk, column_descriptor))| {
                let page_meta_data = PageMetaData {
                    column_start: meta.offset,
                    num_values: meta.num_values as i64,
                    compression: meta.compression.into(),
                    descriptor: column_descriptor.descriptor.clone(),
                };
                let pages = PageReader::new_with_page_meta(
                    std::io::Cursor::new(chunk),
                    page_meta_data,
                    Arc::new(|_, _| true),
                    vec![],
                    usize::MAX,
                );
                Ok(BasicDecompressor::new(pages, vec![]))
            })
            .collect::<Result<Vec<_>>>()?;

        let types = column_descriptors
            .iter()
            .map(|column_descriptor| &column_descriptor.descriptor.primitive_type)
            .collect::<Vec<_>>();

        Ok(column_iter_to_arrays(
            columns,
            types,
            field,
            Some(rows),
            rows,
        )?)
    }

    pub fn deserialize(
        &self,
        part: PartInfoPtr,
        chunks: Vec<(usize, Vec<u8>)>,
    ) -> Result<DataBlock> {
        let part = ParquetPartInfo::from_part(&part)?;
        let mut chunk_map: HashMap<usize, Vec<u8>> = chunks.into_iter().collect();
        let mut columns_array_iter = Vec::with_capacity(self.projection.len());

        let num_rows = part.nums_rows;
        let columns = self.projection.project_column_leaves(&self.column_leaves)?;
        let mut cnt_map = Self::build_projection_count_map(&columns);
        for column in &columns {
            let field = column.field.clone();
            let indices = &column.leaf_ids;
            let mut column_metas = Vec::with_capacity(indices.len());
            let mut column_chunks = Vec::with_capacity(indices.len());
            let mut column_descriptors = Vec::with_capacity(indices.len());
            for index in indices {
                let column_meta = &part.columns_meta[index];
                let cnt = cnt_map.get_mut(index).unwrap();
                *cnt -= 1;
                let column_chunk = if cnt > &mut 0 {
                    chunk_map.get(index).unwrap().clone()
                } else {
                    chunk_map.remove(index).unwrap()
                };
                let column_descriptor = &self.parquet_schema_descriptor.columns()[*index];
                column_metas.push(column_meta);
                column_chunks.push(column_chunk);
                column_descriptors.push(column_descriptor);
            }
            columns_array_iter.push(Self::to_array_iter(
                column_metas,
                column_chunks,
                num_rows,
                column_descriptors,
                field,
            )?);
        }

        let mut deserializer = RowGroupDeserializer::new(columns_array_iter, num_rows, None);

        self.try_next_block(&mut deserializer)
    }

    fn try_next_block(&self, deserializer: &mut RowGroupDeserializer) -> Result<DataBlock> {
        match deserializer.next() {
            None => Err(ErrorCode::Internal(
                "deserializer from row group: fail to get a chunk",
            )),
            Some(Err(cause)) => Err(ErrorCode::from(cause)),
            Some(Ok(chunk)) => DataBlock::from_chunk(&self.projected_schema, &chunk),
        }
    }

    // Build a map to record the count number of each leaf_id
    fn build_projection_count_map(columns: &Vec<&ColumnLeaf>) -> HashMap<usize, usize> {
        let mut cnt_map = HashMap::with_capacity(columns.len());
        for column in columns {
            for index in &column.leaf_ids {
                if let Entry::Vacant(e) = cnt_map.entry(*index) {
                    e.insert(1);
                } else {
                    let cnt = cnt_map.get_mut(index).unwrap();
                    *cnt += 1;
                }
            }
        }
        cnt_map
    }
}
