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
use std::collections::HashSet;
use std::sync::Arc;

use common_arrow::arrow::datatypes::Field;
use common_arrow::arrow::io::parquet::read::column_iter_to_arrays;
use common_arrow::arrow::io::parquet::read::ArrayIter;
use common_arrow::arrow::io::parquet::read::RowGroupDeserializer;
use common_arrow::arrow::io::parquet::write::to_parquet_schema;
use common_arrow::parquet::metadata::ColumnDescriptor;
use common_arrow::parquet::metadata::SchemaDescriptor;
use common_arrow::parquet::read::BasicDecompressor;
use common_arrow::parquet::read::PageMetaData;
use common_arrow::parquet::read::PageReader;
use common_catalog::plan::PartInfoPtr;
use common_catalog::plan::Projection;
use common_datablocks::DataBlock;
use common_datavalues::DataSchemaRef;
use common_exception::ErrorCode;
use common_exception::Result;
use common_storage::ColumnLeaf;
use common_storage::ColumnLeaves;
use opendal::Object;
use opendal::Operator;

use crate::ParquetColumnMeta;
use crate::ParquetPartInfo;

#[derive(Clone)]
pub struct ParquetReader {
    operator: Operator,
    projection: Projection,
    projected_schema: DataSchemaRef,
    column_leaves: ColumnLeaves,
    parquet_schema_descriptor: SchemaDescriptor,
}

impl ParquetReader {
    pub fn create(
        operator: Operator,
        schema: DataSchemaRef,
        projection: Projection,
    ) -> Result<Arc<ParquetReader>> {
        let projected_schema = match projection {
            Projection::Columns(ref indices) => DataSchemaRef::new(schema.project(indices)),
            Projection::InnerColumns(ref path_indices) => {
                DataSchemaRef::new(schema.inner_project(path_indices))
            }
        };

        let arrow_schema = schema.to_arrow();
        let parquet_schema_descriptor = to_parquet_schema(&arrow_schema)?;
        let column_leaves = ColumnLeaves::new_from_schema(&arrow_schema);

        Ok(Arc::new(ParquetReader {
            operator,
            projection,
            projected_schema,
            parquet_schema_descriptor,
            column_leaves,
        }))
    }

    pub fn schema(&self) -> DataSchemaRef {
        self.projected_schema.clone()
    }

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

    pub fn support_blocking_api(&self) -> bool {
        self.operator.metadata().can_blocking()
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

    fn try_next_block(&self, deserializer: &mut RowGroupDeserializer) -> Result<DataBlock> {
        match deserializer.next() {
            None => Err(ErrorCode::Internal(
                "deserializer from row group: fail to get a chunk",
            )),
            Some(Err(cause)) => Err(ErrorCode::from(cause)),
            Some(Ok(chunk)) => DataBlock::from_chunk(&self.projected_schema, &chunk),
        }
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
