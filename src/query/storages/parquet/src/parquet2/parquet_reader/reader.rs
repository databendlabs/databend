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

use databend_common_arrow::arrow::bitmap::Bitmap;
use databend_common_arrow::arrow::datatypes::Schema as ArrowSchema;
use databend_common_arrow::arrow::io::parquet::read::RowGroupDeserializer;
use databend_common_arrow::parquet::metadata::ColumnDescriptor;
use databend_common_arrow::parquet::metadata::SchemaDescriptor;
use databend_common_catalog::plan::PartInfoPtr;
use databend_common_catalog::plan::Projection;
use databend_common_catalog::table_context::TableContext;
use databend_common_exception::Result;
use databend_common_expression::DataBlock;
use databend_common_expression::DataSchema;
use databend_common_expression::DataSchemaRef;
use databend_common_expression::FieldIndex;
use databend_common_metrics::storage::*;
use databend_common_storage::ColumnNodes;
use opendal::Operator;

use super::data::OneBlock;
use super::BlockIterator;
use super::IndexedChunk;
use super::IndexedChunks;
use super::Parquet2PartData;
use crate::parquet2::parquet_reader::deserialize::try_next_block;
use crate::parquet2::parquet_table::arrow_to_table_schema;
use crate::parquet2::projection::project_parquet_schema;
use crate::parquet2::Parquet2RowGroupPart;
use crate::ParquetPart;
use crate::ReadSettings;

/// The reader to parquet files with a projected schema.
///
/// **ALERT**: dictionary type is not supported yet.
/// If there are dictionary pages in the parquet file, the reading process may fail.
#[derive(Clone)]
pub struct Parquet2Reader {
    operator: Operator,
    /// The indices of columns need to read by this reader.
    ///
    /// Use [`HashSet`] to avoid duplicate indices.
    /// Duplicate indices will exist when there are nested types or
    /// select a same field multiple times.
    ///
    /// For example:
    ///
    /// ```sql
    /// select a, a.b, a.c from t;
    /// select a, b, a from t;
    /// ```
    columns_to_read: HashSet<FieldIndex>,
    /// The schema of the [`common_expression::DataBlock`] this reader produces.
    ///
    /// ```
    /// output_schema = DataSchema::from(projected_arrow_schema)
    /// ```
    pub output_schema: DataSchemaRef,
    /// The actual schema used to read parquet.
    ///
    /// The reason of using [`ArrowSchema`] to read parquet is that
    /// There are some types that Databend not support such as Timestamp of nanoseconds.
    /// Such types will be convert to supported types after deserialization.
    pub(crate) projected_arrow_schema: ArrowSchema,
    /// [`ColumnNodes`] corresponding to the `projected_arrow_schema`.
    pub(crate) projected_column_nodes: ColumnNodes,
    /// [`ColumnDescriptor`]s corresponding to the `projected_arrow_schema`.
    pub(crate) projected_column_descriptors: HashMap<FieldIndex, ColumnDescriptor>,
}

/// Project the schema and get the needed column leaves.
/// Part -> IndexedReaders -> IndexedChunk -> DataBlock
impl Parquet2Reader {
    pub fn create(
        operator: Operator,
        schema: &ArrowSchema,
        schema_descr: &SchemaDescriptor,
        projection: Projection,
    ) -> Result<Arc<Parquet2Reader>> {
        let (
            projected_arrow_schema,
            projected_column_nodes,
            projected_column_descriptors,
            columns_to_read,
        ) = project_parquet_schema(schema, schema_descr, &projection)?;

        let t_schema = arrow_to_table_schema(projected_arrow_schema.clone());
        let output_schema = DataSchema::from(&t_schema);

        Ok(Arc::new(Parquet2Reader {
            operator,
            columns_to_read,
            output_schema: Arc::new(output_schema),
            projected_arrow_schema,
            projected_column_nodes,
            projected_column_descriptors,
        }))
    }

    pub fn output_schema(&self) -> &DataSchema {
        &self.output_schema
    }

    pub fn columns_to_read(&self) -> &HashSet<FieldIndex> {
        &self.columns_to_read
    }

    pub fn operator(&self) -> &Operator {
        &self.operator
    }

    pub fn deserialize(
        &self,
        part: &Parquet2RowGroupPart,
        chunks: Vec<(FieldIndex, Vec<u8>)>,
        filter: Option<Bitmap>,
    ) -> Result<DataBlock> {
        if chunks.is_empty() {
            return Ok(DataBlock::new(vec![], part.num_rows));
        }

        let mut chunk_map: HashMap<FieldIndex, Vec<u8>> = chunks.into_iter().collect();
        let mut columns_array_iter = Vec::with_capacity(self.projected_arrow_schema.fields.len());
        let mut nested_columns_array_iter =
            Vec::with_capacity(self.projected_arrow_schema.fields.len());
        let mut normal_fields = Vec::with_capacity(self.projected_arrow_schema.fields.len());
        let mut nested_fields = Vec::with_capacity(self.projected_arrow_schema.fields.len());

        let column_nodes = &self.projected_column_nodes.column_nodes;
        let mut cnt_map = Self::build_projection_count_map(column_nodes);

        for (idx, column_node) in column_nodes.iter().enumerate() {
            let indices = &column_node.leaf_indices;
            let mut metas = Vec::with_capacity(indices.len());
            let mut chunks = Vec::with_capacity(indices.len());
            for index in indices {
                // in `read_parquet` function, there is no `TableSchema`, so index treated as column id
                let column_meta = &part.column_metas[index];
                let cnt = cnt_map.get_mut(index).unwrap();
                *cnt -= 1;
                let column_chunk = if cnt > &mut 0 {
                    chunk_map.get(index).unwrap().clone()
                } else {
                    chunk_map.remove(index).unwrap()
                };
                let descriptor = &self.projected_column_descriptors[index];
                metas.push((column_meta, descriptor));
                chunks.push(column_chunk);
            }
            if let Some(ref bitmap) = filter {
                // Filter push down for nested type is not supported now.
                // If the array is nested type, do not push down filter to it.
                if chunks.len() > 1 {
                    nested_columns_array_iter.push(Self::to_array_iter(
                        metas,
                        chunks,
                        part.num_rows,
                        column_node.field.clone(),
                        column_node.init.clone(),
                    )?);
                    nested_fields.push(self.output_schema.field(idx).clone());
                } else {
                    columns_array_iter.push(Self::to_array_iter_with_filter(
                        metas,
                        chunks,
                        part.num_rows,
                        column_node.field.clone(),
                        column_node.init.clone(),
                        bitmap.clone(),
                    )?);
                    normal_fields.push(self.output_schema.field(idx).clone());
                }
            } else {
                columns_array_iter.push(Self::to_array_iter(
                    metas,
                    chunks,
                    part.num_rows,
                    column_node.field.clone(),
                    column_node.init.clone(),
                )?)
            }
        }

        if nested_fields.is_empty() {
            let mut deserializer =
                RowGroupDeserializer::new(columns_array_iter, part.num_rows, None);
            return self.full_deserialize(&mut deserializer);
        }

        let bitmap = filter.unwrap();
        let normal_block = try_next_block(
            &DataSchema::new(normal_fields.clone()),
            &mut RowGroupDeserializer::new(columns_array_iter, part.num_rows, None),
        )?;
        let nested_block = try_next_block(
            &DataSchema::new(nested_fields.clone()),
            &mut RowGroupDeserializer::new(nested_columns_array_iter, part.num_rows, None),
        )?;
        // need to filter nested block
        let nested_block = DataBlock::filter_with_bitmap(nested_block, &bitmap)?;

        // Construct the final output
        let mut final_columns = Vec::with_capacity(self.output_schema.fields().len());
        final_columns.extend_from_slice(normal_block.columns());
        final_columns.extend_from_slice(nested_block.columns());
        let final_block = DataBlock::new(final_columns, bitmap.len() - bitmap.unset_bits());

        normal_fields.extend_from_slice(&nested_fields);
        let src_schema = DataSchema::new(normal_fields);
        final_block.resort(&src_schema, &self.output_schema)
    }

    pub fn get_deserializer(
        &self,
        part: &Parquet2RowGroupPart,
        chunks: Vec<(FieldIndex, Vec<u8>)>,
        filter: Option<Bitmap>,
    ) -> Result<Box<dyn BlockIterator>> {
        let block = self.deserialize(part, chunks, filter)?;
        Ok(Box::new(OneBlock(Some(block))))
    }

    pub fn read_from_merge_io(
        &self,
        column_chunks: &mut IndexedChunks,
    ) -> Result<Vec<IndexedChunk>> {
        let mut chunks = Vec::with_capacity(self.columns_to_read().len());

        for index in self.columns_to_read() {
            let bytes = column_chunks.get_mut(index).unwrap();
            let data = bytes.to_vec();

            chunks.push((*index, data));
        }

        Ok(chunks)
    }

    pub fn readers_from_blocking_io(
        &self,
        ctx: Arc<dyn TableContext>,
        part: PartInfoPtr,
    ) -> Result<Parquet2PartData> {
        let part = ParquetPart::from_part(&part)?;
        match part {
            ParquetPart::Parquet2RowGroup(part) => Ok(Parquet2PartData::RowGroup(
                self.sync_read_columns_data_by_merge_io(
                    &ReadSettings::from_ctx(&ctx)?,
                    part,
                    &self.operator().blocking(),
                )?
                .column_buffers()?,
            )),
            ParquetPart::ParquetFiles(part) => {
                let op = self.operator().blocking();
                let mut buffers = Vec::with_capacity(part.files.len());
                for path in &part.files {
                    let buffer = op.read(path.0.as_str())?;
                    buffers.push(buffer);
                }
                metrics_inc_copy_read_size_bytes(part.compressed_size());
                Ok(Parquet2PartData::SmallFiles(buffers))
            }
            ParquetPart::ParquetRSRowGroup(_) => unreachable!(),
        }
    }

    #[async_backtrace::framed]
    pub async fn readers_from_non_blocking_io(
        &self,
        ctx: Arc<dyn TableContext>,
        part: &ParquetPart,
    ) -> Result<Parquet2PartData> {
        match part {
            ParquetPart::Parquet2RowGroup(part) => {
                let chunks = self
                    .read_columns_data_by_merge_io(
                        &ReadSettings::from_ctx(&ctx)?,
                        part,
                        self.operator(),
                    )
                    .await?
                    .column_buffers()?;
                Ok(Parquet2PartData::RowGroup(chunks))
            }
            ParquetPart::ParquetFiles(part) => {
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

                Ok(Parquet2PartData::SmallFiles(buffers))
            }
            ParquetPart::ParquetRSRowGroup(_) => unreachable!(),
        }
    }
}
