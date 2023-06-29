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

use std::collections::hash_map::Entry;
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::Mutex;

use common_arrow::arrow::bitmap::Bitmap;
use common_arrow::arrow::datatypes::Field;
use common_arrow::arrow::io::parquet::read::column_iter_to_arrays;
use common_arrow::arrow::io::parquet::read::ArrayIter;
use common_arrow::arrow::io::parquet::read::RowGroupDeserializer;
use common_arrow::parquet::metadata::ColumnDescriptor;
use common_arrow::parquet::page::CompressedPage;
use common_arrow::parquet::read::BasicDecompressor;
use common_arrow::parquet::read::PageMetaData;
use common_arrow::parquet::read::PageReader;
use common_base::runtime::GLOBAL_MEM_STAT;
use common_exception::ErrorCode;
use common_exception::Result;
use common_expression::DataBlock;
use common_expression::DataSchema;
use common_expression::FieldIndex;
use common_storage::ColumnNode;

use super::filter::FilterState;
use crate::parquet_part::ColumnMeta;
use crate::parquet_part::ParquetRowGroupPart;
use crate::parquet_reader::ParquetReader;

impl ParquetReader {
    pub fn deserialize(
        &self,
        part: &ParquetRowGroupPart,
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
                    )?);
                    nested_fields.push(self.output_schema.field(idx).clone());
                } else {
                    columns_array_iter.push(Self::to_array_iter_with_filter(
                        metas,
                        chunks,
                        part.num_rows,
                        column_node.field.clone(),
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

    /// The number of columns can be greater than 1 because the it may be a nested type.
    /// Combine multiple columns into one arrow array.
    fn to_array_iter(
        metas: Vec<(&ColumnMeta, &ColumnDescriptor)>,
        chunks: Vec<Vec<u8>>,
        rows: usize,
        field: Field,
    ) -> Result<ArrayIter<'static>> {
        let (columns, types) = metas
            .iter()
            .zip(chunks.into_iter())
            .map(|(&(meta, descriptor), chunk)| {
                let pages = PageReader::new_with_page_meta(
                    std::io::Cursor::new(chunk),
                    PageMetaData {
                        column_start: meta.offset,
                        num_values: meta.num_values,
                        compression: meta.compression,
                        descriptor: descriptor.descriptor.clone(),
                    },
                    Arc::new(|_, _| true),
                    vec![],
                    usize::MAX,
                );
                (
                    BasicDecompressor::new(pages, vec![]),
                    &descriptor.descriptor.primitive_type,
                )
            })
            .unzip();

        Ok(column_iter_to_arrays(
            columns,
            types,
            field,
            Some(rows),
            rows,
        )?)
    }

    /// Almost the same as `to_array_iter`, but with a filter.
    fn to_array_iter_with_filter(
        metas: Vec<(&ColumnMeta, &ColumnDescriptor)>,
        chunks: Vec<Vec<u8>>,
        rows: usize,
        field: Field,
        filter: Bitmap,
    ) -> Result<ArrayIter<'static>> {
        let (columns, types) = metas
            .iter()
            .zip(chunks.into_iter())
            .map(|(&(meta, descriptor), chunk)| {
                let filter_state = Arc::new(Mutex::new(FilterState::new(filter.clone())));
                let iter_filter_state = filter_state.clone();

                let pages = PageReader::new_with_page_meta(
                    std::io::Cursor::new(chunk),
                    PageMetaData {
                        column_start: meta.offset,
                        num_values: meta.num_values,
                        compression: meta.compression,
                        descriptor: descriptor.descriptor.clone(),
                    },
                    Arc::new(move |_, header| {
                        // If the bitmap for current page is all unset, skip it.
                        let mut state = filter_state.lock().unwrap();
                        let num_rows = header.num_values();
                        let all_unset = state.range_all_unset(num_rows);
                        if all_unset {
                            // skip this page.
                            state.advance(num_rows);
                        }
                        !all_unset
                    }),
                    vec![],
                    usize::MAX,
                )
                .map(move |page| {
                    page.map(|page| match page {
                        CompressedPage::Data(mut page) => {
                            let num_rows = page.num_values();
                            let mut state = iter_filter_state.lock().unwrap();
                            if state.range_all_unset(num_rows) {
                                page.select_rows(vec![]);
                            } else if !state.range_all_set(num_rows) {
                                page.select_rows(state.convert_to_intervals(num_rows));
                            };
                            state.advance(num_rows);
                            CompressedPage::Data(page)
                        }
                        CompressedPage::Dict(_) => page, // do nothing
                    })
                });
                (
                    BasicDecompressor::new(pages, vec![]),
                    &descriptor.descriptor.primitive_type,
                )
            })
            .unzip();

        Ok(column_iter_to_arrays(
            columns,
            types,
            field,
            Some(rows - filter.unset_bits()),
            rows,
        )?)
    }

    fn full_deserialize(&self, deserializer: &mut RowGroupDeserializer) -> Result<DataBlock> {
        try_next_block(&self.output_schema, deserializer)
    }

    // Build a map to record the count number of each leaf_id
    fn build_projection_count_map(columns: &[ColumnNode]) -> HashMap<usize, usize> {
        let mut cnt_map = HashMap::with_capacity(columns.len());
        for column in columns {
            for index in &column.leaf_indices {
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

fn try_next_block(
    schema: &DataSchema,
    deserializer: &mut RowGroupDeserializer,
) -> Result<DataBlock> {
    match deserializer.next() {
        None => Err(ErrorCode::Internal(
            "deserializer from row group: fail to get a chunk",
        )),
        Some(Err(cause)) => Err(ErrorCode::from(cause)),
        Some(Ok(chunk)) => {
            let (mem, peak_mem) = if tracing::enabled!(tracing::Level::DEBUG) {
                (
                    GLOBAL_MEM_STAT.get_memory_usage(),
                    GLOBAL_MEM_STAT.get_peak_memory_usage(),
                )
            } else {
                (0, 0)
            };

            let block = DataBlock::from_arrow_chunk(&chunk, schema);
            if tracing::enabled!(tracing::Level::DEBUG) {
                let mem_new = GLOBAL_MEM_STAT.get_memory_usage();
                let peak_mem_new = GLOBAL_MEM_STAT.get_memory_usage();
                tracing::debug!(
                    "load new arrow chunk, mem: {mem}, peak_mem: {peak_mem}; to block, mem {mem_new},  peak_mem: {peak_mem_new}"
                );
            };
            block
        }
    }
}
