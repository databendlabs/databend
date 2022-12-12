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
use common_datablocks::DataBlock;
use common_exception::ErrorCode;
use common_exception::Result;
use common_storage::ColumnLeaf;

use super::filter::FilterState;
use crate::parquet_part::ColumnMeta;
use crate::parquet_part::ParquetRowGroupPart;
use crate::ParquetReader;

impl ParquetReader {
    pub fn deserialize(
        &self,
        part: &ParquetRowGroupPart,
        chunks: Vec<(usize, Vec<u8>)>,
        filter: Option<Bitmap>,
    ) -> Result<DataBlock> {
        let mut chunk_map: HashMap<usize, Vec<u8>> = chunks.into_iter().collect();
        let mut columns_array_iter = Vec::with_capacity(self.projected_arrow_schema.fields.len());

        let column_leaves = &self.projected_column_leaves.column_leaves;
        let mut cnt_map = Self::build_projection_count_map(column_leaves);

        for column_leaf in column_leaves {
            let indices = &column_leaf.leaf_ids;
            let mut metas = Vec::with_capacity(indices.len());
            let mut chunks = Vec::with_capacity(indices.len());
            for index in indices {
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
            let array_iter = if let Some(ref bitmap) = filter {
                Self::to_array_iter_with_filter(
                    metas,
                    chunks,
                    part.num_rows,
                    column_leaf.field.clone(),
                    bitmap.clone(),
                )?
            } else {
                Self::to_array_iter(metas, chunks, part.num_rows, column_leaf.field.clone())?
            };
            columns_array_iter.push(array_iter);
        }

        let mut deserializer = RowGroupDeserializer::new(columns_array_iter, part.num_rows, None);

        self.try_next_block(&mut deserializer)
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
                        num_values: meta.length as i64,
                        compression: meta.compression.into(),
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
                        num_values: meta.length as i64,
                        compression: meta.compression.into(),
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

    fn try_next_block(&self, deserializer: &mut RowGroupDeserializer) -> Result<DataBlock> {
        match deserializer.next() {
            None => Err(ErrorCode::Internal(
                "deserializer from row group: fail to get a chunk",
            )),
            Some(Err(cause)) => Err(ErrorCode::from(cause)),
            Some(Ok(chunk)) => DataBlock::from_chunk(&self.output_schema, &chunk),
        }
    }

    // Build a map to record the count number of each leaf_id
    fn build_projection_count_map(columns: &[ColumnLeaf]) -> HashMap<usize, usize> {
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
