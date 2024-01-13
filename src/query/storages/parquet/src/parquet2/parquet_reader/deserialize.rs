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

use databend_common_arrow::arrow::bitmap::Bitmap;
use databend_common_arrow::arrow::datatypes::Field;
use databend_common_arrow::arrow::io::parquet::read::column_iter_to_arrays;
use databend_common_arrow::arrow::io::parquet::read::nested_column_iter_to_arrays;
use databend_common_arrow::arrow::io::parquet::read::ArrayIter;
use databend_common_arrow::arrow::io::parquet::read::InitNested;
use databend_common_arrow::arrow::io::parquet::read::RowGroupDeserializer;
use databend_common_arrow::parquet::metadata::ColumnDescriptor;
use databend_common_arrow::parquet::page::CompressedPage;
use databend_common_arrow::parquet::read::BasicDecompressor;
use databend_common_arrow::parquet::read::PageMetaData;
use databend_common_arrow::parquet::read::PageReader;
use databend_common_base::runtime::GLOBAL_MEM_STAT;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::DataBlock;
use databend_common_expression::DataSchema;
use databend_common_storage::ColumnNode;
use log::debug;

use super::filter::FilterState;
use crate::parquet2::parquet_reader::Parquet2Reader;
use crate::parquet2::partition::ColumnMeta;

impl Parquet2Reader {
    /// The number of columns can be greater than 1 because the it may be a nested type.
    /// Combine multiple columns into one arrow array.
    pub(crate) fn to_array_iter(
        metas: Vec<(&ColumnMeta, &ColumnDescriptor)>,
        chunks: Vec<Vec<u8>>,
        rows: usize,
        field: Field,
        init: Vec<InitNested>,
    ) -> Result<ArrayIter<'static>> {
        let (columns, types) = metas
            .iter()
            .zip(chunks)
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

        let array_iter = if init.is_empty() {
            column_iter_to_arrays(columns, types, field, Some(rows), rows)?
        } else {
            nested_column_iter_to_arrays(columns, types, field, init, Some(rows), rows)?
        };
        Ok(array_iter)
    }

    /// Almost the same as `to_array_iter`, but with a filter.
    pub(crate) fn to_array_iter_with_filter(
        metas: Vec<(&ColumnMeta, &ColumnDescriptor)>,
        chunks: Vec<Vec<u8>>,
        rows: usize,
        field: Field,
        init: Vec<InitNested>,
        filter: Bitmap,
    ) -> Result<ArrayIter<'static>> {
        let (columns, types) = metas
            .iter()
            .zip(chunks)
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

        let array_iter = if init.is_empty() {
            column_iter_to_arrays(
                columns,
                types,
                field,
                Some(rows - filter.unset_bits()),
                rows,
            )?
        } else {
            nested_column_iter_to_arrays(
                columns,
                types,
                field,
                init,
                Some(rows - filter.unset_bits()),
                rows,
            )?
        };
        Ok(array_iter)
    }

    pub(crate) fn full_deserialize(
        &self,
        deserializer: &mut RowGroupDeserializer,
    ) -> Result<DataBlock> {
        try_next_block(&self.output_schema, deserializer)
    }

    // Build a map to record the count number of each leaf_id
    pub(crate) fn build_projection_count_map(columns: &[ColumnNode]) -> HashMap<usize, usize> {
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

pub(crate) fn try_next_block(
    schema: &DataSchema,
    deserializer: &mut RowGroupDeserializer,
) -> Result<DataBlock> {
    match deserializer.next() {
        None => Err(ErrorCode::Internal(
            "deserializer from row group: fail to get a chunk",
        )),
        Some(Err(cause)) => Err(ErrorCode::from(cause)),
        Some(Ok(chunk)) => {
            debug!(mem = GLOBAL_MEM_STAT.get_memory_usage(), peak_mem = GLOBAL_MEM_STAT.get_peak_memory_usage(); "before load arrow chunk");
            let block = DataBlock::from_arrow_chunk(&chunk, schema);
            debug!(mem = GLOBAL_MEM_STAT.get_memory_usage(), peak_mem = GLOBAL_MEM_STAT.get_peak_memory_usage(); "after load arrow chunk");

            block
        }
    }
}
