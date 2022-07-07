// Copyright 2021 Datafuse Labs.
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

use common_arrow::arrow::array::growable::make_growable;
use common_arrow::arrow::array::Array;
use common_arrow::arrow::compute::merge_sort::MergeSlice;
use common_arrow::arrow::types::Index;
use common_arrow::ArrayRef;
use common_datavalues::prelude::*;
use common_exception::Result;

use crate::DataBlock;

impl DataBlock {
    pub fn block_take_by_indices<I: Index>(raw: &DataBlock, indices: &[I]) -> Result<DataBlock> {
        if indices.is_empty() {
            return Ok(DataBlock::empty_with_schema(raw.schema().clone()));
        }
        let fields = raw.schema().fields();
        let columns = fields
            .iter()
            .map(|f| {
                let column = raw.try_column_by_name(f.name())?;
                Series::take(column, indices)
            })
            .collect::<Result<Vec<_>>>()?;

        Ok(DataBlock::create(raw.schema().clone(), columns))
    }

    pub fn block_take_by_chunk_indices(
        blocks: &[DataBlock],
        slices: &[ChunkRowIndex],
    ) -> Result<DataBlock> {
        debug_assert!(!blocks.is_empty());

        let schema = blocks[0].schema();
        let mut result_columns = Vec::with_capacity(schema.num_fields());

        for index in 0..schema.num_fields() {
            let mut columns = Vec::with_capacity(blocks.len());

            for block in blocks {
                columns.push(block.column(index).clone());
            }
            let column = Series::take_chunk_indices(&columns, slices)?;
            result_columns.push(column);
        }

        Ok(DataBlock::create(schema.clone(), result_columns))
    }

    pub fn take_columns_by_slices_limit(
        data_type: &DataTypeImpl,
        columns: &[ColumnRef],
        slices: &[MergeSlice],
        limit: Option<usize>,
    ) -> Result<ColumnRef> {
        let arrays: Vec<ArrayRef> = columns.iter().map(|c| c.as_arrow_array()).collect();
        let arrays: Vec<&dyn Array> = arrays.iter().map(|c| c.as_ref()).collect();
        let taked = Self::take_arrays_by_slices_limit(&arrays, slices, limit);

        match data_type.is_nullable() {
            false => Ok(taked.into_column()),
            true => Ok(taked.into_nullable_column()),
        }
    }

    // TODO use ColumnBuilder to static dispath the extend
    pub fn take_arrays_by_slices_limit(
        arrays: &[&dyn Array],
        slices: &[MergeSlice],
        limit: Option<usize>,
    ) -> Box<dyn Array> {
        let slices = slices.iter();
        let len = arrays.iter().map(|array| array.len()).sum();

        let limit = limit.unwrap_or(len);
        let limit = limit.min(len);
        let mut growable = make_growable(arrays, false, limit);

        if limit != len {
            let mut current_len = 0;
            for (index, start, len) in slices {
                if len + current_len >= limit {
                    growable.extend(*index, *start, limit - current_len);
                    break;
                } else {
                    growable.extend(*index, *start, *len);
                    current_len += len;
                }
            }
        } else {
            for (index, start, len) in slices {
                growable.extend(*index, *start, *len);
            }
        }

        growable.as_box()
    }
}
