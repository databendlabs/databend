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

use common_exception::Result;
use common_expression::DataBlock;
use common_expression::TopKSorter;
use parquet::arrow::arrow_reader::ParquetRecordBatchReader;
use parquet::arrow::arrow_reader::RowSelection;

use crate::parquet_rs::parquet_reader::row_group::InMemoryRowGroup;
use crate::parquet_rs::parquet_reader::topk::ParquetTopK;
use crate::parquet_rs::parquet_reader::utils::bitmap_to_boolean_array;
use crate::parquet_rs::parquet_reader::utils::transform_record_batch;

pub fn evaluate_topk(
    row_group: &InMemoryRowGroup,
    topk: &ParquetTopK,
    selection: &mut Option<RowSelection>,
    num_rows: usize,
    sorter: &mut TopKSorter,
) -> Result<Option<DataBlock>> {
    let mut reader = ParquetRecordBatchReader::try_new_with_row_groups(
        topk.field_levels(),
        row_group,
        num_rows,
        selection.clone(),
    )?;
    let batch = reader.next().transpose()?.unwrap();
    debug_assert!(reader.next().is_none());
    // Topk column **must** not be in a nested column.
    let block = transform_record_batch(&batch, &None)?;
    debug_assert_eq!(block.num_columns(), 1);
    let topk_col = block.columns()[0].value.as_column().unwrap();
    let filter = topk.evaluate_column(topk_col, sorter);
    if filter.unset_bits() == num_rows {
        // All rows are filtered out.
        return Ok(None);
    }
    let block = block.filter_with_bitmap(&filter)?;
    let filter = bitmap_to_boolean_array(filter);
    let sel = RowSelection::from_filters(&[filter]);
    // Update row selection.
    match selection.as_mut() {
        Some(selection) => {
            *selection = selection.and_then(&sel);
        }
        None => {
            *selection = Some(sel);
        }
    }
    Ok(Some(block))
}
