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
use std::sync::Arc;

use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::BlockEntry;
use databend_common_expression::Column;
use databend_common_expression::ColumnBuilder;
use databend_common_expression::DataBlock;
use databend_common_expression::types::Bitmap;
use databend_common_expression::types::DataType;
use databend_common_expression::types::NumberDataType;
use databend_common_expression::types::nullable::NullableColumn;
use databend_common_pipeline_transforms::AccumulatingTransform;

use super::meta::RowFetchBatch;
use super::meta::RowFetched;
use super::meta::take_row_fetch_meta;

/// A batch of `Assemble` and the fetched parts received so far, in any order.
struct PendingBatch {
    batch: Arc<RowFetchBatch>,
    fetched: Vec<(u32, DataBlock)>,
}

impl PendingBatch {
    fn is_complete(&self) -> bool {
        self.batch.parts == self.fetched.len()
    }
}

/// Adds the fetched columns to the input rows, batch after batch.
pub struct TransformRowFetchAssemble {
    row_id_col_offset: usize,
    need_wrap_nullable: bool,
    fetched_data_types: Vec<DataType>,
    next_batch_id: u64,
    pending: HashMap<u64, PendingBatch>,
}

impl AccumulatingTransform for TransformRowFetchAssemble {
    const NAME: &'static str = "TransformRowFetchAssemble";

    fn transform(&mut self, data: DataBlock) -> Result<Vec<DataBlock>> {
        let RowFetched { batch, block } = take_row_fetch_meta(data)?;
        let pending = self.pending.entry(batch.id).or_insert(PendingBatch {
            batch,
            fetched: Vec::new(),
        });
        pending.fetched.extend(block);

        let mut output = Vec::new();
        while self
            .pending
            .get(&self.next_batch_id)
            .is_some_and(PendingBatch::is_complete)
        {
            let pending = self.pending.remove(&self.next_batch_id).unwrap();
            self.next_batch_id += 1;
            output.push(self.assemble(pending)?);
        }
        Ok(output)
    }

    fn on_finish(&mut self, output: bool) -> Result<Vec<DataBlock>> {
        if output && !self.pending.is_empty() {
            return Err(ErrorCode::Internal(format!(
                "Row fetch finished with {} incomplete batches",
                self.pending.len()
            )));
        }
        Ok(vec![])
    }
}

impl TransformRowFetchAssemble {
    pub(super) fn create(
        row_id_col_offset: usize,
        need_wrap_nullable: bool,
        fetched_data_types: Vec<DataType>,
    ) -> Self {
        TransformRowFetchAssemble {
            row_id_col_offset,
            need_wrap_nullable,
            fetched_data_types,
            next_batch_id: 0,
            pending: HashMap::new(),
        }
    }

    fn assemble(&self, pending: PendingBatch) -> Result<DataBlock> {
        let batch = pending.batch;
        let num_blocks = batch.inputs.len();
        let mut data = DataBlock::concat(&batch.inputs)?;
        let num_rows = data.num_rows();

        let entry = &data.columns()[self.row_id_col_offset];
        let row_id_validity = match entry.data_type() {
            DataType::Nullable(inner)
                if matches!(inner.as_ref(), DataType::Number(NumberDataType::UInt64)) =>
            {
                let column = entry.to_column();
                let value = column.into_nullable().unwrap();
                Some(value.validity)
            }
            _ => None,
        };
        let non_null_count = batch.indices.len();

        if non_null_count == num_rows {
            let fetched_block = take_fetched_rows(pending.fetched, &batch.indices)?;

            for mut entry in fetched_block.take_columns() {
                if self.need_wrap_nullable {
                    entry = wrap_true_validity(&entry, num_rows);
                }

                data.add_entry(entry);
            }
        } else {
            let validity =
                row_id_validity.expect("Row id validity should be available for null handling");

            if non_null_count == 0 {
                for data_type in &self.fetched_data_types {
                    let nullable_type = data_type.wrap_nullable();
                    let mut builder = ColumnBuilder::with_capacity(&nullable_type, num_rows);
                    for _ in 0..num_rows {
                        builder.push_default();
                    }
                    data.add_entry(builder.build().into());
                }
            } else {
                let fetched_block = take_fetched_rows(pending.fetched, &batch.indices)?;
                for entry in fetched_block.take_columns() {
                    let column = entry.to_column();
                    let mut fetched_iter = column.iter();
                    let nullable_type = entry.data_type().wrap_nullable();
                    let mut builder = ColumnBuilder::with_capacity(&nullable_type, num_rows);
                    for is_valid in validity.iter() {
                        if is_valid {
                            let scalar = fetched_iter.next().unwrap();
                            builder.push(scalar);
                        } else {
                            builder.push_default();
                        }
                    }
                    data.add_entry(builder.build().into());
                }
            }
        }

        log::info!(
            "Row fetch flush: num_rows: {}, num_bytes: {}, input blocks: {} in {} milliseconds.",
            num_rows,
            data.memory_size(),
            num_blocks,
            batch.start.elapsed().as_millis()
        );

        Ok(data)
    }
}

/// The rows of a batch, from the parts they were fetched into.
fn take_fetched_rows(
    mut fetched: Vec<(u32, DataBlock)>,
    indices: &[(u32, u32)],
) -> Result<DataBlock> {
    fetched.sort_unstable_by_key(|(part, _)| *part);
    let blocks = fetched
        .into_iter()
        .map(|(_, block)| block)
        .collect::<Vec<_>>();

    // check if row index is in valid bounds cause we don't ensure rowid is valid
    for (block_idx, row_idx) in indices {
        if *block_idx as usize >= blocks.len()
            || *row_idx as usize >= blocks[*block_idx as usize].num_rows()
        {
            return Err(ErrorCode::Internal(format!(
                "RowID is invalid, block idx {block_idx}, row idx {row_idx}, blocks len {}, block idx len {:?}",
                blocks.len(),
                blocks.get(*block_idx as usize).map(|b| b.num_rows()),
            )));
        }
    }

    Ok(DataBlock::take_blocks(&blocks, indices))
}

fn wrap_true_validity(column: &BlockEntry, num_rows: usize) -> BlockEntry {
    let (value, data_type) = (&column.value(), &column.data_type());
    let col = value.convert_to_full_column(data_type, num_rows);
    if matches!(col, Column::Null { .. }) || col.as_nullable().is_some() {
        column.clone()
    } else {
        NullableColumn::new_column(col, Bitmap::new_trued(num_rows)).into()
    }
}

#[cfg(test)]
mod tests {
    use std::time::Instant;

    use databend_common_expression::FromData;
    use databend_common_expression::types::Int32Type;
    use databend_common_expression::types::UInt64Type;

    use super::*;

    fn int32s(block: &DataBlock, column: usize) -> Vec<i32> {
        block.columns()[column]
            .to_column()
            .into_number()
            .unwrap()
            .into_int32()
            .unwrap()
            .to_vec()
    }

    fn batch(
        id: u64,
        row_ids: Vec<u64>,
        indices: Vec<(u32, u32)>,
        parts: usize,
    ) -> Arc<RowFetchBatch> {
        let inputs = vec![DataBlock::new_from_columns(vec![UInt64Type::from_data(
            row_ids,
        )])];
        Arc::new(RowFetchBatch {
            id,
            inputs,
            indices,
            parts,
            start: Instant::now(),
        })
    }

    fn fetched(batch: &Arc<RowFetchBatch>, part: u32, values: Vec<i32>) -> DataBlock {
        DataBlock::empty_with_meta(Box::new(RowFetched {
            batch: batch.clone(),
            block: Some((
                part,
                DataBlock::new_from_columns(vec![Int32Type::from_data(values)]),
            )),
        }))
    }

    fn assemble() -> TransformRowFetchAssemble {
        TransformRowFetchAssemble::create(0, false, vec![DataType::Number(NumberDataType::Int32)])
    }

    #[test]
    fn test_assemble_restores_batches_and_rows() -> Result<()> {
        let mut assemble = assemble();
        let first = batch(0, vec![1, 2, 3, 4], vec![(1, 1), (0, 0), (1, 1), (1, 0)], 2);
        let second = batch(1, vec![10], vec![(0, 0)], 1);

        // The second batch completes first, it waits for the first one.
        assert!(assemble.transform(fetched(&second, 0, vec![7]))?.is_empty());

        // The parts of the first batch arrive out of order. Row 0 and row 2 of the
        // batch are the same fetched row.
        assert!(
            assemble
                .transform(fetched(&first, 1, vec![20, 30]))?
                .is_empty()
        );
        let output = assemble.transform(fetched(&first, 0, vec![40]))?;

        assert_eq!(output.len(), 2);
        assert_eq!(int32s(&output[0], 1), vec![30, 40, 30, 20]);
        assert_eq!(int32s(&output[1], 1), vec![7]);
        assert!(assemble.on_finish(true)?.is_empty());
        Ok(())
    }

    #[test]
    fn test_assemble_batch_without_row_ids() -> Result<()> {
        let mut assemble = TransformRowFetchAssemble::create(0, false, vec![DataType::Number(
            NumberDataType::Int32,
        )]);
        let null_row_ids =
            NullableColumn::new_column(UInt64Type::from_data(vec![0u64, 0]), Bitmap::new_zeroed(2));
        let batch = Arc::new(RowFetchBatch {
            id: 0,
            inputs: vec![DataBlock::new_from_columns(vec![null_row_ids])],
            indices: vec![],
            parts: 0,
            start: Instant::now(),
        });

        let output = assemble.transform(DataBlock::empty_with_meta(Box::new(RowFetched {
            batch,
            block: None,
        })))?;

        assert_eq!(output.len(), 1);
        assert_eq!(output[0].num_rows(), 2);
        assert_eq!(output[0].num_columns(), 2);
        Ok(())
    }

    #[test]
    fn test_assemble_rejects_incomplete_batch_on_finish() -> Result<()> {
        let mut assemble = assemble();
        let batch = batch(0, vec![1, 2], vec![(0, 0), (1, 0)], 2);
        assert!(assemble.transform(fetched(&batch, 0, vec![1]))?.is_empty());

        assert!(assemble.on_finish(true).is_err());
        // Downstream stopped reading: nothing is lost.
        assert!(assemble.on_finish(false)?.is_empty());
        Ok(())
    }

    #[test]
    fn test_assemble_rejects_row_out_of_fetched_block() {
        let mut assemble = assemble();
        let batch = batch(0, vec![1], vec![(0, 1)], 1);

        let result = assemble.transform(fetched(&batch, 0, vec![1]));
        assert!(result.err().unwrap().message().contains("RowID is invalid"));
    }
}
