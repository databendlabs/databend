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

use std::collections::VecDeque;
use std::sync::Arc;

use common_exception::Result;
use common_expression::types::DataType;
use common_expression::DataBlock;
use common_expression::DataSchemaRef;
use common_expression::TableField;
use common_expression::TopKSorter;
use parquet::arrow::arrow_reader::ParquetRecordBatchReader;
use parquet::arrow::arrow_reader::RowSelection;
use parquet::arrow::FieldLevels;
use parquet::arrow::ProjectionMask;
use parquet::schema::types::SchemaDescriptor;

use super::policy::ReadPolicy;
use super::policy::ReadPolicyBuilder;
use super::policy::ReadPolicyImpl;
use crate::parquet_rs::parquet_reader::predicate::ParquetPredicate;
use crate::parquet_rs::parquet_reader::row_group::InMemoryRowGroup;
use crate::parquet_rs::parquet_reader::topk::ParquetTopK;
use crate::parquet_rs::parquet_reader::utils::bitmap_to_boolean_array;
use crate::parquet_rs::parquet_reader::utils::transform_record_batch;
use crate::parquet_rs::parquet_reader::utils::FieldPaths;

pub struct PredicateAndTopkPolicyBuilder {
    // Prefetched columns
    prefetch_projection: ProjectionMask,
    predicate: ParquetPredicate,
    topk: Option<ParquetTopK>,
    predicate_field_paths: Arc<Option<FieldPaths>>,

    // Remain columns
    remain_projection: ProjectionMask,
    remain_field_levels: FieldLevels,
    remain_field_paths: Arc<Option<FieldPaths>>,

    src_schema: DataSchemaRef,
    dst_schema: DataSchemaRef,
}

impl PredicateAndTopkPolicy {
    pub fn create(
        schema: &SchemaDescriptor,
        predicate: &(Arc<ParquetPredicate>, Vec<usize>),
        topk: Option<&(Arc<ParquetTopK>, &TableField)>,
    ) -> Result<Box<dyn ReadPolicyBuilder>> {
        todo!()
    }
}

#[async_trait::async_trait]
impl ReadPolicyBuilder for PredicateAndTopkPolicyBuilder {
    async fn build(
        &self,
        mut row_group: InMemoryRowGroup<'_>,
        mut selection: Option<RowSelection>,
        sorter: &mut Option<TopKSorter>,
        batch_size: usize,
    ) -> Result<Option<ReadPolicyImpl>> {
        let mut num_rows = selection
            .as_ref()
            .map(|x| x.row_count())
            .unwrap_or(row_group.row_count());
        // Prefetch predicate and topk columns.
        row_group
            .fetch(&self.prefetch_projection, selection.as_ref())
            .await?;
        let mut prefetched = DataBlock::new(vec![], 0);

        // Evaluate predicate
        {
            let mut reader = ParquetRecordBatchReader::try_new_with_row_groups(
                self.predicate.field_levels(),
                &row_group,
                num_rows, // Read all rows at one time.
                selection.clone(),
            )?;
            let batch = reader.next().transpose()?.unwrap();
            debug_assert!(reader.next().is_none());
            prefetched = transform_record_batch(&batch, &self.predicate_field_paths)?;
            let filter = self.predicate.evaluate_block(&prefetched)?;
            if filter.unset_bits() == num_rows {
                // All rows in current row group are filtered out.
                return Ok(None);
            }
            prefetched = prefetched.filter_with_bitmap(&filter)?;
            num_rows = prefetched.num_rows();
            let filter = bitmap_to_boolean_array(filter);
            let sel = RowSelection::from_filters(&[filter]);
            match selection.as_mut() {
                Some(selection) => {
                    selection.and_then(&sel);
                }
                None => {
                    selection = Some(sel);
                }
            }
        }

        // Evaluate topk
        if let Some((topk, sorter)) = self.topk.as_ref().zip(sorter.as_mut()) {
            let mut reader = ParquetRecordBatchReader::try_new_with_row_groups(
                topk.field_levels(),
                &row_group,
                num_rows, // Read all rows at one time.
                selection.clone(),
            )?;
            let batch = reader.next().transpose()?.unwrap();
            debug_assert!(reader.next().is_none());
            let block = transform_record_batch(&batch, &None)?;
            debug_assert_eq!(block.num_columns(), 1);
            let col = block.columns()[0]
                .value
                .convert_to_full_column(&DataType::Boolean, num_rows);
            let filter = topk.evaluate_column(&col, sorter);
            if filter.unset_bits() == num_rows {
                // All rows in current row group are filtered out.
                return Ok(None);
            }
            prefetched.merge_block(block);
            prefetched = prefetched.filter_with_bitmap(&filter)?;
            let filter = bitmap_to_boolean_array(filter);
            let sel = RowSelection::from_filters(&[filter]);
            match selection.as_mut() {
                Some(selection) => {
                    selection.and_then(&sel);
                }
                None => {
                    selection = Some(sel);
                }
            }
        }

        // Slice the prefetched block by `batch_size`.
        num_rows = prefetched.num_rows();
        let mut prefetched_blocks = VecDeque::with_capacity(num_rows.div_ceil(batch_size));
        if num_rows > batch_size {
            for i in (0..num_rows).step_by(batch_size) {
                let end = std::cmp::min(i + batch_size, num_rows);
                let block = prefetched.slice(i..end);
                prefetched_blocks.push_back(block);
            }
        } else {
            prefetched_blocks.push_back(prefetched);
        }

        // Fetch remain columns.
        row_group
            .fetch(&self.remain_projection, selection.as_ref())
            .await?;
        let reader = ParquetRecordBatchReader::try_new_with_row_groups(
            &self.remain_field_levels,
            &row_group,
            batch_size,
            selection,
        )?;

        Ok(Some(Box::new(PredicateAndTopkPolicy {
            prefetched: prefetched_blocks,
            reader,
            remain_field_paths: self.remain_field_paths.clone(),
            src_schema: self.src_schema.clone(),
            dst_schema: self.dst_schema.clone(),
        })))
    }
}

/// This policy is for the case that predicate is [Some] (topk may be [Some]).
/// We will prefetch predicate and topk columns first
/// and then evaluate predicate and topk to get the final row selection.
/// Finally, we use the final row selection to build the remain data reader.
pub struct PredicateAndTopkPolicy {
    prefetched: VecDeque<DataBlock>,
    reader: ParquetRecordBatchReader,

    /// See the comments of `field_paths` in [`super::NoPrefetchPolicy`].
    remain_field_paths: Arc<Option<FieldPaths>>,
    /// The schema of remain block + prefetched block.
    src_schema: DataSchemaRef,
    /// The final output schema.
    dst_schema: DataSchemaRef,
}

impl ReadPolicy for PredicateAndTopkPolicy {
    fn read_block(&mut self) -> Result<Option<DataBlock>> {
        let batch = self.reader.next().transpose()?;
        if let Some(batch) = batch {
            debug_assert!(!self.prefetched.is_empty());
            let prefetched = self.prefetched.pop_front().unwrap();
            let mut block = transform_record_batch(&batch, &self.remain_field_paths)?;
            block.merge_block(prefetched);
            let block = block.resort(&self.src_schema, &self.dst_schema)?;
            Ok(Some(block))
        } else {
            debug_assert!(self.prefetched.is_empty());
            Ok(None)
        }
    }
}
