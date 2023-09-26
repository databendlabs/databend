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
use common_expression::BlockEntry;
use common_expression::DataBlock;
use common_expression::DataSchema;
use common_expression::DataSchemaRef;
use common_expression::TableField;
use common_expression::TableSchema;
use common_expression::TopKSorter;
use parquet::arrow::arrow_reader::ParquetRecordBatchReader;
use parquet::arrow::arrow_reader::RowSelection;
use parquet::arrow::parquet_to_arrow_field_levels;
use parquet::arrow::FieldLevels;
use parquet::arrow::ProjectionMask;
use parquet::schema::types::SchemaDescriptor;

use super::policy::ReadPolicy;
use super::policy::ReadPolicyBuilder;
use super::policy::ReadPolicyImpl;
use crate::parquet_rs::parquet_reader::row_group::InMemoryRowGroup;
use crate::parquet_rs::parquet_reader::topk::ParquetTopK;
use crate::parquet_rs::parquet_reader::utils::bitmap_to_boolean_array;
use crate::parquet_rs::parquet_reader::utils::compute_output_field_paths;
use crate::parquet_rs::parquet_reader::utils::transform_record_batch;
use crate::parquet_rs::parquet_reader::utils::FieldPaths;

pub struct TopkOnlyPolicyBuilder {
    topk: Arc<ParquetTopK>,
    /// Output columns - topk column.
    remain_projection: ProjectionMask,
    remain_field_levels: FieldLevels,
    remain_field_paths: Arc<Option<FieldPaths>>,

    src_schema: DataSchemaRef,
    dst_schema: DataSchemaRef,
}

impl TopkOnlyPolicyBuilder {
    pub fn create(
        schema_desc: &SchemaDescriptor,
        topk: (Arc<ParquetTopK>, TableField),
        output_schema: &TableSchema,
        output_leaves: &[usize],
        inner_projection: bool,
    ) -> Result<Box<dyn ReadPolicyBuilder>> {
        let (topk, topk_field) = topk;

        // Prefetch the topk column. Compute the remain columns.
        let remain_leaves = output_leaves
            .iter()
            .cloned()
            .filter(|i| *i != topk_field.column_id as usize)
            .collect::<Vec<_>>();
        let remain_projection = ProjectionMask::leaves(schema_desc, remain_leaves);
        let remain_fields = output_schema
            .fields()
            .iter()
            .cloned()
            .filter(|f| f.name() != topk_field.name())
            .collect::<Vec<_>>();
        let remain_schema = TableSchema::new(remain_fields);
        let remain_field_levels =
            parquet_to_arrow_field_levels(schema_desc, remain_projection.clone(), None)?;
        let remain_field_paths = Arc::new(compute_output_field_paths(
            schema_desc,
            &remain_projection,
            &remain_schema,
            inner_projection,
        )?);

        let mut src_schema = remain_schema;
        src_schema.fields.push(topk_field);
        let src_schema = Arc::new(DataSchema::from(&src_schema));
        let dst_schema = Arc::new(DataSchema::from(output_schema));

        Ok(Box::new(Self {
            topk,
            remain_projection,
            remain_field_levels,
            remain_field_paths,
            src_schema,
            dst_schema,
        }))
    }
}

#[async_trait::async_trait]
impl ReadPolicyBuilder for TopkOnlyPolicyBuilder {
    async fn build(
        &self,
        mut row_group: InMemoryRowGroup<'_>,
        mut selection: Option<RowSelection>,
        sorter: &mut Option<TopKSorter>,
        batch_size: usize,
    ) -> Result<Option<ReadPolicyImpl>> {
        debug_assert!(sorter.is_some());
        let sorter = sorter.as_mut().unwrap();
        let mut num_rows = selection
            .as_ref()
            .map(|x| x.row_count())
            .unwrap_or(row_group.row_count());

        // Prefetch the topk column.
        row_group
            .fetch(self.topk.projection(), selection.as_ref())
            .await?;
        let mut reader = ParquetRecordBatchReader::try_new_with_row_groups(
            self.topk.field_levels(),
            &row_group,
            num_rows, // Read all rows at one time.
            selection.clone(),
        )?;
        let batch = reader.next().transpose()?.unwrap();
        debug_assert!(reader.next().is_none());
        // Topk column **must** not be in a nested column.
        let prefetched = transform_record_batch(&batch, &None)?;
        debug_assert_eq!(prefetched.num_columns(), 1);
        let topk_col = prefetched.columns()[0]
            .value
            .convert_to_full_column(&DataType::Boolean, num_rows);
        let filter = self.topk.evaluate_column(&topk_col, sorter);
        if filter.unset_bits() == num_rows {
            // All rows are filtered out.
            return Ok(None);
        }
        let prefetched = prefetched.filter_with_bitmap(&filter)?;
        let filter = bitmap_to_boolean_array(filter);
        let sel = RowSelection::from_filters(&[filter]);
        // Update row selection.
        match selection.as_mut() {
            Some(selection) => {
                selection.and_then(&sel);
            }
            None => {
                selection = Some(sel);
            }
        }

        // Slice the prefetched block by `batch_size`.
        num_rows = prefetched.num_rows();
        let mut prefetched_cols = VecDeque::with_capacity(num_rows.div_ceil(batch_size));
        if num_rows > batch_size {
            for i in (0..num_rows).step_by(batch_size) {
                let end = std::cmp::min(i + batch_size, num_rows);
                let block = prefetched.slice(i..end);
                prefetched_cols.push_back(block.columns()[0].clone());
            }
        } else {
            prefetched_cols.push_back(prefetched.columns()[0].clone());
        }

        // Fetch  remain columns.
        row_group
            .fetch(&self.remain_projection, selection.as_ref())
            .await?;
        let reader = ParquetRecordBatchReader::try_new_with_row_groups(
            &self.remain_field_levels,
            &row_group,
            batch_size,
            selection,
        )?;
        Ok(Some(Box::new(TopkOnlyPolicy {
            prefetched: prefetched_cols,
            reader,
            remain_field_paths: self.remain_field_paths.clone(),
            src_schema: self.src_schema.clone(),
            dst_schema: self.dst_schema.clone(),
        })))
    }
}

/// This policy is for the case that predicate is [None] but topk is [Some].
/// We will prefetch the topk column (must be 1 column) and update the topk heap ([`TopKSorter`]),
/// and then read other columns.
pub struct TopkOnlyPolicy {
    prefetched: VecDeque<BlockEntry>,
    reader: ParquetRecordBatchReader,

    /// See the comments of `field_paths` in [`super::NoPrefetchPolicy`].
    remain_field_paths: Arc<Option<FieldPaths>>,
    /// The schema of remain columns + topk column (topk column is at the last).
    src_schema: DataSchemaRef,
    /// The final output schema.
    dst_schema: DataSchemaRef,
}

impl ReadPolicy for TopkOnlyPolicy {
    fn read_block(&mut self) -> Result<Option<DataBlock>> {
        let batch = self.reader.next().transpose()?;
        if let Some(batch) = batch {
            debug_assert!(!self.prefetched.is_empty());
            let prefetched = self.prefetched.pop_front().unwrap();
            let mut block = transform_record_batch(&batch, &self.remain_field_paths)?;
            block.add_column(prefetched);
            let block = block.resort(&self.src_schema, &self.dst_schema)?;
            Ok(Some(block))
        } else {
            debug_assert!(self.prefetched.is_empty());
            Ok(None)
        }
    }
}
