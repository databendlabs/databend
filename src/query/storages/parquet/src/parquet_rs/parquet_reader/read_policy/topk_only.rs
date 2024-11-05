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

use databend_common_exception::Result;
use databend_common_expression::BlockEntry;
use databend_common_expression::DataBlock;
use databend_common_expression::DataField;
use databend_common_expression::DataSchema;
use databend_common_expression::DataSchemaRef;
use databend_common_expression::TableSchema;
use databend_common_expression::TopKSorter;
use parquet::arrow::arrow_reader::ParquetRecordBatchReader;
use parquet::arrow::arrow_reader::RowSelection;
use parquet::arrow::parquet_to_arrow_field_levels;
use parquet::arrow::FieldLevels;
use parquet::arrow::ProjectionMask;
use parquet::schema::types::SchemaDescriptor;

use super::policy::ReadPolicy;
use super::policy::ReadPolicyBuilder;
use super::policy::ReadPolicyImpl;
use super::utils::evaluate_topk;
use super::utils::read_all;
use crate::parquet_rs::parquet_reader::row_group::InMemoryRowGroup;
use crate::parquet_rs::parquet_reader::topk::BuiltTopK;
use crate::parquet_rs::parquet_reader::topk::ParquetTopK;
use crate::parquet_rs::parquet_reader::utils::compute_output_field_paths;
use crate::parquet_rs::parquet_reader::utils::transform_record_batch;
use crate::parquet_rs::parquet_reader::utils::FieldPaths;

pub struct TopkOnlyPolicyBuilder {
    topk: Arc<ParquetTopK>,
    /// Output columns - topk column.
    remain_projection: ProjectionMask,
    remain_field_levels: FieldLevels,
    remain_field_paths: Arc<Option<FieldPaths>>,

    prefetch_schema: DataSchemaRef,
    remain_schema: DataSchemaRef,
    src_schema: DataSchemaRef,
    dst_schema: DataSchemaRef,

    /// If the topk column is in the output columns.
    topk_in_output: bool,
}

impl TopkOnlyPolicyBuilder {
    pub fn create(
        schema_desc: &SchemaDescriptor,
        arrow_schema: Option<&arrow_schema::Schema>,
        topk: &BuiltTopK,
        output_schema: &TableSchema,
        output_leaves: &[usize],
        inner_projection: bool,
    ) -> Result<Box<dyn ReadPolicyBuilder>> {
        let BuiltTopK {
            topk,
            field: topk_field,
            leaf_id,
        } = topk;

        // Prefetch the topk column. Compute the remain columns.
        let remain_leaves = output_leaves
            .iter()
            .cloned()
            .filter(|i| i != leaf_id)
            .collect::<Vec<_>>();
        let remain_projection = ProjectionMask::leaves(schema_desc, remain_leaves);

        let mut remain_fields = Vec::with_capacity(output_schema.num_fields());
        let mut topk_in_output = false;
        for f in output_schema.fields() {
            if f.name() != topk_field.name() {
                remain_fields.push(f.clone());
            } else {
                topk_in_output = true;
            }
        }
        let remain_schema = TableSchema::new(remain_fields);
        let remain_field_levels = parquet_to_arrow_field_levels(
            schema_desc,
            remain_projection.clone(),
            arrow_schema.map(|s| &s.fields),
        )?;
        let remain_field_paths = Arc::new(compute_output_field_paths(
            schema_desc,
            &remain_projection,
            &remain_schema,
            inner_projection,
        )?);

        let mut src_schema = remain_schema.clone();
        if topk_in_output {
            src_schema.fields.push(topk_field.clone());
        }
        let src_schema = Arc::new(DataSchema::from(&src_schema));
        let dst_schema = Arc::new(DataSchema::from(output_schema));
        let prefetch_schema = Arc::new(DataSchema::new(vec![DataField::from(topk_field)]));
        let remain_schema = Arc::new(DataSchema::from(&remain_schema));

        Ok(Box::new(Self {
            topk: topk.clone(),
            remain_projection,
            remain_field_levels,
            remain_field_paths,
            prefetch_schema,
            remain_schema,
            src_schema,
            dst_schema,
            topk_in_output,
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
        let block = read_all(
            &self.prefetch_schema,
            &row_group,
            self.topk.field_levels(),
            selection.clone(),
            self.topk.field_paths(),
            num_rows,
        )?;
        let prefetched =
            if let Some(block) = evaluate_topk(block, &self.topk, &mut selection, sorter)? {
                block
            } else {
                // All rows are filtered out.
                return Ok(None);
            };

        // Only store the topk column when we need to output it.
        let prefetched_cols = if self.topk_in_output {
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
            Some(prefetched_cols)
        } else {
            None
        };

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
        Ok(Some(Box::new(TopkOnlyPolicy {
            prefetched: prefetched_cols,
            reader,
            remain_field_paths: self.remain_field_paths.clone(),
            remain_schema: self.remain_schema.clone(),
            src_schema: self.src_schema.clone(),
            dst_schema: self.dst_schema.clone(),
        })))
    }
}

/// This policy is for the case that predicate is [None] but topk is [Some].
/// We will prefetch the topk column (must be 1 column) and update the topk heap ([`TopKSorter`]),
/// and then read other columns.
pub struct TopkOnlyPolicy {
    prefetched: Option<VecDeque<BlockEntry>>,
    reader: ParquetRecordBatchReader,

    /// See the comments of `field_paths` in [`super::NoPrefetchPolicy`].
    remain_field_paths: Arc<Option<FieldPaths>>,
    /// The schema of remain columns.
    remain_schema: DataSchemaRef,
    /// The schema of remain columns + topk column (topk column is at the last).
    src_schema: DataSchemaRef,
    /// The final output schema.
    dst_schema: DataSchemaRef,
}

impl ReadPolicy for TopkOnlyPolicy {
    fn read_block(&mut self) -> Result<Option<DataBlock>> {
        let batch = self.reader.next().transpose()?;
        if let Some(batch) = batch {
            debug_assert!(
                self.prefetched.is_none() || !self.prefetched.as_ref().unwrap().is_empty()
            );
            let mut block =
                transform_record_batch(&self.remain_schema, &batch, &self.remain_field_paths)?;
            if let Some(q) = self.prefetched.as_mut() {
                let prefetched = q.pop_front().unwrap();
                block.add_column(prefetched);
            }
            let block = block.resort(&self.src_schema, &self.dst_schema)?;
            Ok(Some(block))
        } else {
            debug_assert!(
                self.prefetched.is_none() || self.prefetched.as_ref().unwrap().is_empty()
            );
            Ok(None)
        }
    }
}
