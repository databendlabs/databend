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

use std::sync::Arc;

use databend_common_exception::Result;
use databend_common_expression::DataBlock;
use databend_common_expression::DataSchema;
use databend_common_expression::TopKSorter;
use parquet::arrow::FieldLevels;
use parquet::arrow::ProjectionMask;
use parquet::arrow::arrow_reader::ParquetRecordBatchReader;
use parquet::arrow::arrow_reader::RowSelection;
use parquet::arrow::parquet_to_arrow_field_levels;
use parquet::schema::types::SchemaDescriptor;

use super::policy::ReadPolicy;
use super::policy::ReadPolicyBuilder;
use super::policy::ReadPolicyImpl;
use crate::parquet_reader::predicate::ParquetPredicate;
use crate::parquet_reader::read_policy::utils::read_all;
use crate::parquet_reader::row_group::InMemoryRowGroup;
use crate::parquet_reader::utils::FieldPaths;
use crate::parquet_reader::utils::bitmap_to_boolean_array;
use crate::parquet_reader::utils::transform_record_batch;
use crate::transformer::RecordBatchTransformer;

pub struct NoPretchPolicyBuilder {
    projection: ProjectionMask,
    data_schema: DataSchema,
    field_levels: FieldLevels,
    field_paths: Arc<Option<FieldPaths>>,
}

#[async_trait::async_trait]
impl ReadPolicyBuilder for NoPretchPolicyBuilder {
    async fn fetch_and_build(
        &self,
        mut row_group: InMemoryRowGroup<'_>,
        mut row_selection: Option<RowSelection>,
        _sorter: &mut Option<TopKSorter>,
        transformer: Option<RecordBatchTransformer>,
        batch_size: usize,
        filter: Option<Arc<ParquetPredicate>>,
    ) -> Result<Option<ReadPolicyImpl>> {
        if let Some(predicate) = filter {
            row_group
                .fetch(predicate.projection(), row_selection.as_ref())
                .await?;

            let num_rows = row_selection
                .as_ref()
                .map(|x| x.row_count())
                .unwrap_or(row_group.row_count());

            let block = read_all(
                &DataSchema::from(predicate.schema()),
                &row_group,
                predicate.field_levels(),
                row_selection.clone(),
                predicate.field_paths(),
                num_rows,
            )?;
            let filter = predicate.evaluate_block(&block)?;
            if filter.null_count() == num_rows {
                // All rows in current row group are filtered out.
                return Ok(None);
            }
            let filter = bitmap_to_boolean_array(filter);
            let sel = RowSelection::from_filters(&[filter]);
            match row_selection.as_mut() {
                Some(selection) => {
                    *selection = selection.and_then(&sel);
                }
                None => {
                    row_selection = Some(sel);
                }
            }
        }

        row_group.fetch(&self.projection, None).await?;
        let reader = ParquetRecordBatchReader::try_new_with_row_groups(
            &self.field_levels,
            &row_group,
            batch_size,
            row_selection,
        )?;
        Ok(Some(Box::new(NoPrefetchPolicy {
            field_paths: self.field_paths.clone(),
            data_schema: self.data_schema.clone(),
            reader,
            transformer,
        })))
    }
}

impl NoPretchPolicyBuilder {
    pub fn create(
        schema: &SchemaDescriptor,
        arrow_schema: Option<&arrow_schema::Schema>,
        data_schema: DataSchema,
        projection: ProjectionMask,
        field_paths: Arc<Option<FieldPaths>>,
    ) -> Result<Box<dyn ReadPolicyBuilder>> {
        let field_levels = parquet_to_arrow_field_levels(
            schema,
            projection.clone(),
            arrow_schema.map(|s| &s.fields),
        )?;
        Ok(Box::new(NoPretchPolicyBuilder {
            field_levels,
            data_schema,
            projection,
            field_paths,
        }))
    }
}

/// This policy is for the case that predicate and topk are both [None].
/// We can only read all the output columns and don't do any evaluation.
pub struct NoPrefetchPolicy {
    /// Field paths helping to traverse columns.
    ///
    /// If we use [`ProjectionMask`] to get inner columns of a struct,
    /// the columns will be contains in a struct array in the read [`arrow_array::RecordBatch`].
    ///
    /// Therefore, if `field_paths` is [Some],
    /// we should extract inner columns from the struct manually by traversing the nested column;
    /// if `field_paths` is [None], we can skip the traversing.
    field_paths: Arc<Option<FieldPaths>>,
    data_schema: DataSchema,

    reader: ParquetRecordBatchReader,
    transformer: Option<RecordBatchTransformer>,
}

impl ReadPolicy for NoPrefetchPolicy {
    fn read_block(&mut self) -> Result<Option<DataBlock>> {
        let batch = self.reader.next().transpose()?;
        if let Some(mut batch) = batch {
            if let Some(transformer) = &mut self.transformer {
                batch = transformer.process_record_batch(batch)?;
            }
            let block = transform_record_batch(&self.data_schema, &batch, &self.field_paths)?;
            Ok(Some(block))
        } else {
            Ok(None)
        }
    }
}
