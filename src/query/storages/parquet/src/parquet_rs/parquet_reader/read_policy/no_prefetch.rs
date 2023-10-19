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

use common_exception::Result;
use common_expression::DataBlock;
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
use crate::parquet_rs::parquet_reader::utils::transform_record_batch;
use crate::parquet_rs::parquet_reader::utils::FieldPaths;

pub struct NoPretchPolicyBuilder {
    projection: ProjectionMask,
    field_levels: FieldLevels,
    field_paths: Arc<Option<FieldPaths>>,
}

#[async_trait::async_trait]
impl ReadPolicyBuilder for NoPretchPolicyBuilder {
    async fn build(
        &self,
        mut row_group: InMemoryRowGroup<'_>,
        _row_selection: Option<RowSelection>,
        _sorter: &mut Option<TopKSorter>,
        batch_size: usize,
    ) -> Result<Option<ReadPolicyImpl>> {
        row_group.fetch(&self.projection, None).await?;
        let reader = ParquetRecordBatchReader::try_new_with_row_groups(
            &self.field_levels,
            &row_group,
            batch_size,
            None,
        )?;
        Ok(Some(Box::new(NoPrefetchPolicy {
            field_paths: self.field_paths.clone(),
            reader,
        })))
    }
}

impl NoPretchPolicyBuilder {
    pub fn create(
        schema: &SchemaDescriptor,
        projection: ProjectionMask,
        field_paths: Arc<Option<FieldPaths>>,
    ) -> Result<Box<dyn ReadPolicyBuilder>> {
        let field_levels = parquet_to_arrow_field_levels(schema, projection.clone(), None)?;
        Ok(Box::new(NoPretchPolicyBuilder {
            field_levels,
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

    reader: ParquetRecordBatchReader,
}

impl ReadPolicy for NoPrefetchPolicy {
    fn read_block(&mut self) -> Result<Option<DataBlock>> {
        let batch = self.reader.next().transpose()?;
        if let Some(batch) = batch {
            let block = transform_record_batch(&batch, &self.field_paths)?;
            Ok(Some(block))
        } else {
            Ok(None)
        }
    }
}
