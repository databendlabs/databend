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

use arrow_array::RecordBatch;
use arrow_schema::ArrowError;

use crate::Column;
use crate::DataBlock;
use crate::DataSchema;

impl DataBlock {
    pub fn to_record_batch(self, data_schema: &DataSchema) -> Result<RecordBatch, ArrowError> {
        let schema = Arc::new(data_schema.into());
        self.to_record_batch_with_arrow_schema(schema)
    }

    pub fn to_record_batch_with_arrow_schema(
        self,
        arrow_schema: arrow_schema::SchemaRef,
    ) -> Result<RecordBatch, ArrowError> {
        let mut arrays = Vec::with_capacity(self.columns().len());
        for entry in self.convert_to_full().columns() {
            let column = entry.value.to_owned().into_column().unwrap();
            arrays.push(column.into_arrow_rs()?)
        }
        RecordBatch::try_new(arrow_schema, arrays)
    }

    pub fn from_record_batch(
        schema: &DataSchema,
        batch: &RecordBatch,
    ) -> Result<(Self, DataSchema), ArrowError> {
        if batch.num_columns() == 0 {
            return Ok((DataBlock::new(vec![], batch.num_rows()), schema.clone()));
        }

        let mut columns = Vec::with_capacity(batch.columns().len());
        for (array, field) in batch.columns().iter().zip(schema.fields().iter()) {
            columns.push(Column::from_arrow_rs(array.clone(), field)?)
        }
        Ok((DataBlock::new_from_columns(columns), schema.clone()))
    }
}
