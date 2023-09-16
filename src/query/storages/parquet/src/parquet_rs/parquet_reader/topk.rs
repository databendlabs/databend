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

use arrow_array::RecordBatch;
use common_arrow::arrow::array::Arrow2Arrow;
use common_arrow::arrow::bitmap::MutableBitmap;
use common_exception::Result;
use common_expression::Column;
use common_expression::TopKSorter;
use parquet::arrow::FieldLevels;
use parquet::arrow::ProjectionMask;

pub struct ParquetTopK {
    projection: ProjectionMask,
    field_levels: FieldLevels,
}

impl ParquetTopK {
    pub fn new(projection: ProjectionMask, field_levels: FieldLevels) -> Self {
        Self {
            projection,
            field_levels,
        }
    }

    pub fn projection(&self) -> &ProjectionMask {
        &self.projection
    }

    pub fn field_levels(&self) -> &FieldLevels {
        &self.field_levels
    }

    pub fn evaluate(
        &self,
        batch: &RecordBatch,
        sorter: &mut TopKSorter,
    ) -> Result<arrow_array::BooleanArray> {
        assert_eq!(batch.num_columns(), 1);
        let array = batch.column(0);
        let col = Column::from_arrow_rs(array.clone(), batch.schema().field(0))?;
        let num_rows = col.len();
        let mut bitmap = MutableBitmap::with_capacity(num_rows);
        bitmap.extend_constant(num_rows, true);
        sorter.push_column(&col, &mut bitmap);
        let boolean_array = common_arrow::arrow::array::BooleanArray::new(
            common_arrow::arrow::datatypes::DataType::Boolean,
            bitmap.into(),
            None,
        );
        Ok(arrow_array::BooleanArray::from(boolean_array.to_data()))
    }
}
