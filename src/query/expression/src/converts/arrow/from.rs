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
use arrow_schema::Field;
use arrow_schema::Schema as ArrowSchema;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;

use crate::types::DataType;
use crate::Column;
use crate::DataBlock;
use crate::DataField;
use crate::DataSchema;
use crate::TableField;
use crate::TableSchema;

impl TryFrom<&Field> for DataField {
    type Error = ErrorCode;
    fn try_from(arrow_f: &Field) -> Result<DataField> {
        Ok(DataField::from(&TableField::try_from(arrow_f)?))
    }
}

impl TryFrom<&Field> for TableField {
    type Error = ErrorCode;
    fn try_from(arrow_f: &Field) -> Result<TableField> {
        todo!("cc")
    }
}

impl TryFrom<&ArrowSchema> for DataSchema {
    type Error = ErrorCode;
    fn try_from(schema: &ArrowSchema) -> Result<DataSchema> {
        let fields = schema
            .fields
            .iter()
            .map(|arrow_f| {
                // Ok(DataField::from(&TableField::try_from(&Arrow2Field::from(
                //     arrow_f,
                // ))?))
                todo!("cc")
            })
            .collect::<Result<Vec<_>>>()?;
        Ok(DataSchema::new_from(
            fields,
            schema.metadata.clone().into_iter().collect(),
        ))
    }
}

impl TryFrom<&ArrowSchema> for TableSchema {
    type Error = ErrorCode;
    fn try_from(schema: &ArrowSchema) -> Result<TableSchema> {
        // let fields = schema
        //     .fields
        //     .iter()
        //     .map(|arrow_f| TableField::try_from(&Arrow2Field::from(arrow_f)))
        //     .collect::<Result<Vec<_>>>()?;
        // Ok(TableSchema::new_from(
        //     fields,
        //     schema.metadata.clone().into_iter().collect(),
        // ))
        todo!("cc")
    }
}

impl DataBlock {
    pub fn from_record_batch(
        schema: &DataSchema,
        batch: &RecordBatch,
    ) -> Result<(Self, DataSchema)> {
        assert_eq!(
            schema.num_fields(),
            batch.num_columns(),
            "expect schema: {:?}, actual schema: {:?}",
            schema.fields,
            batch.schema().fields
        );

        if schema.fields().len() != batch.num_columns() {
            return Err(ErrorCode::Internal(format!(
                "conversion from RecordBatch to DataBlock failed, schema fields len: {}, RecordBatch columns len: {}",
                schema.fields().len(),
                batch.num_columns()
            )));
        }

        if batch.num_columns() == 0 {
            return Ok((DataBlock::new(vec![], batch.num_rows()), schema.clone()));
        }

        let mut columns = Vec::with_capacity(batch.columns().len());
        for (array, field) in batch.columns().iter().zip(schema.fields()) {
            columns.push(Column::from_arrow_rs(array.clone(), field.data_type())?)
        }

        Ok((DataBlock::new_from_columns(columns), schema.clone()))
    }
}

impl Column {
    pub fn from_arrow_rs(array: Arc<dyn arrow_array::Array>, data_type: &DataType) -> Result<Self> {
        todo!("cc")
    }
}
