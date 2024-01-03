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
use arrow_schema::DataType as ArrowDataType;
use arrow_schema::Field as ArrowField;
use arrow_schema::Schema as ArrowSchema;
use databend_common_arrow::arrow::datatypes::DataType as Arrow2DataType;
use databend_common_arrow::arrow::datatypes::Field as Arrow2Field;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;

use super::EXTENSION_KEY;
use crate::types::DataType;
use crate::Column;
use crate::DataBlock;
use crate::DataField;
use crate::DataSchema;
use crate::TableField;
use crate::TableSchema;

impl TryFrom<&ArrowSchema> for DataSchema {
    type Error = ErrorCode;

    fn try_from(schema: &ArrowSchema) -> Result<DataSchema> {
        let fields = schema
            .fields
            .iter()
            .map(|arrow_f| {
                Ok(DataField::from(&TableField::try_from(
                    &arrow2_field_from_arrow_field(arrow_f),
                )?))
            })
            .collect::<Result<Vec<_>>>()?;
        Ok(DataSchema::new(fields))
    }
}

impl TryFrom<&ArrowSchema> for TableSchema {
    type Error = ErrorCode;

    fn try_from(schema: &ArrowSchema) -> Result<TableSchema> {
        let fields = schema
            .fields
            .iter()
            .map(|arrow_f| TableField::try_from(&arrow2_field_from_arrow_field(arrow_f)))
            .collect::<Result<Vec<_>>>()?;
        Ok(TableSchema::new(fields))
    }
}

impl DataBlock {
    pub fn from_record_batch(
        schema: &DataSchema,
        batch: &RecordBatch,
    ) -> Result<(Self, DataSchema)> {
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
        let arrow2_array: Box<dyn databend_common_arrow::arrow::array::Array> = array.into();

        Ok(Column::from_arrow(arrow2_array.as_ref(), data_type))
    }
}

fn arrow2_field_from_arrow_field(field: &ArrowField) -> Arrow2Field {
    let mut data_type = match field.data_type() {
        ArrowDataType::List(f) => Arrow2DataType::List(Box::new(arrow2_field_from_arrow_field(f))),
        ArrowDataType::LargeList(f) => {
            Arrow2DataType::LargeList(Box::new(arrow2_field_from_arrow_field(f)))
        }
        ArrowDataType::FixedSizeList(f, size) => {
            Arrow2DataType::FixedSizeList(Box::new(arrow2_field_from_arrow_field(f)), *size as _)
        }
        ArrowDataType::Map(f, ordered) => {
            Arrow2DataType::Map(Box::new(arrow2_field_from_arrow_field(f)), *ordered)
        }
        ArrowDataType::Struct(f) => {
            Arrow2DataType::Struct(f.iter().map(|f| arrow2_field_from_arrow_field(f)).collect())
        }
        other => other.clone().into(),
    };

    if let Some(extension_type) = field.metadata().get(EXTENSION_KEY) {
        data_type = Arrow2DataType::Extension(extension_type.clone(), Box::new(data_type), None);
    }

    Arrow2Field::new(field.name(), data_type, field.is_nullable())
}
