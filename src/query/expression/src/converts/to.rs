// Copyright 2022 Datafuse Labs.
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

use common_datablocks::DataBlock;
use common_datavalues::Column as DvColumn;
use common_datavalues::ColumnRef;
use common_datavalues::ConstColumn;
use common_datavalues::DataSchemaRef;
use common_datavalues::DataTypeImpl;
use common_datavalues::IntoColumn;
use common_arrow::arrow::datatypes::Field as ArrowField;


use crate::DataField;
use crate::DataSchema;
use crate::types::AnyType;
use crate::types::DataType;
use crate::Chunk;
use crate::Column;
use crate::ColumnBuilder;
use crate::Value;

pub fn to_type(datatype: &DataType) -> DataTypeImpl {
    let f = DataField::new("tmp", datatype.clone());
    let arrow_f: ArrowField = (&f).into();
    common_datavalues::from_arrow_field(&arrow_f)
}


pub fn to_schema(schema: &DataSchema) -> common_datavalues::DataSchema {
    let fields = schema
        .fields()
        .iter()
        .map(|f| {
            let ty = to_type(f.data_type());
            common_datavalues::DataField::new(f.name(), ty)
        })
        .collect();
    common_datavalues::DataSchema::new_from(fields, schema.meta().clone())
}


// we do not need conver scalar to datavalue

pub fn to_column(column: &Value<AnyType>, size: usize, data_type: &DataType) -> ColumnRef {
    match column {
        Value::Scalar(s) => {
            let col = ColumnBuilder::repeat(&s.as_ref(), 1, data_type).build();
            let col = to_column(&Value::Column(col), 1, data_type);
            ConstColumn::new(col, size).arc()
        }
        Value::Column(c) => {
            let is_nullable = matches!(c, Column::Nullable(_));
            let arrow_column = c.as_arrow();
            if is_nullable {
                arrow_column.into_nullable_column()
            } else {
                arrow_column.into_column()
            }
        }
    }
}

pub fn to_datablock(chunk: &Chunk, schema: DataSchemaRef) -> DataBlock {
    let columns = chunk
        .columns()
        .iter()
        .map(|(c, ty)| to_column(c, chunk.num_rows(), ty))
        .collect();
    DataBlock::create(schema, columns)
}
