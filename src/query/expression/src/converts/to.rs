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

use crate::types::AnyType;
use crate::types::DataType;
use crate::types::NumberDataType;
use crate::with_number_type;
use crate::Chunk;
use crate::Column;
use crate::ColumnBuilder;
use crate::ColumnIndex;
use crate::Value;

pub fn to_type(datatype: &DataTypeImpl) -> DataType {
    with_number_type!(|TYPE| match datatype {
        DataTypeImpl::TYPE(_) => DataType::Number(NumberDataType::TYPE),

        DataTypeImpl::Null(_) => DataType::Null,
        DataTypeImpl::Nullable(v) => DataType::Nullable(Box::new(to_type(v.inner_type()))),
        DataTypeImpl::Boolean(_) => DataType::Boolean,
        DataTypeImpl::Timestamp(_) => DataType::Timestamp,
        DataTypeImpl::Date(_) => DataType::Date,
        DataTypeImpl::String(_) => DataType::String,
        DataTypeImpl::Struct(ty) => {
            let inners = ty.types().iter().map(to_type).collect();
            DataType::Tuple(inners)
        }
        DataTypeImpl::Array(ty) => DataType::Array(Box::new(to_type(ty.inner_type()))),
        DataTypeImpl::Variant(_)
        | DataTypeImpl::VariantArray(_)
        | DataTypeImpl::VariantObject(_) => DataType::Variant,
        DataTypeImpl::Interval(_) => unimplemented!(),
    })
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

pub fn to_datablock<Index: ColumnIndex>(chunk: &Chunk<Index>, schema: DataSchemaRef) -> DataBlock {
    let columns = chunk
        .columns()
        .map(|entry| to_column(&entry.value, chunk.num_rows(), &entry.data_type))
        .collect();
    DataBlock::create(schema, columns)
}
