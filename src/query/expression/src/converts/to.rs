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

use common_arrow::arrow::datatypes::Field as ArrowField;
use common_datavalues::DataTypeImpl;

use crate::TableDataType;
use crate::TableField;
use crate::TableSchema;

pub fn to_type(datatype: &TableDataType) -> DataTypeImpl {
    let f = TableField::new("tmp", datatype.clone());
    let arrow_f: ArrowField = (&f).into();
    common_datavalues::from_arrow_field(&arrow_f)
}

pub fn to_schema(schema: &TableSchema) -> common_datavalues::DataSchema {
    let fields = schema
        .fields()
        .iter()
        .map(|f| {
            let ty = to_type(f.data_type());
            common_datavalues::DataField::new(f.name(), ty)
                .with_default_expr(f.default_expr().cloned())
        })
        .collect();
    common_datavalues::DataSchema::new_from(fields, schema.meta().clone())
}
