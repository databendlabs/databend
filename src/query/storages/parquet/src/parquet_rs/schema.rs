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

use arrow_schema::DataType as ArrowDataType;
use arrow_schema::Field as ArrowField;
use arrow_schema::Schema as ArrowSchema;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::TableSchema;

pub(crate) fn lower_field_name(field: &ArrowField) -> ArrowField {
    let name = field.name().to_lowercase();
    let field = field.clone().with_name(name);
    match &field.data_type() {
        ArrowDataType::List(f) => {
            let inner = lower_field_name(f);
            field.with_data_type(ArrowDataType::List(Arc::new(inner)))
        }
        ArrowDataType::Struct(fields) => {
            let typ = ArrowDataType::Struct(
                fields
                    .iter()
                    .map(|f| lower_field_name(f))
                    .collect::<Vec<_>>()
                    .into(),
            );
            field.with_data_type(typ)
        }
        _ => field,
    }
}

pub(crate) fn arrow_to_table_schema(schema: &ArrowSchema) -> Result<TableSchema> {
    let fields = schema
        .fields
        .iter()
        .map(|f| Arc::new(lower_field_name(f)))
        .collect::<Vec<_>>();
    let schema = ArrowSchema::new_with_metadata(fields, schema.metadata().clone());
    TableSchema::try_from(&schema).map_err(ErrorCode::from_std_error)
}
