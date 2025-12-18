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

use databend_common_expression::TableDataType;
use databend_common_expression::TableSchema;
use databend_common_expression::types::NumberDataType;

pub fn schema_date_time_to_int(schema: &mut TableSchema) {
    schema
        .fields
        .iter_mut()
        .for_each(|field| date_time_to_int(&mut field.data_type));
}

fn date_time_to_int(typ: &mut TableDataType) {
    match typ {
        TableDataType::Nullable(t) => date_time_to_int(t),
        TableDataType::Array(t) => date_time_to_int(t),
        TableDataType::Map(t) => date_time_to_int(t),
        TableDataType::Tuple { fields_type, .. } => {
            fields_type.iter_mut().for_each(date_time_to_int);
        }
        TableDataType::Timestamp => *typ = TableDataType::Number(NumberDataType::Int64),
        TableDataType::Date => *typ = TableDataType::Number(NumberDataType::Int32),
        _ => {}
    }
}
