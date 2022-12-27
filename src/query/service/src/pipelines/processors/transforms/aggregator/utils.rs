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

#[macro_export]
macro_rules! data_type_of_group_key_column {
    ($key:expr) => {{
        {
            use common_expression::types::DataType;
            use common_expression::Column;

            match $key {
                Column::String(_) => DataType::String,
                Column::Number(number_ty) => match number_ty {
                    NumberColumn::UInt8 => DataType::Number(NumberDataType::UInt8),
                    NumberColumn::UInt16 => DataType::Number(NumberDataType::UInt16),
                    NumberColumn::UInt32 => DataType::Number(NumberDataType::UInt32),
                    NumberColumn::UInt64 => DataType::Number(NumberDataType::UInt64),
                    NumberColumn::Int8 => DataType::Number(NumberDataType::Int8),
                    NumberColumn::Int16 => DataType::Number(NumberDataType::Int16),
                    NumberColumn::Int32 => DataType::Number(NumberDataType::Int32),
                    NumberColumn::Int64 => DataType::Number(NumberDataType::Int64),
                    NumberColumn::Float32 => DataType::Number(NumberDataType::Float32),
                    NumberColumn::Float64 => DataType::Number(NumberDataType::Float64),
                },
                _ => unreachable!("Group key can only be number or string type."),
            }
        }
    }};
}
