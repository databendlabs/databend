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

use databend_common_exception::Result;
use databend_common_expression::DataBlock;
use databend_common_expression::TableSchema;
use databend_storages_common_table_meta::meta::StatisticsOfColumns;
use parquet_rs::basic::Type as PhysicalType;
use parquet_rs::file::properties::WriterPropertiesBuilder;
use parquet_rs::schema::types::Type;
use parquet_rs::schema::types::TypePtr;

use super::byte_array::choose_byte_array_encoding;
use super::int::choose_int_encoding;

pub fn choose_codec(
    mut props: WriterPropertiesBuilder,
    block: &DataBlock,
    parquet_fields: &[TypePtr],
    table_schema: &TableSchema,
    stat: &StatisticsOfColumns,
) -> Result<WriterPropertiesBuilder> {
    for ((parquet_field, table_field), entry) in parquet_fields
        .iter()
        .zip(table_schema.fields.iter())
        .zip(block.columns())
    {
        let column = entry.to_column(block.num_rows());
        let array = column.into_arrow_rs();
        let stat = stat.get(&table_field.column_id);
        let column_name = table_field.name.as_str();
        match parquet_field.as_ref() {
            Type::PrimitiveType { physical_type, .. } => match physical_type {
                PhysicalType::BYTE_ARRAY | PhysicalType::FIXED_LEN_BYTE_ARRAY => {
                    props = choose_byte_array_encoding(props, stat, array, column_name)?;
                }
                PhysicalType::INT32 | PhysicalType::INT64 | PhysicalType::INT96 => {
                    props = choose_int_encoding(props, stat, array, column_name)?
                }
                _ => {}
            },
            Type::GroupType {
                basic_info: _,
                fields: _,
            } => {} // TODO: handle nested fields
        }
    }
    Ok(props)
}
