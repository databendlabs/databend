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

use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::types::AnyType;
use databend_common_expression::BlockEntry;
use databend_common_expression::DataBlock;
use databend_common_expression::TableField;
use databend_common_expression::TableSchemaRef;
use databend_common_expression::Value;

use crate::hive_partition::HivePartInfo;
use crate::utils::str_field_to_scalar;

#[derive(Debug, Clone)]
pub struct HivePartitionFiller {
    pub partition_fields: Vec<TableField>,
}

impl HivePartitionFiller {
    pub fn create(_schema: TableSchemaRef, partition_fields: Vec<TableField>) -> Self {
        HivePartitionFiller { partition_fields }
    }

    fn generate_value(
        &self,
        _num_rows: usize,
        value: String,
        field: &TableField,
    ) -> Result<Value<AnyType>> {
        let value = str_field_to_scalar(&value, &field.data_type().into())?;
        Ok(Value::Scalar(value))
    }

    fn extract_partition_values(&self, hive_part: &HivePartInfo) -> Result<Vec<String>> {
        let partition_map = hive_part.get_partition_map();

        let mut partition_values = vec![];
        for field in self.partition_fields.iter() {
            match partition_map.get(field.name()) {
                Some(v) => partition_values.push(v.to_string()),
                None => {
                    return Err(ErrorCode::TableInfoError(format!(
                        "couldn't find hive partition info :{}, hive partition maps:{:?}",
                        field.name(),
                        partition_map
                    )));
                }
            };
        }
        Ok(partition_values)
    }

    pub fn fill_data(
        &self,
        data_block: DataBlock,
        part: &HivePartInfo,
        origin_num_rows: usize,
    ) -> Result<DataBlock> {
        let data_values = self.extract_partition_values(part)?;

        // create column, create datafield
        let mut num_rows = data_block.num_rows();
        if num_rows == 0 {
            num_rows = origin_num_rows;
        }

        let mut columns = data_block.columns().to_vec();

        for (i, field) in self.partition_fields.iter().enumerate() {
            let value = &data_values[i];
            let column = self.generate_value(num_rows, value.clone(), field)?;
            columns.push(BlockEntry::new(field.data_type().into(), column));
        }

        Ok(DataBlock::new(columns, num_rows))
    }
}
