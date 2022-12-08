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

use std::sync::Arc;

use common_arrow::arrow::bitmap::Bitmap;
use common_exception::ErrorCode;
use common_exception::Result;
use common_expression::types::AnyType;
use common_expression::types::DataType;
use common_expression::types::NumberDataType;
use common_expression::with_number_mapped_type;
use common_expression::with_number_type;
use common_expression::Chunk;
use common_expression::DataField;
use common_expression::Scalar;
use common_expression::Value;

use crate::hive_partition::HivePartInfo;
use crate::utils::str_field_to_column;
use crate::hive_table::HIVE_DEFAULT_PARTITION;

#[derive(Debug, Clone)]
pub struct HivePartitionFiller {
    pub partition_fields: Vec<DataField>,
}

macro_rules! generate_primitive_column {
    ($T:ty, $num_rows:expr, $value:expr) => {{
        let column = Series::from_data(vec![$value.parse::<$T>().unwrap()]);
        Ok(Arc::new(ConstColumn::new(column, $num_rows)))
    }};
}

fn generate_string_column(num_rows: usize, value: String) -> Result<ColumnRef> {
    let validity = if value == HIVE_DEFAULT_PARTITION {
        Some(Bitmap::from(vec![false]))
    } else {
        None
    };
    let column = Series::from_data(vec![value]);

    Ok(Arc::new(ConstColumn::new(
        NullableColumn::wrap_inner(column, validity),
        num_rows,
    )))
}

impl HivePartitionFiller {
    pub fn create(partition_fields: Vec<DataField>) -> Self {
        HivePartitionFiller { partition_fields }
    }

    fn generate_column(
        &self,
        num_rows: usize,
        value: String,
        field: &DataField,
    ) -> Result<Value<AnyType>> {
        let column = str_field_to_column(num_rows, value, field.data_type())?;
        Ok(Value::Column(column))
    }

    fn extract_partition_values(&self, hive_part: &HivePartInfo) -> Result<Vec<String>> {
        let partition_map = hive_part.get_partition_map();

        let mut partition_values = vec![];
        for field in self.partition_fields.iter() {
            match partition_map.get(field.name()) {
                Some(v) => partition_values.push(v.to_string()),
                None => {
                    return Err(ErrorCode::TableInfoError(format!(
                        "could't find hive partition info :{}, hive partition maps:{:?}",
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
        mut chunk: Chunk,
        part: &HivePartInfo,
        origin_num_rows: usize,
    ) -> Result<Chunk> {
        let data_values = self.extract_partition_values(part)?;

        // create column, create datafiled
        let mut num_rows = chunk.num_rows();
        if num_rows == 0 {
            num_rows = origin_num_rows;
        }
        for (i, field) in self.partition_fields.iter().enumerate() {
            let value = &data_values[i];
            let column = self.generate_column(num_rows, value.clone(), field)?;
            chunk.add_column(column, field.data_type().clone());
        }
        Ok(chunk)
    }
}
