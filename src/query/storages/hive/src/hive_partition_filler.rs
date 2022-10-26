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

use common_datablocks::DataBlock;
use common_datavalues::ColumnRef;
use common_datavalues::ConstColumn;
use common_datavalues::DataField;
use common_datavalues::DataTypeImpl;
use common_datavalues::Series;
use common_datavalues::SeriesFrom;
use common_exception::ErrorCode;
use common_exception::Result;

use crate::hive_partition::HivePartInfo;

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
    let column = Series::from_data(vec![value]);
    Ok(Arc::new(ConstColumn::new(column, num_rows)))
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
    ) -> Result<ColumnRef> {
        match field.data_type().clone() {
            DataTypeImpl::String(_) => generate_string_column(num_rows, value),
            DataTypeImpl::Int8(_) => generate_primitive_column!(i8, num_rows, value),
            DataTypeImpl::Int16(_) => generate_primitive_column!(i16, num_rows, value),
            DataTypeImpl::Int32(_) => generate_primitive_column!(i32, num_rows, value),
            DataTypeImpl::Int64(_) => generate_primitive_column!(i64, num_rows, value),
            DataTypeImpl::UInt8(_) => generate_primitive_column!(u8, num_rows, value),
            DataTypeImpl::UInt16(_) => generate_primitive_column!(u16, num_rows, value),
            DataTypeImpl::UInt32(_) => generate_primitive_column!(u32, num_rows, value),
            DataTypeImpl::UInt64(_) => generate_primitive_column!(u64, num_rows, value),
            DataTypeImpl::Float32(_) => generate_primitive_column!(f32, num_rows, value),
            DataTypeImpl::Float64(_) => generate_primitive_column!(f64, num_rows, value),
            _ => Err(ErrorCode::UnImplement(format!(
                "generate column failed, {:?}",
                field
            ))),
        }
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
        mut data_block: DataBlock,
        part: &HivePartInfo,
        origin_num_rows: usize,
    ) -> Result<DataBlock> {
        let data_values = self.extract_partition_values(part)?;

        // create column, create datafiled
        let mut num_rows = data_block.num_rows();
        if num_rows == 0 {
            num_rows = origin_num_rows;
        }
        for (i, field) in self.partition_fields.iter().enumerate() {
            let value = &data_values[i];
            let column = self.generate_column(num_rows, value.clone(), field)?;
            data_block = data_block.add_column(column, field.clone())?;
        }
        Ok(data_block)
    }
}
