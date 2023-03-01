// Copyright 2023 Datafuse Labs.
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

use common_expression::types::number::NumberScalar;
use common_expression::types::DataType;
use common_expression::types::NumberColumnBuilder;
use common_expression::types::NumberDataType;
use common_expression::BlockEntry;
use common_expression::Column;
use common_expression::ColumnId;
use common_expression::TableDataType;
use common_expression::Value;

pub const ROW_ID: &str = "_row_id";

// meta data for generate virtual columns
pub struct VirtualColumnMeta {
    pub segment_idx: usize,
    pub block_idx: usize,
}

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, PartialEq)]
pub enum VirtualColumnType {
    RowId,
}

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, PartialEq)]
pub struct VirtualColumn {
    column_name: String,
    column_type: VirtualColumnType,
}

impl VirtualColumn {
    pub fn new(name: &str, column_type: VirtualColumnType) -> Self {
        VirtualColumn {
            column_name: name.to_string(),
            column_type,
        }
    }

    pub fn column_type(&self) -> &VirtualColumnType {
        &self.column_type
    }

    pub fn table_data_type(&self) -> TableDataType {
        match &self.column_type {
            VirtualColumnType::RowId => TableDataType::Number(NumberDataType::UInt64),
        }
    }

    pub fn data_type(&self) -> DataType {
        match &self.column_type {
            VirtualColumnType::RowId => DataType::Number(NumberDataType::UInt64),
        }
    }

    pub fn column_name(&self) -> &String {
        &self.column_name
    }

    pub fn column_id(&self) -> ColumnId {
        u32::MAX
    }

    pub fn generate_column_values(&self, meta: &VirtualColumnMeta, num_rows: usize) -> BlockEntry {
        match &self.column_type {
            VirtualColumnType::RowId => {
                let block_id = meta.block_idx as u64;
                let seg_id = meta.segment_idx as u64;
                let high_32bit = (block_id << 48) + (seg_id << 32);
                let mut builder =
                    NumberColumnBuilder::with_capacity(&NumberDataType::UInt64, num_rows);
                for i in 0..num_rows {
                    let row_id = high_32bit + i as u64;
                    builder.push(NumberScalar::UInt64(row_id));
                }
                BlockEntry {
                    data_type: DataType::Number(NumberDataType::UInt64),
                    value: Value::Column(Column::Number(builder.build())),
                }
            }
        }
    }
}
