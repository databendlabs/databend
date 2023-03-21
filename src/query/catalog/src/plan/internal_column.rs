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

use common_expression::types::string::StringColumnBuilder;
use common_expression::types::DataType;
use common_expression::types::NumberDataType;
use common_expression::types::UInt64Type;
use common_expression::BlockEntry;
use common_expression::ColumnId;
use common_expression::FromData;
use common_expression::Scalar;
use common_expression::TableDataType;
use common_expression::Value;

// Segment and Block id Bits when generate internal column `_row_id`
// Since `DEFAULT_BLOCK_PER_SEGMENT` is 1000, so `block_id` 10 bits is enough.
const NUM_BLOCK_ID_BITS: usize = 10;
const NUM_SEGMENT_ID_BITS: usize = 22;

pub const ROW_ID: &str = "_row_id";
pub const SNAPSHOT_NAME: &str = "_snapshot_name";
pub const SEGMENT_NAME: &str = "_segment_name";
pub const BLOCK_NAME: &str = "_block_name";

// meta data for generate internal columns
#[derive(Debug)]
pub struct InternalColumnMeta {
    pub segment_id: usize,
    pub block_id: usize,
    pub block_location: String,
    pub segment_location: String,
    pub snapshot_location: String,
}

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, PartialEq)]
pub enum InternalColumnType {
    RowId,
    BlockName,
    SegmentName,
    SnapshotName,
}

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, PartialEq)]
pub struct InternalColumn {
    pub column_name: String,
    pub column_type: InternalColumnType,
}

impl InternalColumn {
    pub fn new(name: &str, column_type: InternalColumnType) -> Self {
        InternalColumn {
            column_name: name.to_string(),
            column_type,
        }
    }

    pub fn column_type(&self) -> &InternalColumnType {
        &self.column_type
    }

    pub fn table_data_type(&self) -> TableDataType {
        match &self.column_type {
            InternalColumnType::RowId => TableDataType::Number(NumberDataType::UInt64),
            InternalColumnType::BlockName => TableDataType::String,
            InternalColumnType::SegmentName => TableDataType::String,
            InternalColumnType::SnapshotName => TableDataType::String,
        }
    }

    pub fn data_type(&self) -> DataType {
        let t = &self.table_data_type();
        t.into()
    }

    pub fn column_name(&self) -> &String {
        &self.column_name
    }

    pub fn column_id(&self) -> ColumnId {
        match &self.column_type {
            InternalColumnType::RowId => u32::MAX,
            InternalColumnType::BlockName => u32::MAX - 1,
            InternalColumnType::SegmentName => u32::MAX - 2,
            InternalColumnType::SnapshotName => u32::MAX - 3,
        }
    }

    pub fn generate_column_values(&self, meta: &InternalColumnMeta, num_rows: usize) -> BlockEntry {
        match &self.column_type {
            InternalColumnType::RowId => {
                let block_id = meta.block_id as u64;
                let seg_id = meta.segment_id as u64;
                let high_32bit = (seg_id << NUM_SEGMENT_ID_BITS) + (block_id << NUM_BLOCK_ID_BITS);
                let mut row_ids = Vec::with_capacity(num_rows);
                for i in 0..num_rows {
                    let row_id = high_32bit + i as u64;
                    row_ids.push(row_id);
                }
                BlockEntry {
                    data_type: DataType::Number(NumberDataType::UInt64),
                    value: Value::Column(UInt64Type::from_data(row_ids)),
                }
            }
            InternalColumnType::BlockName => {
                let mut builder = StringColumnBuilder::with_capacity(1, meta.block_location.len());
                builder.put_str(&meta.block_location);
                builder.commit_row();
                BlockEntry {
                    data_type: DataType::String,
                    value: Value::Scalar(Scalar::String(builder.build_scalar())),
                }
            }
            InternalColumnType::SegmentName => {
                let mut builder =
                    StringColumnBuilder::with_capacity(1, meta.segment_location.len());
                builder.put_str(&meta.segment_location);
                builder.commit_row();
                BlockEntry {
                    data_type: DataType::String,
                    value: Value::Scalar(Scalar::String(builder.build_scalar())),
                }
            }
            InternalColumnType::SnapshotName => {
                let mut builder =
                    StringColumnBuilder::with_capacity(1, meta.snapshot_location.len());
                builder.put_str(&meta.snapshot_location);
                builder.commit_row();
                BlockEntry {
                    data_type: DataType::String,
                    value: Value::Scalar(Scalar::String(builder.build_scalar())),
                }
            }
        }
    }
}
