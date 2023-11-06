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

use common_exception::ErrorCode;
use common_exception::Result;
use common_expression::types::string::StringColumnBuilder;
use common_expression::types::DataType;
use common_expression::types::NumberDataType;
use common_expression::types::NumberScalar;
use common_expression::types::UInt64Type;
use common_expression::BlockEntry;
use common_expression::BlockMetaInfo;
use common_expression::BlockMetaInfoDowncast;
use common_expression::BlockMetaInfoPtr;
use common_expression::ColumnId;
use common_expression::FromData;
use common_expression::Scalar;
use common_expression::TableDataType;
use common_expression::Value;
use common_expression::ORIGIN_BLOCK_ID_COLUMN_ID;
use common_expression::ORIGIN_BLOCK_ROW_NUM_COLUMN_ID;
use common_expression::ORIGIN_VERSION_COLUMN_ID;

// meta data for generate internal columns
#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, Default, PartialEq, Eq)]
pub struct StreamColumnMeta {
    pub block_id: Option<String>,
    pub version: u64,
}

#[typetag::serde(name = "stream_column_meta")]
impl BlockMetaInfo for StreamColumnMeta {
    fn equals(&self, info: &Box<dyn BlockMetaInfo>) -> bool {
        StreamColumnMeta::downcast_ref_from(info).is_some_and(|other| self == other)
    }

    fn clone_self(&self) -> Box<dyn BlockMetaInfo> {
        Box::new(self.clone())
    }
}

impl StreamColumnMeta {
    pub fn from_meta(info: &BlockMetaInfoPtr) -> Result<&StreamColumnMeta> {
        StreamColumnMeta::downcast_ref_from(info).ok_or(ErrorCode::Internal(
            "Cannot downcast from BlockMetaInfo to StreamColumnMeta.",
        ))
    }
}

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, PartialEq, Eq)]
pub enum StreamColumnType {
    OriginVersion,
    OriginBlockId,
    OriginRowNum,
}

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct StreamColumn {
    pub column_name: String,
    pub column_type: StreamColumnType,
}

impl StreamColumn {
    pub fn new(name: &str, column_type: StreamColumnType) -> Self {
        Self {
            column_name: name.to_string(),
            column_type,
        }
    }

    pub fn column_type(&self) -> &StreamColumnType {
        &self.column_type
    }

    pub fn table_data_type(&self) -> TableDataType {
        match self.column_type {
            StreamColumnType::OriginVersion => TableDataType::Number(NumberDataType::UInt64),
            StreamColumnType::OriginBlockId => TableDataType::String,
            StreamColumnType::OriginRowNum => TableDataType::Number(NumberDataType::UInt64),
        }
    }

    pub fn column_name(&self) -> &str {
        &self.column_name
    }

    pub fn data_type(&self) -> DataType {
        let t = &self.table_data_type();
        t.into()
    }

    pub fn column_id(&self) -> ColumnId {
        match &self.column_type {
            StreamColumnType::OriginVersion => ORIGIN_VERSION_COLUMN_ID,
            StreamColumnType::OriginBlockId => ORIGIN_BLOCK_ID_COLUMN_ID,
            StreamColumnType::OriginRowNum => ORIGIN_BLOCK_ROW_NUM_COLUMN_ID,
        }
    }

    pub fn generate_column_values(&self, meta: &StreamColumnMeta, num_rows: usize) -> BlockEntry {
        match &self.column_type {
            StreamColumnType::OriginVersion => BlockEntry::new(
                DataType::Number(NumberDataType::UInt64),
                Value::Scalar(Scalar::Number(NumberScalar::UInt64(meta.version))),
            ),
            StreamColumnType::OriginBlockId => {
                assert!(meta.block_id.is_some());
                let block_id = meta.block_id.clone().unwrap();
                let mut builder = StringColumnBuilder::with_capacity(1, block_id.len());
                builder.put_str(&block_id);
                builder.commit_row();
                BlockEntry::new(
                    DataType::String,
                    Value::Scalar(Scalar::String(builder.build_scalar())),
                )
            }
            StreamColumnType::OriginRowNum => {
                let mut row_ids = Vec::with_capacity(num_rows);
                for i in 0..num_rows {
                    row_ids.push(i as u64);
                }
                BlockEntry::new(
                    DataType::Number(NumberDataType::UInt64),
                    Value::Column(UInt64Type::from_data(row_ids)),
                )
            }
        }
    }
}
