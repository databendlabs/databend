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

use std::path::Path;

use common_arrow::arrow::bitmap::Bitmap;
use common_base::base::uuid::Uuid;
use common_exception::ErrorCode;
use common_exception::Result;
use common_expression::types::decimal::DecimalScalar;
use common_expression::types::nullable::NullableColumn;
use common_expression::types::AnyType;
use common_expression::types::DataType;
use common_expression::types::DecimalDataType;
use common_expression::types::DecimalSize;
use common_expression::types::NumberDataType;
use common_expression::types::NumberScalar;
use common_expression::types::UInt64Type;
use common_expression::BlockEntry;
use common_expression::BlockMetaInfo;
use common_expression::BlockMetaInfoDowncast;
use common_expression::BlockMetaInfoPtr;
use common_expression::Column;
use common_expression::ColumnId;
use common_expression::FromData;
use common_expression::Scalar;
use common_expression::TableDataType;
use common_expression::TableField;
use common_expression::Value;
use common_expression::ORIGIN_BLOCK_ID_COLUMN_ID;
use common_expression::ORIGIN_BLOCK_ID_COL_NAME;
use common_expression::ORIGIN_BLOCK_ROW_NUM_COLUMN_ID;
use common_expression::ORIGIN_BLOCK_ROW_NUM_COL_NAME;
use common_expression::ORIGIN_VERSION_COLUMN_ID;
use common_expression::ORIGIN_VERSION_COL_NAME;

// meta data for generate internal columns
#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, PartialEq)]
pub enum StreamColumnMeta {
    Append(u64),
    Mutation {
        block_id: u128,
        inner: Option<BlockMetaInfoPtr>,
    },
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

    pub fn build_origin_version(&self) -> Value<AnyType> {
        match self {
            StreamColumnMeta::Append(version) => {
                Value::Scalar(Scalar::Number(NumberScalar::UInt64(*version)))
            }
            StreamColumnMeta::Mutation { .. } => Value::Scalar(Scalar::Null),
        }
    }

    pub fn build_origin_block_id(&self) -> Value<AnyType> {
        match self {
            StreamColumnMeta::Append(_) => Value::Scalar(Scalar::Null),
            StreamColumnMeta::Mutation { block_id, .. } => Value::Scalar(Scalar::Decimal(
                DecimalScalar::Decimal128(*block_id as i128, DecimalSize {
                    precision: 38,
                    scale: 0,
                }),
            )),
        }
    }

    pub fn build_origin_block_row_num(&self, num_rows: usize) -> Value<AnyType> {
        match self {
            StreamColumnMeta::Append(_) => Value::Scalar(Scalar::Null),
            StreamColumnMeta::Mutation { .. } => {
                let mut row_ids = Vec::with_capacity(num_rows);
                for i in 0..num_rows {
                    row_ids.push(i as u64);
                }
                let column = UInt64Type::from_data(row_ids);
                Value::Column(Column::Nullable(Box::new(NullableColumn {
                    column,
                    validity: Bitmap::new_constant(true, num_rows),
                })))
            }
        }
    }

    pub fn inner_meta(&self) -> Option<BlockMetaInfoPtr> {
        match self {
            StreamColumnMeta::Append(_) => None,
            StreamColumnMeta::Mutation { inner, .. } => inner.clone(),
        }
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

    pub fn table_field(&self) -> TableField {
        TableField::new_from_column_id(&self.column_name, self.table_data_type(), self.column_id())
            .with_default_expr(Some("Null".to_string()))
    }

    pub fn column_type(&self) -> &StreamColumnType {
        &self.column_type
    }

    pub fn table_data_type(&self) -> TableDataType {
        match self.column_type {
            StreamColumnType::OriginVersion => {
                TableDataType::Nullable(Box::new(TableDataType::Number(NumberDataType::UInt64)))
            }
            StreamColumnType::OriginBlockId => TableDataType::Nullable(Box::new(
                TableDataType::Decimal(DecimalDataType::Decimal128(DecimalSize {
                    precision: 38,
                    scale: 0,
                })),
            )),
            StreamColumnType::OriginRowNum => {
                TableDataType::Nullable(Box::new(TableDataType::Number(NumberDataType::UInt64)))
            }
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
                DataType::Nullable(Box::new(DataType::Number(NumberDataType::UInt64))),
                meta.build_origin_version(),
            ),
            StreamColumnType::OriginBlockId => BlockEntry::new(
                DataType::Nullable(Box::new(DataType::Decimal(DecimalDataType::Decimal128(
                    DecimalSize {
                        precision: 38,
                        scale: 0,
                    },
                )))),
                meta.build_origin_block_id(),
            ),
            StreamColumnType::OriginRowNum => BlockEntry::new(
                DataType::Nullable(Box::new(DataType::Number(NumberDataType::UInt64))),
                meta.build_origin_block_row_num(num_rows),
            ),
        }
    }
}

pub fn gen_append_stream_columns() -> Vec<StreamColumn> {
    vec![
        StreamColumn::new(ORIGIN_VERSION_COL_NAME, StreamColumnType::OriginVersion),
        StreamColumn::new(ORIGIN_BLOCK_ID_COL_NAME, StreamColumnType::OriginBlockId),
        StreamColumn::new(
            ORIGIN_BLOCK_ROW_NUM_COL_NAME,
            StreamColumnType::OriginRowNum,
        ),
    ]
}

pub fn gen_mutation_stream_meta(
    inner: Option<BlockMetaInfoPtr>,
    path: &str,
) -> Result<StreamColumnMeta> {
    if let Some(file_stem) = Path::new(path).file_stem() {
        let file_strs = file_stem
            .to_str()
            .unwrap_or("")
            .split('_')
            .collect::<Vec<&str>>();
        let block_id = Uuid::parse_str(file_strs[0])
            .map_err(|e| e.to_string())?
            .as_u128();
        Ok(StreamColumnMeta::Mutation { block_id, inner })
    } else {
        Err(ErrorCode::Internal(format!(
            "Illegal meta file format: {}",
            path
        )))
    }
}
