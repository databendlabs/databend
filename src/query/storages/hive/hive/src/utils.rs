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

use std::fmt::Debug;

use databend_common_catalog::partition_columns::str_to_scalar;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::Scalar;
use databend_common_expression::TableField;
use databend_common_expression::types::DataType;
use databend_storages_common_pruner::partition_prunner::FetchPartitionScalars;
use volo_thrift::MaybeException;

use crate::hive_table::HIVE_DEFAULT_PARTITION;

pub(crate) fn str_field_to_scalar(value: &str, data_type: &DataType) -> Result<Scalar> {
    match data_type {
        DataType::Nullable(c) => {
            if value == HIVE_DEFAULT_PARTITION {
                Ok(Scalar::Null)
            } else {
                str_field_to_scalar(value, c.as_ref())
            }
        }
        _ => str_to_scalar(value, data_type),
    }
}

pub struct HiveFetchPartitionScalars;

impl FetchPartitionScalars<String> for HiveFetchPartitionScalars {
    fn eval(value: &String, partition_fields: &[TableField]) -> Result<Vec<Scalar>> {
        let mut res = Vec::new();
        let v = value.split('/');
        let mut idx = 0;
        for singe_value in v {
            let kv = singe_value.split('=').collect::<Vec<&str>>();
            if kv.len() == 2 {
                let field = &partition_fields[idx];
                let scalar = str_field_to_scalar(kv[1], &field.data_type().into())?;
                res.push(scalar);
                idx += 1;
            }
        }
        if res.len() != partition_fields.len() {
            Err(ErrorCode::ParquetFileInvalid(format!(
                "Partition values mismatch, expect {}, got {} in {}",
                partition_fields.len(),
                res.len(),
                value
            )))
        } else {
            Ok(res)
        }
    }
}

/// Format a thrift error into iceberg error.
///
/// Please only throw this error when you are sure that the error is caused by thrift.
pub fn from_thrift_error(error: impl std::error::Error) -> ErrorCode {
    ErrorCode::Internal(format!(
        "thrift error: {:?}, please check your thrift client config",
        error
    ))
}

/// Format a thrift exception into iceberg error.
pub fn from_thrift_exception<T, E: Debug>(value: MaybeException<T, E>) -> Result<T> {
    match value {
        MaybeException::Ok(v) => Ok(v),
        MaybeException::Exception(err) => Err(ErrorCode::Internal(format!(
            "thrift error: {err:?}, please check your thrift client config"
        ))),
    }
}
