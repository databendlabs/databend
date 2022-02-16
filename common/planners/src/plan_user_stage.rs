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

use common_exception::ErrorCode;
use common_exception::Result;

#[derive(serde::Serialize, serde::Deserialize, PartialEq, Clone)]
pub enum FileFormatType {
    Csv,
    Json,
    Avro,
    Orc,
    Parquet,
    Xml,
}

impl FileFormatType {
    pub fn to_file_format(typ: &str) -> Result<FileFormatType> {
        Ok(match typ.to_uppercase().as_str() {
            "CSV" => FileFormatType::Csv,
            "JSON" => FileFormatType::Json,
            "AVRO" => FileFormatType::Avro,
            "ORC" => FileFormatType::Orc,
            "PARQUET" => FileFormatType::Parquet,
            "XML" => FileFormatType::Xml,
            _ => Err(ErrorCode::SyntaxException(
                "Unknown file format type, must one of  { CSV | JSON | AVRO | ORC | PARQUET | XML }",
            )),
        })
    }
}

#[derive(serde::Serialize, serde::Deserialize, PartialEq, Clone)]
pub struct DiskStorage {}

#[derive(serde::Serialize, serde::Deserialize, PartialEq, Clone)]
pub struct S3Storage {
    pub credentials_aws_key_id: String,
    pub credentials_aws_secret_key: String,
    pub encryption_master_key: String,
}

#[derive(serde::Serialize, serde::Deserialize, PartialEq, Clone)]
pub enum FileStorageType {
    Disk(DiskStorage),
    S3(S3Storage),
}

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, PartialEq)]
pub struct UserStagePlan {
    pub location: String,
    pub storage: FileStorageType,
    pub file_format_type: FileFormatType,
}
