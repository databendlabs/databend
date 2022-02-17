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

#[derive(serde::Serialize, serde::Deserialize, PartialEq, Debug, Clone)]
pub enum FileFormatType {
    Csv,
    Json,
    Avro,
    Orc,
    Parquet,
    Xml,
}

#[derive(serde::Serialize, serde::Deserialize, PartialEq, Debug, Clone)]
pub struct DiskStoragePlan {}

#[derive(serde::Serialize, serde::Deserialize, PartialEq, Debug, Clone)]
pub struct S3StoragePlan {
    pub credentials_aws_key_id: String,
    pub credentials_aws_secret_key: String,
    pub encryption_master_key: String,
}

#[derive(serde::Serialize, serde::Deserialize, PartialEq, Debug, Clone)]
pub enum FileStoragePlan {
    // Location is local.
    Disk(DiskStoragePlan),

    // Location is aws s3.
    S3(S3StoragePlan),
}

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, PartialEq)]
pub struct UserStagePlan {
    pub location: String,
    pub storage: FileStoragePlan,
    pub file_format_type: FileFormatType,
}
