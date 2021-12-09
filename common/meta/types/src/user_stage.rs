// Copyright 2021 Datafuse Labs.
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

use std::str::FromStr;

use common_exception::ErrorCode;
use common_exception::Result;

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, Eq, PartialEq)]
pub struct StageParams {
    pub url: String,
    pub credentials: Credentials,
}

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, Eq, PartialEq)]
pub enum Credentials {
    S3 {
        access_key_id: String,
        secret_access_key: String,
    },
}

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, Eq, PartialEq)]
pub struct FileFormat {
    format: Format,
    record_delimiter: String,
    field_delimiter: String,
    csv_header: bool,
    compression: Compression,
}

impl Default for FileFormat {
    fn default() -> Self {
        Self {
            record_delimiter: "\n".to_string(),
            field_delimiter: ",".to_string(),
            ..Default::default()
        }
    }


#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, Eq, PartialEq)]
pub enum Format {
    Csv,
    Parquet,
    Json,
}

impl Default for Format {
    fn default() -> Self {
        Self::Csv
    }
}


#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, Eq, PartialEq)]
pub enum Compression {
    Auto,
    Gzip,
    Bz2,
    Brotli,
    Zstd,
    Deflate,
    RawDeflate,
    Lzo,
    Snappy,
    None,
}

impl Default for Compression {
    fn default() -> Self {
        Self::None
    }
}


impl FromStr for Compression {
    type Err = &'static str;

    fn from_str(s: &str) -> std::result::Result<Compression, &'static str> {
        let s = s.to_uppercase();
        match s.as_str() {
            "AUTO" => Ok(Compression::Auto),
            "GZIP" => Ok(Compression::Gzip),
            "BZ2" => Ok(Compression::Bz2),
            "BROTLI" => Ok(Compression::Brotli),
            "ZSTD" => Ok(Compression::Zstd),
            "DEFLATE" => Ok(Compression::Deflate),
            "RAW_DEFLATE" => Ok(Compression::RawDeflate),
            "NONE" => Ok(Compression::None),
            _ => Err("no match for compression"),
        }
    }
}

impl StageParams {
    pub fn new(url: &str, credentials: Credentials) -> Self {
        StageParams {
            url: url.to_string(),
            credentials,
        }
    }
}
/// Stage for data stage location.
#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, Eq, PartialEq)]
pub struct UserStageInfo {
    #[serde(default)]
    pub stage_name: String,

    pub stage_params: StageParams,
    #[serde(default)]
    pub file_format: FileFormat,
    #[serde(default)]
    pub comments: String,
}

impl UserStageInfo {
    pub fn new(
        stage_name: &str,
        comments: &str,
        stage_params: StageParams,
        file_format:  FileFormat,
    ) -> Self {
        UserStageInfo {
            stage_name: stage_name.to_string(),
            comments: comments.to_string(),
            stage_params,
            file_format,
        }
    }
}

impl TryFrom<Vec<u8>> for UserStageInfo {
    type Error = ErrorCode;

    fn try_from(value: Vec<u8>) -> Result<Self> {
        match serde_json::from_slice(&value) {
            Ok(info) => Ok(info),
            Err(serialize_error) => Err(ErrorCode::IllegalUserInfoFormat(format!(
                "Cannot deserialize stage from bytes. cause {}",
                serialize_error
            ))),
        }
    }
}
