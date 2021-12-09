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

use std::collections::HashMap;
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
    pub format: Format,
    pub record_delimiter: String,
    pub field_delimiter: String,
    pub csv_header: bool,
    pub compression: Compression,
}

impl Default for FileFormat {
    fn default() -> Self {
        Self {
            format: Format::default(),
            record_delimiter: "\n".to_string(),
            field_delimiter: ",".to_string(),
            csv_header: false,
            compression: Compression::default(),
        }
    }
}

impl FileFormat {
    pub fn inject_from_map(&mut self, map: HashMap<String, String>) -> Result<()> {
        for (k, v) in map.iter() {
            match k.to_lowercase().as_str() {
                "format" => self.format = Format::from_str(v)?,
                "record_delimiter" => self.record_delimiter = v.to_string(),
                "field_delimiter" => self.field_delimiter = v.to_string(),
                "csv_header" => self.csv_header = v == "1" || v == "true",
                "compression" => self.compression = Compression::from_str(v)?,
                other => {
                    return Err(ErrorCode::StrParseError(format!(
                        "no match for key:{}",
                        other
                    )))
                }
            }
        }

        Ok(())
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

impl FromStr for Format {
    type Err = ErrorCode;

    fn from_str(s: &str) -> Result<Format> {
        let s = s.to_lowercase();
        match s.as_str() {
            "csv" => Ok(Format::Csv),
            "parquet" => Ok(Format::Parquet),
            "json" => Ok(Format::Json),

            other => Err(ErrorCode::StrParseError(format!(
                "no match for format: {}",
                other
            ))),
        }
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
    type Err = ErrorCode;

    fn from_str(s: &str) -> Result<Compression> {
        let s = s.to_lowercase();
        match s.as_str() {
            "auto" => Ok(Compression::Auto),
            "gzip" => Ok(Compression::Gzip),
            "bz2" => Ok(Compression::Bz2),
            "brotli" => Ok(Compression::Brotli),
            "zstd" => Ok(Compression::Zstd),
            "deflate" => Ok(Compression::Deflate),
            "raw_deflate" => Ok(Compression::RawDeflate),
            "none" => Ok(Compression::None),
            other => Err(ErrorCode::StrParseError(format!(
                "no match for compression: {}",
                other
            ))),
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
    pub file_format: FileFormat,
    pub comments: String,
}

impl UserStageInfo {
    pub fn new(
        stage_name: &str,
        comments: &str,
        stage_params: StageParams,
        file_format: FileFormat,
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
